package com.splunk.otel.db2.pipeline;

import com.splunk.otel.db2.config.Db2Config;
import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.receiver.Db2Receiver;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.CollectionRegistration;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class Db2PipelineResourceTest {

    private static Db2Config minimalConfig(String hostname, int port, String database) {
        Db2ReceiverConfig rc = new Db2ReceiverConfig();
        rc.setHostname(hostname);
        rc.setHostport(port);
        rc.setUsername("user");
        rc.setPassword("pass");
        rc.setDatabase(database);

        Db2Config cfg = new Db2Config();
        cfg.setReceivers(List.of(rc));
        return cfg;
    }

    @Test
    void loopbackConfigProducesHostQualifiedInstanceId() throws Exception {
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        Db2Config cfg = minimalConfig("localhost", 50000, "TESTDB");

        Db2Pipeline pipeline = new Db2Pipeline(cfg, reader);

        // Access meterProvider via reflection to record a dummy counter
        Field mpField = Db2Pipeline.class.getDeclaredField("meterProvider");
        mpField.setAccessible(true);
        SdkMeterProvider meterProvider = (SdkMeterProvider) mpField.get(pipeline);

        Meter meter = meterProvider.get("splunk-otel-db2");
        meter.counterBuilder("test.counter").build().add(1);

        Collection<MetricData> metrics = reader.collectAllMetrics();
        assertThat(metrics).isNotEmpty();

        Resource resource = metrics.iterator().next().getResource();
        String instanceId = resource.getAttribute(AttributeKey.stringKey("service.instance.id"));

        String expectedHost = InetAddress.getLocalHost().getHostName();
        assertThat(instanceId)
            .as("service.instance.id should be qualified with the resolved hostname, not 'localhost'")
            .startsWith(expectedHost)
            .doesNotStartWith("localhost:");
    }

    /**
     * Verifies that the pipeline-level AtomicBoolean guard prevents a concurrent second stop()
     * call from closing JDBC while the first caller is still inside the SDK shutdown (simulated
     * here by a blocking MetricReader).
     *
     * Sequence:
     *   T1: stop() → stopScheduler() → meterProvider.shutdown() → [blocks in reader.shutdown()]
     *   T2: stop() → sees stopped=true → returns immediately (must NOT reach receiver.stop())
     *   main: releases the block → T1 finishes → receiver.stop() called exactly once
     */
    @Test
    void concurrentStopIsSerializedByPipelineGuard() throws Exception {
        // Latches to coordinate the race scenario
        CountDownLatch shutdownEntered = new CountDownLatch(1); // T1 is inside reader.shutdown()
        CountDownLatch shutdownRelease = new CountDownLatch(1); // allows T1 to complete

        // A MetricReader whose shutdown() blocks until released, simulating PeriodicMetricReader's
        // final collection window.
        MetricReader blockingReader = new MetricReader() {
            @Override public void register(CollectionRegistration r) {}
            @Override public AggregationTemporality getAggregationTemporality(InstrumentType t) {
                return AggregationTemporality.CUMULATIVE;
            }
            @Override public Aggregation getDefaultAggregation(InstrumentType t) {
                return Aggregation.defaultAggregation();
            }
            @Override public CompletableResultCode forceFlush() {
                return CompletableResultCode.ofSuccess();
            }
            @Override public CompletableResultCode shutdown() {
                shutdownEntered.countDown();           // signal: T1 is now blocked
                try { shutdownRelease.await(10, TimeUnit.SECONDS); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                return CompletableResultCode.ofSuccess();
            }
        };

        Db2Config cfg = minimalConfig("db2host.example.com", 50000, "TESTDB");
        Db2Pipeline pipeline = new Db2Pipeline(cfg, blockingReader);

        // Observe how many times receiver.stop() is called via the AtomicBoolean field
        Field receiverField = Db2Pipeline.class.getDeclaredField("receiver");
        receiverField.setAccessible(true);
        Db2Receiver receiver = (Db2Receiver) receiverField.get(pipeline);
        Field stoppedField = Db2Receiver.class.getDeclaredField("stopped");
        stoppedField.setAccessible(true);
        AtomicBoolean receiverStopped = (AtomicBoolean) stoppedField.get(receiver);

        CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();

        Thread t1 = new Thread(() -> {
            try { pipeline.stop(); } catch (Throwable e) { errors.add(e); }
        }, "t1-stop");
        Thread t2 = new Thread(() -> {
            try { pipeline.stop(); } catch (Throwable e) { errors.add(e); }
        }, "t2-stop");

        t1.start();
        // Wait until T1 is confirmed blocked inside the reader shutdown
        assertThat(shutdownEntered.await(5, TimeUnit.SECONDS))
            .as("T1 should have entered reader.shutdown() within 5s").isTrue();

        // receiver.stop() must NOT have been called yet — JDBC still open
        assertThat(receiverStopped.get())
            .as("receiver.stop() must not have been called while SDK shutdown is in progress")
            .isFalse();

        // T2 fires while T1 is blocked — the pipeline guard must make it return immediately
        t2.start();
        t2.join(2000);
        assertThat(t2.isAlive())
            .as("T2 should have returned immediately due to the pipeline-level stopped guard")
            .isFalse();

        // JDBC still not closed — T1 is still inside meterProvider.shutdown()
        assertThat(receiverStopped.get())
            .as("receiver.stop() must not have been called while T1 is still in SDK shutdown")
            .isFalse();

        // Release T1 to complete the shutdown sequence
        shutdownRelease.countDown();
        t1.join(5000);
        assertThat(t1.isAlive()).as("T1 should have completed after the latch was released").isFalse();

        // Now receiver.stop() should have been called exactly once
        assertThat(receiverStopped.get())
            .as("receiver.stop() should have been called exactly once after SDK shutdown completed")
            .isTrue();

        assertThat(errors).as("no exceptions from either stop() call").isEmpty();
    }
}
