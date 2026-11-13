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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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

    // The test constructor does not run the instance-name lookup, so this also covers the
    // production path where the lookup failed: the attribute must be absent rather than empty,
    // since an empty dimension reads as a real value.
    @Test
    void instanceNameIsAbsentWhenItWasNotCollected() throws Exception {
        Resource resource = resourceOf(minimalConfig("db2.example.com", 50000, "TESTDB"));

        assertThat(resource.getAttribute(AttributeKey.stringKey("db2.instance.name")))
            .as("db2.instance.name must be omitted, not blank, when the lookup yielded nothing")
            .isNull();
    }

    // Supplies a collected value AND a conflicting override, which is the only arrangement that
    // pins the precedence: with one side null, reversing the order still passes.
    @Test
    void anOverrideBeatsACollectedInstanceName() {
        Db2ReceiverConfig rc = minimalConfig("db2.example.com", 50000, "TESTDB").getReceivers().get(0);
        rc.setResourceAttributes(Map.of("db2.instance.name", "from-config"));

        Resource resource = Db2Pipeline.buildResource(rc, "db2.example.com", "from-server");

        assertThat(resource.getAttribute(AttributeKey.stringKey("db2.instance.name")))
            .as("the operator's override must win over the collected value")
            .isEqualTo("from-config");
    }

    // A blank override is treated as unset rather than honoured: exporting an empty dimension
    // would undo the reason a failed lookup omits the attribute entirely.
    @Test
    void aBlankOverrideNeitherWinsNorErasesTheCollectedName() {
        Db2ReceiverConfig rc = minimalConfig("db2.example.com", 50000, "TESTDB").getReceivers().get(0);
        rc.setResourceAttributes(Map.of("db2.instance.name", "   "));

        assertThat(Db2Pipeline.buildResource(rc, "db2.example.com", "from-server")
            .getAttribute(AttributeKey.stringKey("db2.instance.name")))
            .as("a blank override must not overwrite a collected name")
            .isEqualTo("from-server");

        assertThat(Db2Pipeline.buildResource(rc, "db2.example.com", null)
            .getAttribute(AttributeKey.stringKey("db2.instance.name")))
            .as("and must not be exported as an empty dimension on its own")
            .isNull();

        assertThat(Db2Pipeline.instanceNameLookupNeeded(rc))
            .as("a blank override must not suppress the lookup either")
            .isTrue();
    }

    // Exercises the caller, not just the predicate: deleting the guard inside resolveInstanceName
    // leaves a predicate-only test green.
    @Test
    void theLookupIsNotInvokedWhenAnOverrideIsPresent() {
        Db2ReceiverConfig rc = minimalConfig("db2.example.com", 50000, "TESTDB").getReceivers().get(0);
        AtomicInteger invocations = new AtomicInteger();
        Db2Pipeline.InstanceNameLookup counting = (config, label) -> {
            invocations.incrementAndGet();
            return "from-server";
        };

        assertThat(Db2Pipeline.resolveInstanceName(rc, "", counting)).isEqualTo("from-server");
        assertThat(invocations.get()).as("no override, so the server must be asked").isEqualTo(1);

        rc.setResourceAttributes(Map.of("db2.instance.name", "from-config"));
        assertThat(Db2Pipeline.resolveInstanceName(rc, "", counting))
            .as("the override is applied later by buildResource, so nothing is collected here")
            .isNull();
        assertThat(invocations.get())
            .as("the override makes the connect pure waste, so it must not happen")
            .isEqualTo(1);
    }

    // Pipelines are constructed serially, so a lookup against an unreachable database delays every
    // later receiver by its connect_timeout. When the value would be discarded anyway, the connect
    // must not happen at all.
    @Test
    void anOverrideSkipsTheLookupEntirely() {
        Db2ReceiverConfig rc = minimalConfig("db2.example.com", 50000, "TESTDB").getReceivers().get(0);
        assertThat(Db2Pipeline.instanceNameLookupNeeded(rc))
            .as("no override configured, so the instance name has to be collected")
            .isTrue();

        rc.setResourceAttributes(Map.of("db2.instance.name", "db2inst9"));
        assertThat(Db2Pipeline.instanceNameLookupNeeded(rc))
            .as("override present, so connecting to collect a value we would discard is waste")
            .isFalse();
    }

    // Without this the attrs.put wiring could be deleted and every other test would still pass:
    // the production path needs a live Db2, and the override test goes through the
    // resource_attributes loop rather than the collected value.
    @Test
    void aCollectedNameReachesTheResource() {
        Db2ReceiverConfig rc = minimalConfig("db2.example.com", 50000, "TESTDB").getReceivers().get(0);

        Resource resource = Db2Pipeline.buildResource(rc, "db2.example.com", "db2inst1");

        assertThat(resource.getAttribute(AttributeKey.stringKey("db2.instance.name")))
            .isEqualTo("db2inst1");
    }

    // A blank or absent name must not reach the Resource as an empty dimension.
    @Test
    void aBlankCollectedNameIsNotPutOnTheResource() {
        Db2ReceiverConfig rc = minimalConfig("db2.example.com", 50000, "TESTDB").getReceivers().get(0);

        assertThat(Db2Pipeline.buildResource(rc, "db2.example.com", "   ")
            .getAttribute(AttributeKey.stringKey("db2.instance.name"))).isNull();
        assertThat(Db2Pipeline.buildResource(rc, "db2.example.com", null)
            .getAttribute(AttributeKey.stringKey("db2.instance.name"))).isNull();
    }

    // An unrelated override must not suppress the lookup.
    @Test
    void anUnrelatedOverrideStillLeavesTheLookupNeeded() {
        Db2ReceiverConfig rc = minimalConfig("db2.example.com", 50000, "TESTDB").getReceivers().get(0);
        rc.setResourceAttributes(Map.of("deployment.environment", "production"));

        assertThat(Db2Pipeline.instanceNameLookupNeeded(rc)).isTrue();
    }

    /** Builds a pipeline and returns the Resource stamped on its metrics. */
    private static Resource resourceOf(Db2Config cfg) throws Exception {
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        Db2Pipeline pipeline = new Db2Pipeline(cfg, reader);

        Field mpField = Db2Pipeline.class.getDeclaredField("meterProvider");
        mpField.setAccessible(true);
        SdkMeterProvider meterProvider = (SdkMeterProvider) mpField.get(pipeline);
        meterProvider.get("splunk-otel-db2").counterBuilder("test.counter").build().add(1);

        Collection<MetricData> metrics = reader.collectAllMetrics();
        assertThat(metrics).isNotEmpty();
        return metrics.iterator().next().getResource();
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
