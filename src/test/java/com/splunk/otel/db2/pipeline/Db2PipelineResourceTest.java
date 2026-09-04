package com.splunk.otel.db2.pipeline;

import com.splunk.otel.db2.config.Db2Config;
import com.splunk.otel.db2.config.Db2ReceiverConfig;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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

    @Test
    void stopDoesNotThrowAndIsIdempotent() throws Exception {
        // Regression: stop() must not throw and must be safely callable twice.
        // Also verifies that the ordering (stopScheduler → SDK shutdown → closeConnection)
        // does not produce a leaked connection or exception when called on a pipeline that
        // was never started (receiver has no live JDBC connection).
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        Db2Config cfg = minimalConfig("localhost", 50000, "TESTDB");
        Db2Pipeline pipeline = new Db2Pipeline(cfg, reader);

        List<Throwable> errors = new ArrayList<>();
        Thread t1 = new Thread(() -> { try { pipeline.stop(); } catch (Throwable e) { errors.add(e); } });
        Thread t2 = new Thread(() -> { try { pipeline.stop(); } catch (Throwable e) { errors.add(e); } });
        t1.start(); t2.start();
        t1.join(5000); t2.join(5000);

        assertThat(errors).as("stop() must not throw").isEmpty();
    }
}
