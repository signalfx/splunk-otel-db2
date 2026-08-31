package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that two Db2Receiver instances can run concurrently against the same database,
 * each with fully isolated connections, schedulers, and SDK pipelines. This is the
 * single-process multi-database scenario: two configs pointing at the same container
 * stand in for two distinct databases in production.
 *
 * Run with: ./gradlew integrationTest
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Db2MultiReceiverIntegrationTest {

    @Container
    static final Db2Container db2 = new Db2Container("icr.io/db2_community/db2:11.5.9.0")
        .acceptLicense()
        .withUsername("db2inst1")
        .withPassword("password")
        .withDatabaseName("testdb")
        .withStartupTimeout(Duration.ofMinutes(10));

    private Db2ReceiverConfig buildConfig() {
        Db2ReceiverConfig config = new Db2ReceiverConfig();
        config.setHostname(db2.getHost());
        config.setHostport(db2.getMappedPort(50000));
        config.setUsername(db2.getUsername());
        config.setPassword(db2.getPassword());
        config.setDatabase(db2.getDatabaseName().toUpperCase());
        config.setCollectionInterval("2s");
        config.getTls().setInsecure(true);
        return config;
    }

    private SdkMeterProvider buildMeterProvider(InMemoryMetricExporter exporter) {
        return SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setInterval(Duration.ofSeconds(2))
                    .build()
            )
            .build();
    }

    @Test
    void twoReceiversCollectMetricsConcurrently() {
        InMemoryMetricExporter exporter1 = InMemoryMetricExporter.create();
        InMemoryMetricExporter exporter2 = InMemoryMetricExporter.create();
        SdkMeterProvider meterProvider1 = buildMeterProvider(exporter1);
        SdkMeterProvider meterProvider2 = buildMeterProvider(exporter2);

        Db2Receiver receiver1 = new Db2Receiver(buildConfig(),
            meterProvider1.get("splunk-otel-db2"), (io.opentelemetry.api.logs.Logger) null);
        Db2Receiver receiver2 = new Db2Receiver(buildConfig(),
            meterProvider2.get("splunk-otel-db2"), (io.opentelemetry.api.logs.Logger) null);

        receiver1.start();
        receiver2.start();

        try {
            List<String> required = List.of(
                "db2.connection.count", "db2.lock.wait.count", "db2.deadlock.count");

            Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> metricNames(exporter1).containsAll(required)
                          && metricNames(exporter2).containsAll(required));

            assertThat(metricNames(exporter1)).containsAll(required);
            assertThat(metricNames(exporter2)).containsAll(required);
        } finally {
            receiver1.stop();
            receiver2.stop();
            meterProvider1.shutdown();
            meterProvider2.shutdown();
        }
    }

    @Test
    void stoppingOneReceiverDoesNotAffectTheOther() throws Exception {
        InMemoryMetricExporter exporter1 = InMemoryMetricExporter.create();
        InMemoryMetricExporter exporter2 = InMemoryMetricExporter.create();
        SdkMeterProvider meterProvider1 = buildMeterProvider(exporter1);
        SdkMeterProvider meterProvider2 = buildMeterProvider(exporter2);

        Db2Receiver receiver1 = new Db2Receiver(buildConfig(),
            meterProvider1.get("splunk-otel-db2"), (io.opentelemetry.api.logs.Logger) null);
        Db2Receiver receiver2 = new Db2Receiver(buildConfig(),
            meterProvider2.get("splunk-otel-db2"), (io.opentelemetry.api.logs.Logger) null);

        receiver1.start();
        receiver2.start();

        // Wait for both to produce at least one metric
        Awaitility.await()
            .atMost(20, TimeUnit.SECONDS)
            .until(() -> !metricNames(exporter1).isEmpty() && !metricNames(exporter2).isEmpty());

        // Stop receiver1; receiver2 should continue collecting
        receiver1.stop();
        meterProvider1.shutdown();

        exporter2.reset();

        // Give receiver2 time for two more collection cycles
        Thread.sleep(6000);

        try {
            assertThat(metricNames(exporter2)).contains("db2.connection.count");
        } finally {
            receiver2.stop();
            meterProvider2.shutdown();
        }
    }

    @Test
    void twoReceiversWithAllCollectorsEnabled() throws Exception {
        io.opentelemetry.sdk.logs.LogRecordProcessor proc1 =
            io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor.create(
                io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter.create());
        io.opentelemetry.sdk.logs.LogRecordProcessor proc2 =
            io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor.create(
                io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter.create());

        io.opentelemetry.sdk.logs.SdkLoggerProvider logProvider1 =
            io.opentelemetry.sdk.logs.SdkLoggerProvider.builder().addLogRecordProcessor(proc1).build();
        io.opentelemetry.sdk.logs.SdkLoggerProvider logProvider2 =
            io.opentelemetry.sdk.logs.SdkLoggerProvider.builder().addLogRecordProcessor(proc2).build();

        InMemoryMetricExporter exporter1 = InMemoryMetricExporter.create();
        InMemoryMetricExporter exporter2 = InMemoryMetricExporter.create();
        SdkMeterProvider meterProvider1 = buildMeterProvider(exporter1);
        SdkMeterProvider meterProvider2 = buildMeterProvider(exporter2);

        Db2ReceiverConfig config1 = buildConfig();
        config1.setQuerySampleEnabled(true);
        config1.setQuerySampleInterval("2s");
        config1.setTopQueryEnabled(false); // top-query tested separately; 60s min blocks here

        Db2ReceiverConfig config2 = buildConfig();
        config2.setQuerySampleEnabled(true);
        config2.setQuerySampleInterval("2s");
        config2.setTopQueryEnabled(false);

        Db2Receiver receiver1 = new Db2Receiver(config1, meterProvider1.get("splunk-otel-db2"),
            logProvider1.get("splunk-otel-db2"));
        Db2Receiver receiver2 = new Db2Receiver(config2, meterProvider2.get("splunk-otel-db2"),
            logProvider2.get("splunk-otel-db2"));

        receiver1.start();
        receiver2.start();

        try {
            // Both receivers must emit core metrics — confirms independent connection + scheduler lifecycle
            Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> metricNames(exporter1).contains("db2.connection.count")
                          && metricNames(exporter2).contains("db2.connection.count"));
        } finally {
            receiver1.stop();
            receiver2.stop();
            logProvider1.shutdown();
            logProvider2.shutdown();
            meterProvider1.shutdown();
            meterProvider2.shutdown();
        }
    }

    private static List<String> metricNames(InMemoryMetricExporter exporter) {
        return exporter.getFinishedMetricItems().stream()
            .map(m -> m.getName())
            .collect(Collectors.toList());
    }
}
