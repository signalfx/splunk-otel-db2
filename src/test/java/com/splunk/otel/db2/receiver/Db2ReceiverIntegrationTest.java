package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test — spins up a real DB2 container via Testcontainers.
 *
 * Run with: ./gradlew integrationTest
 * Skipped by default in ./gradlew build (requires Docker).
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Db2ReceiverIntegrationTest {

    @Container
    static final Db2Container db2 = new Db2Container("icr.io/db2_community/db2:latest")
        .acceptLicense()
        .withUsername("db2inst1")
        .withPassword("password")
        .withDatabaseName("testdb")
        .withStartupTimeout(Duration.ofMinutes(10));

    private InMemoryMetricExporter exporter;
    private SdkMeterProvider meterProvider;
    private Db2Receiver receiver;

    @BeforeAll
    void setUp() {
        exporter = InMemoryMetricExporter.create();
        meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setInterval(Duration.ofSeconds(2))
                    .build()
            )
            .build();

        Db2ReceiverConfig config = new Db2ReceiverConfig();
        config.setHostname(db2.getHost());
        config.setHostport(db2.getMappedPort(50000));
        config.setUsername(db2.getUsername());
        config.setPassword(db2.getPassword());
        config.setDatabase(db2.getDatabaseName().toUpperCase());

        receiver = new Db2Receiver(config, meterProvider.get("splunk-otel-db2"), (io.opentelemetry.api.logs.Logger) null);
        receiver.start();
    }

    @AfterAll
    void tearDown() {
        if (receiver != null) receiver.stop();
        if (meterProvider != null) meterProvider.shutdown();
    }

    @Test
    void allExpectedMetricsAreEmitted() {
        Set<String> expected = Set.of(
            "db2.user.connection.count",
            "db2.deadlocks",
            "db2.lock.waits",
            "db2.lock.wait.time",
            "db2.lock.timeouts",
            "db2.buffer_pool.hit_ratio",
            "db2.buffer_pool.reads",
            "db2.buffer_pool.physical_reads",
            "db2.tablespace.size"
        );

        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                Set<String> actual = exporter.getFinishedMetricItems().stream()
                    .map(MetricData::getName)
                    .collect(Collectors.toSet());
                assertThat(actual).containsAll(expected);
            });
    }

    @Test
    void metricsCarryDbNamespaceAttribute() {
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                List<MetricData> items = exporter.getFinishedMetricItems();
                assertThat(items).isNotEmpty();

                for (MetricData metric : items) {
                    Stream<? extends PointData> points = allPoints(metric);
                    points.forEach(point -> {
                        String ns = point.getAttributes().get(AttributeKey.stringKey("db.namespace"));
                        assertThat(ns)
                            .as("db.namespace on %s", metric.getName())
                            .isNotNull()
                            .isNotBlank();
                    });
                }
            });
    }

    @Test
    void bufferPoolMetricsHaveBufferPoolAttribute() {
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                List<MetricData> items = exporter.getFinishedMetricItems();
                MetricData reads = items.stream()
                    .filter(m -> m.getName().equals("db2.buffer_pool.reads"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("db2.buffer_pool.reads not found"));

                allPoints(reads).forEach(point -> {
                    assertThat(point.getAttributes().get(AttributeKey.stringKey("db2.buffer_pool")))
                        .as("db2.buffer_pool attribute")
                        .isNotNull();
                    assertThat(point.getAttributes().get(AttributeKey.stringKey("db2.read_type")))
                        .as("db2.read_type attribute")
                        .isIn("data", "index");
                });
            });
    }

    @Test
    void tablespaceMetricsHaveStateAttribute() {
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                List<MetricData> items = exporter.getFinishedMetricItems();
                MetricData size = items.stream()
                    .filter(m -> m.getName().equals("db2.tablespace.size"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("db2.tablespace.size not found"));

                Set<String> states = allPoints(size)
                    .map(p -> p.getAttributes().get(AttributeKey.stringKey("db2.tablespace.state")))
                    .collect(Collectors.toSet());
                assertThat(states).containsAll(Set.of("used", "free", "capacity"));
            });
    }

    @Test
    void connectionCountIsNonNegative() {
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                List<MetricData> items = exporter.getFinishedMetricItems();
                MetricData connections = items.stream()
                    .filter(m -> m.getName().equals("db2.user.connection.count"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("db2.user.connection.count not found"));

                connections.getLongSumData().getPoints().forEach(point ->
                    assertThat(point.getValue()).isGreaterThanOrEqualTo(1L) // at least our own connection
                );
            });
    }

    private static Stream<? extends PointData> allPoints(MetricData metric) {
        switch (metric.getType()) {
            case LONG_SUM:         return metric.getLongSumData().getPoints().stream();
            case DOUBLE_SUM:       return metric.getDoubleSumData().getPoints().stream();
            case LONG_GAUGE:       return metric.getLongGaugeData().getPoints().stream();
            case DOUBLE_GAUGE:     return metric.getDoubleGaugeData().getPoints().stream();
            case HISTOGRAM:        return metric.getHistogramData().getPoints().stream();
            case EXPONENTIAL_HISTOGRAM: return metric.getExponentialHistogramData().getPoints().stream();
            default:               return Stream.empty();
        }
    }
}
