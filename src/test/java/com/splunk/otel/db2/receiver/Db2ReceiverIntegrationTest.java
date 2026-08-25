package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test — spins up a real DB2 container and verifies the
 * pipeline can connect, collect metrics, and start/stop cleanly.
 *
 * Run with: ./gradlew integrationTest
 * Skipped by default in ./gradlew build (requires Docker).
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Db2ReceiverIntegrationTest {

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
        config.getTls().setInsecure(true); // Testcontainers DB2 runs plain TCP, no TLS
        return config;
    }

    @Test
    void receiverConnectsAndStartsCleanly() {
        InMemoryMetricExporter exporter = InMemoryMetricExporter.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setInterval(Duration.ofSeconds(2))
                    .build()
            )
            .build();

        Db2Receiver receiver = new Db2Receiver(buildConfig(), meterProvider.get("splunk-otel-db2"),
            (io.opentelemetry.api.logs.Logger) null);
        receiver.start();
        receiver.stop();
        meterProvider.shutdown();
    }

    @Test
    void databaseMetricsAreEmitted() {
        InMemoryMetricExporter exporter = InMemoryMetricExporter.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setInterval(Duration.ofSeconds(2))
                    .build()
            )
            .build();

        Db2Receiver receiver = new Db2Receiver(buildConfig(), meterProvider.get("splunk-otel-db2"), (io.opentelemetry.api.logs.Logger) null);
        receiver.start();

        Awaitility.await()
            .atMost(15, TimeUnit.SECONDS)
            .until(() -> metricNames(exporter).containsAll(List.of(
                "db2.connection.count",
                "db2.lock.wait.count",
                "db2.deadlock.count",
                "db2.sort.count"
            )));

        assertThat(metricNames(exporter)).contains(
            "db2.connection.count",
            "db2.connection.active",
            "db2.connection.max",
            "db2.lock.wait.count",
            "db2.lock.wait_time",
            "db2.lock.timeout.count",
            "db2.lock.escalation.count",
            "db2.lock.space_used",
            "db2.deadlock.count",
            "db2.sort.count",
            "db2.sort.overflow.count"
        );

        receiver.stop();
        meterProvider.shutdown();
    }

    @Test
    void bufferpoolMetricsAreEmitted() {
        InMemoryMetricExporter exporter = InMemoryMetricExporter.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setInterval(Duration.ofSeconds(2))
                    .build()
            )
            .build();

        Db2Receiver receiver = new Db2Receiver(buildConfig(), meterProvider.get("splunk-otel-db2"), (io.opentelemetry.api.logs.Logger) null);
        receiver.start();

        Awaitility.await()
            .atMost(15, TimeUnit.SECONDS)
            .until(() -> metricNames(exporter).contains("db2.buffer_pool.logical_reads"));

        assertThat(metricNames(exporter)).contains(
            "db2.buffer_pool.logical_reads",
            "db2.buffer_pool.physical_reads",
            "db2.buffer_pool.writes",
            "db2.buffer_pool.read_time",
            "db2.buffer_pool.write_time"
        );

        receiver.stop();
        meterProvider.shutdown();
    }

    @Test
    void tablespaceMetricsAreEmitted() {
        InMemoryMetricExporter exporter = InMemoryMetricExporter.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setInterval(Duration.ofSeconds(2))
                    .build()
            )
            .build();

        Db2Receiver receiver = new Db2Receiver(buildConfig(), meterProvider.get("splunk-otel-db2"), (io.opentelemetry.api.logs.Logger) null);
        receiver.start();

        Awaitility.await()
            .atMost(15, TimeUnit.SECONDS)
            .until(() -> metricNames(exporter).contains("db2.tablespace.size"));

        // verify all three usage dimensions are present
        assertThat(exporter.getFinishedMetricItems()).anySatisfy(metric -> {
            assertThat(metric.getName()).isEqualTo("db2.tablespace.size");
            assertThat(metric.getLongSumData().getPoints()).anySatisfy(pt ->
                assertThat(pt.getAttributes().get(
                    io.opentelemetry.api.common.AttributeKey.stringKey("db2.tablespace.usage")))
                    .isIn("capacity", "used", "free"));
        });

        receiver.stop();
        meterProvider.shutdown();
    }

    @Test
    void jdbcConnectionIsValid() throws Exception {
        Properties props = new Properties();
        props.setProperty("user",     db2.getUsername());
        props.setProperty("password", db2.getPassword());

        try (Connection conn = DriverManager.getConnection(db2.getJdbcUrl(), props)) {
            assertThat(conn.isValid(5)).isTrue();

            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                     "SELECT APPLS_CUR_CONS FROM TABLE(MON_GET_DATABASE(-1)) AS T")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("APPLS_CUR_CONS")).isGreaterThanOrEqualTo(1L);
            }
        }
    }

    @Test
    void tableFunctionsAreAccessible() throws Exception {
        Properties props = new Properties();
        props.setProperty("user",     db2.getUsername());
        props.setProperty("password", db2.getPassword());

        try (Connection conn = DriverManager.getConnection(db2.getJdbcUrl(), props);
             Statement st = conn.createStatement()) {

            try (ResultSet rs = st.executeQuery(
                    "SELECT COUNT(*) AS CNT FROM TABLE(MON_GET_BUFFERPOOL(NULL, -2)) AS T")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("CNT")).isGreaterThan(0L);
            }

            try (ResultSet rs = st.executeQuery(
                    "SELECT COUNT(*) AS CNT FROM TABLE(MON_GET_TABLESPACE(NULL, -2)) AS T")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("CNT")).isGreaterThan(0L);
            }
        }
    }

    @Test
    void querySamplerEmitsObfuscatedRecordForActiveQuery() throws Exception {
        InMemoryLogRecordExporter logExporter = InMemoryLogRecordExporter.create();
        SdkLoggerProvider loggerProvider = SdkLoggerProvider.builder()
            .addLogRecordProcessor(SimpleLogRecordProcessor.create(logExporter))
            .build();
        Logger otelLogger = loggerProvider.get("splunk-otel-db2");

        InMemoryMetricExporter metricExporter = InMemoryMetricExporter.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(metricExporter)
                    .setInterval(Duration.ofSeconds(2))
                    .build()
            )
            .build();

        Db2ReceiverConfig config = buildConfig();
        config.setQuerySampleInterval("1s");

        Properties props = new Properties();
        props.setProperty("user",     db2.getUsername());
        props.setProperty("password", db2.getPassword());

        // Run a long-lived cross-join on catalog tables in a background thread so it stays
        // visible in MON_GET_ACTIVITY long enough for the sampler to catch it.
        // No literals in this query so the obfuscated form is identical to the input.
        ExecutorService exec = Executors.newSingleThreadExecutor();
        Connection bgConn = DriverManager.getConnection(db2.getJdbcUrl(), props);
        Statement bgStmt = bgConn.createStatement();
        Future<?> bgQuery = exec.submit(() -> {
            try {
                bgStmt.executeQuery(
                    "SELECT A.COLNAME FROM SYSCAT.COLUMNS A, SYSCAT.COLUMNS B, SYSCAT.COLUMNS C");
            } catch (Exception ignored) {
                // cancelled when we close bgStmt below — expected
            }
        });

        Db2Receiver receiver = new Db2Receiver(config, meterProvider.get("splunk-otel-db2"), otelLogger);
        receiver.start();

        try {
            // Wait until the sampler emits at least one record whose query text
            // contains the sentinel table name from our background query.
            Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> logExporter.getFinishedLogRecordItems().stream()
                    .anyMatch(r -> {
                        String text = r.getAttributes().get(AttributeKey.stringKey("db.query.text"));
                        return text != null && text.toUpperCase().contains("SYSCAT.COLUMNS");
                    }));
        } finally {
            bgStmt.cancel();
            bgConn.close();
            bgQuery.cancel(true);
            exec.shutdownNow();
            receiver.stop();
            loggerProvider.shutdown();
            meterProvider.shutdown();
        }

        // Find the matching record and assert all attributes
        io.opentelemetry.sdk.logs.data.LogRecordData record = logExporter.getFinishedLogRecordItems()
            .stream()
            .filter(r -> {
                String text = r.getAttributes().get(AttributeKey.stringKey("db.query.text"));
                return text != null && text.toUpperCase().contains("SYSCAT.COLUMNS");
            })
            .findFirst()
            .orElseThrow();

        String queryText = record.getAttributes().get(AttributeKey.stringKey("db.query.text"));
        assertThat(queryText).doesNotContain("'");
        assertThat(queryText).doesNotMatch(".*(?<![\\p{L}\\p{N}_$#@])\\d{4,}(?![\\p{L}\\p{N}_$#@]).*");

        assertThat(record.getAttributes().get(AttributeKey.stringKey("db.system.name"))).isEqualTo("ibm.db2");
        assertThat(record.getAttributes().get(AttributeKey.stringKey("db.namespace"))).isEqualTo(config.getDatabase());
        assertThat(record.getAttributes().get(AttributeKey.longKey("db2.elapsed_time_ms"))).isNotNull();
        assertThat(record.getAttributes().get(AttributeKey.longKey("db2.member"))).isNotNull();
        assertThat(record.getSeverity()).isEqualTo(io.opentelemetry.api.logs.Severity.INFO);
    }

    private static List<String> metricNames(InMemoryMetricExporter exporter) {
        return exporter.getFinishedMetricItems().stream()
            .map(m -> m.getName())
            .collect(Collectors.toList());
    }
}
