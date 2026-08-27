package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.receiver.queries.QuerySampleEvent;
import com.splunk.otel.db2.receiver.queries.TopQueryEvent;
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
        config.setQuerySampleEnabled(true);
        config.setQuerySampleInterval("1s");

        Properties props = new Properties();
        props.setProperty("user",     db2.getUsername());
        props.setProperty("password", db2.getPassword());

        // Lock-based approach: owner connection holds an exclusive row lock; background
        // connection tries to read the same row WITH RR and blocks in LOCKWAIT DML.
        // This is 100% deterministic — DB2 cannot optimise away a real row lock,
        // regardless of container speed or catalog size.
        //
        // DDL (CREATE/DROP TABLE) uses a separate autoCommit=true connection so that
        // ownerConn.rollback() only releases the INSERT lock — not the table itself.
        // Sentinel query text contains "SPLUNK_OTEL_LOCK_TEST" — no literals, so
        // obfuscated text == input text.
        Connection ddlConn = DriverManager.getConnection(db2.getJdbcUrl(), props);
        try (Statement st = ddlConn.createStatement()) {
            // Guard against a leftover table from a previously aborted test run
            try { st.execute("DROP TABLE SPLUNK_OTEL_LOCK_TEST"); } catch (Exception ignored) {}
            st.execute("CREATE TABLE SPLUNK_OTEL_LOCK_TEST (ID INT NOT NULL PRIMARY KEY, VAL INT)");
        }
        ddlConn.close();

        // ownerConn: INSERT and hold uncommitted — this is the X-lock holder
        Connection ownerConn = DriverManager.getConnection(db2.getJdbcUrl(), props);
        ownerConn.setAutoCommit(false);
        try (Statement st = ownerConn.createStatement()) {
            st.execute("INSERT INTO SPLUNK_OTEL_LOCK_TEST VALUES (1, 42)");
        }

        ExecutorService exec = Executors.newSingleThreadExecutor();
        Connection bgConn = DriverManager.getConnection(db2.getJdbcUrl(), props);
        bgConn.setAutoCommit(false);
        Statement bgStmt = bgConn.createStatement();
        Future<?> bgQuery = exec.submit(() -> {
            try {
                // WITH RR requests a share lock — blocks until ownerConn commits/rolls back
                bgStmt.executeQuery(
                    "SELECT ID FROM SPLUNK_OTEL_LOCK_TEST WITH RR");
            } catch (Exception ignored) {
                // unblocked when ownerConn rolls back or bgStmt.cancel() is called
            }
        });

        // Give the background connection time to hit the lock before the first poll
        Thread.sleep(3000);

        Db2Receiver receiver = new Db2Receiver(config, meterProvider.get("splunk-otel-db2"), otelLogger);
        receiver.start();

        // Capture record BEFORE shutdown — loggerProvider.shutdown() clears the exporter.
        io.opentelemetry.sdk.logs.data.LogRecordData record;
        try {
            Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> logExporter.getFinishedLogRecordItems().stream()
                    .anyMatch(r -> {
                        String text = r.getAttributes().get(QuerySampleEvent.DB_QUERY_TEXT);
                        return text != null && text.toUpperCase().contains("SPLUNK_OTEL_LOCK_TEST");
                    }));

            record = logExporter.getFinishedLogRecordItems().stream()
                .filter(r -> {
                    String text = r.getAttributes().get(QuerySampleEvent.DB_QUERY_TEXT);
                    return text != null && text.toUpperCase().contains("SPLUNK_OTEL_LOCK_TEST");
                })
                .findFirst()
                .orElseThrow();
        } finally {
            ownerConn.rollback();  // releases the X-lock, unblocks bgConn; table still exists
            ownerConn.close();
            bgStmt.cancel();
            bgConn.rollback();
            bgConn.close();
            bgQuery.cancel(true);
            exec.shutdownNow();
            // Drop the table via a fresh autoCommit connection — DDL must not be inside a txn
            Connection cleanupConn = DriverManager.getConnection(db2.getJdbcUrl(), props);
            try (Statement st = cleanupConn.createStatement()) {
                st.execute("DROP TABLE SPLUNK_OTEL_LOCK_TEST");
            } finally {
                cleanupConn.close();
            }
            receiver.stop();
            loggerProvider.shutdown();
            meterProvider.shutdown();
        }

        String queryText = record.getAttributes().get(QuerySampleEvent.DB_QUERY_TEXT);
        assertThat(queryText).doesNotContain("'");
        assertThat(queryText).doesNotMatch(".*(?<![\\p{L}\\p{N}_$#@])\\d{4,}(?![\\p{L}\\p{N}_$#@]).*");

        assertThat(record.getAttributes().get(QuerySampleEvent.DB_SYSTEM)).isEqualTo("ibm.db2");
        assertThat(record.getAttributes().get(QuerySampleEvent.DB_NAMESPACE)).isEqualTo(config.getDatabase());
        assertThat(record.getAttributes().get(QuerySampleEvent.DB2_DURATION)).isNotNull();
        assertThat(record.getAttributes().get(QuerySampleEvent.DB2_MEMBER)).isNotNull();
        assertThat(record.getSeverity()).isEqualTo(io.opentelemetry.api.logs.Severity.INFO);
    }

    @Test
    void topQueryEmitsObfuscatedRecordAfterTwoCollectionCycles() throws Exception {
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
        config.setTopQueryEnabled(true);
        config.setTopQueryInterval("3s");   // two cycles: seed at 0s, emit at 3s

        Properties props = new Properties();
        props.setProperty("user",     db2.getUsername());
        props.setProperty("password", db2.getPassword());

        // Run a recognisable workload query so MON_GET_PKG_CACHE_STMT has something to return.
        // SPLUNK_OTEL_TOP_QUERY_TEST has no literals so obfuscated text == raw text.
        Connection workConn = DriverManager.getConnection(db2.getJdbcUrl(), props);
        try (Statement st = workConn.createStatement()) {
            for (int i = 0; i < 5; i++) {
                try (ResultSet r = st.executeQuery(
                        "SELECT COUNT(*) FROM TABLE(MON_GET_DATABASE(-2)) AS SPLUNK_OTEL_TOP_QUERY_TEST")) {
                    while (r.next()) { /* drain */ }
                }
            }
        } finally {
            workConn.close();
        }

        Db2Receiver receiver = new Db2Receiver(config, meterProvider.get("splunk-otel-db2"), otelLogger);
        receiver.start();

        try {
            // Wait for at least one top_query log record to appear (requires two scheduler cycles).
            Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .until(() -> logExporter.getFinishedLogRecordItems().stream()
                    .anyMatch(r -> "db.server.top_query".equals(r.getEventName())));

            io.opentelemetry.sdk.logs.data.LogRecordData record =
                logExporter.getFinishedLogRecordItems().stream()
                    .filter(r -> "db.server.top_query".equals(r.getEventName()))
                    .findFirst()
                    .orElseThrow();

            // Core OTel attributes
            assertThat(record.getSeverity())
                .isEqualTo(io.opentelemetry.api.logs.Severity.INFO);
            assertThat(record.getAttributes().get(TopQueryEvent.DB_SYSTEM))
                .isEqualTo("ibm.db2");
            assertThat(record.getAttributes().get(TopQueryEvent.DB_NAMESPACE))
                .isEqualTo(config.getDatabase());

            // Query text must be present and obfuscated (no raw literals)
            String queryText = record.getAttributes().get(TopQueryEvent.DB_QUERY_TEXT);
            assertThat(queryText).isNotBlank();
            assertThat(queryText).doesNotContain("'");

            // Numeric attributes must be present and non-negative
            assertThat(record.getAttributes().get(TopQueryEvent.DB2_MEMBER)).isNotNull();
            assertThat(record.getAttributes().get(TopQueryEvent.DB2_EXECUTIONS)).isNotNull();
            assertThat(record.getAttributes().get(TopQueryEvent.DB2_TOTAL_EXEC_TIME))
                .isNotNull()
                .matches(v -> v >= 0.0);
        } finally {
            receiver.stop();
            loggerProvider.shutdown();
            meterProvider.shutdown();
        }
    }

    private static List<String> metricNames(InMemoryMetricExporter exporter) {
        return exporter.getFinishedMetricItems().stream()
            .map(m -> m.getName())
            .collect(Collectors.toList());
    }
}
