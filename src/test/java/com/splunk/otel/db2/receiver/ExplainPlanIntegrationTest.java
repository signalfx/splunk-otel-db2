package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.receiver.queries.ExplainPlanEvent;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the explain plan feature.
 *
 * Verifies that {@code ExplainPlanQuery} fetches plan XML from the Db2 plan cache
 * via {@code MON_GET_PKG_CACHE_STMT_DETAILS} and emits correctly-structured
 * {@code db.server.query_plan} log records.
 *
 * Run with: ./gradlew integrationTest
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExplainPlanIntegrationTest {

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

    @Test
    void explainPlanEmitsQueryPlanRecordForKnownStatement() throws Exception {
        InMemoryLogRecordExporter logExporter = InMemoryLogRecordExporter.create();
        SdkLoggerProvider loggerProvider = SdkLoggerProvider.builder()
            .addLogRecordProcessor(SimpleLogRecordProcessor.create(logExporter))
            .build();

        InMemoryMetricExporter metricExporter = InMemoryMetricExporter.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(metricExporter)
                    .setInterval(Duration.ofSeconds(2))
                    .build())
            .build();

        Db2ReceiverConfig config = buildConfig();
        config.setExplainPlanEnabled(false); // we start the scheduler manually below

        Properties props = new Properties();
        props.setProperty("user",     db2.getUsername());
        props.setProperty("password", db2.getPassword());

        // Run a recognisable query to warm the plan cache before the first explain-plan collection.
        // SPLUNK_OTEL_EXPLAIN_PROBE is a unique marker in the alias; no literals, not filtered.
        Connection workConn = DriverManager.getConnection(db2.getJdbcUrl(), props);
        try (Statement st = workConn.createStatement()) {
            for (int i = 0; i < 3; i++) {
                try (var rs = st.executeQuery(
                        "SELECT COUNT(*) AS SPLUNK_OTEL_EXPLAIN_PROBE " +
                        "FROM SYSCAT.COLUMNS A, SYSCAT.COLUMNS B " +
                        "WHERE A.TABSCHEMA = B.TABSCHEMA")) {
                    while (rs.next()) { /* drain */ }
                }
            }
        } finally {
            workConn.close();
        }

        Db2Receiver receiver = new Db2Receiver(config,
            meterProvider.get("splunk-otel-db2"),
            loggerProvider.get("splunk-otel-db2"));
        receiver.start();
        // Inject a short interval so the test doesn't wait 10 minutes.
        receiver.startExplainPlanScheduler(4L);

        try {
            Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .until(() -> logExporter.getFinishedLogRecordItems().stream()
                    .anyMatch(r -> {
                        String eventName = r.getEventName();
                        String text = r.getAttributes().get(ExplainPlanEvent.DB_QUERY_TEXT);
                        return "db.server.query_plan".equals(eventName)
                            && text != null
                            && text.toUpperCase().contains("SPLUNK_OTEL_EXPLAIN_PROBE");
                    }));

            io.opentelemetry.sdk.logs.data.LogRecordData record =
                logExporter.getFinishedLogRecordItems().stream()
                    .filter(r -> {
                        String text = r.getAttributes().get(ExplainPlanEvent.DB_QUERY_TEXT);
                        return "db.server.query_plan".equals(r.getEventName())
                            && text != null
                            && text.toUpperCase().contains("SPLUNK_OTEL_EXPLAIN_PROBE");
                    })
                    .findFirst()
                    .orElseThrow();

            // Verify standard attributes
            assertThat(record.getAttributes().get(ExplainPlanEvent.DB_SYSTEM)).isEqualTo("ibm.db2");
            assertThat(record.getAttributes().get(ExplainPlanEvent.DB_NAMESPACE))
                .isEqualTo(config.getDatabase());
            assertThat(record.getAttributes().get(ExplainPlanEvent.DB2_MEMBER)).isNotNull();
            assertThat(record.getSeverity())
                .isEqualTo(io.opentelemetry.api.logs.Severity.INFO);

            // Plan XML must be present and non-empty
            String planXml = record.getAttributes().get(ExplainPlanEvent.DB2_QUERY_PLAN);
            assertThat(planXml).isNotNull().isNotBlank();

            // Plan hash must be a 64-char hex string (SHA-256)
            String planHash = record.getAttributes().get(ExplainPlanEvent.DB2_PLAN_HASH);
            assertThat(planHash).isNotNull().hasSize(64).matches("[0-9a-f]+");

            // SQL text must be obfuscated — no literal values (probe query has none, but verify)
            String queryText = record.getAttributes().get(ExplainPlanEvent.DB_QUERY_TEXT);
            assertThat(queryText).doesNotContain("'");

        } finally {
            receiver.stop();
            loggerProvider.shutdown();
            meterProvider.shutdown();
        }
    }

    @Test
    void explainPlanCachePreventsDuplicateEmitsWithinTtl() throws Exception {
        InMemoryLogRecordExporter logExporter = InMemoryLogRecordExporter.create();
        SdkLoggerProvider loggerProvider = SdkLoggerProvider.builder()
            .addLogRecordProcessor(SimpleLogRecordProcessor.create(logExporter))
            .build();

        InMemoryMetricExporter metricExporter = InMemoryMetricExporter.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(metricExporter)
                    .setInterval(Duration.ofSeconds(2))
                    .build())
            .build();

        Db2ReceiverConfig config = buildConfig();
        config.setExplainPlanEnabled(false);

        Properties props = new Properties();
        props.setProperty("user",     db2.getUsername());
        props.setProperty("password", db2.getPassword());

        // Warm the plan cache with a recognisable query
        Connection workConn = DriverManager.getConnection(db2.getJdbcUrl(), props);
        try (Statement st = workConn.createStatement()) {
            for (int i = 0; i < 2; i++) {
                try (var rs = st.executeQuery(
                        "SELECT COUNT(*) AS SPLUNK_OTEL_CACHE_PROBE " +
                        "FROM SYSCAT.COLUMNS")) {
                    while (rs.next()) { /* drain */ }
                }
            }
        } finally {
            workConn.close();
        }

        Db2Receiver receiver = new Db2Receiver(config,
            meterProvider.get("splunk-otel-db2"),
            loggerProvider.get("splunk-otel-db2"));
        receiver.start();
        // Short collect interval, but plan cache TTL is the default 10 minutes —
        // so the same plan should appear at most once across multiple cycles.
        receiver.startExplainPlanScheduler(3L);

        try {
            // Wait for at least one plan record to be emitted
            Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> logExporter.getFinishedLogRecordItems().stream()
                    .anyMatch(r -> "db.server.query_plan".equals(r.getEventName())
                        && r.getAttributes().get(ExplainPlanEvent.DB_QUERY_TEXT) != null
                        && r.getAttributes().get(ExplainPlanEvent.DB_QUERY_TEXT)
                               .toUpperCase().contains("SPLUNK_OTEL_CACHE_PROBE")));

            // Wait two more collection cycles
            Thread.sleep(8000);

            // Count how many times the probe query plan was emitted — must be exactly 1
            // because the cache prevents re-emission within the TTL.
            long probeCount = logExporter.getFinishedLogRecordItems().stream()
                .filter(r -> "db.server.query_plan".equals(r.getEventName())
                    && r.getAttributes().get(ExplainPlanEvent.DB_QUERY_TEXT) != null
                    && r.getAttributes().get(ExplainPlanEvent.DB_QUERY_TEXT)
                           .toUpperCase().contains("SPLUNK_OTEL_CACHE_PROBE"))
                .count();

            assertThat(probeCount).isEqualTo(1);
        } finally {
            receiver.stop();
            loggerProvider.shutdown();
            meterProvider.shutdown();
        }
    }
}
