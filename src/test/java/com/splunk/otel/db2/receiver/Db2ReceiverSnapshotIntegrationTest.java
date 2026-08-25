package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.receiver.queries.QuerySampleEvent;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the snapshot monitor path (use_snapshot_monitor=true).
 *
 * Requires DFT_MON_STMT ON — enabled via JDBC before the sampler starts.
 * Runs as a separate CI job tagged "snapshot" so it does not block the main
 * integration-test job.
 *
 * Run with: ./gradlew integrationTestSnapshot
 */
@Tag("snapshot")
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Db2ReceiverSnapshotIntegrationTest {

    @Container
    static final Db2Container db2 = new Db2Container("icr.io/db2_community/db2:11.5.9.0")
        .acceptLicense()
        .withUsername("db2inst1")
        .withPassword("password")
        .withDatabaseName("testdb")
        .withStartupTimeout(Duration.ofMinutes(10));

    private Properties jdbcProps() {
        Properties p = new Properties();
        p.setProperty("user",     db2.getUsername());
        p.setProperty("password", db2.getPassword());
        return p;
    }

    private Db2ReceiverConfig buildSnapshotConfig() {
        Db2ReceiverConfig config = new Db2ReceiverConfig();
        config.setHostname(db2.getHost());
        config.setHostport(db2.getMappedPort(50000));
        config.setUsername(db2.getUsername());
        config.setPassword(db2.getPassword());
        config.setDatabase(db2.getDatabaseName().toUpperCase());
        config.setCollectionInterval("2s");
        config.setQuerySampleEnabled(true);
        config.setQuerySampleInterval("1s");
        config.setUseSnapshotMonitor(true);
        config.getTls().setInsecure(true);
        return config;
    }

    @Test
    void snapshotPathEmitsObfuscatedRecordForActiveQuery() throws Exception {
        // Enable statement monitoring via the container's Db2 CLP as db2inst1.
        // Must run as db2inst1 (SYSADM) — root lacks authority for UPDATE DBM CFG.
        // Pass a multi-line script via stdin to `su - db2inst1` so all db2 commands
        // share the same session: UPDATE DBM CFG persists the switch for new
        // connections, CONNECT TO opens a DB session, UPDATE MONITOR SWITCHES
        // activates statement capture for currently connected applications.
        String dbName = db2.getDatabaseName().toUpperCase();
        // su - sources db2inst1's .bash_profile which sets up the db2 environment
        String script =
            "db2 UPDATE DBM CFG USING DFT_MON_STMT ON && " +
            "db2 CONNECT TO " + dbName + " && " +
            "db2 UPDATE MONITOR SWITCHES USING STATEMENT ON && " +
            "db2 CONNECT RESET";
        org.testcontainers.containers.Container.ExecResult result = db2.execInContainer(
            "su", "-", "db2inst1", "-c", script
        );
        if (result.getExitCode() != 0) {
            throw new IllegalStateException(
                "Failed to enable DFT_MON_STMT: exit=" + result.getExitCode() +
                " stdout=" + result.getStdout() +
                " stderr=" + result.getStderr());
        }

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

        Db2ReceiverConfig config = buildSnapshotConfig();

        // Lock-based background query — same pattern as the default-path test.
        // DDL via its own autoCommit connection, INSERT held in ownerConn.
        Connection ddlConn = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps());
        try (Statement st = ddlConn.createStatement()) {
            try { st.execute("DROP TABLE SPLUNK_SNAP_LOCK_TEST"); } catch (Exception ignored) {}
            st.execute("CREATE TABLE SPLUNK_SNAP_LOCK_TEST (ID INT NOT NULL PRIMARY KEY, VAL INT)");
        }
        ddlConn.close();

        Connection ownerConn = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps());
        ownerConn.setAutoCommit(false);
        try (Statement st = ownerConn.createStatement()) {
            st.execute("INSERT INTO SPLUNK_SNAP_LOCK_TEST VALUES (1, 42)");
        }

        ExecutorService exec = Executors.newSingleThreadExecutor();
        Connection bgConn = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps());
        bgConn.setAutoCommit(false);
        Statement bgStmt = bgConn.createStatement();
        Future<?> bgQuery = exec.submit(() -> {
            try {
                bgStmt.executeQuery("SELECT ID FROM SPLUNK_SNAP_LOCK_TEST WITH RR");
            } catch (Exception ignored) {}
        });

        Thread.sleep(3000);

        Db2Receiver receiver = new Db2Receiver(config, meterProvider.get("splunk-otel-db2"), otelLogger);
        receiver.start();

        io.opentelemetry.sdk.logs.data.LogRecordData record;
        try {
            Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> logExporter.getFinishedLogRecordItems().stream()
                    .anyMatch(r -> {
                        String text = r.getAttributes().get(QuerySampleEvent.DB_QUERY_TEXT);
                        return text != null && text.toUpperCase().contains("SPLUNK_SNAP_LOCK_TEST");
                    }));

            record = logExporter.getFinishedLogRecordItems().stream()
                .filter(r -> {
                    String text = r.getAttributes().get(QuerySampleEvent.DB_QUERY_TEXT);
                    return text != null && text.toUpperCase().contains("SPLUNK_SNAP_LOCK_TEST");
                })
                .findFirst()
                .orElseThrow();
        } finally {
            ownerConn.rollback();
            ownerConn.close();
            bgStmt.cancel();
            bgConn.rollback();
            bgConn.close();
            bgQuery.cancel(true);
            exec.shutdownNow();
            Connection cleanupConn = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps());
            try (Statement st = cleanupConn.createStatement()) {
                st.execute("DROP TABLE SPLUNK_SNAP_LOCK_TEST");
            } finally {
                cleanupConn.close();
            }
            receiver.stop();
            loggerProvider.shutdown();
            meterProvider.shutdown();
        }

        // Snapshot path: no CPU time attribute (sentinel -1 suppressed)
        assertThat(record.getAttributes().get(QuerySampleEvent.DB2_CPU_TIME)).isNull();

        String queryText = record.getAttributes().get(QuerySampleEvent.DB_QUERY_TEXT);
        assertThat(queryText).doesNotContain("'");
        assertThat(record.getAttributes().get(QuerySampleEvent.DB_SYSTEM)).isEqualTo("ibm.db2");
        assertThat(record.getAttributes().get(QuerySampleEvent.DB_NAMESPACE)).isEqualTo(config.getDatabase());
        assertThat(record.getAttributes().get(QuerySampleEvent.DB2_DURATION)).isNotNull();
        assertThat(record.getAttributes().get(QuerySampleEvent.DB2_MEMBER)).isNotNull();
        assertThat(record.getSeverity()).isEqualTo(io.opentelemetry.api.logs.Severity.INFO);
    }
}
