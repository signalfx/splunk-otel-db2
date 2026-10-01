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
 * Requires DFT_MON_STMT ON — set via UPDATE DBM CFG, then the instance is
 * restarted (db2stop force / db2start) so the new default takes effect for
 * all subsequent connections. Runs as a separate CI job tagged "snapshot".
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

    /** Distinct values of a string attribute across every point of a metric. */
    private static java.util.List<String> stringAttributeValues(
            InMemoryMetricExporter exporter, String metric, String attribute) {
        return exporter.getFinishedMetricItems().stream()
            .filter(m -> m.getName().equals(metric))
            .flatMap(m -> m.getData().getPoints().stream())
            .map(pt -> pt.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey(attribute)))
            .filter(java.util.Objects::nonNull)
            .distinct()
            .collect(java.util.stream.Collectors.toList());
    }

    private Db2ReceiverConfig buildSnapshotConfig() {
        Db2ReceiverConfig config = new Db2ReceiverConfig();
        config.setHostname(db2.getHost());
        config.setHostport(db2.getMappedPort(50000));
        config.setUsername(db2.getUsername());
        config.setPassword(db2.getPassword());
        config.setDatabase(db2.getDatabaseName().toUpperCase());
        config.setCollectionInterval("2s");
        config.getEvents().getDbServerQuerySample().setEnabled(true);
        config.getQuerySampleCollection().setCollectionInterval("1s");
        config.setUseSnapshotMonitor(true);
        config.getTls().setInsecure(true);
        return config;
    }

    private void recycleWithMonitorSwitches() throws Exception {
        // DFT_MON_STMT takes effect only after instance recycle.
        // DFT_MON_LOCK is not needed: snapshot path does not query MON_LOCKWAITS.
        String script =
            "db2 UPDATE DBM CFG USING DFT_MON_STMT ON && " +
            "db2stop force && " +
            "db2start";
        org.testcontainers.containers.Container.ExecResult result = db2.execInContainer(
            "su", "-", "db2inst1", "-c", script
        );
        if (result.getExitCode() != 0) {
            throw new IllegalStateException(
                "Failed to recycle DB2 instance: exit=" + result.getExitCode() +
                " stdout=" + result.getStdout() +
                " stderr=" + result.getStderr());
        }
        Thread.sleep(5000);
    }

    @Test
    void snapshotPathEmitsObfuscatedRecordForActiveQuery() throws Exception {
        String script =
            "db2 UPDATE DBM CFG USING DFT_MON_STMT ON && " +
            "db2stop force && " +
            "db2start";
        org.testcontainers.containers.Container.ExecResult result = db2.execInContainer(
            "su", "-", "db2inst1", "-c", script
        );
        if (result.getExitCode() != 0) {
            throw new IllegalStateException(
                "Failed to recycle DB2 instance with DFT_MON_STMT ON: exit=" + result.getExitCode() +
                " stdout=" + result.getStdout() +
                " stderr=" + result.getStderr());
        }
        Thread.sleep(5000);

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

        StringBuilder snapDiag = new StringBuilder();
        try (Connection diagConn = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps());
             java.sql.Statement diagSt = diagConn.createStatement();
             java.sql.ResultSet diagRs = diagSt.executeQuery(
                 "SELECT SA.AGENT_ID, SA.APPL_STATUS, SS.STMT_TEXT, SS.STMT_STOP " +
                 "FROM SYSIBM.SYSDUMMY1, " +
                 "     TABLE(SNAP_GET_APPL_INFO(CAST(NULL AS VARCHAR(255)), -1)) AS SA, " +
                 "     TABLE(SNAP_GET_STMT(CAST(NULL AS VARCHAR(255)), -1)) AS SS " +
                 "WHERE SA.AGENT_ID = SS.AGENT_ID")) {
            int diagCount = 0;
            while (diagRs.next()) {
                diagCount++;
                snapDiag.append(String.format("  agent=%s status=%s stop=%s text=%s\n",
                    diagRs.getString("AGENT_ID"), diagRs.getString("APPL_STATUS"),
                    diagRs.getString("STMT_STOP"), diagRs.getString("STMT_TEXT")));
            }
            snapDiag.insert(0, "SNAP_GET_STMT rows=" + diagCount + "\n");
        } catch (Exception diagEx) {
            snapDiag.append("SNAP_GET_STMT query failed: ").append(diagEx.getMessage());
        }

        Db2Receiver receiver = new Db2Receiver(config, meterProvider.get("splunk-otel-db2"), otelLogger);
        receiver.start();

        io.opentelemetry.sdk.logs.data.LogRecordData record;
        try {
            try {
                Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .until(() -> logExporter.getFinishedLogRecordItems().stream()
                        .anyMatch(r -> {
                            String text = r.getAttributes().get(QuerySampleEvent.DB_QUERY_TEXT);
                            return text != null && text.toUpperCase().contains("SPLUNK_SNAP_LOCK_TEST");
                        }));
            } catch (org.awaitility.core.ConditionTimeoutException cte) {
                throw new AssertionError("No query sample found for SPLUNK_SNAP_LOCK_TEST after 60s. " + snapDiag, cte);
            }

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
        // Snapshot path: blocking detection not available, so no blocker handle
        assertThat(record.getAttributes().get(QuerySampleEvent.DB2_BLOCKER_APP_HANDLE)).isNull();
    }

    /**
     * Proves the traceparent round-trips on the snapshot path, where the accounting string is
     * exposed as SNAP_GET_APPL_INFO.TPMON_ACC_STR rather than CLIENT_ACCTNG.
     *
     * <p>This path is where a wrong column name previously slipped through: the unit tests mock
     * ResultSet, so CLIENT_ACCTNG_ID looked fine until a live Db2 rejected it with SQL0206N and
     * took the whole snapshot query down with it. Only an assertion against a real server closes
     * that gap.
     */
    @Test
    void snapshotPathPicksUpTraceparentFromAccountingString() throws Exception {
        recycleWithMonitorSwitches();

        String traceId = "0af7651916cd43dd8448eb211c80319c";
        String spanId  = "b7ad6b7169203331";
        String traceparent = "traceparent=00-" + traceId + "-" + spanId + "-01";

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

        Connection ddlConn = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps());
        try (Statement st = ddlConn.createStatement()) {
            try { st.execute("DROP TABLE SPLUNK_SNAP_TRACE_TEST"); } catch (Exception ignored) {}
            st.execute("CREATE TABLE SPLUNK_SNAP_TRACE_TEST (ID INT NOT NULL PRIMARY KEY, VAL INT)");
        }
        ddlConn.close();

        Connection ownerConn = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps());
        ownerConn.setAutoCommit(false);
        try (Statement st = ownerConn.createStatement()) {
            st.execute("INSERT INTO SPLUNK_SNAP_TRACE_TEST VALUES (1, 42)");
        }

        ExecutorService exec = Executors.newSingleThreadExecutor();
        Connection bgConn = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps());
        bgConn.setAutoCommit(false);

        try (java.sql.CallableStatement cs =
                 bgConn.prepareCall("CALL SYSPROC.WLM_SET_CLIENT_INFO(?,?,?,?,?)")) {
            cs.setNull(1, java.sql.Types.VARCHAR);
            cs.setNull(2, java.sql.Types.VARCHAR);
            cs.setNull(3, java.sql.Types.VARCHAR);
            cs.setString(4, traceparent);
            cs.setNull(5, java.sql.Types.VARCHAR);
            cs.execute();
        }

        Statement bgStmt = bgConn.createStatement();
        Future<?> bgQuery = exec.submit(() -> {
            try {
                bgStmt.executeQuery("SELECT ID FROM SPLUNK_SNAP_TRACE_TEST WITH RR");
            } catch (Exception ignored) {
                // unblocked on rollback/cancel
            }
        });

        Thread.sleep(3000);

        // If the assertion fails, knowing what the server actually exposed is the whole diagnosis.
        String acctngSeen = "<not read>";
        try (Connection diagConn = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps());
             Statement diagSt = diagConn.createStatement();
             java.sql.ResultSet diagRs = diagSt.executeQuery(
                 "SELECT SA.AGENT_ID, SA.TPMON_ACC_STR " +
                 "FROM TABLE(SNAP_GET_APPL_INFO(CAST(NULL AS VARCHAR(255)), -1)) AS SA " +
                 "WHERE SA.TPMON_ACC_STR IS NOT NULL AND SA.TPMON_ACC_STR <> ''")) {
            StringBuilder sb = new StringBuilder();
            while (diagRs.next()) {
                sb.append(String.format("agent=%s acctng=[%s] ",
                    diagRs.getString("AGENT_ID"), diagRs.getString("TPMON_ACC_STR")));
            }
            acctngSeen = sb.length() == 0 ? "<no non-empty TPMON_ACC_STR rows>" : sb.toString();
        } catch (Exception e) {
            acctngSeen = "TPMON_ACC_STR probe failed: " + e.getMessage();
        }

        Db2Receiver receiver = new Db2Receiver(config, meterProvider.get("splunk-otel-db2"), otelLogger);
        receiver.start();

        io.opentelemetry.sdk.logs.data.LogRecordData record;
        try {
            try {
                Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .until(() -> logExporter.getFinishedLogRecordItems().stream()
                        .anyMatch(r -> {
                            String text = r.getAttributes().get(QuerySampleEvent.DB_QUERY_TEXT);
                            return text != null && text.toUpperCase().contains("SPLUNK_SNAP_TRACE_TEST");
                        }));
            } catch (org.awaitility.core.ConditionTimeoutException cte) {
                throw new AssertionError(
                    "No query sample for SPLUNK_SNAP_TRACE_TEST after 60s. TPMON_ACC_STR: "
                        + acctngSeen, cte);
            }

            record = logExporter.getFinishedLogRecordItems().stream()
                .filter(r -> {
                    String text = r.getAttributes().get(QuerySampleEvent.DB_QUERY_TEXT);
                    return text != null && text.toUpperCase().contains("SPLUNK_SNAP_TRACE_TEST");
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
                st.execute("DROP TABLE SPLUNK_SNAP_TRACE_TEST");
            } finally {
                cleanupConn.close();
            }
            receiver.stop();
            loggerProvider.shutdown();
            meterProvider.shutdown();
        }

        assertThat(record.getSpanContext().isValid())
            .withFailMessage("record not linked to a trace; TPMON_ACC_STR was: %s", acctngSeen)
            .isTrue();
        assertThat(record.getSpanContext().getTraceId()).isEqualTo(traceId);
        assertThat(record.getSpanContext().getSpanId()).isEqualTo(spanId);
        assertThat(record.getSpanContext().isSampled()).isTrue();
        assertThat(record.getSpanContext().isRemote()).isTrue();
    }

    @Test
    void snapshotPathDoesNotEmitBlockingAttributes() throws Exception {
        // SYSIBMADM.LOCKWAITS requires DFT_MON_LOCK ON but on snapshot path we don't
        // join MON_LOCKWAITS — confirm no blocking attributes appear even with DFT_MON_LOCK ON.
        recycleWithMonitorSwitches();

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

        Connection ddlConn = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps());
        try (Statement st = ddlConn.createStatement()) {
            try { st.execute("DROP TABLE SPLUNK_SNAP_BLOCK_TEST"); } catch (Exception ignored) {}
            st.execute("CREATE TABLE SPLUNK_SNAP_BLOCK_TEST (ID INT NOT NULL PRIMARY KEY, VAL INT)");
            st.execute("INSERT INTO SPLUNK_SNAP_BLOCK_TEST VALUES (1, 1)");
        }
        ddlConn.setAutoCommit(false);
        ddlConn.commit();
        ddlConn.close();

        Connection blockerConn = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps());
        blockerConn.setAutoCommit(false);
        try (Statement st = blockerConn.createStatement()) {
            st.execute("UPDATE SPLUNK_SNAP_BLOCK_TEST SET VAL = 2 WHERE ID = 1");
        }

        ExecutorService exec = Executors.newSingleThreadExecutor();
        Connection blockedConn = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps());
        blockedConn.setAutoCommit(false);
        Statement blockedStmt = blockedConn.createStatement();
        exec.submit(() -> {
            try { blockedStmt.executeQuery("SELECT VAL FROM SPLUNK_SNAP_BLOCK_TEST WITH RR"); }
            catch (Exception ignored) {}
        });

        Thread.sleep(2000);

        Db2ReceiverConfig config = buildSnapshotConfig();
        Db2Receiver receiver = new Db2Receiver(config, meterProvider.get("splunk-otel-db2"), otelLogger);
        receiver.start();

        io.opentelemetry.sdk.logs.data.LogRecordData snapRecord;
        try {
            try {
                Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .until(() -> logExporter.getFinishedLogRecordItems().stream()
                        .anyMatch(r -> {
                            String text = r.getAttributes().get(QuerySampleEvent.DB_QUERY_TEXT);
                            return text != null && text.toUpperCase().contains("SPLUNK_SNAP_BLOCK_TEST");
                        }));
            } catch (org.awaitility.core.ConditionTimeoutException cte) {
                throw new AssertionError("No query sample found for SPLUNK_SNAP_BLOCK_TEST after 60s", cte);
            }

            snapRecord = logExporter.getFinishedLogRecordItems().stream()
                .filter(r -> {
                    String text = r.getAttributes().get(QuerySampleEvent.DB_QUERY_TEXT);
                    return text != null && text.toUpperCase().contains("SPLUNK_SNAP_BLOCK_TEST");
                })
                .findFirst()
                .orElseThrow();

            // Snapshot path: blocking detection not available — always INFO, no blocking attributes.
            assertThat(snapRecord.getSeverity()).isEqualTo(io.opentelemetry.api.logs.Severity.INFO);
            assertThat(snapRecord.getAttributes().get(QuerySampleEvent.DB2_BLOCKER_APP_HANDLE)).isNull();
            assertThat(snapRecord.getAttributes().get(QuerySampleEvent.DB2_WAIT_DURATION)).isNull();
        } finally {
            blockerConn.rollback();
            blockerConn.close();
            blockedStmt.cancel();
            blockedConn.rollback();
            blockedConn.close();
            exec.shutdownNow();
            Connection cleanupConn = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps());
            try (Statement st = cleanupConn.createStatement()) {
                st.execute("DROP TABLE SPLUNK_SNAP_BLOCK_TEST");
            } finally { cleanupConn.close(); }
            receiver.stop();
            loggerProvider.shutdown();
            meterProvider.shutdown();
        }
    }

    /**
     * Gates the SNAP_GET_DB supplement in SqlActivityQuery. The snapshot-only columns have no
     * MON_GET_* equivalent, so without this the SQL would be exercised by nothing: a failure is
     * only logged as a warning. These are asynchronous instruments, so a failed query records
     * no measurement and the dimensions below are simply absent.
     */
    @Test
    void snapshotOnlyMetricDimensionsAreEmitted() {
        InMemoryMetricExporter metricExporter = InMemoryMetricExporter.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(metricExporter)
                    .setInterval(Duration.ofSeconds(2))
                    .build()
            )
            .build();

        Db2Receiver receiver = new Db2Receiver(
            buildSnapshotConfig(), meterProvider.get("splunk-otel-db2"), (Logger) null);
        receiver.start();

        try {
            // db2.deadlock.rollback.count comes only from SNAP_GET_DB.
            Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> metricExporter.getFinishedMetricItems().stream()
                    .anyMatch(m -> m.getName().equals("db2.deadlock.rollback.count")));

            assertThat(metricExporter.getFinishedMetricItems())
                .filteredOn(m -> m.getName().equals("db2.deadlock.rollback.count"))
                .allSatisfy(metric -> assertThat(metric.getLongSumData().getPoints())
                    .allSatisfy(pt -> assertThat(pt.getAttributes().get(
                        io.opentelemetry.api.common.AttributeKey.longKey("db2.member"))).isNotNull()));

            // The snapshot path must not reintroduce quantities already available natively:
            // COMMIT_SQL_STMTS, ROWS_SELECTED and INT_AUTO_REBINDS duplicate MON_GET_DATABASE
            // columns, so emitting them would double-count within an additive metric.
            assertThat(stringAttributeValues(metricExporter, "db2.sql.statement.count", "db2.statement_type"))
                .doesNotContain("commit", "rollback");
            assertThat(stringAttributeValues(metricExporter, "db2.row.count", "db2.row_operation"))
                .doesNotContain("selected", "returned", "modified");
            assertThat(stringAttributeValues(metricExporter, "db2.sql.rebind.count", "db2.rebind_type"))
                .isEmpty();
        } finally {
            receiver.stop();
            meterProvider.shutdown();
        }
    }
}
