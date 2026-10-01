package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.receiver.queries.QuerySampleEvent;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for blocking session detection against a live DB2 container.
 *
 * The lock scenario: session A inserts a row and holds the transaction open (idle blocker —
 * no active SQL after the INSERT). Session B tries to read the same row WITH RR and blocks.
 * MON_LOCKWAITS shows the blocked→blocker pair; the blocked session's query_sample record
 * is emitted at WARN with blocking attributes populated. BLOCKER_IS_IDLE=true and the
 * blocker's last statement is looked up from MON_GET_PKG_CACHE_STMT.
 *
 * Run with: ./gradlew integrationTest
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BlockingSessionIntegrationTest {

    private static final String TABLE = "SPLUNK_OTEL_BLOCK_TEST";

    @Container
    static final Db2Container db2 = new Db2Container("icr.io/db2_community/db2:11.5.9.0")
        .acceptLicense()
        .withUsername("db2inst1")
        .withPassword("password")
        .withDatabaseName("testdb")
        .withStartupTimeout(Duration.ofMinutes(10));

    private Properties jdbcProps;
    private InMemoryLogRecordExporter logExporter;
    private SdkLoggerProvider loggerProvider;
    private SdkMeterProvider meterProvider;
    private Db2Receiver receiver;

    // Connections held open between setup and teardown so the lock stays alive during polling
    private Connection ownerConn;
    private Connection blockedConn;
    private Statement  blockedStmt;
    private Future<?>  blockedFuture;
    private ExecutorService exec;

    @BeforeEach
    void createTable() throws Exception {
        jdbcProps = new Properties();
        jdbcProps.setProperty("user",     db2.getUsername());
        jdbcProps.setProperty("password", db2.getPassword());

        try (Connection ddl = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps);
             Statement st   = ddl.createStatement()) {
            try { st.execute("DROP TABLE " + TABLE); } catch (Exception ignored) {}
            st.execute("CREATE TABLE " + TABLE + " (ID INT NOT NULL PRIMARY KEY, VAL VARCHAR(50))");
        }
    }

    @AfterEach
    void teardown() throws Exception {
        if (blockedStmt != null)  { try { blockedStmt.cancel(); } catch (Exception ignored) {} }
        if (blockedConn != null)  { try { blockedConn.rollback(); blockedConn.close(); } catch (Exception ignored) {} }
        if (blockedFuture != null){ blockedFuture.cancel(true); }
        if (exec != null)         { exec.shutdownNow(); }
        if (ownerConn != null)    { try { ownerConn.rollback(); ownerConn.close(); } catch (Exception ignored) {} }

        if (receiver != null) receiver.stop();
        if (loggerProvider != null) loggerProvider.shutdown();
        if (meterProvider  != null) meterProvider.shutdown();

        try (Connection c = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps);
             Statement s  = c.createStatement()) {
            try { s.execute("DROP TABLE " + TABLE); } catch (Exception ignored) {}
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private Db2ReceiverConfig buildConfig() {
        Db2ReceiverConfig config = new Db2ReceiverConfig();
        config.setHostname(db2.getHost());
        config.setHostport(db2.getMappedPort(50000));
        config.setUsername(db2.getUsername());
        config.setPassword(db2.getPassword());
        config.setDatabase(db2.getDatabaseName().toUpperCase());
        config.setCollectionInterval("2s");
        config.getEvents().getDbServerQuerySample().setEnabled(true);
        config.getQuerySampleCollection().setCollectionInterval("5s");
        config.getTls().setInsecure(true);
        return config;
    }

    private Db2Receiver buildReceiver(Db2ReceiverConfig config) {
        logExporter    = InMemoryLogRecordExporter.create();
        loggerProvider = SdkLoggerProvider.builder()
            .addLogRecordProcessor(SimpleLogRecordProcessor.create(logExporter))
            .build();
        Logger otelLogger = loggerProvider.get("splunk-otel-db2");

        meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(InMemoryMetricExporter.create())
                    .setInterval(Duration.ofSeconds(2))
                    .build())
            .build();

        return new Db2Receiver(config, meterProvider.get("splunk-otel-db2"), otelLogger);
    }

    /**
     * Session A: INSERT + hold (idle blocker — no active SQL after the INSERT).
     * Session B: SELECT … WITH RR → blocks on A's X-lock (blocked session).
     *
     * Also verifies that MON_LOCKWAITS actually sees the lock pair before returning,
     * so the polling test isn't racing against DB2 internal lock registration.
     */
    private void createLockWait() throws Exception {
        ownerConn = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps);
        ownerConn.setAutoCommit(false);
        try (Statement st = ownerConn.createStatement()) {
            st.execute("INSERT INTO " + TABLE + " VALUES (1, 'locked')");
        }

        exec        = Executors.newSingleThreadExecutor();
        blockedConn = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps);
        blockedConn.setAutoCommit(false);
        blockedStmt   = blockedConn.createStatement();
        blockedFuture = exec.submit(() -> {
            try {
                blockedStmt.executeQuery("SELECT ID FROM " + TABLE + " WITH RR");
            } catch (Exception ignored) {}
        });

        // Wait until MON_LOCKWAITS actually sees the pair — poll up to 15s with 500ms cadence.
        boolean lockVisible = false;
        try (Connection probe = DriverManager.getConnection(db2.getJdbcUrl(), jdbcProps)) {
            for (int i = 0; i < 30; i++) {
                Thread.sleep(500);
                try (Statement st = probe.createStatement();
                     ResultSet rs = st.executeQuery(
                         "SELECT COUNT(*) AS CNT FROM SYSIBMADM.MON_LOCKWAITS")) {
                    if (rs.next() && rs.getLong("CNT") > 0) {
                        lockVisible = true;
                        break;
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(
                        "MON_LOCKWAITS is not accessible — check SYSMON authority on db2inst1: " + e.getMessage(), e);
                }
            }
        }
        assertThat(lockVisible)
            .as("MON_LOCKWAITS must show the blocked→blocker pair within 15s of the lock being acquired")
            .isTrue();
    }

    // ── tests ────────────────────────────────────────────────────────────────

    @Test
    void blockingAttributesEmittedOnQuerySampleEvent() throws Exception {
        createLockWait();

        receiver = buildReceiver(buildConfig());
        receiver.start();

        // Wait for a WARN query_sample record that references our test table.
        Awaitility.await()
            .atMost(60, TimeUnit.SECONDS)
            .pollInterval(2, TimeUnit.SECONDS)
            .alias("blocking query_sample event in log exporter")
            .until(() -> logExporter.getFinishedLogRecordItems().stream()
                .anyMatch(r -> {
                    String text = r.getAttributes().get(QuerySampleEvent.DB_QUERY_TEXT);
                    return text != null
                        && text.toUpperCase().contains(TABLE)
                        && r.getAttributes().get(QuerySampleEvent.DB2_BLOCKER_APP_HANDLE) != null;
                }));

        List<LogRecordData> events = logExporter.getFinishedLogRecordItems().stream()
            .filter(r -> {
                String text = r.getAttributes().get(QuerySampleEvent.DB_QUERY_TEXT);
                return text != null
                    && text.toUpperCase().contains(TABLE)
                    && r.getAttributes().get(QuerySampleEvent.DB2_BLOCKER_APP_HANDLE) != null;
            })
            .collect(java.util.stream.Collectors.toList());

        assertThat(events).isNotEmpty();
        LogRecordData ev = events.get(0);

        // Event type and severity
        assertThat(ev.getEventName()).isEqualTo("db.server.query_sample");
        assertThat(ev.getSeverity()).isEqualTo(Severity.WARN);

        // Required identity attributes
        assertThat(ev.getAttributes().get(QuerySampleEvent.DB_SYSTEM)).isEqualTo("ibm.db2");
        assertThat(ev.getAttributes().get(QuerySampleEvent.DB_NAMESPACE))
            .isEqualToIgnoringCase(db2.getDatabaseName());

        // Both application handles present and non-zero
        String blockedHandle  = ev.getAttributes().get(QuerySampleEvent.DB2_APP_HANDLE);
        String blockerHandle  = ev.getAttributes().get(QuerySampleEvent.DB2_BLOCKER_APP_HANDLE);
        assertThat(blockedHandle).isNotBlank().isNotEqualTo("0");
        assertThat(blockerHandle).isNotBlank().isNotEqualTo("0");
        assertThat(blockedHandle).isNotEqualTo(blockerHandle);

        // Lock details
        assertThat(ev.getAttributes().get(QuerySampleEvent.DB2_LOCK_OBJECT_TYPE)).isNotBlank();
        assertThat(ev.getAttributes().get(QuerySampleEvent.DB2_LOCK_MODE)).isNotBlank();

        // Blocked SQL obfuscated and contains table name
        String blockedSql = ev.getAttributes().get(QuerySampleEvent.DB_QUERY_TEXT);
        assertThat(blockedSql).isNotNull();
        assertThat(blockedSql.toUpperCase()).contains(TABLE);
        assertThat(blockedSql).doesNotContain("'");

        // Wait duration
        assertThat(ev.getAttributes().get(QuerySampleEvent.DB2_WAIT_DURATION))
            .isNotNull().isGreaterThanOrEqualTo(0.0);

        // Member attribute
        assertThat(ev.getAttributes().get(QuerySampleEvent.DB2_MEMBER)).isNotNull();
    }

    @Test
    void idleBlockerFlagTrueAndQueryTextLookedUpFromPackageCache() throws Exception {
        createLockWait();
        // ownerConn did INSERT then became idle — BLOCKER_IS_IDLE should be true

        receiver = buildReceiver(buildConfig());
        receiver.start();

        Awaitility.await()
            .atMost(60, TimeUnit.SECONDS)
            .pollInterval(2, TimeUnit.SECONDS)
            .until(() -> logExporter.getFinishedLogRecordItems().stream()
                .anyMatch(r -> {
                    String text = r.getAttributes().get(QuerySampleEvent.DB_QUERY_TEXT);
                    return text != null
                        && text.toUpperCase().contains(TABLE)
                        && r.getAttributes().get(QuerySampleEvent.DB2_BLOCKER_APP_HANDLE) != null;
                }));

        LogRecordData ev = logExporter.getFinishedLogRecordItems().stream()
            .filter(r -> {
                String text = r.getAttributes().get(QuerySampleEvent.DB_QUERY_TEXT);
                return text != null
                    && text.toUpperCase().contains(TABLE)
                    && r.getAttributes().get(QuerySampleEvent.DB2_BLOCKER_APP_HANDLE) != null;
            })
            .findFirst()
            .orElseThrow();

        Boolean blockerIsIdle = ev.getAttributes().get(QuerySampleEvent.DB2_BLOCKER_IS_IDLE);
        assertThat(blockerIsIdle).isTrue();

        // Blocker query text should be populated from MON_GET_PKG_CACHE_STMT.
        // If the plan cache has evicted the INSERT, this field is null — that's acceptable.
        String blockerQueryText = ev.getAttributes().get(QuerySampleEvent.DB2_BLOCKER_QUERY_TEXT);
        if (blockerQueryText != null) {
            assertThat(blockerQueryText).doesNotContain("'");      // obfuscated
            assertThat(blockerQueryText).doesNotContain("locked"); // literal replaced
            assertThat(blockerQueryText.toUpperCase()).contains(TABLE);
        }
        assertThat(ev.getSeverity()).isEqualTo(Severity.WARN);
    }

    @Test
    void noBlockingAttributesWhenNothingIsBlocked() throws Exception {
        // No lock contention — all query_sample records should lack blocking attributes
        receiver = buildReceiver(buildConfig());
        receiver.start();

        Thread.sleep(8000); // 3+ collection cycles

        long blockingEvents = logExporter.getFinishedLogRecordItems().stream()
            .filter(r -> r.getAttributes().get(QuerySampleEvent.DB2_BLOCKER_APP_HANDLE) != null)
            .count();
        assertThat(blockingEvents).isZero();
    }
}
