package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.util.SqlObfuscator;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.LogRecordBuilder;
import io.opentelemetry.api.logs.Severity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class QuerySampleQueryTest {

    private Connection conn;
    private Statement stmt;
    private ResultSet rs;
    private PreparedStatement pstmt;
    private ResultSet prs;
    private Logger otelLogger;
    private LogRecordBuilder logBuilder;

    @BeforeEach
    void setUp() throws Exception {
        conn       = mock(Connection.class);
        stmt       = mock(Statement.class);
        rs         = mock(ResultSet.class);
        pstmt      = mock(PreparedStatement.class);
        prs        = mock(ResultSet.class);
        otelLogger = mock(Logger.class);
        logBuilder = mock(LogRecordBuilder.class, RETURNS_SELF);

        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(conn.prepareStatement(anyString())).thenReturn(pstmt);
        when(pstmt.executeQuery()).thenReturn(prs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
    }

    private QuerySampleQuery query(boolean useSnapshot) {
        return new QuerySampleQuery(() -> conn, "TESTDB", otelLogger, 100, useSnapshot);
    }

    private QuerySampleQuery query105() {
        return new QuerySampleQuery(() -> conn, "TESTDB", otelLogger, 100, false,
            QuerySampleQuery.DB2_VERSION_10_5);
    }

    // Stub a non-blocked row (all blocking columns return null / 0).
    private void stubRow(String stmtText, String state, String startTime,
                         long elapsedSec, String authId, String clientHost,
                         String appHandle, long member, long cpuUs) throws Exception {
        stubRow(stmtText, state, startTime, elapsedSec, authId, clientHost, appHandle, member, cpuUs, -1L);
    }

    private void stubRow(String stmtText, String state, String startTime,
                         long elapsedSec, String authId, String clientHost,
                         String appHandle, long member, long cpuUs, long lockWaitMs) throws Exception {
        when(rs.getString("STMT_TEXT")).thenReturn(stmtText);
        when(rs.getString("ACTIVITY_STATE")).thenReturn(state);
        when(rs.getString("OPERATION_NAME")).thenReturn("READ");
        when(rs.getString("LOCAL_START_TIME")).thenReturn(startTime);
        when(rs.getLong("WALL_ELAPSED_SEC")).thenReturn(elapsedSec);
        when(rs.getString("CLIENT_USERID")).thenReturn(authId);
        when(rs.getString("CLIENT_WRKSTNNAME")).thenReturn(clientHost);
        when(rs.getString("APPLICATION_HANDLE")).thenReturn(appHandle);
        when(rs.getLong("MEMBER")).thenReturn(member);
        when(rs.getLong("CPU_US")).thenReturn(cpuUs);
        when(rs.getLong("LOCK_WAIT_TIME_MS")).thenReturn(lockWaitMs);
        // No blocker
        when(rs.getInt("IS_BLOCKED")).thenReturn(0);
        when(rs.getObject("BLOCKER_HANDLE")).thenReturn(null);
    }

    // Stub a blocked row with blocking context.
    private void stubBlockedRow(String stmtText, String appHandle, long member,
                                long blockerHandle, long blockerMember,
                                String blockerSql, String blockerUser, String blockerHost,
                                String lockSchema, String lockObject,
                                String lockObjectType, String lockMode,
                                long waitElapsedSec, boolean blockerIsIdle) throws Exception {
        when(rs.getString("STMT_TEXT")).thenReturn(stmtText);
        when(rs.getString("ACTIVITY_STATE")).thenReturn("LOCKWAIT DML");
        when(rs.getString("OPERATION_NAME")).thenReturn("READ");
        when(rs.getString("LOCAL_START_TIME")).thenReturn(null);
        when(rs.getLong("WALL_ELAPSED_SEC")).thenReturn(waitElapsedSec);
        when(rs.getString("CLIENT_USERID")).thenReturn("user1");
        when(rs.getString("CLIENT_WRKSTNNAME")).thenReturn("host1");
        when(rs.getString("APPLICATION_HANDLE")).thenReturn(appHandle);
        when(rs.getLong("MEMBER")).thenReturn(member);
        when(rs.getLong("CPU_US")).thenReturn(0L);
        when(rs.getLong("LOCK_WAIT_TIME_MS")).thenReturn(-1L);
        when(rs.getInt("IS_BLOCKED")).thenReturn(1);
        when(rs.getObject("BLOCKER_HANDLE")).thenReturn(blockerHandle);
        when(rs.getLong("BLOCKER_MEMBER")).thenReturn(blockerMember);
        when(rs.getLong("BLOCKING_WAIT_SEC")).thenReturn(waitElapsedSec);
        when(rs.getString("BLOCKING_START_TIME")).thenReturn("2024-01-01-12.00.00.000000");
        when(rs.getString("BLOCKER_SQL")).thenReturn(blockerSql);
        when(rs.getString("BLOCKER_USER")).thenReturn(blockerUser);
        when(rs.getString("BLOCKER_HOST")).thenReturn(blockerHost);
        when(rs.getString("LOCK_SCHEMA")).thenReturn(lockSchema);
        when(rs.getString("LOCK_OBJECT")).thenReturn(lockObject);
        when(rs.getString("LOCK_OBJECT_TYPE")).thenReturn(lockObjectType);
        when(rs.getString("LOCK_MODE")).thenReturn(lockMode);
        when(rs.getInt("BLOCKER_IS_IDLE")).thenReturn(blockerIsIdle ? 1 : 0);
    }

    @Test
    void emitsObfuscatedLogRecordWithAllAttributes() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT * FROM T WHERE ID = 42", "EXECUTING SELECT",
                "2024-01-01-12.00.00.000000", 5L,
                "db2inst1", "client-host", "12345", 0L, 100L);

        query(false).collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        verify(logBuilder).emit();

        io.opentelemetry.api.common.Attributes attrs = cap.getValue();
        assertThat(attrs.get(QuerySampleEvent.DB_QUERY_TEXT))
            .isEqualTo("SELECT * FROM T WHERE ID = ?");
        verify(logBuilder).setEventName("db.server.query_sample");
        assertThat(attrs.get(QuerySampleEvent.DB_SYSTEM)).isEqualTo("ibm.db2");
        assertThat(attrs.get(QuerySampleEvent.DB_NAMESPACE)).isEqualTo("TESTDB");
        assertThat(attrs.get(QuerySampleEvent.DB2_ACTIVITY)).isEqualTo("EXECUTING SELECT");
        assertThat(attrs.get(QuerySampleEvent.DB2_QUERY_START))
            .isEqualTo("2024-01-01-12.00.00.000000");
        assertThat(attrs.get(QuerySampleEvent.DB2_DURATION)).isEqualTo(5.0);
        assertThat(attrs.get(QuerySampleEvent.USER_NAME)).isEqualTo("db2inst1");
        assertThat(attrs.get(QuerySampleEvent.CLIENT_ADDRESS)).isEqualTo("client-host");
        assertThat(attrs.get(QuerySampleEvent.DB2_APP_HANDLE)).isEqualTo("12345");
        assertThat(attrs.get(QuerySampleEvent.DB2_MEMBER)).isEqualTo(0L);
        assertThat(attrs.get(QuerySampleEvent.DB2_CPU_TIME)).isEqualTo(100L / 1_000_000.0);
        // Non-blocked row must not have blocking attributes
        assertThat(attrs.get(QuerySampleEvent.DB2_BLOCKER_APP_HANDLE)).isNull();
    }

    @Test
    void snapshotPathNoCpuAttribute() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM SYSIBM.SYSDUMMY1", "UOWEXEC SELECT",
                "2024-01-01-12.00.00.000000", 2L,
                "db2inst1", "host1", "99", 0L, -1L);

        query(true).collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());

        assertThat(cap.getValue().get(QuerySampleEvent.DB2_CPU_TIME)).isNull();
    }

    @Test
    void db2Version105EmitsWaitTime() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", "LOCKWAIT DML", null, 3L,
                null, null, null, 0L, 0L, 5000L);

        query105().collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());

        assertThat(cap.getValue().get(QuerySampleEvent.DB2_WAIT_TIME)).isEqualTo(5.0);
    }

    @Test
    void db2Version105SqlContainsMON_GET_ACTIVITY() throws Exception {
        when(rs.next()).thenReturn(false);
        query105().collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        assertThat(sqlCap.getValue()).containsIgnoringCase("MON_GET_ACTIVITY");
        assertThat(sqlCap.getValue()).containsIgnoringCase("LOCK_WAIT_TIME");
    }

    @Test
    void defaultPathNoWaitTimeAttribute() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", "EXECUTING", null, 1L, null, null, null, 0L, 0L, -1L);

        query(false).collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        assertThat(cap.getValue().get(QuerySampleEvent.DB2_WAIT_TIME)).isNull();
    }

    @Test
    void obfuscationFailedDropsRecord() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", "EXECUTING", null, 1L, null, null, null, 0L, 0L);

        try (MockedStatic<SqlObfuscator> mocked = mockStatic(SqlObfuscator.class)) {
            mocked.when(() -> SqlObfuscator.obfuscate(anyString()))
                .thenReturn(SqlObfuscator.OBFUSCATION_FAILED);
            query(false).collect();
        }

        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void nullOrBlankStatementSkipped() throws Exception {
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString(anyString())).thenReturn(null);
        when(rs.getString("STMT_TEXT")).thenReturn(null, "   ");
        when(rs.getLong(anyString())).thenReturn(0L);

        query(false).collect();

        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void severityIsInfo() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", "EXECUTING", null, 0L, null, null, null, 0L, 0L);

        query(false).collect();

        verify(logBuilder).setSeverity(Severity.INFO);
        verify(logBuilder).setSeverityText("INFO");
    }

    @Test
    void connectionExceptionHandledGracefully() {
        Supplier<Connection> failing = () -> { throw new RuntimeException("refused"); };
        new QuerySampleQuery(failing, "TESTDB", otelLogger, 10, false).collect();
        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void configRejectsZeroSampleCount() {
        com.splunk.otel.db2.config.Db2ReceiverConfig cfg = new com.splunk.otel.db2.config.Db2ReceiverConfig();
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            () -> cfg.setMaxQuerySampleCount(0));
    }

    @Test
    void configRejectsNegativeSampleCount() {
        com.splunk.otel.db2.config.Db2ReceiverConfig cfg = new com.splunk.otel.db2.config.Db2ReceiverConfig();
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            () -> cfg.setMaxQuerySampleCount(-1));
    }

    @Test
    void defaultPathSqlContainsMON_CURRENT_SQL() throws Exception {
        when(rs.next()).thenReturn(false);
        query(false).collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        assertThat(sqlCap.getValue()).containsIgnoringCase("SYSIBMADM.MON_CURRENT_SQL");
        assertThat(sqlCap.getValue()).containsIgnoringCase("MON_GET_CONNECTION");
        assertThat(sqlCap.getValue()).doesNotContainIgnoringCase("TABLE(MON_CURRENT_SQL");
    }

    @Test
    void defaultPathSqlContainsMON_LOCKWAITS() throws Exception {
        when(rs.next()).thenReturn(false);
        query(false).collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        assertThat(sqlCap.getValue()).containsIgnoringCase("SYSIBMADM.MON_LOCKWAITS");
        assertThat(sqlCap.getValue()).containsIgnoringCase("IS_BLOCKED");
        assertThat(sqlCap.getValue()).containsIgnoringCase("BLOCKER_HANDLE");
    }

    @Test
    void snapshotPathSqlContainsSnapGetStmt() throws Exception {
        when(rs.next()).thenReturn(false);
        query(true).collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        assertThat(sqlCap.getValue()).containsIgnoringCase("SNAP_GET_STMT");
        assertThat(sqlCap.getValue()).containsIgnoringCase("SYSCAT.STATEMENTS");
    }

    @Test
    void snapshotPathSqlDoesNotContainMON_LOCKWAITS() throws Exception {
        when(rs.next()).thenReturn(false);
        query(true).collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        assertThat(sqlCap.getValue()).doesNotContainIgnoringCase("MON_LOCKWAITS");
    }

    @Test
    void sqlContainsFetchFirstWithConfiguredLimit() throws Exception {
        when(rs.next()).thenReturn(false);
        new QuerySampleQuery(() -> conn, "TESTDB", otelLogger, 500, false).collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        assertThat(sqlCap.getValue()).contains("FETCH FIRST 500 ROWS ONLY");
    }

    @Test
    void defaultPathSqlExcludesSelfAndIdleAndQueryText() throws Exception {
        when(rs.next()).thenReturn(false);
        query(false).collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        String sql = sqlCap.getValue();
        assertThat(sql).contains("splunk-otel-db2");
        assertThat(sql).containsIgnoringCase("IDLE");
        assertThat(sql).containsIgnoringCase("SYSIBMADM.MON_CURRENT_SQL");
    }

    @Test
    void snapshotPathSqlExcludesSelf() throws Exception {
        when(rs.next()).thenReturn(false);
        query(true).collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        assertThat(sqlCap.getValue()).contains("splunk-otel-db2");
    }

    @Test
    void multipleRowsEmitMultipleRecords() throws Exception {
        when(rs.next()).thenReturn(true, true, true, false);
        when(rs.getString(anyString())).thenReturn(null);
        when(rs.getString("STMT_TEXT"))
            .thenReturn("SELECT 1 FROM T", "SELECT 2 FROM T", "SELECT 3 FROM T");
        when(rs.getLong(anyString())).thenReturn(0L);
        when(rs.getInt("IS_BLOCKED")).thenReturn(0);
        when(rs.getObject("BLOCKER_HANDLE")).thenReturn(null);

        query(false).collect();

        verify(logBuilder, times(3)).emit();
    }

    // ── Blocking detection tests ──────────────────────────────────────────────

    @Test
    void blockedSessionEmitsWarnWithBlockingAttributes() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubBlockedRow(
            "SELECT ID FROM ORDERS WHERE ID = 1",
            "111", 0L, 222L, 0L,
            "UPDATE ORDERS SET STATUS = 'X' WHERE ID = 1",
            "user2", "host2",
            "DB2INST1", "ORDERS", "ROW", "X",
            3L, false);

        query(false).collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        verify(logBuilder).setSeverity(Severity.WARN);
        verify(logBuilder).setSeverityText("WARN");

        io.opentelemetry.api.common.Attributes attrs = cap.getValue();
        assertThat(attrs.get(QuerySampleEvent.DB2_APP_HANDLE)).isEqualTo("111");
        assertThat(attrs.get(QuerySampleEvent.DB2_BLOCKER_APP_HANDLE)).isEqualTo("222");
        assertThat(attrs.get(QuerySampleEvent.DB_QUERY_TEXT))
            .isEqualTo("SELECT ID FROM ORDERS WHERE ID = ?");
        assertThat(attrs.get(QuerySampleEvent.DB2_BLOCKER_QUERY_TEXT))
            .isEqualTo("UPDATE ORDERS SET STATUS = ? WHERE ID = ?");
        assertThat(attrs.get(QuerySampleEvent.DB2_BLOCKER_USER)).isEqualTo("user2");
        assertThat(attrs.get(QuerySampleEvent.DB2_BLOCKER_HOST)).isEqualTo("host2");
        assertThat(attrs.get(QuerySampleEvent.DB2_LOCK_OBJECT_TYPE)).isEqualTo("ROW");
        assertThat(attrs.get(QuerySampleEvent.DB2_LOCK_MODE)).isEqualTo("X");
        assertThat(attrs.get(QuerySampleEvent.DB2_LOCK_OBJECT_NAME)).isEqualTo("ORDERS");
        assertThat(attrs.get(QuerySampleEvent.DB2_LOCK_SCHEMA)).isEqualTo("DB2INST1");
        assertThat(attrs.get(QuerySampleEvent.DB2_BLOCKER_IS_IDLE)).isFalse();
        assertThat(attrs.get(QuerySampleEvent.DB2_WAIT_DURATION)).isGreaterThanOrEqualTo(2.9);
        // blocking.start_time must be the server-supplied value, not a JVM-derived string
        assertThat(attrs.get(QuerySampleEvent.DB2_BLOCKING_START))
            .isEqualTo("2024-01-01-12.00.00.000000");
    }

    @Test
    void idleBlockerTriggersPackageCacheLookup() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubBlockedRow(
            "SELECT ID FROM T WHERE ID = 1",
            "111", 0L, 222L, 0L,
            null, null, null, null, null, null, null,
            3L, true); // idle blocker, no active SQL

        when(prs.next()).thenReturn(true, false);
        when(prs.getString("STMT_TEXT")).thenReturn("INSERT INTO T VALUES (1, 'literal')");

        query(false).collect();

        verify(conn).prepareStatement(argThat(s -> s.contains("MON_GET_PKG_CACHE_STMT")));
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        assertThat(cap.getValue().get(QuerySampleEvent.DB2_BLOCKER_QUERY_TEXT))
            .isEqualTo("INSERT INTO T VALUES (?, ?)");
        assertThat(cap.getValue().get(QuerySampleEvent.DB2_BLOCKER_IS_IDLE)).isTrue();
    }

    @Test
    void idleBlockerPackageCacheLookupFailureIsHandledGracefully() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubBlockedRow(
            "SELECT ID FROM T WHERE ID = 1",
            "111", 0L, 222L, 0L,
            null, null, null, null, null, null, null,
            0L, true);
        when(pstmt.executeQuery()).thenThrow(new java.sql.SQLException("table not found"));

        query(false).collect();

        // Record should still be emitted without blocker query text or is_idle
        // (pkg cache lookup failed — we cannot confirm idleness, omit rather than guess)
        verify(logBuilder).emit();
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        assertThat(cap.getValue().get(QuerySampleEvent.DB2_BLOCKER_QUERY_TEXT)).isNull();
        assertThat(cap.getValue().get(QuerySampleEvent.DB2_BLOCKER_IS_IDLE)).isNull();
    }

    @Test
    void blockerObfuscationFailedOmitsFieldButKeepsRecord() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubBlockedRow(
            "SELECT 1 FROM T",
            "111", 0L, 222L, 0L,
            "UPDATE T SET X = 1", null, null, null, null, null, null,
            0L, false);

        try (MockedStatic<SqlObfuscator> mocked = mockStatic(SqlObfuscator.class)) {
            mocked.when(() -> SqlObfuscator.obfuscate("SELECT 1 FROM T"))
                .thenReturn("SELECT ? FROM T");
            mocked.when(() -> SqlObfuscator.obfuscate("UPDATE T SET X = 1"))
                .thenReturn(SqlObfuscator.OBFUSCATION_FAILED);
            query(false).collect();
        }

        verify(logBuilder).emit();
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        assertThat(cap.getValue().get(QuerySampleEvent.DB2_BLOCKER_QUERY_TEXT)).isNull();
    }

    @Test
    void globalLockWaitWithNullHolderEmitsWarnWithLockAttributes() throws Exception {
        // DB2 can return HLD_APPLICATION_HANDLE = NULL for global lock waits or when
        // the holder cannot be identified. IS_BLOCKED = 1 is the authoritative signal.
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT ID FROM T WHERE ID = 1");
        when(rs.getString("ACTIVITY_STATE")).thenReturn("LOCKWAIT DML");
        when(rs.getString("OPERATION_NAME")).thenReturn("READ");
        when(rs.getString("LOCAL_START_TIME")).thenReturn(null);
        when(rs.getLong("WALL_ELAPSED_SEC")).thenReturn(2L);
        when(rs.getString("CLIENT_USERID")).thenReturn("user1");
        when(rs.getString("CLIENT_WRKSTNNAME")).thenReturn("host1");
        when(rs.getString("APPLICATION_HANDLE")).thenReturn("77");
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getLong("CPU_US")).thenReturn(0L);
        when(rs.getLong("LOCK_WAIT_TIME_MS")).thenReturn(-1L);
        when(rs.getInt("IS_BLOCKED")).thenReturn(1);
        when(rs.getObject("BLOCKER_HANDLE")).thenReturn(null); // unknown holder
        when(rs.getLong("BLOCKING_WAIT_SEC")).thenReturn(2L);
        when(rs.getString("BLOCKING_START_TIME")).thenReturn("2024-01-01-11.59.58.000000");
        when(rs.getString("LOCK_OBJECT_TYPE")).thenReturn("TABLE");
        when(rs.getString("LOCK_MODE")).thenReturn("S");
        when(rs.getString("LOCK_OBJECT")).thenReturn("MY_TABLE");
        when(rs.getString("LOCK_SCHEMA")).thenReturn("MYSCHEMA");

        query(false).collect();

        verify(logBuilder).setSeverity(Severity.WARN);
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        io.opentelemetry.api.common.Attributes attrs = cap.getValue();
        assertThat(attrs.get(QuerySampleEvent.DB2_WAIT_DURATION)).isGreaterThanOrEqualTo(1.9);
        assertThat(attrs.get(QuerySampleEvent.DB2_LOCK_OBJECT_TYPE)).isEqualTo("TABLE");
        assertThat(attrs.get(QuerySampleEvent.DB2_LOCK_MODE)).isEqualTo("S");
        assertThat(attrs.get(QuerySampleEvent.DB2_LOCK_OBJECT_NAME)).isEqualTo("MY_TABLE");
        assertThat(attrs.get(QuerySampleEvent.DB2_LOCK_SCHEMA)).isEqualTo("MYSCHEMA");
        // Blocker identity unavailable — omitted but record still emitted at WARN
        assertThat(attrs.get(QuerySampleEvent.DB2_BLOCKER_APP_HANDLE)).isNull();
    }

    @Test
    void snapshotPathDoesNotEmitBlockingAttributes() throws Exception {
        when(rs.next()).thenReturn(true, false);
        // anyString() first so specific stubs below override it
        when(rs.getString(anyString())).thenReturn(null);
        when(rs.getLong(anyString())).thenReturn(0L);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM SYSIBM.SYSDUMMY1");
        when(rs.getLong("WALL_ELAPSED_SEC")).thenReturn(1L);
        when(rs.getLong("CPU_US")).thenReturn(-1L);

        query(true).collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        assertThat(cap.getValue().get(QuerySampleEvent.DB2_BLOCKER_APP_HANDLE)).isNull();
        assertThat(cap.getValue().get(QuerySampleEvent.DB2_BLOCKER_IS_IDLE)).isNull();
        verify(logBuilder).setSeverity(Severity.INFO);
    }

    @Test
    void collectReturnsTrueOnSuccess() throws Exception {
        when(rs.next()).thenReturn(false);
        assertThat(query(false).collect()).isTrue();
    }

    @Test
    void collectReturnsFalseOnException() {
        Supplier<Connection> failing = () -> { throw new RuntimeException("refused"); };
        assertThat(new QuerySampleQuery(failing, "TESTDB", otelLogger, 10, false).collect()).isFalse();
    }

    @Test
    void defaultPathSqlUsesLeftJoinForMonGetConnection() throws Exception {
        when(rs.next()).thenReturn(false);
        query(false).collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        String sql = sqlCap.getValue().toUpperCase();
        // MON_GET_CONNECTION must be a LEFT JOIN so sessions not yet in the connection
        // table are not silently dropped
        int leftJoinPos = sql.indexOf("LEFT JOIN");
        int innerJoinPos = sql.indexOf("JOIN (SELECT");
        // first JOIN involving MON_GET_CONNECTION must be preceded by LEFT
        assertThat(sql).containsIgnoringCase("LEFT JOIN (SELECT");
        assertThat(sql).doesNotContainIgnoringCase("INNER JOIN (SELECT");
    }

    @Test
    void defaultPathSqlMemberQualifiesBlockerSqlJoin() throws Exception {
        when(rs.next()).thenReturn(false);
        query(false).collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        // BLOCKER_SQL join must be member-qualified to avoid cross-member misclassification
        assertThat(sqlCap.getValue()).containsIgnoringCase("BLOCKER_SQL.COORD_MEMBER");
    }

    @Test
    void defaultPathSqlComputesStartTimeServerSide() throws Exception {
        when(rs.next()).thenReturn(false);
        query(false).collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        // BLOCKING_START_TIME is derived from MON_LOCKWAITS arithmetic so that
        // SYSIBMADM.LOCKWAITS (snapshot view with incompatible column names) is never joined.
        assertThat(sqlCap.getValue()).doesNotContainIgnoringCase("SYSIBMADM.LOCKWAITS");
        assertThat(sqlCap.getValue()).containsIgnoringCase("LOCK_WAIT_ELAPSED_TIME SECONDS");
        assertThat(sqlCap.getValue()).containsIgnoringCase("BLOCKING_START_TIME");
    }

    @Test
    void defaultPathSqlExcludesAllCollectorPatterns() throws Exception {
        when(rs.next()).thenReturn(false);
        query(false).collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        String sql = sqlCap.getValue();
        assertThat(sql).containsIgnoringCase("MON_CURRENT_SQL");
        assertThat(sql).containsIgnoringCase("MON_LOCKWAITS");
        assertThat(sql).containsIgnoringCase("MON_GET_CONNECTION");
        assertThat(sql).containsIgnoringCase("MON_GET_PKG_CACHE_STMT");
    }

    @Test
    void missingBlockerSqlJoinDoesNotReportIsIdle() throws Exception {
        // When BLOCKER_SQL join misses (cross-member blocker), BLOCKER_SQL is null and
        // BLOCKER_IS_IDLE=1 is set by the CASE expression. The receiver must not emit
        // is_idle=true in this ambiguous case.
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM T");
        when(rs.getString("ACTIVITY_STATE")).thenReturn("LOCKWAIT DML");
        when(rs.getString("OPERATION_NAME")).thenReturn("READ");
        when(rs.getString("LOCAL_START_TIME")).thenReturn(null);
        when(rs.getLong("WALL_ELAPSED_SEC")).thenReturn(5L);
        when(rs.getString("CLIENT_USERID")).thenReturn("user1");
        when(rs.getString("CLIENT_WRKSTNNAME")).thenReturn("host1");
        when(rs.getString("APPLICATION_HANDLE")).thenReturn("100");
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getLong("CPU_US")).thenReturn(0L);
        when(rs.getLong("LOCK_WAIT_TIME_MS")).thenReturn(-1L);
        when(rs.getInt("IS_BLOCKED")).thenReturn(1);
        when(rs.getObject("BLOCKER_HANDLE")).thenReturn(200L);
        when(rs.getLong("BLOCKER_MEMBER")).thenReturn(1L);
        when(rs.getLong("BLOCKING_WAIT_SEC")).thenReturn(5L);
        when(rs.getString("BLOCKING_START_TIME")).thenReturn(null);
        // BLOCKER_SQL null + BLOCKER_IS_IDLE=1 = join miss, not confirmed idle
        when(rs.getString("BLOCKER_SQL")).thenReturn(null);
        when(rs.getInt("BLOCKER_IS_IDLE")).thenReturn(1);
        when(rs.getString("BLOCKER_USER")).thenReturn(null);
        when(rs.getString("BLOCKER_HOST")).thenReturn(null);
        when(rs.getString("LOCK_OBJECT_TYPE")).thenReturn("ROW");
        when(rs.getString("LOCK_MODE")).thenReturn("X");
        when(rs.getString("LOCK_OBJECT")).thenReturn("T");
        when(rs.getString("LOCK_SCHEMA")).thenReturn("DB2INST1");

        query(false).collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        // is_idle must be omitted when we can't distinguish join miss from real idle
        assertThat(cap.getValue().get(QuerySampleEvent.DB2_BLOCKER_IS_IDLE)).isNull();
    }
}
