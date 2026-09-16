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
import java.sql.Timestamp;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class BlockingSessionQueryTest {

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

    private BlockingSessionQuery query() {
        return new BlockingSessionQuery(() -> conn, "TESTDB", otelLogger, 10L);
    }

    // waitStartMs: epoch ms when the lock wait began (from SYSIBMADM.LOCKWAITS.LOCK_WAIT_START_TIME)
    private void stubRow(long blockedHandle, long blockerHandle,
                          String blockedSql, String blockerSql,
                          String activityState, String blockedUser, String blockedHost,
                          String blockerUser, String blockerHost,
                          String lockSchema, String lockObject, String lockObjectType, String lockMode,
                          long waitStartMs, long member, boolean blockerIsIdle) throws Exception {
        when(rs.getLong("BLOCKED_HANDLE")).thenReturn(blockedHandle);
        when(rs.getLong("BLOCKED_MEMBER")).thenReturn(member);
        when(rs.getLong("BLOCKER_HANDLE")).thenReturn(blockerHandle);
        when(rs.getLong("BLOCKER_MEMBER")).thenReturn(member);
        when(rs.getString("BLOCKED_SQL")).thenReturn(blockedSql);
        when(rs.getString("BLOCKER_SQL")).thenReturn(blockerSql);
        when(rs.getString("ACTIVITY_STATE")).thenReturn(activityState);
        when(rs.getString("BLOCKED_USER")).thenReturn(blockedUser);
        when(rs.getString("BLOCKED_HOST")).thenReturn(blockedHost);
        when(rs.getString("BLOCKER_USER")).thenReturn(blockerUser);
        when(rs.getString("BLOCKER_HOST")).thenReturn(blockerHost);
        when(rs.getString("LOCK_SCHEMA")).thenReturn(lockSchema);
        when(rs.getString("LOCK_OBJECT")).thenReturn(lockObject);
        when(rs.getString("LOCK_OBJECT_TYPE")).thenReturn(lockObjectType);
        when(rs.getString("LOCK_MODE")).thenReturn(lockMode);
        when(rs.getTimestamp("LOCK_WAIT_START_TIME"))
            .thenReturn(waitStartMs > 0 ? new Timestamp(waitStartMs) : null);
        when(rs.getInt("BLOCKER_IS_IDLE")).thenReturn(blockerIsIdle ? 1 : 0);
    }

    @Test
    void emitsObfuscatedLogRecordWithAllAttributes() throws Exception {
        when(rs.next()).thenReturn(true, false);
        // start time 3 seconds ago — duration should be >= 2.9s
        long startMs = System.currentTimeMillis() - 3000;
        stubRow(111L, 222L,
            "SELECT * FROM T WHERE ID = 99", "UPDATE T SET X = 1 WHERE ID = 99",
            "LOCK_WAIT READ_DML", "user1", "host1",
            "user2", "host2",
            "DB2INST1", "ORDERS", "ROW", "X",
            startMs, 0L, false);

        query().collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        verify(logBuilder).emit();

        io.opentelemetry.api.common.Attributes attrs = cap.getValue();
        assertThat(attrs.get(BlockingSessionEvent.DB_SYSTEM)).isEqualTo("ibm.db2");
        assertThat(attrs.get(BlockingSessionEvent.DB_NAMESPACE)).isEqualTo("TESTDB");
        assertThat(attrs.get(BlockingSessionEvent.DB2_APP_HANDLE)).isEqualTo("111");
        assertThat(attrs.get(BlockingSessionEvent.DB2_BLOCKER_APP_HANDLE)).isEqualTo("222");
        assertThat(attrs.get(BlockingSessionEvent.DB_QUERY_TEXT))
            .isEqualTo("SELECT * FROM T WHERE ID = ?");
        assertThat(attrs.get(BlockingSessionEvent.DB2_BLOCKER_QUERY_TEXT))
            .isEqualTo("UPDATE T SET X = ? WHERE ID = ?");
        assertThat(attrs.get(BlockingSessionEvent.USER_NAME)).isEqualTo("user1");
        assertThat(attrs.get(BlockingSessionEvent.CLIENT_ADDRESS)).isEqualTo("host1");
        assertThat(attrs.get(BlockingSessionEvent.DB2_BLOCKER_USER)).isEqualTo("user2");
        assertThat(attrs.get(BlockingSessionEvent.DB2_BLOCKER_HOST)).isEqualTo("host2");
        assertThat(attrs.get(BlockingSessionEvent.DB2_LOCK_OBJECT_TYPE)).isEqualTo("ROW");
        assertThat(attrs.get(BlockingSessionEvent.DB2_LOCK_MODE)).isEqualTo("X");
        assertThat(attrs.get(BlockingSessionEvent.DB2_LOCK_OBJECT_NAME)).isEqualTo("ORDERS");
        assertThat(attrs.get(BlockingSessionEvent.DB2_LOCK_SCHEMA)).isEqualTo("DB2INST1");
        assertThat(attrs.get(BlockingSessionEvent.DB2_BLOCKER_IS_IDLE)).isFalse();
        assertThat(attrs.get(BlockingSessionEvent.DB2_WAIT_DURATION)).isGreaterThanOrEqualTo(2.9);
        assertThat(attrs.get(BlockingSessionEvent.DB2_MEMBER)).isEqualTo(0L);
    }

    @Test
    void severityIsWarn() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubRow(1L, 2L, "SELECT 1 FROM T", null, null, null, null,
            null, null, null, null, null, null, 0L, 0L, false);

        query().collect();

        verify(logBuilder).setSeverity(Severity.WARN);
        verify(logBuilder).setSeverityText("WARN");
    }

    @Test
    void eventNameIsBlockingSession() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubRow(1L, 2L, "SELECT 1 FROM T", null, null, null, null,
            null, null, null, null, null, null, 0L, 0L, false);

        query().collect();

        verify(logBuilder).setEventName("db.server.blocking_session");
    }

    @Test
    void blockedObfuscationFailedDropsRecord() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubRow(1L, 2L, "SELECT 1 FROM T", null, null, null, null,
            null, null, null, null, null, null, 0L, 0L, false);

        try (MockedStatic<SqlObfuscator> mocked = mockStatic(SqlObfuscator.class)) {
            mocked.when(() -> SqlObfuscator.obfuscate(anyString()))
                .thenReturn(SqlObfuscator.OBFUSCATION_FAILED);
            query().collect();
        }

        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void blockerObfuscationFailedOmitsFieldButKeepsRecord() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubRow(1L, 2L, "SELECT 1 FROM T", "UPDATE T SET X = 1", null, null, null,
            null, null, null, null, null, null, 0L, 0L, false);

        try (MockedStatic<SqlObfuscator> mocked = mockStatic(SqlObfuscator.class)) {
            mocked.when(() -> SqlObfuscator.obfuscate("SELECT 1 FROM T"))
                .thenReturn("SELECT ? FROM T");
            mocked.when(() -> SqlObfuscator.obfuscate("UPDATE T SET X = 1"))
                .thenReturn(SqlObfuscator.OBFUSCATION_FAILED);
            query().collect();
        }

        verify(logBuilder).emit(); // record is still emitted

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        assertThat(cap.getValue().get(BlockingSessionEvent.DB2_BLOCKER_QUERY_TEXT)).isNull();
    }

    @Test
    void idleBlockerTriggersPackageCacheLookup() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubRow(1L, 2L, "SELECT 1 FROM T", null, null, null, null,
            null, null, null, null, null, null, 0L, 0L, true); // blockerIsIdle = true
        when(prs.next()).thenReturn(true, false);
        when(prs.getString("STMT_TEXT")).thenReturn("UPDATE T SET X = 'literal'");

        query().collect();

        verify(conn).prepareStatement(argThat(s -> s.contains("MON_GET_PKG_CACHE_STMT")));
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        assertThat(cap.getValue().get(BlockingSessionEvent.DB2_BLOCKER_QUERY_TEXT))
            .isEqualTo("UPDATE T SET X = ?");
        assertThat(cap.getValue().get(BlockingSessionEvent.DB2_BLOCKER_IS_IDLE)).isTrue();
    }

    @Test
    void idleBlockerPackageCacheLookupFailureIsHandledGracefully() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubRow(1L, 2L, "SELECT 1 FROM T", null, null, null, null,
            null, null, null, null, null, null, 0L, 0L, true);
        when(pstmt.executeQuery()).thenThrow(new java.sql.SQLException("table not found"));

        query().collect();

        // Record still emitted, blocker query text just omitted
        verify(logBuilder).emit();
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        assertThat(cap.getValue().get(BlockingSessionEvent.DB2_BLOCKER_QUERY_TEXT)).isNull();
    }

    @Test
    void nullStartTimeEmitsZeroDurationAndNullStartTime() throws Exception {
        when(rs.next()).thenReturn(true, false);
        // 0L → stubRow returns null Timestamp (simulates LEFT JOIN miss on SYSIBMADM.LOCKWAITS)
        stubRow(1L, 2L, "SELECT 1 FROM T", null, null, null, null,
            null, null, null, null, null, null, 0L, 0L, false);

        query().collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        // null start time → duration falls back to 0ms → 0.0s; start_time attribute omitted
        assertThat(cap.getValue().get(BlockingSessionEvent.DB2_WAIT_DURATION)).isEqualTo(0.0);
        assertThat(cap.getValue().get(BlockingSessionEvent.DB2_BLOCKING_START)).isNull();
    }

    @Test
    void sqlContainsMON_LOCKWAITS() throws Exception {
        when(rs.next()).thenReturn(false);
        query().collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        assertThat(sqlCap.getValue()).containsIgnoringCase("SYSIBMADM.MON_LOCKWAITS");
        assertThat(sqlCap.getValue()).containsIgnoringCase("MON_CURRENT_SQL");
        assertThat(sqlCap.getValue()).containsIgnoringCase("MON_GET_CONNECTION");
    }

    @Test
    void connectionExceptionHandledGracefully() {
        Supplier<Connection> failing = () -> { throw new RuntimeException("refused"); };
        new BlockingSessionQuery(failing, "TESTDB", otelLogger, 10L).collect();
        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void multipleRowsEmitMultipleRecords() throws Exception {
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getLong("BLOCKED_HANDLE")).thenReturn(1L, 3L);
        when(rs.getLong("BLOCKER_HANDLE")).thenReturn(2L, 4L);
        when(rs.getString("BLOCKED_SQL")).thenReturn("SELECT 1 FROM T", "SELECT 2 FROM T");
        when(rs.getString(argThat(s -> !s.equals("BLOCKED_SQL")))).thenReturn(null);
        when(rs.getLong(argThat(s -> !s.equals("BLOCKED_HANDLE") && !s.equals("BLOCKER_HANDLE")
                && !s.equals("BLOCKED_MEMBER") && !s.equals("BLOCKER_MEMBER"))))
            .thenReturn(0L);
        when(rs.getLong("BLOCKED_MEMBER")).thenReturn(0L);
        when(rs.getLong("BLOCKER_MEMBER")).thenReturn(0L);
        when(rs.getInt("BLOCKER_IS_IDLE")).thenReturn(0);

        query().collect();

        verify(logBuilder, times(2)).emit();
    }
}
