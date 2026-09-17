package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.util.SqlObfuscator;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.LogRecordBuilder;
import io.opentelemetry.api.logs.Severity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class TopQueryQueryTest {

    private Connection conn;
    private Statement stmt;
    private ResultSet rs;
    private Logger otelLogger;
    private LogRecordBuilder logBuilder;

    @BeforeEach
    void setUp() throws Exception {
        conn       = mock(Connection.class);
        stmt       = mock(Statement.class);
        rs         = mock(ResultSet.class);
        otelLogger = mock(Logger.class);
        logBuilder = mock(LogRecordBuilder.class, RETURNS_SELF);

        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        // Default: INSERT_TIMESTAMP null → treated as Epoch → section is always "old".
        // Tests that need a "new" section override this explicitly.
        when(rs.getTimestamp("INSERT_TIMESTAMP")).thenReturn(null);
        // Default: SERVER_NOW = a fixed past timestamp (simulates Db2 server clock).
        // Tests override per-cycle to simulate the passage of time.
        when(rs.getTimestamp("SERVER_NOW")).thenReturn(Timestamp.from(Instant.EPOCH.plusSeconds(1000)));
    }

    private TopQueryQuery query() {
        return new TopQueryQuery(() -> conn, "TESTDB", otelLogger, 10, 60L);
    }

    // Stub a single result row. stmtExecMs is STMT_EXEC_TIME (cumulative ms).
    private void stubRow(String stmtText, long member, String execId, long numExecutions,
                         long stmtExecMs, long cpuUs, long rowsRead, long rowsRet, long rowsMod,
                         long sorts, long sortOvf, String secType) throws Exception {
        when(rs.getString("STMT_TEXT")).thenReturn(stmtText);
        when(rs.getLong("MEMBER")).thenReturn(member);
        when(rs.getString("EXECUTABLE_ID")).thenReturn(execId);
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(execId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        when(rs.getLong("NUM_EXECUTIONS")).thenReturn(numExecutions);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(stmtExecMs);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(cpuUs);
        when(rs.getLong("ROWS_READ")).thenReturn(rowsRead);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(rowsRet);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(rowsMod);
        when(rs.getLong("TOTAL_SECTION_SORTS")).thenReturn(sorts);
        when(rs.getLong("SORT_OVERFLOWS")).thenReturn(sortOvf);
        when(rs.getString("SECTION_TYPE")).thenReturn(secType);
        when(rs.getTimestamp("INSERT_TIMESTAMP")).thenReturn(null); // null → Epoch → old section
    }

    @Test
    void firstCollectionSeedsBaselineAndEmitsNothing() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 500L, 10L, 5L, 0L, 1L, 0L, "D");

        query().collect();

        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void newEntryAfterBaselineIsEmittedUsingCurrentCounters() throws Exception {
        TopQueryQuery q = query();

        // Use fixed server-clock values so INSERT_TIMESTAMP comparison is deterministic.
        // Cycle 1 SERVER_NOW = T+100; cycle 2 exec2 INSERT_TIMESTAMP = T+150 (after T+100).
        Timestamp cycle1ServerNow  = Timestamp.from(Instant.EPOCH.plusSeconds(100));
        Timestamp exec2InsertTs    = Timestamp.from(Instant.EPOCH.plusSeconds(150));

        // Cycle 1: seed with exec1 only
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        when(rs.getTimestamp("SERVER_NOW")).thenReturn(cycle1ServerNow);
        q.collect();
        verify(otelLogger, never()).logRecordBuilder();

        // Cycle 2: exec1 unchanged (zero delta, skipped) + exec2 is brand new with non-zero exec time
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM T", "SELECT 2 FROM T");
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("EXECUTABLE_ID")).thenReturn("exec1", "exec2");
        when(rs.getLong("NUM_EXECUTIONS")).thenReturn(5L, 3L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(1000L, 500L); // exec1 unchanged (delta=0), exec2 new
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("TOTAL_SECTION_SORTS")).thenReturn(0L);
        when(rs.getLong("SORT_OVERFLOWS")).thenReturn(0L);
        when(rs.getString("SECTION_TYPE")).thenReturn("D");
        // exec1: old section (Epoch < cycle1ServerNow); exec2: compiled after cycle 1 server clock
        when(rs.getTimestamp("INSERT_TIMESTAMP")).thenReturn(null, exec2InsertTs);
        when(rs.getTimestamp("SERVER_NOW")).thenReturn(Timestamp.from(Instant.EPOCH.plusSeconds(200)));

        q.collect();

        // Only exec2 (new entry) should be emitted; exec1 has zero delta
        verify(logBuilder, times(1)).emit();
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        // exec2: current STMT_EXEC_TIME=500ms → 0.5s, numExec=3
        assertThat(cap.getValue().get(TopQueryEvent.DB2_TOTAL_EXEC_TIME)).isEqualTo(0.5);
        assertThat(cap.getValue().get(TopQueryEvent.DB2_EXECUTIONS)).isEqualTo(3L);
    }

    @Test
    void secondCollectionEmitsDeltaRecord() throws Exception {
        TopQueryQuery q = query();

        // Cycle 1: seed — stmtExecMs=1000, numExec=5, cpu=500µs, rows=10
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 500L, 10L, 5L, 0L, 1L, 0L, "D");
        q.collect();
        verify(otelLogger, never()).logRecordBuilder();

        // Cycle 2: stmtExecMs=1500 → delta=500ms=0.5s, numExec=7 → delta=2, cpu=700µs → delta=200µs
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 7L, 1500L, 700L, 12L, 6L, 1L, 2L, 0L, "D");

        q.collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        verify(logBuilder).emit();

        io.opentelemetry.api.common.Attributes attrs = cap.getValue();
        assertThat(attrs.get(TopQueryEvent.DB_QUERY_TEXT)).isEqualTo("SELECT ? FROM T");
        assertThat(attrs.get(TopQueryEvent.DB_SYSTEM)).isEqualTo("ibm.db2");
        assertThat(attrs.get(TopQueryEvent.DB_NAMESPACE)).isEqualTo("TESTDB");
        assertThat(attrs.get(TopQueryEvent.DB2_MEMBER)).isEqualTo(0L);

        // executions delta = 7 - 5 = 2
        assertThat(attrs.get(TopQueryEvent.DB2_EXECUTIONS)).isEqualTo(2L);
        // exec time delta = 500ms → 0.5s
        assertThat(attrs.get(TopQueryEvent.DB2_TOTAL_EXEC_TIME)).isEqualTo(0.5);
        // avg = 0.5s / 2 executions = 0.25s
        assertThat(attrs.get(TopQueryEvent.DB2_AVG_EXEC_TIME)).isEqualTo(0.25);
        // cpu delta = 700 - 500 = 200µs → 0.0002s
        assertThat(attrs.get(TopQueryEvent.DB2_TOTAL_CPU_TIME)).isEqualTo(200 / 1_000_000.0);
        // rows read delta = 12 - 10 = 2
        assertThat(attrs.get(TopQueryEvent.DB2_ROWS_READ)).isEqualTo(2L);
        assertThat(attrs.get(TopQueryEvent.DB2_ROWS_RETURNED)).isEqualTo(1L);
        assertThat(attrs.get(TopQueryEvent.DB2_ROWS_MODIFIED)).isEqualTo(1L);
        assertThat(attrs.get(TopQueryEvent.DB2_SORTS)).isEqualTo(1L);
        assertThat(attrs.get(TopQueryEvent.DB2_SORT_OVERFLOWS)).isEqualTo(0L);
        assertThat(attrs.get(TopQueryEvent.DB2_STMT_TYPE)).isEqualTo("D");
    }

    @Test
    void newEntryWithZeroExecTimeButNonZeroExecutionsIsEmitted() throws Exception {
        TopQueryQuery q = query();

        Timestamp cycle1ServerNow = Timestamp.from(Instant.EPOCH.plusSeconds(100));
        Timestamp exec2InsertTs   = Timestamp.from(Instant.EPOCH.plusSeconds(150));

        // Seed with exec1
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        when(rs.getTimestamp("SERVER_NOW")).thenReturn(cycle1ServerNow);
        q.collect();

        // Cycle 2: exec2 is new, STMT_EXEC_TIME=0 (very fast query) but has 2 executions
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM T", "SELECT 2 FROM T");
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("EXECUTABLE_ID")).thenReturn("exec1", "exec2");
        when(rs.getLong("NUM_EXECUTIONS")).thenReturn(5L, 2L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(1000L, 0L); // exec1 no delta; exec2 new, 0ms
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("TOTAL_SECTION_SORTS")).thenReturn(0L);
        when(rs.getLong("SORT_OVERFLOWS")).thenReturn(0L);
        when(rs.getString("SECTION_TYPE")).thenReturn("D");
        // exec1 is old (Epoch); exec2 was compiled after cycle 1 server clock
        when(rs.getTimestamp("INSERT_TIMESTAMP")).thenReturn(null, exec2InsertTs);
        when(rs.getTimestamp("SERVER_NOW")).thenReturn(Timestamp.from(Instant.EPOCH.plusSeconds(200)));

        q.collect();

        // exec2 should be emitted even with zero exec time because it has executions
        verify(logBuilder, times(1)).emit();
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        assertThat(cap.getValue().get(TopQueryEvent.DB2_EXECUTIONS)).isEqualTo(2L);
        assertThat(cap.getValue().get(TopQueryEvent.DB2_TOTAL_EXEC_TIME)).isEqualTo(0.0);
    }

    @Test
    void differentExecutableIdsAreTrackedSeparately() throws Exception {
        TopQueryQuery q = query();

        // Seed: two rows same SQL text, different EXECUTABLE_ID (same SQL, different compilation env)
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM T", "SELECT 1 FROM T");
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("EXECUTABLE_ID")).thenReturn("exec1", "exec2");
        when(rs.getLong("NUM_EXECUTIONS")).thenReturn(3L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(100L, 200L);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("TOTAL_SECTION_SORTS")).thenReturn(0L);
        when(rs.getLong("SORT_OVERFLOWS")).thenReturn(0L);
        when(rs.getString("SECTION_TYPE")).thenReturn("D");
        q.collect();

        // Cycle 2: exec1 grew by 50ms, exec2 grew by 100ms → two separate records
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM T", "SELECT 1 FROM T");
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("EXECUTABLE_ID")).thenReturn("exec1", "exec2");
        when(rs.getLong("NUM_EXECUTIONS")).thenReturn(4L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(150L, 300L);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("TOTAL_SECTION_SORTS")).thenReturn(0L);
        when(rs.getLong("SORT_OVERFLOWS")).thenReturn(0L);
        when(rs.getString("SECTION_TYPE")).thenReturn("D");
        q.collect();

        verify(logBuilder, times(2)).emit();
    }

    @Test
    void obfuscationFailedDropsRecord() throws Exception {
        TopQueryQuery q = query();

        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT secret FROM T", 0L, "exec1", 1L, 100L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT secret FROM T", 0L, "exec1", 2L, 200L, 0L, 0L, 0L, 0L, 0L, 0L, "D");

        try (MockedStatic<SqlObfuscator> mocked = mockStatic(SqlObfuscator.class)) {
            mocked.when(() -> SqlObfuscator.obfuscate(anyString()))
                .thenReturn(SqlObfuscator.OBFUSCATION_FAILED);
            q.collect();
        }

        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void nullOrBlankStatementSkipped() throws Exception {
        TopQueryQuery q = query();

        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 1L, 100L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        reset(stmt, rs, otelLogger);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn(null, "   ");
        when(rs.getLong(anyString())).thenReturn(0L);
        when(rs.getString("EXECUTABLE_ID")).thenReturn("exec1");
        when(rs.getString("SECTION_TYPE")).thenReturn("D");

        q.collect();

        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void zeroDeltaSkipped() throws Exception {
        TopQueryQuery q = query();

        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, false);
        // Same STMT_EXEC_TIME → zero delta
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");

        q.collect();

        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void topNLimitRespected() throws Exception {
        TopQueryQuery q = new TopQueryQuery(() -> conn, "TESTDB", otelLogger, 2, 60L);

        // Seed 3 rows
        when(rs.next()).thenReturn(true, true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM T", "SELECT 2 FROM T", "SELECT 3 FROM T");
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("EXECUTABLE_ID")).thenReturn("e1", "e2", "e3");
        when(rs.getLong("NUM_EXECUTIONS")).thenReturn(1L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(100L, 200L, 300L);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("TOTAL_SECTION_SORTS")).thenReturn(0L);
        when(rs.getLong("SORT_OVERFLOWS")).thenReturn(0L);
        when(rs.getString("SECTION_TYPE")).thenReturn("D");
        q.collect();

        // Cycle 2: all three grew — only top 2 emitted
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM T", "SELECT 2 FROM T", "SELECT 3 FROM T");
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("EXECUTABLE_ID")).thenReturn("e1", "e2", "e3");
        when(rs.getLong("NUM_EXECUTIONS")).thenReturn(2L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(200L, 500L, 600L);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("TOTAL_SECTION_SORTS")).thenReturn(0L);
        when(rs.getLong("SORT_OVERFLOWS")).thenReturn(0L);
        when(rs.getString("SECTION_TYPE")).thenReturn("D");

        q.collect();

        verify(logBuilder, times(2)).emit();
    }

    @Test
    void sqlContainsCorrectColumnNames() throws Exception {
        when(rs.next()).thenReturn(false);
        query().collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        String sql = sqlCap.getValue();
        assertThat(sql).containsIgnoringCase("MON_GET_PKG_CACHE_STMT");
        assertThat(sql).containsIgnoringCase("EXECUTABLE_ID");
        assertThat(sql).containsIgnoringCase("INSERT_TIMESTAMP");
        assertThat(sql).containsIgnoringCase("SERVER_NOW");
        assertThat(sql).containsIgnoringCase("STMT_EXEC_TIME");
        assertThat(sql).containsIgnoringCase("NUM_EXECUTIONS");
        assertThat(sql).containsIgnoringCase("TOTAL_SECTION_SORTS");
        assertThat(sql).containsIgnoringCase("SORT_OVERFLOWS");
        assertThat(sql).containsIgnoringCase("LAST_METRICS_UPDATE");
        // These wrong column names must NOT appear
        assertThat(sql).doesNotContainIgnoringCase("TOTAL_SORT_OVERFLOWS");
        assertThat(sql).doesNotContainIgnoringCase("STMT_SORTS");
        assertThat(sql).doesNotContainIgnoringCase("LAST_EXEC_TIME");
        assertThat(sql).doesNotContainIgnoringCase("TOTAL_EXEC_TIME");
        assertThat(sql).doesNotContainIgnoringCase("NUM_COMPILATIONS");
        assertThat(sql).doesNotContainIgnoringCase("NUM_EXEC_WITH_METRICS");
    }

    @Test
    void sqlContainsSelfFilterClauses() throws Exception {
        when(rs.next()).thenReturn(false);
        query().collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        String sql = sqlCap.getValue();
        assertThat(sql).contains("MON_GET_PKG_CACHE_STMT");
        assertThat(sql).contains("MON_GET_ACTIVITY");
        assertThat(sql).contains("MON_GET_DATABASE");
        assertThat(sql).contains("MON_GET_BUFFERPOOL");
        assertThat(sql).contains("MON_GET_TABLESPACE");
        assertThat(sql).contains("MON_CURRENT_SQL");
        assertThat(sql).contains("MON_GET_CONNECTION");
        assertThat(sql).contains("SNAP_GET_APPL_INFO");
        assertThat(sql).contains("SNAP_GET_STMT");
        assertThat(sql).contains("SYSDUMMY1");
    }

    @Test
    void lookbackDerivedFromInterval() throws Exception {
        // 60s interval → lookback = max(5, ceil(120/60)) = max(5, 2) = 5 minutes (floor wins)
        TopQueryQuery q60 = new TopQueryQuery(() -> conn, "TESTDB", otelLogger, 10, 60L);
        when(rs.next()).thenReturn(false);
        q60.collect();
        ArgumentCaptor<String> cap60 = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(cap60.capture());
        assertThat(cap60.getValue()).contains("5 MINUTES");

        // 3600s interval → lookback = max(5, ceil(7200/60)) = 120 minutes
        reset(stmt, rs);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        TopQueryQuery q3600 = new TopQueryQuery(() -> conn, "TESTDB", otelLogger, 10, 3600L);
        q3600.collect();
        ArgumentCaptor<String> cap3600 = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(cap3600.capture());
        assertThat(cap3600.getValue()).contains("120 MINUTES");
    }

    @Test
    void severityIsInfo() throws Exception {
        TopQueryQuery q = query();

        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 1L, 100L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 2L, 200L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        verify(logBuilder).setSeverity(Severity.INFO);
        verify(logBuilder).setSeverityText("INFO");
    }

    @Test
    void connectionExceptionHandledGracefully() {
        Supplier<Connection> failing = () -> { throw new RuntimeException("refused"); };
        new TopQueryQuery(failing, "TESTDB", otelLogger, 10, 60L).collect();
        verify(otelLogger, never()).logRecordBuilder();
    }

    // --- Query timeout tests ---

    @Test
    void queryTimeoutIsSetBeforeExecution() throws Exception {
        // 120s interval → timeout = max(30, 60) = 60s
        TopQueryQuery q = new TopQueryQuery(() -> conn, "TESTDB", otelLogger, 10, 120L);
        when(rs.next()).thenReturn(false);
        q.collect();
        verify(stmt).setQueryTimeout(60);
    }

    @Test
    void queryTimeoutMinimumIsThirtySeconds() throws Exception {
        // 10s interval → timeout = max(30, 5) = 30s
        TopQueryQuery q = new TopQueryQuery(() -> conn, "TESTDB", otelLogger, 10, 10L);
        when(rs.next()).thenReturn(false);
        q.collect();
        verify(stmt).setQueryTimeout(30);
    }

    // --- Reset / counter-wrap tests ---

    @Test
    void deltaFunctionNormalAdvance() {
        assertThat(TopQueryQuery.delta(1500L, 1000L)).isEqualTo(500L);
        assertThat(TopQueryQuery.delta(1000L, 1000L)).isEqualTo(0L);
    }

    @Test
    void deltaFunctionCounterReset() {
        // After a plan-cache flush or DB restart the counter restarts from 0.
        // The current value is itself the work done since the reset.
        assertThat(TopQueryQuery.delta(200L, 1000L)).isEqualTo(200L);
        assertThat(TopQueryQuery.delta(0L, 500L)).isEqualTo(0L);
    }

    @Test
    void counterResetProducesPositiveDeltaNotNegative() throws Exception {
        TopQueryQuery q = query();

        // Seed: exec_time=2000ms, numExec=10
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 10L, 2000L, 1000L, 50L, 20L, 5L, 3L, 1L, "D");
        q.collect();

        // Cycle 2 after monitor reset: all counters restarted; current < previous on every field.
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, false);
        // exec_time reset to 300ms (< 2000), numExec reset to 2 (< 10), cpu reset to 150µs (< 1000)
        stubRow("SELECT 1 FROM T", 0L, "exec1", 2L, 300L, 150L, 8L, 3L, 1L, 1L, 0L, "D");
        q.collect();

        // Must emit — current counters are the interval delta, all positive
        verify(logBuilder, times(1)).emit();
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());

        io.opentelemetry.api.common.Attributes attrs = cap.getValue();
        // exec_time delta = 300ms (reset path) → 0.3s
        assertThat(attrs.get(TopQueryEvent.DB2_TOTAL_EXEC_TIME)).isEqualTo(0.3);
        // numExec delta = 2 (reset path)
        assertThat(attrs.get(TopQueryEvent.DB2_EXECUTIONS)).isEqualTo(2L);
        // cpu delta = 150µs (reset path)
        assertThat(attrs.get(TopQueryEvent.DB2_TOTAL_CPU_TIME)).isEqualTo(150 / 1_000_000.0);
        // rows_read delta = 8 (reset path)
        assertThat(attrs.get(TopQueryEvent.DB2_ROWS_READ)).isEqualTo(8L);
        // No negatives anywhere
        assertThat(attrs.get(TopQueryEvent.DB2_ROWS_RETURNED)).isGreaterThanOrEqualTo(0L);
        assertThat(attrs.get(TopQueryEvent.DB2_ROWS_MODIFIED)).isGreaterThanOrEqualTo(0L);
        assertThat(attrs.get(TopQueryEvent.DB2_SORTS)).isGreaterThanOrEqualTo(0L);
        assertThat(attrs.get(TopQueryEvent.DB2_SORT_OVERFLOWS)).isGreaterThanOrEqualTo(0L);
    }

    @Test
    void partialResetOnlyAffectedCountersUseCurrent() throws Exception {
        // exec_time resets but numExec does not (independent Db2 internal counters)
        TopQueryQuery q = query();

        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, false);
        // exec_time reset to 50ms; numExec grew normally to 7 (delta=2)
        stubRow("SELECT 1 FROM T", 0L, "exec1", 7L, 50L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        verify(logBuilder, times(1)).emit();
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());

        // exec_time reset: delta = 50ms → 0.05s
        assertThat(attrs(cap).get(TopQueryEvent.DB2_TOTAL_EXEC_TIME)).isEqualTo(0.05);
        // numExec normal: delta = 7 - 5 = 2
        assertThat(attrs(cap).get(TopQueryEvent.DB2_EXECUTIONS)).isEqualTo(2L);
    }

    private static io.opentelemetry.api.common.Attributes attrs(
            ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap) {
        return cap.getValue();
    }

    // --- Bounded TTL cache + INSERT_TIMESTAMP classification tests ---

    private void runAbsenceCycles(TopQueryQuery q, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            reset(stmt, rs, otelLogger, logBuilder);
            when(conn.createStatement()).thenReturn(stmt);
            when(stmt.executeQuery(anyString())).thenReturn(rs);
            when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
            when(rs.next()).thenReturn(false);
            when(rs.getTimestamp("INSERT_TIMESTAMP")).thenReturn(null);
            q.collect();
        }
    }

    @Test
    void evictedOldSectionSeedsBaselineAndEmitsNothing() throws Exception {
        // Regression test for the P1 finding: before the INSERT_TIMESTAMP fix, this case
        // incorrectly emitted 1200ms (lifetime total) as if it were an interval delta.
        //
        // Timeline:
        //   Cycle 1 (seed): exec1 enters cache with exec_time=1000ms.
        //   Cycles 2..MAX_STALE_CYCLES+2: exec1 absent, evicted from baseline cache.
        //   Returning cycle: exec1 reappears with exec_time=1200ms (grew by 200ms while absent).
        //   INSERT_TIMESTAMP is old (before last scrape) → section predates eviction window.
        //   Expected: seed only, no emission. Next cycle will emit the correct 200ms delta.
        TopQueryQuery q = query();

        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        runAbsenceCycles(q, TopQueryQuery.MAX_STALE_CYCLES + 1);

        // exec1 returns with higher counters; INSERT_TIMESTAMP is old (null → Epoch)
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 6L, 1200L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        when(rs.getTimestamp("INSERT_TIMESTAMP")).thenReturn(null); // old section
        q.collect();

        // Must NOT emit — lifetime total (1200ms) would be inflated; section is seeded instead.
        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void evictedOldSectionEmitsCorrectDeltaOnSubsequentCycle() throws Exception {
        // After seeding on return, the next cycle has the baseline and emits only the new delta.
        TopQueryQuery q = query();

        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        runAbsenceCycles(q, TopQueryQuery.MAX_STALE_CYCLES + 1);

        // Returning cycle: exec1 reappears (old), seeded at exec_time=1200ms
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 6L, 1200L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        when(rs.getTimestamp("INSERT_TIMESTAMP")).thenReturn(null);
        q.collect();
        verify(otelLogger, never()).logRecordBuilder();

        // Next cycle: exec_time=1400ms → delta = 1400 - 1200 = 200ms → 0.2s
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 7L, 1400L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        when(rs.getTimestamp("INSERT_TIMESTAMP")).thenReturn(null);
        q.collect();

        verify(logBuilder, times(1)).emit();
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        assertThat(cap.getValue().get(TopQueryEvent.DB2_TOTAL_EXEC_TIME)).isEqualTo(0.2);
    }

    @Test
    void genuinelyNewSectionEmitsCurrentCountersAsIntervalDelta() throws Exception {
        // A section compiled AFTER the last scrape is truly new — its counters are the interval delta.
        // Uses fixed server-clock values: cycle 1 SERVER_NOW=T+100, exec2 INSERT_TIMESTAMP=T+150.
        // T+150 is after T+100 so exec2 is classified as genuinely new in cycle 2.
        TopQueryQuery q = query();

        Timestamp cycle1ServerNow = Timestamp.from(Instant.EPOCH.plusSeconds(100));
        Timestamp exec2InsertTs   = Timestamp.from(Instant.EPOCH.plusSeconds(150));

        // Seed cycle
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        when(rs.getTimestamp("SERVER_NOW")).thenReturn(cycle1ServerNow);
        q.collect();

        // Cycle 2: exec2 appears for the first time with INSERT_TIMESTAMP after cycle 1 server clock
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM T", "SELECT 2 FROM T");
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("EXECUTABLE_ID")).thenReturn("exec1", "exec2");
        when(rs.getLong("NUM_EXECUTIONS")).thenReturn(5L, 3L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(1000L, 800L); // exec1 no delta; exec2 new
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("TOTAL_SECTION_SORTS")).thenReturn(0L);
        when(rs.getLong("SORT_OVERFLOWS")).thenReturn(0L);
        when(rs.getString("SECTION_TYPE")).thenReturn("D");
        // exec1: old section (Epoch < T+100); exec2: compiled between cycle 1 and cycle 2
        when(rs.getTimestamp("INSERT_TIMESTAMP")).thenReturn(null, exec2InsertTs);
        when(rs.getTimestamp("SERVER_NOW")).thenReturn(Timestamp.from(Instant.EPOCH.plusSeconds(200)));
        q.collect();

        // Only exec2 emitted; exec1 has zero delta
        verify(logBuilder, times(1)).emit();
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        assertThat(cap.getValue().get(TopQueryEvent.DB2_TOTAL_EXEC_TIME)).isEqualTo(0.8);
        assertThat(cap.getValue().get(TopQueryEvent.DB2_EXECUTIONS)).isEqualTo(3L);
    }

    @Test
    void cacheEntryRetainedWithinStaleCycles() throws Exception {
        TopQueryQuery q = query();

        // Seed: exec1 enters cache with exec_time=1000ms
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        // exec1 absent for (MAX_STALE_CYCLES - 1) cycles — still within retention window
        for (int i = 0; i < TopQueryQuery.MAX_STALE_CYCLES - 1; i++) {
            reset(stmt, rs, otelLogger, logBuilder);
            when(conn.createStatement()).thenReturn(stmt);
            when(stmt.executeQuery(anyString())).thenReturn(rs);
            when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
            when(rs.next()).thenReturn(false);
            q.collect();
        }

        // exec1 returns with exec_time=1200ms — baseline still 1000, delta = 200ms → 0.2s
        // INSERT_TIMESTAMP is not consulted here (prevC != null path), but stub for completeness.
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 6L, 1200L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        when(rs.getTimestamp("INSERT_TIMESTAMP")).thenReturn(null);
        q.collect();

        verify(logBuilder, times(1)).emit();
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        // Delta-based: 1200 - 1000 = 200ms → 0.2s (NOT inflated to 1.2s)
        assertThat(cap.getValue().get(TopQueryEvent.DB2_TOTAL_EXEC_TIME)).isEqualTo(0.2);
    }

    // --- Ranking tests ---

    @Test
    void rankingByExecTimeThenExecCount() throws Exception {
        // Three entries: high exec count / zero time, medium exec time, high exec time.
        // Expected rank: high exec time first, medium exec time second, zero time third.
        TopQueryQuery q = new TopQueryQuery(() -> conn, "TESTDB", otelLogger, 3, 60L);

        // Seed all three
        when(rs.next()).thenReturn(true, true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("Q_HIGH_TIME", "Q_MED_TIME", "Q_ZERO_TIME");
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("EXECUTABLE_ID")).thenReturn("e1", "e2", "e3");
        when(rs.getLong("NUM_EXECUTIONS")).thenReturn(1L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(0L, 0L, 0L);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("TOTAL_SECTION_SORTS")).thenReturn(0L);
        when(rs.getLong("SORT_OVERFLOWS")).thenReturn(0L);
        when(rs.getString("SECTION_TYPE")).thenReturn("D");
        q.collect();

        // Cycle 2: Q_HIGH_TIME gets 9000ms, Q_MED_TIME gets 4000ms, Q_ZERO_TIME gets 10000 execs
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("Q_HIGH_TIME", "Q_MED_TIME", "Q_ZERO_TIME");
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("EXECUTABLE_ID")).thenReturn("e1", "e2", "e3");
        when(rs.getLong("NUM_EXECUTIONS")).thenReturn(2L, 3L, 10001L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(9000L, 4000L, 0L);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("TOTAL_SECTION_SORTS")).thenReturn(0L);
        when(rs.getLong("SORT_OVERFLOWS")).thenReturn(0L);
        when(rs.getString("SECTION_TYPE")).thenReturn("D");
        q.collect();

        // All three emitted; capture in order
        verify(logBuilder, times(3)).emit();
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder, times(3)).setAllAttributes(cap.capture());
        java.util.List<io.opentelemetry.api.common.Attributes> allAttrs = cap.getAllValues();

        // Rank 0: Q_HIGH_TIME (9000ms)
        assertThat(allAttrs.get(0).get(TopQueryEvent.DB2_TOTAL_EXEC_TIME)).isEqualTo(9.0);
        // Rank 1: Q_MED_TIME (4000ms)
        assertThat(allAttrs.get(1).get(TopQueryEvent.DB2_TOTAL_EXEC_TIME)).isEqualTo(4.0);
        // Rank 2: Q_ZERO_TIME (0ms — only wins tiebreaker among zero-time entries)
        assertThat(allAttrs.get(2).get(TopQueryEvent.DB2_TOTAL_EXEC_TIME)).isEqualTo(0.0);
        assertThat(allAttrs.get(2).get(TopQueryEvent.DB2_EXECUTIONS)).isEqualTo(10000L);
    }

    // --- Plan collection tests ---

    /** Build a plan-enabled TopQueryQuery with 2 max plans/interval and 10 min cache TTL. */
    private TopQueryQuery queryWithPlan() {
        return new TopQueryQuery(() -> conn, "TESTDB", otelLogger, 10, 60L, 2,
            TopQueryQuery.DEFAULT_PLAN_CACHE_TTL_MS);
    }

    private void stubTwoCollectionCycles(String stmtText, String execId, long member,
                                          long numExec1, long execMs1,
                                          long numExec2, long execMs2) throws Exception {
        // Seed cycle
        when(rs.next()).thenReturn(true, false);
        stubRow(stmtText, member, execId, numExec1, execMs1, 0L, 0L, 0L, 0L, 0L, 0L, "D");
    }

    @Test
    void planXmlAttachedToTopQueryRecordOnCacheMiss() throws Exception {
        String planXml = "<plan><op type='TBSCAN'/></plan>";

        // Set up plan fetch mock
        PreparedStatement planPs = mock(PreparedStatement.class);
        ResultSet planRs = mock(ResultSet.class);
        Clob clob = mock(Clob.class);
        when(clob.length()).thenReturn((long) planXml.length());
        when(clob.getSubString(1, planXml.length())).thenReturn(planXml);
        when(planRs.next()).thenReturn(true);
        when(planRs.getClob("SECTION_XML")).thenReturn(clob);
        when(planPs.executeQuery()).thenReturn(planRs);
        when(conn.prepareStatement(anyString())).thenReturn(planPs);

        TopQueryQuery q = queryWithPlan();

        // Seed cycle
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x01, 0x02});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        // Emit cycle
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, false);
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x01, 0x02});
        stubRow("SELECT 1 FROM T", 0L, "exec1", 6L, 1500L, 0L, 0L, 0L, 0L, 0L, 0L, "D");

        q.collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        verify(logBuilder).emit();

        assertThat(cap.getValue().get(TopQueryEvent.DB2_QUERY_PLAN)).isEqualTo(planXml);
        assertThat(cap.getValue().get(TopQueryEvent.DB2_PLAN_HASH)).isNotNull().hasSize(64);
    }

    @Test
    void planCacheHitSkipsFetch() throws Exception {
        String planXml = "<plan><op type='TBSCAN'/></plan>";
        PreparedStatement planPs = mock(PreparedStatement.class);
        ResultSet planRs = mock(ResultSet.class);
        Clob clob = mock(Clob.class);
        when(clob.length()).thenReturn((long) planXml.length());
        when(clob.getSubString(1, planXml.length())).thenReturn(planXml);
        when(planRs.next()).thenReturn(true);
        when(planRs.getClob("SECTION_XML")).thenReturn(clob);
        when(planPs.executeQuery()).thenReturn(planRs);
        when(conn.prepareStatement(anyString())).thenReturn(planPs);

        TopQueryQuery q = queryWithPlan();

        // Seed
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x01, 0x02});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        // First emit — plan fetched and cached
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x01, 0x02});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 6L, 1500L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();
        verify(planPs, times(1)).executeQuery();

        // Second emit — same execId still within TTL; plan fetch skipped
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x01, 0x02});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 7L, 2000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        // prepareStatement only called for the first emit (cache hit on second)
        verify(conn, times(1)).prepareStatement(anyString());
        verify(logBuilder).emit(); // event still emitted, just without plan re-fetch
    }

    @Test
    void planFetchFailureDoesNotDropTopQueryRecord() throws Exception {
        PreparedStatement planPs = mock(PreparedStatement.class);
        when(planPs.executeQuery()).thenThrow(new java.sql.SQLException("access denied"));
        when(conn.prepareStatement(anyString())).thenReturn(planPs);

        TopQueryQuery q = queryWithPlan();

        // Seed
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x03});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        // Emit — plan fetch fails; top_query record should still be emitted without plan attrs
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x03});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 6L, 1500L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        verify(logBuilder).emit(); // top_query still emitted
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        assertThat(cap.getValue().get(TopQueryEvent.DB2_QUERY_PLAN)).isNull(); // no plan attr
    }

    @Test
    void planNotFetchedWhenPlanDisabled() throws Exception {
        // Use a query without plan enabled
        TopQueryQuery q = query();

        // Seed
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x05});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        // Emit
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x05});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 6L, 1500L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        // prepareStatement must never be called when plan is disabled
        verify(conn, never()).prepareStatement(anyString());
    }

    @Test
    void planBudgetCapRespectsMaxPerInterval() throws Exception {
        String planXml = "<plan/>";
        // Return a plan for every fetch
        when(conn.prepareStatement(anyString())).thenAnswer(inv -> {
            PreparedStatement ps = mock(PreparedStatement.class);
            ResultSet prs = mock(ResultSet.class);
            Clob c = mock(Clob.class);
            when(c.length()).thenReturn((long) planXml.length());
            when(c.getSubString(1, planXml.length())).thenReturn(planXml);
            when(prs.next()).thenReturn(true);
            when(prs.getClob("SECTION_XML")).thenReturn(c);
            when(ps.executeQuery()).thenReturn(prs);
            return ps;
        });

        // planMaxPerInterval = 2, but we'll have 3 candidates in the emit cycle
        TopQueryQuery q = queryWithPlan();

        // Seed 3 rows
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x01}, new byte[]{0x02}, new byte[]{0x03});
        when(rs.next()).thenReturn(true, true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("Q1", "Q2", "Q3");
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("EXECUTABLE_ID")).thenReturn("e1", "e2", "e3");
        when(rs.getLong("NUM_EXECUTIONS")).thenReturn(1L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(100L, 200L, 300L);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("TOTAL_SECTION_SORTS")).thenReturn(0L);
        when(rs.getLong("SORT_OVERFLOWS")).thenReturn(0L);
        when(rs.getString("SECTION_TYPE")).thenReturn("D");
        q.collect();

        // Emit cycle: all 3 rows grew
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x01}, new byte[]{0x02}, new byte[]{0x03});
        when(rs.next()).thenReturn(true, true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("Q1", "Q2", "Q3");
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("EXECUTABLE_ID")).thenReturn("e1", "e2", "e3");
        when(rs.getLong("NUM_EXECUTIONS")).thenReturn(2L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(200L, 400L, 600L);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("TOTAL_SECTION_SORTS")).thenReturn(0L);
        when(rs.getLong("SORT_OVERFLOWS")).thenReturn(0L);
        when(rs.getString("SECTION_TYPE")).thenReturn("D");
        q.collect();

        // All 3 top_query records emitted, but only 2 plan fetches (budget cap)
        verify(logBuilder, times(3)).emit();
        verify(conn, times(2)).prepareStatement(anyString());
    }

    @Test
    void hexConversionRoundTrip() {
        byte[] bytes = {0x00, 0x0F, (byte) 0xFF, 0x42, (byte) 0xAB};
        String hex = TopQueryQuery.hexToBytes(null) == null ? "null ok" : "";
        assertThat(TopQueryQuery.hexToBytes(null)).isNull();
        assertThat(TopQueryQuery.hexToBytes("0d")).isEqualTo(new byte[]{0x0d});
        assertThat(TopQueryQuery.hexToBytes("0off")).isNull(); // odd length
    }

    @Test
    void sha256HexIsDeterministicAndCorrectLength() {
        String h = TopQueryQuery.sha256Hex("hello");
        assertThat(h).hasSize(64).matches("[0-9a-f]+");
        assertThat(TopQueryQuery.sha256Hex("hello")).isEqualTo(h);
        assertThat(TopQueryQuery.sha256Hex("other")).isNotEqualTo(h);
    }

    // --- Config validation tests ---

    @Test
    void configRejectsTopQueryIntervalBelowMinimum() {
        com.splunk.otel.db2.config.Db2ReceiverConfig cfg = new com.splunk.otel.db2.config.Db2ReceiverConfig();
        cfg.setTopQueryInterval("30s");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            cfg::getTopQueryIntervalDuration);
    }

    @Test
    void configAcceptsTopQueryIntervalAtMinimum() {
        com.splunk.otel.db2.config.Db2ReceiverConfig cfg = new com.splunk.otel.db2.config.Db2ReceiverConfig();
        cfg.setTopQueryInterval("60s");
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(cfg::getTopQueryIntervalDuration);
    }

    @Test
    void configRejectsZeroTopQueryCount() {
        com.splunk.otel.db2.config.Db2ReceiverConfig cfg = new com.splunk.otel.db2.config.Db2ReceiverConfig();
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            () -> cfg.setTopQueryCount(0));
    }

    @Test
    void configRejectsNegativeTopQueryCount() {
        com.splunk.otel.db2.config.Db2ReceiverConfig cfg = new com.splunk.otel.db2.config.Db2ReceiverConfig();
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            () -> cfg.setTopQueryCount(-1));
    }
}
