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
import java.sql.ResultSet;
import java.sql.Statement;
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
    }

    private TopQueryQuery query() {
        return new TopQueryQuery(() -> conn, "TESTDB", otelLogger, 10, 60L);
    }

    // Stub a single result row. stmtExecMs is STMT_EXEC_TIME (cumulative ms).
    private void stubRow(String stmtText, long member, String execId, long numExecWithMetrics,
                         long stmtExecMs, long cpuUs, long rowsRead, long rowsRet, long rowsMod,
                         long sorts, long sortOvf, String secType) throws Exception {
        when(rs.getString("STMT_TEXT")).thenReturn(stmtText);
        when(rs.getLong("MEMBER")).thenReturn(member);
        when(rs.getString("EXECUTABLE_ID")).thenReturn(execId);
        when(rs.getLong("NUM_EXEC_WITH_METRICS")).thenReturn(numExecWithMetrics);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(stmtExecMs);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(cpuUs);
        when(rs.getLong("ROWS_READ")).thenReturn(rowsRead);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(rowsRet);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(rowsMod);
        when(rs.getLong("STMT_SORTS")).thenReturn(sorts);
        when(rs.getLong("SORT_OVERFLOWS")).thenReturn(sortOvf);
        when(rs.getString("SECTION_TYPE")).thenReturn(secType);
    }

    @Test
    void firstCollectionSeedsBaselineAndEmitsNothing() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 500L, 10L, 5L, 0L, 1L, 0L, "D");

        query().collect();

        verify(otelLogger, never()).logRecordBuilder();
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
    void differentExecutableIdsAreTrackedSeparately() throws Exception {
        TopQueryQuery q = query();

        // Seed: two rows same SQL text, different EXECUTABLE_ID (same SQL, different compilation env)
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM T", "SELECT 1 FROM T");
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("EXECUTABLE_ID")).thenReturn("exec1", "exec2");
        when(rs.getLong("NUM_EXEC_WITH_METRICS")).thenReturn(3L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(100L, 200L);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("STMT_SORTS")).thenReturn(0L);
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
        when(rs.getLong("NUM_EXEC_WITH_METRICS")).thenReturn(4L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(150L, 300L);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("STMT_SORTS")).thenReturn(0L);
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
        when(rs.getLong("NUM_EXEC_WITH_METRICS")).thenReturn(1L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(100L, 200L, 300L);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("STMT_SORTS")).thenReturn(0L);
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
        when(rs.getLong("NUM_EXEC_WITH_METRICS")).thenReturn(2L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(200L, 500L, 600L);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("STMT_SORTS")).thenReturn(0L);
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
        assertThat(sql).containsIgnoringCase("STMT_EXEC_TIME");
        assertThat(sql).containsIgnoringCase("NUM_EXEC_WITH_METRICS");
        assertThat(sql).containsIgnoringCase("STMT_SORTS");
        assertThat(sql).containsIgnoringCase("SORT_OVERFLOWS");
        assertThat(sql).containsIgnoringCase("LAST_METRICS_UPDATE");
        // These wrong column names must NOT appear
        assertThat(sql).doesNotContainIgnoringCase("TOTAL_SORT_OVERFLOWS");
        assertThat(sql).doesNotContainIgnoringCase("TOTAL_SECTION_SORTS");
        assertThat(sql).doesNotContainIgnoringCase("LAST_EXEC_TIME");
        assertThat(sql).doesNotContainIgnoringCase("TOTAL_EXEC_TIME");
        assertThat(sql).doesNotContainIgnoringCase("NUM_COMPILATIONS");
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
