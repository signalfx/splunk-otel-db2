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
        return new TopQueryQuery(() -> conn, "TESTDB", otelLogger, 10);
    }

    // Stub a single result row. stmtExecMs is STMT_EXEC_TIME (cumulative ms).
    private void stubRow(String stmtText, long member, long numExec, long stmtExecMs,
                         long cpuUs, long rowsRead, long rowsRet, long rowsMod,
                         long sorts, long sortOvf, String secType) throws Exception {
        when(rs.getString("STMT_TEXT")).thenReturn(stmtText);
        when(rs.getLong("MEMBER")).thenReturn(member);
        when(rs.getLong("NUM_EXECUTIONS")).thenReturn(numExec);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(stmtExecMs);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(cpuUs);
        when(rs.getLong("ROWS_READ")).thenReturn(rowsRead);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(rowsRet);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(rowsMod);
        when(rs.getLong("TOTAL_SECTION_SORTS")).thenReturn(sorts);
        when(rs.getLong("TOTAL_SORT_OVERFLOWS")).thenReturn(sortOvf);
        when(rs.getString("SECTION_TYPE")).thenReturn(secType);
    }

    @Test
    void firstCollectionSeedsBaselineAndEmitsNothing() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, 5L, 1000L, 500L, 10L, 5L, 0L, 1L, 0L, "D");

        query().collect();

        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void secondCollectionEmitsDeltaRecord() throws Exception {
        TopQueryQuery q = query();

        // First cycle: seed baseline with STMT_EXEC_TIME = 1000 ms
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, 5L, 1000L, 500L, 10L, 5L, 0L, 1L, 0L, "D");
        q.collect();
        verify(otelLogger, never()).logRecordBuilder();

        // Second cycle: STMT_EXEC_TIME grew to 1500 ms → delta = 500 ms = 0.5 s
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(logBuilder.setEventName(anyString())).thenReturn(logBuilder);
        when(logBuilder.setSeverity(any())).thenReturn(logBuilder);
        when(logBuilder.setSeverityText(anyString())).thenReturn(logBuilder);
        when(logBuilder.setTimestamp(anyLong(), any())).thenReturn(logBuilder);
        when(logBuilder.setBody(anyString())).thenReturn(logBuilder);
        when(logBuilder.setAllAttributes(any())).thenReturn(logBuilder);

        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, 7L, 1500L, 700L, 12L, 6L, 0L, 1L, 0L, "D");

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
        assertThat(attrs.get(TopQueryEvent.DB2_EXECUTIONS)).isEqualTo(7L);
        // delta 500 ms → 0.5 s
        assertThat(attrs.get(TopQueryEvent.DB2_TOTAL_EXEC_TIME)).isEqualTo(0.5);
        assertThat(attrs.get(TopQueryEvent.DB2_TOTAL_CPU_TIME)).isNotNull();
        assertThat(attrs.get(TopQueryEvent.DB2_ROWS_READ)).isEqualTo(12L);
        assertThat(attrs.get(TopQueryEvent.DB2_ROWS_RETURNED)).isEqualTo(6L);
        assertThat(attrs.get(TopQueryEvent.DB2_ROWS_MODIFIED)).isEqualTo(0L);
        assertThat(attrs.get(TopQueryEvent.DB2_SORTS)).isEqualTo(1L);
        assertThat(attrs.get(TopQueryEvent.DB2_SORT_OVERFLOWS)).isEqualTo(0L);
        assertThat(attrs.get(TopQueryEvent.DB2_STMT_TYPE)).isEqualTo("D");
    }

    @Test
    void obfuscationFailedDropsRecord() throws Exception {
        TopQueryQuery q = query();

        // Seed
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT secret FROM T", 0L, 1L, 100L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT secret FROM T", 0L, 2L, 200L, 0L, 0L, 0L, 0L, 0L, 0L, "D");

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

        // Seed with valid row
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, 1L, 100L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        // Second cycle: null + blank
        reset(stmt, rs, otelLogger);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn(null, "   ");
        when(rs.getLong(anyString())).thenReturn(0L);
        when(rs.getString("SECTION_TYPE")).thenReturn("D");

        q.collect();

        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void zeroDeltaSkipped() throws Exception {
        TopQueryQuery q = query();

        // Seed
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        // Second cycle: same STMT_EXEC_TIME — zero delta
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");

        q.collect();

        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void topNLimitRespected() throws Exception {
        TopQueryQuery q = new TopQueryQuery(() -> conn, "TESTDB", otelLogger, 2);

        // Seed three rows
        when(rs.next()).thenReturn(true, true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM T", "SELECT 2 FROM T", "SELECT 3 FROM T");
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getLong("NUM_EXECUTIONS")).thenReturn(1L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(100L, 200L, 300L);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("TOTAL_SECTION_SORTS")).thenReturn(0L);
        when(rs.getLong("TOTAL_SORT_OVERFLOWS")).thenReturn(0L);
        when(rs.getString("SECTION_TYPE")).thenReturn("D");
        q.collect();

        // Second cycle: all three grew — only top 2 should be emitted
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(logBuilder.setEventName(anyString())).thenReturn(logBuilder);
        when(logBuilder.setSeverity(any())).thenReturn(logBuilder);
        when(logBuilder.setSeverityText(anyString())).thenReturn(logBuilder);
        when(logBuilder.setTimestamp(anyLong(), any())).thenReturn(logBuilder);
        when(logBuilder.setBody(anyString())).thenReturn(logBuilder);
        when(logBuilder.setAllAttributes(any())).thenReturn(logBuilder);

        when(rs.next()).thenReturn(true, true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM T", "SELECT 2 FROM T", "SELECT 3 FROM T");
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getLong("NUM_EXECUTIONS")).thenReturn(2L);
        when(rs.getLong("STMT_EXEC_TIME")).thenReturn(200L, 500L, 600L);
        when(rs.getLong("TOTAL_CPU_TIME")).thenReturn(0L);
        when(rs.getLong("ROWS_READ")).thenReturn(0L);
        when(rs.getLong("ROWS_RETURNED")).thenReturn(0L);
        when(rs.getLong("ROWS_MODIFIED")).thenReturn(0L);
        when(rs.getLong("TOTAL_SECTION_SORTS")).thenReturn(0L);
        when(rs.getLong("TOTAL_SORT_OVERFLOWS")).thenReturn(0L);
        when(rs.getString("SECTION_TYPE")).thenReturn("D");

        q.collect();

        verify(logBuilder, times(2)).emit();
    }

    @Test
    void sqlContainsMON_GET_PKG_CACHE_STMT() throws Exception {
        when(rs.next()).thenReturn(false);
        query().collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        assertThat(sqlCap.getValue()).containsIgnoringCase("MON_GET_PKG_CACHE_STMT");
        assertThat(sqlCap.getValue()).containsIgnoringCase("STMT_EXEC_TIME");
        assertThat(sqlCap.getValue()).containsIgnoringCase("TOTAL_SECTION_SORTS");
        assertThat(sqlCap.getValue()).containsIgnoringCase("LAST_METRICS_UPDATE");
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
    void severityIsInfo() throws Exception {
        TopQueryQuery q = query();

        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, 1L, 100L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(logBuilder.setEventName(anyString())).thenReturn(logBuilder);
        when(logBuilder.setSeverity(any())).thenReturn(logBuilder);
        when(logBuilder.setSeverityText(anyString())).thenReturn(logBuilder);
        when(logBuilder.setTimestamp(anyLong(), any())).thenReturn(logBuilder);
        when(logBuilder.setBody(anyString())).thenReturn(logBuilder);
        when(logBuilder.setAllAttributes(any())).thenReturn(logBuilder);

        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, 2L, 200L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        verify(logBuilder).setSeverity(Severity.INFO);
        verify(logBuilder).setSeverityText("INFO");
    }

    @Test
    void connectionExceptionHandledGracefully() {
        Supplier<Connection> failing = () -> { throw new RuntimeException("refused"); };
        new TopQueryQuery(failing, "TESTDB", otelLogger, 10).collect();
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
