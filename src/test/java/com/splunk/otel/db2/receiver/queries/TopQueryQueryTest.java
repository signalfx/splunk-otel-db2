package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.util.SqlObfuscator;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.LogRecordBuilder;
import io.opentelemetry.api.logs.Severity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.sql.CallableStatement;
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
    void sqlDoesNotApplyFetchFirstLimit() throws Exception {
        // All eligible rows are fetched — Java ranks interval deltas over the complete set
        // so a newly hot query cannot be displaced by a historically expensive idle one.
        TopQueryQuery q = new TopQueryQuery(() -> conn, "TESTDB", otelLogger, 10, 500, 60L,
            null, 1000, 3600_000L, "");
        when(rs.next()).thenReturn(false);
        q.collect();
        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        assertThat(sqlCap.getValue()).doesNotContainIgnoringCase("FETCH FIRST");
    }

    @Test
    void postFetchTruncationRespectsMaxQuerySampleCount() throws Exception {
        // maxQuerySampleCount=2; DB returns 3 rows. After truncation only the first 2
        // rows enter the delta-ranking loop, so at most 2 records are emitted.
        // topQueryCount=10 (higher than maxQuerySampleCount) to ensure truncation — not
        // topQueryCount — is the binding constraint.
        TopQueryQuery q = new TopQueryQuery(() -> conn, "TESTDB", otelLogger, 10, 2, 60L,
            null, 1000, 3600_000L, "");

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
        when(rs.getTimestamp("INSERT_TIMESTAMP")).thenReturn(null);
        q.collect();

        // Cycle 2: all 3 rows grew — but only 2 (the first 2 after truncation) should rank
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.next()).thenReturn(true, true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM T", "SELECT 2 FROM T", "SELECT 3 FROM T");
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
        when(rs.getTimestamp("INSERT_TIMESTAMP")).thenReturn(null);
        q.collect();

        // Only 2 records emitted — the 3rd row was truncated by maxQuerySampleCount
        verify(logBuilder, times(2)).emit();
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

    /** Build a plan-enabled query: 2 max attempts/interval, cache size 100, TTL 1h. */
    private TopQueryQuery queryWithPlan() {
        return new TopQueryQuery(() -> conn, "TESTDB", otelLogger, 10, 60L, 2, 100, 3600_000L);
    }

    /** Build a plan-enabled query with 0ms TTL — cache always expires between calls. */
    private TopQueryQuery queryWithPlanNoCache() {
        return new TopQueryQuery(() -> conn, "TESTDB", otelLogger, 10, 60L, 2, 100, 0L);
    }

    /**
     * Stub the EXPLAIN_FROM_SECTION CallableStatement and subsequent explain table
     * PreparedStatements so that fetchPlanXml returns a full JSON plan string.
     *
     * Produces one operator (TBSCAN, cost=1.0), one stream (id=1, from=-1, to=1, cols=3),
     * one argument (PREFETCHTYPE=NONE), one object (DB2INST1.ORDERS TABLE stream=1),
     * and one predicate (SARG on op=1, text="Q1.ID = ?").
     *
     * Returns the CallableStatement mock so callers can verify call counts.
     */
    private CallableStatement stubPlanPs(String ignoredPlanXml) throws Exception {
        CallableStatement cs = mock(CallableStatement.class);
        // EXPLAIN_FROM_SECTION params: (3)=EXPLAIN_SCHEMA INOUT, (4)=EXPLAIN_REQUESTER, (5)=EXPLAIN_TIME,
        //                              (6)=SOURCE_NAME, (7)=SOURCE_SCHEMA, (8)=SOURCE_VERSION
        when(cs.getString(3)).thenReturn("TESTUSER"); // EXPLAIN_SCHEMA INOUT — actual schema used
        when(cs.getString(4)).thenReturn("TESTUSER"); // EXPLAIN_REQUESTER OUT
        Timestamp ts = Timestamp.from(java.time.Instant.EPOCH.plusSeconds(999));
        when(cs.getTimestamp(5)).thenReturn(ts);
        when(cs.getString(6)).thenReturn("SYSSH200"); // SOURCE_NAME OUT
        when(cs.getString(7)).thenReturn("NULLID");   // SOURCE_SCHEMA OUT
        when(cs.getString(8)).thenReturn("0100270A"); // SOURCE_VERSION OUT

        // EXPLAIN_OPERATOR — one row: TBSCAN cost=1.0
        PreparedStatement opPs = mock(PreparedStatement.class);
        ResultSet opRs = mock(ResultSet.class);
        when(opRs.next()).thenReturn(true, false);
        when(opRs.getInt("OPERATOR_ID")).thenReturn(1);
        when(opRs.getString("OPERATOR_TYPE")).thenReturn("TBSCAN");
        when(opRs.getDouble("TOTAL_COST")).thenReturn(1.0);
        when(opPs.executeQuery()).thenReturn(opRs);

        // EXPLAIN_STREAM — one row: id=1 from=-1 to=1 cols=3, with SOURCE_TYPE/TARGET_TYPE/OBJECT_SCHEMA/OBJECT_NAME
        PreparedStatement streamPs = mock(PreparedStatement.class);
        ResultSet streamRs = mock(ResultSet.class);
        when(streamRs.next()).thenReturn(true, false);
        when(streamRs.getInt("STREAM_ID")).thenReturn(1);
        when(streamRs.getInt("SOURCE_ID")).thenReturn(-1);
        when(streamRs.getInt("TARGET_ID")).thenReturn(1);
        when(streamRs.getInt("COLUMN_COUNT")).thenReturn(3);
        when(streamRs.getString("SOURCE_TYPE")).thenReturn("TBSCAN");
        when(streamRs.getString("TARGET_TYPE")).thenReturn("RETURN");
        when(streamRs.getString("OBJECT_SCHEMA")).thenReturn("DB2INST1");
        when(streamRs.getString("OBJECT_NAME")).thenReturn("ORDERS");
        when(streamPs.executeQuery()).thenReturn(streamRs);

        // EXPLAIN_ARGUMENT — one row: op=1 PREFETCHTYPE=NONE
        PreparedStatement argPs = mock(PreparedStatement.class);
        ResultSet argRs = mock(ResultSet.class);
        when(argRs.next()).thenReturn(true, false);
        when(argRs.getInt("OPERATOR_ID")).thenReturn(1);
        when(argRs.getString("ARGUMENT_TYPE")).thenReturn("PREFETCHTYPE");
        when(argRs.getString("ARGUMENT_VALUE")).thenReturn("NONE");
        when(argPs.executeQuery()).thenReturn(argRs);

        // EXPLAIN_OBJECT — one row: DB2INST1.ORDERS TABLE rows=1000 cols=6
        PreparedStatement objPs = mock(PreparedStatement.class);
        ResultSet objRs = mock(ResultSet.class);
        when(objRs.next()).thenReturn(true, false);
        when(objRs.getString("OBJECT_SCHEMA")).thenReturn("DB2INST1");
        when(objRs.getString("OBJECT_NAME")).thenReturn("ORDERS");
        when(objRs.getString("OBJECT_TYPE")).thenReturn("TABLE");
        when(objRs.getDouble("ROW_COUNT")).thenReturn(1000.0);
        when(objRs.getInt("COLUMN_COUNT")).thenReturn(6);
        when(objPs.executeQuery()).thenReturn(objRs);

        // EXPLAIN_PREDICATE — one row: op=1 predId=1 SARG "Q1.ID = 42" (raw, obfuscated to "Q1.ID = ?")
        PreparedStatement predPs = mock(PreparedStatement.class);
        ResultSet predRs = mock(ResultSet.class);
        when(predRs.next()).thenReturn(true, false);
        when(predRs.getInt("OPERATOR_ID")).thenReturn(1);
        when(predRs.getInt("PREDICATE_ID")).thenReturn(1);
        when(predRs.getString("HOW_APPLIED")).thenReturn("SARG");
        when(predRs.getString("PREDICATE_TEXT")).thenReturn("Q1.ID = 42");
        when(predPs.executeQuery()).thenReturn(predRs);

        // EXPLAIN_INSTANCE DELETE
        PreparedStatement deletePs = mock(PreparedStatement.class);
        when(deletePs.executeUpdate()).thenReturn(1);

        // Wire: prepareCall → cs; prepareStatement in order: op, stream, arg, obj, pred, delete
        when(conn.prepareCall(anyString())).thenReturn(cs);
        when(conn.prepareStatement(anyString()))
            .thenReturn(opPs, streamPs, argPs, objPs, predPs, deletePs);

        return cs;
    }

    @Test
    void planXmlAttachedToTopQueryRecordOnCacheMiss() throws Exception {
        stubPlanPs(null);

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

        // Plan JSON must contain all 5 explain sections with obfuscated predicate text
        String plan = cap.getValue().get(TopQueryEvent.DB2_QUERY_PLAN);
        assertThat(plan).contains("\"operators\"").contains("TBSCAN");
        assertThat(plan).contains("\"streams\"").contains("\"from\":-1");
        // Stream must include SOURCE_TYPE, TARGET_TYPE, OBJECT_SCHEMA, OBJECT_NAME (P2-4)
        assertThat(plan).contains("src_type").contains("tgt_type").contains("obj_schema").contains("obj_name");
        assertThat(plan).contains("\"arguments\"").contains("PREFETCHTYPE");
        assertThat(plan).contains("\"objects\"").contains("ORDERS").contains("TABLE").contains("\"rows\"");
        assertThat(plan).contains("\"predicates\"").contains("SARG");
        // Literal in predicate text must be obfuscated: "Q1.ID = 42" → "Q1.ID = ?"
        assertThat(plan).contains("Q1.ID = ?").doesNotContain("42");
        assertThat(cap.getValue().get(TopQueryEvent.DB2_PLAN_HASH)).isNotNull().matches("[0-9a-fA-F]+");
    }

    @Test
    void planServedFromCacheOnSecondEmit() throws Exception {
        // First emit fetches the plan from DB2; second emit uses the cache (TTL=1h).
        CallableStatement cs = stubPlanPs(null);

        TopQueryQuery q = queryWithPlan();

        // Seed
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x01, 0x02});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        // First emit — cache miss, DB2 called once
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x01, 0x02});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 6L, 1500L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();
        verify(conn, times(1)).prepareCall(anyString());

        // Second emit — cache hit, no new DB2 call
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x01, 0x02});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 7L, 2000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        verify(conn, times(1)).prepareCall(anyString()); // still 1 — second emit served from cache
        verify(logBuilder).emit();
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        // Plan JSON must be present (one operator: TBSCAN)
        assertThat(cap.getValue().get(TopQueryEvent.DB2_QUERY_PLAN)).contains("TBSCAN");
    }

    @Test
    void planRefetchedAfterCacheExpiry() throws Exception {
        // With TTL=0ms the cache always expires — DB2 is called on every emit interval.
        CallableStatement cs = stubPlanPs(null);

        TopQueryQuery q = queryWithPlanNoCache(); // TTL=0

        // Seed
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x01, 0x02});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        // First emit
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x01, 0x02});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 6L, 1500L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();
        verify(conn, times(1)).prepareCall(anyString());

        // Second emit — cache expired (TTL=0), DB2 called again
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        // Re-stub prepareCall after reset
        when(conn.prepareCall(anyString())).thenReturn(cs);
        when(conn.prepareStatement(anyString())).thenAnswer(inv -> {
            PreparedStatement ps = mock(PreparedStatement.class);
            ResultSet prs = mock(ResultSet.class);
            when(prs.next()).thenReturn(false);
            when(ps.executeQuery()).thenReturn(prs);
            when(ps.executeUpdate()).thenReturn(1);
            return ps;
        });
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x01, 0x02});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "exec1", 7L, 2000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        // 2 total calls (first + second emit after cache expiry)
        verify(conn, times(2)).prepareCall(anyString());
        verify(logBuilder).emit();
    }

    @Test
    void planFetchFailureDoesNotDropTopQueryRecord() throws Exception {
        when(conn.prepareCall(anyString())).thenThrow(new java.sql.SQLException("access denied"));

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

        // prepareCall must never be called when plan is disabled
        verify(conn, never()).prepareCall(anyString());
    }

    @Test
    void planBudgetCapRespectsMaxAttemptsPerInterval() throws Exception {
        // planMaxPerInterval=2; 3 candidates; budget counts attempts before the DB2 call.
        // The 3rd candidate must not trigger a plan lookup even when the first two succeed.
        when(conn.prepareCall(anyString())).thenAnswer(inv -> {
            CallableStatement cs = mock(CallableStatement.class);
            when(cs.getString(3)).thenReturn("TESTUSER"); // EXPLAIN_SCHEMA INOUT
            when(cs.getString(4)).thenReturn("TESTUSER"); // EXPLAIN_REQUESTER
            when(cs.getTimestamp(5)).thenReturn(
                Timestamp.from(java.time.Instant.EPOCH.plusSeconds(999)));
            when(cs.getString(6)).thenReturn("SYSSH200"); // SOURCE_NAME
            when(cs.getString(7)).thenReturn("NULLID");   // SOURCE_SCHEMA
            when(cs.getString(8)).thenReturn("0100270A"); // SOURCE_VERSION
            return cs;
        });
        when(conn.prepareStatement(anyString())).thenAnswer(inv -> {
            PreparedStatement ps = mock(PreparedStatement.class);
            ResultSet prs = mock(ResultSet.class);
            when(prs.next()).thenReturn(false);
            when(ps.executeQuery()).thenReturn(prs);
            when(ps.executeUpdate()).thenReturn(1);
            return ps;
        });

        // Use TTL=0 so no cache hits inflate attempt counts
        TopQueryQuery q = queryWithPlanNoCache();

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

        // All 3 top_query records emitted, but only 2 plan attempts (budget cap = max attempts)
        verify(logBuilder, times(3)).emit();
        verify(conn, times(2)).prepareCall(anyString());
    }

    @Test
    void planBudgetCapCountsFailedAttemptsAgainstBudget() throws Exception {
        // planMaxPerInterval=2; all calls fail; the 3rd candidate must still be skipped.
        when(conn.prepareCall(anyString())).thenThrow(new java.sql.SQLException("access denied"));

        TopQueryQuery q = queryWithPlanNoCache();

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

        // Emit cycle
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

        // All 3 records emitted without plans; prepareCall called exactly 2 times (not 3)
        verify(logBuilder, times(3)).emit();
        verify(conn, times(2)).prepareCall(anyString());
    }

    @Test
    void hexConversionRoundTrip() {
        byte[] bytes = {0x00, 0x0F, (byte) 0xFF, 0x42, (byte) 0xAB};
        String hex = TopQueryQuery.hexToBytes(null) == null ? "null ok" : "";
        assertThat(TopQueryQuery.hexToBytes(null)).isNull();
        assertThat(TopQueryQuery.hexToBytes("0d")).isEqualTo(new byte[]{0x0d});
        assertThat(TopQueryQuery.hexToBytes("abc")).isNull();  // odd length
        assertThat(TopQueryQuery.hexToBytes("0off")).isNull(); // non-hex character
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
        cfg.getTopQueryCollection().setCollectionInterval("30s");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            cfg.getTopQueryCollection()::getCollectionIntervalDuration);
    }

    @Test
    void configAcceptsTopQueryIntervalAtMinimum() {
        com.splunk.otel.db2.config.Db2ReceiverConfig cfg = new com.splunk.otel.db2.config.Db2ReceiverConfig();
        cfg.getTopQueryCollection().setCollectionInterval("60s");
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(cfg.getTopQueryCollection()::getCollectionIntervalDuration);
    }

    @Test
    void configRejectsZeroTopQueryCount() {
        com.splunk.otel.db2.config.Db2ReceiverConfig cfg = new com.splunk.otel.db2.config.Db2ReceiverConfig();
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            () -> cfg.getTopQueryCollection().setTopQueryCount(0));
    }

    @Test
    void jsonEscapeHandlesSpecialCharacters() {
        assertThat(TopQueryQuery.jsonEscape(null)).isEmpty();
        assertThat(TopQueryQuery.jsonEscape("plain")).isEqualTo("plain");
        assertThat(TopQueryQuery.jsonEscape("say \"hi\"")).isEqualTo("say \\\"hi\\\"");
        assertThat(TopQueryQuery.jsonEscape("a\\b")).isEqualTo("a\\\\b");
        assertThat(TopQueryQuery.jsonEscape("tab\there")).isEqualTo("tab\\u0009here");
    }

    @Test
    void predicateTextObfuscationFailureOmitsTextButKeepsRow() throws Exception {
        // When PREDICATE_TEXT cannot be obfuscated the row is still emitted without "text"
        stubPlanPs(null); // sets up normal mocks first, then we override predicate
        CallableStatement cs2 = mock(CallableStatement.class);
        when(cs2.getString(3)).thenReturn("TESTUSER");
        when(cs2.getString(4)).thenReturn("TESTUSER");
        Timestamp ts = Timestamp.from(java.time.Instant.EPOCH.plusSeconds(999));
        when(cs2.getTimestamp(5)).thenReturn(ts);
        when(cs2.getString(6)).thenReturn("SYSSH200");
        when(cs2.getString(7)).thenReturn("NULLID");
        when(cs2.getString(8)).thenReturn("0100270A");

        PreparedStatement opPs2 = mock(PreparedStatement.class);
        ResultSet opRs2 = mock(ResultSet.class);
        when(opRs2.next()).thenReturn(true, false);
        when(opRs2.getInt("OPERATOR_ID")).thenReturn(1);
        when(opRs2.getString("OPERATOR_TYPE")).thenReturn("TBSCAN");
        when(opRs2.getDouble("TOTAL_COST")).thenReturn(1.0);
        when(opPs2.executeQuery()).thenReturn(opRs2);

        PreparedStatement streamPs2 = mock(PreparedStatement.class);
        ResultSet streamRs2 = mock(ResultSet.class);
        when(streamRs2.next()).thenReturn(false);
        when(streamPs2.executeQuery()).thenReturn(streamRs2);

        PreparedStatement argPs2 = mock(PreparedStatement.class);
        ResultSet argRs2 = mock(ResultSet.class);
        when(argRs2.next()).thenReturn(false);
        when(argPs2.executeQuery()).thenReturn(argRs2);

        PreparedStatement objPs2 = mock(PreparedStatement.class);
        ResultSet objRs2 = mock(ResultSet.class);
        when(objRs2.next()).thenReturn(false);
        when(objPs2.executeQuery()).thenReturn(objRs2);

        // Predicate with unterminated string literal — obfuscation will fail
        PreparedStatement predPs2 = mock(PreparedStatement.class);
        ResultSet predRs2 = mock(ResultSet.class);
        when(predRs2.next()).thenReturn(true, false);
        when(predRs2.getInt("OPERATOR_ID")).thenReturn(1);
        when(predRs2.getInt("PREDICATE_ID")).thenReturn(1);
        when(predRs2.getString("HOW_APPLIED")).thenReturn("SARG");
        when(predRs2.getString("PREDICATE_TEXT")).thenReturn("Q1.NAME = 'unterminated");
        when(predPs2.executeQuery()).thenReturn(predRs2);

        PreparedStatement delPs2 = mock(PreparedStatement.class);
        when(delPs2.executeUpdate()).thenReturn(1);

        TopQueryQuery q = queryWithPlan();

        // Seed
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x0A});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "execA", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        reset(conn);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(conn.prepareCall(anyString())).thenReturn(cs2);
        when(conn.prepareStatement(anyString()))
            .thenReturn(opPs2, streamPs2, argPs2, objPs2, predPs2, delPs2);
        q.collect();

        // Emit
        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x0A});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "execA", 6L, 1500L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        String plan = cap.getValue().get(TopQueryEvent.DB2_QUERY_PLAN);
        assertThat(plan).isNotNull().contains("SARG");
        // text field must be absent — obfuscation failed, literal must not leak
        assertThat(plan).doesNotContain("unterminated").doesNotContain("Q1.NAME");
    }

    @Test
    void configRejectsNegativeTopQueryCount() {
        com.splunk.otel.db2.config.Db2ReceiverConfig cfg = new com.splunk.otel.db2.config.Db2ReceiverConfig();
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            () -> cfg.getTopQueryCollection().setTopQueryCount(-1));
    }

    // --- P1-1: ARGUMENT_VALUE obfuscation ---

    @Test
    void argumentValueWithLiteralIsObfuscated() throws Exception {
        // PREFETCHTYPE is in the allowlist — its value "NONE" passes through obfuscation
        // unchanged (no literals) and appears in the plan JSON.
        CallableStatement cs = mock(CallableStatement.class);
        when(cs.getString(3)).thenReturn("TESTUSER");
        when(cs.getString(4)).thenReturn("TESTUSER");
        Timestamp ts = Timestamp.from(java.time.Instant.EPOCH.plusSeconds(999));
        when(cs.getTimestamp(5)).thenReturn(ts);
        when(cs.getString(6)).thenReturn("SYSSH200");
        when(cs.getString(7)).thenReturn("NULLID");
        when(cs.getString(8)).thenReturn("0100270A");

        PreparedStatement opPs = mock(PreparedStatement.class);
        ResultSet opRs = mock(ResultSet.class);
        when(opRs.next()).thenReturn(true, false);
        when(opRs.getInt("OPERATOR_ID")).thenReturn(1);
        when(opRs.getString("OPERATOR_TYPE")).thenReturn("TBSCAN");
        when(opRs.getDouble("TOTAL_COST")).thenReturn(1.0);
        when(opPs.executeQuery()).thenReturn(opRs);

        PreparedStatement streamPs = mock(PreparedStatement.class);
        ResultSet streamRs = mock(ResultSet.class);
        when(streamRs.next()).thenReturn(false);
        when(streamPs.executeQuery()).thenReturn(streamRs);

        PreparedStatement argPs = mock(PreparedStatement.class);
        ResultSet argRs = mock(ResultSet.class);
        when(argRs.next()).thenReturn(true, false);
        when(argRs.getInt("OPERATOR_ID")).thenReturn(1);
        when(argRs.getString("ARGUMENT_TYPE")).thenReturn("PREFETCHTYPE");
        when(argRs.getString("ARGUMENT_VALUE")).thenReturn("NONE");
        when(argPs.executeQuery()).thenReturn(argRs);

        PreparedStatement objPs = mock(PreparedStatement.class);
        ResultSet objRs = mock(ResultSet.class);
        when(objRs.next()).thenReturn(false);
        when(objPs.executeQuery()).thenReturn(objRs);

        PreparedStatement predPs = mock(PreparedStatement.class);
        ResultSet predRs = mock(ResultSet.class);
        when(predRs.next()).thenReturn(false);
        when(predPs.executeQuery()).thenReturn(predRs);

        PreparedStatement delPs = mock(PreparedStatement.class);
        when(delPs.executeUpdate()).thenReturn(1);

        when(conn.prepareCall(anyString())).thenReturn(cs);
        when(conn.prepareStatement(anyString()))
            .thenReturn(opPs, streamPs, argPs, objPs, predPs, delPs);

        TopQueryQuery q = queryWithPlan();

        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x20});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "execLit", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x20});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "execLit", 6L, 1500L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        String plan = cap.getValue().get(TopQueryEvent.DB2_QUERY_PLAN);
        assertThat(plan).isNotNull().contains("PREFETCHTYPE");
        // PREFETCHTYPE is in the allowlist; "NONE" has no literals so passes through obfuscation unchanged.
        assertThat(plan).contains("NONE");
    }

    @Test
    void unknownArgumentTypeValueOmitted() throws Exception {
        // NEWTYPE is not in the allowlist — its value must be omitted from the plan JSON
        // to prevent leaking literals from unknown argument formats.
        CallableStatement cs = mock(CallableStatement.class);
        when(cs.getString(3)).thenReturn("TESTUSER");
        when(cs.getString(4)).thenReturn("TESTUSER");
        Timestamp ts = Timestamp.from(java.time.Instant.EPOCH.plusSeconds(999));
        when(cs.getTimestamp(5)).thenReturn(ts);
        when(cs.getString(6)).thenReturn("SYSSH200");
        when(cs.getString(7)).thenReturn("NULLID");
        when(cs.getString(8)).thenReturn("0100270A");

        PreparedStatement opPs = mock(PreparedStatement.class);
        ResultSet opRs = mock(ResultSet.class);
        when(opRs.next()).thenReturn(true, false);
        when(opRs.getInt("OPERATOR_ID")).thenReturn(1);
        when(opRs.getString("OPERATOR_TYPE")).thenReturn("TBSCAN");
        when(opRs.getDouble("TOTAL_COST")).thenReturn(1.0);
        when(opPs.executeQuery()).thenReturn(opRs);

        PreparedStatement streamPs = mock(PreparedStatement.class);
        ResultSet streamRs = mock(ResultSet.class);
        when(streamRs.next()).thenReturn(false);
        when(streamPs.executeQuery()).thenReturn(streamRs);

        PreparedStatement argPs = mock(PreparedStatement.class);
        ResultSet argRs = mock(ResultSet.class);
        when(argRs.next()).thenReturn(true, false);
        when(argRs.getInt("OPERATOR_ID")).thenReturn(1);
        when(argRs.getString("ARGUMENT_TYPE")).thenReturn("NEWTYPE");
        when(argRs.getString("ARGUMENT_VALUE")).thenReturn("some value");
        when(argPs.executeQuery()).thenReturn(argRs);

        PreparedStatement objPs = mock(PreparedStatement.class);
        ResultSet objRs = mock(ResultSet.class);
        when(objRs.next()).thenReturn(false);
        when(objPs.executeQuery()).thenReturn(objRs);

        PreparedStatement predPs = mock(PreparedStatement.class);
        ResultSet predRs = mock(ResultSet.class);
        when(predRs.next()).thenReturn(false);
        when(predPs.executeQuery()).thenReturn(predRs);

        PreparedStatement delPs = mock(PreparedStatement.class);
        when(delPs.executeUpdate()).thenReturn(1);

        when(conn.prepareCall(anyString())).thenReturn(cs);
        when(conn.prepareStatement(anyString()))
            .thenReturn(opPs, streamPs, argPs, objPs, predPs, delPs);

        TopQueryQuery q = queryWithPlan();

        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x21});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "execNew", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x21});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "execNew", 6L, 1500L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        String plan = cap.getValue().get(TopQueryEvent.DB2_QUERY_PLAN);
        assertThat(plan).isNotNull().contains("NEWTYPE");
        // Unknown type — value must be omitted to avoid leaking literals.
        assertThat(plan).doesNotContain("some value");
    }

    // --- P1-2: ORDER BY before FETCH FIRST ---

    @Test
    void sqlHasNoOrderByOrFetchFirst() throws Exception {
        // No ORDER BY or FETCH FIRST in the DB query — Java ranks interval deltas over
        // the full eligible set after computing per-interval deltas.
        when(rs.next()).thenReturn(false);
        query().collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        String sql = sqlCap.getValue();
        assertThat(sql).doesNotContainIgnoringCase("ORDER BY STMT_EXEC_TIME");
        assertThat(sql).doesNotContainIgnoringCase("FETCH FIRST");
    }

    // --- P1-3: maxExplainEachInterval defaults to 0 ---

    @Test
    void maxExplainDefaultIsZero() {
        com.splunk.otel.db2.config.Db2ReceiverConfig.TopQueryCollectionConfig cfg =
            new com.splunk.otel.db2.config.Db2ReceiverConfig.TopQueryCollectionConfig();
        assertThat(cfg.getMaxExplainEachInterval()).isEqualTo(0);
    }

    // --- P2-5: transient timeout in per-table try/catch returns null plan (not cached) ---

    @Test
    void transientTimeoutInArgumentTableReturnsNullPlan() throws Exception {
        // Stub EXPLAIN_FROM_SECTION and EXPLAIN_OPERATOR normally, but EXPLAIN_ARGUMENT throws
        // a generic SQLException (not SQLCODE -206, not SQLFeatureNotSupportedException).
        // The outer catch should return null so the entry is not cached.
        CallableStatement cs = mock(CallableStatement.class);
        when(cs.getString(3)).thenReturn("TESTUSER");
        when(cs.getString(4)).thenReturn("TESTUSER");
        Timestamp ts = Timestamp.from(java.time.Instant.EPOCH.plusSeconds(999));
        when(cs.getTimestamp(5)).thenReturn(ts);
        when(cs.getString(6)).thenReturn("SYSSH200");
        when(cs.getString(7)).thenReturn("NULLID");
        when(cs.getString(8)).thenReturn("0100270A");

        PreparedStatement opPs = mock(PreparedStatement.class);
        ResultSet opRs = mock(ResultSet.class);
        when(opRs.next()).thenReturn(true, false);
        when(opRs.getInt("OPERATOR_ID")).thenReturn(1);
        when(opRs.getString("OPERATOR_TYPE")).thenReturn("TBSCAN");
        when(opRs.getDouble("TOTAL_COST")).thenReturn(1.0);
        when(opPs.executeQuery()).thenReturn(opRs);

        PreparedStatement streamPs = mock(PreparedStatement.class);
        ResultSet streamRs = mock(ResultSet.class);
        when(streamRs.next()).thenReturn(false);
        when(streamPs.executeQuery()).thenReturn(streamRs);

        // EXPLAIN_ARGUMENT throws a transient SQLException (not a compatibility signal)
        PreparedStatement argPs = mock(PreparedStatement.class);
        when(argPs.executeQuery()).thenThrow(new java.sql.SQLException("timeout"));

        PreparedStatement delPs = mock(PreparedStatement.class);
        when(delPs.executeUpdate()).thenReturn(1);

        when(conn.prepareCall(anyString())).thenReturn(cs);
        // op, stream, arg, delete (cleanup always runs)
        when(conn.prepareStatement(anyString()))
            .thenReturn(opPs, streamPs, argPs, delPs);

        TopQueryQuery q = queryWithPlan();

        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x30});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "execTimeout", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x30});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "execTimeout", 6L, 1500L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        // top_query record should still be emitted (plan failure doesn't drop the record)
        verify(logBuilder).emit();
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        // But plan must be null (transient exception → not cached → null returned)
        assertThat(cap.getValue().get(TopQueryEvent.DB2_QUERY_PLAN)).isNull();
    }

    // --- P2-6: per-plan byte budget cap ---

    @Test
    void planExceedingByteBudgetReturnsNull() throws Exception {
        // Generate a large predicate result set that pushes sb.length() over 100 KiB.
        // Each predicate row contributes ~60 bytes; need ~1700 rows to exceed 100 KiB.
        CallableStatement cs = mock(CallableStatement.class);
        when(cs.getString(3)).thenReturn("TESTUSER");
        when(cs.getString(4)).thenReturn("TESTUSER");
        Timestamp ts = Timestamp.from(java.time.Instant.EPOCH.plusSeconds(999));
        when(cs.getTimestamp(5)).thenReturn(ts);
        when(cs.getString(6)).thenReturn("SYSSH200");
        when(cs.getString(7)).thenReturn("NULLID");
        when(cs.getString(8)).thenReturn("0100270A");

        PreparedStatement opPs = mock(PreparedStatement.class);
        ResultSet opRs = mock(ResultSet.class);
        when(opRs.next()).thenReturn(true, false);
        when(opRs.getInt("OPERATOR_ID")).thenReturn(1);
        when(opRs.getString("OPERATOR_TYPE")).thenReturn("TBSCAN");
        when(opRs.getDouble("TOTAL_COST")).thenReturn(1.0);
        when(opPs.executeQuery()).thenReturn(opRs);

        PreparedStatement streamPs = mock(PreparedStatement.class);
        ResultSet streamRs = mock(ResultSet.class);
        when(streamRs.next()).thenReturn(false);
        when(streamPs.executeQuery()).thenReturn(streamRs);

        PreparedStatement argPs = mock(PreparedStatement.class);
        ResultSet argRs = mock(ResultSet.class);
        when(argRs.next()).thenReturn(false);
        when(argPs.executeQuery()).thenReturn(argRs);

        PreparedStatement objPs = mock(PreparedStatement.class);
        ResultSet objRs = mock(ResultSet.class);
        when(objRs.next()).thenReturn(false);
        when(objPs.executeQuery()).thenReturn(objRs);

        // EXPLAIN_PREDICATE: produce 2000 rows, each with a 50-char text → well over 100 KiB
        PreparedStatement predPs = mock(PreparedStatement.class);
        ResultSet predRs = mock(ResultSet.class);
        // Return true 2000 times then false
        Boolean[] nexts = new Boolean[2001];
        java.util.Arrays.fill(nexts, Boolean.TRUE);
        nexts[2000] = Boolean.FALSE;
        when(predRs.next()).thenReturn(true, nexts);
        when(predRs.getInt("OPERATOR_ID")).thenReturn(1);
        when(predRs.getInt("PREDICATE_ID")).thenReturn(1);
        when(predRs.getString("HOW_APPLIED")).thenReturn("SARG");
        // 60-char text per predicate row
        when(predRs.getString("PREDICATE_TEXT")).thenReturn("ABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890ABCDEFGHIJKLMNOPQRSTUVWX");
        when(predPs.executeQuery()).thenReturn(predRs);

        PreparedStatement delPs = mock(PreparedStatement.class);
        when(delPs.executeUpdate()).thenReturn(1);

        when(conn.prepareCall(anyString())).thenReturn(cs);
        when(conn.prepareStatement(anyString()))
            .thenReturn(opPs, streamPs, argPs, objPs, predPs, delPs);

        TopQueryQuery q = queryWithPlan();

        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x40});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "execBig", 5L, 1000L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        reset(stmt, rs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        when(rs.getBytes("EXECUTABLE_ID")).thenReturn(new byte[]{0x40});
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM T", 0L, "execBig", 6L, 1500L, 0L, 0L, 0L, 0L, 0L, 0L, "D");
        q.collect();

        // top_query record emitted, but plan must be null (budget exceeded → not cached)
        verify(logBuilder).emit();
        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        assertThat(cap.getValue().get(TopQueryEvent.DB2_QUERY_PLAN)).isNull();
    }
}
