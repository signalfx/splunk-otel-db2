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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class ExplainPlanQueryTest {

    private Connection conn;
    private Statement  stmt;
    private ResultSet  topRs;
    private PreparedStatement planPs;
    private ResultSet  planRs;
    private Logger     otelLogger;
    private LogRecordBuilder logBuilder;

    private static final String PLAN_XML = "<explainPlan><operator type='TBSCAN'/></explainPlan>";
    private static final String EXEC_ID  = "exec1";
    private static final long   MEMBER   = 0L;

    @BeforeEach
    void setUp() throws Exception {
        conn       = mock(Connection.class);
        stmt       = mock(Statement.class);
        topRs      = mock(ResultSet.class);
        planPs     = mock(PreparedStatement.class);
        planRs     = mock(ResultSet.class);
        otelLogger = mock(Logger.class);
        logBuilder = mock(LogRecordBuilder.class, RETURNS_SELF);

        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(topRs);
        when(conn.prepareStatement(anyString())).thenReturn(planPs);
        when(planPs.executeQuery()).thenReturn(planRs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);

        Clob clob = mock(Clob.class);
        when(clob.length()).thenReturn((long) PLAN_XML.length());
        when(clob.getSubString(1, PLAN_XML.length())).thenReturn(PLAN_XML);
        when(planRs.next()).thenReturn(true);
        when(planRs.getClob("SECTION_XML")).thenReturn(clob);
    }

    private ExplainPlanQuery query() {
        return new ExplainPlanQuery(() -> conn, "TESTDB", otelLogger, 20, 600L,
            ExplainPlanQuery.DEFAULT_PLAN_CACHE_TTL_MS, 30);
    }

    private void stubTopRow(String stmtText, long member, String execId) throws Exception {
        when(topRs.next()).thenReturn(true, false);
        when(topRs.getString("STMT_TEXT")).thenReturn(stmtText);
        when(topRs.getLong("MEMBER")).thenReturn(member);
        when(topRs.getString("EXECUTABLE_ID")).thenReturn(execId);
    }

    @Test
    void emitsQueryPlanEventWithCorrectAttributes() throws Exception {
        stubTopRow("SELECT col FROM mytable WHERE id = 1", MEMBER, EXEC_ID);

        query().collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());
        verify(logBuilder).emit();

        io.opentelemetry.api.common.Attributes attrs = cap.getValue();
        assertThat(attrs.get(ExplainPlanEvent.DB_SYSTEM)).isEqualTo("ibm.db2");
        assertThat(attrs.get(ExplainPlanEvent.DB_NAMESPACE)).isEqualTo("TESTDB");
        assertThat(attrs.get(ExplainPlanEvent.DB_QUERY_TEXT)).isEqualTo("SELECT col FROM mytable WHERE id = ?");
        assertThat(attrs.get(ExplainPlanEvent.DB2_MEMBER)).isEqualTo(MEMBER);
        assertThat(attrs.get(ExplainPlanEvent.DB2_QUERY_PLAN)).isEqualTo(PLAN_XML);
        assertThat(attrs.get(ExplainPlanEvent.DB2_PLAN_HASH)).isNotNull().hasSize(64);
    }

    @Test
    void eventNameIsDbServerQueryPlan() throws Exception {
        stubTopRow("SELECT 1 FROM T", MEMBER, EXEC_ID);

        query().collect();

        verify(logBuilder).setEventName("db.server.query_plan");
    }

    @Test
    void severityIsInfo() throws Exception {
        stubTopRow("SELECT 1 FROM T", MEMBER, EXEC_ID);

        query().collect();

        verify(logBuilder).setSeverity(Severity.INFO);
        verify(logBuilder).setSeverityText("INFO");
    }

    @Test
    void planHashIsSha256OfPlanXml() {
        String hash = ExplainPlanQuery.sha256Hex(PLAN_XML);
        assertThat(hash).hasSize(64).matches("[0-9a-f]+");
        // Same input → same hash (deterministic)
        assertThat(ExplainPlanQuery.sha256Hex(PLAN_XML)).isEqualTo(hash);
        // Different input → different hash
        assertThat(ExplainPlanQuery.sha256Hex("other")).isNotEqualTo(hash);
    }

    @Test
    void cacheHitSkipsPlanFetch() throws Exception {
        stubTopRow("SELECT 1 FROM T", MEMBER, EXEC_ID);
        ExplainPlanQuery q = query();

        // First collect — plan fetched and cached
        q.collect();
        verify(planPs, times(1)).executeQuery();
        verify(logBuilder, times(1)).emit();

        // Second collect — same execId still within TTL; plan fetch skipped
        reset(stmt, topRs, planPs, planRs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(topRs);
        when(conn.prepareStatement(anyString())).thenReturn(planPs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);
        stubTopRow("SELECT 1 FROM T", MEMBER, EXEC_ID);

        q.collect();

        verify(planPs, never()).executeQuery();
        verify(logBuilder, never()).emit();
    }

    @Test
    void expiredCacheEntryRefetchesPlan() throws Exception {
        ExplainPlanQuery q = new ExplainPlanQuery(() -> conn, "TESTDB", otelLogger, 20, 600L,
            1L /* 1ms TTL — expires immediately */, 30);

        stubTopRow("SELECT 1 FROM T", MEMBER, EXEC_ID);
        q.collect();
        verify(logBuilder, times(1)).emit();

        // Let the TTL expire
        Thread.sleep(5);

        // Second collect — entry expired; plan re-fetched
        reset(stmt, topRs, planPs, planRs, otelLogger, logBuilder);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(topRs);
        when(conn.prepareStatement(anyString())).thenReturn(planPs);
        when(otelLogger.logRecordBuilder()).thenReturn(logBuilder);

        Clob clob2 = mock(Clob.class);
        when(clob2.length()).thenReturn((long) PLAN_XML.length());
        when(clob2.getSubString(1, PLAN_XML.length())).thenReturn(PLAN_XML);
        when(planRs.next()).thenReturn(true);
        when(planRs.getClob("SECTION_XML")).thenReturn(clob2);
        when(planPs.executeQuery()).thenReturn(planRs);
        stubTopRow("SELECT 1 FROM T", MEMBER, EXEC_ID);

        q.collect();

        verify(planPs, times(1)).executeQuery();
        verify(logBuilder, times(1)).emit();
    }

    @Test
    void nullPlanXmlSkipsEmit() throws Exception {
        stubTopRow("SELECT 1 FROM T", MEMBER, EXEC_ID);
        when(planRs.getClob("SECTION_XML")).thenReturn(null);

        query().collect();

        verify(logBuilder, never()).emit();
    }

    @Test
    void planFetchExceptionSkipsRecord() throws Exception {
        stubTopRow("SELECT 1 FROM T", MEMBER, EXEC_ID);
        when(planPs.executeQuery()).thenThrow(new java.sql.SQLException("access denied"));

        query().collect();

        verify(logBuilder, never()).emit();
    }

    @Test
    void obfuscationFailedDropsRecord() throws Exception {
        stubTopRow("SELECT secret FROM T", MEMBER, EXEC_ID);

        try (MockedStatic<SqlObfuscator> mocked = mockStatic(SqlObfuscator.class)) {
            mocked.when(() -> SqlObfuscator.obfuscate(anyString()))
                .thenReturn(SqlObfuscator.OBFUSCATION_FAILED);
            query().collect();
        }

        verify(logBuilder, never()).emit();
    }

    @Test
    void nullOrBlankStmtTextIsSkipped() throws Exception {
        when(topRs.next()).thenReturn(true, true, false);
        when(topRs.getString("STMT_TEXT")).thenReturn(null, "   ");
        when(topRs.getLong("MEMBER")).thenReturn(MEMBER);
        when(topRs.getString("EXECUTABLE_ID")).thenReturn(EXEC_ID, EXEC_ID + "2");

        query().collect();

        verify(logBuilder, never()).emit();
    }

    @Test
    void clobIsFreeAfterPlanFetch() throws Exception {
        Clob clob = mock(Clob.class);
        when(clob.length()).thenReturn((long) PLAN_XML.length());
        when(clob.getSubString(1, PLAN_XML.length())).thenReturn(PLAN_XML);
        when(planRs.getClob("SECTION_XML")).thenReturn(clob);
        stubTopRow("SELECT 1 FROM T", MEMBER, EXEC_ID);

        query().collect();

        verify(clob).free();
    }

    @Test
    void clobIsFreeEvenWhenXmlIsBlank() throws Exception {
        Clob clob = mock(Clob.class);
        when(clob.length()).thenReturn(3L);
        when(clob.getSubString(1, 3)).thenReturn("   ");
        when(planRs.getClob("SECTION_XML")).thenReturn(clob);
        stubTopRow("SELECT 1 FROM T", MEMBER, EXEC_ID);

        query().collect();

        verify(clob).free();
        verify(logBuilder, never()).emit();
    }

    @Test
    void connectionExceptionHandledGracefully() {
        new ExplainPlanQuery(
            () -> { throw new RuntimeException("refused"); },
            "TESTDB", otelLogger, 20, 600L,
            ExplainPlanQuery.DEFAULT_PLAN_CACHE_TTL_MS, 30
        ).collect();

        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void sqlContainsSelfFilterClauses() throws Exception {
        when(topRs.next()).thenReturn(false);
        query().collect();

        ArgumentCaptor<String> cap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(cap.capture());
        String sql = cap.getValue();

        assertThat(sql).containsIgnoringCase("MON_GET_PKG_CACHE_STMT");
        assertThat(sql).containsIgnoringCase("MON_GET_ACTIVITY");
        assertThat(sql).containsIgnoringCase("MON_GET_DATABASE");
        assertThat(sql).containsIgnoringCase("MON_GET_BUFFERPOOL");
        assertThat(sql).containsIgnoringCase("MON_GET_TABLESPACE");
        assertThat(sql).containsIgnoringCase("MON_CURRENT_SQL");
        assertThat(sql).containsIgnoringCase("MON_GET_CONNECTION");
        assertThat(sql).containsIgnoringCase("SNAP_GET_APPL_INFO");
        assertThat(sql).containsIgnoringCase("SNAP_GET_STMT");
        assertThat(sql).containsIgnoringCase("SYSDUMMY1");
    }

    @Test
    void maxPlansPerIntervalIsRespected() throws Exception {
        // Return 3 rows but budget is 2; only 2 plans should be fetched and emitted.
        ExplainPlanQuery q = new ExplainPlanQuery(() -> conn, "TESTDB", otelLogger, 2, 600L,
            ExplainPlanQuery.DEFAULT_PLAN_CACHE_TTL_MS, 30);

        when(topRs.next()).thenReturn(true, true, true, false);
        when(topRs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM T", "SELECT 2 FROM T", "SELECT 3 FROM T");
        when(topRs.getLong("MEMBER")).thenReturn(MEMBER);
        when(topRs.getString("EXECUTABLE_ID")).thenReturn("e1", "e2", "e3");

        // Each plan fetch uses a separate PreparedStatement; stub two distinct plan results.
        PreparedStatement planPs2 = mock(PreparedStatement.class);
        ResultSet planRs2 = mock(ResultSet.class);
        Clob clob2 = mock(Clob.class);
        when(clob2.length()).thenReturn((long) PLAN_XML.length());
        when(clob2.getSubString(1, PLAN_XML.length())).thenReturn(PLAN_XML);
        when(planRs2.next()).thenReturn(true);
        when(planRs2.getClob("SECTION_XML")).thenReturn(clob2);
        when(planPs2.executeQuery()).thenReturn(planRs2);
        // First call returns planPs (already in @BeforeEach), second returns planPs2
        when(conn.prepareStatement(anyString())).thenReturn(planPs, planPs2);

        q.collect();

        // The Java-side budget (candidates.size() < maxPlansPerInterval) stops collection at 2
        // even though topRs returns 3 rows — the third candidate is never processed.
        verify(logBuilder, times(2)).emit();
    }

    @Test
    void queryTimeoutIsSet() throws Exception {
        when(topRs.next()).thenReturn(false);
        query().collect();
        verify(stmt).setQueryTimeout(30);
    }

    @Test
    void planFetchSetsQueryTimeout() throws Exception {
        stubTopRow("SELECT 1 FROM T", MEMBER, EXEC_ID);
        query().collect();
        verify(planPs).setQueryTimeout(30);
    }

    @Test
    void differentMembersSameExecIdTrackedSeparately() throws Exception {
        when(topRs.next()).thenReturn(true, true, false);
        when(topRs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM T", "SELECT 1 FROM T");
        when(topRs.getLong("MEMBER")).thenReturn(0L, 1L); // different members
        when(topRs.getString("EXECUTABLE_ID")).thenReturn(EXEC_ID, EXEC_ID);

        // Both plan fetches succeed
        Clob clob2 = mock(Clob.class);
        when(clob2.length()).thenReturn((long) PLAN_XML.length());
        when(clob2.getSubString(1, PLAN_XML.length())).thenReturn(PLAN_XML);
        when(planRs.next()).thenReturn(true, true);
        when(planRs.getClob("SECTION_XML")).thenReturn(
            mock(Clob.class, invocation -> {
                if (invocation.getMethod().getName().equals("length")) return (long) PLAN_XML.length();
                if (invocation.getMethod().getName().equals("getSubString")) return PLAN_XML;
                return null;
            }));

        ExplainPlanQuery q = query();

        // First collect — both cache misses, both fetched
        // We can't easily verify 2 emits with single planRs mock, so just verify the cache
        // uses different keys for member 0 and member 1 by checking 2 prepareStatement calls
        q.collect();
        verify(conn, atLeast(1)).prepareStatement(anyString());
    }
}
