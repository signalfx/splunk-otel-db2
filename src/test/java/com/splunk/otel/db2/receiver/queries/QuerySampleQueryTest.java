package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.util.SqlObfuscator;
import io.opentelemetry.api.common.AttributeKey;
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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class QuerySampleQueryTest {

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

    private QuerySampleQuery query(boolean useSnapshot) {
        return new QuerySampleQuery(() -> conn, "TESTDB", otelLogger, 100, useSnapshot);
    }

    private void stubRow(String stmtText, String state, String startTime,
                         long elapsedSec, String authId, String clientHost,
                         String appHandle, long member, long cpuMs) throws Exception {
        when(rs.getString("STMT_TEXT")).thenReturn(stmtText);
        when(rs.getString("ACTIVITY_STATE")).thenReturn(state);
        when(rs.getString("LOCAL_START_TIME")).thenReturn(startTime);
        when(rs.getLong("WALL_ELAPSED_SEC")).thenReturn(elapsedSec);
        when(rs.getString("CLIENT_USERID")).thenReturn(authId);
        when(rs.getString("CLIENT_WRKSTNNAME")).thenReturn(clientHost);
        when(rs.getString("APPLICATION_HANDLE")).thenReturn(appHandle);
        when(rs.getLong("MEMBER")).thenReturn(member);
        when(rs.getLong("CPU_MS")).thenReturn(cpuMs);
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
        assertThat(attrs.get(AttributeKey.stringKey("db.query.text")))
            .isEqualTo("SELECT * FROM T WHERE ID = ?");
        assertThat(attrs.get(AttributeKey.stringKey("event.name")))
            .isEqualTo("db.server.query_sample");
        assertThat(attrs.get(AttributeKey.stringKey("db.system.name"))).isEqualTo("ibm.db2");
        assertThat(attrs.get(AttributeKey.stringKey("db.namespace"))).isEqualTo("TESTDB");
        assertThat(attrs.get(AttributeKey.stringKey("db2.activity_state"))).isEqualTo("EXECUTING SELECT");
        assertThat(attrs.get(AttributeKey.stringKey("db2.query_start")))
            .isEqualTo("2024-01-01-12.00.00.000000");
        assertThat(attrs.get(AttributeKey.doubleKey("db2.session.duration"))).isEqualTo(5.0);
        assertThat(attrs.get(AttributeKey.stringKey("user.name"))).isEqualTo("db2inst1");
        assertThat(attrs.get(AttributeKey.stringKey("client.address"))).isEqualTo("client-host");
        assertThat(attrs.get(AttributeKey.stringKey("db2.application_handle"))).isEqualTo("12345");
        assertThat(attrs.get(AttributeKey.longKey("db2.member"))).isEqualTo(0L);
        assertThat(attrs.get(AttributeKey.longKey("db2.cpu_time_ms"))).isEqualTo(100L);
    }

    @Test
    void snapshotPathNoCpuAttribute() throws Exception {
        when(rs.next()).thenReturn(true, false);
        stubRow("SELECT 1 FROM SYSIBM.SYSDUMMY1", "UOWEXEC SELECT",
                "2024-01-01-12.00.00.000000", 2L,
                "db2inst1", "host1", "99", 0L, -1L); // -1 sentinel from SNAP path

        query(true).collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> cap =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(cap.capture());

        assertThat(cap.getValue().get(AttributeKey.longKey("db2.cpu_time_ms"))).isNull();
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
    void defaultPathSqlContainsMON_CURRENT_SQL() throws Exception {
        when(rs.next()).thenReturn(false);
        query(false).collect();

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCap.capture());
        assertThat(sqlCap.getValue()).containsIgnoringCase("MON_CURRENT_SQL");
        assertThat(sqlCap.getValue()).containsIgnoringCase("MON_GET_CONNECTION");
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
        assertThat(sql).containsIgnoringCase("MON_CURRENT_SQL");
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

        query(false).collect();

        verify(logBuilder, times(3)).emit();
    }
}
