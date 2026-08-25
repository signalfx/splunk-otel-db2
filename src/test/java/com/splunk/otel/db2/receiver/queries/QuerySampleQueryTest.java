package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.util.SqlObfuscator;
import io.opentelemetry.api.common.AttributeKey;
import org.mockito.MockedStatic;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.LogRecordBuilder;
import io.opentelemetry.api.logs.Severity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
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
        return new QuerySampleQuery(
            () -> conn, "TESTDB", otelLogger, 100, useSnapshot
        );
    }

    @Test
    void emitsObfuscatedLogRecord() throws Exception {
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT * FROM T WHERE ID = 42");
        when(rs.getLong("ELAPSED_MS")).thenReturn(150L);
        when(rs.getLong("CPU_MS")).thenReturn(50L);
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("APPLICATION_ID")).thenReturn("*LOCAL.db2inst1.230101120000");

        query(false).collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> attrsCaptor =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(attrsCaptor.capture());
        verify(logBuilder).emit();

        io.opentelemetry.api.common.Attributes attrs = attrsCaptor.getValue();
        assertThat(attrs.get(AttributeKey.stringKey("db.query.text")))
            .isEqualTo("SELECT * FROM T WHERE ID = ?");
        assertThat(attrs.get(AttributeKey.stringKey("db.namespace"))).isEqualTo("TESTDB");
        assertThat(attrs.get(AttributeKey.stringKey("db.system.name"))).isEqualTo("ibm.db2");
        assertThat(attrs.get(AttributeKey.longKey("db2.elapsed_time_ms"))).isEqualTo(150L);
        assertThat(attrs.get(AttributeKey.longKey("db2.cpu_time_ms"))).isEqualTo(50L);
        assertThat(attrs.get(AttributeKey.stringKey("db2.application.id")))
            .isEqualTo("*LOCAL.db2inst1.230101120000");
    }

    @Test
    void snapshotPathNoCpuAttribute() throws Exception {
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM SYSIBM.SYSDUMMY1");
        when(rs.getLong("ELAPSED_MS")).thenReturn(10L);
        when(rs.getLong("CPU_MS")).thenReturn(-1L); // sentinel from SNAPSTMT query
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("APPLICATION_ID")).thenReturn("APP1");

        query(true).collect();

        ArgumentCaptor<io.opentelemetry.api.common.Attributes> attrsCaptor =
            ArgumentCaptor.forClass(io.opentelemetry.api.common.Attributes.class);
        verify(logBuilder).setAllAttributes(attrsCaptor.capture());

        // cpu_time_ms must be absent when sentinel -1 is returned
        assertThat(attrsCaptor.getValue().get(AttributeKey.longKey("db2.cpu_time_ms"))).isNull();
    }

    @Test
    void obfuscationFailedDropsRecord() throws Exception {
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM T");
        when(rs.getLong("ELAPSED_MS")).thenReturn(5L);
        when(rs.getLong("CPU_MS")).thenReturn(1L);
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("APPLICATION_ID")).thenReturn(null);

        try (MockedStatic<SqlObfuscator> mocked = mockStatic(SqlObfuscator.class)) {
            mocked.when(() -> SqlObfuscator.obfuscate(anyString()))
                .thenReturn(SqlObfuscator.OBFUSCATION_FAILED);

            query(false).collect();
        }

        // No log record should be emitted
        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void nullOrBlankStatementSkipped() throws Exception {
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("STMT_TEXT")).thenReturn(null, "   ");
        when(rs.getLong(anyString())).thenReturn(0L);
        when(rs.getString("APPLICATION_ID")).thenReturn(null);

        query(false).collect();

        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void severityIsInfo() throws Exception {
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("STMT_TEXT")).thenReturn("SELECT 1 FROM T");
        when(rs.getLong("ELAPSED_MS")).thenReturn(1L);
        when(rs.getLong("CPU_MS")).thenReturn(0L);
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("APPLICATION_ID")).thenReturn(null);

        query(false).collect();

        verify(logBuilder).setSeverity(Severity.INFO);
        verify(logBuilder).setSeverityText("INFO");
    }

    @Test
    void connectionExceptionIsHandledGracefully() {
        Supplier<Connection> failingSupplier = () -> { throw new RuntimeException("connection refused"); };
        QuerySampleQuery q = new QuerySampleQuery(failingSupplier, "TESTDB", otelLogger, 10, false);

        // must not throw
        q.collect();
        verify(otelLogger, never()).logRecordBuilder();
    }

    @Test
    void sqlContainsFetchFirstWithConfiguredLimit() throws Exception {
        when(rs.next()).thenReturn(false);
        QuerySampleQuery q = new QuerySampleQuery(() -> conn, "TESTDB", otelLogger, 500, false);
        q.collect();

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCaptor.capture());
        assertThat(sqlCaptor.getValue()).contains("FETCH FIRST 500 ROWS ONLY");
    }

    @Test
    void snapshotPathUsesSnapstmtTable() throws Exception {
        when(rs.next()).thenReturn(false);
        query(true).collect();

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCaptor.capture());
        assertThat(sqlCaptor.getValue()).containsIgnoringCase("SNAPSTMT");
    }

    @Test
    void monGetActivityPathUsesMonGetActivity() throws Exception {
        when(rs.next()).thenReturn(false);
        query(false).collect();

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCaptor.capture());
        assertThat(sqlCaptor.getValue()).containsIgnoringCase("MON_GET_ACTIVITY");
    }

    @Test
    void multipleRowsEmitMultipleRecords() throws Exception {
        when(rs.next()).thenReturn(true, true, true, false);
        when(rs.getString("STMT_TEXT"))
            .thenReturn("SELECT 1 FROM T", "SELECT 2 FROM T", "SELECT 3 FROM T");
        when(rs.getLong("ELAPSED_MS")).thenReturn(1L);
        when(rs.getLong("CPU_MS")).thenReturn(0L);
        when(rs.getLong("MEMBER")).thenReturn(0L);
        when(rs.getString("APPLICATION_ID")).thenReturn(null);

        query(false).collect();

        verify(logBuilder, times(3)).emit();
    }
}
