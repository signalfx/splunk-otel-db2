package com.splunk.otel.db2.receiver.queries;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class InstanceInfoQueryTest {

    private Connection conn;
    private Statement stmt;
    private ResultSet rs;

    @BeforeEach
    void setUp() throws Exception {
        conn = mock(Connection.class);
        stmt = mock(Statement.class);
        rs   = mock(ResultSet.class);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
    }

    @Test
    void readsInstanceName() throws Exception {
        when(rs.next()).thenReturn(true);
        when(rs.getString("INST_NAME")).thenReturn("db2inst1");

        assertThat(InstanceInfoQuery.fetchInstanceName(conn, 10, "")).isEqualTo("db2inst1");
    }

    // ENV_INST_INFO declares INST_NAME far wider than any instance name, so the value arrives
    // padded. Untrimmed it would become a dimension with trailing spaces.
    @Test
    void trimsPaddingFromTheColumn() throws Exception {
        when(rs.next()).thenReturn(true);
        when(rs.getString("INST_NAME")).thenReturn("db2inst1                      ");

        assertThat(InstanceInfoQuery.fetchInstanceName(conn, 10, "")).isEqualTo("db2inst1");
    }

    @Test
    void appliesTheQueryTimeout() throws Exception {
        when(rs.next()).thenReturn(true);
        when(rs.getString("INST_NAME")).thenReturn("db2inst1");

        InstanceInfoQuery.fetchInstanceName(conn, 7, "");

        verify(stmt).setQueryTimeout(7);
    }

    // A monitoring user without access to the view, or a Db2 that drops the connection. The
    // attribute is dropped; startup must continue.
    @Test
    void returnsNullWhenTheQueryFails() throws Exception {
        when(stmt.executeQuery(anyString())).thenThrow(new SQLException("SQL0551N not authorized"));

        assertThat(InstanceInfoQuery.fetchInstanceName(conn, 10, "")).isNull();
    }

    @Test
    void returnsNullWhenNoRowsComeBack() throws Exception {
        when(rs.next()).thenReturn(false);

        assertThat(InstanceInfoQuery.fetchInstanceName(conn, 10, "")).isNull();
    }

    // Guards against emitting an empty-string dimension, which is worse than omitting the
    // attribute: it looks like a real value.
    @Test
    void returnsNullForBlankOrNullColumn() throws Exception {
        when(rs.next()).thenReturn(true);

        when(rs.getString("INST_NAME")).thenReturn("     ");
        assertThat(InstanceInfoQuery.fetchInstanceName(conn, 10, "")).isNull();

        when(rs.getString("INST_NAME")).thenReturn(null);
        assertThat(InstanceInfoQuery.fetchInstanceName(conn, 10, "")).isNull();
    }

    @Test
    void closesTheStatement() throws Exception {
        when(rs.next()).thenReturn(true);
        when(rs.getString("INST_NAME")).thenReturn("db2inst1");

        InstanceInfoQuery.fetchInstanceName(conn, 10, "");

        verify(stmt).close();
        verify(rs).close();
    }

    @Test
    void queriesTheAdministrativeView() {
        assertThat(InstanceInfoQuery.SQL)
            .isEqualTo("SELECT INST_NAME FROM SYSIBMADM.ENV_INST_INFO");
    }
}
