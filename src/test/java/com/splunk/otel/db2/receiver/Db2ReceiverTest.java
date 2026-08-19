package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.receiver.queries.BufferpoolQuery;
import com.splunk.otel.db2.receiver.queries.DatabaseQuery;
import com.splunk.otel.db2.receiver.queries.TablespaceQuery;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class Db2ReceiverTest {

    private InMemoryMetricExporter exporter;
    private SdkMeterProvider meterProvider;
    private Connection mockConn;
    private Db2ReceiverConfig config;

    @BeforeEach
    void setUp() throws Exception {
        exporter = InMemoryMetricExporter.create();

        meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setInterval(Duration.ofMillis(200))
                    .build()
            )
            .build();

        mockConn = Mockito.mock(Connection.class);
        config = new Db2ReceiverConfig();
        config.setDatabase("TESTDB");
        config.setUsername("db2inst1");
        config.setPassword("password");

        // default: statements return empty result sets
        when(mockConn.prepareStatement(anyString())).thenAnswer(inv -> {
            PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
            ResultSet rs = Mockito.mock(ResultSet.class);
            when(rs.next()).thenReturn(false);
            when(stmt.executeQuery()).thenReturn(rs);
            return stmt;
        });
        when(mockConn.createStatement()).thenAnswer(inv -> {
            Statement stmt = Mockito.mock(Statement.class);
            ResultSet rs = Mockito.mock(ResultSet.class);
            when(rs.next()).thenReturn(false);
            when(stmt.executeQuery(anyString())).thenReturn(rs);
            return stmt;
        });
    }

    @Test
    void receiverStartsAndStopsWithoutError() {
        Db2Connection mockDb2Conn = Mockito.mock(Db2Connection.class);
        Db2Receiver receiver = new Db2Receiver(config, meterProvider.get("splunk-otel-db2"), mockDb2Conn);
        receiver.start();
        receiver.stop();
    }

    @Test
    void receiverConfigHasExpectedDatabase() {
        assertThat(config.getDatabase()).isEqualTo("TESTDB");
    }

    @Test
    void receiverConfigDefaultIntervalIsParseable() {
        assertThat(config.getCollectionIntervalDuration().getSeconds()).isGreaterThan(0);
        assertThat(config.getQuerySampleIntervalDuration().getSeconds()).isGreaterThan(0);
    }

    @Test
    void databaseQueryRegistersExpectedMetrics() throws Exception {
        Connection conn = Mockito.mock(Connection.class);
        Statement st = Mockito.mock(Statement.class);
        ResultSet rs = Mockito.mock(ResultSet.class);
        when(conn.createStatement()).thenReturn(st);
        when(st.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getInt("MEMBER")).thenReturn(0);
        when(rs.getLong(anyString())).thenReturn(1L);

        new DatabaseQuery(() -> conn, "TESTDB", meterProvider.get("splunk-otel-db2"));

        Awaitility.await().atMost(2, TimeUnit.SECONDS)
            .until(() -> metricNames(exporter).contains("db2.connection.count"));

        assertThat(metricNames(exporter)).contains(
            "db2.connection.count",
            "db2.connection.active",
            "db2.connection.max",
            "db2.lock.wait.count",
            "db2.lock.wait_time",
            "db2.lock.timeout.count",
            "db2.lock.escalation.count",
            "db2.lock.space_used",
            "db2.deadlock.count",
            "db2.sort.count",
            "db2.sort.overflow.count"
        );
    }

    @Test
    void bufferpoolQueryRegistersExpectedMetrics() throws Exception {
        Connection conn = Mockito.mock(Connection.class);
        Statement st = Mockito.mock(Statement.class);
        ResultSet rs = Mockito.mock(ResultSet.class);
        when(conn.createStatement()).thenReturn(st);
        when(st.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getInt("MEMBER")).thenReturn(0);
        when(rs.getString("BP_NAME")).thenReturn("IBMDEFAULTBP");
        when(rs.getLong(anyString())).thenReturn(1L);

        new BufferpoolQuery(() -> conn, "TESTDB", meterProvider.get("splunk-otel-db2"));

        Awaitility.await().atMost(2, TimeUnit.SECONDS)
            .until(() -> metricNames(exporter).contains("db2.buffer_pool.logical_reads"));

        assertThat(metricNames(exporter)).contains(
            "db2.buffer_pool.logical_reads",
            "db2.buffer_pool.physical_reads",
            "db2.buffer_pool.writes",
            "db2.buffer_pool.read_time",
            "db2.buffer_pool.write_time"
        );
    }

    @Test
    void tablespaceQueryRegistersExpectedMetrics() throws Exception {
        Connection conn = Mockito.mock(Connection.class);
        Statement st = Mockito.mock(Statement.class);
        ResultSet rs = Mockito.mock(ResultSet.class);
        when(conn.createStatement()).thenReturn(st);
        when(st.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getInt("MEMBER")).thenReturn(0);
        when(rs.getString("TBSP_NAME")).thenReturn("USERSPACE1");
        when(rs.getString("TBSP_TYPE")).thenReturn("DMS");
        when(rs.getString("TBSP_STATE")).thenReturn("NORMAL");
        when(rs.getLong("PAGE_SIZE")).thenReturn(4096L);
        when(rs.getLong("TBSP_TOTAL_PAGES")).thenReturn(1000L);
        when(rs.getLong("TBSP_USED_PAGES")).thenReturn(600L);
        when(rs.getLong("TBSP_FREE_PAGES")).thenReturn(400L);

        new TablespaceQuery(() -> conn, "TESTDB", meterProvider.get("splunk-otel-db2"));

        Awaitility.await().atMost(2, TimeUnit.SECONDS)
            .until(() -> metricNames(exporter).contains("db2.tablespace.size"));

        assertThat(metricNames(exporter)).contains("db2.tablespace.size");
    }

    private static List<String> metricNames(InMemoryMetricExporter exporter) {
        return exporter.getFinishedMetricItems().stream()
            .map(MetricData::getName)
            .collect(Collectors.toList());
    }
}
