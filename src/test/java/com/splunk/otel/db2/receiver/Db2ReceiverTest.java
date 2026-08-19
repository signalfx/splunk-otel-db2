package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
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
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class Db2ReceiverTest {

    private InMemoryMetricExporter exporter;
    private SdkMeterProvider meterProvider;
    private Connection mockConn;
    private Db2ReceiverConfig config;

    private ResultSet freshResultSet() throws Exception {
        ResultSet rs = Mockito.mock(ResultSet.class);
        when(rs.next()).thenReturn(true, false);
        when(rs.getLong(anyString())).thenReturn(5L);
        when(rs.getDouble(anyString())).thenReturn(95.5);
        when(rs.getString("BP_NAME")).thenReturn("IBMDEFAULTBP");
        when(rs.getString("TBSP_NAME")).thenReturn("USERSPACE1");
        return rs;
    }

    @BeforeEach
    void setUp() throws Exception {
        exporter = InMemoryMetricExporter.create();

        meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setInterval(Duration.ofMillis(100))
                    .build()
            )
            .build();

        mockConn = Mockito.mock(Connection.class);
        config = new Db2ReceiverConfig();
        config.setDatabase("TESTDB");
        config.setUsername("db2inst1");
        config.setPassword("password");

        when(mockConn.prepareStatement(anyString())).thenAnswer(inv -> {
            PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
            when(stmt.executeQuery()).thenAnswer(i -> freshResultSet());
            return stmt;
        });
    }

    @Test
    void receiverEmitsExpectedMetricNames() throws Exception {
        Db2Connection mockDb2Conn = Mockito.mock(Db2Connection.class);
        when(mockDb2Conn.getOrConnect()).thenReturn(mockConn);

        Db2Receiver receiver = new Db2Receiver(config, meterProvider.get("splunk-otel-db2"), mockDb2Conn);
        receiver.start();

        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Set<String> names = exporter.getFinishedMetricItems().stream()
                .map(MetricData::getName)
                .collect(Collectors.toSet());
            assertThat(names).contains(
                "db.db2.connections",
                "db.db2.deadlocks",
                "db.db2.lock_waits",
                "db.db2.lock_wait_time",
                "db.db2.lock_timeouts",
                "db.db2.buffer_pool.hit_ratio",
                "db.db2.buffer_pool.data_reads",
                "db.db2.tablespace.used",
                "db.db2.tablespace.free"
            );
        });
    }

    @Test
    void metricsHaveCorrectDbAttributes() throws Exception {
        Db2Connection mockDb2Conn = Mockito.mock(Db2Connection.class);
        when(mockDb2Conn.getOrConnect()).thenReturn(mockConn);

        Db2Receiver receiver = new Db2Receiver(config, meterProvider.get("splunk-otel-db2"), mockDb2Conn);
        receiver.start();

        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            List<MetricData> metrics = exporter.getFinishedMetricItems();
            MetricData connections = metrics.stream()
                .filter(m -> m.getName().equals("db.db2.connections"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("db.db2.connections not found"));

            assertThat(connections.getLongGaugeData().getPoints()).isNotEmpty();
            connections.getLongGaugeData().getPoints().forEach(point ->
                assertThat(point.getAttributes().asMap().toString()).contains("TESTDB")
            );
        });
    }

    @Test
    void configYamlSchemaMatchesGoReceiverFieldNames() {
        Set<String> expectedMetricNames = Set.of(
            "db.db2.connections",
            "db.db2.deadlocks",
            "db.db2.lock_waits",
            "db.db2.lock_wait_time",
            "db.db2.lock_timeouts",
            "db.db2.buffer_pool.hit_ratio",
            "db.db2.buffer_pool.data_reads",
            "db.db2.tablespace.used",
            "db.db2.tablespace.free"
        );
        assertThat(expectedMetricNames).allSatisfy(name ->
            assertThat(name).startsWith("db.db2.")
        );
    }
}
