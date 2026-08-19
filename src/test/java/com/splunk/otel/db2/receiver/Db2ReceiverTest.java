package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;

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
            ResultSet rs = Mockito.mock(ResultSet.class);
            when(rs.next()).thenReturn(false);
            when(stmt.executeQuery()).thenReturn(rs);
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
}
