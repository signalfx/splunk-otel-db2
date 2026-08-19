package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test — spins up a real DB2 container and verifies the
 * pipeline can connect, run a query, and start/stop cleanly.
 *
 * Metric query integration tests will be added in subsequent PRs
 * as each query class is implemented.
 *
 * Run with: ./gradlew integrationTest
 * Skipped by default in ./gradlew build (requires Docker).
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Db2ReceiverIntegrationTest {

    @Container
    static final Db2Container db2 = new Db2Container("icr.io/db2_community/db2:latest")
        .acceptLicense()
        .withUsername("db2inst1")
        .withPassword("password")
        .withDatabaseName("testdb")
        .withStartupTimeout(Duration.ofMinutes(10));

    @Test
    void receiverConnectsAndStartsCleanly() throws Exception {
        Db2ReceiverConfig config = new Db2ReceiverConfig();
        config.setHostname(db2.getHost());
        config.setHostport(db2.getMappedPort(50000));
        config.setUsername(db2.getUsername());
        config.setPassword(db2.getPassword());
        config.setDatabase(db2.getDatabaseName().toUpperCase());

        InMemoryMetricExporter exporter = InMemoryMetricExporter.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setInterval(Duration.ofSeconds(2))
                    .build()
            )
            .build();

        Db2Receiver receiver = new Db2Receiver(config, meterProvider.get("splunk-otel-db2"),
            (io.opentelemetry.api.logs.Logger) null);
        receiver.start();
        receiver.stop();
        meterProvider.shutdown();
    }

    @Test
    void jdbcConnectionIsValid() throws Exception {
        Properties props = new Properties();
        props.setProperty("user",     db2.getUsername());
        props.setProperty("password", db2.getPassword());

        try (Connection conn = DriverManager.getConnection(db2.getJdbcUrl(), props)) {
            assertThat(conn.isValid(5)).isTrue();

            // verify MON_GET_DATABASE is accessible — used by DatabaseQuery in a future PR
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                     "SELECT APPLS_CUR_CONS FROM TABLE(MON_GET_DATABASE(-1)) AS T")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("APPLS_CUR_CONS")).isGreaterThanOrEqualTo(1L);
            }
        }
    }

    @Test
    void tableFunctionsAreAccessible() throws Exception {
        Properties props = new Properties();
        props.setProperty("user",     db2.getUsername());
        props.setProperty("password", db2.getPassword());

        try (Connection conn = DriverManager.getConnection(db2.getJdbcUrl(), props);
             Statement st = conn.createStatement()) {

            // MON_GET_BUFFERPOOL — used by BufferpoolQuery in a future PR
            try (ResultSet rs = st.executeQuery(
                    "SELECT COUNT(*) AS CNT FROM TABLE(MON_GET_BUFFERPOOL(NULL, -2)) AS T")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("CNT")).isGreaterThan(0L);
            }

            // MON_GET_TABLESPACE — used by TablespaceQuery in a future PR
            try (ResultSet rs = st.executeQuery(
                    "SELECT COUNT(*) AS CNT FROM TABLE(MON_GET_TABLESPACE(NULL, -2)) AS T")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("CNT")).isGreaterThan(0L);
            }
        }
    }
}
