package com.splunk.otel.db2.receiver.queries;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the instance-name lookup against a real Db2. The unit tests mock the driver, so this is
 * what proves SYSIBMADM.ENV_INST_INFO exists and that INST_NAME can be read from it.
 *
 * Run with: ./gradlew integrationTest
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InstanceInfoQueryIntegrationTest {

    @Container
    static final Db2Container db2 = new Db2Container("icr.io/db2_community/db2:11.5.9.0")
        .acceptLicense()
        .withUsername("db2inst1")
        .withPassword("password")
        .withDatabaseName("testdb")
        .withStartupTimeout(Duration.ofMinutes(10));

    private static Connection connect() throws Exception {
        Properties props = new Properties();
        props.setProperty("user",     db2.getUsername());
        props.setProperty("password", db2.getPassword());
        return DriverManager.getConnection(db2.getJdbcUrl(), props);
    }

    @Test
    void readsTheInstanceNameFromTheAdministrativeView() throws Exception {
        try (Connection conn = connect()) {
            assertThat(InstanceInfoQuery.fetchInstanceName(conn, 10, ""))
                .as("INST_NAME from SYSIBMADM.ENV_INST_INFO")
                .isEqualTo("db2inst1");
        }
    }

    // The column is declared much wider than any instance name, so a real Db2 is the only place
    // the padding behaviour can be confirmed.
    @Test
    void theReturnedNameCarriesNoPadding() throws Exception {
        try (Connection conn = connect()) {
            String name = InstanceInfoQuery.fetchInstanceName(conn, 10, "");

            assertThat(name).isNotNull();
            assertThat(name).isEqualTo(name.trim());
            assertThat(name).doesNotContain(" ");
        }
    }
}
