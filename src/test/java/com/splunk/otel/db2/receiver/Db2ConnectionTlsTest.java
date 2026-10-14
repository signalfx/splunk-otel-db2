package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Asserts the TLS properties handed to the JDBC driver.
 *
 * <p>Encryption without hostname validation is the failure this guards: {@code sslConnection}
 * alone does not check that the certificate was issued for the host being connected to, because
 * {@code propertyDefault_sslClientHostnameValidation} in jcc 11.5.9.0 is {@code "OFF"}. A
 * certificate the JVM trusts but issued for another host would be accepted, so the setting is
 * asserted here rather than left to review.
 *
 * <p>This does not prove a mismatched certificate is rejected end to end — that needs a
 * TLS-enabled Db2 with a deliberately wrong-CN certificate, which arrives with trust-material
 * support.
 */
class Db2ConnectionTlsTest {

    private static Db2ReceiverConfig config(boolean insecure) {
        Db2ReceiverConfig c = new Db2ReceiverConfig();
        c.setHostname("db2.example.com");
        c.setDatabase("PROD");
        c.setUsername("db2mon");
        c.setPassword("secret");
        c.getTls().setInsecure(insecure);
        return c;
    }

    @Test
    void enablesTlsAndValidatesTheServerHostname() {
        Properties props = Db2Connection.buildConnectionProperties(config(false));
        assertEquals("true", props.getProperty("sslConnection"));
        assertEquals("BASIC", props.getProperty("sslClientHostnameValidation"),
            "sslConnection alone defaults hostname validation to OFF in this driver, which "
                + "accepts a trusted certificate issued for a different host");
    }

    @Test
    void setsNoTlsPropertiesWhenInsecure() {
        Properties props = Db2Connection.buildConnectionProperties(config(true));
        assertNull(props.getProperty("sslConnection"));
        assertNull(props.getProperty("sslClientHostnameValidation"));
    }

    @Test
    void carriesCredentialsAndTimeoutsRegardlessOfTls() {
        for (boolean insecure : new boolean[] {true, false}) {
            Properties props = Db2Connection.buildConnectionProperties(config(insecure));
            assertEquals("db2mon", props.getProperty("user"));
            assertEquals("secret", props.getProperty("password"));
            assertNull(props.getProperty("sslTrustStoreLocation"),
                "no trust material is supported yet; see the follow-up for private CAs");
        }
    }
}
