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
    void keepsEncryptionWhenHostnameVerificationIsWaived() {
        Db2ReceiverConfig c = config(false);
        c.getTls().setInsecureSkipVerify(true);
        Properties props = Db2Connection.buildConnectionProperties(c);
        assertEquals("true", props.getProperty("sslConnection"),
            "waiving the hostname check must not drop TLS; that is what insecure does");
        assertEquals("OFF", props.getProperty("sslClientHostnameValidation"));
    }

    @Test
    void passesTrustMaterialToTheDriver() {
        Db2ReceiverConfig c = config(false);
        c.getTls().setCaFile("/etc/splunk-otel-db2/ca/my-ca/ca.crt");
        Properties props = Db2Connection.buildConnectionProperties(c);
        assertEquals("/etc/splunk-otel-db2/ca/my-ca/ca.crt", props.getProperty("sslCertLocation"),
            "without this the driver falls back to the JVM truststore, which will not hold "
                + "an internal CA - the reason those installs had no option but insecure");
    }

    @Test
    void passesATrustStoreWhenOneIsConfigured() {
        Db2ReceiverConfig c = config(false);
        c.getTls().setTrustStore("/etc/ssl/db2.jks");
        c.getTls().setTrustStorePassword("changeit");
        c.getTls().setTrustStoreType("JKS");
        Properties props = Db2Connection.buildConnectionProperties(c);
        assertEquals("/etc/ssl/db2.jks", props.getProperty("sslTrustStoreLocation"));
        assertEquals("changeit", props.getProperty("sslTrustStorePassword"));
        assertEquals("JKS", props.getProperty("sslTrustStoreType"));
    }

    @Test
    void omitsBlankTrustMaterialRatherThanPassingEmptyStrings() {
        Db2ReceiverConfig c = config(false);
        c.getTls().setCaFile("");
        c.getTls().setTrustStore("");
        Properties props = Db2Connection.buildConnectionProperties(c);
        // The chart renders these fields unconditionally, so blanks reach here. Passing
        // sslCertLocation="" makes the driver look for a certificate at the empty path and
        // fail, rather than falling back to the JVM truststore.
        assertNull(props.getProperty("sslCertLocation"));
        assertNull(props.getProperty("sslTrustStoreLocation"));
    }

    @Test
    void sendsNoTrustMaterialWhenTlsIsOff() {
        Db2ReceiverConfig c = config(true);
        c.getTls().setCaFile("/etc/ca.crt");
        Properties props = Db2Connection.buildConnectionProperties(c);
        assertNull(props.getProperty("sslCertLocation"),
            "trust material is meaningless without TLS and would only confuse a reader of "
                + "the connection properties");
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
