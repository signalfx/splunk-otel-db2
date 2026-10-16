package com.splunk.otel.db2.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Parses TLS settings from YAML rather than through setters.
 *
 * <p>This exists because setter-based tests passed while the keys the chart writes were not
 * being read at all: the parser only handled {@code insecure}, so {@code ca_file} and the
 * hostname setting were rendered into config.yaml and silently ignored. Anything the chart
 * emits has to be asserted through this path, not just through the config object.
 *
 * <p>Key names match {@code exporters.otlp.tls} in this same file and the collector's own
 * database receivers, so a reader does not meet a third vocabulary for the same concepts.
 */
class Db2ConfigTlsKeysTest {

    private static Db2ReceiverConfig.TlsConfig tlsFrom(Path dir, String tlsBlock) throws Exception {
        Path f = dir.resolve("config.yaml");
        Files.writeString(f, ""
            + "receivers:\n"
            + "  db2:\n"
            + "    hostname: db2.example.com\n"
            + "    hostport: 50001\n"
            + "    username: u\n"
            + "    password: p\n"
            + "    database: PROD\n"
            + tlsBlock
            + "exporters:\n"
            + "  otlp:\n"
            + "    endpoint: http://localhost:4317\n"
            + "    tls:\n"
            + "      insecure: true\n");
        return Db2Config.load(f.toString()).getReceivers().get(0).getTls();
    }

    @Test
    void readsEveryKeyTheChartRenders(@TempDir Path dir) throws Exception {
        Db2ReceiverConfig.TlsConfig tls = tlsFrom(dir, ""
            + "    tls:\n"
            + "      insecure: false\n"
            + "      insecure_skip_verify: true\n"
            + "      ca_file: /etc/splunk-otel-db2-ca/default/ca.crt\n"
            + "      trust_store: /etc/ssl/db2.jks\n"
            + "      trust_store_password: changeit\n"
            + "      trust_store_type: JKS\n");

        assertFalse(tls.isInsecure());
        assertTrue(tls.isInsecureSkipVerify(), "insecure_skip_verify was not parsed");
        assertEquals("/etc/splunk-otel-db2-ca/default/ca.crt", tls.getCaFile(), "ca_file was not parsed");
        assertEquals("/etc/ssl/db2.jks", tls.getTrustStore());
        assertEquals("changeit", tls.getTrustStorePassword());
        assertEquals("JKS", tls.getTrustStoreType());
    }

    @Test
    void defaultsToVerifyingWithNoTrustMaterial(@TempDir Path dir) throws Exception {
        Db2ReceiverConfig.TlsConfig tls = tlsFrom(dir, "");
        assertFalse(tls.isInsecure(), "TLS is on by default; the credentials travel over it");
        assertFalse(tls.isInsecureSkipVerify(), "the certificate hostname is checked by default");
        assertEquals("", tls.getCaFile());
    }

    /**
     * The chart refuses this pair, but systemd, Docker and bare-JVM installs never run Helm,
     * so it has to be refused here too. Accepting it configured neither TLS nor trust
     * material and sent the Db2 credentials in the clear, which is the opposite of what
     * supplying a CA asks for.
     */
    @Test
    void refusesTrustMaterialWhenTlsIsDisabled(@TempDir Path dir) {
        for (String key : new String[] {"ca_file", "trust_store"}) {
            IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> tlsFrom(dir, ""
                    + "    tls:\n"
                    + "      insecure: true\n"
                    + "      " + key + ": /etc/ssl/thing\n"),
                key + " with insecure: true must be refused, not silently ignored");
            assertTrue(e.getMessage().contains(key),
                "the error must name " + key + ", got: " + e.getMessage());
        }
    }

    @Test
    void allowsInsecureOnItsOwn(@TempDir Path dir) throws Exception {
        assertTrue(tlsFrom(dir, "    tls:\n      insecure: true\n").isInsecure(),
            "only the contradictory pair is refused, not insecure by itself");
    }

    /**
     * A truststore password is a credential, so it has to be sourceable from the environment
     * like the Db2 password. It was being passed to the driver verbatim, which both broke
     * password-protected truststores and left the config file as the only place to put it.
     */
    @Test
    void expandsTheTrustStorePasswordFromTheEnvironment(@TempDir Path dir) throws Exception {
        String var = System.getenv().keySet().stream().findFirst().orElseThrow();
        Db2ReceiverConfig.TlsConfig tls = tlsFrom(dir, ""
            + "    tls:\n"
            + "      insecure: false\n"
            + "      trust_store: /etc/ssl/db2.jks\n"
            + "      trust_store_password: ${env:" + var + "}\n");
        assertEquals(System.getenv(var), tls.getTrustStorePassword(),
            "${env:...} must be expanded, as it is for the Db2 password");
    }
}
