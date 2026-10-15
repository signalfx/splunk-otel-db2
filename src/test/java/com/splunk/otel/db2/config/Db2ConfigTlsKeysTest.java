package com.splunk.otel.db2.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
            + "      ca_file: /etc/splunk-otel-db2/ca/my-ca/ca.crt\n"
            + "      trust_store: /etc/ssl/db2.jks\n"
            + "      trust_store_password: changeit\n"
            + "      trust_store_type: JKS\n");

        assertFalse(tls.isInsecure());
        assertTrue(tls.isInsecureSkipVerify(), "insecure_skip_verify was not parsed");
        assertEquals("/etc/splunk-otel-db2/ca/my-ca/ca.crt", tls.getCaFile(), "ca_file was not parsed");
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
}
