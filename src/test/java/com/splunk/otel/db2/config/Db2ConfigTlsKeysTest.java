package com.splunk.otel.db2.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Parses TLS settings from YAML rather than through setters.
 *
 * <p>This exists because a setter-based test passes happily while the key the chart writes
 * is never read. {@code insecure_skip_verify} was exposed in the schema, the README and the
 * config object, but neither rendered nor parsed, so the option had no effect on the
 * connection at all. Anything the chart emits has to be asserted through this path.
 *
 * <p>Key names match {@code exporters.otlp.tls} in {@link Db2Config} and the collector's own
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
    void readsInsecureSkipVerifyFromYaml(@TempDir Path dir) throws Exception {
        Db2ReceiverConfig.TlsConfig tls = tlsFrom(dir, ""
            + "    tls:\n"
            + "      insecure: false\n"
            + "      insecure_skip_verify: true\n");
        assertFalse(tls.isInsecure());
        assertTrue(tls.isInsecureSkipVerify(),
            "insecure_skip_verify was not parsed, so the chart value has no effect");
    }

    @Test
    void defaultsToCheckingTheCertificateHostname(@TempDir Path dir) throws Exception {
        Db2ReceiverConfig.TlsConfig tls = tlsFrom(dir, "");
        assertFalse(tls.isInsecure(), "TLS is on by default; the credentials travel over it");
        assertFalse(tls.isInsecureSkipVerify(), "the certificate hostname is checked by default");
    }
}
