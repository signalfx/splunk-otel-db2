package com.splunk.otel.db2.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Db2ConfigTest {

    @TempDir
    Path tmp;

    @Test
    void validConfigLoadsSuccessfully() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  db2:\n" +
            "    hostname: mydb.example.com\n" +
            "    hostport: 50000\n" +
            "    username: db2inst1\n" +
            "    password: secret\n" +
            "    database: SAMPLE\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        Db2Config cfg = Db2Config.load(f.toString());
        assertThat(cfg.getReceivers()).hasSize(1);
        assertThat(cfg.getReceivers().get(0).getDatabase()).isEqualTo("SAMPLE");
        assertThat(cfg.getExporters().getOtlp().getEndpoint()).isEqualTo("http://collector:4317");
    }

    @Test
    void multiDatabaseListFormLoadsAllEntries() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  - hostname: db1.example.com\n" +
            "    hostport: 50000\n" +
            "    username: user1\n" +
            "    password: pass1\n" +
            "    database: PROD\n" +
            "  - hostname: db2.example.com\n" +
            "    hostport: 50001\n" +
            "    username: user2\n" +
            "    password: pass2\n" +
            "    database: ANALYTICS\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        Db2Config cfg = Db2Config.load(f.toString());
        assertThat(cfg.getReceivers()).hasSize(2);
        assertThat(cfg.getReceivers().get(0).getDatabase()).isEqualTo("PROD");
        assertThat(cfg.getReceivers().get(0).getPort()).isEqualTo(50000);
        assertThat(cfg.getReceivers().get(1).getDatabase()).isEqualTo("ANALYTICS");
        assertThat(cfg.getReceivers().get(1).getPort()).isEqualTo(50001);
    }

    @Test
    void connectTimeoutDefaultIsTen() {
        assertThat(new Db2ReceiverConfig().getConnectTimeoutSeconds()).isEqualTo(10);
    }

    @Test
    void connectTimeoutIsConfigurable() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  db2:\n" +
            "    hostname: mydb.example.com\n" +
            "    username: db2inst1\n" +
            "    password: secret\n" +
            "    database: SAMPLE\n" +
            "    connect_timeout_seconds: 30\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        Db2Config cfg = Db2Config.load(f.toString());
        assertThat(cfg.getReceivers().get(0).getConnectTimeoutSeconds()).isEqualTo(30);
    }

    @Test
    void missingUsernameFailsAtLoad() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  db2:\n" +
            "    hostname: mydb.example.com\n" +
            "    password: secret\n" +
            "    database: SAMPLE\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        assertThatThrownBy(() -> Db2Config.load(f.toString()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("username");
    }

    @Test
    void missingPasswordFailsAtLoad() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  db2:\n" +
            "    hostname: mydb.example.com\n" +
            "    username: db2inst1\n" +
            "    database: SAMPLE\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        assertThatThrownBy(() -> Db2Config.load(f.toString()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("password");
    }

    @Test
    void emptyOtlpEndpointFailsAtLoad() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  db2:\n" +
            "    hostname: mydb.example.com\n" +
            "    username: db2inst1\n" +
            "    password: secret\n" +
            "    database: SAMPLE\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: \"\"\n"
        );
        assertThatThrownBy(() -> Db2Config.load(f.toString()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("endpoint");
    }

    @Test
    void missingEnvVarFailsAtLoad() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  db2:\n" +
            "    hostname: mydb.example.com\n" +
            "    username: \"${env:DB2_USER_NONEXISTENT_12345}\"\n" +
            "    password: secret\n" +
            "    database: SAMPLE\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        assertThatThrownBy(() -> Db2Config.load(f.toString()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("DB2_USER_NONEXISTENT_12345");
    }

    @Test
    void querySampleEnabledTrueIsParsed() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  db2:\n" +
            "    hostname: mydb.example.com\n" +
            "    username: db2inst1\n" +
            "    password: secret\n" +
            "    database: SAMPLE\n" +
            "    query_sample_enabled: true\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        Db2Config cfg = Db2Config.load(f.toString());
        assertThat(cfg.getReceivers().get(0).isQuerySampleEnabled()).isTrue();
    }

    @Test
    void querySampleEnabledDefaultIsFalse() {
        assertThat(new Db2ReceiverConfig().isQuerySampleEnabled()).isFalse();
    }

    private static Path writeConfig(Path dir, String content) throws IOException {
        Path f = dir.resolve("config.yaml");
        Files.writeString(f, content);
        return f;
    }
}
