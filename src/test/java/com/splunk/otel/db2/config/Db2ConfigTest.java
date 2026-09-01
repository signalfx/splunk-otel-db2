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
    void multiDatabaseComponentIdSyntaxLoadsAllEntries() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  db2/prod:\n" +
            "    hostname: db1.example.com\n" +
            "    hostport: 50000\n" +
            "    username: user1\n" +
            "    password: pass1\n" +
            "    database: PROD\n" +
            "  db2/analytics:\n" +
            "    hostname: db2.example.com\n" +
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
        // order guaranteed by LinkedHashMap (SnakeYAML preserves insertion order)
        Db2ReceiverConfig prod = cfg.getReceivers().stream()
            .filter(r -> r.getDatabase().equals("PROD")).findFirst().orElseThrow();
        Db2ReceiverConfig analytics = cfg.getReceivers().stream()
            .filter(r -> r.getDatabase().equals("ANALYTICS")).findFirst().orElseThrow();
        assertThat(prod.getComponentName()).isEqualTo("prod");
        assertThat(prod.getPort()).isEqualTo(50000);
        assertThat(analytics.getComponentName()).isEqualTo("analytics");
        assertThat(analytics.getPort()).isEqualTo(50001);
    }

    @Test
    void listFormIsRejected() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  - hostname: db1.example.com\n" +
            "    hostport: 50000\n" +
            "    username: user1\n" +
            "    password: pass1\n" +
            "    database: PROD\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        assertThatThrownBy(() -> Db2Config.load(f.toString()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("receivers");
    }

    @Test
    void duplicateTargetFailsAtLoad() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  db2/a:\n" +
            "    hostname: mydb.example.com\n" +
            "    hostport: 50000\n" +
            "    username: db2inst1\n" +
            "    password: secret\n" +
            "    database: SAMPLE\n" +
            "  db2/b:\n" +
            "    hostname: mydb.example.com\n" +
            "    hostport: 50000\n" +
            "    username: db2inst1\n" +
            "    password: secret\n" +
            "    database: SAMPLE\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        assertThatThrownBy(() -> Db2Config.load(f.toString()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Duplicate");
    }

    @Test
    void plainDb2KeyHasEmptyComponentName() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  db2:\n" +
            "    hostname: mydb.example.com\n" +
            "    username: db2inst1\n" +
            "    password: secret\n" +
            "    database: SAMPLE\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        Db2Config cfg = Db2Config.load(f.toString());
        assertThat(cfg.getReceivers().get(0).getComponentName()).isEmpty();
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

    @Test
    void unsupportedReceiverTypeFailsAtLoad() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  sqlserver:\n" +
            "    hostname: mydb.example.com\n" +
            "    username: sa\n" +
            "    password: secret\n" +
            "    database: master\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        assertThatThrownBy(() -> Db2Config.load(f.toString()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("sqlserver");
    }

    @Test
    void emptyNameAfterSlashFailsAtLoad() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  db2/:\n" +
            "    hostname: mydb.example.com\n" +
            "    username: db2inst1\n" +
            "    password: secret\n" +
            "    database: SAMPLE\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        assertThatThrownBy(() -> Db2Config.load(f.toString()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("empty");
    }

    @Test
    void nameWithInvalidCharactersFailsAtLoad() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  \"db2/bad name\":\n" +
            "    hostname: mydb.example.com\n" +
            "    username: db2inst1\n" +
            "    password: secret\n" +
            "    database: SAMPLE\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        assertThatThrownBy(() -> Db2Config.load(f.toString()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("invalid characters");
    }

    @Test
    void nonMapReceiverValueFailsAtLoad() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  db2/prod: \"not-a-map\"\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        assertThatThrownBy(() -> Db2Config.load(f.toString()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("map");
    }

    @Test
    void mixtureOfValidAndInvalidKeysFails() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  db2/prod:\n" +
            "    hostname: mydb.example.com\n" +
            "    username: db2inst1\n" +
            "    password: secret\n" +
            "    database: PROD\n" +
            "  db3/typo:\n" +
            "    hostname: other.example.com\n" +
            "    username: db2inst1\n" +
            "    password: secret\n" +
            "    database: OTHER\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        assertThatThrownBy(() -> Db2Config.load(f.toString()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("db3");
    }

    @Test
    void loopbackAliasesDetectedAsDuplicate() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  db2/a:\n" +
            "    hostname: localhost\n" +
            "    hostport: 50000\n" +
            "    username: db2inst1\n" +
            "    password: secret\n" +
            "    database: SAMPLE\n" +
            "  db2/b:\n" +
            "    hostname: 127.0.0.1\n" +
            "    hostport: 50000\n" +
            "    username: db2inst1\n" +
            "    password: secret\n" +
            "    database: SAMPLE\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        assertThatThrownBy(() -> Db2Config.load(f.toString()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Duplicate");
    }

    @Test
    void hostnameCasingDetectedAsDuplicate() throws Exception {
        Path f = writeConfig(tmp,
            "receivers:\n" +
            "  db2/a:\n" +
            "    hostname: MyDB.example.com\n" +
            "    hostport: 50000\n" +
            "    username: db2inst1\n" +
            "    password: secret\n" +
            "    database: SAMPLE\n" +
            "  db2/b:\n" +
            "    hostname: mydb.example.com\n" +
            "    hostport: 50000\n" +
            "    username: db2inst1\n" +
            "    password: secret\n" +
            "    database: SAMPLE\n" +
            "exporters:\n" +
            "  otlp:\n" +
            "    endpoint: http://collector:4317\n"
        );
        assertThatThrownBy(() -> Db2Config.load(f.toString()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Duplicate");
    }

    private static Path writeConfig(Path dir, String content) throws IOException {
        Path f = dir.resolve("config.yaml");
        Files.writeString(f, content);
        return f;
    }
}
