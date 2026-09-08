package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that two Db2Receiver instances can run concurrently against the same database,
 * each with fully isolated connections, schedulers, and SDK pipelines. This is the
 * single-process multi-database scenario: two configs pointing at the same container
 * stand in for two distinct databases in production.
 *
 * Run with: ./gradlew integrationTest
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Db2MultiReceiverIntegrationTest {

    @Container
    static final Db2Container db2 = new Db2Container("icr.io/db2_community/db2:11.5.9.0")
        .acceptLicense()
        .withUsername("db2inst1")
        .withPassword("password")
        .withDatabaseName("testdb")
        .withStartupTimeout(Duration.ofMinutes(10));

    private Db2ReceiverConfig buildConfig() {
        Db2ReceiverConfig config = new Db2ReceiverConfig();
        config.setHostname(db2.getHost());
        config.setHostport(db2.getMappedPort(50000));
        config.setUsername(db2.getUsername());
        config.setPassword(db2.getPassword());
        config.setDatabase(db2.getDatabaseName().toUpperCase());
        config.setCollectionInterval("2s");
        config.getTls().setInsecure(true);
        return config;
    }

    private SdkMeterProvider buildMeterProvider(InMemoryMetricExporter exporter) {
        return SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setInterval(Duration.ofSeconds(2))
                    .build()
            )
            .build();
    }

    @Test
    void twoReceiversCollectMetricsConcurrently() {
        InMemoryMetricExporter exporter1 = InMemoryMetricExporter.create();
        InMemoryMetricExporter exporter2 = InMemoryMetricExporter.create();
        SdkMeterProvider meterProvider1 = buildMeterProvider(exporter1);
        SdkMeterProvider meterProvider2 = buildMeterProvider(exporter2);

        Db2Receiver receiver1 = new Db2Receiver(buildConfig(),
            meterProvider1.get("splunk-otel-db2"), (io.opentelemetry.api.logs.Logger) null);
        Db2Receiver receiver2 = new Db2Receiver(buildConfig(),
            meterProvider2.get("splunk-otel-db2"), (io.opentelemetry.api.logs.Logger) null);

        receiver1.start();
        receiver2.start();

        try {
            List<String> required = List.of(
                "db2.connection.count", "db2.lock.wait.count", "db2.deadlock.count");

            Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> metricNames(exporter1).containsAll(required)
                          && metricNames(exporter2).containsAll(required));

            assertThat(metricNames(exporter1)).containsAll(required);
            assertThat(metricNames(exporter2)).containsAll(required);
        } finally {
            receiver1.stop();
            receiver2.stop();
            meterProvider1.shutdown();
            meterProvider2.shutdown();
        }
    }

    @Test
    void stoppingOneReceiverDoesNotAffectTheOther() throws Exception {
        InMemoryMetricExporter exporter1 = InMemoryMetricExporter.create();
        InMemoryMetricExporter exporter2 = InMemoryMetricExporter.create();
        SdkMeterProvider meterProvider1 = buildMeterProvider(exporter1);
        SdkMeterProvider meterProvider2 = buildMeterProvider(exporter2);

        Db2Receiver receiver1 = new Db2Receiver(buildConfig(),
            meterProvider1.get("splunk-otel-db2"), (io.opentelemetry.api.logs.Logger) null);
        Db2Receiver receiver2 = new Db2Receiver(buildConfig(),
            meterProvider2.get("splunk-otel-db2"), (io.opentelemetry.api.logs.Logger) null);

        receiver1.start();
        receiver2.start();

        // Wait for both to produce at least one metric
        Awaitility.await()
            .atMost(20, TimeUnit.SECONDS)
            .until(() -> !metricNames(exporter1).isEmpty() && !metricNames(exporter2).isEmpty());

        // Stop receiver1; receiver2 should continue collecting
        receiver1.stop();
        meterProvider1.shutdown();

        exporter2.reset();

        // Give receiver2 time for two more collection cycles
        Thread.sleep(6000);

        try {
            assertThat(metricNames(exporter2)).contains("db2.connection.count");
        } finally {
            receiver2.stop();
            meterProvider2.shutdown();
        }
    }

    @Test
    void twoReceiversWithQuerySampleBothEmitRecords() throws Exception {
        io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter logExporter1 =
            io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter.create();
        io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter logExporter2 =
            io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter.create();

        io.opentelemetry.sdk.logs.SdkLoggerProvider logProvider1 =
            io.opentelemetry.sdk.logs.SdkLoggerProvider.builder()
                .addLogRecordProcessor(
                    io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor.create(logExporter1))
                .build();
        io.opentelemetry.sdk.logs.SdkLoggerProvider logProvider2 =
            io.opentelemetry.sdk.logs.SdkLoggerProvider.builder()
                .addLogRecordProcessor(
                    io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor.create(logExporter2))
                .build();

        InMemoryMetricExporter metricExporter1 = InMemoryMetricExporter.create();
        InMemoryMetricExporter metricExporter2 = InMemoryMetricExporter.create();
        SdkMeterProvider meterProvider1 = buildMeterProvider(metricExporter1);
        SdkMeterProvider meterProvider2 = buildMeterProvider(metricExporter2);

        Db2ReceiverConfig config1 = buildConfig();
        config1.setQuerySampleEnabled(true);
        config1.setQuerySampleInterval("1s");
        config1.setTopQueryEnabled(false);

        Db2ReceiverConfig config2 = buildConfig();
        config2.setQuerySampleEnabled(true);
        config2.setQuerySampleInterval("1s");
        config2.setTopQueryEnabled(false);

        java.util.Properties props = new java.util.Properties();
        props.setProperty("user",     db2.getUsername());
        props.setProperty("password", db2.getPassword());

        // DDL connection — separate autoCommit=true so rollback only releases the row lock
        java.sql.Connection ddlConn = java.sql.DriverManager.getConnection(db2.getJdbcUrl(), props);
        try (java.sql.Statement st = ddlConn.createStatement()) {
            try { st.execute("DROP TABLE SPLUNK_MULTI_LOCK_TEST"); } catch (Exception ignored) {}
            st.execute("CREATE TABLE SPLUNK_MULTI_LOCK_TEST (ID INT NOT NULL PRIMARY KEY, VAL INT)");
        }
        ddlConn.close();

        // ownerConn holds an X-lock via uncommitted INSERT
        java.sql.Connection ownerConn = java.sql.DriverManager.getConnection(db2.getJdbcUrl(), props);
        ownerConn.setAutoCommit(false);
        try (java.sql.Statement st = ownerConn.createStatement()) {
            st.execute("INSERT INTO SPLUNK_MULTI_LOCK_TEST VALUES (1, 99)");
        }

        // bgConn blocks in LOCKWAIT trying to read the locked row — this is the sampled query
        java.util.concurrent.ExecutorService exec = java.util.concurrent.Executors.newSingleThreadExecutor();
        java.sql.Connection bgConn = java.sql.DriverManager.getConnection(db2.getJdbcUrl(), props);
        bgConn.setAutoCommit(false);
        java.sql.Statement bgStmt = bgConn.createStatement();
        java.util.concurrent.Future<?> bgFuture = exec.submit(() -> {
            try {
                bgStmt.executeQuery("SELECT ID FROM SPLUNK_MULTI_LOCK_TEST WITH RR");
            } catch (Exception ignored) {}
        });

        // Let the background connection enter LOCKWAIT before starting the receivers
        Thread.sleep(2000);

        Db2Receiver receiver1 = new Db2Receiver(config1, meterProvider1.get("splunk-otel-db2"),
            logProvider1.get("splunk-otel-db2"));
        Db2Receiver receiver2 = new Db2Receiver(config2, meterProvider2.get("splunk-otel-db2"),
            logProvider2.get("splunk-otel-db2"));

        receiver1.start();
        receiver2.start();

        try {
            // Both receivers must independently observe and emit a query-sample record for the
            // blocked SELECT — confirms each has its own querySampleConnection and scheduler.
            Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> {
                    boolean r1HasRecord = logExporter1.getFinishedLogRecordItems().stream()
                        .anyMatch(r -> {
                            String text = r.getAttributes().get(
                                com.splunk.otel.db2.receiver.queries.QuerySampleEvent.DB_QUERY_TEXT);
                            return text != null && text.toUpperCase().contains("SPLUNK_MULTI_LOCK_TEST");
                        });
                    boolean r2HasRecord = logExporter2.getFinishedLogRecordItems().stream()
                        .anyMatch(r -> {
                            String text = r.getAttributes().get(
                                com.splunk.otel.db2.receiver.queries.QuerySampleEvent.DB_QUERY_TEXT);
                            return text != null && text.toUpperCase().contains("SPLUNK_MULTI_LOCK_TEST");
                        });
                    return r1HasRecord && r2HasRecord;
                });

            // Verify record structure from receiver1 (representative)
            io.opentelemetry.sdk.logs.data.LogRecordData record =
                logExporter1.getFinishedLogRecordItems().stream()
                    .filter(r -> {
                        String text = r.getAttributes().get(
                            com.splunk.otel.db2.receiver.queries.QuerySampleEvent.DB_QUERY_TEXT);
                        return text != null && text.toUpperCase().contains("SPLUNK_MULTI_LOCK_TEST");
                    })
                    .findFirst().orElseThrow();

            assertThat(record.getAttributes().get(
                com.splunk.otel.db2.receiver.queries.QuerySampleEvent.DB_SYSTEM))
                .isEqualTo("db2");
            assertThat(record.getAttributes().get(
                com.splunk.otel.db2.receiver.queries.QuerySampleEvent.DB2_MEMBER))
                .isNotNull();
            assertThat(record.getSeverity())
                .isEqualTo(io.opentelemetry.api.logs.Severity.INFO);
        } finally {
            ownerConn.rollback();
            ownerConn.close();
            bgStmt.cancel();
            bgConn.rollback();
            bgConn.close();
            bgFuture.cancel(true);
            exec.shutdownNow();
            java.sql.Connection cleanConn = java.sql.DriverManager.getConnection(db2.getJdbcUrl(), props);
            try (java.sql.Statement st = cleanConn.createStatement()) {
                st.execute("DROP TABLE SPLUNK_MULTI_LOCK_TEST");
            } finally { cleanConn.close(); }
            receiver1.stop();
            receiver2.stop();
            logProvider1.shutdown();
            logProvider2.shutdown();
            meterProvider1.shutdown();
            meterProvider2.shutdown();
        }
    }

    @Test
    void unreachableDatabaseDoesNotBlockHealthyOne() {
        // A7: one receiver targeting an unroutable address must not prevent the other from collecting
        InMemoryMetricExporter healthyExporter = InMemoryMetricExporter.create();
        SdkMeterProvider healthyProvider = buildMeterProvider(healthyExporter);

        InMemoryMetricExporter deadExporter = InMemoryMetricExporter.create();
        SdkMeterProvider deadProvider = buildMeterProvider(deadExporter);

        Db2ReceiverConfig healthyConfig = buildConfig();

        Db2ReceiverConfig deadConfig = new Db2ReceiverConfig();
        deadConfig.setHostname("10.255.255.1"); // unroutable — will time out
        deadConfig.setHostport(50000);
        deadConfig.setUsername("db2inst1");
        deadConfig.setPassword("password");
        deadConfig.setDatabase("NOPE");
        deadConfig.setCollectionInterval("2s");
        deadConfig.setConnectTimeoutSeconds(2);
        deadConfig.getTls().setInsecure(true);

        Db2Receiver healthyReceiver = new Db2Receiver(healthyConfig,
            healthyProvider.get("splunk-otel-db2"), (io.opentelemetry.api.logs.Logger) null);
        Db2Receiver deadReceiver = new Db2Receiver(deadConfig,
            deadProvider.get("splunk-otel-db2"), (io.opentelemetry.api.logs.Logger) null);

        deadReceiver.start();
        healthyReceiver.start();

        try {
            // The healthy receiver must emit metrics within 20s regardless of the dead one cycling backoff
            Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> metricNames(healthyExporter).contains("db2.connection.count"));

            assertThat(metricNames(healthyExporter)).contains("db2.connection.count");
        } finally {
            healthyReceiver.stop();
            deadReceiver.stop();
            healthyProvider.shutdown();
            deadProvider.shutdown();
        }
    }

    private static List<String> metricNames(InMemoryMetricExporter exporter) {
        return exporter.getFinishedMetricItems().stream()
            .map(m -> m.getName())
            .collect(Collectors.toList());
    }
}
