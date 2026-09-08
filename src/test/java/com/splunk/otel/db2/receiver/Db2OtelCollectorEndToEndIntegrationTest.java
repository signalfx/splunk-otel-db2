package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.config.OtlpExporterConfig;
import com.splunk.otel.db2.pipeline.Db2Pipeline;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test: live DB2 → splunk-otel-db2 scraper → OTel Collector.
 *
 * Topology:
 *   Db2Container (Testcontainers) — real IBM DB2 11.5 database
 *   CollectorContainer            — official otel/opentelemetry-collector-contrib image
 *                                   with debug exporter at verbosity:detailed
 *   Db2Pipeline (in-process)      — connects to DB2, exports via OTLP gRPC to the collector
 *
 * Assertions:
 *   - Collector container logs contain "db.server.query_sample" event records
 *     with db.system.name = "db2"
 *   - Collector container logs contain "db.server.top_query" event records
 *     with db.system.name = "db2"
 *   - Both event types carry db.namespace set to the configured database name
 *
 * Run with: ./gradlew integrationTest
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Db2OtelCollectorEndToEndIntegrationTest {

    private static final String COLLECTOR_IMAGE = "otel/opentelemetry-collector-contrib:0.127.0";
    private static final int COLLECTOR_OTLP_GRPC_PORT = 4317;

    @Container
    static final Db2Container db2 = new Db2Container("icr.io/db2_community/db2:11.5.9.0")
        .acceptLicense()
        .withUsername("db2inst1")
        .withPassword("password")
        .withDatabaseName("testdb")
        .withStartupTimeout(Duration.ofMinutes(10));

    @Container
    static final GenericContainer<?> collector = new GenericContainer<>(COLLECTOR_IMAGE)
        .withCopyFileToContainer(
            MountableFile.forClasspathResource("otel-collector-e2e.yaml"),
            "/etc/otel/config.yaml")
        .withCommand("--config=/etc/otel/config.yaml")
        .withExposedPorts(COLLECTOR_OTLP_GRPC_PORT)
        .waitingFor(Wait.forLogMessage(".*Everything is ready.*", 1)
            .withStartupTimeout(Duration.ofSeconds(60)));

    @Test
    void scraperEmitsQuerySampleAndTopQueryThroughRealCollector() throws Exception {
        // ── 1. Build a config pointing at the collector container ──────────────
        Db2ReceiverConfig rc = new Db2ReceiverConfig();
        rc.setHostname(db2.getHost());
        rc.setHostport(db2.getMappedPort(50000));
        rc.setUsername(db2.getUsername());
        rc.setPassword(db2.getPassword());
        rc.setDatabase(db2.getDatabaseName().toUpperCase());
        rc.setCollectionInterval("2s");
        rc.setQuerySampleEnabled(true);
        rc.setQuerySampleInterval("2s");
        rc.setTopQueryEnabled(false); // prevent start() scheduling 60s cycle; injected below
        rc.getTls().setInsecure(true);

        OtlpExporterConfig otlp = new OtlpExporterConfig();
        otlp.setEndpoint(
            "http://" + collector.getHost() + ":" + collector.getMappedPort(COLLECTOR_OTLP_GRPC_PORT));
        otlp.getTls().setInsecure(true);

        // ── 2. Create a lock-wait workload so query_sample has something to capture ──
        Properties props = new Properties();
        props.setProperty("user",     db2.getUsername());
        props.setProperty("password", db2.getPassword());

        // DDL on a fresh autoCommit connection — avoids mixing DDL into a transaction
        try (Connection ddlConn = DriverManager.getConnection(db2.getJdbcUrl(), props);
             Statement st = ddlConn.createStatement()) {
            try { st.execute("DROP TABLE E2E_LOCK_TEST"); } catch (Exception ignored) {}
            st.execute("CREATE TABLE E2E_LOCK_TEST (ID INT NOT NULL PRIMARY KEY, VAL INT)");
        }

        // ownerConn holds an exclusive INSERT lock — blocks any reader using WITH RR
        Connection ownerConn = DriverManager.getConnection(db2.getJdbcUrl(), props);
        ownerConn.setAutoCommit(false);
        try (Statement st = ownerConn.createStatement()) {
            st.execute("INSERT INTO E2E_LOCK_TEST VALUES (1, 42)");
        }

        ExecutorService exec = Executors.newSingleThreadExecutor();
        Connection bgConn = DriverManager.getConnection(db2.getJdbcUrl(), props);
        bgConn.setAutoCommit(false);
        Statement bgStmt = bgConn.createStatement();
        Future<?> bgFuture = exec.submit(() -> {
            try {
                bgStmt.executeQuery("SELECT ID FROM E2E_LOCK_TEST WITH RR");
            } catch (Exception ignored) {}
        });

        // Give the background connection time to hit the lock before the pipeline polls
        Thread.sleep(2000);

        // ── 3. Start the pipeline — it will connect to real DB2 and real Collector ──
        Db2Pipeline pipeline = new Db2Pipeline(rc, otlp);
        pipeline.start();
        // Cycle 1 (baseline): scheduleWithFixedDelay fires immediately with initialDelay=0
        // but the scrape runs asynchronously. Wait one full cycle (6s > 5s interval) so the
        // seed collection finishes before the workload runs. If the workload executes inside
        // the first scrape window it becomes part of the baseline and produces a zero delta
        // on cycle 2, causing an intermittent 90-second timeout.
        pipeline.startTopQueryScheduler(5L); // 5s cycle — bypass the 60s minimum
        Thread.sleep(6000); // wait for seed scrape to complete

        // Cycle 2+ workload: this query now appears after the baseline and will be emitted
        // as a delta on the next collection. Cross-join on SYSCAT.COLUMNS is guaranteed to
        // produce non-zero STMT_EXEC_TIME even on minimal containers.
        try (Connection workConn = DriverManager.getConnection(db2.getJdbcUrl(), props);
             Statement st = workConn.createStatement()) {
            for (int i = 0; i < 3; i++) {
                st.executeQuery(
                    "SELECT COUNT(*) AS E2E_TOPQ_PROBE " +
                    "FROM SYSCAT.COLUMNS A, SYSCAT.COLUMNS B " +
                    "WHERE A.TABSCHEMA = B.TABSCHEMA").close();
            }
        }

        try {
            // ── 4. Wait for query_sample to appear in collector logs ────────────────
            // verbosity:detailed prints every attribute on its own line:
            //   -> "db.server.query_sample"
            //   -> db.system.name: Str(db2)
            Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(3, TimeUnit.SECONDS)
                .alias("query_sample in collector logs")
                .until(() -> {
                    String logs = collector.getLogs();
                    return logs.contains("db.server.query_sample")
                        && logs.contains("Str(db2)");
                });

            // ── 5. Wait for top_query to appear in collector logs ───────────────────
            Awaitility.await()
                .atMost(90, TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .alias("top_query in collector logs")
                .until(() -> collector.getLogs().contains("db.server.top_query"));

            // ── 6. Assert key attributes in collector output ────────────────────────
            String logs = collector.getLogs();

            assertThat(logs)
                .as("collector must have received query_sample events")
                .contains("db.server.query_sample");

            assertThat(logs)
                .as("collector must have received top_query events")
                .contains("db.server.top_query");

            assertThat(logs)
                .as("db.system.name must be 'db2")
                .contains("Str(db2)");

            // db.namespace = database name (TESTDB)
            assertThat(logs)
                .as("db.namespace must be set to the configured database name")
                .containsIgnoringCase(rc.getDatabase());

            // The lock-wait query text must appear (obfuscated — no literals)
            assertThat(logs)
                .as("E2E_LOCK_TEST table name must appear in a query_sample record")
                .containsIgnoringCase("E2E_LOCK_TEST");

        } finally {
            pipeline.stop();

            ownerConn.rollback();
            ownerConn.close();
            bgStmt.cancel();
            bgConn.rollback();
            bgConn.close();
            bgFuture.cancel(true);
            exec.shutdownNow();

            try (Connection c = DriverManager.getConnection(db2.getJdbcUrl(), props);
                 Statement s = c.createStatement()) {
                s.execute("DROP TABLE E2E_LOCK_TEST");
            }
        }
    }
}
