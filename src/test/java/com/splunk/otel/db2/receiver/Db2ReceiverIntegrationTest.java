package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test — spins up a real DB2 container via Testcontainers,
 * seeds it with a schema + data, runs read/write load, then asserts
 * that the receiver emits correct metrics with real non-zero values.
 *
 * Run with: ./gradlew integrationTest
 * Skipped by default in ./gradlew build (requires Docker).
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Db2ReceiverIntegrationTest {

    @Container
    static final Db2Container db2 = new Db2Container("icr.io/db2_community/db2:latest")
        .acceptLicense()
        .withUsername("db2inst1")
        .withPassword("password")
        .withDatabaseName("testdb")
        .withStartupTimeout(Duration.ofMinutes(10));

    private static final int ROW_COUNT   = 5_000;
    private static final int READ_PASSES = 20;

    private InMemoryMetricExporter exporter;
    private SdkMeterProvider       meterProvider;
    private Db2Receiver            receiver;

    // ── lifecycle ──────────────────────────────────────────────────────────────

    @BeforeAll
    void setUp() throws Exception {
        seedDatabase();

        exporter = InMemoryMetricExporter.create();
        meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setInterval(Duration.ofSeconds(2))
                    .build()
            )
            .build();

        Db2ReceiverConfig config = new Db2ReceiverConfig();
        config.setHostname(db2.getHost());
        config.setHostport(db2.getMappedPort(50000));
        config.setUsername(db2.getUsername());
        config.setPassword(db2.getPassword());
        config.setDatabase(db2.getDatabaseName().toUpperCase());

        receiver = new Db2Receiver(config, meterProvider.get("splunk-otel-db2"), (io.opentelemetry.api.logs.Logger) null);
        receiver.start();
    }

    @AfterAll
    void tearDown() {
        if (receiver    != null) receiver.stop();
        if (meterProvider != null) meterProvider.shutdown();
    }

    // ── tests ──────────────────────────────────────────────────────────────────

    @Test
    void allExpectedMetricsAreEmitted() {
        Set<String> expected = Set.of(
            "db2.user.connection.count",
            "db2.deadlocks",
            "db2.lock.waits",
            "db2.lock.wait.time",
            "db2.lock.timeouts",
            "db2.buffer_pool.hit_ratio",
            "db2.buffer_pool.reads",
            "db2.buffer_pool.physical_reads",
            "db2.tablespace.size"
        );

        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                Set<String> actual = exporter.getFinishedMetricItems().stream()
                    .map(MetricData::getName)
                    .collect(Collectors.toSet());
                assertThat(actual).containsAll(expected);
            });
    }

    @Test
    void connectionCountIsAtLeastOne() {
        // seeding + receiver open at least 2 connections
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                MetricData m = requireMetric("db2.user.connection.count");
                m.getLongSumData().getPoints().forEach(p ->
                    assertThat(p.getValue())
                        .as("connection count should be >= 1")
                        .isGreaterThanOrEqualTo(1L)
                );
            });
    }

    @Test
    void bufferPoolReadsAreNonZeroAfterLoad() {
        // ROW_COUNT rows were read READ_PASSES times in seedDatabase() — logical reads must be > 0
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                MetricData m = requireMetric("db2.buffer_pool.reads");
                long totalReads = longSumPoints(m).mapToLong(p -> p.getValue()).sum();
                assertThat(totalReads)
                    .as("buffer pool logical reads should be > 0 after load")
                    .isGreaterThan(0L);
            });
    }

    @Test
    void bufferPoolHitRatioIsBetweenZeroAndHundred() {
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                MetricData m = requireMetric("db2.buffer_pool.hit_ratio");
                m.getDoubleGaugeData().getPoints().forEach(p ->
                    assertThat(p.getValue())
                        .as("hit ratio should be 0–100")
                        .isBetween(0.0, 100.0)
                );
            });
    }

    @Test
    void tablespaceUsedIsGreaterThanZero() {
        // inserted ROW_COUNT rows — tablespace.used must be > 0
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                MetricData m = requireMetric("db2.tablespace.size");
                long usedBytes = longGaugePoints(m)
                    .filter(p -> "used".equals(p.getAttributes().get(AttributeKey.stringKey("db2.tablespace.state"))))
                    .mapToLong(p -> p.getValue())
                    .sum();
                assertThat(usedBytes)
                    .as("tablespace used bytes should be > 0 after row inserts")
                    .isGreaterThan(0L);
            });
    }

    @Test
    void tablespaceCapacityExceedsUsed() {
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                MetricData m = requireMetric("db2.tablespace.size");
                AttributeKey<String> stateKey = AttributeKey.stringKey("db2.tablespace.state");

                long used     = longGaugePoints(m).filter(p -> "used"    .equals(p.getAttributes().get(stateKey))).mapToLong(p -> p.getValue()).sum();
                long capacity = longGaugePoints(m).filter(p -> "capacity".equals(p.getAttributes().get(stateKey))).mapToLong(p -> p.getValue()).sum();

                assertThat(capacity)
                    .as("tablespace capacity should be >= used")
                    .isGreaterThanOrEqualTo(used);
            });
    }

    @Test
    void bufferPoolMetricsHaveCorrectDimensionAttributes() {
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                MetricData reads = requireMetric("db2.buffer_pool.reads");
                Set<String> readTypes = longSumPoints(reads)
                    .map(p -> p.getAttributes().get(AttributeKey.stringKey("db2.read_type")))
                    .collect(Collectors.toSet());
                assertThat(readTypes).containsAll(Set.of("data", "index"));

                allPoints(reads).forEach(p ->
                    assertThat(p.getAttributes().get(AttributeKey.stringKey("db2.buffer_pool")))
                        .as("db2.buffer_pool attribute must be present")
                        .isNotNull()
                );
            });
    }

    @Test
    void tablespaceStateHasAllThreeCategories() {
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                MetricData m = requireMetric("db2.tablespace.size");
                Set<String> states = longGaugePoints(m)
                    .map(p -> p.getAttributes().get(AttributeKey.stringKey("db2.tablespace.state")))
                    .collect(Collectors.toSet());
                assertThat(states).containsAll(Set.of("used", "free", "capacity"));
            });
    }

    @Test
    void allMetricsCarryDbNamespaceAttribute() {
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                List<MetricData> items = exporter.getFinishedMetricItems();
                assertThat(items).isNotEmpty();
                for (MetricData metric : items) {
                    allPoints(metric).forEach(point ->
                        assertThat(point.getAttributes().get(AttributeKey.stringKey("db.namespace")))
                            .as("db.namespace on %s", metric.getName())
                            .isNotNull().isNotBlank()
                    );
                }
            });
    }

    // ── seed ───────────────────────────────────────────────────────────────────

    /**
     * Creates a test schema + table, inserts ROW_COUNT rows, then runs
     * full-table scans READ_PASSES times to drive buffer pool logical reads
     * to a measurable value.
     */
    private void seedDatabase() throws Exception {
        Properties props = new Properties();
        props.setProperty("user",     db2.getUsername());
        props.setProperty("password", db2.getPassword());

        try (Connection conn = DriverManager.getConnection(db2.getJdbcUrl(), props)) {
            conn.setAutoCommit(false);

            try (Statement st = conn.createStatement()) {
                // schema
                st.execute(
                    "CREATE TABLE IF NOT EXISTS DB2INST1.SPLUNK_LOAD_TEST (" +
                    "  ID         INTEGER NOT NULL," +
                    "  CATEGORY   VARCHAR(32)," +
                    "  VALUE      DECIMAL(15,2)," +
                    "  CREATED_AT TIMESTAMP DEFAULT CURRENT TIMESTAMP," +
                    "  PRIMARY KEY (ID)" +
                    ")"
                );
                conn.commit();
            }

            // insert ROW_COUNT rows in batches of 500
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO DB2INST1.SPLUNK_LOAD_TEST (ID, CATEGORY, VALUE) VALUES (?, ?, ?)")) {
                int batch = 0;
                for (int i = 1; i <= ROW_COUNT; i++) {
                    ps.setInt(1, i);
                    ps.setString(2, "category_" + (i % 10));
                    ps.setBigDecimal(3, new java.math.BigDecimal(i * 1.5));
                    ps.addBatch();
                    if (++batch == 500) {
                        ps.executeBatch();
                        conn.commit();
                        batch = 0;
                    }
                }
                if (batch > 0) { ps.executeBatch(); conn.commit(); }
            }

            // full-table scans + aggregations to drive buffer pool reads
            for (int pass = 0; pass < READ_PASSES; pass++) {
                try (Statement st = conn.createStatement()) {
                    // full scan
                    try (ResultSet rs = st.executeQuery(
                            "SELECT COUNT(*), SUM(VALUE), MAX(VALUE) FROM DB2INST1.SPLUNK_LOAD_TEST")) {
                        rs.next();
                    }
                    // category group-by — forces index + data reads
                    try (ResultSet rs = st.executeQuery(
                            "SELECT CATEGORY, COUNT(*), AVG(VALUE) " +
                            "FROM DB2INST1.SPLUNK_LOAD_TEST " +
                            "GROUP BY CATEGORY ORDER BY CATEGORY")) {
                        while (rs.next()) { /* drain */ }
                    }
                    // range scan
                    try (ResultSet rs = st.executeQuery(
                            "SELECT ID, VALUE FROM DB2INST1.SPLUNK_LOAD_TEST " +
                            "WHERE ID BETWEEN 1 AND " + (ROW_COUNT / 2))) {
                        while (rs.next()) { /* drain */ }
                    }
                }
            }
            conn.commit();
        }
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private MetricData requireMetric(String name) {
        return exporter.getFinishedMetricItems().stream()
            .filter(m -> m.getName().equals(name))
            .findFirst()
            .orElseThrow(() -> new AssertionError(name + " not found in exported metrics"));
    }

    @SuppressWarnings("unchecked")
    private Stream<? extends PointData> allPoints(MetricData metric) {
        switch (metric.getType()) {
            case LONG_SUM:               return metric.getLongSumData().getPoints().stream();
            case DOUBLE_SUM:             return metric.getDoubleSumData().getPoints().stream();
            case LONG_GAUGE:             return metric.getLongGaugeData().getPoints().stream();
            case DOUBLE_GAUGE:           return metric.getDoubleGaugeData().getPoints().stream();
            case HISTOGRAM:              return metric.getHistogramData().getPoints().stream();
            case EXPONENTIAL_HISTOGRAM:  return metric.getExponentialHistogramData().getPoints().stream();
            default:                     return Stream.empty();
        }
    }

    private Stream<io.opentelemetry.sdk.metrics.data.LongPointData> longSumPoints(MetricData m) {
        return m.getLongSumData().getPoints().stream();
    }

    private Stream<io.opentelemetry.sdk.metrics.data.LongPointData> longGaugePoints(MetricData m) {
        return m.getLongGaugeData().getPoints().stream();
    }
}
