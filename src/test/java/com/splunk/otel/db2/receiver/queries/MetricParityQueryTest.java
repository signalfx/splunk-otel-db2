package com.splunk.otel.db2.receiver.queries;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the db-agent metric parity queries: SqlActivityQuery,
 * SortAndDirectIoQuery and TransactionLogQuery.
 */
class MetricParityQueryTest {

    private InMemoryMetricReader reader;
    private Meter meter;

    @BeforeEach
    void setUp() {
        reader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(reader)
            .build();
        meter = meterProvider.get("splunk-otel-db2");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /** Single-row ResultSet; unlisted columns return 0. */
    private static ResultSet singleRow(int member, Map<String, Long> cols) throws Exception {
        ResultSet rs = Mockito.mock(ResultSet.class);
        when(rs.next()).thenReturn(true, false);
        when(rs.getInt("MEMBER")).thenReturn(member);
        when(rs.getInt("DBPARTITIONNUM")).thenReturn(member);
        when(rs.getLong(anyString())).thenReturn(0L);
        for (Map.Entry<String, Long> e : cols.entrySet()) {
            when(rs.getLong(e.getKey())).thenReturn(e.getValue());
        }
        return rs;
    }

    /** Routes executeQuery to a ResultSet based on a distinctive fragment of the SQL. */
    private static Connection routingConnection(Map<String, ResultSet> byFragment) throws Exception {
        Connection conn = Mockito.mock(Connection.class);
        Statement st = Mockito.mock(Statement.class);
        when(conn.createStatement()).thenReturn(st);
        when(st.executeQuery(anyString())).thenAnswer(inv -> {
            String sql = inv.getArgument(0, String.class);
            for (Map.Entry<String, ResultSet> e : byFragment.entrySet()) {
                if (sql.contains(e.getKey())) {
                    return e.getValue();
                }
            }
            throw new SQLException("no stub for SQL: " + sql);
        });
        return conn;
    }

    private static Map<String, Long> cols(Object... kv) {
        Map<String, Long> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], ((Number) kv[i + 1]).longValue());
        }
        return m;
    }

    /**
     * Performs a collection. The mocked ResultSets are one-shot, so a second collection in the
     * same test observes no rows and every metric disappears — which silently turns a negative
     * assertion into a no-op. Call this at most once per test, or use
     * {@link #namesOf(Collection)} on an already-captured collection.
     */
    private Set<String> metricNames() {
        return namesOf(reader.collectAllMetrics());
    }

    /** Metric names from an already-collected result, performing no further collection. */
    private static Set<String> namesOf(Collection<MetricData> metrics) {
        return metrics.stream().map(MetricData::getName).collect(Collectors.toSet());
    }

    private Collection<MetricData> collect() {
        return reader.collectAllMetrics();
    }

    private static MetricData metric(Collection<MetricData> metrics, String name) {
        return metrics.stream()
            .filter(m -> m.getName().equals(name))
            .findFirst()
            .orElseThrow(() -> new AssertionError("metric not found: " + name
                + " (present: " + metrics.stream().map(MetricData::getName).sorted().collect(Collectors.toList()) + ")"));
    }

    private static Collection<? extends PointData> points(MetricData md) {
        return md.getData().getPoints();
    }

    /** Numeric value of the single point carrying the given attributes, as a double. */
    private static double valueOf(Collection<MetricData> metrics, String name, Attributes attrs) {
        MetricData md = metric(metrics, name);
        return points(md).stream()
            .filter(p -> attrs.asMap().entrySet().stream()
                .allMatch(e -> {
                    @SuppressWarnings("unchecked")
                    AttributeKey<Object> k = (AttributeKey<Object>) e.getKey();
                    return e.getValue().equals(p.getAttributes().get(k));
                }))
            .map(MetricParityQueryTest::numericValue)
            .findFirst()
            .orElseThrow(() -> new AssertionError("no point on " + name + " with " + attrs
                + " (points: " + points(md).stream().map(p -> p.getAttributes().toString()).collect(Collectors.toList()) + ")"));
    }

    private static double numericValue(PointData p) {
        if (p instanceof io.opentelemetry.sdk.metrics.data.LongPointData) {
            return ((io.opentelemetry.sdk.metrics.data.LongPointData) p).getValue();
        }
        return ((io.opentelemetry.sdk.metrics.data.DoublePointData) p).getValue();
    }

    private static Attributes attrs(int member, String key, String value) {
        return Attributes.builder().put("db2.member", member).put(key, value).build();
    }

    private static Attributes memberAttrs(int member) {
        return Attributes.builder().put("db2.member", member).build();
    }

    /** Distinct values of a string attribute across all points of a metric. */
    private static Set<String> attributeValues(Collection<MetricData> metrics, String name, String key) {
        return points(metric(metrics, name)).stream()
            .map(p -> p.getAttributes().get(AttributeKey.stringKey(key)))
            .filter(java.util.Objects::nonNull)
            .collect(Collectors.toSet());
    }

    // ── SqlActivityQuery ─────────────────────────────────────────────────────

    private Connection sqlActivityConnection() throws Exception {
        Map<String, ResultSet> routes = new HashMap<>();
        routes.put("MON_GET_DATABASE", singleRow(0, cols(
            "DYNAMIC_SQL_STMTS", 11, "STATIC_SQL_STMTS", 12, "FAILED_SQL_STMTS", 13,
            "SELECT_SQL_STMTS", 14, "UID_SQL_STMTS", 15, "DDL_SQL_STMTS", 16,
            "TOTAL_APP_COMMITS", 21, "TOTAL_APP_ROLLBACKS", 22,
            "INT_COMMITS", 23, "INT_ROLLBACKS", 24,
            "ROWS_READ", 31, "ROWS_RETURNED", 32, "ROWS_MODIFIED", 200,
            "ROWS_INSERTED", 34, "ROWS_UPDATED", 35, "ROWS_DELETED", 36,
            "INT_ROWS_INSERTED", 37, "INT_ROWS_UPDATED", 38, "INT_ROWS_DELETED", 39,
            "BINDS_PRECOMPILES", 41, "IMPLICIT_REBINDS", 42,
            "TOTAL_COMPILATIONS", 43, "TOTAL_COMPILE_TIME", 2500)));
        return routingConnection(routes);
    }

    @Test
    void sqlActivityRegistersExpectedMetrics() throws Exception {
        Connection conn = sqlActivityConnection();
        new SqlActivityQuery(() -> conn, "TESTDB", meter, false);

        assertThat(metricNames()).contains(
            "db2.sql.statement.count",
            "db2.sql.operation.count",
            "db2.sql.failed.count",
            "db2.transaction.count",
            "db2.row.count",
            "db2.row.modified.count",
            "db2.row.read.count",
            "db2.row.returned.count",
            "db2.sql.compilation.count",
            "db2.sql.compilation.time",
            "db2.sql.bind.count",
            "db2.sql.rebind.count"
        );
    }

    @Test
    void preparationAndOperationAxesAreSeparateMetrics() throws Exception {
        Connection conn = sqlActivityConnection();
        new SqlActivityQuery(() -> conn, "TESTDB", meter, false);

        Collection<MetricData> metrics = collect();
        assertThat(valueOf(metrics, "db2.sql.statement.count", attrs(0, "db2.statement_type", "dynamic"))).isEqualTo(11);
        assertThat(valueOf(metrics, "db2.sql.statement.count", attrs(0, "db2.statement_type", "static"))).isEqualTo(12);
        assertThat(valueOf(metrics, "db2.sql.operation.count", attrs(0, "db2.operation_type", "select"))).isEqualTo(14);
        assertThat(valueOf(metrics, "db2.sql.operation.count", attrs(0, "db2.operation_type", "uid"))).isEqualTo(15);
        assertThat(valueOf(metrics, "db2.sql.operation.count", attrs(0, "db2.operation_type", "ddl"))).isEqualTo(16);
        assertThat(valueOf(metrics, "db2.sql.failed.count", memberAttrs(0))).isEqualTo(13);
    }

    @Test
    void additiveMetricsDoNotMixOverlappingAxes() throws Exception {
        Connection conn = sqlActivityConnection();
        new SqlActivityQuery(() -> conn, "TESTDB", meter, false);

        Collection<MetricData> metrics = collect();

        // db2.sql.statement.count must carry the preparation axis only: mixing in
        // select/uid/ddl would double-count, since a dynamic SELECT is in both.
        assertThat(attributeValues(metrics, "db2.sql.statement.count", "db2.statement_type"))
            .containsExactlyInAnyOrder("dynamic", "static");
        assertThat(attributeValues(metrics, "db2.sql.operation.count", "db2.operation_type"))
            .containsExactlyInAnyOrder("select", "uid", "ddl");

        // db2.row.count must carry only the mutually exclusive modification partition;
        // ROWS_MODIFIED is an aggregate of these and must not appear alongside them.
        assertThat(attributeValues(metrics, "db2.row.count", "db2.row_operation"))
            .containsExactlyInAnyOrder("inserted", "updated", "deleted");
        assertThat(valueOf(metrics, "db2.row.read.count", memberAttrs(0))).isEqualTo(31);
        assertThat(valueOf(metrics, "db2.row.returned.count", memberAttrs(0))).isEqualTo(32);
    }

    @Test
    void rowsModifiedIsReportedIndependentlyOfThePartition() throws Exception {
        // ROWS_MODIFIED is not the sum of inserted+updated+deleted: DB2 also counts rows changed
        // by triggers and referential integrity, so it must not be derived or dropped. The
        // fixture deliberately sets it higher than the partition total (34+35+36 = 105).
        Connection conn = sqlActivityConnection();
        new SqlActivityQuery(() -> conn, "TESTDB", meter, false);

        Collection<MetricData> metrics = collect();
        assertThat(valueOf(metrics, "db2.row.modified.count", memberAttrs(0))).isEqualTo(200);
        assertThat(attributeValues(metrics, "db2.row.count", "db2.row_operation"))
            .doesNotContain("modified");
    }

    @Test
    void sqlActivitySeparatesInternalFromApplicationActivity() throws Exception {
        Connection conn = sqlActivityConnection();
        new SqlActivityQuery(() -> conn, "TESTDB", meter, false);

        Collection<MetricData> metrics = collect();
        Attributes appCommit = Attributes.builder()
            .put("db2.member", 0).put("db2.transaction_type", "commit").put("db2.internal", false).build();
        Attributes intCommit = Attributes.builder()
            .put("db2.member", 0).put("db2.transaction_type", "commit").put("db2.internal", true).build();
        assertThat(valueOf(metrics, "db2.transaction.count", appCommit)).isEqualTo(21);
        assertThat(valueOf(metrics, "db2.transaction.count", intCommit)).isEqualTo(23);

        Attributes appInsert = Attributes.builder()
            .put("db2.member", 0).put("db2.row_operation", "inserted").put("db2.internal", false).build();
        Attributes intInsert = Attributes.builder()
            .put("db2.member", 0).put("db2.row_operation", "inserted").put("db2.internal", true).build();
        assertThat(valueOf(metrics, "db2.row.count", appInsert)).isEqualTo(34);
        assertThat(valueOf(metrics, "db2.row.count", intInsert)).isEqualTo(37);
    }

    @Test
    void compileTimeIsConvertedFromMillisecondsToSeconds() throws Exception {
        Connection conn = sqlActivityConnection();
        new SqlActivityQuery(() -> conn, "TESTDB", meter, false);

        // TOTAL_COMPILE_TIME is milliseconds: 2500ms = 2.5s
        assertThat(valueOf(collect(), "db2.sql.compilation.time", memberAttrs(0))).isEqualTo(2.5);
    }

    @Test
    void snapshotColumnsAreNotQueriedWhenSnapshotMonitorDisabled() throws Exception {
        // Only MON_GET_DATABASE is stubbed; a SNAP_GET_DB attempt would throw and be logged,
        // so assert the snapshot-only dimensions are simply absent.
        Connection conn = sqlActivityConnection();
        new SqlActivityQuery(() -> conn, "TESTDB", meter, false);

        assertThat(metricNames()).doesNotContain("db2.deadlock.rollback.count");
    }

    @Test
    void snapshotContributesOnlyTheDeadlockRollbackMetric() throws Exception {
        // The other SNAP_GET_DB columns duplicate MON_GET_DATABASE values, so emitting them
        // would double-count. Only INT_DEADLOCK_ROLLBACKS has no native equivalent.
        Map<String, ResultSet> routes = new HashMap<>();
        routes.put("MON_GET_DATABASE", singleRow(0, cols(
            "DYNAMIC_SQL_STMTS", 11, "IMPLICIT_REBINDS", 42, "ROWS_RETURNED", 32)));
        routes.put("SNAP_GET_DB", singleRow(0, cols("INT_DEADLOCK_ROLLBACKS", 54)));
        Connection conn = routingConnection(routes);
        new SqlActivityQuery(() -> conn, "TESTDB", meter, true);

        Collection<MetricData> metrics = collect();
        assertThat(valueOf(metrics, "db2.deadlock.rollback.count", memberAttrs(0))).isEqualTo(54);

        // No duplicate of a natively available quantity.
        assertThat(attributeValues(metrics, "db2.sql.statement.count", "db2.statement_type"))
            .doesNotContain("commit", "rollback");
        assertThat(attributeValues(metrics, "db2.row.count", "db2.row_operation"))
            .doesNotContain("selected", "returned", "modified");
        assertThat(valueOf(metrics, "db2.sql.rebind.count", memberAttrs(0))).isEqualTo(42);
    }

    @Test
    void snapshotFailureDoesNotDiscardMonGetDatabaseMetrics() throws Exception {
        Map<String, ResultSet> routes = new HashMap<>();
        routes.put("MON_GET_DATABASE", singleRow(0, cols("DYNAMIC_SQL_STMTS", 11)));
        // SNAP_GET_DB deliberately unstubbed: routingConnection throws SQLException for it.
        Connection conn = routingConnection(routes);
        new SqlActivityQuery(() -> conn, "TESTDB", meter, true);

        assertThat(valueOf(collect(), "db2.sql.statement.count", attrs(0, "db2.statement_type", "dynamic")))
            .isEqualTo(11);
    }

    // ── SortAndDirectIoQuery ─────────────────────────────────────────────────

    @Test
    void sortAndDirectIoRecordsValuesWithCorrectUnits() throws Exception {
        Map<String, ResultSet> routes = new HashMap<>();
        routes.put("MON_GET_DATABASE", singleRow(0, cols(
            "SORT_HEAP_ALLOCATED", 100,
            "SORT_SHRHEAP_ALLOCATED", 2, "SORT_SHRHEAP_TOP", 3,
            "TOTAL_SECTION_SORT_TIME", 1500,
            "ACTIVE_SORTS", 4, "ACTIVE_HASH_JOINS", 5, "ACTIVE_OLAP_FUNCS", 6,
            "DIRECT_READS", 71, "DIRECT_WRITES", 72,
            "DIRECT_READ_REQS", 73, "DIRECT_WRITE_REQS", 74,
            "DIRECT_READ_TIME", 3000, "DIRECT_WRITE_TIME", 4000)));
        Connection conn = routingConnection(routes);
        new SortAndDirectIoQuery(() -> conn, "TESTDB", meter);

        Collection<MetricData> metrics = collect();

        // SORT_HEAP_ALLOCATED stays in pages (Db2 states no page size for it)
        assertThat(valueOf(metrics, "db2.sort.heap.allocated", memberAttrs(0))).isEqualTo(100);
        // SORT_SHRHEAP_* are documented in 4KB pages and converted to bytes
        assertThat(valueOf(metrics, "db2.sort.shared.heap.allocated", memberAttrs(0))).isEqualTo(2 * 4096);
        assertThat(valueOf(metrics, "db2.sort.shared.heap.max", memberAttrs(0))).isEqualTo(3 * 4096);
        // TOTAL_SECTION_SORT_TIME is milliseconds
        assertThat(valueOf(metrics, "db2.sort.time", memberAttrs(0))).isEqualTo(1.5);

        assertThat(valueOf(metrics, "db2.sort.active", memberAttrs(0))).isEqualTo(4);
        assertThat(valueOf(metrics, "db2.hash_join.active", memberAttrs(0))).isEqualTo(5);
        assertThat(valueOf(metrics, "db2.olap_function.active", memberAttrs(0))).isEqualTo(6);

        assertThat(valueOf(metrics, "db2.direct.io.count", attrs(0, "disk.io.direction", "read"))).isEqualTo(71);
        assertThat(valueOf(metrics, "db2.direct.io.count", attrs(0, "disk.io.direction", "write"))).isEqualTo(72);
        assertThat(valueOf(metrics, "db2.direct.io.request.count", attrs(0, "disk.io.direction", "read"))).isEqualTo(73);
        assertThat(valueOf(metrics, "db2.direct.io.request.count", attrs(0, "disk.io.direction", "write"))).isEqualTo(74);
        // DIRECT_*_TIME are milliseconds
        assertThat(valueOf(metrics, "db2.direct.io.time", attrs(0, "disk.io.direction", "read"))).isEqualTo(3.0);
        assertThat(valueOf(metrics, "db2.direct.io.time", attrs(0, "disk.io.direction", "write"))).isEqualTo(4.0);
    }

    @Test
    void sortAndDirectIoDoesNotDuplicateDatabaseQuerySortMetrics() throws Exception {
        Map<String, ResultSet> routes = new HashMap<>();
        routes.put("MON_GET_DATABASE", singleRow(0, cols("ACTIVE_SORTS", 1)));
        Connection conn = routingConnection(routes);
        new SortAndDirectIoQuery(() -> conn, "TESTDB", meter);

        // These two belong to DatabaseQuery and must not be re-registered here.
        assertThat(metricNames()).doesNotContain("db2.sort.count", "db2.sort.overflow.count");
    }

    // ── TransactionLogQuery ──────────────────────────────────────────────────

    private Connection transactionLogConnection() throws Exception {
        Map<String, ResultSet> routes = new LinkedHashMap<>();
        // Ordering matters: MON_GET_TRANSACTION_LOG also contains "MON_GET_TRANSACTION"
        routes.put("MON_GET_TRANSACTION_LOG", singleRow(0, cols(
            "TOTAL_LOG_AVAILABLE", 900, "TOTAL_LOG_USED", 100,
            "SEC_LOG_USED_TOP", 200, "TOT_LOG_USED_TOP", 300,
            "SEC_LOGS_ALLOCATED", 2,
            "LOG_READS", 81, "LOG_WRITES", 82,
            "LOG_READ_TIME", 5000, "LOG_WRITE_TIME", 6000,
            "NUM_LOG_READ_IO", 83, "NUM_LOG_WRITE_IO", 84,
            "NUM_LOG_PART_PAGE_IO", 85,
            "NUM_LOG_BUFFER_FULL", 86, "NUM_LOG_DATA_FOUND_IN_BUFFER", 87,
            "LOG_TO_REDO_FOR_RECOVERY", 88, "LOG_HELD_BY_DIRTY_PAGES", 89,
            "LOG_HADR_WAIT_TIME", 7000, "LOG_HADR_WAITS_TOTAL", 90)));
        routes.put("MON_GET_DATABASE", singleRow(0, cols(
            "LOG_DISK_WAIT_TIME", 8000, "LOG_DISK_WAITS_TOTAL", 91)));
        return routingConnection(routes);
    }

    @Test
    void transactionLogRecordsValuesWithCorrectUnits() throws Exception {
        Connection conn = transactionLogConnection();
        new TransactionLogQuery(() -> conn, "TESTDB", meter);

        Collection<MetricData> metrics = collect();

        assertThat(valueOf(metrics, "db2.log.space.available", memberAttrs(0))).isEqualTo(900);
        assertThat(valueOf(metrics, "db2.log.space.used", memberAttrs(0))).isEqualTo(100);
        assertThat(valueOf(metrics, "db2.log.space.used.max", attrs(0, "db2.log_space_type", "secondary"))).isEqualTo(200);
        assertThat(valueOf(metrics, "db2.log.space.used.max", attrs(0, "db2.log_space_type", "total"))).isEqualTo(300);
        assertThat(valueOf(metrics, "db2.log.secondary_files.allocated", memberAttrs(0))).isEqualTo(2);

        assertThat(valueOf(metrics, "db2.log.io.pages", attrs(0, "disk.io.direction", "read"))).isEqualTo(81);
        assertThat(valueOf(metrics, "db2.log.io.pages", attrs(0, "disk.io.direction", "write"))).isEqualTo(82);
        // LOG_READ_TIME / LOG_WRITE_TIME are milliseconds
        assertThat(valueOf(metrics, "db2.log.io.time", attrs(0, "disk.io.direction", "read"))).isEqualTo(5.0);
        assertThat(valueOf(metrics, "db2.log.io.time", attrs(0, "disk.io.direction", "write"))).isEqualTo(6.0);
        assertThat(valueOf(metrics, "db2.log.io.count", attrs(0, "disk.io.direction", "read"))).isEqualTo(83);
        assertThat(valueOf(metrics, "db2.log.io.count", attrs(0, "disk.io.direction", "write"))).isEqualTo(84);

        assertThat(valueOf(metrics, "db2.log.partial_page_io.count", memberAttrs(0))).isEqualTo(85);
        assertThat(valueOf(metrics, "db2.log.buffer_full.count", memberAttrs(0))).isEqualTo(86);
        assertThat(valueOf(metrics, "db2.log.buffer_hit.count", memberAttrs(0))).isEqualTo(87);
        assertThat(valueOf(metrics, "db2.log.recovery.redo", memberAttrs(0))).isEqualTo(88);
        assertThat(valueOf(metrics, "db2.log.dirty_pages.held", memberAttrs(0))).isEqualTo(89);
        assertThat(valueOf(metrics, "db2.log.hadr.wait.time", memberAttrs(0))).isEqualTo(7.0);
        assertThat(valueOf(metrics, "db2.log.hadr.wait.count", memberAttrs(0))).isEqualTo(90);
    }

    @Test
    void logDiskWaitsComeFromMonGetDatabase() throws Exception {
        Connection conn = transactionLogConnection();
        new TransactionLogQuery(() -> conn, "TESTDB", meter);

        Collection<MetricData> metrics = collect();
        // LOG_DISK_WAIT_TIME is milliseconds
        assertThat(valueOf(metrics, "db2.log.disk_wait.time", memberAttrs(0))).isEqualTo(8.0);
        assertThat(valueOf(metrics, "db2.log.disk_wait.count", memberAttrs(0))).isEqualTo(91);
    }

    @Test
    void diskWaitFailureDoesNotDiscardTransactionLogMetrics() throws Exception {
        Map<String, ResultSet> routes = new HashMap<>();
        routes.put("MON_GET_TRANSACTION_LOG", singleRow(0, cols("TOTAL_LOG_USED", 100)));
        // MON_GET_DATABASE deliberately unstubbed.
        Connection conn = routingConnection(routes);
        new TransactionLogQuery(() -> conn, "TESTDB", meter);

        assertThat(valueOf(collect(), "db2.log.space.used", memberAttrs(0))).isEqualTo(100);
    }

    @Test
    void infiniteLogSpaceSentinelIsNotEmittedAsNegativeBytes() throws Exception {
        // With LOGSECOND=-1 the active log is unbounded and DB2 reports -1. Emitting that as a
        // byte count would look like negative capacity and could fire false low-space alerts.
        Map<String, ResultSet> routes = new LinkedHashMap<>();
        routes.put("MON_GET_TRANSACTION_LOG", singleRow(0, cols(
            "TOTAL_LOG_AVAILABLE", -1, "TOTAL_LOG_USED", 100, "LOG_READS", 5)));
        routes.put("MON_GET_DATABASE", singleRow(0, cols("LOG_DISK_WAITS_TOTAL", 7)));
        Connection conn = routingConnection(routes);
        new TransactionLogQuery(() -> conn, "TESTDB", meter);

        Collection<MetricData> metrics = collect();

        // Assert against the captured collection: calling metricNames() here would collect a
        // second time, see the exhausted mocks, and pass no matter what was emitted.
        assertThat(namesOf(metrics)).doesNotContain("db2.log.space.available");

        // Guards the guard: the sibling readings prove this collection was not empty, so the
        // assertion above cannot pass merely because nothing was recorded.
        assertThat(valueOf(metrics, "db2.log.space.used", memberAttrs(0))).isEqualTo(100);
        assertThat(valueOf(metrics, "db2.log.disk_wait.count", memberAttrs(0))).isEqualTo(7);
    }

    @Test
    void activeLogFileNumbersAreNotEmittedAsMetrics() throws Exception {
        Connection conn = transactionLogConnection();
        new TransactionLogQuery(() -> conn, "TESTDB", meter);

        assertThat(metricNames()).noneMatch(n -> n.contains("active_log"));
    }

    // ── multi-member ─────────────────────────────────────────────────────────

    @Test
    void snapshotMetricsCoverEveryMember() throws Exception {
        ResultSet monRs = singleRow(0, cols("DYNAMIC_SQL_STMTS", 11));

        // SNAP_GET_DB('') returns one row per active member, reported as DBPARTITIONNUM.
        ResultSet snapRs = Mockito.mock(ResultSet.class);
        when(snapRs.next()).thenReturn(true, true, false);
        when(snapRs.getInt("DBPARTITIONNUM")).thenReturn(0, 1);
        when(snapRs.getLong(anyString())).thenReturn(0L);
        when(snapRs.getLong("INT_DEADLOCK_ROLLBACKS")).thenReturn(61L, 62L);

        Map<String, ResultSet> routes = new HashMap<>();
        routes.put("MON_GET_DATABASE", monRs);
        routes.put("SNAP_GET_DB", snapRs);
        Connection conn = routingConnection(routes);
        new SqlActivityQuery(() -> conn, "TESTDB", meter, true);

        Collection<MetricData> metrics = collect();
        assertThat(valueOf(metrics, "db2.deadlock.rollback.count", memberAttrs(0))).isEqualTo(61);
        assertThat(valueOf(metrics, "db2.deadlock.rollback.count", memberAttrs(1))).isEqualTo(62);
    }

    @Test
    void snapshotSqlRequestsAllMembersNotJustTheCurrentOne() throws Exception {
        // -1 would scope to the current member only, and -2 returns one aggregated row with a
        // NULL DBPARTITIONNUM that getInt would silently report as member 0.
        Map<String, ResultSet> routes = new HashMap<>();
        routes.put("MON_GET_DATABASE", singleRow(0, cols("DYNAMIC_SQL_STMTS", 11)));
        routes.put("SNAP_GET_DB", singleRow(0, cols("COMMIT_SQL_STMTS", 51)));
        Connection conn = routingConnection(routes);
        Statement st = conn.createStatement();
        new SqlActivityQuery(() -> conn, "TESTDB", meter, true);
        collect();

        org.mockito.ArgumentCaptor<String> sql = org.mockito.ArgumentCaptor.forClass(String.class);
        Mockito.verify(st, Mockito.atLeastOnce()).executeQuery(sql.capture());
        String snapshotSql = sql.getAllValues().stream()
            .filter(s -> s.contains("SNAP_GET_DB"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("snapshot query never issued"));
        assertThat(snapshotSql).contains("SNAP_GET_DB('')");
        assertThat(snapshotSql).doesNotContain("-1").doesNotContain("-2");
    }

    @Test
    void eachMemberProducesItsOwnSeries() throws Exception {
        ResultSet rs = Mockito.mock(ResultSet.class);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getInt("MEMBER")).thenReturn(0, 1);
        when(rs.getLong(anyString())).thenReturn(0L);
        when(rs.getLong("DYNAMIC_SQL_STMTS")).thenReturn(11L, 22L);

        Map<String, ResultSet> routes = new HashMap<>();
        routes.put("MON_GET_DATABASE", rs);
        Connection conn = routingConnection(routes);
        new SqlActivityQuery(() -> conn, "TESTDB", meter, false);

        Collection<MetricData> metrics = collect();
        assertThat(valueOf(metrics, "db2.sql.statement.count", attrs(0, "db2.statement_type", "dynamic"))).isEqualTo(11);
        assertThat(valueOf(metrics, "db2.sql.statement.count", attrs(1, "db2.statement_type", "dynamic"))).isEqualTo(22);
    }
}
