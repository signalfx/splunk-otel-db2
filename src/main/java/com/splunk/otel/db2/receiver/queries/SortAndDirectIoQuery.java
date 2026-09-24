package com.splunk.otel.db2.receiver.queries;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.function.Supplier;

/**
 * Sort memory and direct (non-buffer-pool) I/O metrics from MON_GET_DATABASE.
 *
 * <p>TOTAL_SORTS and SORT_OVERFLOWS are already emitted by {@link DatabaseQuery} as
 * db2.sort.count and db2.sort.overflow.count, so they are deliberately not repeated here.
 */
public class SortAndDirectIoQuery {
    private static final Logger log = LoggerFactory.getLogger(SortAndDirectIoQuery.class);

    private static final int QUERY_TIMEOUT_SECONDS = 30;

    private static final long BYTES_PER_4K_PAGE = 4096L;

    // -2 = all active members, one row per member (consistent with DatabaseQuery)
    private static final String SQL =
        "SELECT MEMBER, " +
        "SORT_HEAP_ALLOCATED, SORT_SHRHEAP_ALLOCATED, SORT_SHRHEAP_TOP, " +
        "TOTAL_SECTION_SORT_TIME, ACTIVE_SORTS, ACTIVE_HASH_JOINS, ACTIVE_OLAP_FUNCS, " +
        "DIRECT_READS, DIRECT_WRITES, DIRECT_READ_REQS, DIRECT_WRITE_REQS, " +
        "DIRECT_READ_TIME, DIRECT_WRITE_TIME " +
        "FROM TABLE(MON_GET_DATABASE(-2)) AS T";

    private final Supplier<Connection> connSupplier;
    private final String dbName;

    private ObservableLongMeasurement sortHeapAllocated;
    private ObservableLongMeasurement sortSharedHeapAllocated;
    private ObservableLongMeasurement sortSharedHeapMax;
    private ObservableDoubleMeasurement sortTime;
    private ObservableLongMeasurement activeSorts;
    private ObservableLongMeasurement activeHashJoins;
    private ObservableLongMeasurement activeOlapFunctions;
    private ObservableLongMeasurement directIoOperations;
    private ObservableLongMeasurement directIoRequests;
    private ObservableDoubleMeasurement directIoTime;

    public SortAndDirectIoQuery(Supplier<Connection> connSupplier, String dbName, Meter meter) {
        this.connSupplier = connSupplier;
        this.dbName       = dbName;
        register(meter);
    }

    private void register(Meter meter) {
        // SORT_HEAP_ALLOCATED's own element description says "pages" without stating a size,
        // but 4KB is well supported: its Usage notes document comparing it directly against
        // sheapthres, which is specified in 4KB pages, and the sibling private element
        // SORT_HEAP_TOP is explicitly documented "in 4 KB pages". Sort heap does not track the
        // database page size. Converted for consistency with the shared-sort metrics below.
        sortHeapAllocated = meter.upDownCounterBuilder("db2.sort.heap.allocated")
            .setDescription("Total sort heap space allocated.")
            .setUnit("By")
            .buildObserver();

        sortSharedHeapAllocated = meter.upDownCounterBuilder("db2.sort.shared.heap.allocated")
            .setDescription("Total amount of shared sort memory allocated.")
            .setUnit("By")
            .buildObserver();

        sortSharedHeapMax = meter.gaugeBuilder("db2.sort.shared.heap.max")
            .ofLongs()
            .setDescription("Shared sort memory high watermark.")
            .setUnit("By")
            .buildObserver();

        sortTime = meter.counterBuilder("db2.sort.time")
            .ofDoubles()
            .setDescription("Total elapsed time spent performing section sorts.")
            .setUnit("s")
            .buildObserver();

        // UpDownCounters, not gauges: these counts are additive across db2.member, so a backend
        // must be able to sum them. Matches db2.connection.active in DatabaseQuery.
        activeSorts = meter.upDownCounterBuilder("db2.sort.active")
            .setDescription("Number of sorts currently active.")
            .setUnit("{sort}")
            .buildObserver();

        activeHashJoins = meter.upDownCounterBuilder("db2.hash_join.active")
            .setDescription("Number of hash joins currently active.")
            .setUnit("{hash_join}")
            .buildObserver();

        activeOlapFunctions = meter.upDownCounterBuilder("db2.olap_function.active")
            .setDescription("Number of OLAP functions currently active.")
            .setUnit("{function}")
            .buildObserver();

        directIoOperations = meter.counterBuilder("db2.direct.io.count")
            .setDescription("Number of read or write operations that bypass the buffer pool.")
            .setUnit("{operation}")
            .buildObserver();

        directIoRequests = meter.counterBuilder("db2.direct.io.request.count")
            .setDescription("Number of requests to perform a direct read or write of one or more sectors of data.")
            .setUnit("{request}")
            .buildObserver();

        directIoTime = meter.counterBuilder("db2.direct.io.time")
            .ofDoubles()
            .setDescription("Total elapsed time spent performing direct reads or writes.")
            .setUnit("s")
            .buildObserver();

        meter.batchCallback(
            this::collect,
            sortHeapAllocated, sortSharedHeapAllocated, sortSharedHeapMax, sortTime,
            activeSorts, activeHashJoins, activeOlapFunctions,
            directIoOperations, directIoRequests, directIoTime
        );
    }

    private static Attributes directionAttrs(int member, String direction) {
        return Attributes.builder()
            .put("db2.member", member)
            .put("disk.io.direction", direction)
            .build();
    }

    private void collect() {
        try {
            Connection conn = connSupplier.get();
            try (Statement st = conn.createStatement()) {
                st.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                try (ResultSet rs = st.executeQuery(SQL)) {
                    while (rs.next()) {
                        int member = rs.getInt("MEMBER");
                        Attributes attrs = Attributes.builder().put("db2.member", member).build();
                        Attributes readAttrs  = directionAttrs(member, "read");
                        Attributes writeAttrs = directionAttrs(member, "write");

                        sortHeapAllocated.record(
                            rs.getLong("SORT_HEAP_ALLOCATED") * BYTES_PER_4K_PAGE, attrs);
                        sortSharedHeapAllocated.record(
                            rs.getLong("SORT_SHRHEAP_ALLOCATED") * BYTES_PER_4K_PAGE, attrs);
                        sortSharedHeapMax.record(
                            rs.getLong("SORT_SHRHEAP_TOP") * BYTES_PER_4K_PAGE, attrs);
                        sortTime.record(rs.getLong("TOTAL_SECTION_SORT_TIME") / 1000.0, attrs);

                        activeSorts        .record(rs.getLong("ACTIVE_SORTS"),      attrs);
                        activeHashJoins    .record(rs.getLong("ACTIVE_HASH_JOINS"), attrs);
                        activeOlapFunctions.record(rs.getLong("ACTIVE_OLAP_FUNCS"), attrs);

                        directIoOperations.record(rs.getLong("DIRECT_READS"),      readAttrs);
                        directIoOperations.record(rs.getLong("DIRECT_WRITES"),     writeAttrs);
                        directIoRequests  .record(rs.getLong("DIRECT_READ_REQS"),  readAttrs);
                        directIoRequests  .record(rs.getLong("DIRECT_WRITE_REQS"), writeAttrs);
                        directIoTime      .record(rs.getLong("DIRECT_READ_TIME") / 1000.0,  readAttrs);
                        directIoTime      .record(rs.getLong("DIRECT_WRITE_TIME") / 1000.0, writeAttrs);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("SortAndDirectIoQuery collection failed: {}", e.getMessage());
        }
    }
}
