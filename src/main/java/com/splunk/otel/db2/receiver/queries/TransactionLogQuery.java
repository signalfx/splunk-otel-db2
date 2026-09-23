package com.splunk.otel.db2.receiver.queries;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Supplier;

/**
 * Transaction log metrics, primarily from MON_GET_TRANSACTION_LOG.
 *
 * <p>LOG_DISK_WAIT_TIME and LOG_DISK_WAITS_TOTAL are log metrics but live in
 * MON_GET_DATABASE rather than MON_GET_TRANSACTION_LOG, so they are collected by a
 * second statement.
 *
 * <p>FIRST_ACTIVE_LOG, LAST_ACTIVE_LOG and CURRENT_ACTIVE_LOG are log file numbers
 * rather than measurements, so they are not emitted as metrics.
 */
public class TransactionLogQuery {
    private static final Logger log = LoggerFactory.getLogger(TransactionLogQuery.class);

    private static final int QUERY_TIMEOUT_SECONDS = 30;

    // -2 = all active members, one row per member (consistent with DatabaseQuery)
    private static final String SQL =
        "SELECT MEMBER, " +
        "TOTAL_LOG_AVAILABLE, TOTAL_LOG_USED, SEC_LOG_USED_TOP, TOT_LOG_USED_TOP, " +
        "SEC_LOGS_ALLOCATED, LOG_READS, LOG_WRITES, LOG_READ_TIME, LOG_WRITE_TIME, " +
        "NUM_LOG_READ_IO, NUM_LOG_WRITE_IO, NUM_LOG_PART_PAGE_IO, " +
        "NUM_LOG_BUFFER_FULL, NUM_LOG_DATA_FOUND_IN_BUFFER, " +
        "LOG_TO_REDO_FOR_RECOVERY, LOG_HELD_BY_DIRTY_PAGES, " +
        "LOG_HADR_WAIT_TIME, LOG_HADR_WAITS_TOTAL " +
        "FROM TABLE(MON_GET_TRANSACTION_LOG(-2)) AS T";

    private static final String DISK_WAIT_SQL =
        "SELECT MEMBER, LOG_DISK_WAIT_TIME, LOG_DISK_WAITS_TOTAL " +
        "FROM TABLE(MON_GET_DATABASE(-2)) AS T";

    private final Supplier<Connection> connSupplier;
    private final String dbName;

    private ObservableLongMeasurement logSpaceAvailable;
    private ObservableLongMeasurement logSpaceUsed;
    private ObservableLongMeasurement logSpaceUsedMax;
    private ObservableLongMeasurement secondaryLogsAllocated;
    private ObservableLongMeasurement logIoPages;
    private ObservableDoubleMeasurement logIoTime;
    private ObservableLongMeasurement logIoOperations;
    private ObservableLongMeasurement logPartialPageIo;
    private ObservableLongMeasurement logBufferFull;
    private ObservableLongMeasurement logBufferHits;
    private ObservableLongMeasurement logToRedoForRecovery;
    private ObservableLongMeasurement logHeldByDirtyPages;
    private ObservableDoubleMeasurement hadrWaitTime;
    private ObservableLongMeasurement hadrWaits;
    private ObservableDoubleMeasurement diskWaitTime;
    private ObservableLongMeasurement diskWaits;

    public TransactionLogQuery(Supplier<Connection> connSupplier, String dbName, Meter meter) {
        this.connSupplier = connSupplier;
        this.dbName       = dbName;
        register(meter);
    }

    private void register(Meter meter) {
        logSpaceAvailable = meter.gaugeBuilder("db2.log.space.available")
            .ofLongs()
            .setDescription("Amount of active log space not being used by uncommitted transactions.")
            .setUnit("By")
            .buildObserver();

        logSpaceUsed = meter.gaugeBuilder("db2.log.space.used")
            .ofLongs()
            .setDescription("Total amount of active log space currently used.")
            .setUnit("By")
            .buildObserver();

        logSpaceUsedMax = meter.gaugeBuilder("db2.log.space.used.max")
            .ofLongs()
            .setDescription("Maximum amount of log space used, by log space type.")
            .setUnit("By")
            .buildObserver();

        secondaryLogsAllocated = meter.gaugeBuilder("db2.log.secondary_files.allocated")
            .ofLongs()
            .setDescription("Total number of secondary log files currently in use.")
            .setUnit("{file}")
            .buildObserver();

        logIoPages = meter.counterBuilder("db2.log.io.pages")
            .setDescription("Number of log pages read from or written to disk by the logger.")
            .setUnit("{page}")
            .buildObserver();

        logIoTime = meter.counterBuilder("db2.log.io.time")
            .ofDoubles()
            .setDescription("Total elapsed time spent by the logger reading from or writing to disk.")
            .setUnit("s")
            .buildObserver();

        logIoOperations = meter.counterBuilder("db2.log.io.count")
            .setDescription("Number of I/O requests issued by the logger to read or write log data.")
            .setUnit("{operation}")
            .buildObserver();

        logPartialPageIo = meter.counterBuilder("db2.log.partial_page_io.count")
            .setDescription("Number of I/O requests issued by the logger for writing partial log data.")
            .setUnit("{operation}")
            .buildObserver();

        // Distinct from db2.log.disk_wait.count: this counts only the waits caused by the log
        // buffer being full, so it is a log-buffer-sizing signal rather than a disk-latency one.
        logBufferFull = meter.counterBuilder("db2.log.buffer_full.count")
            .setDescription("Number of times agents had to wait because the log buffer was full "
                + "while copying log records into it.")
            .setUnit("{wait}")
            .buildObserver();

        logBufferHits = meter.counterBuilder("db2.log.buffer_hit.count")
            .setDescription("Number of times an agent read log data from the log buffer.")
            .setUnit("{read}")
            .buildObserver();

        logToRedoForRecovery = meter.gaugeBuilder("db2.log.recovery.redo")
            .ofLongs()
            .setDescription("Amount of log that would have to be redone for crash recovery.")
            .setUnit("By")
            .buildObserver();

        logHeldByDirtyPages = meter.gaugeBuilder("db2.log.dirty_pages.held")
            .ofLongs()
            .setDescription("Amount of log corresponding to the difference between the oldest dirty page and the current log position.")
            .setUnit("By")
            .buildObserver();

        hadrWaitTime = meter.counterBuilder("db2.log.hadr.wait.time")
            .ofDoubles()
            .setDescription("Total time the logger spent waiting on HADR log replication.")
            .setUnit("s")
            .buildObserver();

        hadrWaits = meter.counterBuilder("db2.log.hadr.wait.count")
            .setDescription("Number of times the logger waited on HADR log replication.")
            .setUnit("{wait}")
            .buildObserver();

        diskWaitTime = meter.counterBuilder("db2.log.disk_wait.time")
            .ofDoubles()
            .setDescription("Total time agents spent waiting for log data to be written to disk.")
            .setUnit("s")
            .buildObserver();

        diskWaits = meter.counterBuilder("db2.log.disk_wait.count")
            .setDescription("Number of times agents waited for log data to be written to disk.")
            .setUnit("{wait}")
            .buildObserver();

        meter.batchCallback(
            this::collect,
            logSpaceAvailable, logSpaceUsed, logSpaceUsedMax, secondaryLogsAllocated,
            logIoPages, logIoTime, logIoOperations, logPartialPageIo,
            logBufferFull, logBufferHits, logToRedoForRecovery, logHeldByDirtyPages,
            hadrWaitTime, hadrWaits, diskWaitTime, diskWaits
        );
    }

    private static Attributes directionAttrs(int member, String direction) {
        return Attributes.builder()
            .put("db2.member", member)
            .put("disk.io.direction", direction)
            .build();
    }

    /**
     * Records a log space reading, skipping DB2's infinite-logging sentinel.
     *
     * <p>With LOGSECOND set to -1 the active log is unbounded and DB2 reports these columns as
     * -1 (or NULL). Emitting that as a byte count would look like negative capacity and could
     * fire false low-space alerts, so the point is omitted instead: absent is honest, -1 is not.
     */
    private static void recordLogSpace(ObservableLongMeasurement instrument, ResultSet rs,
                                       String column, Attributes attrs) throws SQLException {
        long value = rs.getLong(column);
        if (rs.wasNull() || value < 0) {
            return;
        }
        instrument.record(value, attrs);
    }

    private static Attributes logSpaceAttrs(int member, String logSpaceType) {
        return Attributes.builder()
            .put("db2.member", member)
            .put("db2.log_space_type", logSpaceType)
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

                        recordLogSpace(logSpaceAvailable, rs, "TOTAL_LOG_AVAILABLE", attrs);
                        recordLogSpace(logSpaceUsed,      rs, "TOTAL_LOG_USED",      attrs);
                        recordLogSpace(logSpaceUsedMax, rs, "SEC_LOG_USED_TOP",
                            logSpaceAttrs(member, "secondary"));
                        recordLogSpace(logSpaceUsedMax, rs, "TOT_LOG_USED_TOP",
                            logSpaceAttrs(member, "total"));
                        recordLogSpace(secondaryLogsAllocated, rs, "SEC_LOGS_ALLOCATED", attrs);

                        logIoPages     .record(rs.getLong("LOG_READS"),        readAttrs);
                        logIoPages     .record(rs.getLong("LOG_WRITES"),       writeAttrs);
                        logIoTime      .record(rs.getLong("LOG_READ_TIME") / 1000.0,  readAttrs);
                        logIoTime      .record(rs.getLong("LOG_WRITE_TIME") / 1000.0, writeAttrs);
                        logIoOperations.record(rs.getLong("NUM_LOG_READ_IO"),  readAttrs);
                        logIoOperations.record(rs.getLong("NUM_LOG_WRITE_IO"), writeAttrs);

                        logPartialPageIo.record(rs.getLong("NUM_LOG_PART_PAGE_IO"), attrs);
                        logBufferFull   .record(rs.getLong("NUM_LOG_BUFFER_FULL"),   attrs);
                        logBufferHits   .record(rs.getLong("NUM_LOG_DATA_FOUND_IN_BUFFER"), attrs);

                        logToRedoForRecovery.record(rs.getLong("LOG_TO_REDO_FOR_RECOVERY"), attrs);
                        logHeldByDirtyPages .record(rs.getLong("LOG_HELD_BY_DIRTY_PAGES"),  attrs);

                        hadrWaitTime.record(rs.getLong("LOG_HADR_WAIT_TIME") / 1000.0, attrs);
                        hadrWaits   .record(rs.getLong("LOG_HADR_WAITS_TOTAL"), attrs);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("TransactionLogQuery collection failed: {}", e.getMessage());
        }

        collectDiskWaits();
    }

    // Log disk wait columns live in MON_GET_DATABASE; a failure here must not discard the
    // MON_GET_TRANSACTION_LOG metrics above.
    private void collectDiskWaits() {
        try {
            Connection conn = connSupplier.get();
            try (Statement st = conn.createStatement()) {
                st.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                try (ResultSet rs = st.executeQuery(DISK_WAIT_SQL)) {
                    while (rs.next()) {
                        Attributes attrs = Attributes.builder()
                            .put("db2.member", rs.getInt("MEMBER"))
                            .build();
                        diskWaitTime.record(rs.getLong("LOG_DISK_WAIT_TIME") / 1000.0, attrs);
                        diskWaits   .record(rs.getLong("LOG_DISK_WAITS_TOTAL"), attrs);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("TransactionLogQuery disk wait collection failed: {}", e.getMessage());
        }
    }
}
