package com.splunk.otel.db2.receiver.queries;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.function.Supplier;

public class DatabaseQuery {
    private static final Logger log = LoggerFactory.getLogger(DatabaseQuery.class);

    // -2 = all active members (consistent with BufferpoolQuery / TablespaceQuery)
    private static final String SQL =
        "SELECT MEMBER, APPLS_CUR_CONS, APPLS_IN_DB2, CONNECTIONS_TOP, " +
        "LOCK_WAITS, LOCK_WAIT_TIME, DEADLOCKS, LOCK_TIMEOUTS, " +
        "LOCK_ESCALS, LOCK_LIST_IN_USE, TOTAL_SORTS, SORT_OVERFLOWS " +
        "FROM TABLE(MON_GET_DATABASE(-2)) AS T";

    private final Supplier<Connection> connSupplier;
    private final String dbName;

    private ObservableLongMeasurement currentConnections;
    private ObservableLongMeasurement activeConnections;
    private ObservableLongMeasurement maxConnections;
    private ObservableLongMeasurement lockWaits;
    private ObservableLongMeasurement lockWaitTime;
    private ObservableLongMeasurement deadlocks;
    private ObservableLongMeasurement lockTimeouts;
    private ObservableLongMeasurement lockEscalations;
    private ObservableLongMeasurement lockListInUse;
    private ObservableLongMeasurement totalSorts;
    private ObservableLongMeasurement sortOverflows;

    public DatabaseQuery(Supplier<Connection> connSupplier, String dbName, Meter meter) {
        this.connSupplier = connSupplier;
        this.dbName       = dbName;
        register(meter);
    }

    private void register(Meter meter) {
        currentConnections = meter.upDownCounterBuilder("db2.connection.count")
            .setDescription("Number of current connections to the database.")
            .setUnit("{connection}")
            .buildObserver();

        activeConnections = meter.upDownCounterBuilder("db2.connection.active")
            .setDescription("Number of connections currently executing in the database engine.")
            .setUnit("{connection}")
            .buildObserver();

        maxConnections = meter.upDownCounterBuilder("db2.connection.max")
            .setDescription("Highest number of simultaneous connections to the database since it was activated.")
            .setUnit("{connection}")
            .buildObserver();

        lockWaits = meter.counterBuilder("db2.lock.wait.count")
            .setDescription("Total number of times an application waited for a lock.")
            .setUnit("{wait}")
            .buildObserver();

        lockWaitTime = meter.counterBuilder("db2.lock.wait_time")
            .setDescription("Total elapsed time in milliseconds spent waiting for locks.")
            .setUnit("ms")
            .buildObserver();

        deadlocks = meter.counterBuilder("db2.deadlock.count")
            .setDescription("Total number of deadlocks that have occurred.")
            .setUnit("{deadlock}")
            .buildObserver();

        lockTimeouts = meter.counterBuilder("db2.lock.timeout.count")
            .setDescription("Number of times a request to lock an object timed out.")
            .setUnit("{timeout}")
            .buildObserver();

        lockEscalations = meter.counterBuilder("db2.lock.escalation.count")
            .setDescription("Number of times a lock was escalated from row to table level.")
            .setUnit("{escalation}")
            .buildObserver();

        lockListInUse = meter.upDownCounterBuilder("db2.lock.space_used")
            .setDescription("Total amount of lock list memory in use, in bytes.")
            .setUnit("By")
            .buildObserver();

        totalSorts = meter.counterBuilder("db2.sort.count")
            .setDescription("Total number of sorts that have been executed.")
            .setUnit("{sort}")
            .buildObserver();

        sortOverflows = meter.counterBuilder("db2.sort.overflow.count")
            .setDescription("Total number of sorts that ran out of sort heap and required disk space.")
            .setUnit("{sort}")
            .buildObserver();

        meter.batchCallback(
            this::collect,
            currentConnections, activeConnections, maxConnections,
            lockWaits, lockWaitTime, deadlocks, lockTimeouts,
            lockEscalations, lockListInUse, totalSorts, sortOverflows
        );
    }

    private void collect() {
        try {
            Connection conn = connSupplier.get();
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(SQL)) {
                while (rs.next()) {
                    Attributes attrs = Attributes.builder()
                        .put("db.namespace", dbName)
                        .put("db2.member",   rs.getInt("MEMBER"))
                        .build();
                    currentConnections.record(rs.getLong("APPLS_CUR_CONS"),  attrs);
                    activeConnections .record(rs.getLong("APPLS_IN_DB2"),    attrs);
                    maxConnections    .record(rs.getLong("CONNECTIONS_TOP"),  attrs);
                    lockWaits         .record(rs.getLong("LOCK_WAITS"),       attrs);
                    lockWaitTime      .record(rs.getLong("LOCK_WAIT_TIME"),   attrs);
                    deadlocks         .record(rs.getLong("DEADLOCKS"),        attrs);
                    lockTimeouts      .record(rs.getLong("LOCK_TIMEOUTS"),    attrs);
                    lockEscalations   .record(rs.getLong("LOCK_ESCALS"),      attrs);
                    lockListInUse     .record(rs.getLong("LOCK_LIST_IN_USE"), attrs);
                    totalSorts        .record(rs.getLong("TOTAL_SORTS"),      attrs);
                    sortOverflows     .record(rs.getLong("SORT_OVERFLOWS"),   attrs);
                }
            }
        } catch (Exception e) {
            log.warn("DatabaseQuery collection failed: {}", e.getMessage());
        }
    }
}
