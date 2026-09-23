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
 * SQL statement and row activity metrics from MON_GET_DATABASE.
 *
 * <p>Each additive metric here holds only mutually exclusive members, so aggregating away an
 * attribute yields a meaningful total. DB2 classifies the same statement along two axes —
 * dynamic/static describes how it was prepared, select/uid/ddl what it did — so those are
 * separate metrics rather than values of one attribute; a dynamic SELECT increments one value
 * in each. Likewise db2.row.count carries only the insert/update/delete partition, with rows
 * read and rows returned as their own metrics.
 *
 * <p>INT_DEADLOCK_ROLLBACKS exists only in the deprecated snapshot interface and is collected
 * from SNAP_GET_DB when use_snapshot_monitor is enabled.
 */
public class SqlActivityQuery {
    private static final Logger log = LoggerFactory.getLogger(SqlActivityQuery.class);

    private static final int QUERY_TIMEOUT_SECONDS = 30;

    // -2 = all active members, one row per member (consistent with DatabaseQuery)
    private static final String SQL =
        "SELECT MEMBER, " +
        "DYNAMIC_SQL_STMTS, STATIC_SQL_STMTS, FAILED_SQL_STMTS, " +
        "SELECT_SQL_STMTS, UID_SQL_STMTS, DDL_SQL_STMTS, " +
        "TOTAL_APP_COMMITS, TOTAL_APP_ROLLBACKS, INT_COMMITS, INT_ROLLBACKS, " +
        "ROWS_READ, ROWS_RETURNED, ROWS_MODIFIED, " +
        "ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, " +
        "INT_ROWS_INSERTED, INT_ROWS_UPDATED, INT_ROWS_DELETED, " +
        "BINDS_PRECOMPILES, IMPLICIT_REBINDS, TOTAL_COMPILATIONS, TOTAL_COMPILE_TIME " +
        "FROM TABLE(MON_GET_DATABASE(-2)) AS T";

    // The member argument is omitted deliberately: that is SNAP_GET_DB's "one row per active
    // member" form, matching MON_GET_DATABASE(-2) above. Passing -1 would return only the
    // current member, and -2 returns a single aggregated row with a NULL DBPARTITIONNUM.
    //
    // Only INT_DEADLOCK_ROLLBACKS is read. The other snapshot columns duplicate MON_GET_DATABASE
    // values measured live on Db2 11.5: COMMIT_SQL_STMTS/ROLLBACK_SQL_STMTS match
    // TOTAL_APP_COMMITS/TOTAL_APP_ROLLBACKS, ROWS_SELECTED matches ROWS_RETURNED, and
    // INT_AUTO_REBINDS is the same rebind event as IMPLICIT_REBINDS. Emitting them would
    // double-count the same quantity.
    private static final String SNAPSHOT_SQL =
        "SELECT DBPARTITIONNUM, INT_DEADLOCK_ROLLBACKS " +
        "FROM TABLE(SNAP_GET_DB('')) AS S";

    private final Supplier<Connection> connSupplier;
    private final String dbName;
    private final boolean useSnapshotMonitor;

    private ObservableLongMeasurement statements;
    private ObservableLongMeasurement operations;
    private ObservableLongMeasurement failedStatements;
    private ObservableLongMeasurement transactions;
    private ObservableLongMeasurement rows;
    private ObservableLongMeasurement rowsRead;
    private ObservableLongMeasurement rowsReturned;
    private ObservableLongMeasurement compilations;
    private ObservableDoubleMeasurement compileTime;
    private ObservableLongMeasurement binds;
    private ObservableLongMeasurement rebinds;
    private ObservableLongMeasurement deadlockRollbacks;

    public SqlActivityQuery(Supplier<Connection> connSupplier, String dbName, Meter meter,
                            boolean useSnapshotMonitor) {
        this.connSupplier       = connSupplier;
        this.dbName             = dbName;
        this.useSnapshotMonitor = useSnapshotMonitor;
        register(meter);
    }

    private void register(Meter meter) {
        statements = meter.counterBuilder("db2.sql.statement.count")
            .setDescription("Number of SQL statements executed, by how the statement was prepared.")
            .setUnit("{statement}")
            .buildObserver();

        operations = meter.counterBuilder("db2.sql.operation.count")
            .setDescription("Number of SQL statements executed, by the kind of operation performed.")
            .setUnit("{statement}")
            .buildObserver();

        failedStatements = meter.counterBuilder("db2.sql.failed.count")
            .setDescription("Number of SQL statements that failed.")
            .setUnit("{statement}")
            .buildObserver();

        transactions = meter.counterBuilder("db2.transaction.count")
            .setDescription("Number of transactions committed or rolled back.")
            .setUnit("{transaction}")
            .buildObserver();

        rows = meter.counterBuilder("db2.row.count")
            .setDescription("Number of rows inserted, updated or deleted.")
            .setUnit("{row}")
            .buildObserver();

        rowsRead = meter.counterBuilder("db2.row.read.count")
            .setDescription("Number of rows read from tables.")
            .setUnit("{row}")
            .buildObserver();

        rowsReturned = meter.counterBuilder("db2.row.returned.count")
            .setDescription("Number of rows returned to clients.")
            .setUnit("{row}")
            .buildObserver();

        compilations = meter.counterBuilder("db2.sql.compilation.count")
            .setDescription("Number of SQL statement compilations.")
            .setUnit("{compilation}")
            .buildObserver();

        compileTime = meter.counterBuilder("db2.sql.compilation.time")
            .ofDoubles()
            .setDescription("Total time spent compiling SQL statements.")
            .setUnit("s")
            .buildObserver();

        binds = meter.counterBuilder("db2.sql.bind.count")
            .setDescription("Number of binds and precompiles attempted.")
            .setUnit("{bind}")
            .buildObserver();

        rebinds = meter.counterBuilder("db2.sql.rebind.count")
            .setDescription("Number of times packages were implicitly rebound.")
            .setUnit("{rebind}")
            .buildObserver();

        deadlockRollbacks = meter.counterBuilder("db2.deadlock.rollback.count")
            .setDescription("Number of internal rollbacks caused by a deadlock.")
            .setUnit("{rollback}")
            .buildObserver();

        meter.batchCallback(
            this::collect,
            statements, operations, failedStatements, transactions,
            rows, rowsRead, rowsReturned,
            compilations, compileTime, binds, rebinds, deadlockRollbacks
        );
    }

    private static Attributes statementAttrs(int member, String statementType) {
        return Attributes.builder()
            .put("db2.member", member)
            .put("db2.statement_type", statementType)
            .build();
    }

    private static Attributes transactionAttrs(int member, String transactionType, boolean internal) {
        return Attributes.builder()
            .put("db2.member", member)
            .put("db2.transaction_type", transactionType)
            .put("db2.internal", internal)
            .build();
    }

    private static Attributes rowAttrs(int member, String rowOperation, boolean internal) {
        return Attributes.builder()
            .put("db2.member", member)
            .put("db2.row_operation", rowOperation)
            .put("db2.internal", internal)
            .build();
    }

    private static Attributes operationAttrs(int member, String operationType) {
        return Attributes.builder()
            .put("db2.member", member)
            .put("db2.operation_type", operationType)
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
                        Attributes memberAttrs = Attributes.builder().put("db2.member", member).build();

                        // Preparation axis only. SELECT/UID/DDL classify the same statements a
                        // second way, so they belong on their own metric rather than as extra
                        // values here — a dynamic SELECT increments one value in each axis.
                        statements.record(rs.getLong("DYNAMIC_SQL_STMTS"), statementAttrs(member, "dynamic"));
                        statements.record(rs.getLong("STATIC_SQL_STMTS"),  statementAttrs(member, "static"));

                        operations.record(rs.getLong("SELECT_SQL_STMTS"), operationAttrs(member, "select"));
                        operations.record(rs.getLong("UID_SQL_STMTS"),    operationAttrs(member, "uid"));
                        operations.record(rs.getLong("DDL_SQL_STMTS"),    operationAttrs(member, "ddl"));

                        failedStatements.record(rs.getLong("FAILED_SQL_STMTS"), memberAttrs);

                        transactions.record(rs.getLong("TOTAL_APP_COMMITS"),   transactionAttrs(member, "commit", false));
                        transactions.record(rs.getLong("TOTAL_APP_ROLLBACKS"), transactionAttrs(member, "rollback", false));
                        transactions.record(rs.getLong("INT_COMMITS"),         transactionAttrs(member, "commit", true));
                        transactions.record(rs.getLong("INT_ROLLBACKS"),       transactionAttrs(member, "rollback", true));

                        // db2.row.count carries only the mutually exclusive insert/update/delete
                        // partition. ROWS_MODIFIED is an aggregate of those three, and rows read
                        // and returned are separate measurements rather than members of the same
                        // partition, so all three get their own metric.
                        rows.record(rs.getLong("ROWS_INSERTED"),     rowAttrs(member, "inserted", false));
                        rows.record(rs.getLong("ROWS_UPDATED"),      rowAttrs(member, "updated", false));
                        rows.record(rs.getLong("ROWS_DELETED"),      rowAttrs(member, "deleted", false));
                        rows.record(rs.getLong("INT_ROWS_INSERTED"), rowAttrs(member, "inserted", true));
                        rows.record(rs.getLong("INT_ROWS_UPDATED"),  rowAttrs(member, "updated", true));
                        rows.record(rs.getLong("INT_ROWS_DELETED"),  rowAttrs(member, "deleted", true));

                        rowsRead    .record(rs.getLong("ROWS_READ"),     memberAttrs);
                        rowsReturned.record(rs.getLong("ROWS_RETURNED"), memberAttrs);

                        compilations.record(rs.getLong("TOTAL_COMPILATIONS"), memberAttrs);
                        compileTime .record(rs.getLong("TOTAL_COMPILE_TIME") / 1000.0, memberAttrs);
                        binds       .record(rs.getLong("BINDS_PRECOMPILES"), memberAttrs);
                        rebinds     .record(rs.getLong("IMPLICIT_REBINDS"), memberAttrs);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("SqlActivityQuery collection failed: {}", e.getMessage());
        }

        if (useSnapshotMonitor) {
            collectSnapshotMetrics();
        }
    }

    // Snapshot-only columns; failure here must not discard the MON_GET_DATABASE metrics above.
    private void collectSnapshotMetrics() {
        try {
            Connection conn = connSupplier.get();
            try (Statement st = conn.createStatement()) {
                st.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                try (ResultSet rs = st.executeQuery(SNAPSHOT_SQL)) {
                    while (rs.next()) {
                        Attributes memberAttrs = Attributes.builder()
                            .put("db2.member", rs.getInt("DBPARTITIONNUM"))
                            .build();
                        deadlockRollbacks.record(rs.getLong("INT_DEADLOCK_ROLLBACKS"), memberAttrs);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("SqlActivityQuery snapshot collection failed: {}", e.getMessage());
        }
    }
}
