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
 * <p>COMMIT_SQL_STMTS, ROLLBACK_SQL_STMTS, ROWS_SELECTED, INT_DEADLOCK_ROLLBACKS and
 * INT_AUTO_REBINDS exist only in the deprecated snapshot interface, so they are collected
 * from SNAP_GET_DB as extra dimensions on the same instruments when use_snapshot_monitor
 * is enabled.
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

    // SNAP_GET_DB takes -1 (current partition); it reports the partition as DBPARTITIONNUM.
    private static final String SNAPSHOT_SQL =
        "SELECT DBPARTITIONNUM, " +
        "COMMIT_SQL_STMTS, ROLLBACK_SQL_STMTS, ROWS_SELECTED, " +
        "INT_DEADLOCK_ROLLBACKS, INT_AUTO_REBINDS " +
        "FROM TABLE(SNAP_GET_DB('', -1)) AS S";

    private final Supplier<Connection> connSupplier;
    private final String dbName;
    private final boolean useSnapshotMonitor;

    private ObservableLongMeasurement statements;
    private ObservableLongMeasurement transactions;
    private ObservableLongMeasurement rows;
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
            .setDescription("Number of SQL statements executed, by statement type.")
            .setUnit("{statement}")
            .buildObserver();

        transactions = meter.counterBuilder("db2.transaction.count")
            .setDescription("Number of transactions committed or rolled back.")
            .setUnit("{transaction}")
            .buildObserver();

        rows = meter.counterBuilder("db2.rows.count")
            .setDescription("Number of rows affected, by operation.")
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
            .setDescription("Number of times packages were rebound, by rebind type.")
            .setUnit("{rebind}")
            .buildObserver();

        deadlockRollbacks = meter.counterBuilder("db2.deadlock.rollback.count")
            .setDescription("Number of internal rollbacks caused by a deadlock.")
            .setUnit("{rollback}")
            .buildObserver();

        meter.batchCallback(
            this::collect,
            statements, transactions, rows,
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

    private static Attributes rebindAttrs(int member, String rebindType) {
        return Attributes.builder()
            .put("db2.member", member)
            .put("db2.rebind_type", rebindType)
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

                        statements.record(rs.getLong("DYNAMIC_SQL_STMTS"), statementAttrs(member, "dynamic"));
                        statements.record(rs.getLong("STATIC_SQL_STMTS"),  statementAttrs(member, "static"));
                        statements.record(rs.getLong("FAILED_SQL_STMTS"),  statementAttrs(member, "failed"));
                        statements.record(rs.getLong("SELECT_SQL_STMTS"),  statementAttrs(member, "select"));
                        statements.record(rs.getLong("UID_SQL_STMTS"),     statementAttrs(member, "uid"));
                        statements.record(rs.getLong("DDL_SQL_STMTS"),     statementAttrs(member, "ddl"));

                        transactions.record(rs.getLong("TOTAL_APP_COMMITS"),   transactionAttrs(member, "commit", false));
                        transactions.record(rs.getLong("TOTAL_APP_ROLLBACKS"), transactionAttrs(member, "rollback", false));
                        transactions.record(rs.getLong("INT_COMMITS"),         transactionAttrs(member, "commit", true));
                        transactions.record(rs.getLong("INT_ROLLBACKS"),       transactionAttrs(member, "rollback", true));

                        rows.record(rs.getLong("ROWS_READ"),         rowAttrs(member, "read", false));
                        rows.record(rs.getLong("ROWS_RETURNED"),     rowAttrs(member, "returned", false));
                        rows.record(rs.getLong("ROWS_MODIFIED"),     rowAttrs(member, "modified", false));
                        rows.record(rs.getLong("ROWS_INSERTED"),     rowAttrs(member, "inserted", false));
                        rows.record(rs.getLong("ROWS_UPDATED"),      rowAttrs(member, "updated", false));
                        rows.record(rs.getLong("ROWS_DELETED"),      rowAttrs(member, "deleted", false));
                        rows.record(rs.getLong("INT_ROWS_INSERTED"), rowAttrs(member, "inserted", true));
                        rows.record(rs.getLong("INT_ROWS_UPDATED"),  rowAttrs(member, "updated", true));
                        rows.record(rs.getLong("INT_ROWS_DELETED"),  rowAttrs(member, "deleted", true));

                        compilations.record(rs.getLong("TOTAL_COMPILATIONS"), memberAttrs);
                        compileTime .record(rs.getLong("TOTAL_COMPILE_TIME") / 1000.0, memberAttrs);
                        binds       .record(rs.getLong("BINDS_PRECOMPILES"), memberAttrs);
                        rebinds     .record(rs.getLong("IMPLICIT_REBINDS"), rebindAttrs(member, "implicit"));
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
                        int member = rs.getInt("DBPARTITIONNUM");
                        Attributes memberAttrs = Attributes.builder().put("db2.member", member).build();

                        statements.record(rs.getLong("COMMIT_SQL_STMTS"),   statementAttrs(member, "commit"));
                        statements.record(rs.getLong("ROLLBACK_SQL_STMTS"), statementAttrs(member, "rollback"));
                        rows      .record(rs.getLong("ROWS_SELECTED"),      rowAttrs(member, "selected", false));

                        deadlockRollbacks.record(rs.getLong("INT_DEADLOCK_ROLLBACKS"), memberAttrs);
                        rebinds.record(rs.getLong("INT_AUTO_REBINDS"), rebindAttrs(member, "auto"));
                    }
                }
            }
        } catch (Exception e) {
            log.warn("SqlActivityQuery snapshot collection failed: {}", e.getMessage());
        }
    }
}
