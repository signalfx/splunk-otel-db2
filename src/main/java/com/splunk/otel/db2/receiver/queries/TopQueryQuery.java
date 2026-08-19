package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.obfuscator.SqlObfuscator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.Severity;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.function.Supplier;

public class TopQueryQuery {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(TopQueryQuery.class);

    // STMT_EXEC_TIME: execution time in milliseconds (TOTAL_CPU_TIME is microseconds)
    // Fetches cumulative totals; delta vs previous snapshot is computed before emitting
    // so only statements with new executions since the last interval are reported.
    private static final String SQL_TEMPLATE =
        "SELECT EXECUTABLE_ID, STMT_TEXT, " +
        "NUM_EXECUTIONS, " +
        "STMT_EXEC_TIME / 1000.0 AS TOTAL_ELAPSED_SEC, " +
        "TOTAL_CPU_TIME, " +
        "ROWS_READ " +
        "FROM TABLE(MON_GET_PKG_CACHE_STMT(NULL, NULL, NULL, -2)) AS T " +
        "WHERE NUM_EXECUTIONS > 0 " +
        "AND STMT_TEXT NOT LIKE '%%MON_GET_PKG_CACHE_STMT%%' " +
        "AND STMT_TEXT NOT LIKE '%%MON_GET_ACTIVITY%%' " +
        "AND STMT_TEXT NOT LIKE '%%MON_GET_DATABASE%%' " +
        "AND STMT_TEXT NOT LIKE '%%MON_GET_BUFFERPOOL%%' " +
        "AND STMT_TEXT NOT LIKE '%%MON_GET_TABLESPACE%%' " +
        "FETCH FIRST %d ROWS ONLY";

    private static final double MICROSECONDS_TO_SECONDS = 1_000_000.0;

    private final Supplier<Connection> connSupplier;
    private final String dbName;
    private final Logger otelLogger;
    private final String sql;

    // previous snapshot keyed by EXECUTABLE_ID for delta calculation
    private final java.util.concurrent.ConcurrentHashMap<String, long[]> prevSnapshot = new java.util.concurrent.ConcurrentHashMap<>();

    public TopQueryQuery(Supplier<Connection> connSupplier, String dbName, Logger otelLogger, int topQueryCount) {
        this.connSupplier = connSupplier;
        this.dbName       = dbName;
        this.otelLogger   = otelLogger;
        this.sql          = String.format(SQL_TEMPLATE, topQueryCount);
    }

    public void collect() {
        try {
            Connection conn = connSupplier.get();
            try (PreparedStatement ps = conn.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {

                java.util.Map<String, long[]> current = new java.util.HashMap<>();
                java.util.List<long[]> deltas = new java.util.ArrayList<>();

                while (rs.next()) {
                    String execId        = rs.getString("EXECUTABLE_ID");
                    String stmtText      = rs.getString("STMT_TEXT");
                    long numExec         = rs.getLong("NUM_EXECUTIONS");
                    long elapsedMs       = (long) (rs.getDouble("TOTAL_ELAPSED_SEC") * 1000);
                    long cpuUs           = rs.getLong("TOTAL_CPU_TIME");
                    long rowsRead        = rs.getLong("ROWS_READ");

                    // [0]=numExec [1]=elapsedMs [2]=cpuUs [3]=rowsRead
                    current.put(execId, new long[]{numExec, elapsedMs, cpuUs, rowsRead});

                    long[] prev = prevSnapshot.get(execId);
                    long deltaExec = prev == null ? numExec : numExec - prev[0];
                    if (deltaExec <= 0) continue; // no new executions this interval

                    long deltaElapsedMs = prev == null ? elapsedMs  : elapsedMs  - prev[1];
                    long deltaCpuUs     = prev == null ? cpuUs      : cpuUs      - prev[2];
                    long deltaRows      = prev == null ? rowsRead   : rowsRead   - prev[3];

                    String obfuscated = SqlObfuscator.getInstance().obfuscate(stmtText);
                    deltas.add(new long[]{deltaExec, deltaElapsedMs, deltaCpuUs, deltaRows,
                        System.identityHashCode(obfuscated)}); // placeholder — emit below
                    emitLog(obfuscated, deltaExec, deltaElapsedMs / 1000.0, deltaCpuUs / MICROSECONDS_TO_SECONDS, deltaRows);
                }

                prevSnapshot.clear();
                prevSnapshot.putAll(current);
            }
        } catch (Exception e) {
            log.error("Failed to query MON_GET_PKG_CACHE_STMT: {}", e.getMessage());
        }
    }

    private void emitLog(String stmtText, long numExec, double elapsedS, double cpuS, long rowsRead) {
        String obfuscated = stmtText != null ? stmtText.trim() : "";
        otelLogger.logRecordBuilder()
            .setBody(obfuscated)
            .setSeverity(Severity.INFO)
            .setAttribute(AttributeKey.stringKey("event.name"),             "db.server.top_query")
            .setAttribute(AttributeKey.stringKey("db.system.name"),         "db2")
            .setAttribute(AttributeKey.stringKey("db.namespace"),           dbName)
            .setAttribute(AttributeKey.stringKey("db.query.text"),          obfuscated)
            .setAttribute(AttributeKey.longKey("db2.execution_count"),      numExec)
            .setAttribute(AttributeKey.doubleKey("db2.total_elapsed_time"), elapsedS)
            .setAttribute(AttributeKey.doubleKey("db2.total_worker_time"),  cpuS)
            .setAttribute(AttributeKey.longKey("db2.rows_read"),            rowsRead)
            .emit();
    }
}
