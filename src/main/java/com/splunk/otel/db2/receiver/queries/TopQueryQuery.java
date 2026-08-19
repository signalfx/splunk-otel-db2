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

    // STMT_EXEC_TIME: execution only (µs), excludes compile cost — better ranking than TOTAL_ACT_TIME
    // 30-minute recency filter avoids surfacing stale all-time aggregates
    private static final String SQL_TEMPLATE =
        "SELECT STMT_TEXT, " +
        "NUM_EXECUTIONS, " +
        "STMT_EXEC_TIME / 1000000.0 AS TOTAL_ELAPSED_SEC, " +
        "TOTAL_CPU_TIME, " +
        "ROWS_READ " +
        "FROM TABLE(MON_GET_PKG_CACHE_STMT(NULL, NULL, NULL, -2)) AS T " +
        "WHERE NUM_EXECUTIONS > 0 " +
        "AND LAST_METRICS_UPDATE > CURRENT TIMESTAMP - 30 MINUTE " +
        "AND STMT_TEXT NOT LIKE '%%MON_GET_PKG_CACHE_STMT%%' " +
        "AND STMT_TEXT NOT LIKE '%%MON_GET_ACTIVITY%%' " +
        "AND STMT_TEXT NOT LIKE '%%MON_GET_DATABASE%%' " +
        "AND STMT_TEXT NOT LIKE '%%MON_GET_BUFFERPOOL%%' " +
        "AND STMT_TEXT NOT LIKE '%%MON_GET_TABLESPACE%%' " +
        "ORDER BY STMT_EXEC_TIME DESC NULLS LAST " +
        "FETCH FIRST %d ROWS ONLY";

    private static final double MICROSECONDS_TO_SECONDS = 1_000_000.0;

    private final Supplier<Connection> connSupplier;
    private final String dbName;
    private final Logger otelLogger;
    private final String sql;

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
                while (rs.next()) {
                    String stmtText      = rs.getString("STMT_TEXT");
                    long numExec         = rs.getLong("NUM_EXECUTIONS");
                    double totalElapsedS = rs.getDouble("TOTAL_ELAPSED_SEC");
                    double totalCpuS     = rs.getLong("TOTAL_CPU_TIME") / MICROSECONDS_TO_SECONDS;
                    long rowsRead        = rs.getLong("ROWS_READ");

                    String obfuscated = SqlObfuscator.getInstance().obfuscate(stmtText);

                    otelLogger.logRecordBuilder()
                        .setBody(obfuscated != null ? obfuscated.trim() : "")
                        .setSeverity(Severity.INFO)
                        .setAttribute(AttributeKey.stringKey("event.name"),                  "db.server.top_query")
                        .setAttribute(AttributeKey.stringKey("db.system.name"),              "db2")
                        .setAttribute(AttributeKey.stringKey("db.namespace"),                dbName)
                        .setAttribute(AttributeKey.stringKey("db.query.text"),               obfuscated != null ? obfuscated.trim() : "")
                        .setAttribute(AttributeKey.longKey("db2.execution_count"),           numExec)
                        .setAttribute(AttributeKey.doubleKey("db2.total_elapsed_time"),      totalElapsedS)
                        .setAttribute(AttributeKey.doubleKey("db2.total_worker_time"),       totalCpuS)
                        .setAttribute(AttributeKey.longKey("db2.rows_read"),                 rowsRead)
                        .emit();
                }
            }
        } catch (Exception e) {
            log.error("Failed to query MON_GET_PKG_CACHE_STMT: {}", e.getMessage());
        }
    }
}
