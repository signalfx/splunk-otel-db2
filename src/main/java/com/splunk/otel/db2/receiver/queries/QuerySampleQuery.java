package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.util.SqlObfuscator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.Severity;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Samples active queries from Db2 and emits each statement as an OTel log record.
 *
 * Default path (useSnapshotMonitor=false, requires Db2 10.5+):
 *   MON_GET_ACTIVITY — returns one row per currently executing activity;
 *   limited to maxQuerySampleCount rows ordered by elapsed time descending.
 *
 * Snapshot monitor fallback (useSnapshotMonitor=true, Db2 9.7+):
 *   SYSIBMADM.SNAPSTMT — administrative view over the snapshot monitor;
 *   same columns re-aliased to match the primary path.
 *
 * Emitted log record attributes (OTel semantic conventions):
 *   db.system.name        = "ibm.db2"
 *   db.namespace          = database name
 *   db.query.text         = obfuscated SQL statement
 *   db2.member            = Db2 member number
 *   db2.application.id    = application identifier
 *   db2.elapsed_time_ms   = total elapsed time in milliseconds
 *   db2.cpu_time_ms       = total CPU time in milliseconds (MON_GET_ACTIVITY only)
 *
 * SQL text is always passed through {@link SqlObfuscator} before emission.
 * If obfuscation fails the record is dropped with a WARN log — never emitted raw.
 */
public class QuerySampleQuery {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(QuerySampleQuery.class);

    // MON_GET_ACTIVITY: available from Db2 10.5+.
    // member=-2 returns all active members.
    private static final String SQL_MON_GET_ACTIVITY =
        "SELECT T.MEMBER, T.APPLICATION_ID, T.STMT_TEXT, " +
        "       T.TOTAL_ELAPSED_TIME / 1000 AS ELAPSED_MS, " +
        "       T.TOTAL_CPU_TIME / 1000     AS CPU_MS " +
        "FROM TABLE(MON_GET_ACTIVITY(NULL, -2)) AS T " +
        "WHERE T.STMT_TEXT IS NOT NULL " +
        "  AND T.STMT_TYPE NOT IN ('CALL', 'RETURN') " +
        "ORDER BY T.TOTAL_ELAPSED_TIME DESC " +
        "FETCH FIRST %d ROWS ONLY";

    // SYSIBMADM.SNAPSTMT: snapshot monitor view, available from Db2 9.7+.
    // No CPU column; uses -1 sentinel so callers can skip that attribute.
    private static final String SQL_SNAPSTMT =
        "SELECT DBPARTITIONNUM AS MEMBER, APPL_ID AS APPLICATION_ID, " +
        "       STMT_TEXT, " +
        "       ELAPSED_TIME_LAST_INVOC / 1000 AS ELAPSED_MS, " +
        "       CAST(-1 AS BIGINT) AS CPU_MS " +
        "FROM SYSIBMADM.SNAPSTMT " +
        "WHERE STMT_TEXT IS NOT NULL " +
        "ORDER BY ELAPSED_TIME_LAST_INVOC DESC " +
        "FETCH FIRST %d ROWS ONLY";

    private static final AttributeKey<String> DB_SYSTEM      = AttributeKey.stringKey("db.system.name");
    private static final AttributeKey<String> DB_NAMESPACE   = AttributeKey.stringKey("db.namespace");
    private static final AttributeKey<String> DB_QUERY_TEXT  = AttributeKey.stringKey("db.query.text");
    private static final AttributeKey<Long>   DB2_MEMBER     = AttributeKey.longKey("db2.member");
    private static final AttributeKey<String> DB2_APP_ID     = AttributeKey.stringKey("db2.application.id");
    private static final AttributeKey<Long>   DB2_ELAPSED_MS = AttributeKey.longKey("db2.elapsed_time_ms");
    private static final AttributeKey<Long>   DB2_CPU_MS     = AttributeKey.longKey("db2.cpu_time_ms");

    private final Supplier<Connection> connSupplier;
    private final String dbName;
    private final Logger otelLogger;
    private final int maxQuerySampleCount;
    private final boolean useSnapshotMonitor;

    public QuerySampleQuery(Supplier<Connection> connSupplier, String dbName,
                            Logger otelLogger, int maxQuerySampleCount,
                            boolean useSnapshotMonitor) {
        this.connSupplier        = connSupplier;
        this.dbName              = dbName;
        this.otelLogger          = otelLogger;
        this.maxQuerySampleCount = maxQuerySampleCount;
        this.useSnapshotMonitor  = useSnapshotMonitor;
    }

    public void collect() {
        String sql = String.format(
            useSnapshotMonitor ? SQL_SNAPSTMT : SQL_MON_GET_ACTIVITY,
            maxQuerySampleCount);

        try {
            Connection conn = connSupplier.get();
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {

                Instant now = Instant.now();
                while (rs.next()) {
                    String rawSql = rs.getString("STMT_TEXT");
                    if (rawSql == null || rawSql.isBlank()) continue;

                    String obfuscated = SqlObfuscator.obfuscate(rawSql.trim());
                    if (SqlObfuscator.OBFUSCATION_FAILED.equals(obfuscated)) {
                        log.warn("QuerySampleQuery: obfuscation failed for a statement — record dropped");
                        continue;
                    }

                    long elapsedMs = rs.getLong("ELAPSED_MS");
                    long cpuMs     = rs.getLong("CPU_MS");
                    long member    = rs.getLong("MEMBER");
                    String appId   = rs.getString("APPLICATION_ID");

                    AttributesBuilder attrs = Attributes.builder()
                        .put(DB_SYSTEM,      "ibm.db2")
                        .put(DB_NAMESPACE,   dbName)
                        .put(DB_QUERY_TEXT,  obfuscated)
                        .put(DB2_MEMBER,     member)
                        .put(DB2_ELAPSED_MS, elapsedMs);

                    if (appId != null) attrs.put(DB2_APP_ID, appId);
                    if (cpuMs >= 0)    attrs.put(DB2_CPU_MS, cpuMs);

                    otelLogger.logRecordBuilder()
                        .setSeverity(Severity.INFO)
                        .setSeverityText("INFO")
                        .setTimestamp(now.toEpochMilli(), TimeUnit.MILLISECONDS)
                        .setAllAttributes(attrs.build())
                        .emit();
                }
            }
        } catch (Exception e) {
            log.warn("QuerySampleQuery collection failed: {}", e.getMessage());
        }
    }
}
