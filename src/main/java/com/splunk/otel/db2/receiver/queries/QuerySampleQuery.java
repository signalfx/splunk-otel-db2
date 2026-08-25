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
 * Default path (useSnapshotMonitor=false, Db2 9.7+):
 *   MON_CURRENT_SQL + MON_GET_CONNECTION JOIN — returns one row per currently executing
 *   or waiting activity. No DFT_MON_STMT requirement. Self-exclusion and IDLE filtering
 *   applied in SQL. Rows ordered by elapsed time descending, limited to maxQuerySampleCount.
 *
 * Snapshot monitor fallback (useSnapshotMonitor=true, Db2 9.1+):
 *   SNAP_GET_APPL_INFO + SNAP_GET_STMT + SYSCAT.STATEMENTS (static SQL fallback) —
 *   richer activity states (LOCKWAIT, UOWWAIT, etc.); requires DFT_MON_STMT ON.
 *
 * Emitted log record attributes:
 *   event.name            = "db.server.query_sample"
 *   db.system.name        = "ibm.db2"
 *   db.namespace          = database name
 *   db.query.text         = obfuscated SQL statement
 *   db2.application_handle= application handle / agent ID
 *   db2.activity_state    = "ACTIVITY_STATE ACTIVITY_TYPE" e.g. "EXECUTING SELECT", "LOCKWAIT DML"
 *   db2.query_start       = statement start timestamp
 *   db2.session.duration  = wall-clock elapsed seconds (double)
 *   db2.member            = Db2 member number (default path only)
 *   db2.cpu_time          = CPU time in seconds (default path only, from MON_GET_CONNECTION)
 *   user.name             = authenticated user ID
 *   client.address        = client workstation name
 *
 * SQL text is always passed through {@link SqlObfuscator} before emission.
 * If obfuscation fails the record is dropped with a WARN log — never emitted raw.
 */
public class QuerySampleQuery {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(QuerySampleQuery.class);

    // Default path: MON_CURRENT_SQL + MON_GET_CONNECTION (Db2 9.7+).
    // No DFT_MON_STMT requirement. Filters out IDLE sessions, the receiver's own
    // connection, and the sampler's own query text.
    private static final String SQL_MON_CURRENT =
        "SELECT S.APPLICATION_HANDLE, " +
        "       S.STMT_TEXT, " +
        "       CONCAT(CONCAT(S.ACTIVITY_STATE, ' '), S.ACTIVITY_TYPE) AS ACTIVITY_STATE, " +
        "       S.LOCAL_START_TIME, " +
        "       TIMESTAMPDIFF(2, CHAR(CURRENT TIMESTAMP - S.LOCAL_START_TIME)) AS WALL_ELAPSED_SEC, " +
        "       C.CLIENT_USERID, " +
        "       C.CLIENT_WRKSTNNAME, " +
        "       C.MEMBER, " +
        "       C.TOTAL_CPU_TIME AS CPU_US " +
        "FROM TABLE(MON_CURRENT_SQL(CAST(NULL AS VARCHAR(255)), -2)) AS S " +
        "JOIN TABLE(MON_GET_CONNECTION(NULL, -2)) AS C " +
        "  ON C.APPLICATION_HANDLE = S.APPLICATION_HANDLE " +
        "WHERE S.STMT_TEXT IS NOT NULL " +
        "  AND S.ACTIVITY_STATE NOT IN ('IDLE') " +
        "  AND COALESCE(C.CLIENT_APPLNAME, '') <> 'splunk-otel-db2' " +
        "  AND S.STMT_TEXT NOT LIKE '%%MON_CURRENT_SQL%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%SNAP_GET_APPL_INFO%%' " +
        "ORDER BY S.LOCAL_START_TIME ASC " +
        "FETCH FIRST %d ROWS ONLY";

    // Snapshot monitor fallback (Db2 9.1+). Requires DFT_MON_STMT ON.
    // Richer states (LOCKWAIT, UOWWAIT, etc.). Falls back to SYSCAT.STATEMENTS
    // for static (package) SQL when STMT_TEXT is null on the snapshot row.
    private static final String SQL_SNAP =
        "SELECT COALESCE(SS.STMT_TEXT, S.TEXT) AS STMT_TEXT, " +
        "       ( CASE WHEN SA.APPL_STATUS = 'UOWEXEC' " +
        "              THEN CAST(SS.STMT_OPERATION AS VARCHAR(22)) " +
        "              ELSE CAST(SA.APPL_STATUS AS VARCHAR(22)) " +
        "         END ) AS ACTIVITY_STATE, " +
        "       SS.STMT_START AS LOCAL_START_TIME, " +
        "       TIMESTAMPDIFF(2, CHAR(CURRENT TIMESTAMP - SS.STMT_START)) AS WALL_ELAPSED_SEC, " +
        "       SA.EXECUTION_ID AS CLIENT_USERID, " +
        "       SA.CLIENT_NNAME AS CLIENT_WRKSTNNAME, " +
        "       SA.AGENT_ID AS APPLICATION_HANDLE, " +
        "       SA.NODE_NUMBER AS MEMBER, " +
        "       CAST(-1 AS BIGINT) AS CPU_US " +
        "FROM SYSIBM.SYSDUMMY1, " +
        "     TABLE(SNAP_GET_APPL_INFO(CAST(NULL AS VARCHAR(255)), -1)) AS SA, " +
        "     TABLE(SNAP_GET_STMT(CAST(NULL AS VARCHAR(255)), -1)) AS SS " +
        "LEFT OUTER JOIN SYSCAT.STATEMENTS S " +
        "  ON (S.PKGNAME = SS.PACKAGE_NAME AND S.SECTNO = SS.SECTION_NUMBER) " +
        "WHERE SS.STMT_STOP IS NULL " +
        "  AND SA.AGENT_ID = SS.AGENT_ID " +
        "  AND COALESCE(SA.APPL_NAME, '') <> 'splunk-otel-db2' " +
        "  AND COALESCE(SS.STMT_TEXT, S.TEXT, '') NOT LIKE '%%SNAP_GET_APPL_INFO%%' " +
        "  AND COALESCE(SS.STMT_TEXT, S.TEXT, '') NOT LIKE '%%SNAP_GET_STMT%%' " +
        "FETCH FIRST %d ROWS ONLY";

    private static final AttributeKey<String> EVENT_NAME      = AttributeKey.stringKey("event.name");
    private static final AttributeKey<String> DB_SYSTEM       = AttributeKey.stringKey("db.system.name");
    private static final AttributeKey<String> DB_NAMESPACE    = AttributeKey.stringKey("db.namespace");
    private static final AttributeKey<String> DB_QUERY_TEXT   = AttributeKey.stringKey("db.query.text");
    private static final AttributeKey<String> DB2_APP_HANDLE  = AttributeKey.stringKey("db2.application_handle");
    private static final AttributeKey<String> DB2_ACTIVITY    = AttributeKey.stringKey("db2.activity_state");
    private static final AttributeKey<String> DB2_QUERY_START = AttributeKey.stringKey("db2.query_start");
    private static final AttributeKey<Double> DB2_DURATION    = AttributeKey.doubleKey("db2.session.duration");
    private static final AttributeKey<Long>   DB2_MEMBER      = AttributeKey.longKey("db2.member");
    private static final AttributeKey<Double> DB2_CPU_TIME    = AttributeKey.doubleKey("db2.cpu_time");
    private static final AttributeKey<String> USER_NAME       = AttributeKey.stringKey("user.name");
    private static final AttributeKey<String> CLIENT_ADDRESS  = AttributeKey.stringKey("client.address");

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
        log.info("QuerySampleQuery: using {} path",
            useSnapshotMonitor ? "SNAP_GET_*" : "MON_CURRENT_SQL");
    }

    public void collect() {
        String sql = String.format(
            useSnapshotMonitor ? SQL_SNAP : SQL_MON_CURRENT,
            maxQuerySampleCount);

        try {
            Connection conn = connSupplier.get();
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {

                Instant now = Instant.now();
                int count = 0;
                while (rs.next()) {
                    String rawSql = rs.getString("STMT_TEXT");
                    if (rawSql == null || rawSql.isBlank()) continue;

                    String obfuscated = SqlObfuscator.obfuscate(rawSql.trim());
                    if (SqlObfuscator.OBFUSCATION_FAILED.equals(obfuscated)) {
                        log.warn("QuerySampleQuery: obfuscation failed for a statement — record dropped");
                        continue;
                    }

                    String appHandle  = rs.getString("APPLICATION_HANDLE");
                    String state      = rs.getString("ACTIVITY_STATE");
                    String startTime  = rs.getString("LOCAL_START_TIME");
                    double elapsedSec = rs.getLong("WALL_ELAPSED_SEC");
                    String authId     = rs.getString("CLIENT_USERID");
                    String clientHost = rs.getString("CLIENT_WRKSTNNAME");
                    long member       = rs.getLong("MEMBER");
                    long cpuUs        = rs.getLong("CPU_US");

                    AttributesBuilder attrs = Attributes.builder()
                        .put(EVENT_NAME,     "db.server.query_sample")
                        .put(DB_SYSTEM,      "ibm.db2")
                        .put(DB_NAMESPACE,   dbName)
                        .put(DB_QUERY_TEXT,  obfuscated)
                        .put(DB2_DURATION,   elapsedSec)
                        .put(DB2_MEMBER,     member);

                    if (appHandle  != null) attrs.put(DB2_APP_HANDLE,  appHandle);
                    if (state      != null) attrs.put(DB2_ACTIVITY,    state);
                    if (startTime  != null) attrs.put(DB2_QUERY_START, startTime);
                    if (authId     != null) attrs.put(USER_NAME,       authId);
                    if (clientHost != null) attrs.put(CLIENT_ADDRESS,  clientHost);
                    if (cpuUs >= 0)         attrs.put(DB2_CPU_TIME,    cpuUs / 1_000_000.0);

                    otelLogger.logRecordBuilder()
                        .setSeverity(Severity.INFO)
                        .setSeverityText("INFO")
                        .setTimestamp(now.toEpochMilli(), TimeUnit.MILLISECONDS)
                        .setAllAttributes(attrs.build())
                        .emit();
                    count++;
                }
                log.debug("QuerySampleQuery: emitted {} samples", count);
            }
        } catch (Exception e) {
            log.warn("QuerySampleQuery collection failed: {}", e.getMessage());
        }
    }
}
