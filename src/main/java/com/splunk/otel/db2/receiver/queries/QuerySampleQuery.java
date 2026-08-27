package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.util.SqlObfuscator;
import io.opentelemetry.api.logs.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.function.Supplier;

/**
 * Samples active queries from Db2 and emits each statement as an OTel log record.
 *
 * Default path (useSnapshotMonitor=false, Db2 9.7+):
 *   MON_CURRENT_SQL + MON_GET_CONNECTION JOIN — returns one row per currently executing
 *   or waiting activity. No DFT_MON_STMT requirement. Self-exclusion and IDLE filtering
 *   applied in SQL. Rows ordered by elapsed time descending, limited to maxQuerySampleCount.
 *
 *   On Db2 10.5+, additionally joins MON_GET_ACTIVITY to expose LOCK_WAIT_TIME as
 *   db2.wait_time (seconds). On earlier versions the attribute is omitted.
 *
 * Snapshot monitor fallback (useSnapshotMonitor=true, Db2 9.7+):
 *   SNAP_GET_APPL_INFO + SNAP_GET_STMT + SYSCAT.STATEMENTS (static SQL fallback) —
 *   richer activity states (LOCKWAIT, UOWWAIT, etc.); requires DFT_MON_STMT ON.
 *
 * Emitted log record attributes — see {@link QuerySampleEvent} for the full key list.
 *
 * SQL text is always passed through {@link SqlObfuscator} before emission.
 * If obfuscation fails the record is dropped with a WARN log — never emitted raw.
 */
public class QuerySampleQuery {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(QuerySampleQuery.class);

    // Default path: SYSIBMADM.MON_CURRENT_SQL (administrative view, Db2 9.7+) +
    // MON_GET_CONNECTION JOIN. MON_CURRENT_SQL is a VIEW, not a table function —
    // do not wrap in TABLE(). Uses ELAPSED_TIME_SEC (seconds) and COORD_MEMBER.
    // No DFT_MON_STMT requirement.
    // Activity filter: APPLICATION_STATUS IN ('CONNECTED', 'UOWEXEC') — matches the
    // DB Agent's APPL_STATUS IN ('CONNECTED', 'UOWEXEC') whitelist approach.
    // LOCAL_START_TIME: reconstructed from CURRENT TIMESTAMP - ELAPSED_TIME_SEC (MON_CURRENT_SQL
    // does not expose a statement-start column directly).
    // CLIENT_USERID: COALESCE with SESSION_AUTH_ID — CLIENT_USERID is often empty when the
    // JDBC client does not set it explicitly, but SESSION_AUTH_ID is always populated.
    private static final String SQL_MON_CURRENT =
        "SELECT S.APPLICATION_HANDLE, " +
        "       S.STMT_TEXT, " +
        "       CONCAT(CONCAT(S.ACTIVITY_STATE, ' '), S.ACTIVITY_TYPE) AS ACTIVITY_STATE, " +
        "       CHAR(CURRENT TIMESTAMP - S.ELAPSED_TIME_SEC SECONDS) AS LOCAL_START_TIME, " +
        "       S.ELAPSED_TIME_SEC AS WALL_ELAPSED_SEC, " +
        "       COALESCE(NULLIF(C.CLIENT_USERID, ''), S.SESSION_AUTH_ID) AS CLIENT_USERID, " +
        "       C.CLIENT_WRKSTNNAME, " +
        "       S.COORD_MEMBER AS MEMBER, " +
        "       C.TOTAL_CPU_TIME AS CPU_US, " +
        "       CAST(-1 AS BIGINT) AS LOCK_WAIT_TIME_MS " +
        "FROM SYSIBMADM.MON_CURRENT_SQL S " +
        "JOIN TABLE(MON_GET_CONNECTION(NULL, -2)) AS C " +
        "  ON C.APPLICATION_HANDLE = S.APPLICATION_HANDLE " +
        " AND C.MEMBER = S.COORD_MEMBER " +
        "WHERE S.STMT_TEXT IS NOT NULL " +
        "  AND C.APPLICATION_STATUS IN ('CONNECTED', 'UOWEXEC') " +
        "  AND COALESCE(C.CLIENT_APPLNAME, '') <> 'splunk-otel-db2' " +
        "  AND S.STMT_TEXT NOT LIKE '%%MON_CURRENT_SQL%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%SNAP_GET_APPL_INFO%%' " +
        "ORDER BY S.ELAPSED_TIME_SEC DESC " +
        "FETCH FIRST %d ROWS ONLY";

    // Db2 10.5+ variant: LEFT JOIN to MON_GET_ACTIVITY exposes LOCK_WAIT_TIME.
    // LOCK_WAIT_TIME is in milliseconds; QuerySampleEvent.waitTimeMillis converts to seconds.
    private static final String SQL_MON_CURRENT_105 =
        "SELECT S.APPLICATION_HANDLE, " +
        "       S.STMT_TEXT, " +
        "       CONCAT(CONCAT(S.ACTIVITY_STATE, ' '), S.ACTIVITY_TYPE) AS ACTIVITY_STATE, " +
        "       CHAR(CURRENT TIMESTAMP - S.ELAPSED_TIME_SEC SECONDS) AS LOCAL_START_TIME, " +
        "       S.ELAPSED_TIME_SEC AS WALL_ELAPSED_SEC, " +
        "       COALESCE(NULLIF(C.CLIENT_USERID, ''), S.SESSION_AUTH_ID) AS CLIENT_USERID, " +
        "       C.CLIENT_WRKSTNNAME, " +
        "       S.COORD_MEMBER AS MEMBER, " +
        "       C.TOTAL_CPU_TIME AS CPU_US, " +
        "       COALESCE(A.LOCK_WAIT_TIME, -1) AS LOCK_WAIT_TIME_MS " +
        "FROM SYSIBMADM.MON_CURRENT_SQL S " +
        "JOIN TABLE(MON_GET_CONNECTION(NULL, -2)) AS C " +
        "  ON C.APPLICATION_HANDLE = S.APPLICATION_HANDLE " +
        " AND C.MEMBER = S.COORD_MEMBER " +
        "LEFT JOIN TABLE(MON_GET_ACTIVITY(NULL, -2)) AS A " +
        "  ON A.APPLICATION_HANDLE = S.APPLICATION_HANDLE " +
        " AND A.UOW_ID = S.UOW_ID " +
        " AND A.ACTIVITY_ID = S.ACTIVITY_ID " +
        "WHERE S.STMT_TEXT IS NOT NULL " +
        "  AND C.APPLICATION_STATUS IN ('CONNECTED', 'UOWEXEC') " +
        "  AND COALESCE(C.CLIENT_APPLNAME, '') <> 'splunk-otel-db2' " +
        "  AND S.STMT_TEXT NOT LIKE '%%MON_CURRENT_SQL%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%SNAP_GET_APPL_INFO%%' " +
        "ORDER BY S.ELAPSED_TIME_SEC DESC " +
        "FETCH FIRST %d ROWS ONLY";

    // Snapshot monitor fallback (Db2 9.7+). Requires DFT_MON_STMT ON.
    // Richer states (LOCKWAIT, UOWWAIT, etc.). Falls back to SYSCAT.STATEMENTS
    // for static (package) SQL when STMT_TEXT is null on the snapshot row.
    // DBPARTITIONNUM is the portable partition/member column in SNAP_GET_APPL_INFO.
    private static final String SQL_SNAP =
        "SELECT COALESCE(SS.STMT_TEXT, S.TEXT) AS STMT_TEXT, " +
        "       ( CASE WHEN SA.APPL_STATUS = 'UOWEXEC' " +
        "              THEN CAST(SS.STMT_OPERATION AS VARCHAR(22)) " +
        "              ELSE CAST(SA.APPL_STATUS AS VARCHAR(22)) " +
        "         END ) AS ACTIVITY_STATE, " +
        "       CHAR(SS.STMT_START) AS LOCAL_START_TIME, " +
        "       TIMESTAMPDIFF(2, CHAR(CURRENT TIMESTAMP - SS.STMT_START)) AS WALL_ELAPSED_SEC, " +
        "       COALESCE(NULLIF(SA.EXECUTION_ID, ''), SA.PRIMARY_AUTH_ID) AS CLIENT_USERID, " +
        "       SA.CLIENT_NNAME AS CLIENT_WRKSTNNAME, " +
        "       SA.AGENT_ID AS APPLICATION_HANDLE, " +
        "       SA.DBPARTITIONNUM AS MEMBER, " +
        "       CAST(-1 AS BIGINT) AS CPU_US " +
        "FROM SYSIBM.SYSDUMMY1, " +
        "     TABLE(SNAP_GET_APPL_INFO(CAST(NULL AS VARCHAR(255)), -1)) AS SA, " +
        "     TABLE(SNAP_GET_STMT(CAST(NULL AS VARCHAR(255)), -1)) AS SS " +
        "LEFT OUTER JOIN SYSCAT.STATEMENTS S " +
        "  ON (S.PKGNAME = SS.PACKAGE_NAME AND S.SECTNO = SS.SECTION_NUMBER) " +
        "WHERE SS.STMT_STOP IS NULL " +
        "  AND SA.AGENT_ID = SS.AGENT_ID " +
        "  AND SA.APPL_STATUS IN ('CONNECTED', 'UOWEXEC') " +
        "  AND COALESCE(SS.STMT_TEXT, S.TEXT) IS NOT NULL " +
        "  AND COALESCE(SA.APPL_NAME, '') <> 'splunk-otel-db2' " +
        "  AND COALESCE(SS.STMT_TEXT, S.TEXT, '') NOT LIKE '%%SNAP_GET_APPL_INFO%%' " +
        "  AND COALESCE(SS.STMT_TEXT, S.TEXT, '') NOT LIKE '%%SNAP_GET_STMT%%' " +
        "ORDER BY TIMESTAMPDIFF(2, CHAR(CURRENT TIMESTAMP - SS.STMT_START)) DESC " +
        "FETCH FIRST %d ROWS ONLY";

    /** Encoded as {@code major * 1000 + minor}, e.g. 10005 for 10.5, 9007 for 9.7. */
    static final int DB2_VERSION_10_5 = 10005;

    private final Supplier<Connection> connSupplier;
    private final String dbName;
    private final Logger otelLogger;
    private final int maxQuerySampleCount;
    private final boolean useSnapshotMonitor;
    private final int db2Version;

    public QuerySampleQuery(Supplier<Connection> connSupplier, String dbName,
                            Logger otelLogger, int maxQuerySampleCount,
                            boolean useSnapshotMonitor) {
        this(connSupplier, dbName, otelLogger, maxQuerySampleCount, useSnapshotMonitor, 0);
    }

    public QuerySampleQuery(Supplier<Connection> connSupplier, String dbName,
                            Logger otelLogger, int maxQuerySampleCount,
                            boolean useSnapshotMonitor, int db2Version) {
        this.connSupplier        = connSupplier;
        this.dbName              = dbName;
        this.otelLogger          = otelLogger;
        this.maxQuerySampleCount = maxQuerySampleCount;
        this.useSnapshotMonitor  = useSnapshotMonitor;
        this.db2Version          = db2Version;
        log.info("QuerySampleQuery: using {} path{}",
            useSnapshotMonitor ? "SNAP_GET_*" : "MON_CURRENT_SQL",
            (!useSnapshotMonitor && db2Version >= DB2_VERSION_10_5) ? " with MON_GET_ACTIVITY (10.5+)" : "");
    }

    public void collect() {
        String sqlTemplate;
        if (useSnapshotMonitor) {
            sqlTemplate = SQL_SNAP;
        } else if (db2Version >= DB2_VERSION_10_5) {
            sqlTemplate = SQL_MON_CURRENT_105;
        } else {
            sqlTemplate = SQL_MON_CURRENT;
        }
        String sql = String.format(sqlTemplate, maxQuerySampleCount);

        try {
            Connection conn = connSupplier.get();
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {

                Instant now = Instant.now();
                int count = 0;
                while (rs.next()) {
                    String rawSql = rs.getString("STMT_TEXT");
                    if (rawSql == null || rawSql.isBlank()) continue;

                    String appHandle  = rs.getString("APPLICATION_HANDLE");
                    String obfuscated = SqlObfuscator.obfuscate(rawSql.trim());
                    if (SqlObfuscator.OBFUSCATION_FAILED.equals(obfuscated)) {
                        log.warn("QuerySampleQuery: obfuscation failed for application_handle={} — record dropped", appHandle);
                        continue;
                    }
                    String state      = rs.getString("ACTIVITY_STATE");
                    String startTime  = rs.getString("LOCAL_START_TIME");
                    double elapsedSec = rs.getLong("WALL_ELAPSED_SEC");
                    String authId     = rs.getString("CLIENT_USERID");
                    String clientHost = rs.getString("CLIENT_WRKSTNNAME");
                    long member       = rs.getLong("MEMBER");
                    long cpuUs        = rs.getLong("CPU_US");
                    long waitMs       = useSnapshotMonitor ? -1L : rs.getLong("LOCK_WAIT_TIME_MS");

                    QuerySampleEvent.of(dbName, obfuscated, elapsedSec, member)
                        .appHandle(appHandle)
                        .activityState(state)
                        .queryStart(startTime)
                        .userName(authId)
                        .clientAddress(clientHost)
                        .cpuTimeMicros(cpuUs)
                        .waitTimeMillis(waitMs)
                        .emit(otelLogger, now);
                    count++;
                }
                log.debug("QuerySampleQuery: emitted {} samples", count);
            }
        } catch (Exception e) {
            log.warn("QuerySampleQuery collection failed: {}", e.getMessage());
        }
    }
}
