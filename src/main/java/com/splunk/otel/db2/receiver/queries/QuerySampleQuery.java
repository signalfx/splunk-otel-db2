package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.util.SqlObfuscator;
import io.opentelemetry.api.logs.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.function.Supplier;

/**
 * Samples active queries from Db2 and emits each statement as an OTel log record
 * ({@code db.server.query_sample}).
 *
 * When a sampled session is in LOCKWAIT the record is enriched with blocking
 * attributes and emitted at severity WARN — same convention as the SQL Server,
 * Oracle, and PostgreSQL OTel receivers.  Idle blockers (holding a lock but no
 * active SQL) have their last statement looked up from the plan cache and
 * included as {@code db2.blocking.blocker.query_text} on the blocked session's record.
 *
 * Default path (useSnapshotMonitor=false, Db2 9.7+):
 *   MON_CURRENT_SQL LEFT JOIN MON_LOCKWAITS + MON_GET_CONNECTION.
 *   On Db2 10.5+ also JOINs MON_GET_ACTIVITY for LOCK_WAIT_TIME.
 *   blocking.start_time is computed as CURRENT TIMESTAMP - CURRENT TIMEZONE - LOCK_WAIT_ELAPSED_TIME SECONDS
 *   (server-side arithmetic — converts to UTC and avoids JVM/DB2 clock skew).
 *
 * Snapshot monitor fallback (useSnapshotMonitor=true, Db2 9.7+):
 *   SNAP_GET_APPL_INFO + SNAP_GET_STMT. Blocking detection not available on
 *   this path; records are always INFO severity.
 *
 * SQL text is always passed through {@link SqlObfuscator} before emission.
 * If obfuscation fails the record is dropped with a WARN log — never emitted raw.
 */
public class QuerySampleQuery {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(QuerySampleQuery.class);

    // Default path (Db2 9.7+) with blocking detection.
    // LOCK_WAIT_ELAPSED_TIME is INTEGER SECONDS on Db2 11.5 (confirmed via SYSCAT.COLUMNS).
    // BLOCKING_START_TIME is computed server-side as CURRENT TIMESTAMP - CURRENT TIMEZONE -
    // LOCK_WAIT_ELAPSED_TIME SECONDS (from MON_LOCKWAITS), converting local time to UTC and
    // avoiding JVM/DB2 clock skew. SYSIBMADM.LOCKWAITS is NOT
    // joined here — its column names differ between DB2 versions (REQ_APPLICATION_HANDLE in
    // MON_LOCKWAITS vs AGENT_ID_WAITING_FOR_LK in the old snapshot view) and it adds no
    // columns we don't already have from MON_LOCKWAITS.
    // MON_GET_CONNECTION appears twice (C for main session, BC for blocker) — each
    // wrapped in a derived table so Db2 does not raise "duplicate TVF" errors.
    // The main session join is LEFT so sessions not yet in MON_GET_CONNECTION (race,
    // closing connection) are not silently dropped — CLIENT_APPLNAME is COALESCE-guarded.
    private static final String SQL_MON_CURRENT =
        "SELECT S.APPLICATION_HANDLE, " +
        "       S.STMT_TEXT, " +
        "       CONCAT(CONCAT(S.ACTIVITY_STATE, ' '), S.ACTIVITY_TYPE) AS ACTIVITY_STATE, " +
        "       S.ACTIVITY_TYPE AS OPERATION_NAME, " +
        "       CHAR(CURRENT TIMESTAMP - S.ELAPSED_TIME_SEC SECONDS) AS LOCAL_START_TIME, " +
        "       S.ELAPSED_TIME_SEC AS WALL_ELAPSED_SEC, " +
        "       COALESCE(NULLIF(C.CLIENT_USERID, ''), S.SESSION_AUTH_ID) AS CLIENT_USERID, " +
        "       C.CLIENT_WRKSTNNAME, " +
        "       S.COORD_MEMBER AS MEMBER, " +
        "       C.TOTAL_CPU_TIME AS CPU_US, " +
        "       CAST(-1 AS BIGINT) AS LOCK_WAIT_TIME_MS, " +
        "       CASE WHEN LW.REQ_APPLICATION_HANDLE IS NOT NULL THEN 1 ELSE 0 END AS IS_BLOCKED, " +
        "       LW.HLD_APPLICATION_HANDLE AS BLOCKER_HANDLE, " +
        "       LW.HLD_MEMBER AS BLOCKER_MEMBER, " +
        "       LW.LOCK_WAIT_ELAPSED_TIME AS BLOCKING_WAIT_SEC, " +
        "       CASE WHEN LW.REQ_APPLICATION_HANDLE IS NOT NULL " +
        "            THEN CHAR(CURRENT TIMESTAMP - CURRENT TIMEZONE - LW.LOCK_WAIT_ELAPSED_TIME SECONDS) " +
        "            ELSE NULL END AS BLOCKING_START_TIME, " +
        "       LW.TABSCHEMA AS LOCK_SCHEMA, " +
        "       LW.TABNAME AS LOCK_OBJECT, " +
        "       LW.LOCK_OBJECT_TYPE AS LOCK_OBJECT_TYPE, " +
        "       LW.LOCK_MODE_REQUESTED AS LOCK_MODE, " +
        "       BLOCKER_SQL.STMT_TEXT AS BLOCKER_SQL, " +
        "       BLOCKER_CON.SESSION_AUTH_ID AS BLOCKER_USER, " +
        "       BLOCKER_CON.CLIENT_WRKSTNNAME AS BLOCKER_HOST, " +
        "       CASE WHEN BLOCKER_SQL.APPLICATION_HANDLE IS NULL THEN 1 ELSE 0 END AS BLOCKER_IS_IDLE, " +
        "       C.LAST_EXECUTABLE_ID AS EXEC_ID_BYTES " +
        "FROM SYSIBMADM.MON_CURRENT_SQL S " +
        "LEFT JOIN (SELECT APPLICATION_HANDLE, CLIENT_USERID, CLIENT_WRKSTNNAME, MEMBER, " +
        "                  TOTAL_CPU_TIME, CLIENT_APPLNAME, LAST_EXECUTABLE_ID " +
        "           FROM TABLE(MON_GET_CONNECTION(NULL, -2))) AS C " +
        "  ON C.APPLICATION_HANDLE = S.APPLICATION_HANDLE " +
        " AND C.MEMBER = S.COORD_MEMBER " +
        "LEFT JOIN SYSIBMADM.MON_LOCKWAITS LW " +
        "  ON LW.REQ_APPLICATION_HANDLE = S.APPLICATION_HANDLE " +
        " AND LW.REQ_MEMBER = S.COORD_MEMBER " +
        "LEFT JOIN SYSIBMADM.MON_CURRENT_SQL BLOCKER_SQL " +
        "  ON BLOCKER_SQL.APPLICATION_HANDLE = LW.HLD_APPLICATION_HANDLE " +
        "LEFT JOIN (SELECT APPLICATION_HANDLE, SESSION_AUTH_ID, CLIENT_WRKSTNNAME, MEMBER " +
        "           FROM TABLE(MON_GET_CONNECTION(CAST(NULL AS BIGINT), -2))) AS BLOCKER_CON " +
        "  ON BLOCKER_CON.APPLICATION_HANDLE = LW.HLD_APPLICATION_HANDLE " +
        " AND BLOCKER_CON.MEMBER = LW.HLD_MEMBER " +
        "WHERE S.STMT_TEXT IS NOT NULL " +
        "  AND S.ACTIVITY_STATE NOT IN ('IDLE') " +
        "  AND COALESCE(C.CLIENT_APPLNAME, '') <> 'splunk-otel-db2' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_CURRENT_SQL%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_LOCKWAITS%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_GET_CONNECTION%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_GET_PKG_CACHE_STMT%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%SNAP_GET_APPL_INFO%%%%' " +
        "ORDER BY S.ELAPSED_TIME_SEC DESC " +
        "FETCH FIRST %d ROWS ONLY";

    // Db2 10.5+ variant: also LEFT JOINs MON_GET_ACTIVITY for LOCK_WAIT_TIME.
    private static final String SQL_MON_CURRENT_105 =
        "SELECT S.APPLICATION_HANDLE, " +
        "       S.STMT_TEXT, " +
        "       CONCAT(CONCAT(S.ACTIVITY_STATE, ' '), S.ACTIVITY_TYPE) AS ACTIVITY_STATE, " +
        "       S.ACTIVITY_TYPE AS OPERATION_NAME, " +
        "       CHAR(CURRENT TIMESTAMP - S.ELAPSED_TIME_SEC SECONDS) AS LOCAL_START_TIME, " +
        "       S.ELAPSED_TIME_SEC AS WALL_ELAPSED_SEC, " +
        "       COALESCE(NULLIF(C.CLIENT_USERID, ''), S.SESSION_AUTH_ID) AS CLIENT_USERID, " +
        "       C.CLIENT_WRKSTNNAME, " +
        "       S.COORD_MEMBER AS MEMBER, " +
        "       C.TOTAL_CPU_TIME AS CPU_US, " +
        "       COALESCE(A.LOCK_WAIT_TIME, -1) AS LOCK_WAIT_TIME_MS, " +
        "       CASE WHEN LW.REQ_APPLICATION_HANDLE IS NOT NULL THEN 1 ELSE 0 END AS IS_BLOCKED, " +
        "       LW.HLD_APPLICATION_HANDLE AS BLOCKER_HANDLE, " +
        "       LW.HLD_MEMBER AS BLOCKER_MEMBER, " +
        "       LW.LOCK_WAIT_ELAPSED_TIME AS BLOCKING_WAIT_SEC, " +
        "       CASE WHEN LW.REQ_APPLICATION_HANDLE IS NOT NULL " +
        "            THEN CHAR(CURRENT TIMESTAMP - CURRENT TIMEZONE - LW.LOCK_WAIT_ELAPSED_TIME SECONDS) " +
        "            ELSE NULL END AS BLOCKING_START_TIME, " +
        "       LW.TABSCHEMA AS LOCK_SCHEMA, " +
        "       LW.TABNAME AS LOCK_OBJECT, " +
        "       LW.LOCK_OBJECT_TYPE AS LOCK_OBJECT_TYPE, " +
        "       LW.LOCK_MODE_REQUESTED AS LOCK_MODE, " +
        "       BLOCKER_SQL.STMT_TEXT AS BLOCKER_SQL, " +
        "       BLOCKER_CON.SESSION_AUTH_ID AS BLOCKER_USER, " +
        "       BLOCKER_CON.CLIENT_WRKSTNNAME AS BLOCKER_HOST, " +
        "       CASE WHEN BLOCKER_SQL.APPLICATION_HANDLE IS NULL THEN 1 ELSE 0 END AS BLOCKER_IS_IDLE, " +
        "       COALESCE(A.EXECUTABLE_ID, C.LAST_EXECUTABLE_ID) AS EXEC_ID_BYTES " +
        "FROM SYSIBMADM.MON_CURRENT_SQL S " +
        "LEFT JOIN (SELECT APPLICATION_HANDLE, CLIENT_USERID, CLIENT_WRKSTNNAME, MEMBER, " +
        "                  TOTAL_CPU_TIME, CLIENT_APPLNAME, LAST_EXECUTABLE_ID " +
        "           FROM TABLE(MON_GET_CONNECTION(NULL, -2))) AS C " +
        "  ON C.APPLICATION_HANDLE = S.APPLICATION_HANDLE " +
        " AND C.MEMBER = S.COORD_MEMBER " +
        "LEFT JOIN TABLE(MON_GET_ACTIVITY(NULL, -2)) AS A " +
        "  ON A.APPLICATION_HANDLE = S.APPLICATION_HANDLE " +
        " AND A.UOW_ID = S.UOW_ID " +
        " AND A.ACTIVITY_ID = S.ACTIVITY_ID " +
        "LEFT JOIN SYSIBMADM.MON_LOCKWAITS LW " +
        "  ON LW.REQ_APPLICATION_HANDLE = S.APPLICATION_HANDLE " +
        " AND LW.REQ_MEMBER = S.COORD_MEMBER " +
        "LEFT JOIN SYSIBMADM.MON_CURRENT_SQL BLOCKER_SQL " +
        "  ON BLOCKER_SQL.APPLICATION_HANDLE = LW.HLD_APPLICATION_HANDLE " +
        "LEFT JOIN (SELECT APPLICATION_HANDLE, SESSION_AUTH_ID, CLIENT_WRKSTNNAME, MEMBER " +
        "           FROM TABLE(MON_GET_CONNECTION(CAST(NULL AS BIGINT), -2))) AS BLOCKER_CON " +
        "  ON BLOCKER_CON.APPLICATION_HANDLE = LW.HLD_APPLICATION_HANDLE " +
        " AND BLOCKER_CON.MEMBER = LW.HLD_MEMBER " +
        "WHERE S.STMT_TEXT IS NOT NULL " +
        "  AND S.ACTIVITY_STATE NOT IN ('IDLE') " +
        "  AND COALESCE(C.CLIENT_APPLNAME, '') <> 'splunk-otel-db2' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_CURRENT_SQL%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_LOCKWAITS%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_GET_CONNECTION%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_GET_PKG_CACHE_STMT%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_GET_ACTIVITY%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%SNAP_GET_APPL_INFO%%%%' " +
        "ORDER BY S.ELAPSED_TIME_SEC DESC " +
        "FETCH FIRST %d ROWS ONLY";

    // Snapshot monitor fallback (Db2 9.7+). Blocking detection not available.
    // All three dbmon-api blocking endpoints return empty on this path — see README.
    private static final String SQL_SNAP =
        "SELECT COALESCE(SS.STMT_TEXT, S.TEXT) AS STMT_TEXT, " +
        "       CAST(SA.APPL_STATUS AS VARCHAR(22)) AS ACTIVITY_STATE, " +
        "       CAST(NULL AS VARCHAR(22)) AS OPERATION_NAME, " +
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
        "  AND COALESCE(SS.STMT_TEXT, S.TEXT) IS NOT NULL " +
        "  AND COALESCE(SA.APPL_NAME, '') <> 'splunk-otel-db2' " +
        "  AND COALESCE(SS.STMT_TEXT, S.TEXT, '') NOT LIKE '%%%%SNAP_GET_APPL_INFO%%%%' " +
        "  AND COALESCE(SS.STMT_TEXT, S.TEXT, '') NOT LIKE '%%%%SNAP_GET_STMT%%%%' " +
        "ORDER BY TIMESTAMPDIFF(2, CHAR(CURRENT TIMESTAMP - SS.STMT_START)) DESC " +
        "FETCH FIRST %d ROWS ONLY";

    // Idle-blocker plan-cache lookup: get the last SQL executed by a blocking session
    // that is now idle (no active statement in MON_CURRENT_SQL).
    private static final String IDLE_BLOCKER_SQL =
        "SELECT P.STMT_TEXT " +
        "FROM TABLE(MON_GET_CONNECTION(CAST(? AS BIGINT), CAST(? AS INTEGER))) AS C " +
        "JOIN TABLE(MON_GET_PKG_CACHE_STMT(NULL, NULL, NULL, CAST(? AS INTEGER))) AS P " +
        "  ON P.EXECUTABLE_ID = C.LAST_EXECUTABLE_ID " +
        "WHERE P.NUM_EXECUTIONS > 0 " +
        "  AND P.STMT_TEXT NOT LIKE '%%%%MON_GET_PKG_CACHE_STMT%%%%' " +
        "FETCH FIRST 1 ROW ONLY";

    /** Encoded as {@code major * 1000 + minor}, e.g. 10005 for 10.5, 9007 for 9.7. */
    static final int DB2_VERSION_10_5 = 10005;

    private final Supplier<Connection> connSupplier;
    private final String dbName;
    private final Logger otelLogger;
    private final int maxQuerySampleCount;
    private final boolean useSnapshotMonitor;
    private final int db2Version;
    private final int queryTimeoutSeconds;

    public QuerySampleQuery(Supplier<Connection> connSupplier, String dbName,
                            Logger otelLogger, int maxQuerySampleCount,
                            boolean useSnapshotMonitor) {
        this(connSupplier, dbName, otelLogger, maxQuerySampleCount, useSnapshotMonitor, 0);
    }

    public QuerySampleQuery(Supplier<Connection> connSupplier, String dbName,
                            Logger otelLogger, int maxQuerySampleCount,
                            boolean useSnapshotMonitor, int db2Version) {
        this(connSupplier, dbName, otelLogger, maxQuerySampleCount, useSnapshotMonitor, db2Version, 30);
    }

    public QuerySampleQuery(Supplier<Connection> connSupplier, String dbName,
                            Logger otelLogger, int maxQuerySampleCount,
                            boolean useSnapshotMonitor, int db2Version, int queryTimeoutSeconds) {
        this.connSupplier         = connSupplier;
        this.dbName               = dbName;
        this.otelLogger           = otelLogger;
        this.maxQuerySampleCount  = maxQuerySampleCount;
        this.useSnapshotMonitor   = useSnapshotMonitor;
        this.db2Version           = db2Version;
        this.queryTimeoutSeconds  = queryTimeoutSeconds;
        log.info("QuerySampleQuery: using {} path{}, timeout={}s{}",
            useSnapshotMonitor ? "SNAP_GET_*" : "MON_CURRENT_SQL",
            (!useSnapshotMonitor && db2Version >= DB2_VERSION_10_5) ? " with MON_GET_ACTIVITY (10.5+)" : "",
            queryTimeoutSeconds,
            useSnapshotMonitor ? " — blocking detection disabled on snapshot monitor path" : "");
    }

    public boolean collect() {
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
            try (Statement st = conn.createStatement()) {
                st.setQueryTimeout(queryTimeoutSeconds);
                try (ResultSet rs = st.executeQuery(sql)) {
                    java.time.Instant now = java.time.Instant.now();
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
                        String opName     = rs.getString("OPERATION_NAME");
                        String startTime  = rs.getString("LOCAL_START_TIME");
                        double elapsedSec = rs.getLong("WALL_ELAPSED_SEC");
                        String authId     = rs.getString("CLIENT_USERID");
                        String clientHost = rs.getString("CLIENT_WRKSTNNAME");
                        long member       = rs.getLong("MEMBER");
                        long cpuUs        = rs.getLong("CPU_US");
                        long waitMs       = useSnapshotMonitor ? -1L : rs.getLong("LOCK_WAIT_TIME_MS");
                        String execIdHex  = null;
                        if (!useSnapshotMonitor) {
                            byte[] execIdBytes = rs.getBytes("EXEC_ID_BYTES");
                            if (execIdBytes != null) execIdHex = bytesToHex(execIdBytes);
                        }

                        QuerySampleEvent ev = QuerySampleEvent.of(dbName, obfuscated, elapsedSec, member)
                            .appHandle(appHandle)
                            .activityState(state)
                            .sessionStatus(deriveSessionStatus(state))
                            .operationName(opName)
                            .queryStart(startTime)
                            .userName(authId)
                            .clientAddress(clientHost)
                            .cpuTimeMicros(cpuUs)
                            .waitTimeMillis(waitMs)
                            .planHash(execIdHex);

                        // Blocking columns only on the default (MON_CURRENT) paths.
                        // IS_BLOCKED is 1 whenever a MON_LOCKWAITS row matched — even when
                        // HLD_APPLICATION_HANDLE is NULL (global lock or unidentified holder).
                        if (!useSnapshotMonitor && rs.getInt("IS_BLOCKED") == 1) {
                            double waitSec = rs.getLong("BLOCKING_WAIT_SEC");

                            String blockingStartTime = toRfc3339(rs.getString("BLOCKING_START_TIME"));

                            ev.lockObjectType(trim(rs.getString("LOCK_OBJECT_TYPE")))
                              .lockMode(trim(rs.getString("LOCK_MODE")))
                              .lockObjectName(trim(rs.getString("LOCK_OBJECT")))
                              .lockSchema(trim(rs.getString("LOCK_SCHEMA")))
                              .blockingStartTime(blockingStartTime)
                              .waitDurationSeconds(waitSec);

                            // Blocker identity is NULL for global lock waits or unknown holders.
                            Object blockerHandleObj = rs.getObject("BLOCKER_HANDLE");
                            if (blockerHandleObj != null) {
                                long    blockerHandleLong = ((Number) blockerHandleObj).longValue();
                                long    blockerMember     = rs.getLong("BLOCKER_MEMBER");
                                String  blockerRawSql = rs.getString("BLOCKER_SQL");
                                // BLOCKER_IS_IDLE=1 when BLOCKER_SQL join returned no active
                                // statement — but this is ambiguous: could be a genuinely idle
                                // holder OR a cross-member miss after the member-qualified join.
                                // Use the pkg cache lookup result to confirm: found = truly idle;
                                // not found = unknown, omit is_idle rather than report false data.
                                boolean mayBeIdle = rs.getInt("BLOCKER_IS_IDLE") == 1;

                                String blockerObf = null;
                                if (blockerRawSql != null && !blockerRawSql.isBlank()) {
                                    String ob = SqlObfuscator.obfuscate(blockerRawSql.trim());
                                    if (!SqlObfuscator.OBFUSCATION_FAILED.equals(ob)) blockerObf = ob;
                                }

                                Boolean isIdle = null;
                                if (!mayBeIdle) {
                                    // Active blocker — join confirmed it has live SQL
                                    isIdle = false;
                                } else if (blockerObf == null) {
                                    // No active SQL — try pkg cache to confirm idleness
                                    blockerObf = fetchIdleBlockerSql(conn, blockerHandleLong, blockerMember);
                                    if (blockerObf != null) {
                                        isIdle = true; // pkg cache confirms: holder is idle
                                    }
                                    // pkg cache miss: could be cross-member or evicted — omit is_idle
                                }

                                ev.blockerHandle(String.valueOf(blockerHandleLong))
                                  .blockerUser(trim(rs.getString("BLOCKER_USER")))
                                  .blockerHost(trim(rs.getString("BLOCKER_HOST")))
                                  .blockerIsIdle(isIdle)
                                  .blockerQueryText(blockerObf);
                            }
                        }

                        ev.emit(otelLogger, now);
                        count++;
                    }
                    log.debug("QuerySampleQuery: emitted {} samples", count);
                }
            }
            return true;
        } catch (Exception e) {
            log.warn("QuerySampleQuery collection failed: {}", e.getMessage(), e);
            return false;
        }
    }

    private String fetchIdleBlockerSql(Connection conn, long blockerHandle, long blockerMember) {
        int member = blockerMember >= 0 ? (int) blockerMember : -2;
        try (PreparedStatement ps = conn.prepareStatement(IDLE_BLOCKER_SQL)) {
            ps.setQueryTimeout(queryTimeoutSeconds);
            ps.setLong(1, blockerHandle);
            ps.setInt(2, member);
            ps.setInt(3, member);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String raw = rs.getString("STMT_TEXT");
                    if (raw != null && !raw.isBlank()) {
                        String ob = SqlObfuscator.obfuscate(raw.trim());
                        return SqlObfuscator.OBFUSCATION_FAILED.equals(ob) ? null : ob;
                    }
                }
            }
        } catch (Exception e) {
            log.debug("QuerySampleQuery: idle blocker SQL lookup failed for handle={} member={}: {}",
                blockerHandle, blockerMember, e.getMessage());
        }
        return null;
    }

    private static String trim(String s) {
        return s != null ? s.trim() : null;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    // Converts Db2's CHAR(TIMESTAMP) output "YYYY-MM-DD-HH.MM.SS.NNNNNN" to RFC3339 UTC
    // "YYYY-MM-DDTHH:MM:SS.NNNNNNZ". The SQL expression already subtracts CURRENT TIMEZONE
    // (DB2's UTC offset) so the value is true UTC before it reaches this method.
    static String toRfc3339(String db2Timestamp) {
        if (db2Timestamp == null) return null;
        // "2024-01-15-12.30.45.123456" → "2024-01-15T12:30:45.123456Z"
        String s = db2Timestamp.trim();
        if (s.length() < 19) return s;
        // Replace the date-time separator '-' (position 10) with 'T'
        // and the first two ':'-equivalent '.' separators in the time part
        char[] c = s.toCharArray();
        c[10] = 'T';
        c[13] = ':';
        c[16] = ':';
        return new String(c) + "Z";
    }

    // Derives a coarse session status from the composite ACTIVITY_STATE column
    // (e.g. "LOCK WAIT DML", "EXECUTING READ"). Follows the same pattern as
    // MongoDB's deriveOperationState(): map raw DB flags to canonical values.
    // Returns null for unknown values so the attribute is omitted rather than
    // emitting a wrong value.
    static String deriveSessionStatus(String activityState) {
        if (activityState == null) return null;
        String s = activityState.trim();
        // MON_CURRENT_SQL composite: "EXECUTING READ", "RECOMP DML", etc.
        if (s.startsWith("EXECUTING") || s.startsWith("RECOMP")) return "active";
        // MON_CURRENT_SQL composite: "LOCKWAIT DML"; snapshot APPL_STATUS: "LOCKWAIT" or "LOCK WAIT"
        if (s.startsWith("LOCKWAIT") || s.startsWith("LOCK WAIT") || s.startsWith("CLIENT WAIT")) return "waiting";
        if (s.startsWith("IDLE")) return "idle";
        // Snapshot monitor APPL_STATUS: UOWEXEC = executing a unit of work
        if (s.equals("UOWEXEC")) return "active";
        return null;
    }
}
