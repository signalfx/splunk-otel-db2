package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.util.SqlObfuscator;
import io.opentelemetry.api.logs.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.function.Supplier;

/**
 * Detects blocking sessions in Db2 and emits each blocked→blocker pair as an OTel
 * log record ({@code db.server.blocking_session}, severity WARN).
 *
 * Two SQL paths selected at construction time via {@code useSnapshotMonitor}:
 *
 * Default path (useSnapshotMonitor=false) — explicit GRANTs required:
 *   Queries {@code SYSIBMADM.MON_LOCKWAITS} joined with {@code MON_CURRENT_SQL}
 *   and {@code MON_GET_CONNECTION}. Provides full SQL text for both sides,
 *   user/host for both sides, activity state, and idle-blocker plan-cache lookup.
 *   Start time approximated as now minus {@code LOCK_WAIT_ELAPSED_TIME} (ms).
 *
 * Snapshot path (useSnapshotMonitor=true) — SYSMON authority sufficient:
 *   Queries {@code SYSIBMADM.LOCKWAITS} only. Requires SYSMON_GROUP membership;
 *   no explicit SQL GRANTs needed. Provides exact {@code LOCK_WAIT_START_TIME}
 *   (TIMESTAMP), lock details, and blocked user. SQL text and blocker user/host
 *   are not available from this view.
 *   Prerequisite: DB2 lock monitor switch must be ON. Enable with:
 *     db2 update monitor switches using lock on
 *   or permanently via DBM CFG: UPDATE DBM CFG USING DFT_MON_LOCK ON (requires db2stop/db2start).
 *
 * SQL design note (default path): DB2 does not allow TABLE(MON_GET_CONNECTION(...))
 * to appear more than once in the same FROM clause. Each side uses an independent
 * derived-table subquery (C1 / C2) to avoid both the duplicate-TVF restriction and
 * the CTE+WITH-UR parser ambiguity.
 */
public class BlockingSessionQuery {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(BlockingSessionQuery.class);

    // Default path: MON_LOCKWAITS + MON_CURRENT_SQL + MON_GET_CONNECTION.
    // Requires explicit GRANTs. Full SQL text, user/host both sides, activity state,
    // idle-blocker lookup. Start time approximated as now - LOCK_WAIT_ELAPSED_TIME (INTEGER ms).
    // exposed for integration test diagnostics — not part of the public API
    public static final String SQL =
        "SELECT " +
        "  LW.REQ_APPLICATION_HANDLE               AS BLOCKED_HANDLE, " +
        "  LW.REQ_MEMBER                           AS BLOCKED_MEMBER, " +
        "  LW.HLD_APPLICATION_HANDLE               AS BLOCKER_HANDLE, " +
        "  LW.HLD_MEMBER                           AS BLOCKER_MEMBER, " +
        "  LW.TABSCHEMA                            AS LOCK_SCHEMA, " +
        "  LW.TABNAME                              AS LOCK_OBJECT, " +
        "  LW.LOCK_OBJECT_TYPE                     AS LOCK_OBJECT_TYPE, " +
        "  LW.LOCK_MODE                            AS LOCK_MODE, " +
        "  LW.LOCK_WAIT_ELAPSED_TIME               AS WAIT_ELAPSED_SEC, " +
        "  BLOCKED_SQL.STMT_TEXT                   AS BLOCKED_SQL, " +
        "  CONCAT(CONCAT(BLOCKED_SQL.ACTIVITY_STATE, ' '), BLOCKED_SQL.ACTIVITY_TYPE) AS ACTIVITY_STATE, " +
        "  BLOCKED_CON.SESSION_AUTH_ID             AS BLOCKED_USER, " +
        "  BLOCKED_CON.CLIENT_WRKSTNNAME           AS BLOCKED_HOST, " +
        "  BLOCKER_SQL.STMT_TEXT                   AS BLOCKER_SQL, " +
        "  BLOCKER_CON.SESSION_AUTH_ID             AS BLOCKER_USER, " +
        "  BLOCKER_CON.CLIENT_WRKSTNNAME           AS BLOCKER_HOST, " +
        "  CASE WHEN BLOCKER_SQL.STMT_TEXT IS NULL THEN 1 ELSE 0 END AS BLOCKER_IS_IDLE " +
        "FROM SYSIBMADM.MON_LOCKWAITS LW " +
        "LEFT JOIN SYSIBMADM.MON_CURRENT_SQL BLOCKED_SQL " +
        "  ON BLOCKED_SQL.APPLICATION_HANDLE = LW.REQ_APPLICATION_HANDLE " +
        "LEFT JOIN (SELECT APPLICATION_HANDLE, SESSION_AUTH_ID, CLIENT_WRKSTNNAME, MEMBER " +
        "           FROM TABLE(MON_GET_CONNECTION(CAST(NULL AS BIGINT), -2)) AS C1) BLOCKED_CON " +
        "  ON BLOCKED_CON.APPLICATION_HANDLE = LW.REQ_APPLICATION_HANDLE " +
        "  AND BLOCKED_CON.MEMBER = LW.REQ_MEMBER " +
        "LEFT JOIN SYSIBMADM.MON_CURRENT_SQL BLOCKER_SQL " +
        "  ON BLOCKER_SQL.APPLICATION_HANDLE = LW.HLD_APPLICATION_HANDLE " +
        "LEFT JOIN (SELECT APPLICATION_HANDLE, SESSION_AUTH_ID, CLIENT_WRKSTNNAME, MEMBER " +
        "           FROM TABLE(MON_GET_CONNECTION(CAST(NULL AS BIGINT), -2)) AS C2) BLOCKER_CON " +
        "  ON BLOCKER_CON.APPLICATION_HANDLE = LW.HLD_APPLICATION_HANDLE " +
        "  AND BLOCKER_CON.MEMBER = LW.HLD_MEMBER " +
        "WITH UR";

    // Snapshot path: SYSIBMADM.LOCKWAITS — SYSMON authority sufficient (Db2 9.1+).
    // Provides exact LOCK_WAIT_START_TIME. No SQL text or blocker user/host.
    // exposed for integration test diagnostics — not part of the public API
    public static final String SQL_SNAPSHOT =
        "SELECT " +
        "  LK.AGENT_ID              AS BLOCKED_HANDLE, " +
        "  LK.MEMBER                AS BLOCKED_MEMBER, " +
        "  LK.AGENT_ID_HOLDING_LK   AS BLOCKER_HANDLE, " +
        "  LK.MEMBER                AS BLOCKER_MEMBER, " +
        "  LK.TABSCHEMA             AS LOCK_SCHEMA, " +
        "  LK.TABNAME               AS LOCK_OBJECT, " +
        "  LK.LOCK_OBJECT_TYPE      AS LOCK_OBJECT_TYPE, " +
        "  LK.LOCK_MODE_REQUESTED   AS LOCK_MODE, " +
        "  LK.LOCK_WAIT_START_TIME  AS LOCK_WAIT_START_TIME, " +
        "  LK.AUTHID                AS BLOCKED_USER " +
        "FROM SYSIBMADM.LOCKWAITS LK " +
        "WITH UR";

    // Default path only: look up the idle blocker's last statement from the plan cache.
    private static final String IDLE_BLOCKER_SQL =
        "SELECT P.STMT_TEXT " +
        "FROM TABLE(MON_GET_CONNECTION(CAST(? AS BIGINT), CAST(? AS INTEGER))) AS C " +
        "JOIN TABLE(MON_GET_PKG_CACHE_STMT(NULL, NULL, NULL, CAST(? AS INTEGER))) AS P " +
        "  ON P.EXECUTABLE_ID = C.LAST_EXECUTABLE_ID " +
        "WHERE P.NUM_EXECUTIONS > 0 " +
        "  AND P.STMT_TEXT NOT LIKE '%%MON_GET_PKG_CACHE_STMT%%' " +
        "FETCH FIRST 1 ROW ONLY";

    private final Supplier<Connection> connSupplier;
    private final String dbName;
    private final Logger otelLogger;
    private final int queryTimeoutSeconds;
    private final boolean useSnapshotMonitor;
    private final String sql;

    public BlockingSessionQuery(Supplier<Connection> connSupplier, String dbName,
                                 Logger otelLogger, long intervalSeconds, boolean useSnapshotMonitor) {
        this.connSupplier        = connSupplier;
        this.dbName              = dbName;
        this.otelLogger          = otelLogger;
        this.queryTimeoutSeconds = (int) Math.max(10L, intervalSeconds / 2);
        this.useSnapshotMonitor  = useSnapshotMonitor;
        this.sql                 = useSnapshotMonitor ? SQL_SNAPSHOT : SQL;
        log.info("BlockingSessionQuery: interval={}s, timeout={}s, path={}",
            intervalSeconds, queryTimeoutSeconds, useSnapshotMonitor ? "snapshot" : "default");
    }

    // backwards-compat: defaults to default (MON_LOCKWAITS) path
    public BlockingSessionQuery(Supplier<Connection> connSupplier, String dbName,
                                 Logger otelLogger, long intervalSeconds) {
        this(connSupplier, dbName, otelLogger, intervalSeconds, false);
    }

    public void collect() {
        Instant now = Instant.now();
        int emitCount = 0;

        try {
            Connection conn = connSupplier.get();
            try (Statement st = conn.createStatement()) {
                st.setQueryTimeout(queryTimeoutSeconds);
                try (ResultSet rs = st.executeQuery(sql)) {
                    while (rs.next()) {
                        String blockedHandle  = String.valueOf(rs.getLong("BLOCKED_HANDLE"));
                        long   blockedMember  = rs.getLong("BLOCKED_MEMBER");
                        // blocker handle can be NULL if holder is not tracked — preserve as null
                        Object blockerHandleObj = rs.getObject("BLOCKER_HANDLE");
                        String blockerHandle  = blockerHandleObj != null ? String.valueOf(((Number) blockerHandleObj).longValue()) : null;
                        long   blockerMember  = rs.getLong("BLOCKER_MEMBER");
                        String lockSchema     = rs.getString("LOCK_SCHEMA");
                        String lockObject     = rs.getString("LOCK_OBJECT");
                        String lockObjectType = rs.getString("LOCK_OBJECT_TYPE");
                        String lockMode       = rs.getString("LOCK_MODE");
                        String blockedUser    = rs.getString("BLOCKED_USER");

                        final String waitStartStr;
                        final double waitSec;
                        final String blockedRawSql;
                        final String blockerRawSql;
                        final String activityState;
                        final String blockedHost;
                        final String blockerUser;
                        final String blockerHost;
                        final Boolean blockerIsIdle;

                        if (useSnapshotMonitor) {
                            // Snapshot path: exact start time from LOCKWAITS; blocker state unknown
                            Timestamp ts = rs.getTimestamp("LOCK_WAIT_START_TIME");
                            waitStartStr = ts != null ? ts.toString() : null;
                            waitSec      = ts != null ? (now.toEpochMilli() - ts.getTime()) / 1000.0 : 0;
                            blockedRawSql  = null;
                            blockerRawSql  = null;
                            activityState  = null;
                            blockedHost    = null;
                            blockerUser    = null;
                            blockerHost    = null;
                            blockerIsIdle  = null; // LOCKWAITS has no active-SQL concept
                        } else {
                            // Default path: LOCK_WAIT_ELAPSED_TIME is in seconds on Db2 11.5
                            waitSec      = rs.getLong("WAIT_ELAPSED_SEC");
                            waitStartStr = new Timestamp(now.toEpochMilli() - (long)(waitSec * 1000)).toString();
                            blockedRawSql  = rs.getString("BLOCKED_SQL");
                            blockerRawSql  = rs.getString("BLOCKER_SQL");
                            activityState  = rs.getString("ACTIVITY_STATE");
                            blockedHost    = rs.getString("BLOCKED_HOST");
                            blockerUser    = rs.getString("BLOCKER_USER");
                            blockerHost    = rs.getString("BLOCKER_HOST");
                            blockerIsIdle  = rs.getInt("BLOCKER_IS_IDLE") == 1;
                        }

                        // Obfuscate blocked SQL — drop record if obfuscation fails
                        String blockedObfuscated = null;
                        if (blockedRawSql != null && !blockedRawSql.isBlank()) {
                            blockedObfuscated = SqlObfuscator.obfuscate(blockedRawSql.trim());
                            if (SqlObfuscator.OBFUSCATION_FAILED.equals(blockedObfuscated)) {
                                log.warn("BlockingSessionQuery: obfuscation failed for blocked handle={} — record dropped", blockedHandle);
                                continue;
                            }
                        }

                        // Obfuscate blocker SQL — omit field if obfuscation fails
                        String blockerObfuscated = null;
                        if (blockerRawSql != null && !blockerRawSql.isBlank()) {
                            String ob = SqlObfuscator.obfuscate(blockerRawSql.trim());
                            if (!SqlObfuscator.OBFUSCATION_FAILED.equals(ob)) {
                                blockerObfuscated = ob;
                            }
                        }

                        // Default path only: idle-blocker plan-cache lookup
                        if (Boolean.TRUE.equals(blockerIsIdle) && blockerObfuscated == null) {
                            blockerObfuscated = fetchIdleBlockerSql(conn, ((Number) rs.getObject("BLOCKER_HANDLE")).longValue(), blockerMember);
                        }

                        BlockingSessionEvent.of(dbName, blockedMember, blockedHandle, blockerHandle)
                            .blockedQueryText(blockedObfuscated)
                            .activityState(trim(activityState))
                            .blockedUser(blockedUser)
                            .blockedHost(blockedHost)
                            .blockerUser(blockerUser)
                            .blockerHost(blockerHost)
                            .blockerQueryText(blockerObfuscated)
                            .blockerIsIdle(blockerIsIdle)
                            .lockObjectType(trim(lockObjectType))
                            .lockMode(trim(lockMode))
                            .lockObjectName(trim(lockObject))
                            .lockSchema(trim(lockSchema))
                            .blockingStartTime(waitStartStr)
                            .waitDurationSeconds(waitSec)
                            .emit(otelLogger, now);
                        emitCount++;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("BlockingSessionQuery collection failed: {}", e.getMessage());
            return;
        }
        log.debug("BlockingSessionQuery: emitted {} blocking pairs", emitCount);
    }

    private String fetchIdleBlockerSql(Connection conn, long blockerHandle, long blockerMember) {
        int member = blockerMember >= 0 ? (int) blockerMember : -2;
        try (java.sql.PreparedStatement ps = conn.prepareStatement(IDLE_BLOCKER_SQL)) {
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
            log.debug("BlockingSessionQuery: idle blocker SQL lookup failed for handle={} member={}: {}",
                blockerHandle, blockerMember, e.getMessage());
        }
        return null;
    }

    private static String trim(String s) {
        return s != null ? s.trim() : null;
    }
}
