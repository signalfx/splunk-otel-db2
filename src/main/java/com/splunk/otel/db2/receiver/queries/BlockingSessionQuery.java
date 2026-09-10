package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.util.SqlObfuscator;
import io.opentelemetry.api.logs.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Detects blocking sessions in Db2 and emits each blocked→blocker pair as an OTel
 * log record ({@code db.server.blocking_session}, severity WARN).
 *
 * Source: {@code SYSIBMADM.MON_LOCKWAITS} (Db2 9.7+) joined with
 * {@code SYSIBMADM.MON_CURRENT_SQL} (blocked side) and {@code MON_GET_CONNECTION}
 * (both sides for user/host). All members (-2 on MON_GET_CONNECTION).
 *
 * Implementation note: DB2 does not allow the same table function (e.g.
 * TABLE(MON_GET_CONNECTION(...))) to appear more than once in a single FROM
 * clause, even with different aliases. MON_CURRENT_SQL is an administrative
 * view that may carry the same restriction. To avoid SqlSyntaxErrorException,
 * the implementation runs three simple queries and joins in Java:
 *   1. SQL_LOCKWAITS — one row per blocked→blocker pair (handles + lock detail)
 *   2. SQL_CONN_INFO — SESSION_AUTH_ID / CLIENT_WRKSTNNAME / MEMBER per handle
 *   3. SQL_ACTIVE_SQL — STMT_TEXT / ACTIVITY_STATE / TYPE per handle
 *
 * Idle-blocker handling: the blocker may not appear in MON_CURRENT_SQL if it is
 * idle (holding an open transaction). Rows without a matching ACTIVE_SQL row
 * are emitted with {@code db2.blocking.blocker.is_idle=true} and null SQL.
 * An optional second pass looks up the blocker's last statement from
 * MON_GET_PKG_CACHE_STMT by application_handle and emits it as
 * {@code db2.blocking.blocker.query_text} when available.
 *
 * Lock detail: LOCK_OBJECT_TYPE and LOCK_MODE from MON_LOCKWAITS capture the
 * DB2-specific lock types (ROW, TABLE, PLAN, INTERNAL, TABLESPACE) and modes
 * (S, X, IX, IS, SIX, Z, U, NS, NX) that have no equivalent in SQL Server or
 * Oracle/PostgreSQL OTel implementations.
 */
public class BlockingSessionQuery {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(BlockingSessionQuery.class);

    // Step 1: lock pairs from MON_LOCKWAITS — no joins, no WITH UR.
    // exposed for integration test diagnostics — not part of the public API.
    public static final String SQL_LOCKWAITS =
        "SELECT " +
        "  REQ_APPLICATION_HANDLE AS BLOCKED_HANDLE, " +
        "  HLD_APPLICATION_HANDLE AS BLOCKER_HANDLE, " +
        "  TABSCHEMA               AS LOCK_SCHEMA, " +
        "  TABNAME                 AS LOCK_OBJECT, " +
        "  LOCK_OBJECT_TYPE, " +
        "  LOCK_MODE, " +
        "  LOCK_WAIT_START_TIME    AS WAIT_START_TIME " +
        "FROM SYSIBMADM.MON_LOCKWAITS";

    // Step 2: connection info for all sessions (user, host, member).
    private static final String SQL_CONN_INFO =
        "SELECT APPLICATION_HANDLE, SESSION_AUTH_ID, CLIENT_WRKSTNNAME, MEMBER " +
        "FROM TABLE(MON_GET_CONNECTION(CAST(NULL AS BIGINT), -2)) AS T";

    // Step 3: active SQL for all sessions (includes LOCKWAIT, EXECUTING, etc.).
    // No IDLE filter — blocked sessions appear with ACTIVITY_STATE = 'LOCKWAIT'.
    private static final String SQL_ACTIVE_SQL =
        "SELECT APPLICATION_HANDLE, STMT_TEXT, ACTIVITY_STATE, ACTIVITY_TYPE " +
        "FROM SYSIBMADM.MON_CURRENT_SQL";

    // Idle blocker lookup: fetch the last statement executed by the blocker from the
    // plan cache. Used when the blocker has no row in MON_CURRENT_SQL.
    private static final String IDLE_BLOCKER_SQL =
        "SELECT STMT_TEXT " +
        "FROM TABLE(MON_GET_PKG_CACHE_STMT(NULL, NULL, NULL, -2)) AS T " +
        "WHERE APPLICATION_HANDLE = ? " +
        "  AND NUM_EXECUTIONS > 0 " +
        "  AND STMT_TEXT NOT LIKE '%%MON_GET_PKG_CACHE_STMT%%' " +
        "ORDER BY LAST_METRICS_UPDATE DESC " +
        "FETCH FIRST 1 ROW ONLY";

    // Kept for backward-compat with integration tests that reference BlockingSessionQuery.SQL
    public static final String SQL = SQL_LOCKWAITS;

    private final Supplier<Connection> connSupplier;
    private final String dbName;
    private final Logger otelLogger;
    private final int queryTimeoutSeconds;

    public BlockingSessionQuery(Supplier<Connection> connSupplier, String dbName,
                                 Logger otelLogger, long intervalSeconds) {
        this.connSupplier        = connSupplier;
        this.dbName              = dbName;
        this.otelLogger          = otelLogger;
        this.queryTimeoutSeconds = (int) Math.max(10L, intervalSeconds / 2);
        log.info("BlockingSessionQuery: interval={}s, timeout={}s", intervalSeconds, queryTimeoutSeconds);
    }

    public void collect() {
        Instant now = Instant.now();
        int emitCount = 0;

        try {
            Connection conn = connSupplier.get();

            // --- Step 1: read lock pairs ---
            List<LockPair> pairs = new ArrayList<>();
            try (Statement st = conn.createStatement()) {
                st.setQueryTimeout(queryTimeoutSeconds);
                try (ResultSet rs = st.executeQuery(SQL_LOCKWAITS)) {
                    while (rs.next()) {
                        LockPair p = new LockPair();
                        p.blockedHandle  = rs.getLong("BLOCKED_HANDLE");
                        p.blockerHandle  = rs.getLong("BLOCKER_HANDLE");
                        p.lockSchema     = rs.getString("LOCK_SCHEMA");
                        p.lockObject     = rs.getString("LOCK_OBJECT");
                        p.lockObjectType = rs.getString("LOCK_OBJECT_TYPE");
                        p.lockMode       = rs.getString("LOCK_MODE");
                        p.waitStart      = rs.getTimestamp("WAIT_START_TIME");
                        pairs.add(p);
                    }
                }
            }
            if (pairs.isEmpty()) {
                log.debug("BlockingSessionQuery: no lock waits, skipping");
                return;
            }

            // --- Step 2: connection info (user, host, member) keyed by handle ---
            Map<Long, ConnInfo> conns = new HashMap<>();
            try (Statement st = conn.createStatement()) {
                st.setQueryTimeout(queryTimeoutSeconds);
                try (ResultSet rs = st.executeQuery(SQL_CONN_INFO)) {
                    while (rs.next()) {
                        long handle = rs.getLong("APPLICATION_HANDLE");
                        ConnInfo c = new ConnInfo();
                        c.user   = rs.getString("SESSION_AUTH_ID");
                        c.host   = rs.getString("CLIENT_WRKSTNNAME");
                        c.member = rs.getLong("MEMBER");
                        conns.put(handle, c);
                    }
                }
            }

            // --- Step 3: active SQL (stmt text, state) keyed by handle ---
            Map<Long, SqlInfo> activeSql = new HashMap<>();
            try (Statement st = conn.createStatement()) {
                st.setQueryTimeout(queryTimeoutSeconds);
                try (ResultSet rs = st.executeQuery(SQL_ACTIVE_SQL)) {
                    while (rs.next()) {
                        long handle = rs.getLong("APPLICATION_HANDLE");
                        SqlInfo s = new SqlInfo();
                        s.stmtText     = rs.getString("STMT_TEXT");
                        s.activityState = rs.getString("ACTIVITY_STATE");
                        s.activityType  = rs.getString("ACTIVITY_TYPE");
                        activeSql.put(handle, s);
                    }
                }
            }

            // --- Step 4: emit one event per lock pair ---
            for (LockPair p : pairs) {
                ConnInfo blockedCon = conns.get(p.blockedHandle);
                ConnInfo blockerCon = conns.get(p.blockerHandle);
                SqlInfo  blockedSql = activeSql.get(p.blockedHandle);
                SqlInfo  blockerSql = activeSql.get(p.blockerHandle);

                boolean blockerIsIdle = (blockerSql == null || blockerSql.stmtText == null);
                long member = blockedCon != null ? blockedCon.member : 0L;

                String activityState = null;
                if (blockedSql != null && blockedSql.activityState != null) {
                    activityState = blockedSql.activityState.trim()
                        + (blockedSql.activityType != null ? " " + blockedSql.activityType.trim() : "");
                }

                long waitMs = p.waitStart != null
                    ? Math.max(0, now.toEpochMilli() - p.waitStart.getTime()) : -1;
                String waitStartStr = p.waitStart != null ? p.waitStart.toString() : null;

                // Obfuscate blocked SQL — drop record if obfuscation fails
                String blockedObfuscated = null;
                if (blockedSql != null && blockedSql.stmtText != null && !blockedSql.stmtText.isBlank()) {
                    blockedObfuscated = SqlObfuscator.obfuscate(blockedSql.stmtText.trim());
                    if (SqlObfuscator.OBFUSCATION_FAILED.equals(blockedObfuscated)) {
                        log.warn("BlockingSessionQuery: obfuscation failed for blocked handle={} — record dropped", p.blockedHandle);
                        continue;
                    }
                }

                // Obfuscate blocker SQL — omit field if obfuscation fails (don't drop the record)
                String blockerObfuscated = null;
                if (!blockerIsIdle && blockerSql.stmtText != null && !blockerSql.stmtText.isBlank()) {
                    String ob = SqlObfuscator.obfuscate(blockerSql.stmtText.trim());
                    if (!SqlObfuscator.OBFUSCATION_FAILED.equals(ob)) {
                        blockerObfuscated = ob;
                    }
                }

                // For idle blockers with no SQL in MON_CURRENT_SQL, try plan cache lookup
                if (blockerIsIdle) {
                    blockerObfuscated = fetchIdleBlockerSql(conn, p.blockerHandle);
                }

                BlockingSessionEvent.of(dbName, member,
                        String.valueOf(p.blockedHandle), String.valueOf(p.blockerHandle))
                    .blockedQueryText(blockedObfuscated)
                    .activityState(activityState)
                    .blockedUser(blockedCon != null ? blockedCon.user : null)
                    .blockedHost(blockedCon != null ? blockedCon.host : null)
                    .blockerUser(blockerCon != null ? blockerCon.user : null)
                    .blockerHost(blockerCon != null ? blockerCon.host : null)
                    .blockerQueryText(blockerObfuscated)
                    .blockerIsIdle(blockerIsIdle)
                    .lockObjectType(trim(p.lockObjectType))
                    .lockMode(trim(p.lockMode))
                    .lockObjectName(trim(p.lockObject))
                    .lockSchema(trim(p.lockSchema))
                    .blockingStartTime(waitStartStr)
                    .waitDurationMillis(waitMs)
                    .emit(otelLogger, now);
                emitCount++;
            }
        } catch (Exception e) {
            log.warn("BlockingSessionQuery collection failed: {}", e.getMessage());
            return;
        }
        log.debug("BlockingSessionQuery: emitted {} blocking pairs", emitCount);
    }

    private String fetchIdleBlockerSql(Connection conn, long blockerHandle) {
        try (PreparedStatement ps = conn.prepareStatement(IDLE_BLOCKER_SQL)) {
            ps.setQueryTimeout(queryTimeoutSeconds);
            ps.setLong(1, blockerHandle);
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
            log.debug("BlockingSessionQuery: idle blocker SQL lookup failed for handle={}: {}", blockerHandle, e.getMessage());
        }
        return null;
    }

    private static String trim(String s) {
        return s != null ? s.trim() : null;
    }

    private static final class LockPair {
        long blockedHandle;
        long blockerHandle;
        String lockSchema;
        String lockObject;
        String lockObjectType;
        String lockMode;
        Timestamp waitStart;
    }

    private static final class ConnInfo {
        String user;
        String host;
        long   member;
    }

    private static final class SqlInfo {
        String stmtText;
        String activityState;
        String activityType;
    }
}
