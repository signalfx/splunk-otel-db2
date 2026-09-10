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
 * Source: {@code SYSIBMADM.MON_LOCKWAITS} (Db2 9.7+) joined with
 * {@code SYSIBMADM.MON_CURRENT_SQL} (both sides) and {@code MON_GET_CONNECTION}
 * (both sides for user/host). All members (-2 on MON_GET_CONNECTION).
 *
 * Idle-blocker handling: the blocker may not appear in MON_CURRENT_SQL if it is
 * idle (holding an open transaction). The query uses a LEFT JOIN so those rows
 * are still emitted with {@code db2.blocking.blocker.is_idle=true} and null SQL.
 * An optional second pass looks up the blocker's last statement from
 * MON_GET_PKG_CACHE_STMT by application_handle and emits it as
 * {@code db2.blocking.blocker.query_text} when available.
 *
 * Lock detail: LOCK_OBJECT_TYPE and LOCK_MODE from MON_LOCKWAITS capture the
 * DB2-specific lock types (ROW, TABLE, PLAN, INTERNAL, TABLESPACE) and modes
 * (S, X, IX, IS, SIX, Z, U, NS, NX) that have no equivalent in SQL Server or
 * Oracle/PostgreSQL OTel implementations.
 *
 * SQL design note: DB2 does not allow TABLE(MON_GET_CONNECTION(...)) to appear
 * more than once in the same FROM clause. Each side (blocked, blocker) uses an
 * independent derived-table subquery that wraps the TVF with a distinct alias
 * (C1 / C2). This avoids both the duplicate-TVF restriction and the CTE +
 * WITH-UR parser ambiguity (a query beginning with "WITH x AS ()" causes the
 * trailing "WITH UR" to be misparsed as a second CTE definition).
 */
public class BlockingSessionQuery {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(BlockingSessionQuery.class);

    // One row per blocked→blocker pair.
    // DB2 does not allow TABLE(MON_GET_CONNECTION(...)) to appear more than once
    // in the same FROM clause. Each side uses a correlated subquery that calls the
    // TVF independently — this avoids both the duplicate-TVF restriction and the
    // CTE+WITH-UR parser ambiguity (a query beginning with "WITH x AS ()" causes
    // "WITH UR" at the end to be misparsed as a second CTE definition).
    // exposed for integration test diagnostics — not part of the public API
    public static final String SQL =
        "SELECT " +
        "  LW.REQ_APPLICATION_HANDLE               AS BLOCKED_HANDLE, " +
        "  LW.HLD_APPLICATION_HANDLE               AS BLOCKER_HANDLE, " +
        "  LW.TABSCHEMA                            AS LOCK_SCHEMA, " +
        "  LW.TABNAME                              AS LOCK_OBJECT, " +
        "  LW.LOCK_OBJECT_TYPE                     AS LOCK_OBJECT_TYPE, " +
        "  LW.LOCK_MODE                            AS LOCK_MODE, " +
        "  LW.LOCK_WAIT_START_TIME                 AS WAIT_START_TIME, " +
        "  BLOCKED_SQL.STMT_TEXT                   AS BLOCKED_SQL, " +
        "  CONCAT(CONCAT(BLOCKED_SQL.ACTIVITY_STATE, ' '), BLOCKED_SQL.ACTIVITY_TYPE) AS ACTIVITY_STATE, " +
        "  BLOCKED_CON.SESSION_AUTH_ID             AS BLOCKED_USER, " +
        "  BLOCKED_CON.CLIENT_WRKSTNNAME           AS BLOCKED_HOST, " +
        "  BLOCKED_CON.MEMBER                      AS MEMBER, " +
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
        "LEFT JOIN SYSIBMADM.MON_CURRENT_SQL BLOCKER_SQL " +
        "  ON BLOCKER_SQL.APPLICATION_HANDLE = LW.HLD_APPLICATION_HANDLE " +
        "LEFT JOIN (SELECT APPLICATION_HANDLE, SESSION_AUTH_ID, CLIENT_WRKSTNNAME, MEMBER " +
        "           FROM TABLE(MON_GET_CONNECTION(CAST(NULL AS BIGINT), -2)) AS C2) BLOCKER_CON " +
        "  ON BLOCKER_CON.APPLICATION_HANDLE = LW.HLD_APPLICATION_HANDLE " +
        "WITH UR";

    // Idle blocker lookup: fetch the last statement executed by the blocker from the
    // plan cache. Used when BLOCKER_IS_IDLE=1 and the blocker has a known handle.
    private static final String IDLE_BLOCKER_SQL =
        "SELECT STMT_TEXT " +
        "FROM TABLE(MON_GET_PKG_CACHE_STMT(NULL, NULL, NULL, -2)) AS T " +
        "WHERE APPLICATION_HANDLE = ? " +
        "  AND NUM_EXECUTIONS > 0 " +
        "  AND STMT_TEXT NOT LIKE '%%MON_GET_PKG_CACHE_STMT%%' " +
        "ORDER BY LAST_METRICS_UPDATE DESC " +
        "FETCH FIRST 1 ROW ONLY";

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
            try (Statement st = conn.createStatement()) {
                st.setQueryTimeout(queryTimeoutSeconds);
                try (ResultSet rs = st.executeQuery(SQL)) {
                    while (rs.next()) {
                        String blockedHandle  = String.valueOf(rs.getLong("BLOCKED_HANDLE"));
                        String blockerHandle  = String.valueOf(rs.getLong("BLOCKER_HANDLE"));
                        String blockedRawSql  = rs.getString("BLOCKED_SQL");
                        String blockerRawSql  = rs.getString("BLOCKER_SQL");
                        String activityState  = rs.getString("ACTIVITY_STATE");
                        String blockedUser    = rs.getString("BLOCKED_USER");
                        String blockedHost    = rs.getString("BLOCKED_HOST");
                        String blockerUser    = rs.getString("BLOCKER_USER");
                        String blockerHost    = rs.getString("BLOCKER_HOST");
                        String lockSchema     = rs.getString("LOCK_SCHEMA");
                        String lockObject     = rs.getString("LOCK_OBJECT");
                        String lockObjectType = rs.getString("LOCK_OBJECT_TYPE");
                        String lockMode       = rs.getString("LOCK_MODE");
                        Timestamp waitStart   = rs.getTimestamp("WAIT_START_TIME");
                        long member           = rs.getLong("MEMBER");
                        boolean blockerIsIdle = rs.getInt("BLOCKER_IS_IDLE") == 1;

                        // Compute wait duration in ms from LOCK_WAIT_START_TIME to now
                        long waitMs = waitStart != null
                            ? Math.max(0, now.toEpochMilli() - waitStart.getTime()) : -1;

                        String waitStartStr = waitStart != null ? waitStart.toString() : null;

                        // Obfuscate blocked SQL — drop record if obfuscation fails
                        String blockedObfuscated = null;
                        if (blockedRawSql != null && !blockedRawSql.isBlank()) {
                            blockedObfuscated = SqlObfuscator.obfuscate(blockedRawSql.trim());
                            if (SqlObfuscator.OBFUSCATION_FAILED.equals(blockedObfuscated)) {
                                log.warn("BlockingSessionQuery: obfuscation failed for blocked handle={} — record dropped", blockedHandle);
                                continue;
                            }
                        }

                        // Obfuscate blocker SQL — omit field if obfuscation fails (don't drop the record)
                        String blockerObfuscated = null;
                        if (blockerRawSql != null && !blockerRawSql.isBlank()) {
                            String ob = SqlObfuscator.obfuscate(blockerRawSql.trim());
                            if (!SqlObfuscator.OBFUSCATION_FAILED.equals(ob)) {
                                blockerObfuscated = ob;
                            }
                        }

                        // For idle blockers with no SQL in MON_CURRENT_SQL, try plan cache lookup
                        if (blockerIsIdle && blockerObfuscated == null) {
                            blockerObfuscated = fetchIdleBlockerSql(conn, rs.getLong("BLOCKER_HANDLE"));
                        }

                        BlockingSessionEvent.of(dbName, member, blockedHandle, blockerHandle)
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
                            .waitDurationMillis(waitMs)
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

    private String fetchIdleBlockerSql(Connection conn, long blockerHandle) {
        try (java.sql.PreparedStatement ps = conn.prepareStatement(IDLE_BLOCKER_SQL)) {
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
}
