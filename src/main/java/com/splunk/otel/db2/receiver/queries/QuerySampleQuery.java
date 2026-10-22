package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.util.SqlObfuscator;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
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
        "       C.CLIENT_ACCTNG, " +
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
        "       CAST(NULL AS CHAR(16) FOR BIT DATA) AS EXEC_ID_BYTES " +
        "FROM SYSIBMADM.MON_CURRENT_SQL S " +
        "LEFT JOIN (SELECT APPLICATION_HANDLE, CLIENT_USERID, CLIENT_WRKSTNNAME, MEMBER, " +
        "                  TOTAL_CPU_TIME, CLIENT_APPLNAME, CLIENT_ACCTNG " +
        "           FROM TABLE(MON_GET_CONNECTION(NULL, -2))) AS C " +
        "  ON C.APPLICATION_HANDLE = S.APPLICATION_HANDLE " +
        " AND C.MEMBER = S.COORD_MEMBER " +
        // Guarantees at most one row per sampled session, so a blocked session can never be emitted
        // twice at the same timestamp — which would inflate active-session counts and, because
        // FETCH FIRST applies after the join, let duplicates consume the row budget real sessions
        // need.
        //
        // Measured on 11.5.9.0: MON_LOCKWAITS returns one row per waiting request naming a single
        // holder, even with three concurrent S-lock holders, so it does not multiply there. The
        // dedup is therefore defensive, not a fix for observed duplication — it holds the invariant
        // across the 9.7, 10.5+ and snapshot paths and across pureScale/DPF, rather than relying on
        // MON_LOCKWAITS semantics being identical on all of them.
        //
        // Matched per application, not per member. MON_CURRENT_SQL aggregates activities across all
        // members and reports only COORD_MEMBER, so requiring REQ_MEMBER = S.COORD_MEMBER dropped
        // blocking data for any pureScale/DPF activity whose waiting agent sits on a non-coordinator
        // member — the sample was still emitted, just silently without its lock wait. Collapsing at
        // application scope matches the scope of the row this decorates. Longest wait wins, with
        // REQ_MEMBER then HLD_APPLICATION_HANDLE as tie-breaks so the choice is stable across
        // collections instead of flapping between equally-blocked members or holders.
        "LEFT JOIN (SELECT REQ_APPLICATION_HANDLE, REQ_MEMBER, " +
        "                  HLD_APPLICATION_HANDLE, HLD_MEMBER, LOCK_WAIT_ELAPSED_TIME, " +
        "                  TABSCHEMA, TABNAME, LOCK_OBJECT_TYPE, LOCK_MODE_REQUESTED, " +
        "                  ROW_NUMBER() OVER (PARTITION BY REQ_APPLICATION_HANDLE " +
        "                                     ORDER BY LOCK_WAIT_ELAPSED_TIME DESC NULLS LAST, " +
        "                                              REQ_MEMBER, " +
        "                                              HLD_APPLICATION_HANDLE) AS RN " +
        "           FROM SYSIBMADM.MON_LOCKWAITS) AS LW " +
        "  ON LW.REQ_APPLICATION_HANDLE = S.APPLICATION_HANDLE " +
        " AND LW.RN = 1 " +
        // Joined on APPLICATION_HANDLE alone, deliberately. The handle is system-wide unique in
        // Db2, while LW.HLD_MEMBER is the member holding the lock and MON_CURRENT_SQL.COORD_MEMBER
        // is the activity's coordinator — in a partitioned or pureScale instance those differ, so a
        // member predicate here would silently drop valid distributed blockers.
        // Deduplicated by handle because a connection can have more than one activity in
        // MON_CURRENT_SQL, which would otherwise multiply the outer row just as MON_LOCKWAITS did.
        "LEFT JOIN (SELECT APPLICATION_HANDLE, STMT_TEXT, " +
        "                  ROW_NUMBER() OVER (PARTITION BY APPLICATION_HANDLE " +
        "                                     ORDER BY ELAPSED_TIME_SEC DESC NULLS LAST) AS RN " +
        "           FROM SYSIBMADM.MON_CURRENT_SQL) AS BLOCKER_SQL " +
        "  ON BLOCKER_SQL.APPLICATION_HANDLE = LW.HLD_APPLICATION_HANDLE " +
        " AND BLOCKER_SQL.RN = 1 " +
        "LEFT JOIN (SELECT APPLICATION_HANDLE, SESSION_AUTH_ID, CLIENT_WRKSTNNAME, MEMBER " +
        "           FROM TABLE(MON_GET_CONNECTION(CAST(NULL AS BIGINT), -2))) AS BLOCKER_CON " +
        "  ON BLOCKER_CON.APPLICATION_HANDLE = LW.HLD_APPLICATION_HANDLE " +
        " AND BLOCKER_CON.MEMBER = LW.HLD_MEMBER " +
        "WHERE S.STMT_TEXT IS NOT NULL " +
        "  AND COALESCE(C.CLIENT_APPLNAME, '') <> 'splunk-otel-db2' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_CURRENT_SQL%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_LOCKWAITS%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_GET_CONNECTION%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_GET_PKG_CACHE_STMT%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%SNAP_GET_APPL_INFO%%%%' " +
        // Priority tiers before elapsed time, because this ordering feeds FETCH FIRST and the row
        // budget is shared. An IDLE activity waits on the client, while ELAPSED_TIME_SEC counts
        // from when the activity began — so a long-open idle cursor outranks a newer executing or
        // lock-waiting statement on elapsed time alone and would consume the budget ahead of it.
        // Blocked rows first (they also drive blocking detection), then other non-IDLE activity,
        // then IDLE, which can therefore only ever fill leftover capacity.
        // S.ACTIVITY_STATE is the base column ('IDLE'), not the SELECT alias, which is the
        // concatenation of state and type ('IDLE READ_DML').
        // NULLS LAST here too: a row with an unknown elapsed time would otherwise be sampled ahead
        // of genuinely long-running statements and consume the row budget.
        "ORDER BY CASE WHEN LW.REQ_APPLICATION_HANDLE IS NOT NULL THEN 0 " +
        "              WHEN S.ACTIVITY_STATE <> 'IDLE' THEN 1 " +
        "              ELSE 2 END, " +
        "         S.ELAPSED_TIME_SEC DESC NULLS LAST " +
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
        "       C.CLIENT_ACCTNG, " +
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
        "       A.EXECUTABLE_ID AS EXEC_ID_BYTES " +
        "FROM SYSIBMADM.MON_CURRENT_SQL S " +
        "LEFT JOIN (SELECT APPLICATION_HANDLE, CLIENT_USERID, CLIENT_WRKSTNNAME, MEMBER, " +
        "                  TOTAL_CPU_TIME, CLIENT_APPLNAME, CLIENT_ACCTNG " +
        "           FROM TABLE(MON_GET_CONNECTION(NULL, -2))) AS C " +
        "  ON C.APPLICATION_HANDLE = S.APPLICATION_HANDLE " +
        " AND C.MEMBER = S.COORD_MEMBER " +
        // MON_GET_ACTIVITY(NULL, -2) returns a row per member, so a distributed activity yields
        // several rows for one (handle, uow, activity) and would multiply the outer row — the same
        // duplication the lock-wait join had, with the same cost: inflated session counts and
        // duplicates eating the FETCH FIRST budget.
        //
        // Aggregated rather than picking one member's row, because db2.wait_time is documented as
        // cumulative lock wait time: a 4s wait on one member and 6s on another is 10s of waiting,
        // not 6s. Selecting the largest member would have under-reported every distributed wait.
        // SUM(LOCK_WAIT_TIME) with MIN(EXECUTABLE_ID) is the collapse IBM's db2mon.sql uses; the
        // members' rows describe the same section, so MIN is just a deterministic pick.
        "LEFT JOIN (SELECT APPLICATION_HANDLE, UOW_ID, ACTIVITY_ID, " +
        "                  SUM(LOCK_WAIT_TIME) AS LOCK_WAIT_TIME, " +
        "                  MIN(EXECUTABLE_ID) AS EXECUTABLE_ID " +
        "           FROM TABLE(MON_GET_ACTIVITY(NULL, -2)) " +
        "           GROUP BY APPLICATION_HANDLE, UOW_ID, ACTIVITY_ID) AS A " +
        "  ON A.APPLICATION_HANDLE = S.APPLICATION_HANDLE " +
        " AND A.UOW_ID = S.UOW_ID " +
        " AND A.ACTIVITY_ID = S.ACTIVITY_ID " +
        // Guarantees at most one row per sampled session, so a blocked session can never be emitted
        // twice at the same timestamp — which would inflate active-session counts and, because
        // FETCH FIRST applies after the join, let duplicates consume the row budget real sessions
        // need.
        //
        // Measured on 11.5.9.0: MON_LOCKWAITS returns one row per waiting request naming a single
        // holder, even with three concurrent S-lock holders, so it does not multiply there. The
        // dedup is therefore defensive, not a fix for observed duplication — it holds the invariant
        // across the 9.7, 10.5+ and snapshot paths and across pureScale/DPF, rather than relying on
        // MON_LOCKWAITS semantics being identical on all of them.
        //
        // Matched per application, not per member. MON_CURRENT_SQL aggregates activities across all
        // members and reports only COORD_MEMBER, so requiring REQ_MEMBER = S.COORD_MEMBER dropped
        // blocking data for any pureScale/DPF activity whose waiting agent sits on a non-coordinator
        // member — the sample was still emitted, just silently without its lock wait. Collapsing at
        // application scope matches the scope of the row this decorates. Longest wait wins, with
        // REQ_MEMBER then HLD_APPLICATION_HANDLE as tie-breaks so the choice is stable across
        // collections instead of flapping between equally-blocked members or holders.
        "LEFT JOIN (SELECT REQ_APPLICATION_HANDLE, REQ_MEMBER, " +
        "                  HLD_APPLICATION_HANDLE, HLD_MEMBER, LOCK_WAIT_ELAPSED_TIME, " +
        "                  TABSCHEMA, TABNAME, LOCK_OBJECT_TYPE, LOCK_MODE_REQUESTED, " +
        "                  ROW_NUMBER() OVER (PARTITION BY REQ_APPLICATION_HANDLE " +
        "                                     ORDER BY LOCK_WAIT_ELAPSED_TIME DESC NULLS LAST, " +
        "                                              REQ_MEMBER, " +
        "                                              HLD_APPLICATION_HANDLE) AS RN " +
        "           FROM SYSIBMADM.MON_LOCKWAITS) AS LW " +
        "  ON LW.REQ_APPLICATION_HANDLE = S.APPLICATION_HANDLE " +
        " AND LW.RN = 1 " +
        // Joined on APPLICATION_HANDLE alone, deliberately. The handle is system-wide unique in
        // Db2, while LW.HLD_MEMBER is the member holding the lock and MON_CURRENT_SQL.COORD_MEMBER
        // is the activity's coordinator — in a partitioned or pureScale instance those differ, so a
        // member predicate here would silently drop valid distributed blockers.
        // Deduplicated by handle because a connection can have more than one activity in
        // MON_CURRENT_SQL, which would otherwise multiply the outer row just as MON_LOCKWAITS did.
        "LEFT JOIN (SELECT APPLICATION_HANDLE, STMT_TEXT, " +
        "                  ROW_NUMBER() OVER (PARTITION BY APPLICATION_HANDLE " +
        "                                     ORDER BY ELAPSED_TIME_SEC DESC NULLS LAST) AS RN " +
        "           FROM SYSIBMADM.MON_CURRENT_SQL) AS BLOCKER_SQL " +
        "  ON BLOCKER_SQL.APPLICATION_HANDLE = LW.HLD_APPLICATION_HANDLE " +
        " AND BLOCKER_SQL.RN = 1 " +
        "LEFT JOIN (SELECT APPLICATION_HANDLE, SESSION_AUTH_ID, CLIENT_WRKSTNNAME, MEMBER " +
        "           FROM TABLE(MON_GET_CONNECTION(CAST(NULL AS BIGINT), -2))) AS BLOCKER_CON " +
        "  ON BLOCKER_CON.APPLICATION_HANDLE = LW.HLD_APPLICATION_HANDLE " +
        " AND BLOCKER_CON.MEMBER = LW.HLD_MEMBER " +
        "WHERE S.STMT_TEXT IS NOT NULL " +
        "  AND COALESCE(C.CLIENT_APPLNAME, '') <> 'splunk-otel-db2' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_CURRENT_SQL%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_LOCKWAITS%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_GET_CONNECTION%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_GET_PKG_CACHE_STMT%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%MON_GET_ACTIVITY%%%%' " +
        "  AND S.STMT_TEXT NOT LIKE '%%%%SNAP_GET_APPL_INFO%%%%' " +
        // Priority tiers before elapsed time, because this ordering feeds FETCH FIRST and the row
        // budget is shared. An IDLE activity waits on the client, while ELAPSED_TIME_SEC counts
        // from when the activity began — so a long-open idle cursor outranks a newer executing or
        // lock-waiting statement on elapsed time alone and would consume the budget ahead of it.
        // Blocked rows first (they also drive blocking detection), then other non-IDLE activity,
        // then IDLE, which can therefore only ever fill leftover capacity.
        // S.ACTIVITY_STATE is the base column ('IDLE'), not the SELECT alias, which is the
        // concatenation of state and type ('IDLE READ_DML').
        // NULLS LAST here too: a row with an unknown elapsed time would otherwise be sampled ahead
        // of genuinely long-running statements and consume the row budget.
        "ORDER BY CASE WHEN LW.REQ_APPLICATION_HANDLE IS NOT NULL THEN 0 " +
        "              WHEN S.ACTIVITY_STATE <> 'IDLE' THEN 1 " +
        "              ELSE 2 END, " +
        "         S.ELAPSED_TIME_SEC DESC NULLS LAST " +
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
        // The snapshot monitor names the accounting string TPMON_ACC_STR; SNAP_GET_APPL_INFO has
        // no CLIENT_ACCTNG or CLIENT_ACCTNG_ID column (referencing either fails with SQL0206N).
        "       SA.TPMON_ACC_STR AS CLIENT_ACCTNG, " +
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
        "ORDER BY TIMESTAMPDIFF(2, CHAR(CURRENT TIMESTAMP - SS.STMT_START)) DESC NULLS LAST " +
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

    /**
     * Whether anything will actually populate {@code db2.query_plan}. When plan collection is off —
     * which is the default, since {@code max_explain_each_interval} defaults to 0 — emitting
     * {@code db2.query_plan.hash} advertises a plan that can never be resolved, so the hash is
     * suppressed instead.
     */
    private final boolean planCollectionEnabled;

    /**
     * Cap on idle-blocker plan-cache lookups per collection, mirroring the explain path's budget.
     * Each lookup scans MON_GET_PKG_CACHE_STMT on the sampling connection while the outer result
     * set is still open, so an uncapped run could issue one per sampled row every interval.
     */
    static final int MAX_IDLE_BLOCKER_LOOKUPS_PER_INTERVAL = 10;

    /**
     * Idle blockers resolved in this collection, keyed by {@code member:handle}. A single holder
     * commonly blocks many requestors, which would otherwise repeat the same lookup per blocked
     * row. Cleared each collection so a blocker's statement is never reported from stale state.
     */
    private final java.util.Map<String, String> idleBlockerSqlCache = new java.util.HashMap<>();
    private int idleBlockerLookupsThisInterval;

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
        this(connSupplier, dbName, otelLogger, maxQuerySampleCount, useSnapshotMonitor, db2Version,
            queryTimeoutSeconds, true);
    }

    public QuerySampleQuery(Supplier<Connection> connSupplier, String dbName,
                            Logger otelLogger, int maxQuerySampleCount,
                            boolean useSnapshotMonitor, int db2Version, int queryTimeoutSeconds,
                            boolean planCollectionEnabled) {
        this.connSupplier         = connSupplier;
        this.dbName               = dbName;
        this.otelLogger           = otelLogger;
        this.maxQuerySampleCount  = maxQuerySampleCount;
        this.useSnapshotMonitor   = useSnapshotMonitor;
        this.db2Version           = db2Version;
        this.queryTimeoutSeconds  = queryTimeoutSeconds;
        this.planCollectionEnabled = planCollectionEnabled;
        log.info("QuerySampleQuery: using {} path{}, timeout={}s{}",
            useSnapshotMonitor ? "SNAP_GET_*" : "MON_CURRENT_SQL",
            (!useSnapshotMonitor && db2Version >= DB2_VERSION_10_5) ? " with MON_GET_ACTIVITY (10.5+)" : "",
            queryTimeoutSeconds,
            useSnapshotMonitor ? " — blocking detection disabled on snapshot monitor path" : "");
    }

    public boolean collect() {
        // Per-interval state: the cache must not carry a blocker's statement across collections,
        // and the budget resets so a busy interval does not starve the next one.
        idleBlockerSqlCache.clear();
        idleBlockerLookupsThisInterval = 0;

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
                        if (!useSnapshotMonitor && planCollectionEnabled) {
                            byte[] execIdBytes = rs.getBytes("EXEC_ID_BYTES");
                            if (execIdBytes != null) execIdHex = bytesToHex(execIdBytes);
                        }

                        String sessionStatus = deriveSessionStatus(state);

                        QuerySampleEvent ev = QuerySampleEvent.of(dbName, obfuscated, elapsedSec, member)
                            .spanContext(linksToTrace(sessionStatus)
                                ? parseTraceparent(rs.getString("CLIENT_ACCTNG"))
                                : null)
                            .appHandle(appHandle)
                            .activityState(state)
                            .sessionStatus(sessionStatus)
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
                            // getLong turns SQL NULL into 0, and 0 clears waitDurationSeconds'
                            // >= 0 guard — which would set the attribute, escalate the record to
                            // WARN and report a zero-second lock wait that never happened.
                            // wasNull must be read immediately after the getter.
                            long waitSecRaw = rs.getLong("BLOCKING_WAIT_SEC");
                            boolean haveWaitSec = !rs.wasNull();

                            String blockingStartTime = toRfc3339(rs.getString("BLOCKING_START_TIME"));

                            // IS_BLOCKED == 1 means a MON_LOCKWAITS row matched, so this session is
                            // blocked whether or not the elapsed time came back NULL. Severity must
                            // follow that fact, not the availability of the duration.
                            ev.blocked(true)
                              .lockObjectType(trim(rs.getString("LOCK_OBJECT_TYPE")))
                              .lockMode(trim(rs.getString("LOCK_MODE")))
                              .lockObjectName(trim(rs.getString("LOCK_OBJECT")))
                              .lockSchema(trim(rs.getString("LOCK_SCHEMA")))
                              .blockingStartTime(blockingStartTime);
                            if (haveWaitSec) {
                                ev.waitDurationSeconds(waitSecRaw);
                            }

                            // Blocker identity is NULL for global lock waits or unknown holders.
                            Object blockerHandleObj = rs.getObject("BLOCKER_HANDLE");
                            if (blockerHandleObj != null) {
                                long    blockerHandleLong = ((Number) blockerHandleObj).longValue();
                                // NULL would arrive as 0 — a real member number — instead of the
                                // -2 "all members" sentinel the lookup expects.
                                long    blockerMemberRaw  = rs.getLong("BLOCKER_MEMBER");
                                long    blockerMember     = rs.wasNull() ? -2L : blockerMemberRaw;
                                String  blockerRawSql = rs.getString("BLOCKER_SQL");
                                // BLOCKER_IS_IDLE=1 when the BLOCKER_SQL join returned no active
                                // statement, which is ambiguous: a genuinely idle holder, or one
                                // whose statement is not in MON_CURRENT_SQL at this instant. Use
                                // the pkg cache lookup to disambiguate: found = truly idle; not
                                // found = unknown, so omit is_idle rather than report false data.
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
                                    blockerObf = fetchIdleBlockerSqlBudgeted(
                                        conn, blockerHandleLong, blockerMember);
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

    /**
     * Resolves an idle blocker's last statement from the package cache, memoized per collection and
     * capped at {@link #MAX_IDLE_BLOCKER_LOOKUPS_PER_INTERVAL}. Returns null when the budget is
     * exhausted, which the caller treats the same as a cache miss: is_idle is omitted rather than
     * guessed.
     */
    private String fetchIdleBlockerSqlBudgeted(Connection conn, long blockerHandle, long blockerMember) {
        String key = blockerMember + ":" + blockerHandle;
        if (idleBlockerSqlCache.containsKey(key)) {
            return idleBlockerSqlCache.get(key);
        }
        if (idleBlockerLookupsThisInterval >= MAX_IDLE_BLOCKER_LOOKUPS_PER_INTERVAL) {
            log.debug("QuerySampleQuery: idle blocker lookup budget ({}) exhausted this interval",
                MAX_IDLE_BLOCKER_LOOKUPS_PER_INTERVAL);
            return null;
        }
        idleBlockerLookupsThisInterval++;
        String sql = fetchIdleBlockerSql(conn, blockerHandle, blockerMember);
        idleBlockerSqlCache.put(key, sql);
        return sql;
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

    /** Prefix an instrumented application puts on CLIENT_ACCTNG before the traceparent. */
    private static final String TRACEPARENT_PREFIX = "traceparent=";

    private static final String TRACEPARENT_KEY = "traceparent";

    /**
     * The carrier is the traceparent value itself rather than a map, which avoids allocating a
     * carrier map per sampled row. The propagator still allocates its own substrings and span
     * context.
     */
    private static final TextMapGetter<String> TRACEPARENT_GETTER = new TextMapGetter<String>() {
        @Override public Iterable<String> keys(String carrier) {
            return Collections.singleton(TRACEPARENT_KEY);
        }
        @Override public String get(String carrier, String key) {
            return TRACEPARENT_KEY.equals(key) ? carrier : null;
        }
    };

    /**
     * Parses {@code traceparent=<version>-<traceId>-<spanId>-<flags>} out of a session's
     * CLIENT_ACCTNG, as written by an instrumented application via WLM_SET_CLIENT_INFO.
     *
     * <p>Validation is delegated to {@link W3CTraceContextPropagator} because it enforces the full
     * W3C grammar — field count, lengths, lowercase hex, the forbidden {@code ff} version, all-zero
     * ids — and reports failure with an invalid span context rather than an exception.
     *
     * <p>Returns null when the value is absent, not a traceparent, or malformed. CLIENT_ACCTNG is
     * free-form and set by arbitrary client applications, so nothing here may throw — a bad value
     * must cost that one record its trace linkage, not the rest of the collection cycle. The catch
     * is defence in depth behind the propagator's contract, not the primary guard.
     *
     * <p>The prefix match is case-insensitive: the convention is ours, the field is free-form, and
     * silently dropping {@code TRACEPARENT=} would be a confusing way to enforce spelling.
     */
    static SpanContext parseTraceparent(String clientAcctng) {
        if (clientAcctng == null) return null;
        String value = clientAcctng.trim();
        if (value.length() < TRACEPARENT_PREFIX.length()
            || !value.regionMatches(true, 0, TRACEPARENT_PREFIX, 0, TRACEPARENT_PREFIX.length())) {
            return null;
        }

        String traceparent = value.substring(TRACEPARENT_PREFIX.length());
        SpanContext sc;
        try {
            sc = Span.fromContext(
                    W3CTraceContextPropagator.getInstance()
                        .extract(Context.root(), traceparent, TRACEPARENT_GETTER))
                .getSpanContext();
        } catch (RuntimeException e) {
            log.debug("QuerySampleQuery: traceparent in CLIENT_ACCTNG could not be parsed: {}",
                e.getMessage());
            return null;
        }

        if (!sc.isValid()) {
            log.debug("QuerySampleQuery: ignoring malformed traceparent in CLIENT_ACCTNG");
            return null;
        }
        return sc;
    }

    // Converts Db2's CHAR(TIMESTAMP) output "YYYY-MM-DD-HH.MM.SS.NNNNNN" to RFC3339 UTC
    // "YYYY-MM-DDTHH:MM:SS.NNNNNNZ". The SQL expression already subtracts CURRENT TIMEZONE
    // (DB2's UTC offset) so the value is true UTC before it reaches this method.
    static String toRfc3339(String db2Timestamp) {
        if (db2Timestamp == null) return null;
        // "2024-01-15-12.30.45.123456" → "2024-01-15T12:30:45.123456Z"
        String s = db2Timestamp.trim();
        // Too short to carry a full date and time. Returning it unchanged produced a value that
        // looked like a timestamp but had no trailing Z, which downstream requires to build the
        // blocking-event key — so it broke event merging silently instead of failing. Omit it.
        if (s.length() < 19) {
            log.debug("QuerySampleQuery: ignoring unparseable Db2 timestamp '{}'", s);
            return null;
        }
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
    /**
     * Whether a session in this state may carry a usable traceparent.
     *
     * <p>CLIENT_ACCTNG is connection-scoped and outlives the request that set it, so a session that
     * is not currently in a request still holds the last traceparent written to it. Linking those
     * would parent a query sample to a trace that has already finished — wrong linkage rather than
     * missing linkage, which is harder to spot because the record looks correct.
     *
     * <p>{@code waiting} is included deliberately: a LOCKWAIT session is executing a statement, just
     * blocked on a lock, so its traceparent is current — and those are the records blocking
     * detection escalates to WARN, where the correlation is most useful. Unmapped states (snapshot
     * APPL_STATUS values such as UOWWAIT or CONNECTED, which derive to null) are treated as not in a
     * request, matching how sqlserverreceiver forces idle blockers to come out unlinked.
     */
    static boolean linksToTrace(String sessionStatus) {
        return "active".equals(sessionStatus) || "waiting".equals(sessionStatus);
    }

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
