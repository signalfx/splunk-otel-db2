package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.util.SqlObfuscator;
import io.opentelemetry.api.logs.Logger;
import org.slf4j.LoggerFactory;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Collects top SQL statements from the Db2 plan cache and emits each as an OTel
 * log record ({@code db.server.top_query}, severity INFO).
 *
 * Source: {@code MON_GET_PKG_CACHE_STMT} (Db2 9.7+), all members (-2).
 *
 * Ranking: per-interval delta of STMT_EXEC_TIME (primary) then NUM_EXECUTIONS
 * (tiebreaker for sub-millisecond queries). Both keys are in their natural units
 * so the sort is always meaningful.
 *
 * Plan cache key: EXECUTABLE_ID, which is treated as identifying a plan-cache section on its
 * own. This deliberately matches the emitted db2.query_plan.hash, because that hash is the
 * plan's object-storage key downstream — keying the cache by member while emitting a bare id
 * would have the agent and the pipeline disagree about what identifies a plan.
 * Using raw SQL text as a key is incorrect because Db2 can hold multiple sections
 * with identical text for different compilation environments or packages.
 *
 * Baseline cache: bounded TTL map keyed by "member:executableId". Entries are
 * updated each cycle they appear and evicted after {@link #MAX_STALE_CYCLES}
 * consecutive absences. This one stays member-qualified: it is internal delta state that is
 * never emitted, so there is no downstream contract to agree with, and per-member deltas are
 * the correct granularity for per-member execution counters.
 *
 * Cache-miss classification via INSERT_TIMESTAMP: on a cache miss, the section's
 * INSERT_TIMESTAMP is compared to lastServerScrapeInstant (Db2 CURRENT TIMESTAMP
 * captured during the previous successful scrape):
 *   - insertTs &gt; lastServerScrapeInstant → section was compiled after the last scrape →
 *     genuinely new → use current counters as interval delta.
 *   - insertTs &lt;= lastServerScrapeInstant → section predates the last scrape and was
 *     evicted while inactive → seed baseline and skip emission this cycle.
 * This prevents lifetime cumulative totals from being emitted as a single interval
 * when an evicted-but-old section resumes execution.
 *
 * Server-clock comparison: both INSERT_TIMESTAMP and CURRENT TIMESTAMP originate from
 * the Db2 server (same clock, same timezone). Comparing two server-local timestamps via
 * JDBC avoids the JVM/Db2 timezone skew that would arise if a JVM Instant were used as
 * the boundary.
 *
 * Recency filter: {@code LAST_METRICS_UPDATE > CURRENT TIMESTAMP - N MINUTES} where
 * N is derived from the collection interval (2x, minimum 5 minutes) so statements
 * executed early in a long interval are not filtered before the next collection.
 *
 * SQL text is obfuscated via {@link SqlObfuscator} before emission. Records whose
 * obfuscation fails are dropped with a WARN log — never emitted raw.
 *
 * Plan collection (optional): when {@code top_query_collection.max_explain_each_interval > 0}, each emitted
 * top-query record is enriched with {@code db2.query_plan} (JSON plan summary) and
 * {@code db2.query_plan.hash} (EXECUTABLE_ID hex string, changes on recompile). The plan is retrieved via
 * {@code EXPLAIN_FROM_SECTION}: the stored procedure populates the explain tables from
 * the compiled section in the package cache (no query re-execution), then
 * {@code EXPLAIN_OPERATOR} and {@code EXPLAIN_STREAM} are queried to build a JSON plan
 * summary. Requires explain tables to be pre-created in the schema configured by
 * {@code top_query_collection.explain_schema} (run
 * {@code CALL SYSPROC.SYSINSTALLOBJECTS('EXPLAIN','C',NULL,CURRENT SCHEMA)} once).
 * At most {@code top_query_collection.max_explain_each_interval} <em>attempts</em> are
 * made per interval to protect against large {@code top_query_count} values.
 * Plans are cached client-side (LRU, size and TTL from
 * {@code top_query_collection.query_plan_cache_size} /
 * {@code top_query_collection.query_plan_cache_ttl}) to avoid re-explaining unchanged
 * sections on every scrape.
 */
public class TopQueryQuery {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(TopQueryQuery.class);

    // Entries absent from the plan-cache result for this many consecutive scrapes are evicted.
    static final int MAX_STALE_CYCLES = 3;

    // Maximum bytes a fetched plan JSON may accumulate before being discarded.
    // If exceeded, fetchPlanXml returns null (result not cached) so a truncated plan
    // is never stored or emitted.
    private static final int PLAN_MAX_BYTES = 100 * 1024;

    // EXECUTABLE_ID identifies a plan-cache section; it is treated as unique on its own, which
    // is what lets the plan cache key match the emitted db2.query_plan.hash exactly.
    // NUM_EXECUTIONS: total executions (always populated); used for filter, db2.executions, and avg.
    //   NUM_EXEC_WITH_METRICS requires WLM activity monitoring and is 0 on most vanilla Db2
    //   installations, which would cause fast queries (STMT_EXEC_TIME=0) to be dropped.
    // INSERT_TIMESTAMP: when this plan-cache section was compiled. Used to distinguish genuinely
    //   new sections from evicted-old sections that resume execution.
    // CURRENT TIMESTAMP: captured alongside each query result so cache-miss classification
    //   compares two server-local timestamps (same clock, same timezone) rather than mixing
    //   server time with JVM time.
    // STMT_EXEC_TIME: cumulative execution elapsed time (ms), excludes compile/bind cost.
    // TOTAL_CPU_TIME: cumulative CPU time (µs).
    // SORT_OVERFLOWS: correct column name (not TOTAL_SORT_OVERFLOWS).
    // TOTAL_SECTION_SORTS: correct column name (not STMT_SORTS).
    // Lookback is parameterised as %1$d minutes (derived from the collection interval).
    // ORDER BY STMT_EXEC_TIME DESC, NUM_EXECUTIONS DESC with FETCH FIRST %2$d ROWS ONLY
    // yields a deterministic candidate set bounded to maxQuerySampleCount rows: the rows
    // with the highest cumulative elapsed time (a proxy for historical hot queries).
    // Interval-delta ranking over this candidate set is approximate — a statement that
    // had zero activity before the last scrape but is genuinely new-and-hot this interval
    // can only be found if it falls in the top maxQuerySampleCount by cumulative cost.
    // This is documented behaviour; use a large maxQuerySampleCount to widen coverage.
    private static final String SQL =
        "SELECT STMT_TEXT, " +
        "       MEMBER, " +
        "       EXECUTABLE_ID, " +
        "       INSERT_TIMESTAMP, " +
        "       CURRENT TIMESTAMP AS SERVER_NOW, " +
        "       NUM_EXECUTIONS, " +
        "       STMT_EXEC_TIME, " +
        "       TOTAL_CPU_TIME, " +
        "       ROWS_READ, " +
        "       ROWS_RETURNED, " +
        "       ROWS_MODIFIED, " +
        "       TOTAL_SECTION_SORTS, " +
        "       SORT_OVERFLOWS, " +
        "       SECTION_TYPE " +
        "FROM TABLE(MON_GET_PKG_CACHE_STMT(NULL, NULL, NULL, -2)) AS T " +
        "WHERE NUM_EXECUTIONS > 0 " +
        "  AND LAST_METRICS_UPDATE > CURRENT TIMESTAMP - %1$d MINUTES " +
        "  AND STMT_TEXT NOT LIKE '%%%%MON_GET_PKG_CACHE_STMT%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%MON_GET_ACTIVITY%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%MON_GET_DATABASE%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%MON_GET_BUFFERPOOL%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%MON_GET_TABLESPACE%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%MON_GET_TRANSACTION_LOG%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%MON_CURRENT_SQL%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%MON_GET_CONNECTION%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%SNAP_GET_APPL_INFO%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%SNAP_GET_STMT%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%SNAP_GET_DB%%%%' " +
        // No SYSDUMMY1 filter. SELECT ... FROM SYSIBM.SYSDUMMY1 is everyday Db2 application code,
        // so excluding it dropped legitimate customer top queries. It was also redundant: this
        // receiver's only SYSDUMMY1 statement is the snapshot query sample, which the
        // SNAP_GET_APPL_INFO and SNAP_GET_STMT filters above already exclude. Db2 LIKE is
        // case-sensitive too, so an uppercase pattern would not have caught a lowercase occurrence.
        "ORDER BY STMT_EXEC_TIME DESC, NUM_EXECUTIONS DESC " +
        "FETCH FIRST %2$d ROWS ONLY";

    // DB2 explain workflow — two steps (verified on DB2 11.5.8.0):
    //
    // 1. EXPLAIN_FROM_SECTION: populate explain tables from the executable ID.
    //    EXECUTABLE_ID is a VARCHAR(32) FOR BIT DATA token.
    //    SECTION_SOURCE_TYPE='M' (Memory) pulls the section from the live in-memory package
    //    cache — always correct since our IDs come from MON_GET_PKG_CACHE_STMT.
    //    Returns EXPLAIN_REQUESTER + EXPLAIN_TIME as OUT params (the key to the inserted rows).
    //    Requires autocommit=true (or explicit COMMIT) — writes are transaction-scoped.
    //    Requires explain tables to exist in EXPLAIN_SCHEMA (create once via
    //    CALL SYSPROC.SYSINSTALLOBJECTS('EXPLAIN','C',NULL,CURRENT SCHEMA)).
    //
    // 2. EXPLAIN_OPERATOR + EXPLAIN_STREAM: read the inserted plan data and build JSON.
    //    EXPLAIN_INSTANCE rows cascade-delete to all child tables, so deleting from
    //    EXPLAIN_INSTANCE by (EXPLAIN_REQUESTER, EXPLAIN_TIME) cleans up everything.

    // Step 1 — EXPLAIN_FROM_SECTION.
    // JDBC params: (1)=EXECUTABLE_ID, (2)=MEMBER, (3)=EXPLAIN_SCHEMA INOUT, (4-8)=OUT params.
    // SECTION_SOURCE_TYPE='M' (Memory): instructs Db2 to pull the section from the live in-memory
    // package cache. Always correct here — our EXECUTABLE_IDs come from MON_GET_PKG_CACHE_STMT,
    // which only returns sections currently held in memory.
    private static final String SQL_EXPLAIN_FROM_SECTION =
        "CALL SYSPROC.EXPLAIN_FROM_SECTION(CAST(? AS VARCHAR(32) FOR BIT DATA), 'M', '-', ?, ?, ?, ?, ?, ?, ?)";

    // Step 2a — read plan operators for this explain session.
    // Full 5-part EXPLAIN_INSTANCE key prevents row mixing when concurrent collectors
    // share the same explain schema.
    private static final String SQL_EXPLAIN_OPERATOR =
        "SELECT OPERATOR_ID, RTRIM(OPERATOR_TYPE) AS OPERATOR_TYPE, TOTAL_COST " +
        "FROM %s.EXPLAIN_OPERATOR " +
        "WHERE EXPLAIN_REQUESTER = ? AND EXPLAIN_TIME = ? " +
        "  AND SOURCE_NAME = ? AND SOURCE_SCHEMA = ? AND SOURCE_VERSION = ? " +
        "ORDER BY OPERATOR_ID";

    // Step 2b — read plan streams (operator connectivity) for this explain session.
    private static final String SQL_EXPLAIN_STREAM =
        "SELECT STREAM_ID, SOURCE_ID, TARGET_ID, STREAM_COUNT, " +
        "       RTRIM(SOURCE_TYPE) AS SOURCE_TYPE, RTRIM(TARGET_TYPE) AS TARGET_TYPE, " +
        "       RTRIM(OBJECT_SCHEMA) AS OBJECT_SCHEMA, RTRIM(OBJECT_NAME) AS OBJECT_NAME " +
        "FROM %s.EXPLAIN_STREAM " +
        "WHERE EXPLAIN_REQUESTER = ? AND EXPLAIN_TIME = ? " +
        "  AND SOURCE_NAME = ? AND SOURCE_SCHEMA = ? AND SOURCE_VERSION = ? " +
        "ORDER BY STREAM_ID";

    // Step 2c — operator arguments (configuration: join type, prefetch, sort columns, etc.).
    // COST_DETAIL arguments are omitted — they are verbose cost-breakdown strings that
    // duplicate TOTAL_COST already captured in EXPLAIN_OPERATOR.
    private static final String SQL_EXPLAIN_ARGUMENT =
        "SELECT OPERATOR_ID, RTRIM(ARGUMENT_TYPE) AS ARGUMENT_TYPE, RTRIM(ARGUMENT_VALUE) AS ARGUMENT_VALUE " +
        "FROM %s.EXPLAIN_ARGUMENT " +
        "WHERE EXPLAIN_REQUESTER = ? AND EXPLAIN_TIME = ? " +
        "  AND SOURCE_NAME = ? AND SOURCE_SCHEMA = ? AND SOURCE_VERSION = ? " +
        "  AND ARGUMENT_TYPE <> 'COST_DETAIL' " +
        "ORDER BY OPERATOR_ID, ARGUMENT_TYPE";

    // Step 2d — objects (tables and indexes) accessed. EXPLAIN_OBJECT has no STREAM_ID column;
    // the positional correspondence with SOURCE_ID=-1 streams is implicit. ROW_COUNT and
    // COLUMN_COUNT from EXPLAIN_OBJECT are included for cardinality annotations.
    private static final String SQL_EXPLAIN_OBJECT =
        "SELECT RTRIM(OBJECT_SCHEMA) AS OBJECT_SCHEMA, RTRIM(OBJECT_NAME) AS OBJECT_NAME, " +
        "       RTRIM(OBJECT_TYPE) AS OBJECT_TYPE, ROW_COUNT, COLUMN_COUNT " +
        "FROM %s.EXPLAIN_OBJECT " +
        "WHERE EXPLAIN_REQUESTER = ? AND EXPLAIN_TIME = ? " +
        "  AND SOURCE_NAME = ? AND SOURCE_SCHEMA = ? AND SOURCE_VERSION = ? " +
        "ORDER BY OBJECT_NAME";

    // Step 2e — predicates (filter/join conditions with PREDICATE_TEXT containing literals).
    // PREDICATE_TEXT is obfuscated via SqlObfuscator.obfuscateFragment before emission.
    private static final String SQL_EXPLAIN_PREDICATE =
        "SELECT OPERATOR_ID, PREDICATE_ID, RTRIM(HOW_APPLIED) AS HOW_APPLIED, PREDICATE_TEXT " +
        "FROM %s.EXPLAIN_PREDICATE " +
        "WHERE EXPLAIN_REQUESTER = ? AND EXPLAIN_TIME = ? " +
        "  AND SOURCE_NAME = ? AND SOURCE_SCHEMA = ? AND SOURCE_VERSION = ? " +
        "ORDER BY OPERATOR_ID, PREDICATE_ID";

    // Cleanup — delete explain instance; cascades to all child explain tables via FK.
    private static final String SQL_EXPLAIN_DELETE =
        "DELETE FROM %s.EXPLAIN_INSTANCE " +
        "WHERE EXPLAIN_REQUESTER = ? AND EXPLAIN_TIME = ? " +
        "  AND SOURCE_NAME = ? AND SOURCE_SCHEMA = ? AND SOURCE_VERSION = ?";

    // Counter array indices — I_NUM_EXEC is NUM_EXECUTIONS (total), not NUM_EXEC_WITH_METRICS
    private static final int I_EXEC_TIME = 0;
    private static final int I_NUM_EXEC  = 1;
    private static final int I_CPU_US    = 2;
    private static final int I_ROWS_READ = 3;
    private static final int I_ROWS_RET  = 4;
    private static final int I_ROWS_MOD  = 5;
    private static final int I_SORTS     = 6;
    private static final int I_SORT_OVF  = 7;

    private final Supplier<Connection> connSupplier;
    private final String dbName;
    private final Logger otelLogger;
    private final int topQueryCount;
    private final int maxQuerySampleCount;
    private final String sql;
    private final int queryTimeoutSeconds;

    // Plan collection settings — null means plan collection is disabled.
    private final Integer planMaxPerInterval;
    private final int planCacheSize;
    private final long planCacheTtlMs;
    // Configured explain schema (null/empty → pass null to DB2, which chooses USER or default).
    // The actual schema used is read back from the EXPLAIN_SCHEMA INOUT param after each call.
    private final String explainSchema;

    // Client-side LRU plan cache. Key = executableIdHex, matching the emitted
    // db2.query_plan.hash. Evicted by LRU + TTL.
    // Single-threaded scheduler — no synchronization needed.
    private final Map<String, PlanCacheEntry> planCache;


    private static final class PlanCacheEntry {
        final String xml;
        final String hash;
        final long expiresMs;
        PlanCacheEntry(String xml, String hash, long expiresMs) {
            this.xml = xml; this.hash = hash; this.expiresMs = expiresMs;
        }
    }

    // Delta baseline. Single-threaded scheduler — no synchronization needed.
    private final Map<String, CacheEntry> baselineCache = new HashMap<>();
    private int scrapeGen = 0;

    // Db2 CURRENT TIMESTAMP from the most recent successful scrape, used to classify cache misses.
    // Stored as a server-local Timestamp so INSERT_TIMESTAMP comparison stays on the server clock.
    // Null until the first scrape completes.
    private java.sql.Timestamp lastServerScrapeInstant = null;

    private static final class CacheEntry {
        long[] counters;
        int lastSeenGen;
        CacheEntry(long[] counters, int gen) { this.counters = counters; this.lastSeenGen = gen; }
    }

    /**
     * Reset-aware delta for cumulative Db2 counters.
     * If {@code curr >= prev} the counter advanced normally; return the difference.
     * If {@code curr < prev} the plan cache was flushed or the database was restarted and
     * the counter restarted from zero; treat {@code curr} itself as the interval contribution
     * (same approach as DB Agent ADBQueryStatistics#accept).
     */
    static long delta(long curr, long prev) {
        return curr >= prev ? curr - prev : curr;
    }

    public TopQueryQuery(Supplier<Connection> connSupplier, String dbName,
                         Logger otelLogger, int topQueryCount, long intervalSeconds) {
        this(connSupplier, dbName, otelLogger, topQueryCount, 1000, intervalSeconds,
             null, 1000, 3600_000L, "");
    }

    /** Plan-collection enabled variant. */
    public TopQueryQuery(Supplier<Connection> connSupplier, String dbName,
                         Logger otelLogger, int topQueryCount, long intervalSeconds,
                         Integer planMaxPerInterval) {
        this(connSupplier, dbName, otelLogger, topQueryCount, 1000, intervalSeconds,
             planMaxPerInterval, 1000, 3600_000L, "");
    }

    /** Full variant — plan cache size, TTL, and explain schema configurable. */
    public TopQueryQuery(Supplier<Connection> connSupplier, String dbName,
                         Logger otelLogger, int topQueryCount, long intervalSeconds,
                         Integer planMaxPerInterval, int planCacheSize, long planCacheTtlMs) {
        this(connSupplier, dbName, otelLogger, topQueryCount, 1000, intervalSeconds,
             planMaxPerInterval, planCacheSize, planCacheTtlMs, "");
    }

    /** Full variant with explainSchema. */
    public TopQueryQuery(Supplier<Connection> connSupplier, String dbName,
                         Logger otelLogger, int topQueryCount, long intervalSeconds,
                         Integer planMaxPerInterval, int planCacheSize, long planCacheTtlMs,
                         String explainSchema) {
        this(connSupplier, dbName, otelLogger, topQueryCount, 1000, intervalSeconds,
             planMaxPerInterval, planCacheSize, planCacheTtlMs, explainSchema);
    }

    /** Full variant with maxQuerySampleCount and explainSchema. */
    public TopQueryQuery(Supplier<Connection> connSupplier, String dbName,
                         Logger otelLogger, int topQueryCount, int maxQuerySampleCount,
                         long intervalSeconds,
                         Integer planMaxPerInterval, int planCacheSize, long planCacheTtlMs,
                         String explainSchema) {
        this.connSupplier          = connSupplier;
        this.dbName                = dbName;
        this.otelLogger            = otelLogger;
        this.topQueryCount         = topQueryCount;
        this.maxQuerySampleCount   = maxQuerySampleCount;
        this.planMaxPerInterval    = planMaxPerInterval;
        this.planCacheSize         = planCacheSize;
        this.planCacheTtlMs        = planCacheTtlMs;
        // Empty/null → pass null to EXPLAIN_FROM_SECTION so DB2 selects the schema (USER/default).
        // The INOUT param returns the actual schema used; queries are built dynamically per call.
        this.explainSchema = (explainSchema == null || explainSchema.isBlank())
                             ? null : explainSchema.trim().toUpperCase();
        // Lookback = 2x interval, minimum 5 minutes
        long lookbackMinutes = Math.max(5L, (intervalSeconds * 2 + 59) / 60);
        this.sql = String.format(SQL, lookbackMinutes, maxQuerySampleCount);
        // Timeout = half the interval; with the enforced 60s minimum that is always >= 30s.
        this.queryTimeoutSeconds = (int) Math.max(30L, intervalSeconds / 2);
        this.planCache = planMaxPerInterval != null ? buildLruCache(planCacheSize) : null;
        log.info("TopQueryQuery: top={}, sampleLimit={}, interval={}s, lookback={}min, timeout={}s, planEnabled={}, cacheSize={}, cacheTtlMs={}, explainSchema={}",
            topQueryCount, maxQuerySampleCount, intervalSeconds, lookbackMinutes, queryTimeoutSeconds,
            planMaxPerInterval != null, planCacheSize, planCacheTtlMs,
            this.explainSchema != null ? this.explainSchema : "(DB2 default)");
    }

    @SuppressWarnings("serial")
    private static Map<String, PlanCacheEntry> buildLruCache(int maxSize) {
        return new LinkedHashMap<String, PlanCacheEntry>(16, 0.75f, true) {
            @Override protected boolean removeEldestEntry(Map.Entry<String, PlanCacheEntry> e) {
                return size() > maxSize;
            }
        };
    }

    /** Returns {@code true} when the scrape completed (baseline seeded or deltas emitted), {@code false} on failure. */
    public boolean collect() {
        Map<String, long[]>        curr              = new HashMap<>();
        // texts[i] = [rawSql, secType, key, member, executableIdHex]
        List<String[]>             texts             = new ArrayList<>();
        List<long[]>               counters          = new ArrayList<>();
        List<java.sql.Timestamp>   insertTimestamps  = new ArrayList<>();
        // SERVER_NOW from the first result row (same for all rows in one query).
        // Null until at least one row is processed; left null on empty result sets.
        java.sql.Timestamp thisServerNow = null;

        Connection conn;
        try {
            conn = connSupplier.get();
        } catch (Exception e) {
            log.warn("TopQueryQuery collection failed (connection): {}", e.getMessage());
            return false;
        }

        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(queryTimeoutSeconds);
            try (ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    String rawSql = rs.getString("STMT_TEXT");
                    if (rawSql == null || rawSql.isBlank()) continue;
                    if (thisServerNow == null) {
                        thisServerNow = rs.getTimestamp("SERVER_NOW");
                    }
                    long   member      = rs.getLong("MEMBER");
                    String execId      = rs.getString("EXECUTABLE_ID");
                    String key         = member + ":" + execId;
                    java.sql.Timestamp insertTs = rs.getTimestamp("INSERT_TIMESTAMP");
                    long[] c = {
                        rs.getLong("STMT_EXEC_TIME"),
                        rs.getLong("NUM_EXECUTIONS"),
                        rs.getLong("TOTAL_CPU_TIME"),
                        rs.getLong("ROWS_READ"),
                        rs.getLong("ROWS_RETURNED"),
                        rs.getLong("ROWS_MODIFIED"),
                        rs.getLong("TOTAL_SECTION_SORTS"),
                        rs.getLong("SORT_OVERFLOWS")
                    };
                    curr.put(key, c);
                    // execIdHex is only needed when plan collection is enabled.
                    //
                    // The baseline key above uses getString while this uses hex(getBytes), which
                    // looks like one column read two ways. Verified against JCC 11.5.9.0 that they
                    // agree exactly: getString on this FOR BIT DATA column returns the hex of
                    // getBytes (64 chars for 32 bytes) with no replacement or control characters, so
                    // there is no lossy charset decode that could collide distinct sections. Both
                    // forms are kept because each cache is internally consistent and the explain
                    // path is specified in terms of the binary id.
                    String execIdHex = null;
                    if (planMaxPerInterval != null) {
                        byte[] execIdBytes = rs.getBytes("EXECUTABLE_ID");
                        execIdHex = execIdBytes != null ? bytesToHex(execIdBytes) : execId;
                    }
                    texts.add(new String[]{rawSql, rs.getString("SECTION_TYPE"),
                                           key, String.valueOf(member), execIdHex});
                    counters.add(c);
                    // Null INSERT_TIMESTAMP (unexpected) treated as Epoch — always predates
                    // any real scrape, so the section is conservatively seeded, never inflated.
                    insertTimestamps.add(insertTs != null ? insertTs
                        : new java.sql.Timestamp(0));
                }
            }
        } catch (Exception e) {
            log.warn("TopQueryQuery collection failed: {}", e.getMessage());
            return false; // lastServerScrapeInstant not updated — next cycle re-classifies misses correctly
        }
        if (curr.size() > 5000) {
            log.warn("TopQueryQuery: plan cache returned {} rows — consider tuning LAST_METRICS_UPDATE lookback", curr.size());
        }

        scrapeGen++;
        boolean isFirstCollection = (scrapeGen == 1);

        // Snapshot old baseline counters for keys present in curr (before updating the cache).
        Map<String, long[]> prev = new HashMap<>();
        for (String key : curr.keySet()) {
            CacheEntry existing = baselineCache.get(key);
            if (existing != null) prev.put(key, existing.counters);
        }

        // Update cache: add new entries, refresh existing ones.
        for (Map.Entry<String, long[]> e : curr.entrySet()) {
            CacheEntry ce = baselineCache.get(e.getKey());
            if (ce == null) {
                baselineCache.put(e.getKey(), new CacheEntry(e.getValue(), scrapeGen));
            } else {
                ce.counters    = e.getValue();
                ce.lastSeenGen = scrapeGen;
            }
        }

        // Evict entries absent for more than MAX_STALE_CYCLES consecutive scrapes.
        Iterator<CacheEntry> it = baselineCache.values().iterator();
        while (it.hasNext()) {
            if (it.next().lastSeenGen < scrapeGen - MAX_STALE_CYCLES) it.remove();
        }

        // Capture the PREVIOUS scrape boundary before overwriting it.
        // The delta loop must compare INSERT_TIMESTAMP against the *previous* scrape's server
        // clock — not the current one. Overwriting first caused A1: insertTs.isAfter(current)
        // is always false (no section is compiled in the future), so every cache miss was
        // silently seeded instead of emitted.
        java.sql.Timestamp prevServerScrapeInstant = lastServerScrapeInstant;
        // Only advance the boundary when the query returned rows (SERVER_NOW is set).
        // An empty result set means the lookback window matched nothing — the previous
        // boundary is still the correct reference for the next cycle's cache-miss classification.
        // Resetting to Epoch here would incorrectly classify every returning old section as
        // new because virtually every INSERT_TIMESTAMP is after Epoch.
        if (thisServerNow != null) {
            lastServerScrapeInstant = thisServerNow;
        }

        if (isFirstCollection) {
            log.debug("TopQueryQuery: first collection, seeding delta baseline ({} rows)", curr.size());
            return true;
        }

        // Compute interval deltas.
        // prevC != null → normal delta path.
        // prevC == null → cache miss; use INSERT_TIMESTAMP to decide:
        //   - insertTs > prevServerScrapeInstant: compiled after the last scrape → genuinely
        //     new → use current counters as delta (correct interval contribution).
        //   - insertTs <= prevServerScrapeInstant: section predates the last scrape, was
        //     evicted while inactive → skip emission; baseline is already seeded in cache.
        //   - prevServerScrapeInstant == null (first post-seed cycle): treat all misses as old
        //     (conservative — no prior server clock to compare against; next cycle is normal).
        // deltas[i] = [row_index, execTimeDelta, numExecDelta]
        List<long[]> deltas = new ArrayList<>();
        for (int i = 0; i < texts.size(); i++) {
            String key   = texts.get(i)[2];
            long[] c     = counters.get(i);
            long[] prevC = prev.get(key);
            long execTimeDelta;
            long numExecDelta;
            if (prevC == null) {
                java.sql.Timestamp insertTs = insertTimestamps.get(i);
                if (prevServerScrapeInstant == null || !insertTs.after(prevServerScrapeInstant)) {
                    // Old section resuming after eviction — emit nothing; next cycle has the baseline.
                    continue;
                }
                execTimeDelta = c[I_EXEC_TIME];
                numExecDelta  = c[I_NUM_EXEC];
            } else {
                execTimeDelta = delta(c[I_EXEC_TIME], prevC[I_EXEC_TIME]);
                numExecDelta  = delta(c[I_NUM_EXEC],  prevC[I_NUM_EXEC]);
            }
            if (execTimeDelta <= 0 && numExecDelta <= 0) continue;
            deltas.add(new long[]{i, execTimeDelta, numExecDelta});
        }

        // Primary sort: STMT_EXEC_TIME delta DESC (elapsed time ranks highest).
        // Tiebreaker: NUM_EXECUTIONS delta DESC (for sub-millisecond queries with same exec time).
        // Units are never mixed — both keys are independent sort criteria.
        deltas.sort((a, b) -> {
            int cmp = Long.compare(b[1], a[1]);
            return cmp != 0 ? cmp : Long.compare(b[2], a[2]);
        });

        Instant now = Instant.now();
        int emitCount = 0;
        // planAttempts counts only EXPLAIN_FROM_SECTION calls (cache misses + failures) against the
        // budget. Cache hits are free — the budget cap guards only DB2 round-trips.
        int planAttempts = 0;
        for (int rank = 0; rank < Math.min(topQueryCount, deltas.size()); rank++) {
            int  idx           = (int) deltas.get(rank)[0];
            long execTimeDelta = deltas.get(rank)[1];
            long[] c           = counters.get(idx);
            long[] prevC       = prev.get(texts.get(idx)[2]); // null for new entries
            String[] t         = texts.get(idx);

            String rawSql  = t[0];
            String secType = t[1];
            long member    = Long.parseLong(t[3]);

            long numExecDelta  = prevC == null ? c[I_NUM_EXEC]  : delta(c[I_NUM_EXEC],  prevC[I_NUM_EXEC]);
            long cpuUsDelta    = prevC == null ? c[I_CPU_US]    : delta(c[I_CPU_US],    prevC[I_CPU_US]);
            long rowsReadDelta = prevC == null ? c[I_ROWS_READ] : delta(c[I_ROWS_READ], prevC[I_ROWS_READ]);
            long rowsRetDelta  = prevC == null ? c[I_ROWS_RET]  : delta(c[I_ROWS_RET],  prevC[I_ROWS_RET]);
            long rowsModDelta  = prevC == null ? c[I_ROWS_MOD]  : delta(c[I_ROWS_MOD],  prevC[I_ROWS_MOD]);
            long sortsDelta    = prevC == null ? c[I_SORTS]     : delta(c[I_SORTS],     prevC[I_SORTS]);
            long sortOvfDelta  = prevC == null ? c[I_SORT_OVF]  : delta(c[I_SORT_OVF],  prevC[I_SORT_OVF]);

            String obfuscated = SqlObfuscator.obfuscate(rawSql.trim());
            if (SqlObfuscator.OBFUSCATION_FAILED.equals(obfuscated)) {
                log.warn("TopQueryQuery: obfuscation failed for member={} — record dropped", member);
                continue;
            }

            double totalExecSec = execTimeDelta / 1000.0;
            double avgExecSec   = numExecDelta > 0 ? totalExecSec / numExecDelta : 0.0;

            TopQueryEvent event = TopQueryEvent.of(dbName, obfuscated, member, numExecDelta, totalExecSec)
                .avgExecTimeSec(avgExecSec)
                .totalCpuTimeMicros(cpuUsDelta)
                .rowsRead(rowsReadDelta)
                .rowsReturned(rowsRetDelta)
                .rowsModified(rowsModDelta)
                .sorts(sortsDelta)
                .sortOverflows(sortOvfDelta)
                .stmtType(secType);

            // Plan collection: dynamic SQL only ('D'). Static SQL ('S') lives in packages
            // on disk — SECTION_SOURCE_TYPE='M' (Memory) cannot find them and always
            // returns SQLSTATE=55075. Skip static sections to avoid wasting budget.
            if (planMaxPerInterval != null && "D".equals(secType)) {
                String execIdHex = t[4];
                // Keyed by EXECUTABLE_ID alone, matching db2.query_plan.hash exactly. The hash is
                // what downstream uses as the plan's object-storage key, so a member-qualified cache
                // key would mean the agent and the rest of the pipeline disagreed about what
                // identifies a plan — one cache entry per member, but a single object per id.
                String cacheKey = execIdHex;
                String planXml = null;
                String planHash = null;

                PlanCacheEntry cached = planCache.get(cacheKey);
                if (cached != null && cached.expiresMs > System.currentTimeMillis()) {
                    // Cache hit — free, no budget consumed
                    planXml  = cached.xml;
                    planHash = cached.hash;
                } else if (planAttempts < planMaxPerInterval) {
                    planAttempts++;
                    PlanCacheEntry fetched = fetchPlanEntry(conn, execIdHex, (int) member);
                    if (fetched != null) {
                        // fetched.xml == null means permanent failure — cache it so we don't retry.
                        // fetched.xml != null means success.
                        planCache.put(cacheKey, fetched);
                        planXml  = fetched.xml;
                        planHash = fetched.hash;
                    }
                    // fetched == null means transient failure — do not cache; retry next interval.
                }
                if (planXml != null) {
                    event.queryPlan(planXml, planHash);
                }
            }

            event.emit(otelLogger, now);
            emitCount++;
        }
        log.debug("TopQueryQuery: emitted {} top queries (baselineCacheSize={}, planAttempts={})",
            emitCount, baselineCache.size(), planAttempts);
        return true;
    }

    /**
     * Retrieve the explain plan for one section using EXPLAIN_FROM_SECTION.
     *
     * Returns a {@link PlanCacheEntry} on success or permanent failure (xml==null),
     * or {@code null} on transient failure so the caller does not negative-cache it.
     *
     * 1. Call {@code SYSPROC.EXPLAIN_FROM_SECTION} with EXECUTABLE_ID and SECTION_SOURCE_TYPE='M'
     *    (empirically verified on DB2 11.5 as the correct value for dynamic SQL).
     *    Returns EXPLAIN_REQUESTER and EXPLAIN_TIME that identify the inserted rows.
     * 2. Read {@code EXPLAIN_OPERATOR} and {@code EXPLAIN_STREAM} in the explain schema.
     * 3. Build a JSON plan summary string.
     * 4. DELETE from {@code EXPLAIN_INSTANCE} (cascades to all child tables via FK).
     *
     * Requires explain tables pre-created:
     *   CALL SYSPROC.SYSINSTALLOBJECTS('EXPLAIN','C',NULL,CURRENT SCHEMA)
     */
    private PlanCacheEntry fetchPlanEntry(Connection conn, String execIdHex, int member) {
        byte[] execIdBytes = hexToBytes(execIdHex);
        if (execIdBytes == null) return null;

        String explainRequester = null;
        Timestamp explainTime   = null;
        String usedSchema       = null;
        String sourceName       = null;
        String sourceSchema     = null;
        String sourceVersion    = null;

        try {
            // Step 1: EXPLAIN_FROM_SECTION
            // Params: (1)=EXECUTABLE_ID, (2)=MEMBER, (3)=EXPLAIN_SCHEMA INOUT, (4-8)=OUT params
            try (CallableStatement cs = conn.prepareCall(SQL_EXPLAIN_FROM_SECTION)) {
                cs.setQueryTimeout(queryTimeoutSeconds);
                cs.setBytes(1, execIdBytes);
                cs.setInt(2, member);
                cs.setString(3, explainSchema);          // null → DB2 selects USER/default
                cs.registerOutParameter(3, Types.VARCHAR);   // EXPLAIN_SCHEMA INOUT — read back actual schema
                cs.registerOutParameter(4, Types.VARCHAR);   // EXPLAIN_REQUESTER
                cs.registerOutParameter(5, Types.TIMESTAMP); // EXPLAIN_TIME
                cs.registerOutParameter(6, Types.VARCHAR);   // SOURCE_NAME
                cs.registerOutParameter(7, Types.VARCHAR);   // SOURCE_SCHEMA
                cs.registerOutParameter(8, Types.VARCHAR);   // SOURCE_VERSION
                cs.execute();
                usedSchema       = cs.getString(3);  // actual schema DB2 used
                explainRequester = cs.getString(4);
                explainTime      = cs.getTimestamp(5);
                sourceName       = cs.getString(6);
                sourceSchema     = cs.getString(7);
                sourceVersion    = cs.getString(8);
            }

            if (explainRequester == null || explainTime == null || usedSchema == null) {
                // EXPLAIN_FROM_SECTION returned no output params — section was evicted from memory.
                // Permanent: cache null entry so we don't retry this interval.
                return new PlanCacheEntry(null, null, System.currentTimeMillis() + planCacheTtlMs);
            }

            // Step 2a: read EXPLAIN_OPERATOR using the schema DB2 confirmed
            String sqlOp   = String.format(SQL_EXPLAIN_OPERATOR,  usedSchema);
            String sqlSt   = String.format(SQL_EXPLAIN_STREAM,    usedSchema);
            String sqlArg  = String.format(SQL_EXPLAIN_ARGUMENT,  usedSchema);
            String sqlObj  = String.format(SQL_EXPLAIN_OBJECT,    usedSchema);
            String sqlPred = String.format(SQL_EXPLAIN_PREDICATE, usedSchema);

            // planBudgetExceeded is set when the incremental UTF-8 byte count of the plan
            // JSON exceeds PLAN_MAX_BYTES. A running byte counter avoids the O(n²) cost of
            // re-encoding the entire StringBuilder on every row.
            boolean planBudgetExceeded = false;

            StringBuilder sb = new StringBuilder("{\"operators\":[");
            int planByteCount = sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            boolean firstOp = true;
            try (PreparedStatement ps = conn.prepareStatement(sqlOp)) {
                ps.setQueryTimeout(queryTimeoutSeconds);
                ps.setString(1, explainRequester);
                ps.setTimestamp(2, explainTime);
                ps.setString(3, sourceName);
                ps.setString(4, sourceSchema);
                ps.setString(5, sourceVersion);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        int opId      = rs.getInt("OPERATOR_ID");
                        String opType = rs.getString("OPERATOR_TYPE");
                        double cost   = rs.getDouble("TOTAL_COST");
                        String fragment = "{\"id\":" + opId
                            + ",\"type\":\"" + (opType == null ? "" : opType.trim()) + '"'
                            + ",\"cost\":" + cost + '}';
                        int fragBytes = fragment.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                        if (!firstOp) fragBytes += 1; // comma separator
                        planByteCount += fragBytes;
                        if (planByteCount > PLAN_MAX_BYTES) { planBudgetExceeded = true; break; }
                        if (!firstOp) sb.append(',');
                        firstOp = false;
                        sb.append(fragment);
                    }
                }
            }
            if (firstOp) return null; // no operators means nothing was explained

            // Step 2b: read EXPLAIN_STREAM
            sb.append("],\"streams\":[");
            planByteCount += "],\"streams\":[".getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            boolean firstSt = true;
            if (!planBudgetExceeded) {
            try (PreparedStatement ps = conn.prepareStatement(sqlSt)) {
                ps.setQueryTimeout(queryTimeoutSeconds);
                ps.setString(1, explainRequester);
                ps.setTimestamp(2, explainTime);
                ps.setString(3, sourceName);
                ps.setString(4, sourceSchema);
                ps.setString(5, sourceVersion);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        int streamId     = rs.getInt("STREAM_ID");
                        int srcId        = rs.getInt("SOURCE_ID");
                        int tgtId        = rs.getInt("TARGET_ID");
                        double streamCount = rs.getDouble("STREAM_COUNT");
                        String srcType   = rs.getString("SOURCE_TYPE");
                        String tgtType   = rs.getString("TARGET_TYPE");
                        String objSchema = rs.getString("OBJECT_SCHEMA");
                        String objName   = rs.getString("OBJECT_NAME");
                        StringBuilder frag = new StringBuilder();
                        frag.append("{\"id\":").append(streamId)
                            .append(",\"from\":").append(srcId)
                            .append(",\"to\":").append(tgtId)
                            .append(",\"rows\":").append(streamCount);
                        if (srcType != null && !srcType.isEmpty())
                            frag.append(",\"src_type\":\"").append(jsonEscape(srcType)).append('"');
                        if (tgtType != null && !tgtType.isEmpty())
                            frag.append(",\"tgt_type\":\"").append(jsonEscape(tgtType)).append('"');
                        if (objSchema != null && !objSchema.isEmpty())
                            frag.append(",\"obj_schema\":\"").append(jsonEscape(objSchema)).append('"');
                        if (objName != null && !objName.isEmpty())
                            frag.append(",\"obj_name\":\"").append(jsonEscape(objName)).append('"');
                        frag.append('}');
                        String fragment = frag.toString();
                        int fragBytes = fragment.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                        if (!firstSt) fragBytes += 1; // comma separator
                        planByteCount += fragBytes;
                        if (planByteCount > PLAN_MAX_BYTES) { planBudgetExceeded = true; break; }
                        if (!firstSt) sb.append(',');
                        firstSt = false;
                        sb.append(fragment);
                    }
                }
            }
            } // end if (!planBudgetExceeded)

            // Steps 2c-2e: argument, object, predicate tables are standard in all supported
            // DB2 versions but each is queried in its own try/catch.
            // SQLFeatureNotSupportedException degrades silently to an empty array.
            // All other exceptions (timeouts, permissions) are re-thrown so the outer
            // catch returns null (keeping the entry uncached).

            // Step 2c: EXPLAIN_ARGUMENT — operator configuration (join type, prefetch, etc.)
            // For PREDICATE-typed values (which may be SQL or XQuery), double-quoted content
            // is pre-redacted before SQL obfuscation via redactDoubleQuotedStrings(). This is
            // safe regardless of operator language: in SQL, "..." is a delimited identifier
            // (never PII); in XQuery, "..." is a string literal that must be redacted.
            // No language classification is needed.
            sb.append("],\"arguments\":[");
            planByteCount += "],\"arguments\":[".getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            if (!planBudgetExceeded) {
            try {
                boolean firstArg = true;
                try (PreparedStatement ps = conn.prepareStatement(sqlArg)) {
                    ps.setQueryTimeout(queryTimeoutSeconds);
                    ps.setString(1, explainRequester); ps.setTimestamp(2, explainTime);
                    ps.setString(3, sourceName);       ps.setString(4, sourceSchema);
                    ps.setString(5, sourceVersion);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            int    opId    = rs.getInt("OPERATOR_ID");
                            String argType = rs.getString("ARGUMENT_TYPE");
                            String rawVal  = rs.getString("ARGUMENT_VALUE");
                            StringBuilder frag = new StringBuilder();
                            frag.append("{\"op\":").append(opId)
                                .append(",\"type\":\"").append(jsonEscape(argType)).append('"');
                            // ARGUMENT_VALUE may contain literals. For SQL-syntax types use the
                            // SQL obfuscator; for PREDICATE-typed values pre-redact double-quoted
                            // content first so XQuery string literals are never passed through.
                            // Unknown or non-SQL types (XPATH) are omitted entirely.
                            if (rawVal != null && isSafeArgumentType(argType)) {
                                String toObf = "PREDICATE".equalsIgnoreCase(argType)
                                    ? redactDoubleQuotedStrings(rawVal.trim())
                                    : rawVal.trim();
                                String obfVal = SqlObfuscator.obfuscateFragment(toObf);
                                if (!SqlObfuscator.OBFUSCATION_FAILED.equals(obfVal)) {
                                    frag.append(",\"value\":\"").append(jsonEscape(obfVal)).append('"');
                                }
                            }
                            frag.append('}');
                            String fragment = frag.toString();
                            int fragBytes = fragment.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                            if (!firstArg) fragBytes += 1; // comma separator
                            planByteCount += fragBytes;
                            if (planByteCount > PLAN_MAX_BYTES) { planBudgetExceeded = true; break; }
                            if (!firstArg) sb.append(',');
                            firstArg = false;
                            sb.append(fragment);
                        }
                    }
                }
            } catch (Exception e) {
                if (isCompatibilityException(e)) {
                    log.debug("TopQueryQuery: EXPLAIN_ARGUMENT query failed (compatibility) for execIdHex={}: {}", execIdHex, e.getMessage());
                } else {
                    throw e;
                }
            }
            } // end if (!planBudgetExceeded)

            // Step 2d: EXPLAIN_OBJECT — tables and indexes accessed
            sb.append("],\"objects\":[");
            planByteCount += "],\"objects\":[".getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            if (!planBudgetExceeded) {
            try {
                boolean firstObj = true;
                try (PreparedStatement ps = conn.prepareStatement(sqlObj)) {
                    ps.setQueryTimeout(queryTimeoutSeconds);
                    ps.setString(1, explainRequester); ps.setTimestamp(2, explainTime);
                    ps.setString(3, sourceName);       ps.setString(4, sourceSchema);
                    ps.setString(5, sourceVersion);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            String fragment = "{\"schema\":\"" + jsonEscape(rs.getString("OBJECT_SCHEMA")) + '"'
                                + ",\"name\":\"" + jsonEscape(rs.getString("OBJECT_NAME")) + '"'
                                + ",\"type\":\"" + jsonEscape(rs.getString("OBJECT_TYPE")) + '"'
                                + ",\"rows\":" + rs.getDouble("ROW_COUNT")
                                + ",\"cols\":" + rs.getInt("COLUMN_COUNT") + '}';
                            int fragBytes = fragment.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                            if (!firstObj) fragBytes += 1; // comma separator
                            planByteCount += fragBytes;
                            if (planByteCount > PLAN_MAX_BYTES) { planBudgetExceeded = true; break; }
                            if (!firstObj) sb.append(',');
                            firstObj = false;
                            sb.append(fragment);
                        }
                    }
                }
            } catch (Exception e) {
                if (isCompatibilityException(e)) {
                    log.debug("TopQueryQuery: EXPLAIN_OBJECT query failed (compatibility) for execIdHex={}: {}", execIdHex, e.getMessage());
                } else {
                    throw e;
                }
            }
            } // end if (!planBudgetExceeded)

            // Step 2e: EXPLAIN_PREDICATE — filter/join conditions; PREDICATE_TEXT is obfuscated
            sb.append("],\"predicates\":[");
            planByteCount += "],\"predicates\":[".getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            if (!planBudgetExceeded) {
            try {
                boolean firstPred = true;
                try (PreparedStatement ps = conn.prepareStatement(sqlPred)) {
                    ps.setQueryTimeout(queryTimeoutSeconds);
                    ps.setString(1, explainRequester); ps.setTimestamp(2, explainTime);
                    ps.setString(3, sourceName);       ps.setString(4, sourceSchema);
                    ps.setString(5, sourceVersion);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            int    opId       = rs.getInt("OPERATOR_ID");
                            int    predId     = rs.getInt("PREDICATE_ID");
                            String howApplied = rs.getString("HOW_APPLIED");
                            String rawText    = rs.getString("PREDICATE_TEXT");
                            String obfText    = null;
                            if (rawText != null && !rawText.isBlank()) {
                                // Pre-redact double-quoted content before SQL obfuscation.
                                // In SQL, "..." is a delimited identifier (not PII); in XQuery,
                                // "..." is a string literal that must be redacted. Pre-redacting
                                // makes obfuscation safe for both languages without requiring
                                // operator language classification — so predicate safety does not
                                // depend on EXPLAIN_ARGUMENT collection succeeding.
                                String preRedacted = redactDoubleQuotedStrings(rawText.trim());
                                String ob = SqlObfuscator.obfuscateFragment(preRedacted);
                                if (!SqlObfuscator.OBFUSCATION_FAILED.equals(ob)) obfText = ob;
                            }
                            StringBuilder frag = new StringBuilder();
                            frag.append("{\"op\":").append(opId)
                                .append(",\"id\":").append(predId)
                                .append(",\"how\":\"").append(jsonEscape(howApplied)).append('"');
                            if (obfText != null) {
                                frag.append(",\"text\":\"").append(jsonEscape(obfText)).append('"');
                            }
                            frag.append('}');
                            String fragment = frag.toString();
                            int fragBytes = fragment.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                            if (!firstPred) fragBytes += 1; // comma separator
                            planByteCount += fragBytes;
                            if (planByteCount > PLAN_MAX_BYTES) { planBudgetExceeded = true; break; }
                            if (!firstPred) sb.append(',');
                            firstPred = false;
                            sb.append(fragment);
                        }
                    }
                }
            } catch (Exception e) {
                if (isCompatibilityException(e)) {
                    log.debug("TopQueryQuery: EXPLAIN_PREDICATE query failed (compatibility) for execIdHex={}: {}", execIdHex, e.getMessage());
                } else {
                    throw e;
                }
            }
            } // end if (!planBudgetExceeded)

            planByteCount += "]}".getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            if (planByteCount > PLAN_MAX_BYTES) planBudgetExceeded = true;
            if (planBudgetExceeded) {
                log.debug("TopQueryQuery: plan for execIdHex={} exceeded {} byte budget — discarding", execIdHex, PLAN_MAX_BYTES);
                return null;
            }
            sb.append("]}");
            String planXml = sb.toString();
            return new PlanCacheEntry(planXml, execIdHex, System.currentTimeMillis() + planCacheTtlMs);

        } catch (Exception e) {
            if (isPermanentPlanFailure(e)) {
                log.debug("TopQueryQuery: plan fetch permanently failed for execIdHex={} member={} ({}): {}",
                    execIdHex, member, e.getClass().getSimpleName(), e.getMessage());
                return new PlanCacheEntry(null, null, System.currentTimeMillis() + planCacheTtlMs);
            }
            log.debug("TopQueryQuery: plan fetch transiently failed for execIdHex={} member={}: {}",
                execIdHex, member, e.getMessage());
            return null;
        } finally {
            // Step 3: cleanup — delete explain rows (cascades via FK to all child tables)
            if (explainRequester != null && explainTime != null && usedSchema != null) {
                String sqlDel = String.format(SQL_EXPLAIN_DELETE, usedSchema);
                try (PreparedStatement ps = conn.prepareStatement(sqlDel)) {
                    ps.setQueryTimeout(queryTimeoutSeconds);
                    ps.setString(1, explainRequester);
                    ps.setTimestamp(2, explainTime);
                    ps.setString(3, sourceName);
                    ps.setString(4, sourceSchema);
                    ps.setString(5, sourceVersion);
                    ps.executeUpdate();
                } catch (Exception e) {
                    log.warn("TopQueryQuery: explain cleanup failed for requester={} time={}: {}",
                        explainRequester, explainTime, e.getMessage());
                }
            }
        }
    }

    // Explicit allowlist: default false so unknown future ARGUMENT_TYPE values are
    // omitted rather than risking literal leakage through an unrecognised type.
    private static boolean isSafeArgumentType(String argType) {
        if (argType == null) return false;
        switch (argType.trim().toUpperCase()) {
            // These argument types contain SQL expressions or plain identifiers/keywords
            // that the SQL obfuscator can safely process.
            case "SORTKEY":
            case "PREDICATE":
            case "PREFETCHTYPE":
            case "SCANTYPE":
            case "JOINTYPE":
            case "UNIQUEKEY":
            case "ROWCOUNT":
            case "NUMROWS":
            case "INDEXONLY":
            case "MAXPAGES":
            case "TBSPACEID":
            case "BIDI":
                return true;
            default:
                // Unknown or non-SQL types (e.g. XPATH) — omit value rather than risk leaking literals.
                return false;
        }
    }

    /**
     * Replaces each double-quoted string body with {@code ?} so that XQuery string
     * literals (which use double-quote syntax) are redacted before the SQL obfuscator
     * runs. In SQL, {@code "..."} is a delimited identifier whose content is never PII;
     * replacing its body with {@code ?} is therefore safe for both languages.
     *
     * Handles {@code ""} escape sequences inside the quoted body. An unterminated
     * double-quoted sequence (reached end-of-input) is replaced with {@code ?} to avoid
     * leaking a partial literal.
     */
    static String redactDoubleQuotedStrings(String s) {
        if (s == null || s.indexOf('"') < 0) return s;
        StringBuilder sb = new StringBuilder(s.length());
        int i = 0;
        int len = s.length();
        while (i < len) {
            char c = s.charAt(i);
            if (c == '"') {
                i++; // skip opening quote
                // consume body, handling "" escape
                while (i < len) {
                    char q = s.charAt(i);
                    i++;
                    if (q == '"') {
                        if (i < len && s.charAt(i) == '"') {
                            i++; // "" escape — skip both
                        } else {
                            break; // closing quote
                        }
                    }
                }
                sb.append('"').append('?').append('"');
            } else {
                sb.append(c);
                i++;
            }
        }
        return sb.toString();
    }

    // Only SQLFeatureNotSupportedException is a safe compatibility signal — it means
    // the JDBC driver explicitly reports the feature as unsupported for this DB version.
    // Operational errors (timeout, permissions, syntax) re-throw to the outer catch
    // so fetchPlanEntry returns null (uncached).
    private static boolean isCompatibilityException(Exception e) {
        return e instanceof SQLFeatureNotSupportedException;
    }

    // Permanent plan failures: section is no longer in memory (SQLSTATE 55075) or
    // explain tables have not been created (SQLSTATE 51002 / SQLCODE -805/-218).
    // These conditions will not resolve within the cache TTL window, so negative-
    // caching them avoids repeated failed EXPLAIN_FROM_SECTION calls every interval.
    private static boolean isPermanentPlanFailure(Exception e) {
        if (!(e instanceof java.sql.SQLException)) return false;
        String sqlState = ((java.sql.SQLException) e).getSQLState();
        if (sqlState == null) return false;
        // 55075: DB2 section no longer in memory
        // 42704: object not found (explain tables missing or dropped)
        // 42601/42S02: similar schema/object errors on some JDBC stacks
        return sqlState.startsWith("55075") || sqlState.startsWith("42704")
            || sqlState.startsWith("42601") || sqlState.startsWith("42S02");
    }


    // Escapes a string value for embedding inside a JSON string literal.
    // Covers the characters required by RFC 8259 §7: backslash, double-quote,
    // and the C0 control range (U+0000–U+001F). DB2 explain values are plain
    // ASCII identifiers and keywords — this handles any edge case defensively.
    static String jsonEscape(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"')       sb.append("\\\"");
            else if (c == '\\') sb.append("\\\\");
            else if (c < 0x20)  sb.append(String.format("\\u%04x", (int) c));
            else                sb.append(c);
        }
        return sb.toString();
    }


    static byte[] hexToBytes(String hex) {
        if (hex == null || hex.length() % 2 != 0) return null;
        int len = hex.length();
        byte[] data = new byte[len / 2];
        try {
            for (int i = 0; i < len; i += 2) {
                data[i / 2] = (byte) Integer.parseInt(hex.substring(i, i + 2), 16);
            }
        } catch (NumberFormatException e) {
            return null;
        }
        return data;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
