package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.util.SqlObfuscator;
import io.opentelemetry.api.logs.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
 * Cache key: member + EXECUTABLE_ID uniquely identifies a plan-cache section.
 * Using raw SQL text as a key is incorrect because Db2 can hold multiple sections
 * with identical text for different compilation environments or packages.
 *
 * Baseline cache: bounded TTL map keyed by "member:executableId". Entries are
 * updated each cycle they appear and evicted after {@link #MAX_STALE_CYCLES}
 * consecutive absences.
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
 * Plan collection (optional): when {@code top_query_plan_enabled: true}, each emitted
 * top-query record is enriched with {@code db2.query_plan} (SECTION_XML CLOB from
 * {@code MON_GET_PKG_CACHE_STMT_DETAILS}) and {@code db2.query_plan.hash} (SHA-256).
 * Db2 stores the compiled plan in the package cache alongside the statement — no query
 * re-execution occurs, matching the behaviour of SQL Server and Oracle plan collection.
 * EXECUTABLE_ID is bound as bytes (BLOB) since it is a binary token.
 * At most {@code planMaxPerInterval} CLOB fetches are issued per interval to protect
 * against large {@code top_query_count} values.
 */
public class TopQueryQuery {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(TopQueryQuery.class);

    // Entries absent from the plan-cache result for this many consecutive scrapes are evicted.
    static final int MAX_STALE_CYCLES = 3;

    // EXECUTABLE_ID uniquely identifies a plan-cache section (member + exec ID = unique key).
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
    // Lookback is parameterised as %d minutes, derived from the collection interval.
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
        "  AND LAST_METRICS_UPDATE > CURRENT TIMESTAMP - %d MINUTES " +
        "  AND STMT_TEXT NOT LIKE '%%%%MON_GET_PKG_CACHE_STMT%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%MON_GET_ACTIVITY%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%MON_GET_DATABASE%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%MON_GET_BUFFERPOOL%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%MON_GET_TABLESPACE%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%MON_CURRENT_SQL%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%MON_GET_CONNECTION%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%SNAP_GET_APPL_INFO%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%SNAP_GET_STMT%%%%' " +
        "  AND STMT_TEXT NOT LIKE '%%%%SYSDUMMY1%%%%'";

    // Reads the compiled plan XML for one section from the package cache.
    // EXECUTABLE_ID is a binary token: must be bound with setBytes(), not setString().
    // SECTION_XML is a CLOB column containing the compiled execution plan XML (Db2 10.5+).
    private static final String SQL_PLAN =
        "SELECT SECTION_XML " +
        "FROM TABLE(MON_GET_PKG_CACHE_STMT_DETAILS(CAST(? AS BLOB(100)), ?)) AS D " +
        "FETCH FIRST 1 ROW ONLY";

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
    private final String sql;
    private final int queryTimeoutSeconds;

    // Plan collection settings — null means plan collection is disabled.
    private final Integer planMaxPerInterval;

    // TTL-bounded delta baseline. Single-threaded scheduler — no synchronization needed.
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
        this(connSupplier, dbName, otelLogger, topQueryCount, intervalSeconds, null);
    }

    /** Plan-collection enabled variant. */
    public TopQueryQuery(Supplier<Connection> connSupplier, String dbName,
                         Logger otelLogger, int topQueryCount, long intervalSeconds,
                         Integer planMaxPerInterval) {
        this.connSupplier       = connSupplier;
        this.dbName             = dbName;
        this.otelLogger         = otelLogger;
        this.topQueryCount      = topQueryCount;
        this.planMaxPerInterval = planMaxPerInterval;
        // Lookback = 2x interval, minimum 5 minutes
        long lookbackMinutes = Math.max(5L, (intervalSeconds * 2 + 59) / 60);
        this.sql = String.format(SQL, lookbackMinutes);
        // Timeout = half the interval; with the enforced 60s minimum that is always >= 30s.
        this.queryTimeoutSeconds = (int) Math.max(30L, intervalSeconds / 2);
        log.info("TopQueryQuery: top={}, interval={}s, lookback={}min, timeout={}s, planEnabled={}",
            topQueryCount, intervalSeconds, lookbackMinutes, queryTimeoutSeconds,
            planMaxPerInterval != null);
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
                    // execIdHex is only needed when plan collection is enabled
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
        int plansFetched = 0;
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

            // Optionally enrich with explain plan — plan is already compiled in the package cache,
            // MON_GET_PKG_CACHE_STMT_DETAILS reads it without re-executing the query (same as SQL
            // Server sys.dm_exec_query_plan / Oracle V$SQL_PLAN — no cache needed).
            if (planMaxPerInterval != null && plansFetched < planMaxPerInterval) {
                String execIdHex = t[4];
                String planXml = fetchPlanXml(conn, execIdHex, (int) member);
                if (planXml != null) {
                    event.queryPlan(planXml, sha256Hex(planXml));
                    plansFetched++;
                }
            }

            event.emit(otelLogger, now);
            emitCount++;
        }
        log.debug("TopQueryQuery: emitted {} top queries (cache size={}, plansFetched={})",
            emitCount, baselineCache.size(), plansFetched);
        return true;
    }

    /**
     * Fetch the compiled plan XML for one section from the Db2 package cache.
     *
     * EXECUTABLE_ID is a binary token returned as VARBINARY by MON_GET_PKG_CACHE_STMT.
     * The getString() result is a hex-encoded representation; we pass it back as bytes
     * via setBytes() so the JDBC driver binds it as BLOB, matching the function signature.
     *
     * SECTION_XML is a CLOB column (Db2 10.5+) containing the compiled plan XML.
     */
    private String fetchPlanXml(Connection conn, String execIdHex, int member) {
        byte[] execIdBytes = hexToBytes(execIdHex);
        if (execIdBytes == null) return null;
        try (PreparedStatement ps = conn.prepareStatement(SQL_PLAN)) {
            ps.setQueryTimeout(queryTimeoutSeconds);
            ps.setBytes(1, execIdBytes);
            ps.setInt(2, member < 0 ? -2 : member);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Clob clob = rs.getClob("SECTION_XML");
                    if (clob == null) return null;
                    try {
                        String xml = clob.getSubString(1, (int) Math.min(clob.length(), 2_000_000));
                        return (xml == null || xml.isBlank()) ? null : xml;
                    } finally {
                        try { clob.free(); } catch (Exception ignored) {}
                    }
                }
            }
        } catch (Exception e) {
            log.debug("TopQueryQuery: plan fetch failed for execIdHex={} member={}: {}",
                execIdHex, member, e.getMessage());
        }
        return null;
    }

    public static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
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
}
