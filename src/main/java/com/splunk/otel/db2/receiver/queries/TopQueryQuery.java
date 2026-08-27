package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.util.SqlObfuscator;
import io.opentelemetry.api.logs.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Collects top SQL statements from the Db2 plan cache and emits each as an OTel
 * log record ({@code db.server.top_query}, severity INFO).
 *
 * Source: {@code MON_GET_PKG_CACHE_STMT} (Db2 9.7+), all members (-2).
 *
 * Ranking: per-interval delta of all cumulative counters (STMT_EXEC_TIME,
 * NUM_EXECUTIONS, TOTAL_CPU_TIME, ROWS_READ, ROWS_RETURNED, ROWS_MODIFIED,
 * TOTAL_SECTION_SORTS, SORT_OVERFLOWS). Statements are ranked by the STMT_EXEC_TIME delta;
 * when STMT_EXEC_TIME=0 (sub-millisecond queries) NUM_EXECUTIONS delta is used as tiebreaker.
 * The first collection seeds the baseline; nothing is emitted.
 *
 * Cache key: member + EXECUTABLE_ID uniquely identifies a plan-cache section.
 * Using raw SQL text as a key is incorrect because Db2 can hold multiple sections
 * with identical text for different compilation environments or packages.
 *
 * Recency filter: {@code LAST_METRICS_UPDATE > CURRENT TIMESTAMP - N MINUTES} where
 * N is derived from the collection interval (2x, minimum 5 minutes) so statements
 * executed early in a long interval are not filtered before the next collection.
 *
 * SQL text is obfuscated via {@link SqlObfuscator} before emission. Records whose
 * obfuscation fails are dropped with a WARN log — never emitted raw.
 */
public class TopQueryQuery {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(TopQueryQuery.class);

    // EXECUTABLE_ID uniquely identifies a plan-cache section (member + exec ID = unique key).
    // NUM_EXECUTIONS: total executions (always populated); used for filter, db2.executions, and avg.
    //   NUM_EXEC_WITH_METRICS is NOT used — it requires WLM activity monitoring and is 0 on most
    //   vanilla Db2 installations, which would cause fast queries (STMT_EXEC_TIME=0) to be dropped.
    // STMT_EXEC_TIME: cumulative execution elapsed time (ms), excludes compile/bind cost.
    // TOTAL_CPU_TIME: cumulative CPU time (µs).
    // SORT_OVERFLOWS: correct column name (not TOTAL_SORT_OVERFLOWS).
    // TOTAL_SECTION_SORTS: correct column name (not STMT_SORTS).
    // Lookback is parameterised as %d minutes, derived from the collection interval.
    private static final String SQL =
        "SELECT STMT_TEXT, " +
        "       MEMBER, " +
        "       EXECUTABLE_ID, " +
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
        "  AND STMT_TEXT NOT LIKE '%%%%MON_GET_TABLESPACE%%%%'";

    private final Supplier<Connection> connSupplier;
    private final String dbName;
    private final Logger otelLogger;
    private final int topQueryCount;
    private final String sql;
    private final int queryTimeoutSeconds;

    // Delta baseline keyed by "member:executableId" — unique per plan-cache section.
    // Stores all cumulative counters from the previous cycle.
    // Single-threaded scheduler — HashMap is safe without synchronization.
    private Map<String, long[]> prevCounters = new HashMap<>();

    // Counter array indices — I_NUM_EXEC is NUM_EXECUTIONS (total), not NUM_EXEC_WITH_METRICS
    private static final int I_EXEC_TIME = 0;
    private static final int I_NUM_EXEC  = 1;
    private static final int I_CPU_US    = 2;
    private static final int I_ROWS_READ = 3;
    private static final int I_ROWS_RET  = 4;
    private static final int I_ROWS_MOD  = 5;
    private static final int I_SORTS     = 6;
    private static final int I_SORT_OVF  = 7;

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
        this.connSupplier  = connSupplier;
        this.dbName        = dbName;
        this.otelLogger    = otelLogger;
        this.topQueryCount = topQueryCount;
        // Lookback = 2x interval, minimum 5 minutes
        long lookbackMinutes = Math.max(5L, (intervalSeconds * 2 + 59) / 60);
        this.sql = String.format(SQL, lookbackMinutes);
        // Timeout = half the interval, minimum 30s — prevents a slow plan-cache scan from
        // blocking the scheduler thread into the next collection cycle.
        this.queryTimeoutSeconds = (int) Math.max(30L, intervalSeconds / 2);
        log.info("TopQueryQuery: top={}, interval={}s, lookback={}min, timeout={}s",
            topQueryCount, intervalSeconds, lookbackMinutes, queryTimeoutSeconds);
    }

    public void collect() {
        Map<String, long[]> curr  = new HashMap<>();
        List<String[]>      texts = new ArrayList<>(); // [rawSql, secType, key]
        List<long[]>        counters = new ArrayList<>();

        try {
            Connection conn = connSupplier.get();
            try (Statement st = conn.createStatement()) {
                st.setQueryTimeout(queryTimeoutSeconds);
                try (ResultSet rs = st.executeQuery(sql)) {
                    while (rs.next()) {
                        String rawSql = rs.getString("STMT_TEXT");
                        if (rawSql == null || rawSql.isBlank()) continue;
                        long member   = rs.getLong("MEMBER");
                        String execId = rs.getString("EXECUTABLE_ID");
                        String key    = member + ":" + execId;
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
                        texts.add(new String[]{rawSql, rs.getString("SECTION_TYPE"),
                                               key, String.valueOf(member)});
                        counters.add(c);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("TopQueryQuery collection failed: {}", e.getMessage());
            return;
        }

        boolean isFirstCollection = prevCounters.isEmpty();
        // Snapshot the old baseline before merging so delta computation uses pre-update values.
        // Then merge curr into the persistent baseline: update existing entries, add new ones,
        // but never remove entries. This ensures that an entry aging out of the lookback
        // window retains its baseline counters so its delta is not inflated when it returns.
        Map<String, long[]> prev = isFirstCollection ? new HashMap<>() : new HashMap<>(prevCounters);
        prevCounters.putAll(curr);

        if (isFirstCollection) {
            log.debug("TopQueryQuery: first collection, seeding delta baseline ({} rows)", curr.size());
            return;
        }

        // Compute interval deltas; skip entries with no new executions this interval.
        // New entries (prevC == null): compiled/executed since the last scrape — use current
        // counters as the interval delta (PostgreSQL contrib cache-miss handling).
        // For new entries with STMT_EXEC_TIME=0 (very fast queries), emit if numExecDelta>0.
        // deltas[i] = [row_index, execTimeDeltaMs, rankValue]
        List<long[]> deltas = new ArrayList<>();
        for (int i = 0; i < texts.size(); i++) {
            long[] c     = counters.get(i);
            long[] prevC = prev.get(texts.get(i)[2]);
            long execTimeDelta;
            long numExecDelta;
            if (prevC == null) {
                execTimeDelta = c[I_EXEC_TIME];
                numExecDelta  = c[I_NUM_EXEC];
            } else {
                execTimeDelta = delta(c[I_EXEC_TIME], prevC[I_EXEC_TIME]);
                numExecDelta  = delta(c[I_NUM_EXEC],  prevC[I_NUM_EXEC]);
            }
            // Emit if there was new execution time, or if new executions occurred (fast queries)
            if (execTimeDelta <= 0 && numExecDelta <= 0) continue;
            // Rank by exec time delta; use numExecDelta as tiebreaker for zero-time entries.
            // Store [row_index, execTimeDeltaMs, rankValue] so emit loop always uses the real
            // exec time delta (not the tiebreaker) when computing totalExecSec.
            long rankValue = execTimeDelta > 0 ? execTimeDelta : numExecDelta;
            deltas.add(new long[]{i, execTimeDelta, rankValue});
        }

        deltas.sort((a, b) -> Long.compare(b[2], a[2]));

        Instant now = Instant.now();
        int emitCount = 0;
        for (int rank = 0; rank < Math.min(topQueryCount, deltas.size()); rank++) {
            int  idx           = (int) deltas.get(rank)[0];
            long execTimeDelta = deltas.get(rank)[1]; // real exec time delta, not rank value
            long[] c           = counters.get(idx);
            long[] prevC       = prev.get(texts.get(idx)[2]); // null for new entries
            String[] t         = texts.get(idx);

            String rawSql  = t[0];
            String secType = t[1];
            long member    = Long.parseLong(t[3]);

            // Delta all cumulative counters; new entries use current value as full interval delta.
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

            TopQueryEvent.of(dbName, obfuscated, member, numExecDelta, totalExecSec)
                .avgExecTimeSec(avgExecSec)
                .totalCpuTimeMicros(cpuUsDelta)
                .rowsRead(rowsReadDelta)
                .rowsReturned(rowsRetDelta)
                .rowsModified(rowsModDelta)
                .sorts(sortsDelta)
                .sortOverflows(sortOvfDelta)
                .stmtType(secType)
                .emit(otelLogger, now);
            emitCount++;
        }
        log.debug("TopQueryQuery: emitted {} top queries", emitCount);
    }
}
