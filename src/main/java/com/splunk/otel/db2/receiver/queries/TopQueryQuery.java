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
 * NUM_EXEC_WITH_METRICS, TOTAL_CPU_TIME, ROWS_READ, ROWS_RETURNED, ROWS_MODIFIED,
 * STMT_SORTS, SORT_OVERFLOWS). Statements are ranked by the STMT_EXEC_TIME delta
 * so only work done in the current collection window is ranked — consistent with
 * contrib top-query scrapers. The first collection seeds the baseline; nothing is emitted.
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
    // NUM_EXEC_WITH_METRICS: executions whose activity metrics were captured; used for avg.
    // STMT_EXEC_TIME: cumulative execution elapsed time (ms), excludes compile/bind cost.
    // TOTAL_CPU_TIME: cumulative CPU time (µs).
    // SORT_OVERFLOWS: correct column name (not TOTAL_SORT_OVERFLOWS).
    // TOTAL_SECTION_SORTS: correct column name (not STMT_SORTS).
    // Lookback is parameterised as %d minutes, derived from the collection interval.
    private static final String SQL =
        "SELECT STMT_TEXT, " +
        "       MEMBER, " +
        "       EXECUTABLE_ID, " +
        "       NUM_EXEC_WITH_METRICS, " +
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

    // Delta baseline keyed by "member:executableId" — unique per plan-cache section.
    // Stores all cumulative counters from the previous cycle.
    // Single-threaded scheduler — HashMap is safe without synchronization.
    private Map<String, long[]> prevCounters = new HashMap<>();

    // Counter array indices
    private static final int I_EXEC_TIME = 0;
    private static final int I_NUM_EXEC  = 1;
    private static final int I_CPU_US    = 2;
    private static final int I_ROWS_READ = 3;
    private static final int I_ROWS_RET  = 4;
    private static final int I_ROWS_MOD  = 5;
    private static final int I_SORTS     = 6;
    private static final int I_SORT_OVF  = 7;

    public TopQueryQuery(Supplier<Connection> connSupplier, String dbName,
                         Logger otelLogger, int topQueryCount, long intervalSeconds) {
        this.connSupplier  = connSupplier;
        this.dbName        = dbName;
        this.otelLogger    = otelLogger;
        this.topQueryCount = topQueryCount;
        // Lookback = 2x interval, minimum 5 minutes
        long lookbackMinutes = Math.max(5L, (intervalSeconds * 2 + 59) / 60);
        this.sql = String.format(SQL, lookbackMinutes);
        log.info("TopQueryQuery: top={}, interval={}s, lookback={}min",
            topQueryCount, intervalSeconds, lookbackMinutes);
    }

    public void collect() {
        Map<String, long[]> curr  = new HashMap<>();
        List<String[]>      texts = new ArrayList<>(); // [rawSql, secType, key]
        List<long[]>        counters = new ArrayList<>();

        try {
            Connection conn = connSupplier.get();
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    String rawSql   = rs.getString("STMT_TEXT");
                    if (rawSql == null || rawSql.isBlank()) continue;
                    long member     = rs.getLong("MEMBER");
                    String execId   = rs.getString("EXECUTABLE_ID");
                    String key      = member + ":" + execId;
                    long[] c = {
                        rs.getLong("STMT_EXEC_TIME"),
                        rs.getLong("NUM_EXEC_WITH_METRICS"),
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
        } catch (Exception e) {
            log.warn("TopQueryQuery collection failed: {}", e.getMessage());
            return;
        }

        Map<String, long[]> prev = prevCounters;
        prevCounters = curr;

        if (prev.isEmpty()) {
            log.debug("TopQueryQuery: first collection, seeding delta baseline ({} rows)", curr.size());
            return;
        }

        // Compute interval deltas; skip only zero-delta entries.
        // New entries (prevC == null) were compiled/executed since the last scrape — use their
        // current counters as the interval delta, matching PostgreSQL contrib's cache-miss handling.
        // deltas[i] = [row_index, execTimeDeltaMs]
        List<long[]> deltas = new ArrayList<>();
        for (int i = 0; i < texts.size(); i++) {
            long[] prevC = prev.get(texts.get(i)[2]);
            long execTimeDelta = prevC == null
                ? counters.get(i)[I_EXEC_TIME]
                : counters.get(i)[I_EXEC_TIME] - prevC[I_EXEC_TIME];
            if (execTimeDelta <= 0) continue;
            deltas.add(new long[]{i, execTimeDelta});
        }

        deltas.sort((a, b) -> Long.compare(b[1], a[1]));

        Instant now = Instant.now();
        int emitCount = 0;
        for (int rank = 0; rank < Math.min(topQueryCount, deltas.size()); rank++) {
            int  idx           = (int) deltas.get(rank)[0];
            long execTimeDelta = deltas.get(rank)[1];
            long[] c           = counters.get(idx);
            long[] prevC       = prev.get(texts.get(idx)[2]); // null for new entries
            String[] t         = texts.get(idx);

            String rawSql  = t[0];
            String secType = t[1];
            long member    = Long.parseLong(t[3]);

            // Delta all cumulative counters; new entries use current value as full interval delta.
            long numExecDelta  = prevC == null ? c[I_NUM_EXEC]  : c[I_NUM_EXEC]  - prevC[I_NUM_EXEC];
            long cpuUsDelta    = prevC == null ? c[I_CPU_US]    : c[I_CPU_US]    - prevC[I_CPU_US];
            long rowsReadDelta = prevC == null ? c[I_ROWS_READ] : c[I_ROWS_READ] - prevC[I_ROWS_READ];
            long rowsRetDelta  = prevC == null ? c[I_ROWS_RET]  : c[I_ROWS_RET]  - prevC[I_ROWS_RET];
            long rowsModDelta  = prevC == null ? c[I_ROWS_MOD]  : c[I_ROWS_MOD]  - prevC[I_ROWS_MOD];
            long sortsDelta    = prevC == null ? c[I_SORTS]     : c[I_SORTS]     - prevC[I_SORTS];
            long sortOvfDelta  = prevC == null ? c[I_SORT_OVF]  : c[I_SORT_OVF]  - prevC[I_SORT_OVF];

            String obfuscated = SqlObfuscator.obfuscate(rawSql.trim());
            if (SqlObfuscator.OBFUSCATION_FAILED.equals(obfuscated)) {
                log.warn("TopQueryQuery: obfuscation failed for member={} — record dropped", member);
                continue;
            }

            double totalExecSec = execTimeDelta / 1000.0;
            // Average over interval executions (NUM_EXEC_WITH_METRICS delta)
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
