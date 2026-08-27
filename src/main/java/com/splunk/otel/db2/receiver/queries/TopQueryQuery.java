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
 * Ranking: interval delta of {@code STMT_EXEC_TIME} (execution elapsed time in ms,
 * cumulative per cache entry). On each collection the previous STMT_EXEC_TIME is
 * subtracted so only work done during this interval is ranked — consistent with
 * contrib top-query scrapers. The first collection seeds the baseline and emits nothing.
 *
 * Recency filter: {@code LAST_METRICS_UPDATE > CURRENT TIMESTAMP - 30 MINUTES}
 * excludes entries that have not been touched recently.
 *
 * SQL text is obfuscated via {@link SqlObfuscator} before emission. Records whose
 * obfuscation fails are dropped with a WARN log — never emitted raw.
 *
 * Time values: STMT_EXEC_TIME (ms) is converted to seconds. TOTAL_CPU_TIME (µs)
 * is converted to seconds. Both match OTel semantic conventions.
 */
public class TopQueryQuery {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(TopQueryQuery.class);

    // No ORDER BY or FETCH FIRST here — we fetch all recent rows, compute per-interval
    // STMT_EXEC_TIME deltas in Java, sort those deltas, then emit the top N.
    // STMT_EXEC_TIME: total execution elapsed time (ms), excludes compile/bind cost.
    // TOTAL_CPU_TIME: cumulative CPU time (µs).
    // LAST_METRICS_UPDATE: last time this cache row's counters were updated.
    // TOTAL_SECTION_SORTS / TOTAL_SORT_OVERFLOWS: correct column names for sorts.
    private static final String SQL =
        "SELECT STMT_TEXT, " +
        "       MEMBER, " +
        "       NUM_EXECUTIONS, " +
        "       STMT_EXEC_TIME, " +
        "       TOTAL_CPU_TIME, " +
        "       ROWS_READ, " +
        "       ROWS_RETURNED, " +
        "       ROWS_MODIFIED, " +
        "       TOTAL_SECTION_SORTS, " +
        "       TOTAL_SORT_OVERFLOWS, " +
        "       SECTION_TYPE " +
        "FROM TABLE(MON_GET_PKG_CACHE_STMT(NULL, NULL, NULL, -2)) AS T " +
        "WHERE NUM_EXECUTIONS > 0 " +
        "  AND LAST_METRICS_UPDATE > CURRENT TIMESTAMP - 30 MINUTES " +
        "  AND STMT_TEXT NOT LIKE '%%MON_GET_PKG_CACHE_STMT%%' " +
        "  AND STMT_TEXT NOT LIKE '%%MON_GET_ACTIVITY%%' " +
        "  AND STMT_TEXT NOT LIKE '%%MON_GET_DATABASE%%' " +
        "  AND STMT_TEXT NOT LIKE '%%MON_GET_BUFFERPOOL%%' " +
        "  AND STMT_TEXT NOT LIKE '%%MON_GET_TABLESPACE%%'";

    private final Supplier<Connection> connSupplier;
    private final String dbName;
    private final Logger otelLogger;
    private final int topQueryCount;

    // Previous STMT_EXEC_TIME (ms) per "member:stmtText" key; replaced each cycle.
    // Single-threaded scheduler — HashMap is safe without synchronization.
    private Map<String, Long> prevExecTimeMs = new HashMap<>();

    public TopQueryQuery(Supplier<Connection> connSupplier, String dbName,
                         Logger otelLogger, int topQueryCount) {
        this.connSupplier  = connSupplier;
        this.dbName        = dbName;
        this.otelLogger    = otelLogger;
        this.topQueryCount = topQueryCount;
        log.info("TopQueryQuery: will emit top {} queries by interval execution time", topQueryCount);
    }

    public void collect() {
        // numeric columns: [member, numExec, stmtExecMs, cpuUs, rowsRead, rowsRet, rowsMod, sorts, sortOvf]
        List<long[]>   nums    = new ArrayList<>();
        // text columns:    [rawSql, sectionType, key]
        List<String[]> texts   = new ArrayList<>();
        Map<String, Long> curr = new HashMap<>();

        try {
            Connection conn = connSupplier.get();
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(SQL)) {
                while (rs.next()) {
                    String rawSql = rs.getString("STMT_TEXT");
                    if (rawSql == null || rawSql.isBlank()) continue;
                    long member      = rs.getLong("MEMBER");
                    long stmtExecMs  = rs.getLong("STMT_EXEC_TIME");
                    long numExec     = rs.getLong("NUM_EXECUTIONS");
                    long cpuUs       = rs.getLong("TOTAL_CPU_TIME");
                    long rowsRead    = rs.getLong("ROWS_READ");
                    long rowsRet     = rs.getLong("ROWS_RETURNED");
                    long rowsMod     = rs.getLong("ROWS_MODIFIED");
                    long sorts       = rs.getLong("TOTAL_SECTION_SORTS");
                    long sortOvf     = rs.getLong("TOTAL_SORT_OVERFLOWS");
                    String secType   = rs.getString("SECTION_TYPE");
                    String key       = member + ":" + rawSql;
                    curr.put(key, stmtExecMs);
                    nums.add(new long[]{member, numExec, stmtExecMs, cpuUs, rowsRead, rowsRet, rowsMod, sorts, sortOvf});
                    texts.add(new String[]{rawSql, secType, key});
                }
            }
        } catch (Exception e) {
            log.warn("TopQueryQuery collection failed: {}", e.getMessage());
            return;
        }

        Map<String, Long> prev = prevExecTimeMs;
        prevExecTimeMs = curr;

        if (prev.isEmpty()) {
            log.debug("TopQueryQuery: first collection, seeding delta baseline ({} rows)", nums.size());
            return;
        }

        // Compute per-interval deltas; skip new entries (no previous) and zero-delta entries.
        // deltas[i] = [row_index, deltaMs]
        List<long[]> deltas = new ArrayList<>();
        for (int i = 0; i < nums.size(); i++) {
            Long prevMs = prev.get(texts.get(i)[2]);
            if (prevMs == null) continue;
            long deltaMs = nums.get(i)[2] - prevMs;
            if (deltaMs <= 0) continue;
            deltas.add(new long[]{i, deltaMs});
        }

        deltas.sort((a, b) -> Long.compare(b[1], a[1]));

        Instant now = Instant.now();
        int emitCount = 0;
        for (int rank = 0; rank < Math.min(topQueryCount, deltas.size()); rank++) {
            int idx      = (int) deltas.get(rank)[0];
            long deltaMs = deltas.get(rank)[1];
            long[] n     = nums.get(idx);
            String[] t   = texts.get(idx);

            String rawSql  = t[0];
            String secType = t[1];
            long member    = n[0];
            long numExec   = n[1];
            long cpuUs     = n[3];
            long rowsRead  = n[4];
            long rowsRet   = n[5];
            long rowsMod   = n[6];
            long sorts     = n[7];
            long sortOvf   = n[8];

            String obfuscated = SqlObfuscator.obfuscate(rawSql.trim());
            if (SqlObfuscator.OBFUSCATION_FAILED.equals(obfuscated)) {
                log.warn("TopQueryQuery: obfuscation failed for member={} — record dropped", member);
                continue;
            }

            double totalExecSec = deltaMs / 1000.0;
            double avgExecSec   = numExec > 0 ? totalExecSec / numExec : 0.0;

            TopQueryEvent.of(dbName, obfuscated, member, numExec, totalExecSec)
                .avgExecTimeSec(avgExecSec)
                .totalCpuTimeMicros(cpuUs)
                .rowsRead(rowsRead)
                .rowsReturned(rowsRet)
                .rowsModified(rowsMod)
                .sorts(sorts)
                .sortOverflows(sortOvf)
                .stmtType(secType)
                .emit(otelLogger, now);
            emitCount++;
        }
        log.debug("TopQueryQuery: emitted {} top queries", emitCount);
    }
}
