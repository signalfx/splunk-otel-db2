package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.util.SqlObfuscator;
import io.opentelemetry.api.logs.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Fetches explain plans for the top SQL statements in the Db2 plan cache and
 * emits each as a {@code db.server.query_plan} OTel log record.
 *
 * Source: {@code MON_GET_PKG_CACHE_STMT_DETAILS(executable_id, member)} — reads
 * the compiled plan XML that Db2 stored when the statement was first prepared.
 * No query re-execution occurs; the plan is a pure read from the package cache.
 *
 * Plan cache: an LRU cache keyed on {@code "member:executableId"} limits repeated
 * fetches of stable plans. Entries expire after {@link #DEFAULT_PLAN_CACHE_TTL_MS}
 * (10 minutes). On a cache hit the plan emit is skipped entirely, avoiding
 * expensive {@code MON_GET_PKG_CACHE_STMT_DETAILS} calls for every interval.
 *
 * Per-interval budget: at most {@code maxPlansPerInterval} plan fetches are issued
 * per {@link #collect()} call to protect against thundering-herd on cold starts.
 *
 * Plan XML hash: a SHA-256 hex digest of the raw plan XML is computed and emitted
 * as {@code db2.query_plan.hash} — the same key the dbmon-processor uses as the
 * object-store deduplication key.
 *
 * SQL text is always obfuscated via {@link SqlObfuscator} before emission. If
 * obfuscation fails the record is dropped with a WARN log.
 */
public class ExplainPlanQuery {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(ExplainPlanQuery.class);

    static final long DEFAULT_PLAN_CACHE_TTL_MS = 10 * 60 * 1000L; // 10 minutes
    static final int  DEFAULT_MAX_PLANS_PER_INTERVAL = 20;

    // Selects the top-N plan-cache entries by cumulative elapsed time, filtered to
    // recently-active statements. Parameterised by %d (lookback minutes) and %d (row limit).
    private static final String SQL_TOP_STMTS =
        "SELECT STMT_TEXT, MEMBER, EXECUTABLE_ID " +
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
        "  AND STMT_TEXT NOT LIKE '%%%%SYSDUMMY1%%%%' " +
        "ORDER BY STMT_EXEC_TIME DESC " +
        "FETCH FIRST %d ROWS ONLY";

    // Fetches the compiled plan XML for one section from the package cache.
    // SECTION_XML is the full plan XML returned by MON_GET_PKG_CACHE_STMT_DETAILS.
    private static final String SQL_PLAN =
        "SELECT SECTION_XML " +
        "FROM TABLE(MON_GET_PKG_CACHE_STMT_DETAILS(?, ?)) AS D " +
        "FETCH FIRST 1 ROW ONLY";

    private final Supplier<Connection> connSupplier;
    private final String dbName;
    private final Logger otelLogger;
    private final int  maxPlansPerInterval;
    private final long planCacheTtlMs;
    private final String sql;
    private final int queryTimeoutSeconds;

    // LRU cache: key = "member:executableId", value = expiry timestamp (ms since epoch).
    // Bounded to 2 * maxPlansPerInterval entries to avoid unbounded growth.
    private final Map<String, Long> planCache;

    public ExplainPlanQuery(Supplier<Connection> connSupplier, String dbName,
                            Logger otelLogger, int maxPlansPerInterval,
                            long intervalSeconds) {
        this(connSupplier, dbName, otelLogger, maxPlansPerInterval, intervalSeconds,
             DEFAULT_PLAN_CACHE_TTL_MS, 30);
    }

    ExplainPlanQuery(Supplier<Connection> connSupplier, String dbName,
                     Logger otelLogger, int maxPlansPerInterval,
                     long intervalSeconds, long planCacheTtlMs, int queryTimeoutSeconds) {
        this.connSupplier        = connSupplier;
        this.dbName              = dbName;
        this.otelLogger          = otelLogger;
        this.maxPlansPerInterval = maxPlansPerInterval;
        this.planCacheTtlMs      = planCacheTtlMs;
        this.queryTimeoutSeconds = queryTimeoutSeconds;

        long lookbackMinutes = Math.max(5L, (intervalSeconds * 2 + 59) / 60);
        this.sql = String.format(SQL_TOP_STMTS, lookbackMinutes, maxPlansPerInterval);

        final int maxCacheSize = maxPlansPerInterval * 2;
        this.planCache = new LinkedHashMap<>(maxCacheSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                return size() > maxCacheSize;
            }
        };

        log.info("ExplainPlanQuery: maxPlans={}, cacheTtl={}min, lookback={}min, timeout={}s",
            maxPlansPerInterval, planCacheTtlMs / 60_000, lookbackMinutes, queryTimeoutSeconds);
    }

    public void collect() {
        long now = System.currentTimeMillis();
        evictExpired(now);

        try {
            Connection conn = connSupplier.get();

            // Collect candidate statements from the plan cache.
            java.util.List<String[]> candidates = new java.util.ArrayList<>(); // [rawSql, execId, member]

            try (Statement st = conn.createStatement()) {
                st.setQueryTimeout(queryTimeoutSeconds);
                try (ResultSet rs = st.executeQuery(sql)) {
                    while (rs.next() && candidates.size() < maxPlansPerInterval) {
                        String rawSql = rs.getString("STMT_TEXT");
                        if (rawSql == null || rawSql.isBlank()) continue;
                        long   member = rs.getLong("MEMBER");
                        String execId = rs.getString("EXECUTABLE_ID");
                        candidates.add(new String[]{rawSql, execId, String.valueOf(member)});
                    }
                }
            }

            Instant timestamp = Instant.ofEpochMilli(now);
            int emitCount = 0;
            for (String[] c : candidates) {
                String rawSql  = c[0];
                String execId  = c[1];
                long   member  = Long.parseLong(c[2]);
                String cacheKey = member + ":" + execId;
                Long expiry = planCache.get(cacheKey);
                if (expiry != null && now < expiry) {
                    log.debug("ExplainPlanQuery: cache hit for key={}", cacheKey);
                    continue;
                }

                String planXml = fetchPlan(conn, execId, (int) member);
                if (planXml == null) continue;

                String obfuscated = SqlObfuscator.obfuscate(rawSql.trim());
                if (SqlObfuscator.OBFUSCATION_FAILED.equals(obfuscated)) {
                    log.warn("ExplainPlanQuery: obfuscation failed for member={} execId={} — record dropped",
                        member, execId);
                    continue;
                }

                String planHash = sha256Hex(planXml);
                ExplainPlanEvent.of(dbName, obfuscated, member, planXml, planHash)
                    .emit(otelLogger, timestamp);

                planCache.put(cacheKey, now + planCacheTtlMs);
                emitCount++;
            }
            log.debug("ExplainPlanQuery: emitted {} plan records (cache size={})", emitCount, planCache.size());

        } catch (Exception e) {
            log.warn("ExplainPlanQuery collection failed: {}", e.getMessage());
        }
    }

    private String fetchPlan(Connection conn, String execId, int member) {
        try (PreparedStatement ps = conn.prepareStatement(SQL_PLAN)) {
            ps.setQueryTimeout(queryTimeoutSeconds);
            ps.setString(1, execId);
            ps.setInt(2, member < 0 ? -2 : member);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    java.sql.Clob clob = rs.getClob("SECTION_XML");
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
            log.debug("ExplainPlanQuery: plan fetch failed for execId={} member={}: {}",
                execId, member, e.getMessage());
        }
        return null;
    }

    private void evictExpired(long now) {
        Iterator<Map.Entry<String, Long>> it = planCache.entrySet().iterator();
        while (it.hasNext()) {
            if (now >= it.next().getValue()) it.remove();
        }
    }

    static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            // SHA-256 is always available in the JVM — this branch is unreachable in practice.
            return "unknown";
        }
    }
}
