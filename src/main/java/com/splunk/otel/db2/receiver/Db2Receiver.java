package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.receiver.queries.BufferpoolQuery;
import com.splunk.otel.db2.receiver.queries.DatabaseQuery;
import com.splunk.otel.db2.receiver.queries.QuerySampleQuery;
import com.splunk.otel.db2.receiver.queries.SortAndDirectIoQuery;
import com.splunk.otel.db2.receiver.queries.SqlActivityQuery;
import com.splunk.otel.db2.receiver.queries.TablespaceQuery;
import com.splunk.otel.db2.receiver.queries.TopQueryQuery;
import com.splunk.otel.db2.receiver.queries.TransactionLogQuery;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.metrics.Meter;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class Db2Receiver {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(Db2Receiver.class);

    private final Db2ReceiverConfig config;
    private final Meter meter;
    private final Logger otelLogger;
    private final String label; // e.g. "[prod] db2host:50000/TESTDB" — for log context (A3)

    // Each collector has its own connection so they never contend:
    //   metricsConnection     — DatabaseQuery, BufferpoolQuery, TablespaceQuery (OTel SDK reader thread)
    //   querySampleConnection — QuerySampleQuery (includes blocking detection) (db2-query-sampler thread)
    //   topQueryConnection    — TopQueryQuery (db2-top-query-collector thread)
    private final Db2Connection metricsConnection;
    private Db2Connection querySampleConnection;
    private Db2Connection topQueryConnection;

    private ScheduledExecutorService querySampleScheduler;
    private ScheduledExecutorService topQueryScheduler;

    private final AtomicBoolean stopped = new AtomicBoolean(false); // B3: idempotent stop

    public Db2Receiver(Db2ReceiverConfig config, Meter meter, Logger otelLogger) {
        this.config            = config;
        this.meter             = meter;
        this.otelLogger        = otelLogger;
        this.label             = buildLabel(config);
        this.metricsConnection = new Db2Connection(config, label);
    }

    // test constructor
    public Db2Receiver(Db2ReceiverConfig config, Meter meter, Logger otelLogger, Db2Connection metricsConnection) {
        this.config            = config;
        this.meter             = meter;
        this.otelLogger        = otelLogger;
        this.label             = buildLabel(config);
        this.metricsConnection = metricsConnection;
    }

    // backwards compat for tests that don't need logs
    public Db2Receiver(Db2ReceiverConfig config, Meter meter, Db2Connection metricsConnection) {
        this(config, meter, null, metricsConnection);
    }

    /**
     * Logs what the receiver actually read from config, so a support question about "is top query
     * on?" is answered by the agent's own log rather than by inferring from which events appear
     * downstream. Every collection is listed, including the inactive ones, each with the config key
     * that turned it off — silence about a disabled collection is what makes this hard to diagnose.
     */
    private void logCollectionPlan() {
        log.info("{}", buildCollectionPlan(config, label));
    }

    /** Pure so the plan can be asserted without starting a receiver or a database. */
    static String buildCollectionPlan(Db2ReceiverConfig config, String label) {
        boolean querySample = config.getEvents().getDbServerQuerySample().isEnabled();
        boolean topQuery    = config.getEvents().getDbServerTopQuery().isEnabled();
        int     explain     = config.getTopQueryCollection().getMaxExplainEachInterval();

        StringBuilder sb = new StringBuilder();
        sb.append("[").append(label).append("] Collection plan:");
        sb.append("\n  metrics                : ACTIVE    every ")
          .append(config.getCollectionIntervalDuration().getSeconds()).append("s");

        sb.append("\n  db.server.query_sample : ");
        if (querySample) {
            sb.append("ACTIVE    every ")
              .append(config.getQuerySampleCollection().getCollectionIntervalDuration().getSeconds())
              .append("s, max ")
              .append(config.getQuerySampleCollection().getMaxRowsPerQuery())
              .append(" rows/cycle");
        } else {
            sb.append("INACTIVE  (events.db.server.query_sample.enabled=false)");
        }

        // Blocking detection has no flag of its own; it rides on query sampling and is unavailable
        // on the snapshot path. Stating that here avoids the "why is blocking empty?" question.
        sb.append("\n  blocking detection     : ");
        if (!querySample) {
            sb.append("INACTIVE  (requires events.db.server.query_sample.enabled=true)");
        } else if (config.isUseSnapshotMonitor()) {
            sb.append("INACTIVE  (not available when use_snapshot_monitor=true)");
        } else {
            sb.append("ACTIVE    follows query_sample interval");
        }

        sb.append("\n  db.server.top_query    : ");
        if (topQuery) {
            sb.append("ACTIVE    every ")
              .append(config.getTopQueryCollection().getCollectionIntervalDuration().getSeconds())
              .append("s, top ")
              .append(config.getTopQueryCollection().getTopQueryCount())
              .append(" of max ")
              .append(config.getTopQueryCollection().getMaxQuerySampleCount())
              .append(" candidates");
        } else {
            sb.append("INACTIVE  (events.db.server.top_query.enabled=false)");
        }

        sb.append("\n  query plan (EXPLAIN)   : ");
        if (!topQuery) {
            sb.append("INACTIVE  (requires events.db.server.top_query.enabled=true)");
        } else if (explain <= 0) {
            sb.append("INACTIVE  (top_query_collection.max_explain_each_interval=0)");
        } else {
            sb.append("ACTIVE    max ").append(explain).append(" EXPLAIN/cycle");
        }

        sb.append("\n  monitor path           : ")
          .append(config.isUseSnapshotMonitor() ? "SNAP_GET_* (use_snapshot_monitor=true)"
                                               : "MON_CURRENT_SQL / MON_GET_*");
        return sb.toString();
    }

    private static String buildLabel(Db2ReceiverConfig config) {
        String name = config.getComponentName();
        return name.isEmpty()
            ? config.getHost() + ":" + config.getPort() + "/" + config.getDatabase()
            : name + "/" + config.getDatabase();
    }

    public void start() {
        log.info("[{}] Starting DB2 receiver", label);
        logCollectionPlan();

        Supplier<Connection> metricsConnSupplier = () -> {
            try {
                return metricsConnection.getOrConnect();
            } catch (SQLException e) {
                throw new RuntimeException("DB2 metrics connection failed: " + e.getMessage(), e);
            }
        };

        new DatabaseQuery(metricsConnSupplier, config.getDatabase(), meter);
        new BufferpoolQuery(metricsConnSupplier, config.getDatabase(), meter);
        new TablespaceQuery(metricsConnSupplier, config.getDatabase(), meter);
        new SqlActivityQuery(metricsConnSupplier, config.getDatabase(), meter, config.isUseSnapshotMonitor());
        new SortAndDirectIoQuery(metricsConnSupplier, config.getDatabase(), meter);
        new TransactionLogQuery(metricsConnSupplier, config.getDatabase(), meter);

        if (otelLogger != null && config.getEvents().getDbServerQuerySample().isEnabled()) {
            int db2Version = detectDb2Version(metricsConnSupplier);
            startQuerySampleScheduler(db2Version);
        }

        if (otelLogger != null && config.getEvents().getDbServerTopQuery().isEnabled()) {
            long topQueryIntervalSeconds = config.getTopQueryCollection().getCollectionIntervalDuration().getSeconds();
            startTopQueryScheduler(topQueryIntervalSeconds);
        }

        log.info("[{}] DB2 receiver started", label);
    }

    void startQuerySampleScheduler(int db2Version) {
        querySampleConnection = new Db2Connection(config, label);
        Supplier<Connection> querySampleConnSupplier = () -> {
            try {
                return querySampleConnection.getOrConnect();
            } catch (SQLException e) {
                throw new RuntimeException("[" + label + "] DB2 query-sample connection failed: " + e.getMessage(), e);
            }
        };
        long intervalSeconds           = config.getQuerySampleCollection().getCollectionIntervalDuration().getSeconds();
        int  querySampleTimeoutSeconds = (int) Math.max(5L, intervalSeconds / 2);

        // db2.query_plan.hash is only useful if something will populate db2.query_plan, which needs
        // top-query collection enabled and a positive explain budget.
        boolean planCollectionEnabled =
            config.getEvents().getDbServerTopQuery().isEnabled()
                && config.getTopQueryCollection().getMaxExplainEachInterval() > 0;

        QuerySampleQuery querySampleQuery = new QuerySampleQuery(
            querySampleConnSupplier,
            config.getDatabase(),
            otelLogger,
            config.getQuerySampleCollection().getMaxRowsPerQuery(),
            config.isUseSnapshotMonitor(),
            db2Version,
            querySampleTimeoutSeconds,
            planCollectionEnabled);

        querySampleScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "db2-query-sampler[" + label + "]");
            t.setDaemon(true);
            return t;
        });
        // Fixed delay, not fixed rate: at a fixed rate a collection that overruns its interval
        // makes every later run fire immediately with no gap, so a slow instance gets queried
        // continuously — exactly when it can least afford it. Top-query already uses fixed delay.
        querySampleScheduler.scheduleWithFixedDelay(
            () -> {
                try {
                    querySampleQuery.collect();
                } catch (Exception e) {
                    log.warn("[{}] Query sample collection error: {}", label, e.getMessage());
                }
            },
            0, intervalSeconds, TimeUnit.SECONDS
        );
    }


    // Package-private: allows integration tests to inject a short interval without violating the
    // production minimum. Production code always calls start() which enforces the 60s minimum
    // via getTopQueryIntervalDuration().
    public void startTopQueryScheduler(long intervalSeconds) {
        startTopQueryScheduler(intervalSeconds, null);
    }

    /**
     * Test-only overload: {@code seedDone} is counted down once the first (seed) collection
     * completes, giving callers a deterministic signal instead of a blind {@code Thread.sleep}.
     */
    public void startTopQueryScheduler(long intervalSeconds, CountDownLatch seedDone) {
        topQueryConnection = new Db2Connection(config, label);
        Supplier<Connection> topQueryConnSupplier = () -> {
            try {
                return topQueryConnection.getOrConnect();
            } catch (SQLException e) {
                throw new RuntimeException("[" + label + "] DB2 top-query connection failed: " + e.getMessage(), e);
            }
        };
        int maxExplain = config.getTopQueryCollection().getMaxExplainEachInterval();
        Integer planMaxPlans = maxExplain > 0 ? maxExplain : null; // 0 = plan disabled
        int planCacheSize = config.getTopQueryCollection().getQueryPlanCacheSize();
        long planCacheTtlMs = config.getTopQueryCollection().getQueryPlanCacheTtlDuration().toMillis();
        String explainSchema = config.getTopQueryCollection().getExplainSchema();
        TopQueryQuery topQueryQuery = new TopQueryQuery(
            topQueryConnSupplier,
            config.getDatabase(),
            otelLogger,
            config.getTopQueryCollection().getTopQueryCount(),
            config.getTopQueryCollection().getMaxQuerySampleCount(),
            intervalSeconds,
            planMaxPlans,
            planCacheSize,
            planCacheTtlMs,
            explainSchema
        );
        topQueryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "db2-top-query-collector[" + label + "]");
            t.setDaemon(true);
            return t;
        });
        final boolean[] firstRun = {true};
        topQueryScheduler.scheduleWithFixedDelay(
            () -> {
                boolean succeeded = false;
                try {
                    succeeded = topQueryQuery.collect();
                } catch (Exception e) {
                    log.warn("[{}] Top query collection error: {}", label, e.getMessage());
                }
                // Count down only after a successful seed — a failed scrape leaves no baseline,
                // so the workload would be captured by the next scrape and emitted as a zero delta.
                if (firstRun[0] && succeeded && seedDone != null) {
                    firstRun[0] = false;
                    seedDone.countDown();
                }
            },
            0, intervalSeconds, TimeUnit.SECONDS
        );
    }

    private int detectDb2Version(Supplier<Connection> connSupplier) {
        try {
            java.sql.DatabaseMetaData meta = connSupplier.get().getMetaData();
            int major = meta.getDatabaseMajorVersion();
            int minor = meta.getDatabaseMinorVersion();
            return major * 1000 + minor;
        } catch (Exception e) {
            // B1: make it clear which SQL path will be used and what to check if queries fail
            log.warn("[{}] Could not detect Db2 version ({}); defaulting to MON_CURRENT_SQL path. " +
                "If query-sample fails, verify EXECUTE permission on SYSPROC.MON_GET_CONNECTION.",
                label, e.getMessage());
            return 0;
        }
    }

    public void stop() {
        if (!stopped.compareAndSet(false, true)) return; // B3: idempotent
        // Db2Pipeline calls stopScheduler() and closeConnection() explicitly in the
        // correct order around SDK shutdown. This path is for callers (tests, direct use)
        // that want a single-call teardown.
        stopScheduler();
        closeConnection();
    }

    public void stopScheduler() {
        // B4: graceful drain — let the current collection cycle finish, then force-stop
        shutdownGracefully(querySampleScheduler,  "db2-query-sampler");
        shutdownGracefully(topQueryScheduler,     "db2-top-query-collector");
    }

    private void shutdownGracefully(ScheduledExecutorService scheduler, String name) {
        if (scheduler == null) return;
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("[{}] {} did not finish within 10s — forcing shutdown", label, name);
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public void closeConnection() {
        metricsConnection.close();
        if (querySampleConnection  != null) querySampleConnection.close();
        if (topQueryConnection     != null) topQueryConnection.close();
        log.info("[{}] DB2 receiver stopped", label);
    }
}
