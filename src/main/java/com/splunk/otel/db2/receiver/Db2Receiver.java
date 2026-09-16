package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.receiver.queries.BlockingSessionQuery;
import com.splunk.otel.db2.receiver.queries.BufferpoolQuery;
import com.splunk.otel.db2.receiver.queries.DatabaseQuery;
import com.splunk.otel.db2.receiver.queries.QuerySampleQuery;
import com.splunk.otel.db2.receiver.queries.TablespaceQuery;
import com.splunk.otel.db2.receiver.queries.TopQueryQuery;
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
    //   metricsConnection         — DatabaseQuery, BufferpoolQuery, TablespaceQuery (OTel SDK reader thread)
    //   querySampleConnection     — QuerySampleQuery + co-located BlockingSessionQuery (db2-query-sampler thread)
    //   blockingSessionConnection — BlockingSessionQuery when running standalone (db2-blocking-session thread)
    //   topQueryConnection        — TopQueryQuery (db2-top-query-collector thread)
    private final Db2Connection metricsConnection;
    private Db2Connection querySampleConnection;
    private Db2Connection blockingSessionConnection;
    private Db2Connection topQueryConnection;

    private ScheduledExecutorService querySampleScheduler;
    private ScheduledExecutorService blockingSessionScheduler;
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

    private static String buildLabel(Db2ReceiverConfig config) {
        String name = config.getComponentName();
        return name.isEmpty()
            ? config.getHost() + ":" + config.getPort() + "/" + config.getDatabase()
            : name + "/" + config.getDatabase();
    }

    public void start() {
        log.info("[{}] Starting DB2 receiver", label);

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

        if (otelLogger != null && config.isQuerySampleEnabled()) {
            int db2Version = detectDb2Version(metricsConnSupplier);
            startQuerySampleScheduler(db2Version);
        }

        // Blocking session runs co-located with query sample when both are enabled.
        // When only blocking_session_enabled is set (query_sample_enabled=false), it
        // gets its own dedicated connection and scheduler.
        if (otelLogger != null && config.isBlockingSessionEnabled()
                && !config.isUseSnapshotMonitor()
                && !config.isQuerySampleEnabled()) {
            startBlockingSessionScheduler();
        }

        if (otelLogger != null && config.isTopQueryEnabled()) {
            long topQueryIntervalSeconds = config.getTopQueryIntervalDuration().getSeconds();
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
        long intervalSeconds           = config.getQuerySampleIntervalDuration().getSeconds();
        int  querySampleTimeoutSeconds = (int) Math.max(5L, intervalSeconds / 2);

        QuerySampleQuery querySampleQuery = new QuerySampleQuery(
            querySampleConnSupplier,
            config.getDatabase(),
            otelLogger,
            config.getMaxQuerySampleCount(),
            config.isUseSnapshotMonitor(),
            db2Version,
            querySampleTimeoutSeconds);

        // Co-locate blocking session on the query-sample scheduler when both are enabled
        BlockingSessionQuery blockingSessionQuery =
            (config.isBlockingSessionEnabled() && !config.isUseSnapshotMonitor())
            ? new BlockingSessionQuery(
                querySampleConnSupplier,
                config.getDatabase(),
                otelLogger,
                config.getBlockingSessionIntervalDuration().getSeconds())
            : null;

        querySampleScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "db2-query-sampler[" + label + "]");
            t.setDaemon(true);
            return t;
        });
        querySampleScheduler.scheduleAtFixedRate(
            () -> {
                try {
                    querySampleQuery.collect();
                } catch (Exception e) {
                    log.warn("[{}] Query sample collection error: {}", label, e.getMessage());
                }
                if (blockingSessionQuery != null) {
                    try {
                        blockingSessionQuery.collect();
                    } catch (Exception e) {
                        log.warn("[{}] Blocking session collection error: {}", label, e.getMessage());
                    }
                }
            },
            0, intervalSeconds, TimeUnit.SECONDS
        );
    }

    void startBlockingSessionScheduler() {
        blockingSessionConnection = new Db2Connection(config, label);
        Supplier<Connection> connSupplier = () -> {
            try {
                return blockingSessionConnection.getOrConnect();
            } catch (SQLException e) {
                throw new RuntimeException("[" + label + "] DB2 blocking-session connection failed: " + e.getMessage(), e);
            }
        };
        long intervalSeconds = config.getBlockingSessionIntervalDuration().getSeconds();
        BlockingSessionQuery blockingSessionQuery = new BlockingSessionQuery(
            connSupplier, config.getDatabase(), otelLogger, intervalSeconds);

        blockingSessionScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "db2-blocking-session[" + label + "]");
            t.setDaemon(true);
            return t;
        });
        blockingSessionScheduler.scheduleAtFixedRate(
            () -> {
                try {
                    blockingSessionQuery.collect();
                } catch (Exception e) {
                    log.warn("[{}] Blocking session collection error: {}", label, e.getMessage());
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
        TopQueryQuery topQueryQuery = new TopQueryQuery(
            topQueryConnSupplier,
            config.getDatabase(),
            otelLogger,
            config.getTopQueryCount(),
            intervalSeconds
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
        shutdownGracefully(querySampleScheduler,     "db2-query-sampler");
        shutdownGracefully(blockingSessionScheduler, "db2-blocking-session");
        shutdownGracefully(topQueryScheduler,        "db2-top-query-collector");
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
        if (querySampleConnection     != null) querySampleConnection.close();
        if (blockingSessionConnection != null) blockingSessionConnection.close();
        if (topQueryConnection        != null) topQueryConnection.close();
        log.info("[{}] DB2 receiver stopped", label);
    }
}
