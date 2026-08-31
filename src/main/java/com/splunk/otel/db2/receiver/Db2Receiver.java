package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class Db2Receiver {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(Db2Receiver.class);

    private final Db2ReceiverConfig config;
    private final Meter meter;
    private final Logger otelLogger;

    // Each collector has its own connection so they never contend:
    //   metricsConnection     — DatabaseQuery, BufferpoolQuery, TablespaceQuery (OTel SDK reader thread)
    //   querySampleConnection — QuerySampleQuery (db2-query-sampler thread)
    //   topQueryConnection    — TopQueryQuery (db2-top-query-collector thread)
    private final Db2Connection metricsConnection;
    private Db2Connection querySampleConnection;
    private Db2Connection topQueryConnection;

    private ScheduledExecutorService querySampleScheduler;
    private ScheduledExecutorService topQueryScheduler;

    public Db2Receiver(Db2ReceiverConfig config, Meter meter, Logger otelLogger) {
        this.config            = config;
        this.meter             = meter;
        this.otelLogger        = otelLogger;
        this.metricsConnection = new Db2Connection(config);
    }

    // test constructor
    public Db2Receiver(Db2ReceiverConfig config, Meter meter, Logger otelLogger, Db2Connection metricsConnection) {
        this.config            = config;
        this.meter             = meter;
        this.otelLogger        = otelLogger;
        this.metricsConnection = metricsConnection;
    }

    // backwards compat for tests that don't need logs
    public Db2Receiver(Db2ReceiverConfig config, Meter meter, Db2Connection metricsConnection) {
        this(config, meter, null, metricsConnection);
    }

    public void start() {
        log.info("Starting DB2 receiver for database '{}' at {}:{}",
            config.getDatabase(), config.getHost(), config.getPort());

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

        if (otelLogger != null && config.isTopQueryEnabled()) {
            long topQueryIntervalSeconds = config.getTopQueryIntervalDuration().getSeconds();
            startTopQueryScheduler(topQueryIntervalSeconds);
        }

        log.info("DB2 receiver started");
    }

    void startQuerySampleScheduler(int db2Version) {
        querySampleConnection = new Db2Connection(config);
        Supplier<Connection> querySampleConnSupplier = () -> {
            try {
                return querySampleConnection.getOrConnect();
            } catch (SQLException e) {
                throw new RuntimeException("DB2 query-sample connection failed: " + e.getMessage(), e);
            }
        };
        QuerySampleQuery querySampleQuery = new QuerySampleQuery(
            querySampleConnSupplier,
            config.getDatabase(),
            otelLogger,
            config.getMaxQuerySampleCount(),
            config.isUseSnapshotMonitor(),
            db2Version
        );
        querySampleScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "db2-query-sampler");
            t.setDaemon(true);
            return t;
        });
        long intervalSeconds = config.getQuerySampleIntervalDuration().getSeconds();
        querySampleScheduler.scheduleAtFixedRate(
            () -> {
                try {
                    querySampleQuery.collect();
                } catch (Exception e) {
                    log.warn("Query sample collection error: {}", e.getMessage());
                }
            },
            0, intervalSeconds, TimeUnit.SECONDS
        );
    }

    // Package-private: allows integration tests to inject a short interval without violating the
    // production minimum. Production code always calls start() which enforces the 60s minimum
    // via getTopQueryIntervalDuration().
    void startTopQueryScheduler(long intervalSeconds) {
        topQueryConnection = new Db2Connection(config);
        Supplier<Connection> topQueryConnSupplier = () -> {
            try {
                return topQueryConnection.getOrConnect();
            } catch (SQLException e) {
                throw new RuntimeException("DB2 top-query connection failed: " + e.getMessage(), e);
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
            Thread t = new Thread(r, "db2-top-query-collector");
            t.setDaemon(true);
            return t;
        });
        topQueryScheduler.scheduleWithFixedDelay(
            () -> {
                try {
                    topQueryQuery.collect();
                } catch (Exception e) {
                    log.warn("Top query collection error: {}", e.getMessage());
                }
            },
            0, intervalSeconds, TimeUnit.SECONDS
        );
    }

    private static int detectDb2Version(Supplier<Connection> connSupplier) {
        try {
            java.sql.DatabaseMetaData meta = connSupplier.get().getMetaData();
            int major = meta.getDatabaseMajorVersion();
            int minor = meta.getDatabaseMinorVersion();
            return major * 1000 + minor;
        } catch (Exception e) {
            log.warn("Could not detect Db2 version, defaulting to base SQL path: {}", e.getMessage());
            return 0;
        }
    }

    public void stop() {
        stopScheduler();
        closeConnection();
    }

    public void stopScheduler() {
        if (querySampleScheduler != null) querySampleScheduler.shutdownNow();
        if (topQueryScheduler    != null) topQueryScheduler.shutdownNow();
    }

    public void closeConnection() {
        metricsConnection.close();
        if (querySampleConnection != null) querySampleConnection.close();
        if (topQueryConnection    != null) topQueryConnection.close();
        log.info("DB2 receiver stopped");
    }
}
