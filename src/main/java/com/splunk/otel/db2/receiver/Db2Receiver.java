package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.receiver.queries.QuerySampleQuery;
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
    private final Db2Connection db2Connection;
    private ScheduledExecutorService scheduler;

    public Db2Receiver(Db2ReceiverConfig config, Meter meter, Logger otelLogger) {
        this.config        = config;
        this.meter         = meter;
        this.otelLogger    = otelLogger;
        this.db2Connection = new Db2Connection(config);
    }

    // test constructor
    public Db2Receiver(Db2ReceiverConfig config, Meter meter, Logger otelLogger, Db2Connection db2Connection) {
        this.config        = config;
        this.meter         = meter;
        this.otelLogger    = otelLogger;
        this.db2Connection = db2Connection;
    }

    // backwards compat for tests that don't need logs
    public Db2Receiver(Db2ReceiverConfig config, Meter meter, Db2Connection db2Connection) {
        this(config, meter, null, db2Connection);
    }

    public void start() {
        log.info("Starting DB2 receiver for database '{}' at {}:{}",
            config.getDatabase(), config.getHost(), config.getPort());

        Supplier<Connection> connSupplier = () -> {
            try {
                return db2Connection.getOrConnect();
            } catch (SQLException e) {
                throw new RuntimeException("DB2 connection failed: " + e.getMessage(), e);
            }
        };

        // metric queries will be registered here in subsequent PRs

        if (otelLogger != null) {
            QuerySampleQuery querySample = new QuerySampleQuery(
                connSupplier, config.getDatabase(), otelLogger,
                config.getMaxQuerySampleCount(), config.isUseSnapshotMonitor()
            );

            scheduler = Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "db2-log-collector");
                t.setDaemon(true);
                return t;
            });
            scheduler.scheduleAtFixedRate(querySample::collect, 0,
                config.getQuerySampleIntervalDuration().getSeconds(), TimeUnit.SECONDS);
        }

        log.info("DB2 receiver started");
    }

    public void stop() {
        if (scheduler != null) scheduler.shutdownNow();
        db2Connection.close();
        log.info("DB2 receiver stopped");
    }
}
