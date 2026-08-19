package com.splunk.otel.db2.receiver.queries;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.function.Supplier;

public class DatabaseQuery {
    private static final Logger log = LoggerFactory.getLogger(DatabaseQuery.class);

    private static final String SQL =
        "SELECT APPLS_CUR_CONS, DEADLOCKS, LOCK_WAITS, " +
        "LOCK_WAIT_TIME / 1000.0 AS LOCK_WAIT_TIME_S, " +
        "LOCK_TIMEOUTS " +
        "FROM TABLE(MON_GET_DATABASE(-2)) AS T";

    private final Supplier<Connection> connSupplier;
    private final String dbName;

    public DatabaseQuery(Supplier<Connection> connSupplier, String dbName) {
        this.connSupplier = connSupplier;
        this.dbName = dbName;
    }

    public void register(Meter meter) {
        Attributes attrs = Attributes.of(
            AttributeKey.stringKey("db.system.name"), "db2",
            AttributeKey.stringKey("db.namespace"), dbName
        );

        ObservableLongMeasurement connections = meter.gaugeBuilder("db2.user.connection.count")
            .ofLongs().setDescription("Number of active connections to the database").setUnit("{connections}").buildObserver();
        ObservableLongMeasurement deadlocks = meter.gaugeBuilder("db2.deadlocks")
            .ofLongs().setDescription("Total deadlocks detected").setUnit("{deadlocks}").buildObserver();
        ObservableLongMeasurement lockWaits = meter.gaugeBuilder("db2.lock.waits")
            .ofLongs().setDescription("Total lock waits").setUnit("{waits}").buildObserver();
        ObservableDoubleMeasurement lockWaitTime = meter.gaugeBuilder("db2.lock.wait.time")
            .setDescription("Total time spent waiting for locks").setUnit("s").buildObserver();
        ObservableLongMeasurement lockTimeouts = meter.gaugeBuilder("db2.lock.timeouts")
            .ofLongs().setDescription("Total lock timeouts").setUnit("{timeouts}").buildObserver();

        // single DB round-trip for all five gauges
        meter.batchCallback(() -> {
            try {
                Connection conn = connSupplier.get();
                try (PreparedStatement ps = conn.prepareStatement(SQL);
                     ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        connections.record(rs.getLong("APPLS_CUR_CONS"), attrs);
                        deadlocks.record(rs.getLong("DEADLOCKS"), attrs);
                        lockWaits.record(rs.getLong("LOCK_WAITS"), attrs);
                        lockWaitTime.record(rs.getDouble("LOCK_WAIT_TIME_S"), attrs);
                        lockTimeouts.record(rs.getLong("LOCK_TIMEOUTS"), attrs);
                    }
                }
            } catch (Exception e) {
                log.error("Failed to query MON_GET_DATABASE: {}", e.getMessage());
            }
        }, connections, deadlocks, lockWaits, lockWaitTime, lockTimeouts);
    }
}
