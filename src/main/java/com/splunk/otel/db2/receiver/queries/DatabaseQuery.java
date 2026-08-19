package com.splunk.otel.db2.receiver.queries;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.Meter;
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
        "SELECT APPLS_CUR_CONS, DEADLOCKS, LOCK_WAITS, LOCK_WAIT_TIME, " +
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
            AttributeKey.stringKey("db.system"), "db2",
            AttributeKey.stringKey("db.name"), dbName
        );

        meter.gaugeBuilder("db.db2.connections")
            .ofLongs()
            .setDescription("Number of active connections to the database")
            .setUnit("{connections}")
            .buildWithCallback(obs -> record(obs, attrs, "APPLS_CUR_CONS"));

        meter.gaugeBuilder("db.db2.deadlocks")
            .ofLongs()
            .setDescription("Total deadlocks detected")
            .setUnit("{deadlocks}")
            .buildWithCallback(obs -> record(obs, attrs, "DEADLOCKS"));

        meter.gaugeBuilder("db.db2.lock_waits")
            .ofLongs()
            .setDescription("Total lock waits")
            .setUnit("{waits}")
            .buildWithCallback(obs -> record(obs, attrs, "LOCK_WAITS"));

        meter.gaugeBuilder("db.db2.lock_wait_time")
            .ofLongs()
            .setDescription("Total time spent waiting for locks in milliseconds")
            .setUnit("ms")
            .buildWithCallback(obs -> record(obs, attrs, "LOCK_WAIT_TIME"));

        meter.gaugeBuilder("db.db2.lock_timeouts")
            .ofLongs()
            .setDescription("Total lock timeouts")
            .setUnit("{timeouts}")
            .buildWithCallback(obs -> record(obs, attrs, "LOCK_TIMEOUTS"));
    }

    private void record(ObservableLongMeasurement obs, Attributes attrs, String column) {
        try {
            Connection conn = connSupplier.get();
            try (PreparedStatement ps = conn.prepareStatement(SQL);
                 ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    obs.record(rs.getLong(column), attrs);
                }
            }
        } catch (Exception e) {
            log.error("Failed to query MON_GET_DATABASE for {}: {}", column, e.getMessage());
        }
    }
}
