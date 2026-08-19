package com.splunk.otel.db2.receiver.queries;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.function.Supplier;

public class TablespaceQuery {
    private static final Logger log = LoggerFactory.getLogger(TablespaceQuery.class);

    private static final String SQL =
        "SELECT TBSP_NAME, TBSP_USED_PAGES * TBSP_PAGE_SIZE AS USED_BYTES, " +
        "TBSP_FREE_PAGES * TBSP_PAGE_SIZE AS FREE_BYTES, " +
        "TBSP_TOTAL_PAGES * TBSP_PAGE_SIZE AS TOTAL_BYTES " +
        "FROM TABLE(MON_GET_TABLESPACE(NULL, -2)) AS T";

    private final Supplier<Connection> connSupplier;
    private final String dbName;

    public TablespaceQuery(Supplier<Connection> connSupplier, String dbName) {
        this.connSupplier = connSupplier;
        this.dbName = dbName;
    }

    public void register(Meter meter) {
        meter.gaugeBuilder("db.db2.tablespace.used")
            .ofLongs()
            .setDescription("Tablespace used bytes")
            .setUnit("By")
            .buildWithCallback(obs -> query(obs, null, "USED_BYTES"));

        meter.gaugeBuilder("db.db2.tablespace.free")
            .ofLongs()
            .setDescription("Tablespace free bytes")
            .setUnit("By")
            .buildWithCallback(obs -> query(obs, null, "FREE_BYTES"));
    }

    private void query(io.opentelemetry.api.metrics.ObservableLongMeasurement obs,
                       Object unused, String column) {
        try {
            Connection conn = connSupplier.get();
            try (PreparedStatement ps = conn.prepareStatement(SQL);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tsName = rs.getString("TBSP_NAME").trim();
                    Attributes attrs = Attributes.of(
                        AttributeKey.stringKey("db.system"), "db2",
                        AttributeKey.stringKey("db.name"), dbName,
                        AttributeKey.stringKey("db.db2.tablespace"), tsName
                    );
                    obs.record(rs.getLong(column), attrs);
                }
            }
        } catch (Exception e) {
            log.error("Failed to query MON_GET_TABLESPACE for {}: {}", column, e.getMessage());
        }
    }
}
