package com.splunk.otel.db2.receiver.queries;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
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
        ObservableLongMeasurement size = meter.gaugeBuilder("db2.tablespace.size")
            .ofLongs().setDescription("Tablespace size in bytes").setUnit("By").buildObserver();

        // single DB round-trip; db2.tablespace.state=used|free|capacity differentiates size dimensions
        meter.batchCallback(() -> {
            try {
                Connection conn = connSupplier.get();
                try (PreparedStatement ps = conn.prepareStatement(SQL);
                     ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String tsName = rs.getString("TBSP_NAME").trim();
                        Attributes usedAttrs = Attributes.of(
                            AttributeKey.stringKey("db.system.name"), "db2",
                            AttributeKey.stringKey("db.namespace"), dbName,
                            AttributeKey.stringKey("db2.tablespace"), tsName,
                            AttributeKey.stringKey("db2.tablespace.state"), "used"
                        );
                        Attributes freeAttrs = Attributes.of(
                            AttributeKey.stringKey("db.system.name"), "db2",
                            AttributeKey.stringKey("db.namespace"), dbName,
                            AttributeKey.stringKey("db2.tablespace"), tsName,
                            AttributeKey.stringKey("db2.tablespace.state"), "free"
                        );
                        Attributes capacityAttrs = Attributes.of(
                            AttributeKey.stringKey("db.system.name"), "db2",
                            AttributeKey.stringKey("db.namespace"), dbName,
                            AttributeKey.stringKey("db2.tablespace"), tsName,
                            AttributeKey.stringKey("db2.tablespace.state"), "capacity"
                        );
                        size.record(rs.getLong("USED_BYTES"), usedAttrs);
                        size.record(rs.getLong("FREE_BYTES"), freeAttrs);
                        size.record(rs.getLong("TOTAL_BYTES"), capacityAttrs);
                    }
                }
            } catch (Exception e) {
                log.error("Failed to query MON_GET_TABLESPACE: {}", e.getMessage());
            }
        }, size);
    }
}
