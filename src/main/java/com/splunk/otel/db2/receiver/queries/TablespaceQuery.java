package com.splunk.otel.db2.receiver.queries;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import com.splunk.otel.db2.generated.Db2Metrics;
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

    // -2 = all members; MEMBER attribute differentiates per-member rows
    private static final String SQL =
        "SELECT TBSP_NAME, MEMBER, " +
        "TBSP_USED_PAGES * TBSP_PAGE_SIZE AS USED_BYTES, " +
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
        ObservableLongMeasurement size = Db2Metrics.tablespaceSize(meter);

        meter.batchCallback(() -> {
            try {
                Connection conn = connSupplier.get();
                try (PreparedStatement ps = conn.prepareStatement(SQL);
                     ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String tsName = rs.getString("TBSP_NAME").trim();
                        String member = String.valueOf(rs.getInt("MEMBER"));
                        Attributes base = Attributes.of(
                            AttributeKey.stringKey("db.system.name"), "db2",
                            AttributeKey.stringKey("db.namespace"), dbName,
                            AttributeKey.stringKey("db2.tablespace"), tsName,
                            AttributeKey.stringKey("db2.member"), member
                        );
                        Attributes usedAttrs     = Attributes.builder().putAll(base).put(AttributeKey.stringKey("db2.tablespace.state"), "used").build();
                        Attributes freeAttrs     = Attributes.builder().putAll(base).put(AttributeKey.stringKey("db2.tablespace.state"), "free").build();
                        Attributes capacityAttrs = Attributes.builder().putAll(base).put(AttributeKey.stringKey("db2.tablespace.state"), "capacity").build();
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
