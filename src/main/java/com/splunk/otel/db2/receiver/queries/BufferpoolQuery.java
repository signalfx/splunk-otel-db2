package com.splunk.otel.db2.receiver.queries;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.function.Supplier;

public class BufferpoolQuery {
    private static final Logger log = LoggerFactory.getLogger(BufferpoolQuery.class);

    private static final String SQL =
        "SELECT BP_NAME, " +
        "POOL_DATA_L_READS, POOL_DATA_P_READS, " +
        "POOL_INDEX_L_READS, POOL_INDEX_P_READS, " +
        "CASE WHEN POOL_DATA_L_READS + POOL_INDEX_L_READS = 0 THEN 0 " +
        "ELSE (1.0 - (CAST(POOL_DATA_P_READS + POOL_INDEX_P_READS AS DOUBLE) / " +
        "             (POOL_DATA_L_READS + POOL_INDEX_L_READS))) * 100 " +
        "END AS HIT_RATIO " +
        "FROM TABLE(MON_GET_BUFFERPOOL(NULL, -2)) AS T";

    private final Supplier<Connection> connSupplier;
    private final String dbName;

    public BufferpoolQuery(Supplier<Connection> connSupplier, String dbName) {
        this.connSupplier = connSupplier;
        this.dbName = dbName;
    }

    public void register(Meter meter) {
        ObservableDoubleMeasurement hitRatio = meter.gaugeBuilder("db2.buffer_pool.hit_ratio")
            .setDescription("Buffer pool hit ratio percentage").setUnit("%").buildObserver();
        ObservableLongMeasurement reads = meter.gaugeBuilder("db2.buffer_pool.reads")
            .ofLongs().setDescription("Buffer pool logical reads").setUnit("{reads}").buildObserver();
        ObservableLongMeasurement physicalReads = meter.gaugeBuilder("db2.buffer_pool.physical_reads")
            .ofLongs().setDescription("Buffer pool physical reads").setUnit("{reads}").buildObserver();

        // single DB round-trip; db2.read_type=data|index differentiates logical and physical read dimensions
        meter.batchCallback(() -> {
            try {
                Connection conn = connSupplier.get();
                try (PreparedStatement ps = conn.prepareStatement(SQL);
                     ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String bpName = rs.getString("BP_NAME").trim();
                        Attributes baseAttrs = Attributes.of(
                            AttributeKey.stringKey("db.system.name"), "db2",
                            AttributeKey.stringKey("db.namespace"), dbName,
                            AttributeKey.stringKey("db2.buffer_pool"), bpName
                        );
                        Attributes dataAttrs = Attributes.of(
                            AttributeKey.stringKey("db.system.name"), "db2",
                            AttributeKey.stringKey("db.namespace"), dbName,
                            AttributeKey.stringKey("db2.buffer_pool"), bpName,
                            AttributeKey.stringKey("db2.read_type"), "data"
                        );
                        Attributes indexAttrs = Attributes.of(
                            AttributeKey.stringKey("db.system.name"), "db2",
                            AttributeKey.stringKey("db.namespace"), dbName,
                            AttributeKey.stringKey("db2.buffer_pool"), bpName,
                            AttributeKey.stringKey("db2.read_type"), "index"
                        );
                        hitRatio.record(rs.getDouble("HIT_RATIO"), baseAttrs);
                        reads.record(rs.getLong("POOL_DATA_L_READS"), dataAttrs);
                        reads.record(rs.getLong("POOL_INDEX_L_READS"), indexAttrs);
                        physicalReads.record(rs.getLong("POOL_DATA_P_READS"), dataAttrs);
                        physicalReads.record(rs.getLong("POOL_INDEX_P_READS"), indexAttrs);
                    }
                }
            } catch (Exception e) {
                log.error("Failed to query MON_GET_BUFFERPOOL: {}", e.getMessage());
            }
        }, hitRatio, reads, physicalReads);
    }
}
