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
        meter.gaugeBuilder("db2.buffer_pool.hit_ratio")
            .setDescription("Buffer pool hit ratio percentage")
            .setUnit("%")
            .buildWithCallback(obs -> {
                try {
                    Connection conn = connSupplier.get();
                    try (PreparedStatement ps = conn.prepareStatement(SQL);
                         ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            String bpName = rs.getString("BP_NAME").trim();
                            Attributes attrs = Attributes.of(
                                AttributeKey.stringKey("db.system"), "db2",
                                AttributeKey.stringKey("db.name"), dbName,
                                AttributeKey.stringKey("db2.buffer_pool"), bpName
                            );
                            obs.record(rs.getDouble("HIT_RATIO"), attrs);
                        }
                    }
                } catch (Exception e) {
                    log.error("Failed to query MON_GET_BUFFERPOOL: {}", e.getMessage());
                }
            });

        meter.gaugeBuilder("db2.buffer_pool.data_reads")
            .ofLongs()
            .setDescription("Buffer pool data logical reads")
            .setUnit("{reads}")
            .buildWithCallback(obs -> {
                try {
                    Connection conn = connSupplier.get();
                    try (PreparedStatement ps = conn.prepareStatement(SQL);
                         ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            String bpName = rs.getString("BP_NAME").trim();
                            Attributes attrs = Attributes.of(
                                AttributeKey.stringKey("db.system"), "db2",
                                AttributeKey.stringKey("db.name"), dbName,
                                AttributeKey.stringKey("db2.buffer_pool"), bpName
                            );
                            obs.record(rs.getLong("POOL_DATA_L_READS"), attrs);
                        }
                    }
                } catch (Exception e) {
                    log.error("Failed to query MON_GET_BUFFERPOOL data_reads: {}", e.getMessage());
                }
            });
    }
}
