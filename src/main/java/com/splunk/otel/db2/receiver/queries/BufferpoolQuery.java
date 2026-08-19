package com.splunk.otel.db2.receiver.queries;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.function.Supplier;

public class BufferpoolQuery {
    private static final Logger log = LoggerFactory.getLogger(BufferpoolQuery.class);

    private static final String SQL =
        "SELECT MEMBER, BP_NAME, " +
        "POOL_DATA_L_READS, POOL_DATA_P_READS, " +
        "POOL_INDEX_L_READS, POOL_INDEX_P_READS, " +
        "POOL_XDA_L_READS, POOL_XDA_P_READS, " +
        "POOL_DATA_WRITES, POOL_INDEX_WRITES, " +
        "POOL_READ_TIME, POOL_WRITE_TIME " +
        "FROM TABLE(MON_GET_BUFFERPOOL(NULL, -2)) AS T";

    private final Supplier<Connection> connSupplier;
    private final String dbName;

    private ObservableLongMeasurement logicalReads;
    private ObservableLongMeasurement physicalReads;
    private ObservableLongMeasurement writes;
    private ObservableLongMeasurement readTime;
    private ObservableLongMeasurement writeTime;

    public BufferpoolQuery(Supplier<Connection> connSupplier, String dbName, Meter meter) {
        this.connSupplier = connSupplier;
        this.dbName       = dbName;
        register(meter);
    }

    private void register(Meter meter) {
        logicalReads = meter.counterBuilder("db2.buffer_pool.logical_reads")
            .setDescription("Number of logical read requests for pages in the buffer pool.")
            .setUnit("{read}")
            .buildObserver();

        physicalReads = meter.counterBuilder("db2.buffer_pool.physical_reads")
            .setDescription("Number of physical read requests for pages from disk into the buffer pool.")
            .setUnit("{read}")
            .buildObserver();

        writes = meter.counterBuilder("db2.buffer_pool.writes")
            .setDescription("Number of times buffer pool pages were written to disk.")
            .setUnit("{write}")
            .buildObserver();

        readTime = meter.counterBuilder("db2.buffer_pool.read_time")
            .setDescription("Total elapsed time in milliseconds spent reading from disk into the buffer pool.")
            .setUnit("ms")
            .buildObserver();

        writeTime = meter.counterBuilder("db2.buffer_pool.write_time")
            .setDescription("Total elapsed time in milliseconds spent writing from the buffer pool to disk.")
            .setUnit("ms")
            .buildObserver();

        meter.batchCallback(this::collect, logicalReads, physicalReads, writes, readTime, writeTime);
    }

    private void collect() {
        try (Connection conn = connSupplier.get();
             Statement st   = conn.createStatement();
             ResultSet rs   = st.executeQuery(SQL)) {
            while (rs.next()) {
                String bpName = rs.getString("BP_NAME");
                int    member = rs.getInt("MEMBER");

                Attributes dataAttrs = Attributes.builder()
                    .put("db.name",        dbName)
                    .put("db2.member",     member)
                    .put("db2.bufferpool", bpName)
                    .put("db2.read_type",  "data")
                    .build();
                Attributes indexAttrs = Attributes.builder()
                    .put("db.name",        dbName)
                    .put("db2.member",     member)
                    .put("db2.bufferpool", bpName)
                    .put("db2.read_type",  "index")
                    .build();
                Attributes xdaAttrs = Attributes.builder()
                    .put("db.name",        dbName)
                    .put("db2.member",     member)
                    .put("db2.bufferpool", bpName)
                    .put("db2.read_type",  "xda")
                    .build();
                Attributes bpAttrs = Attributes.builder()
                    .put("db.name",        dbName)
                    .put("db2.member",     member)
                    .put("db2.bufferpool", bpName)
                    .build();

                logicalReads .record(rs.getLong("POOL_DATA_L_READS"),  dataAttrs);
                logicalReads .record(rs.getLong("POOL_INDEX_L_READS"), indexAttrs);
                logicalReads .record(rs.getLong("POOL_XDA_L_READS"),   xdaAttrs);

                physicalReads.record(rs.getLong("POOL_DATA_P_READS"),  dataAttrs);
                physicalReads.record(rs.getLong("POOL_INDEX_P_READS"), indexAttrs);
                physicalReads.record(rs.getLong("POOL_XDA_P_READS"),   xdaAttrs);

                writes   .record(rs.getLong("POOL_DATA_WRITES") + rs.getLong("POOL_INDEX_WRITES"), bpAttrs);
                readTime .record(rs.getLong("POOL_READ_TIME"),  bpAttrs);
                writeTime.record(rs.getLong("POOL_WRITE_TIME"), bpAttrs);
            }
        } catch (Exception e) {
            log.warn("BufferpoolQuery collection failed: {}", e.getMessage());
        }
    }
}
