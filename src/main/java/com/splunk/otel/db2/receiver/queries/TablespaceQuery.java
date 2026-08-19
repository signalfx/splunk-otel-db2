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

public class TablespaceQuery {
    private static final Logger log = LoggerFactory.getLogger(TablespaceQuery.class);

    // TBSP_PAGE_SIZE in bytes; *_TOTAL/USED/FREE_PAGES counts are in pages
    private static final String SQL =
        "SELECT MEMBER, TBSP_NAME, TBSP_TYPE, TBSP_STATE, TBSP_PAGE_SIZE, " +
        "TBSP_TOTAL_PAGES, TBSP_USED_PAGES, TBSP_FREE_PAGES " +
        "FROM TABLE(MON_GET_TABLESPACE(NULL, -2)) AS T";

    private final Supplier<Connection> connSupplier;
    private final String dbName;

    private ObservableLongMeasurement tablespaceSize;

    public TablespaceQuery(Supplier<Connection> connSupplier, String dbName, Meter meter) {
        this.connSupplier = connSupplier;
        this.dbName       = dbName;
        register(meter);
    }

    private void register(Meter meter) {
        tablespaceSize = meter.upDownCounterBuilder("db2.tablespace.size")
            .setDescription("Size of the tablespace in bytes, broken down by usage.")
            .setUnit("By")
            .buildObserver();

        meter.batchCallback(this::collect, tablespaceSize);
    }

    private void collect() {
        Connection conn = connSupplier.get();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(SQL)) {
            while (rs.next()) {
                String tbspName  = rs.getString("TBSP_NAME");
                String tbspType  = rs.getString("TBSP_TYPE");
                String tbspState = rs.getString("TBSP_STATE");
                int    member    = rs.getInt("MEMBER");
                long   pageSize  = rs.getLong("TBSP_PAGE_SIZE");

                long totalPages = rs.getLong("TBSP_TOTAL_PAGES");
                long usedPages  = rs.getLong("TBSP_USED_PAGES");
                long freePages  = rs.getLong("TBSP_FREE_PAGES");

                Attributes base = Attributes.builder()
                    .put("db.namespace",         dbName)
                    .put("db2.member",           member)
                    .put("db2.tablespace",       tbspName)
                    .put("db2.tablespace.type",  tbspType)
                    .put("db2.tablespace.state", tbspState)
                    .build();

                tablespaceSize.record(totalPages * pageSize,
                    Attributes.builder().putAll(base).put("db2.tablespace.usage", "capacity").build());
                tablespaceSize.record(usedPages * pageSize,
                    Attributes.builder().putAll(base).put("db2.tablespace.usage", "used").build());
                tablespaceSize.record(freePages * pageSize,
                    Attributes.builder().putAll(base).put("db2.tablespace.usage", "free").build());
            }
        } catch (Exception e) {
            log.warn("TablespaceQuery collection failed: {}", e.getMessage());
        }
    }
}
