package com.splunk.otel.db2.receiver.queries;

import io.opentelemetry.api.logs.Logger;

import java.sql.Connection;
import java.util.function.Supplier;

// Placeholder — implemented in a future PR
public class TopQueryQuery {

    public TopQueryQuery(Supplier<Connection> connSupplier, String dbName,
                         Logger otelLogger, int topQueryCount) {
    }

    public void collect() {
        // no-op
    }
}
