package com.splunk.otel.db2.exporter;

import com.splunk.otel.db2.config.OtlpExporterConfig;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class Db2Exporter {
    private static final Logger log = LoggerFactory.getLogger(Db2Exporter.class);

    public static MetricReader build(OtlpExporterConfig config, Duration interval) {
        log.info("Exporting metrics via OTLP to {}", config.getEndpoint());

        MetricExporter exporter = OtlpGrpcMetricExporter.builder()
            .setEndpoint(config.getEndpoint())
            .build();

        return PeriodicMetricReader.builder(exporter)
            .setInterval(interval)
            .build();
    }
}
