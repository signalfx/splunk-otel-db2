package com.splunk.otel.db2.exporter;

import com.splunk.otel.db2.config.OtlpExporterConfig;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporterBuilder;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;

public class Db2Exporter {
    private static final Logger log = LoggerFactory.getLogger(Db2Exporter.class);

    public static MetricReader build(OtlpExporterConfig config, Duration interval) {
        log.info("Exporting metrics via OTLP to {}", config.getEndpoint());

        OtlpGrpcMetricExporterBuilder builder = OtlpGrpcMetricExporter.builder()
            .setEndpoint(config.getEndpoint());
        for (Map.Entry<String, String> h : config.getHeaders().entrySet()) {
            builder.addHeader(h.getKey(), h.getValue());
        }
        if ("gzip".equalsIgnoreCase(config.getCompression())) {
            builder.setCompression("gzip");
        }
        applyTls(config.getTls(), builder);
        MetricExporter exporter = builder.build();

        return PeriodicMetricReader.builder(exporter)
            .setInterval(interval)
            .build();
    }

    public static OtlpGrpcLogRecordExporter buildLogExporter(OtlpExporterConfig config) {
        OtlpGrpcLogRecordExporterBuilder builder = OtlpGrpcLogRecordExporter.builder()
            .setEndpoint(config.getEndpoint());
        for (Map.Entry<String, String> h : config.getHeaders().entrySet()) {
            builder.addHeader(h.getKey(), h.getValue());
        }
        if ("gzip".equalsIgnoreCase(config.getCompression())) {
            builder.setCompression("gzip");
        }
        applyTls(config.getTls(), builder);
        return builder.build();
    }

    // ── TLS helpers ──────────────────────────────────────────────────────────

    private static void applyTls(OtlpExporterConfig.TlsConfig tls,
                                  OtlpGrpcMetricExporterBuilder builder) {
        if (tls == null) return;
        if (!tls.getCaFile().isEmpty()) {
            builder.setTrustedCertificates(readFile(tls.getCaFile(), "ca_file"));
        }
        if (!tls.getCertFile().isEmpty() && !tls.getKeyFile().isEmpty()) {
            builder.setClientTls(
                readFile(tls.getCertFile(), "cert_file"),
                readFile(tls.getKeyFile(),  "key_file")
            );
        }
    }

    private static void applyTls(OtlpExporterConfig.TlsConfig tls,
                                  OtlpGrpcLogRecordExporterBuilder builder) {
        if (tls == null) return;
        if (!tls.getCaFile().isEmpty()) {
            builder.setTrustedCertificates(readFile(tls.getCaFile(), "ca_file"));
        }
        if (!tls.getCertFile().isEmpty() && !tls.getKeyFile().isEmpty()) {
            builder.setClientTls(
                readFile(tls.getCertFile(), "cert_file"),
                readFile(tls.getKeyFile(),  "key_file")
            );
        }
    }

    private static byte[] readFile(String path, String configKey) {
        try {
            return Files.readAllBytes(Paths.get(path));
        } catch (IOException e) {
            throw new IllegalStateException(
                "exporters.otlp.tls." + configKey + ": cannot read '" + path + "': " + e.getMessage(), e);
        }
    }
}
