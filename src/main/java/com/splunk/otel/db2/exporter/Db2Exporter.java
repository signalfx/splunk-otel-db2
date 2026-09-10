package com.splunk.otel.db2.exporter;

import com.splunk.otel.db2.config.OtlpExporterConfig;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporterBuilder;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.sdk.common.export.RetryPolicy;
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

    // Retry policy that covers transient gRPC transport errors (e.g. "http2: client
    // connection lost" when the collector pod restarts). The exporter retries up to 5
    // times with exponential backoff; the custom predicate extends the default
    // UNAVAILABLE / RESOURCE_EXHAUSTED status-code handling to also retry on any
    // IOException so that a broken channel reconnects without operator intervention.
    private static final RetryPolicy RETRY_POLICY = RetryPolicy.builder()
        .setMaxAttempts(5)
        .setInitialBackoff(Duration.ofSeconds(1))
        .setMaxBackoff(Duration.ofSeconds(30))
        .setBackoffMultiplier(2.0)
        .setRetryExceptionPredicate(e -> e instanceof IOException)
        .build();

    public static MetricReader build(OtlpExporterConfig config, Duration interval) {
        log.info("Exporting metrics via OTLP to {}", config.getEndpoint());

        OtlpGrpcMetricExporterBuilder builder = OtlpGrpcMetricExporter.builder()
            .setEndpoint(config.getEndpoint())
            .setRetryPolicy(RETRY_POLICY);
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
            .setEndpoint(config.getEndpoint())
            .setRetryPolicy(RETRY_POLICY);
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
