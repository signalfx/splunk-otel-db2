package com.splunk.otel.db2.pipeline;

import com.splunk.otel.db2.config.Db2Config;
import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.config.OtlpExporterConfig;
import com.splunk.otel.db2.exporter.Db2Exporter;
import com.splunk.otel.db2.processor.Db2Processor;
import com.splunk.otel.db2.receiver.Db2Receiver;
import com.splunk.otel.db2.util.HostNormalizer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.resources.Resource;
import org.slf4j.LoggerFactory;

public class Db2Pipeline {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(Db2Pipeline.class);

    private final SdkMeterProvider meterProvider;
    private final SdkLoggerProvider loggerProvider;
    private final Db2Receiver receiver;
    private final String instanceId;

    public Db2Pipeline(Db2ReceiverConfig rc, OtlpExporterConfig otlpConfig) {
        String name = rc.getComponentName().isEmpty() ? "" : "[" + rc.getComponentName() + "] ";
        String serverAddress = HostNormalizer.resolveServerAddress(rc.getHost());
        this.instanceId = name + serverAddress + ":" + rc.getPort() + "/" + rc.getDatabase();
        Resource resource = buildResource(rc);

        MetricReader reader = Db2Exporter.build(otlpConfig, rc.getCollectionIntervalDuration());
        this.meterProvider  = buildMeterProvider(reader, resource);
        // Each pipeline owns its own exporter so that loggerProvider.shutdown() only drains
        // this pipeline's queue — stopping one pipeline cannot close another's export channel.
        OtlpGrpcLogRecordExporter logExporter = buildLogExporter(otlpConfig);
        this.loggerProvider = buildLoggerProvider(logExporter, resource);

        Meter  meter      = meterProvider.get("splunk-otel-db2");
        Logger otelLogger = loggerProvider.get("github.com/signalfx/splunk-otel-db2");
        this.receiver = new Db2Receiver(rc, meter, otelLogger);
    }

    // test constructor — kept for integration tests that inject a custom MetricReader
    public Db2Pipeline(Db2Config config, MetricReader testReader) {
        Db2ReceiverConfig rc = config.getReceivers().get(0);
        this.instanceId = rc.getHost() + ":" + rc.getPort() + "/" + rc.getDatabase();
        Resource resource   = buildResource(rc);
        this.meterProvider  = buildMeterProvider(testReader, resource);
        this.loggerProvider = null;

        Meter meter = meterProvider.get("splunk-otel-db2");
        this.receiver = new Db2Receiver(rc, meter, (Logger) null);
    }

    private Resource buildResource(Db2ReceiverConfig rc) {
        String serverAddress = HostNormalizer.resolveServerAddress(rc.getHost());
        return Resource.getDefault().merge(Resource.create(Attributes.of(
            AttributeKey.stringKey("service.name"),        "splunk-otel-db2",
            AttributeKey.stringKey("service.instance.id"), serverAddress + ":" + rc.getPort() + "/" + rc.getDatabase(),
            AttributeKey.stringKey("server.address"),      serverAddress,
            AttributeKey.longKey("server.port"),           (long) rc.getPort(),
            AttributeKey.stringKey("db.system.name"),      "ibm.db2",
            AttributeKey.stringKey("db.namespace"),        rc.getDatabase()
        )));
    }

    private SdkMeterProvider buildMeterProvider(MetricReader reader, Resource resource) {
        SdkMeterProviderBuilder builder = SdkMeterProvider.builder()
            .registerMetricReader(reader)
            .setResource(resource);
        Db2Processor.apply(builder);
        return builder.build();
    }

    private SdkLoggerProvider buildLoggerProvider(OtlpGrpcLogRecordExporter logExporter, Resource resource) {
        return SdkLoggerProvider.builder()
            .addLogRecordProcessor(BatchLogRecordProcessor.builder(logExporter).build())
            .setResource(resource)
            .build();
    }

    private static OtlpGrpcLogRecordExporter buildLogExporter(OtlpExporterConfig otlpConfig) {
        return OtlpGrpcLogRecordExporter.builder()
            .setEndpoint(otlpConfig.getEndpoint())
            .build();
    }

    public void start() {
        receiver.start();
        log.info("DB2 pipeline started for {} → OTel Collector", instanceId);
    }

    public void stop() {
        receiver.stopScheduler();
        meterProvider.shutdown();
        if (loggerProvider != null) loggerProvider.shutdown();
        receiver.closeConnection();
        log.info("DB2 pipeline stopped for {}", instanceId);
    }
}
