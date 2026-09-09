package com.splunk.otel.db2.pipeline;

import com.splunk.otel.db2.config.Db2Config;
import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.config.OtlpExporterConfig;
import com.splunk.otel.db2.exporter.Db2Exporter;
import com.splunk.otel.db2.processor.Db2Processor;
import com.splunk.otel.db2.receiver.Db2Receiver;
import com.splunk.otel.db2.util.ResourceIdentity;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public Db2Pipeline(Db2ReceiverConfig rc, OtlpExporterConfig otlpConfig) {
        String name = rc.getComponentName().isEmpty() ? "" : "[" + rc.getComponentName() + "] ";
        // Resolve once: loopback → collector hostname so co-located sidecars have unique identities.
        // Remote targets are returned unchanged. Never triggers DNS on the configured host.
        String identityHost = ResourceIdentity.resolveIdentityHost(rc.getHost());
        Resource resource = buildResource(rc, identityHost);
        // instanceId mirrors service.instance.id so logs and backend agree
        String effectiveInstanceId = firstNonBlank(rc.getServiceInstanceId(),
            identityHost + ":" + rc.getPort() + "/" + rc.getDatabase());
        this.instanceId = name + effectiveInstanceId;

        MetricReader reader = Db2Exporter.build(otlpConfig, rc.getCollectionIntervalDuration());
        this.meterProvider  = buildMeterProvider(reader, resource);
        // Each pipeline owns its own exporter so that loggerProvider.shutdown() only drains
        // this pipeline's queue — stopping one pipeline cannot close another's export channel.
        OtlpGrpcLogRecordExporter logExporter = Db2Exporter.buildLogExporter(otlpConfig);
        this.loggerProvider = buildLoggerProvider(logExporter, resource);

        Meter  meter      = meterProvider.get("splunk-otel-db2");
        Logger otelLogger = loggerProvider.get("github.com/signalfx/splunk-otel-db2");
        this.receiver = new Db2Receiver(rc, meter, otelLogger);
    }

    // test constructor — kept for integration tests that inject a custom MetricReader
    public Db2Pipeline(Db2Config config, MetricReader testReader) {
        Db2ReceiverConfig rc = config.getReceivers().get(0);
        String identityHost = ResourceIdentity.resolveIdentityHost(rc.getHost());
        String effectiveInstanceId = firstNonBlank(rc.getServiceInstanceId(),
            identityHost + ":" + rc.getPort() + "/" + rc.getDatabase());
        this.instanceId = effectiveInstanceId;
        Resource resource   = buildResource(rc, identityHost);
        this.meterProvider  = buildMeterProvider(testReader, resource);
        this.loggerProvider = null;

        Meter meter = meterProvider.get("splunk-otel-db2");
        this.receiver = new Db2Receiver(rc, meter, (Logger) null);
    }

    private static Resource buildResource(Db2ReceiverConfig rc, String identityHost) {
        // G1: explicit overrides take precedence over generated values
        String serviceName       = firstNonBlank(rc.getServiceName(), "splunk-otel-db2");
        String serviceInstanceId = firstNonBlank(rc.getServiceInstanceId(),
            identityHost + ":" + rc.getPort() + "/" + rc.getDatabase());

        AttributesBuilder attrs = Attributes.builder()
            .put("service.name",        serviceName)
            .put("service.instance.id", serviceInstanceId)
            .put("server.address",      identityHost)       // resolved: unique per host
            .put("host.name",           rc.getHost())       // operator's typed value preserved
            .put(AttributeKey.longKey("server.port"), (long) rc.getPort())
            .put("db.system.name",      "db2")
            .put("db.namespace",        rc.getDatabase());

        // service.namespace only when configured — avoids polluting the resource for single-cluster setups
        String ns = rc.getServiceNamespace();
        if (ns != null && !ns.isBlank()) attrs.put("service.namespace", ns);

        // resource_attributes overrides win last — applied after G1 fields
        // server.port must stay a long to match semconv type
        for (Map.Entry<String, String> e : rc.getResourceAttributes().entrySet()) {
            if (e.getKey().equals("server.port")) {
                try {
                    attrs.put(AttributeKey.longKey("server.port"), Long.parseLong(e.getValue()));
                } catch (NumberFormatException ex) {
                    throw new IllegalStateException(
                        "resource_attributes.server.port must be an integer, got: " + e.getValue());
                }
            } else {
                attrs.put(e.getKey(), e.getValue());
            }
        }
        return Resource.getDefault().merge(Resource.create(attrs.build()));
    }

    private static String firstNonBlank(String first, String fallback) {
        return (first != null && !first.isBlank()) ? first : fallback;
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

    public void start() {
        receiver.start();
        log.info("DB2 pipeline started for {} → OTel Collector", instanceId);
    }

    /** For tests: bypass the 60s minimum and schedule top-query at the given interval. */
    public void startTopQueryScheduler(long intervalSeconds) {
        receiver.startTopQueryScheduler(intervalSeconds);
    }

    /** For tests: same as above but counts down {@code seedDone} when the seed scrape finishes. */
    public void startTopQueryScheduler(long intervalSeconds, CountDownLatch seedDone) {
        receiver.startTopQueryScheduler(intervalSeconds, seedDone);
    }

    public void stop() {
        // Pipeline-level guard: the entire three-step sequence must run as a unit.
        // Without this, a second concurrent caller could reach receiver.stop() (closing
        // JDBC) while the first is still inside meterProvider.shutdown()'s final collection.
        if (!stopped.compareAndSet(false, true)) return;
        // Step 1: drain schedulers so no new collection cycle starts.
        receiver.stopScheduler();
        // Step 2: shut down the OTel SDK. PeriodicMetricReader.shutdown() triggers a final
        // doRun() collection; JDBC must still be open so the scrape succeeds without leaking
        // a replacement connection.
        meterProvider.shutdown();
        if (loggerProvider != null) loggerProvider.shutdown();
        // Step 3: close JDBC only after the SDK has flushed.
        receiver.stop();
        log.info("DB2 pipeline stopped for {}", instanceId);
    }
}
