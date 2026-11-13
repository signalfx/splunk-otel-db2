package com.splunk.otel.db2.pipeline;

import com.splunk.otel.db2.config.Db2Config;
import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.config.OtlpExporterConfig;
import com.splunk.otel.db2.exporter.Db2Exporter;
import com.splunk.otel.db2.processor.Db2Processor;
import com.splunk.otel.db2.receiver.Db2Connection;
import com.splunk.otel.db2.receiver.Db2Receiver;
import com.splunk.otel.db2.receiver.queries.InstanceInfoQuery;
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
        Resource resource = buildResource(rc, identityHost,
            resolveInstanceName(rc, name, Db2Pipeline::lookUpInstanceName));
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
        Resource resource   = buildResource(rc, identityHost, null);
        this.meterProvider  = buildMeterProvider(testReader, resource);
        this.loggerProvider = null;

        Meter meter = meterProvider.get("splunk-otel-db2");
        this.receiver = new Db2Receiver(rc, meter, (Logger) null);
    }

    /**
     * Reads the instance name over a connection of its own, discarded immediately. The receiver's
     * connections do not exist yet — they are created with the Meter and Logger, which need the
     * Resource this feeds — and the resource is immutable once built, so the lookup cannot be
     * deferred into the receiver's lifecycle.
     *
     * <p>Costs one connect attempt at startup, bounded by {@code connect_timeout}. A failure is not
     * fatal: the attribute is dropped and startup continues.
     */
    static final String INSTANCE_NAME = "db2.instance.name";

    /**
     * The configured override, or null when absent or blank.
     *
     * <p>A blank override is treated as unset rather than honoured. Honouring it would export an
     * empty dimension, which reads downstream as a real value — the same reason a failed lookup
     * omits the attribute instead of emitting an empty string. It would also be the one way to get
     * a blank `db2.instance.name` into the data, having gone to the trouble of preventing it
     * everywhere else.
     */
    private static String instanceNameOverride(Db2ReceiverConfig rc) {
        String configured = rc.getResourceAttributes().get(INSTANCE_NAME);
        return configured != null && !configured.isBlank() ? configured : null;
    }

    /**
     * False when {@code resource_attributes} already names the instance. The override replaces
     * whatever is collected, so connecting to collect it is pure waste — and the waste is not just
     * a connection: pipelines are constructed one after another, so an unreachable database holds
     * up every later receiver for its {@code connect_timeout}.
     */
    static boolean instanceNameLookupNeeded(Db2ReceiverConfig rc) {
        return instanceNameOverride(rc) == null;
    }

    /** Reads the instance name unless an override makes it pointless. */
    static String resolveInstanceName(Db2ReceiverConfig rc, String label, InstanceNameLookup lookup) {
        return instanceNameLookupNeeded(rc) ? lookup.read(rc, label) : null;
    }

    /** Seam for tests: production passes {@link #lookUpInstanceName}. */
    @FunctionalInterface
    interface InstanceNameLookup {
        String read(Db2ReceiverConfig rc, String label);
    }

    private static String lookUpInstanceName(Db2ReceiverConfig rc, String label) {
        Db2Connection conn = new Db2Connection(rc, rc.getComponentName());
        try {
            return InstanceInfoQuery.fetchInstanceName(
                conn.getOrConnect(), rc.getConnectTimeoutSeconds(), label);
        } catch (Exception e) {
            log.warn("{}Could not connect to read the Db2 instance name ({}); "
                + "db2.instance.name omitted", label, e.getMessage());
            return null;
        } finally {
            conn.close();
        }
    }

    // Package-private so a test can assert a collected name actually reaches the Resource. The
    // production path needs a live Db2, and the override path exercises the resource_attributes
    // loop instead, so neither would catch this wiring being dropped.
    static Resource buildResource(Db2ReceiverConfig rc, String identityHost,
                                  String instanceName) {
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
            .put("db.system.name",      "ibm.db2")
            .put("db.namespace",        rc.getDatabase());

        // service.namespace only when configured — avoids polluting the resource for single-cluster setups
        String ns = rc.getServiceNamespace();
        if (ns != null && !ns.isBlank()) attrs.put("service.namespace", ns);

        // Read from the server, so absent when the lookup failed. Placed before the
        // resource_attributes loop so an operator override still wins.
        if (instanceName != null && !instanceName.isBlank()) {
            attrs.put(INSTANCE_NAME, instanceName);
        }

        // resource_attributes overrides win last — applied after G1 fields
        // server.port must stay a long to match semconv type
        for (Map.Entry<String, String> e : rc.getResourceAttributes().entrySet()) {
            // A blank instance name is treated as unset, so it must not overwrite a collected one
            // here either — otherwise the override path is the single way to export the empty
            // dimension the rest of this class exists to avoid.
            // Null-guarded as well as blank-guarded: the map is public API via
            // setResourceAttributes, so a caller can put a null value even though config loading
            // now drops them.
            if (INSTANCE_NAME.equals(e.getKey())
                && (e.getValue() == null || e.getValue().isBlank())) {
                continue;
            }
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
