package com.splunk.otel.db2;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.config.OtlpExporterConfig;
import com.splunk.otel.db2.pipeline.Db2Pipeline;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MainStartupTest {

    // TEST-NET-1 (RFC 5737). Routed nowhere, so a connect attempt blocks until it times out
    // instead of being refused immediately — which is what makes the serial-vs-parallel
    // difference measurable.
    private static final String BLACKHOLE_HOST = "192.0.2.1";

    private static Db2ReceiverConfig unreachableReceiver(String name, int connectTimeoutSeconds) {
        Db2ReceiverConfig rc = new Db2ReceiverConfig();
        rc.setComponentName(name);
        rc.setHostname(BLACKHOLE_HOST);
        rc.setHostport(50000);
        rc.setUsername("user");
        rc.setPassword("pass");
        rc.setDatabase("TESTDB");
        rc.setConnectTimeoutSeconds(connectTimeoutSeconds);
        rc.getTls().setInsecure(true);
        return rc;
    }

    /**
     * Four unreachable databases, each allowed 3s to connect. Serially that is ~12s of instance-name
     * lookups before any healthy receiver could start; in parallel it is ~3s.
     *
     * <p>The threshold is 8s — comfortably above one timeout plus JVM and SDK setup, and well below
     * the 12s a serial implementation needs. It asserts the shape of the startup path, not a
     * performance target, so the margin is deliberately wide rather than tight.
     */
    @Test
    void unreachableDatabasesDoNotDelayEachOther() throws Exception {
        int receiverCount = 4;
        int connectTimeoutSeconds = 3;
        List<Db2ReceiverConfig> receivers = new ArrayList<>();
        for (int i = 0; i < receiverCount; i++) {
            receivers.add(unreachableReceiver("db" + i, connectTimeoutSeconds));
        }

        OtlpExporterConfig otlp = new OtlpExporterConfig();
        otlp.setEndpoint("http://127.0.0.1:4317");

        List<Db2Pipeline> pipelines = List.of();
        long startedAt = System.nanoTime();
        try {
            pipelines = Main.startAll(receivers, otlp);
            long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000;

            assertThat(pipelines).hasSize(receiverCount);
            assertThat(elapsedMs)
                .as("%d unreachable databases at %ds each took %dms; serial startup would need ~%dms",
                    receiverCount, connectTimeoutSeconds, elapsedMs,
                    receiverCount * connectTimeoutSeconds * 1000)
                .isLessThan(8_000);
        } finally {
            for (Db2Pipeline p : pipelines) {
                try { p.stop(); } catch (Exception ignored) {}
            }
        }
    }

    // An unreachable database must not prevent startup: the instance name is dropped and the
    // receiver still comes up to retry on its own schedule.
    @Test
    void anUnreachableDatabaseStillYieldsAStartedPipeline() throws Exception {
        OtlpExporterConfig otlp = new OtlpExporterConfig();
        otlp.setEndpoint("http://127.0.0.1:4317");

        List<Db2Pipeline> pipelines = List.of();
        try {
            pipelines = Main.startAll(List.of(unreachableReceiver("solo", 2)), otlp);
            assertThat(pipelines).hasSize(1);
        } finally {
            for (Db2Pipeline p : pipelines) {
                try { p.stop(); } catch (Exception ignored) {}
            }
        }
    }
}
