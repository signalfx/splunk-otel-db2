package com.splunk.otel.db2;

import com.splunk.otel.db2.config.Db2Config;
import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.config.OtlpExporterConfig;
import com.splunk.otel.db2.pipeline.Db2Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    /**
     * Builds and starts every pipeline in parallel, mirroring the shutdown path below.
     *
     * <p>Startup touches the database twice per receiver: the instance-name lookup while the
     * Resource is assembled, and the Db2 version probe in {@code start()} when query sampling is
     * on. Both are bounded by {@code connect_timeout} and both degrade gracefully, but run serially
     * they compound — one unreachable database used to hold up every healthy receiver behind it, so
     * N stalled databases cost roughly N x connect_timeout before the healthy ones scraped anything.
     *
     * <p>Sized to the receiver count on purpose: a smaller pool would queue receivers behind those
     * blocking connects and reintroduce the problem for whatever did not get a thread.
     *
     * <p>Construction failures still abort startup, as they did when this loop was sequential — the
     * first exception is rethrown once every task has finished, rather than leaving the process
     * half-initialised.
     */
    static List<Db2Pipeline> startAll(List<Db2ReceiverConfig> receivers, OtlpExporterConfig otlpConfig)
            throws Exception {
        if (receivers.size() == 1) {
            Db2ReceiverConfig rc = receivers.get(0);
            Db2Pipeline pipeline = new Db2Pipeline(rc, otlpConfig);
            pipeline.start();
            return new ArrayList<>(List.of(pipeline));
        }

        List<Db2Pipeline> pipelines = new CopyOnWriteArrayList<>();
        ExecutorService startExecutor = Executors.newFixedThreadPool(receivers.size());
        List<Future<?>> futures = new ArrayList<>(receivers.size());
        try {
            for (Db2ReceiverConfig rc : receivers) {
                futures.add(startExecutor.submit(() -> {
                    Db2Pipeline pipeline = new Db2Pipeline(rc, otlpConfig);
                    pipeline.start();
                    pipelines.add(pipeline);
                    return null;
                }));
            }
            // Drain every future before rethrowing so a single bad receiver cannot leave the others
            // half-built, and so their failures are all logged rather than only the first.
            Exception firstFailure = null;
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (ExecutionException e) {
                    log.error("Pipeline failed to start: {}", e.getCause().getMessage(), e.getCause());
                    if (firstFailure == null) {
                        Throwable cause = e.getCause();
                        firstFailure = cause instanceof Exception
                            ? (Exception) cause
                            : new RuntimeException(cause);
                    }
                }
            }
            if (firstFailure != null) {
                for (Db2Pipeline started : pipelines) {
                    try { started.stop(); } catch (Exception ignored) {}
                }
                throw firstFailure;
            }
        } finally {
            startExecutor.shutdown();
        }
        return new ArrayList<>(pipelines);
    }

    public static void main(String[] args) throws Exception {
        // Route java.util.logging (used by gRPC and OTel exporters) through SLF4J/Logback
        // so that logback.xml controls their log levels.
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        if (args.length == 0) {
            System.err.println("Usage: splunk-otel-db2 <config-file>");
            System.err.println("Example: splunk-otel-db2 /etc/splunk-otel-db2/config.yaml");
            System.exit(1);
        }
        String configPath = args[0];

        log.info("Loading config from {}", configPath);
        Db2Config config = Db2Config.load(configPath);

        OtlpExporterConfig otlpConfig = config.getExporters().getOtlp();
        List<Db2ReceiverConfig> receivers = config.getReceivers();
        log.info("Starting {} database pipeline(s)", receivers.size());

        List<Db2Pipeline> pipelines = startAll(receivers, otlpConfig);

        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — stopping {} pipeline(s)", pipelines.size());
            // Use a dedicated executor sized to the pipeline count so every stop() task
            // starts immediately — the common fork-join pool is bounded and can queue tasks
            // behind blocking scheduler timeouts when there are many databases.
            ExecutorService stopExecutor = Executors.newFixedThreadPool(pipelines.size());
            for (Db2Pipeline p : pipelines) {
                stopExecutor.submit(() -> {
                    try { p.stop(); } catch (Exception e) { log.warn("Error stopping pipeline: {}", e.getMessage()); }
                });
            }
            stopExecutor.shutdown();
            try {
                if (!stopExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.warn("Some pipelines did not finish shutdown within 30s — forcing stop");
                    stopExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                stopExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            shutdown.countDown();
        }));

        log.info("splunk-otel-db2 running {} pipeline(s) — press Ctrl+C to stop", pipelines.size());
        shutdown.await();
    }
}
