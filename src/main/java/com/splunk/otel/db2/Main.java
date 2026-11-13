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
import java.util.concurrent.atomic.AtomicBoolean;

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
    /** Seam for tests: production passes {@code Db2Pipeline::new}. */
    @FunctionalInterface
    interface PipelineFactory {
        Db2Pipeline create(Db2ReceiverConfig rc, OtlpExporterConfig otlpConfig);
    }

    static List<Db2Pipeline> startAll(List<Db2ReceiverConfig> receivers, OtlpExporterConfig otlpConfig,
                                      PipelineFactory factory) throws Exception {
        return startAll(receivers, otlpConfig, factory, drainTimeoutSeconds(receivers));
    }

    // drainTimeoutSeconds is a parameter so a test can expire the wait deliberately and assert that
    // a worker outliving it still disposes of its own pipeline.
    static List<Db2Pipeline> startAll(List<Db2ReceiverConfig> receivers, OtlpExporterConfig otlpConfig,
                                      PipelineFactory factory, int drainTimeoutSeconds) throws Exception {
        List<Db2Pipeline> pipelines = new CopyOnWriteArrayList<>();
        // Cleanup cannot rest on the drain below completing: no timeout is guaranteed to outlast a
        // driver that ignores its own. A worker that outlives rollback reads this and disposes of
        // its own pipeline, so nothing leaks even when the wait gives up.
        AtomicBoolean aborted = new AtomicBoolean();
        ExecutorService startExecutor = Executors.newFixedThreadPool(receivers.size());
        List<Future<?>> futures = new ArrayList<>(receivers.size());
        try {
            for (Db2ReceiverConfig rc : receivers) {
                futures.add(startExecutor.submit(() -> {
                    Db2Pipeline pipeline = factory.create(rc, otlpConfig);
                    // Starting now would create schedulers that rollback has already finished
                    // looking for. Never start into an abort — stop instead, and do not register.
                    if (aborted.get()) {
                        pipeline.stop();
                        return null;
                    }
                    // Registered before start() so a pipeline that throws midway through starting
                    // is still rolled back — its schedulers and SDK may already be live.
                    pipelines.add(pipeline);
                    pipeline.start();
                    // The flag can be raised while start() runs. Rollback sets it before taking its
                    // snapshot, so either that snapshot contains this pipeline, or this check sees
                    // the flag — one of the two always stops it, and stop() is idempotent.
                    if (aborted.get()) {
                        pipeline.stop();
                    }
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
                } catch (InterruptedException e) {
                    // Abandoning the wait would leak every pipeline started so far, each holding
                    // JDBC connections and live schedulers.
                    aborted.set(true);
                    futures.forEach(pending -> pending.cancel(true));
                    // A cancelled task is not a finished task. cancel(true) interrupts the worker,
                    // but a JDBC connect does not answer interruption, so a worker can still be
                    // inside the factory. Draining the pool first is what makes the rollback list
                    // final — otherwise a late worker adds and starts its pipeline after stopAll
                    // has already returned and nothing is left to stop it. It also settles the
                    // start()/stop() race: once the pool is quiet no worker is inside start(), so
                    // stop() cannot set the idempotent guard while schedulers are still being
                    // created.
                    drain(startExecutor, drainTimeoutSeconds);
                    stopAll(pipelines, 30);
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
            if (firstFailure != null) {
                // Every future has been drained by this point, so no worker is still running and
                // the list is final. The flag is still set, for symmetry and in case a future
                // change reorders this.
                aborted.set(true);
                stopAll(pipelines, 30);
                throw firstFailure;
            }
        } finally {
            startExecutor.shutdown();
        }
        return new ArrayList<>(pipelines);
    }

    /** Headroom over the configured connect budget, for SDK and scheduler setup. */
    private static final int DRAIN_GRACE_SECONDS = 30;

    /**
     * Derived from configuration rather than fixed: {@code connect_timeout} has no upper bound, and
     * startup spends it twice in sequence — once on the instance-name lookup during construction,
     * once on the version probe inside {@code start()} — so any constant could be shorter than
     * legitimate work and turn a slow startup into a reported leak.
     */
    private static int drainTimeoutSeconds(List<Db2ReceiverConfig> receivers) {
        int slowestConnect = receivers.stream()
            .mapToInt(Db2ReceiverConfig::getConnectTimeoutSeconds)
            .max()
            .orElse(0);
        return slowestConnect * 2 + DRAIN_GRACE_SECONDS;
    }

    /**
     * Waits for cancelled startup workers to stop running, so the rollback snapshot is final.
     *
     * <p>Expiry is not a correctness hole: a worker still running past this point sees the abort
     * flag and stops its own pipeline. The wait exists to keep cleanup on this thread in the normal
     * case, where it can also be logged.
     */
    private static void drain(ExecutorService startExecutor, int timeoutSeconds) {
        startExecutor.shutdownNow();
        try {
            if (!startExecutor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                log.error("Startup tasks still running after {}s; each will stop its own pipeline",
                    timeoutSeconds);
            }
        } catch (InterruptedException secondInterrupt) {
            log.warn("Interrupted again while draining startup tasks; "
                + "in-flight workers will stop their own pipelines");
        }
    }

    /**
     * Stops every pipeline concurrently. {@code stop()} drains the SDK, which triggers a final
     * metric collection and so can block for {@code connect_timeout} against an unresponsive
     * database — serially that compounds across pipelines exactly as startup did. A pool sized to
     * the pipeline count means no pipeline waits behind another's timeout.
     */
    static void stopAll(List<Db2Pipeline> pipelines, int timeoutSeconds) {
        if (pipelines.isEmpty()) {
            return;
        }
        ExecutorService stopExecutor = Executors.newFixedThreadPool(pipelines.size());
        for (Db2Pipeline p : pipelines) {
            stopExecutor.submit(() -> {
                try { p.stop(); } catch (Exception e) { log.warn("Error stopping pipeline: {}", e.getMessage()); }
            });
        }
        stopExecutor.shutdown();
        try {
            if (!stopExecutor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                log.warn("Some pipelines did not finish shutdown within {}s — forcing stop", timeoutSeconds);
                stopExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            stopExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
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

        List<Db2Pipeline> pipelines = startAll(receivers, otlpConfig, Db2Pipeline::new);

        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — stopping {} pipeline(s)", pipelines.size());
            stopAll(pipelines, 30);
            shutdown.countDown();
        }));

        log.info("splunk-otel-db2 running {} pipeline(s) — press Ctrl+C to stop", pipelines.size());
        shutdown.await();
    }
}
