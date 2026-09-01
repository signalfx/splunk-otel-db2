package com.splunk.otel.db2;

import com.splunk.otel.db2.config.Db2Config;
import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.config.OtlpExporterConfig;
import com.splunk.otel.db2.pipeline.Db2Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
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

        List<Db2Pipeline> pipelines = new ArrayList<>();
        for (Db2ReceiverConfig rc : receivers) {
            Db2Pipeline pipeline = new Db2Pipeline(rc, otlpConfig);
            pipeline.start();
            pipelines.add(pipeline);
        }

        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — stopping {} pipeline(s)", pipelines.size());
            for (Db2Pipeline p : pipelines) {
                try { p.stop(); } catch (Exception e) { log.warn("Error stopping pipeline: {}", e.getMessage()); }
            }
            shutdown.countDown();
        }));

        log.info("splunk-otel-db2 running {} pipeline(s) — press Ctrl+C to stop", pipelines.size());
        shutdown.await();
    }
}
