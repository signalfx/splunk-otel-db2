package com.splunk.otel.db2;

import com.splunk.otel.db2.config.Db2Config;
import com.splunk.otel.db2.pipeline.Db2Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        String configPath = System.getProperty("config", "config/config.yaml");
        if (args.length > 0) configPath = args[0];

        log.info("Loading config from {}", configPath);
        Db2Config config = Db2Config.load(configPath);

        Db2Pipeline pipeline = new Db2Pipeline(config);
        pipeline.start();

        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — stopping pipeline");
            pipeline.stop();
            shutdown.countDown();
        }));

        log.info("splunk-otel-db2 running — press Ctrl+C to stop");
        shutdown.await(); // block until SIGTERM/SIGINT
    }
}
