package com.splunk.otel.db2.config;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class Db2Config {

    private Receivers receivers = new Receivers();
    private Exporters exporters = new Exporters();

    public Receivers getReceivers() { return receivers; }
    public void setReceivers(Receivers receivers) { this.receivers = receivers; }

    public Exporters getExporters() { return exporters; }
    public void setExporters(Exporters exporters) { this.exporters = exporters; }

    public static class Receivers {
        private Db2ReceiverConfig db2 = new Db2ReceiverConfig();
        public Db2ReceiverConfig getDb2() { return db2; }
        public void setDb2(Db2ReceiverConfig db2) { this.db2 = db2; }
    }

    public static class Exporters {
        private OtlpExporterConfig otlp = new OtlpExporterConfig();
        public OtlpExporterConfig getOtlp() { return otlp; }
        public void setOtlp(OtlpExporterConfig otlp) { this.otlp = otlp; }
    }

    @SuppressWarnings("unchecked")
    public static Db2Config load(String path) throws IOException {
        Yaml yaml = new Yaml();
        try (InputStream is = new FileInputStream(path)) {
            Map<String, Object> root = yaml.load(is);
            Db2Config cfg = new Db2Config();

            Map<String, Object> receivers = (Map<String, Object>) root.get("receivers");
            if (receivers != null) {
                Map<String, Object> db2 = (Map<String, Object>) receivers.get("db2");
                if (db2 != null) {
                    Db2ReceiverConfig rc = cfg.getReceivers().getDb2();
                    if (db2.get("hostname") != null) rc.setHostname((String) db2.get("hostname"));
                    if (db2.get("hostport") != null) rc.setHostport((Integer) db2.get("hostport"));
                    if (db2.get("username") != null) rc.setUsername((String) db2.get("username"));
                    if (db2.get("password") != null) rc.setPassword((String) db2.get("password"));
                    if (db2.get("database") != null) rc.setDatabase((String) db2.get("database"));
                    if (db2.get("collection_interval") != null) rc.setCollectionInterval((String) db2.get("collection_interval"));
                    if (db2.get("top_query_count") != null) rc.setTopQueryCount((Integer) db2.get("top_query_count"));
                    if (db2.get("max_query_sample_count") != null) rc.setMaxQuerySampleCount((Integer) db2.get("max_query_sample_count"));
                    if (db2.get("query_sample_interval") != null) rc.setQuerySampleInterval((String) db2.get("query_sample_interval"));
                    if (db2.get("top_query_interval") != null) rc.setTopQueryInterval((String) db2.get("top_query_interval"));
                    if (db2.get("use_snapshot_monitor") != null) rc.setUseSnapshotMonitor((Boolean) db2.get("use_snapshot_monitor"));
                    if (db2.get("tls") != null) {
                        Map<String, Object> tls = (Map<String, Object>) db2.get("tls");
                        if (tls.get("insecure") != null) {
                            rc.getTls().setInsecure((Boolean) tls.get("insecure"));
                        }
                    }
                }
            }

            Map<String, Object> exporters = (Map<String, Object>) root.get("exporters");
            if (exporters != null) {
                Map<String, Object> otlp = (Map<String, Object>) exporters.get("otlp");
                if (otlp != null && otlp.get("endpoint") != null) {
                    cfg.getExporters().getOtlp().setEndpoint((String) otlp.get("endpoint"));
                }
            }

            return cfg;
        }
    }
}
