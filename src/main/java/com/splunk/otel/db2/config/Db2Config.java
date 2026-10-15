package com.splunk.otel.db2.config;

import com.splunk.otel.db2.util.HostNormalizer;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class Db2Config {

    // Mirrors the OTel Collector nameRegexp: no Unicode separator (Z), control (C), or symbol (S) chars.
    private static final Pattern NAME_PATTERN = Pattern.compile("^[^\\pZ\\pC\\pS]+$");

    private List<Db2ReceiverConfig> receivers = new ArrayList<>();
    private Exporters exporters = new Exporters();

    public List<Db2ReceiverConfig> getReceivers() { return receivers; }
    public void setReceivers(List<Db2ReceiverConfig> receivers) { this.receivers = receivers; }

    public Exporters getExporters() { return exporters; }
    public void setExporters(Exporters exporters) { this.exporters = exporters; }

    public static class Exporters {
        private OtlpExporterConfig otlp = new OtlpExporterConfig();
        public OtlpExporterConfig getOtlp() { return otlp; }
        public void setOtlp(OtlpExporterConfig otlp) { this.otlp = otlp; }
    }

    @SuppressWarnings("unchecked")
    public static Db2Config load(String path) throws IOException {
        Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
        try (InputStream is = new FileInputStream(path)) {
            Map<String, Object> root = yaml.load(is);
            Db2Config cfg = new Db2Config();

            Object receiversNode = root.get("receivers");
            if (receiversNode instanceof Map) {
                // Standard form: receivers is a map keyed by component ID (db2 or db2/<name>)
                Map<String, Object> receiversMap = (Map<String, Object>) receiversNode;
                for (Map.Entry<String, Object> entry : receiversMap.entrySet()) {
                    String key = entry.getKey();
                    int slashIdx = key.indexOf('/');
                    String type = slashIdx < 0 ? key : key.substring(0, slashIdx);
                    if (!type.equals("db2")) {
                        throw new IllegalStateException(
                            "Unsupported receiver type '" + type + "' in key '" + key +
                            "': only 'db2' is supported");
                    }
                    String name = "";
                    if (slashIdx >= 0) {
                        name = key.substring(slashIdx + 1);
                        if (name.isEmpty()) {
                            throw new IllegalStateException(
                                "Invalid receiver key '" + key + "': name after '/' must not be empty");
                        }
                        if (name.length() > 1024) {
                            throw new IllegalStateException(
                                "Invalid receiver key '" + key + "': name exceeds 1024 characters");
                        }
                        if (!NAME_PATTERN.matcher(name).matches()) {
                            throw new IllegalStateException(
                                "Invalid receiver key '" + key + "': name contains invalid characters " +
                                "(no whitespace, control, or symbol characters allowed)");
                        }
                    }
                    if (!(entry.getValue() instanceof Map)) {
                        throw new IllegalStateException(
                            "Receiver '" + key + "' must be a map of configuration fields");
                    }
                    Db2ReceiverConfig rc = parseDb2Config((Map<String, Object>) entry.getValue());
                    rc.setComponentName(name);
                    cfg.getReceivers().add(rc);
                }
            } else if (receiversNode != null) {
                throw new IllegalStateException(
                    "'receivers' must be a map of component IDs (e.g. db2: or db2/name:), got: " +
                    receiversNode.getClass().getSimpleName());
            }

            if (cfg.getReceivers().isEmpty()) {
                throw new IllegalStateException("No receivers.db2 entries found in config");
            }

            // A5: reject duplicate targets before any connection is attempted.
            // Normalize the host the same way Db2Pipeline does so that localhost/127.0.0.1/::1
            // aliases and hostname casing differences are caught here rather than producing
            // colliding service.instance.id values at runtime.
            Set<String> seen = new HashSet<>();
            for (Db2ReceiverConfig rc : cfg.getReceivers()) {
                String normalizedHost = HostNormalizer.resolveServerAddress(rc.getHostname()).toLowerCase();
                String target = normalizedHost + ":" + rc.getHostport() + "/" + rc.getDatabase();
                if (!seen.add(target)) {
                    throw new IllegalStateException(
                        "Duplicate receiver target '" + target + "': two receivers point at the same database");
                }
            }

            Map<String, Object> exporters = (Map<String, Object>) root.get("exporters");
            if (exporters != null) {
                Map<String, Object> otlp = (Map<String, Object>) exporters.get("otlp");
                if (otlp != null) {
                    if (otlp.get("endpoint") != null)
                        cfg.getExporters().getOtlp().setEndpoint(expandEnv(asString(otlp, "endpoint")));
                    Object headersNode = otlp.get("headers");
                    if (headersNode != null) {
                        if (!(headersNode instanceof Map))
                            throw new IllegalStateException(
                                "exporters.otlp.headers must be a map of key: value pairs, got: " +
                                headersNode.getClass().getSimpleName());
                        Map<?, ?> rawHeaders = (Map<?, ?>) headersNode;
                        Map<String, String> headers = new java.util.LinkedHashMap<>();
                        for (Map.Entry<?, ?> e : rawHeaders.entrySet()) {
                            headers.put(String.valueOf(e.getKey()), expandEnv(String.valueOf(e.getValue())));
                        }
                        cfg.getExporters().getOtlp().setHeaders(headers);
                    }
                    if (otlp.get("compression") != null) {
                        String compression = asString(otlp, "compression");
                        if (!compression.equalsIgnoreCase("gzip") &&
                            !compression.equalsIgnoreCase("none") &&
                            !compression.isEmpty())
                            throw new IllegalStateException(
                                "exporters.otlp.compression: unsupported value '" + compression +
                                "'; supported values: gzip, none");
                        cfg.getExporters().getOtlp().setCompression(compression);
                    }
                    Object tlsNode = otlp.get("tls");
                    if (tlsNode != null) {
                        if (!(tlsNode instanceof Map))
                            throw new IllegalStateException(
                                "exporters.otlp.tls must be a map, got: " +
                                tlsNode.getClass().getSimpleName());
                        Map<String, Object> otlpTls = (Map<String, Object>) tlsNode;
                        OtlpExporterConfig.TlsConfig tlsCfg = new OtlpExporterConfig.TlsConfig();
                        if (otlpTls.get("insecure") != null) tlsCfg.setInsecure(asBool(otlpTls, "insecure"));
                        if (otlpTls.get("ca_file")   != null) tlsCfg.setCaFile(asString(otlpTls, "ca_file"));
                        if (otlpTls.get("cert_file") != null) tlsCfg.setCertFile(asString(otlpTls, "cert_file"));
                        if (otlpTls.get("key_file")  != null) tlsCfg.setKeyFile(asString(otlpTls, "key_file"));
                        cfg.getExporters().getOtlp().setTls(tlsCfg);
                    }
                }
            }

            for (Db2ReceiverConfig rc : cfg.getReceivers()) {
                validate(rc, cfg.getExporters().getOtlp());
            }
            return cfg;
        }
    }

    @SuppressWarnings("unchecked")
    private static Db2ReceiverConfig parseDb2Config(Map<String, Object> db2) {
        Db2ReceiverConfig rc = new Db2ReceiverConfig();
        if (db2.get("hostname")               != null) rc.setHostname(asString(db2, "hostname"));
        if (db2.get("hostport")               != null) rc.setHostport(asInt(db2, "hostport"));
        if (db2.get("username")               != null) rc.setUsername(asString(db2, "username"));
        if (db2.get("password")               != null) rc.setPassword(asString(db2, "password"));
        if (db2.get("database")               != null) rc.setDatabase(asString(db2, "database"));
        if (db2.get("collection_interval")    != null) rc.setCollectionInterval(asString(db2, "collection_interval"));
        if (db2.get("use_snapshot_monitor")   != null) rc.setUseSnapshotMonitor(asBool(db2, "use_snapshot_monitor"));
        if (db2.get("connect_timeout_seconds")!= null) rc.setConnectTimeoutSeconds(asInt(db2, "connect_timeout_seconds"));

        // events block — mirrors OTel LogsBuilderConfig.Events pattern
        Object eventsNode = db2.get("events");
        if (eventsNode != null) {
            if (!(eventsNode instanceof Map))
                throw new IllegalStateException("receivers.db2.events must be a map");
            Map<String, Object> evts = (Map<String, Object>) eventsNode;
            parseEventConfig(evts, "db.server.top_query",    rc.getEvents().getDbServerTopQuery());
            parseEventConfig(evts, "db.server.query_sample", rc.getEvents().getDbServerQuerySample());
        }

        // top_query_collection
        Object tqcNode = db2.get("top_query_collection");
        if (tqcNode != null) {
            if (!(tqcNode instanceof Map))
                throw new IllegalStateException(
                    "receivers.db2.top_query_collection must be a map, got: " +
                    tqcNode.getClass().getSimpleName());
            Map<String, Object> tqc = (Map<String, Object>) tqcNode;
            Db2ReceiverConfig.TopQueryCollectionConfig tqcCfg = rc.getTopQueryCollection();
            if (tqc.get("collection_interval")    != null)
                tqcCfg.setCollectionInterval(asString(tqc, "collection_interval"));
            if (tqc.get("top_query_count")        != null)
                tqcCfg.setTopQueryCount(asInt(tqc, "top_query_count"));
            if (tqc.get("max_query_sample_count") != null)
                tqcCfg.setMaxQuerySampleCount(asInt(tqc, "max_query_sample_count"));
            if (tqc.get("max_explain_each_interval") != null)
                tqcCfg.setMaxExplainEachInterval(asInt(tqc, "max_explain_each_interval"));
            if (tqc.get("query_plan_cache_size")  != null)
                tqcCfg.setQueryPlanCacheSize(asInt(tqc, "query_plan_cache_size"));
            if (tqc.get("query_plan_cache_ttl")   != null)
                tqcCfg.setQueryPlanCacheTtl(asString(tqc, "query_plan_cache_ttl"));
            if (tqc.get("explain_schema")         != null)
                tqcCfg.setExplainSchema(asString(tqc, "explain_schema"));
        }

        // query_sample_collection
        Object qscNode = db2.get("query_sample_collection");
        if (qscNode != null) {
            if (!(qscNode instanceof Map))
                throw new IllegalStateException(
                    "receivers.db2.query_sample_collection must be a map, got: " +
                    qscNode.getClass().getSimpleName());
            Map<String, Object> qsc = (Map<String, Object>) qscNode;
            Db2ReceiverConfig.QuerySampleCollectionConfig qscCfg = rc.getQuerySampleCollection();
            if (qsc.get("collection_interval") != null)
                qscCfg.setCollectionInterval(asString(qsc, "collection_interval"));
            if (qsc.get("max_rows_per_query")  != null)
                qscCfg.setMaxRowsPerQuery(asInt(qsc, "max_rows_per_query"));
        }
        if (db2.get("service_name")           != null) rc.setServiceName(asString(db2, "service_name"));
        if (db2.get("service_namespace")      != null) rc.setServiceNamespace(asString(db2, "service_namespace"));
        if (db2.get("service_instance_id")    != null) rc.setServiceInstanceId(asString(db2, "service_instance_id"));
        Object raNode = db2.get("resource_attributes");
        if (raNode != null) {
            if (!(raNode instanceof Map))
                throw new IllegalStateException(
                    "receivers.db2.resource_attributes must be a map of key: value pairs, got: " +
                    raNode.getClass().getSimpleName());
            Map<?, ?> raw = (Map<?, ?>) raNode;
            Map<String, String> attrs = new java.util.LinkedHashMap<>();
            for (Map.Entry<?, ?> e : raw.entrySet()) {
                attrs.put(String.valueOf(e.getKey()), expandEnv(String.valueOf(e.getValue())));
            }
            rc.setResourceAttributes(attrs);
        }
        if (db2.get("tls") != null) {
            Map<String, Object> tls = (Map<String, Object>) db2.get("tls");
            if (tls.get("insecure") != null) {
                rc.getTls().setInsecure(asBool(tls, "insecure"));
            }
            // Every key the chart renders has to be read here: one written into
            // config.yaml and not parsed is silently ignored, so the values would
            // have no effect on the connection at all.
            if (tls.get("insecure_skip_verify") != null) {
                rc.getTls().setInsecureSkipVerify(asBool(tls, "insecure_skip_verify"));
            }
        }
        return rc;
    }

    @SuppressWarnings("unchecked")
    private static void parseEventConfig(Map<String, Object> evts, String key,
                                         Db2ReceiverConfig.EventConfig target) {
        Object node = evts.get(key);
        if (node == null) return;
        if (!(node instanceof Map))
            throw new IllegalStateException("receivers.db2.events." + key + " must be a map");
        Map<String, Object> m = (Map<String, Object>) node;
        if (m.get("enabled") != null) target.setEnabled(asBool(m, "enabled"));
    }

    private static String asString(Map<String, Object> map, String key) {
        Object v = map.get(key);
        if (v instanceof String) return (String) v;
        throw new IllegalStateException("Config field '" + key + "' must be a string, got: " + v.getClass().getSimpleName() + " (" + v + ")");
    }

    /** Expand {@code ${env:VAR}} references in a string value; fail if the variable is missing. */
    static String expandEnv(String value) {
        if (value == null) return null;
        if (value.startsWith("${env:") && value.endsWith("}")) {
            String key = value.substring(6, value.length() - 1);
            String env = System.getenv(key);
            if (env == null) {
                throw new IllegalStateException("Required environment variable '" + key + "' is not set");
            }
            return env;
        }
        return value;
    }

    private static int asInt(Map<String, Object> map, String key) {
        Object v = map.get(key);
        if (v instanceof Integer) return (Integer) v;
        if (v instanceof String) {
            String expanded = expandEnv((String) v);
            try { return Integer.parseInt(expanded); }
            catch (NumberFormatException e) { /* fall through */ }
        }
        throw new IllegalStateException("Config field '" + key + "' must be an integer, got: " + v.getClass().getSimpleName() + " (" + v + ")");
    }

    private static boolean asBool(Map<String, Object> map, String key) {
        Object v = map.get(key);
        if (v instanceof Boolean) return (Boolean) v;
        if (v instanceof String) {
            String s = ((String) v).trim().toLowerCase();
            if (s.equals("true")) return true;
            if (s.equals("false")) return false;
        }
        throw new IllegalStateException("Config field '" + key + "' must be a boolean (true/false), got: " + v.getClass().getSimpleName() + " (" + v + ")");
    }

    private static void validate(Db2ReceiverConfig rc, OtlpExporterConfig otlp) {
        String username = rc.getUsername();
        String password = rc.getPassword();
        String database = rc.getDatabase();
        String hostname = rc.getHostname();

        if (username == null || username.isEmpty())
            throw new IllegalStateException("receivers.db2.username is required");
        if (password == null || password.isEmpty())
            throw new IllegalStateException("receivers.db2.password is required");
        if (database == null || database.isEmpty())
            throw new IllegalStateException("receivers.db2.database is required");
        if (hostname == null || hostname.isEmpty())
            rc.setHostname("localhost");
        if (rc.getPort() <= 0 || rc.getPort() > 65535)
            throw new IllegalStateException("receivers.db2.hostport must be between 1 and 65535");
        rc.getCollectionIntervalDuration();
        if (rc.getEvents().getDbServerTopQuery().isEnabled()) {
            rc.getTopQueryCollection().getCollectionIntervalDuration();
            if (rc.getTopQueryCollection().getTopQueryCount() <= 0)
                throw new IllegalStateException("top_query_collection.top_query_count must be > 0");
            if (rc.getTopQueryCollection().getMaxQuerySampleCount() <= 0)
                throw new IllegalStateException("top_query_collection.max_query_sample_count must be > 0");
        }
        if (rc.getEvents().getDbServerQuerySample().isEnabled()) {
            rc.getQuerySampleCollection().getCollectionIntervalDuration();
            if (rc.getQuerySampleCollection().getMaxRowsPerQuery() <= 0)
                throw new IllegalStateException("query_sample_collection.max_rows_per_query must be > 0");
        }

        String endpoint = otlp.getEndpoint();
        if (endpoint == null || endpoint.isBlank())
            throw new IllegalStateException("exporters.otlp.endpoint is required");

        OtlpExporterConfig.TlsConfig tls = otlp.getTls();
        if (tls != null) {
            boolean hasCert    = !tls.getCertFile().isEmpty();
            boolean hasKey     = !tls.getKeyFile().isEmpty();
            boolean hasCa      = !tls.getCaFile().isEmpty();
            boolean isHttp     = endpoint.startsWith("http://");
            boolean isHttps    = endpoint.startsWith("https://");

            // cert_file and key_file are a pair — both or neither
            if (hasCert != hasKey)
                throw new IllegalStateException(
                    "exporters.otlp.tls: cert_file and key_file must both be set or both be absent");

            // insecure: true → must use http://, no certificates
            if (tls.isInsecure()) {
                if (!isHttp)
                    throw new IllegalStateException(
                        "exporters.otlp.tls: insecure: true requires an http:// endpoint, got: " + endpoint);
                if (hasCa || hasCert)
                    throw new IllegalStateException(
                        "exporters.otlp.tls: insecure: true is incompatible with ca_file / cert_file");
            }

            // insecure: false (explicit) or any TLS file → must use https://
            if (!tls.isInsecure() && (hasCa || hasCert) && !isHttps)
                throw new IllegalStateException(
                    "exporters.otlp.tls: ca_file / cert_file require an https:// endpoint, got: " + endpoint);
            if (!tls.isInsecure() && tls.isTlsExplicitlySet() && isHttp)
                throw new IllegalStateException(
                    "exporters.otlp.tls: insecure: false requires an https:// endpoint, got: " + endpoint);
        }
    }
}
