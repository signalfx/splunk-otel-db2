package com.splunk.otel.db2.config;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

public class Db2ReceiverConfig {
    private String componentName = ""; // name after the slash in "db2/<name>"; empty for plain "db2"
    private String hostname = "localhost";
    private int hostport = 50000;
    private String username;
    private String password;
    private String database = "SAMPLE";
    private String collectionInterval = "30s";
    private boolean useSnapshotMonitor = false;
    private EventsConfig events = new EventsConfig();
    private TopQueryCollectionConfig topQueryCollection = new TopQueryCollectionConfig();
    private QuerySampleCollectionConfig querySampleCollection = new QuerySampleCollectionConfig();
    private int connectTimeoutSeconds = 10;
    private TlsConfig tls = new TlsConfig();
    private Map<String, String> resourceAttributes = Collections.emptyMap();
    // G1: explicit identity overrides — null means use the auto-generated value
    private String serviceName;        // overrides default "splunk-otel-db2"
    private String serviceNamespace;   // emitted only when set; useful in multi-cluster deployments
    private String serviceInstanceId;  // full override; bypasses host:port/db composition

    public String getComponentName() { return componentName; }
    public void setComponentName(String componentName) { this.componentName = componentName; }

    public String getHostname() { return resolveEnv(hostname); }
    public void setHostname(String hostname) { this.hostname = hostname; }

    public int getHostport() { return hostport; }
    public void setHostport(int hostport) { this.hostport = hostport; }

    public String getUsername() { return resolveEnv(username); }
    public void setUsername(String username) { this.username = username; }

    public String getPassword() { return resolveEnv(password); }
    public void setPassword(String password) { this.password = password; }

    public String getDatabase() { return resolveEnv(database); }
    public void setDatabase(String database) { this.database = database; }

    public Duration getCollectionIntervalDuration() {
        return parseDuration(collectionInterval, 60);
    }
    public void setCollectionInterval(String collectionInterval) { this.collectionInterval = collectionInterval; }

    static Duration parseDuration(String s, long defaultSeconds) {
        if (s == null) return Duration.ofSeconds(defaultSeconds);
        s = s.trim();
        Duration d;
        if (s.endsWith("s")) d = Duration.ofSeconds(Long.parseLong(s.substring(0, s.length() - 1)));
        else if (s.endsWith("m")) d = Duration.ofMinutes(Long.parseLong(s.substring(0, s.length() - 1)));
        else if (s.endsWith("h")) d = Duration.ofHours(Long.parseLong(s.substring(0, s.length() - 1)));
        else throw new IllegalArgumentException("Unsupported duration format '" + s + "': use '30s', '5m', or '1h'");
        if (d.getSeconds() <= 0) throw new IllegalArgumentException("Interval must be at least 1s, got '" + s + "'");
        return d;
    }

    public boolean isUseSnapshotMonitor() { return useSnapshotMonitor; }
    public void setUseSnapshotMonitor(boolean useSnapshotMonitor) { this.useSnapshotMonitor = useSnapshotMonitor; }

    public EventsConfig getEvents() { return events; }
    public void setEvents(EventsConfig events) {
        this.events = events != null ? events : new EventsConfig();
    }

    public TopQueryCollectionConfig getTopQueryCollection() { return topQueryCollection; }
    public void setTopQueryCollection(TopQueryCollectionConfig topQueryCollection) {
        this.topQueryCollection = topQueryCollection != null ? topQueryCollection : new TopQueryCollectionConfig();
    }

    public QuerySampleCollectionConfig getQuerySampleCollection() { return querySampleCollection; }
    public void setQuerySampleCollection(QuerySampleCollectionConfig querySampleCollection) {
        this.querySampleCollection = querySampleCollection != null ? querySampleCollection : new QuerySampleCollectionConfig();
    }

    public int getConnectTimeoutSeconds() { return connectTimeoutSeconds; }
    public void setConnectTimeoutSeconds(int connectTimeoutSeconds) {
        if (connectTimeoutSeconds <= 0)
            throw new IllegalArgumentException("connect_timeout_seconds must be > 0, got " + connectTimeoutSeconds);
        this.connectTimeoutSeconds = connectTimeoutSeconds;
    }

    public TlsConfig getTls() { return tls; }
    public void setTls(TlsConfig tls) { this.tls = tls; }

    public Map<String, String> getResourceAttributes() { return resourceAttributes; }
    public void setResourceAttributes(Map<String, String> resourceAttributes) {
        this.resourceAttributes = resourceAttributes != null ? resourceAttributes : Collections.emptyMap();
    }

    public String getServiceName() { return resolveEnv(serviceName); }
    public void setServiceName(String serviceName) { this.serviceName = serviceName; }

    public String getServiceNamespace() { return resolveEnv(serviceNamespace); }
    public void setServiceNamespace(String serviceNamespace) { this.serviceNamespace = serviceNamespace; }

    public String getServiceInstanceId() { return resolveEnv(serviceInstanceId); }
    public void setServiceInstanceId(String serviceInstanceId) { this.serviceInstanceId = serviceInstanceId; }

    // Convenience accessors for JDBC URL construction
    public String getHost() { return getHostname(); }
    public int getPort() { return getHostport(); }

    private String resolveEnv(String value) {
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

    // --- Events config (mirrors OTel LogsBuilderConfig.Events pattern) ---

    public static class EventConfig {
        private boolean enabled = false;
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
    }

    public static class EventsConfig {
        private EventConfig dbServerTopQuery    = new EventConfig();
        private EventConfig dbServerQuerySample = new EventConfig();

        public EventConfig getDbServerTopQuery()    { return dbServerTopQuery; }
        public void setDbServerTopQuery(EventConfig v) {
            this.dbServerTopQuery = v != null ? v : new EventConfig();
        }
        public EventConfig getDbServerQuerySample() { return dbServerQuerySample; }
        public void setDbServerQuerySample(EventConfig v) {
            this.dbServerQuerySample = v != null ? v : new EventConfig();
        }
    }

    // --- top_query_collection ---

    public static class TopQueryCollectionConfig {
        private String collectionInterval = "60s";
        private int topQueryCount = 250;
        private int maxQuerySampleCount = 1000;
        // 0 = plan collection disabled (no EXPLAIN_FROM_SECTION calls). Default 0 keeps plan collection
        // opt-in; set to a positive value to enable (e.g. 250 to match MongoDB receiver default).
        private int maxExplainEachInterval = 0;
        private int queryPlanCacheSize = 1000;
        private String queryPlanCacheTtl = "1h";
        // Schema where explain tables exist (created by SYSINSTALLOBJECTS).
        // Empty string means use the DB2 runtime default (USER).
        private String explainSchema = "";

        public Duration getCollectionIntervalDuration() {
            Duration d = Db2ReceiverConfig.parseDuration(collectionInterval, 60);
            if (d.getSeconds() < 60)
                throw new IllegalArgumentException(
                    "top_query_collection.collection_interval must be at least 60s, got '" + collectionInterval + "'");
            return d;
        }
        public void setCollectionInterval(String v) { this.collectionInterval = v; }

        public int getTopQueryCount() { return topQueryCount; }
        public void setTopQueryCount(int v) {
            if (v <= 0)
                throw new IllegalArgumentException("top_query_collection.top_query_count must be > 0, got " + v);
            this.topQueryCount = v;
        }

        public int getMaxQuerySampleCount() { return maxQuerySampleCount; }
        public void setMaxQuerySampleCount(int v) {
            if (v <= 0)
                throw new IllegalArgumentException("top_query_collection.max_query_sample_count must be > 0, got " + v);
            this.maxQuerySampleCount = v;
        }

        public int getMaxExplainEachInterval() { return maxExplainEachInterval; }
        public void setMaxExplainEachInterval(int v) {
            if (v < 0)
                throw new IllegalArgumentException("top_query_collection.max_explain_each_interval must be >= 0, got " + v);
            this.maxExplainEachInterval = v;
        }

        public int getQueryPlanCacheSize() { return queryPlanCacheSize; }
        public void setQueryPlanCacheSize(int v) {
            if (v <= 0)
                throw new IllegalArgumentException("top_query_collection.query_plan_cache_size must be > 0, got " + v);
            this.queryPlanCacheSize = v;
        }

        public Duration getQueryPlanCacheTtlDuration() {
            return Db2ReceiverConfig.parseDuration(queryPlanCacheTtl, 600);
        }
        public void setQueryPlanCacheTtl(String v) { this.queryPlanCacheTtl = v; }

        public String getExplainSchema() { return explainSchema; }
        public void setExplainSchema(String v) {
            String s = v == null ? "" : v.trim();
            if (!s.isEmpty() && !s.matches("[A-Za-z_$#@][A-Za-z0-9_$#@]*")) {
                throw new IllegalArgumentException(
                    "top_query_collection.explain_schema must be a valid SQL identifier, got '" + s + "'");
            }
            this.explainSchema = s;
        }
    }

    // --- query_sample_collection ---

    public static class QuerySampleCollectionConfig {
        /** Floor on the sampling interval. Below this the monitor queries dominate the instance. */
        static final long MIN_COLLECTION_INTERVAL_SECONDS = 5;

        private String collectionInterval = "10s";
        private int maxRowsPerQuery = 100;

        public Duration getCollectionIntervalDuration() {
            Duration d = Db2ReceiverConfig.parseDuration(collectionInterval, 10);
            if (d.getSeconds() < MIN_COLLECTION_INTERVAL_SECONDS) {
                throw new IllegalArgumentException(
                    "query_sample_collection.collection_interval must be at least "
                        + MIN_COLLECTION_INTERVAL_SECONDS + "s, got '" + collectionInterval + "'");
            }
            return d;
        }
        public void setCollectionInterval(String v) { this.collectionInterval = v; }

        public int getMaxRowsPerQuery() { return maxRowsPerQuery; }
        public void setMaxRowsPerQuery(int v) {
            if (v <= 0)
                throw new IllegalArgumentException("query_sample_collection.max_rows_per_query must be > 0, got " + v);
            this.maxRowsPerQuery = v;
        }
    }

    public static class TlsConfig {
        private boolean insecure = false;
        /**
         * Whether to skip checking that the certificate was issued for the host being
         * connected to. Named and sensed to match {@code exporters.otlp.tls} in this same
         * codebase and the collector's own database receivers, rather than introducing a
         * third vocabulary for the same concept.
         *
         * <p>Defaults to checking, because encryption without it does not stop an
         * impersonating server. Set true when connecting by IP or by an alias absent from
         * the certificate: that keeps the connection encrypted, unlike {@code insecure},
         * which drops TLS entirely.
         */
        private boolean insecureSkipVerify = false;
        /**
         * Certificate authority used to validate the server's certificate, typically an
         * internal CA mounted into the container. Without it the driver uses the JVM default
         * truststore, which will not contain an internal or self-signed Db2 certificate.
         */
        private String caFile = "";
        /** Truststore, for installs that already distribute one. No OTLP analogue. */
        private String trustStore = "";
        private String trustStorePassword = "";
        /** Defaults to the JVM's own default type when unset. */
        private String trustStoreType = "";

        public boolean isInsecure() { return insecure; }
        public void setInsecure(boolean insecure) { this.insecure = insecure; }
        public boolean isInsecureSkipVerify() { return insecureSkipVerify; }
        public void setInsecureSkipVerify(boolean v) { this.insecureSkipVerify = v; }
        public String getCaFile() { return caFile; }
        public void setCaFile(String caFile) { this.caFile = caFile != null ? caFile : ""; }
        public String getTrustStore() { return trustStore; }
        public void setTrustStore(String v) { this.trustStore = v != null ? v : ""; }
        public String getTrustStorePassword() { return trustStorePassword; }
        public void setTrustStorePassword(String v) { this.trustStorePassword = v != null ? v : ""; }
        public String getTrustStoreType() { return trustStoreType; }
        public void setTrustStoreType(String v) { this.trustStoreType = v != null ? v : ""; }
    }
}
