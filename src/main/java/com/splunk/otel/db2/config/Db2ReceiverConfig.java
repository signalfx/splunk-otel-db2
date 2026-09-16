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
    private int topQueryCount = 250;
    private boolean topQueryEnabled = false;
    private int maxQuerySampleCount = 1000;
    private boolean querySampleEnabled = false;
    private String blockingSessionInterval = "10s";
    private boolean useSnapshotMonitor = false;
    private String querySampleInterval = "10s";
    private String topQueryInterval = "60s";
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

    private static Duration parseDuration(String s, long defaultSeconds) {
        if (s == null) return Duration.ofSeconds(defaultSeconds);
        s = s.trim();
        Duration d;
        if (s.endsWith("s")) d = Duration.ofSeconds(Long.parseLong(s.substring(0, s.length() - 1)));
        else if (s.endsWith("m")) d = Duration.ofMinutes(Long.parseLong(s.substring(0, s.length() - 1)));
        else throw new IllegalArgumentException("Unsupported duration format '" + s + "': use '30s' or '5m'");
        if (d.getSeconds() <= 0) throw new IllegalArgumentException("Interval must be at least 1s, got '" + s + "'");
        return d;
    }

    public int getTopQueryCount() { return topQueryCount; }
    public void setTopQueryCount(int topQueryCount) {
        if (topQueryCount <= 0)
            throw new IllegalArgumentException("top_query_count must be > 0, got " + topQueryCount);
        this.topQueryCount = topQueryCount;
    }

    public boolean isTopQueryEnabled() { return topQueryEnabled; }
    public void setTopQueryEnabled(boolean topQueryEnabled) { this.topQueryEnabled = topQueryEnabled; }


    public int getMaxQuerySampleCount() { return maxQuerySampleCount; }
    public void setMaxQuerySampleCount(int maxQuerySampleCount) {
        if (maxQuerySampleCount <= 0) {
            throw new IllegalArgumentException("max_query_sample_count must be > 0, got " + maxQuerySampleCount);
        }
        this.maxQuerySampleCount = maxQuerySampleCount;
    }

    public boolean isQuerySampleEnabled() { return querySampleEnabled; }
    public void setQuerySampleEnabled(boolean querySampleEnabled) { this.querySampleEnabled = querySampleEnabled; }

    public Duration getBlockingSessionIntervalDuration() { return parseDuration(blockingSessionInterval, 10); }
    public void setBlockingSessionInterval(String blockingSessionInterval) { this.blockingSessionInterval = blockingSessionInterval; }

    public boolean isUseSnapshotMonitor() { return useSnapshotMonitor; }
    public void setUseSnapshotMonitor(boolean useSnapshotMonitor) { this.useSnapshotMonitor = useSnapshotMonitor; }

    public Duration getQuerySampleIntervalDuration() { return parseDuration(querySampleInterval, 10); }
    public void setQuerySampleInterval(String querySampleInterval) { this.querySampleInterval = querySampleInterval; }

    public Duration getTopQueryIntervalDuration() {
        Duration d = parseDuration(topQueryInterval, 60);
        if (d.getSeconds() < 60)
            throw new IllegalArgumentException("top_query_interval must be at least 60s, got '" + topQueryInterval + "'");
        return d;
    }
    public void setTopQueryInterval(String topQueryInterval) { this.topQueryInterval = topQueryInterval; }

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

    public static class TlsConfig {
        private boolean insecure = false;
        public boolean isInsecure() { return insecure; }
        public void setInsecure(boolean insecure) { this.insecure = insecure; }
    }
}
