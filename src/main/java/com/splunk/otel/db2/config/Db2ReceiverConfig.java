package com.splunk.otel.db2.config;

import java.time.Duration;

public class Db2ReceiverConfig {
    private String hostname = "localhost";
    private int hostport = 50000;
    private String username;
    private String password;
    private String database = "SAMPLE";
    private String collectionInterval = "30s";
    private int topQueryCount = 250;
    private int maxQuerySampleCount = 1000;
    private boolean querySampleEnabled = true;
    private boolean useSnapshotMonitor = false;
    private String querySampleInterval = "10s";
    private String topQueryInterval = "60s";
    private TlsConfig tls = new TlsConfig();

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
    public void setTopQueryCount(int topQueryCount) { this.topQueryCount = topQueryCount; }

    public int getMaxQuerySampleCount() { return maxQuerySampleCount; }
    public void setMaxQuerySampleCount(int maxQuerySampleCount) {
        if (maxQuerySampleCount <= 0) {
            throw new IllegalArgumentException("max_query_sample_count must be > 0, got " + maxQuerySampleCount);
        }
        this.maxQuerySampleCount = maxQuerySampleCount;
    }

    public boolean isQuerySampleEnabled() { return querySampleEnabled; }
    public void setQuerySampleEnabled(boolean querySampleEnabled) { this.querySampleEnabled = querySampleEnabled; }

    public boolean isUseSnapshotMonitor() { return useSnapshotMonitor; }
    public void setUseSnapshotMonitor(boolean useSnapshotMonitor) { this.useSnapshotMonitor = useSnapshotMonitor; }

    public Duration getQuerySampleIntervalDuration() { return parseDuration(querySampleInterval, 10); }
    public void setQuerySampleInterval(String querySampleInterval) { this.querySampleInterval = querySampleInterval; }

    public Duration getTopQueryIntervalDuration() { return parseDuration(topQueryInterval, 60); }
    public void setTopQueryInterval(String topQueryInterval) { this.topQueryInterval = topQueryInterval; }

    public TlsConfig getTls() { return tls; }
    public void setTls(TlsConfig tls) { this.tls = tls; }

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
