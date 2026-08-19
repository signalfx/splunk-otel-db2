package com.splunk.otel.db2.config;

public class OtlpExporterConfig {
    private String endpoint = "http://localhost:4317";

    public String getEndpoint() { return endpoint; }
    public void setEndpoint(String endpoint) { this.endpoint = endpoint; }
}
