package com.splunk.otel.db2.config;

import java.util.Collections;
import java.util.Map;

public class OtlpExporterConfig {
    private String endpoint = "http://localhost:4317";
    private Map<String, String> headers = Collections.emptyMap();
    private TlsConfig tls = new TlsConfig();
    private String compression = "";

    public String getEndpoint() { return endpoint; }
    public void setEndpoint(String endpoint) { this.endpoint = endpoint; }

    public Map<String, String> getHeaders() { return headers; }
    public void setHeaders(Map<String, String> headers) {
        this.headers = headers != null ? headers : Collections.emptyMap();
    }

    public TlsConfig getTls() { return tls; }
    public void setTls(TlsConfig tls) { this.tls = tls; }

    public String getCompression() { return compression; }
    public void setCompression(String compression) { this.compression = compression != null ? compression : ""; }

    public static class TlsConfig {
        private boolean insecure = false;
        private boolean insecureSet = false; // true only when explicitly provided in config
        private String caFile = "";
        private String certFile = "";
        private String keyFile = "";

        public boolean isInsecure() { return insecure; }
        public void setInsecure(boolean insecure) { this.insecure = insecure; this.insecureSet = true; }
        public boolean isTlsExplicitlySet() { return insecureSet; }

        public String getCaFile() { return caFile; }
        public void setCaFile(String caFile) { this.caFile = caFile != null ? caFile : ""; }

        public String getCertFile() { return certFile; }
        public void setCertFile(String certFile) { this.certFile = certFile != null ? certFile : ""; }

        public String getKeyFile() { return keyFile; }
        public void setKeyFile(String keyFile) { this.keyFile = keyFile != null ? keyFile : ""; }
    }
}
