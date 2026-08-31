package com.splunk.otel.db2.receiver;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Db2Connection {
    private static final Logger log = LoggerFactory.getLogger(Db2Connection.class);

    static final String APP_NAME = "splunk-otel-db2";

    // Reconnect backoff: 2s, 4s, 8s, 16s, 30s (cap)
    private static final long[] BACKOFF_SECONDS = {2, 4, 8, 16, 30};

    private final Db2ReceiverConfig config;
    private Connection connection;
    private int consecutiveFailures = 0;
    private long nextRetryAt = 0; // epoch ms; 0 = retry immediately

    public Db2Connection(Db2ReceiverConfig config) {
        this.config = config;
    }

    public synchronized Connection getOrConnect() throws SQLException {
        if (connection != null && connection.isValid(config.getConnectTimeoutSeconds())) {
            consecutiveFailures = 0;
            return connection;
        }
        long now = System.currentTimeMillis();
        if (consecutiveFailures > 0 && now < nextRetryAt) {
            throw new SQLException(String.format(
                "DB2 reconnect backing off; next attempt in %ds",
                (nextRetryAt - now) / 1000));
        }
        try {
            connect();
            consecutiveFailures = 0;
            nextRetryAt = 0;
        } catch (SQLException e) {
            consecutiveFailures++;
            long waitSeconds = BACKOFF_SECONDS[Math.min(consecutiveFailures - 1, BACKOFF_SECONDS.length - 1)];
            nextRetryAt = System.currentTimeMillis() + waitSeconds * 1000L;
            log.warn("Db2Connection: connect attempt {} failed; backing off {}s: {}",
                consecutiveFailures, waitSeconds, e.getMessage());
            throw e;
        }
        return connection;
    }

    private void connect() throws SQLException {
        if (connection != null) {
            try { connection.close(); } catch (SQLException ignored) {}
            connection = null;
        }
        String url = String.format("jdbc:db2://%s:%d/%s",
            config.getHost(), config.getPort(), config.getDatabase());
        log.info("Connecting to DB2 at {} (tls.insecure={})", url, config.getTls().isInsecure());
        Properties props = new Properties();
        props.setProperty("user",              config.getUsername());
        props.setProperty("password",          config.getPassword());
        props.setProperty("clientProgramName", APP_NAME);
        props.setProperty("loginTimeout",      String.valueOf(config.getConnectTimeoutSeconds()));
        props.setProperty("blockingReadConnectionTimeout", String.valueOf(config.getConnectTimeoutSeconds()));
        if (!config.getTls().isInsecure()) {
            props.setProperty("sslConnection", "true");
        }
        connection = DriverManager.getConnection(url, props);
        try (Statement st = connection.createStatement()) {
            st.execute("CALL WLM_SET_CLIENT_INFO(NULL, NULL, 'splunk-otel-db2', NULL, NULL)");
        }
        log.info("Connected to DB2 successfully");
    }

    public void close() {
        if (connection != null) {
            try { connection.close(); } catch (SQLException ignored) {}
        }
    }
}
