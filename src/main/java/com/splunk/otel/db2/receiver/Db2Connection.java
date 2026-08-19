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

    private final Db2ReceiverConfig config;
    private Connection connection;

    public Db2Connection(Db2ReceiverConfig config) {
        this.config = config;
    }

    public synchronized Connection getOrConnect() throws SQLException {
        if (connection == null || !connection.isValid(2)) {
            connect();
        }
        return connection;
    }

    private void connect() throws SQLException {
        String url = String.format("jdbc:db2://%s:%d/%s",
            config.getHost(), config.getPort(), config.getDatabase());
        log.info("Connecting to DB2 at {} (tls.insecure={})", url, config.getTls().isInsecure());
        Properties props = new Properties();
        props.setProperty("user", config.getUsername());
        props.setProperty("password", config.getPassword());
        props.setProperty("clientProgramName", APP_NAME);
        if (!config.getTls().isInsecure()) {
            props.setProperty("sslConnection", "true");
        }
        connection = DriverManager.getConnection(url, props);
        try (Statement st = connection.createStatement()) {
            st.execute("CALL WLM_SET_CLIENT_INFO(NULL, NULL, NULL, 'splunk-otel-db2', NULL)");
        }
        log.info("Connected to DB2 successfully");
    }

    public void close() {
        if (connection != null) {
            try { connection.close(); } catch (SQLException ignored) {}
        }
    }
}
