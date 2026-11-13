package com.splunk.otel.db2.receiver.queries;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Reads the Db2 instance name — the {@code db2inst1} an administrator knows the instance by — for
 * the {@code db2.instance.name} resource attribute.
 *
 * <p>This cannot be derived from configuration. A Db2 Type 4 URL is
 * {@code jdbc:db2://host:port/DATABASE}: the trailing token is the database, and the instance is
 * identified only implicitly by the port, since each instance listens on its own SVCENAME. Deriving
 * a name from the URL would reproduce {@code service.instance.id} and carry no new information, so
 * the server is asked instead — the same approach sqlserverreceiver takes with {@code @@SERVERNAME}.
 *
 * <p>Returns null rather than throwing. The attribute is omitted when the value cannot be read, so a
 * Db2 that is unreachable or a monitoring user without access to the view costs the attribute and
 * nothing else. Note a receiver builds its resource once, when its pipeline is constructed, so a
 * name missed at startup stays
 * missing until restart.
 */
public final class InstanceInfoQuery {
    private static final Logger log = LoggerFactory.getLogger(InstanceInfoQuery.class);

    // INST_NAME only. SERVICE_LEVEL and the other columns are deliberately not selected: Oracle's
    // equivalent lookup documents that adding a column unconditionally fails the whole row on
    // releases that lack it, which would cost the instance name too.
    static final String SQL = "SELECT INST_NAME FROM SYSIBMADM.ENV_INST_INFO";

    private InstanceInfoQuery() {}

    /** The instance name, or null when it cannot be read. Never throws. */
    public static String fetchInstanceName(Connection conn, int queryTimeoutSeconds, String label) {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(queryTimeoutSeconds);
            try (ResultSet rs = st.executeQuery(SQL)) {
                if (!rs.next()) {
                    log.warn("{}ENV_INST_INFO returned no rows; db2.instance.name omitted", label);
                    return null;
                }
                String name = rs.getString("INST_NAME");
                // The column is declared far wider than any instance name, so values arrive
                // padded.
                if (name != null) name = name.trim();
                if (name == null || name.isEmpty()) {
                    log.warn("{}ENV_INST_INFO returned a blank INST_NAME; db2.instance.name omitted", label);
                    return null;
                }
                log.info("{}Db2 instance name: {}", label, name);
                return name;
            }
        } catch (Exception e) {
            log.warn("{}Could not read the Db2 instance name ({}); db2.instance.name omitted. "
                + "Verify SELECT on SYSIBMADM.ENV_INST_INFO.", label, e.getMessage());
            return null;
        }
    }
}
