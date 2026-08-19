package com.splunk.otel.db2.receiver.queries;

import com.splunk.otel.db2.obfuscator.SqlObfuscator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.Severity;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.function.Supplier;

public class QuerySampleQuery {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(QuerySampleQuery.class);

    // Default path: MON_CURRENT_SQL + MON_GET_CONNECTION (DB2 >= 9.7)
    // Provides EXECUTING / WAITING states; no DFT_MON_STMT requirement
    private static final String MON_SQL =
        "SELECT S.APPLICATION_HANDLE, " +
        "S.STMT_TEXT, " +
        "S.ACTIVITY_STATE, " +
        "S.LOCAL_START_TIME, " +
        "TIMESTAMPDIFF(2, CHAR(CURRENT TIMESTAMP - S.LOCAL_START_TIME)) AS WALL_ELAPSED_SEC, " +
        "C.CLIENT_USERID, " +
        "C.CLIENT_WRKSTNNAME " +
        "FROM TABLE(MON_CURRENT_SQL(CAST(NULL AS VARCHAR(255)), -2)) AS S " +
        "JOIN TABLE(MON_GET_CONNECTION(NULL, -2)) AS C ON C.APPLICATION_HANDLE = S.APPLICATION_HANDLE " +
        "WHERE S.STMT_TEXT IS NOT NULL " +
        "AND S.ACTIVITY_STATE NOT IN ('IDLE') " +
        "AND COALESCE(C.CLIENT_APPLNAME, '') <> 'splunk-otel-db2' " +
        "AND S.STMT_TEXT NOT LIKE '%%MON_CURRENT_SQL%%' " +
        "AND S.STMT_TEXT NOT LIKE '%%SNAP_GET_APPL_INFO%%' " +
        "FETCH FIRST %d ROWS ONLY";

    // Snapshot path: enabled via use_snapshot_monitor: true config flag
    // Richer states (LOCKWAIT, UOWWAIT, etc.); requires DFT_MON_STMT ON; DB2 >= 9.1
    private static final String SNAP_SQL =
        "SELECT COALESCE(SS.STMT_TEXT, S.TEXT) AS STMT_TEXT, " +
        "( CASE WHEN SA.APPL_STATUS = 'UOWEXEC' THEN CAST(SS.STMT_OPERATION AS VARCHAR(22)) " +
        "       ELSE CAST(SA.APPL_STATUS AS VARCHAR(22)) END ) AS ACTIVITY_STATE, " +
        "SS.STMT_START AS LOCAL_START_TIME, " +
        "TIMESTAMPDIFF(2, CHAR(CURRENT TIMESTAMP - SS.STMT_START)) AS WALL_ELAPSED_SEC, " +
        "SA.EXECUTION_ID AS CLIENT_USERID, " +
        "SA.CLIENT_NNAME AS CLIENT_WRKSTNNAME, " +
        "SA.AGENT_ID AS APPLICATION_HANDLE " +
        "FROM SYSIBM.SYSDUMMY1, " +
        "TABLE(SNAP_GET_APPL_INFO(CAST(NULL AS VARCHAR(255)), -1)) AS SA, " +
        "TABLE(SNAP_GET_STMT(CAST(NULL AS VARCHAR(255)), -1)) AS SS " +
        "LEFT OUTER JOIN SYSCAT.STATEMENTS S " +
        "ON (S.PKGNAME = SS.PACKAGE_NAME AND S.SECTNO = SS.SECTION_NUMBER) " +
        "WHERE SS.STMT_STOP IS NULL " +
        "AND SA.AGENT_ID = SS.AGENT_ID " +
        "AND COALESCE(SA.APPL_NAME, '') <> 'splunk-otel-db2' " +
        "AND COALESCE(SS.STMT_TEXT, S.TEXT, '') NOT LIKE '%%SNAP_GET_APPL_INFO%%' " +
        "AND COALESCE(SS.STMT_TEXT, S.TEXT, '') NOT LIKE '%%SNAP_GET_STMT%%' " +
        "FETCH FIRST %d ROWS ONLY";

    private final Supplier<Connection> connSupplier;
    private final String dbName;
    private final Logger otelLogger;
    private final String sql;
    private final boolean useSnapshotMonitor;

    public QuerySampleQuery(Supplier<Connection> connSupplier, String dbName, Logger otelLogger,
                            int maxQuerySampleCount, boolean useSnapshotMonitor) {
        this.connSupplier       = connSupplier;
        this.dbName             = dbName;
        this.otelLogger         = otelLogger;
        this.useSnapshotMonitor = useSnapshotMonitor;
        this.sql = String.format(useSnapshotMonitor ? SNAP_SQL : MON_SQL, maxQuerySampleCount);
        log.info("QuerySampleQuery: using {} path", useSnapshotMonitor ? "SNAP_GET_*" : "MON_CURRENT_SQL");
    }

    public void collect() {
        try {
            Connection conn = connSupplier.get();
            try (PreparedStatement ps = conn.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {
                int count = emitRows(rs);
                log.debug("QuerySampleQuery: emitted {} samples", count);
            }
        } catch (Exception e) {
            log.error("QuerySampleQuery: failed: {}", e.getMessage());
        }
    }

    private int emitRows(ResultSet rs) throws Exception {
        int count = 0;
        while (rs.next()) {
            String stmtText   = rs.getString("STMT_TEXT");
            if (stmtText == null || stmtText.trim().isEmpty()) continue;
            count++;
            String appHandle  = rs.getString("APPLICATION_HANDLE");
            String state      = rs.getString("ACTIVITY_STATE");
            String startTime  = rs.getString("LOCAL_START_TIME");
            double elapsedSec = rs.getLong("WALL_ELAPSED_SEC");
            String authId     = rs.getString("CLIENT_USERID");
            String clientHost = rs.getString("CLIENT_WRKSTNNAME");

            String obfuscated = SqlObfuscator.getInstance().obfuscate(stmtText);

            otelLogger.logRecordBuilder()
                .setBody(obfuscated != null ? obfuscated.trim() : "")
                .setSeverity(Severity.INFO)
                .setAttribute(AttributeKey.stringKey("event.name"),                "db.server.query_sample")
                .setAttribute(AttributeKey.stringKey("db.system.name"),            "db2")
                .setAttribute(AttributeKey.stringKey("db.namespace"),              dbName)
                .setAttribute(AttributeKey.stringKey("db.query.text"),             obfuscated != null ? obfuscated.trim() : "")
                .setAttribute(AttributeKey.stringKey("db2.application_handle"),    appHandle != null ? appHandle : "")
                .setAttribute(AttributeKey.stringKey("db2.activity_state"),        state != null ? state : "")
                .setAttribute(AttributeKey.stringKey("db2.query_start"),           startTime != null ? startTime : "")
                .setAttribute(AttributeKey.doubleKey("db2.session.duration"),      elapsedSec)
                .setAttribute(AttributeKey.stringKey("user.name"),                 authId != null ? authId : "")
                .setAttribute(AttributeKey.stringKey("client.address"),            clientHost != null ? clientHost : "")
                .emit();
        }
        return count;
    }
}
