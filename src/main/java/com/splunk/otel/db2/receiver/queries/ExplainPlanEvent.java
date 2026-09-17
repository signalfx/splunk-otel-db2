package com.splunk.otel.db2.receiver.queries;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.Severity;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Typed builder for a {@code db.server.query_plan} OTel log record.
 *
 * Each record carries the compiled plan XML for one statement from the Db2 plan
 * cache, fetched via {@code MON_GET_PKG_CACHE_STMT_DETAILS}. The plan is read
 * from the cache — no query re-execution occurs.
 *
 * Attribute keys {@code db2.query_plan} and {@code db2.query_plan.hash} match the
 * keys registered in the dbmon-processor {@code QueryPlanExtractor}. The processor
 * strips the plan text, uploads it to object storage, and replaces it with an
 * {@code _dbmon.query_plan.object_key} pointer — so callers never see raw plan XML
 * in the final event stream.
 */
public final class ExplainPlanEvent {

    public static final AttributeKey<String> DB_SYSTEM        = AttributeKey.stringKey("db.system.name");
    public static final AttributeKey<String> DB_NAMESPACE     = AttributeKey.stringKey("db.namespace");
    public static final AttributeKey<String> DB_QUERY_TEXT    = AttributeKey.stringKey("db.query.text");
    public static final AttributeKey<Long>   DB2_MEMBER       = AttributeKey.longKey("db2.member");
    public static final AttributeKey<String> DB2_QUERY_PLAN   = AttributeKey.stringKey("db2.query_plan");
    public static final AttributeKey<String> DB2_PLAN_HASH    = AttributeKey.stringKey("db2.query_plan.hash");

    // Required
    private final String dbNamespace;
    private final String queryText;
    private final long   member;
    private final String planXml;
    private final String planHash;

    private ExplainPlanEvent(String dbNamespace, String queryText, long member,
                             String planXml, String planHash) {
        this.dbNamespace = dbNamespace;
        this.queryText   = queryText;
        this.member      = member;
        this.planXml     = planXml;
        this.planHash    = planHash;
    }

    public static ExplainPlanEvent of(String dbNamespace, String queryText,
                                      long member, String planXml, String planHash) {
        return new ExplainPlanEvent(dbNamespace, queryText, member, planXml, planHash);
    }

    public void emit(Logger logger, Instant timestamp) {
        AttributesBuilder b = Attributes.builder()
            .put(DB_SYSTEM,      "ibm.db2")
            .put(DB_NAMESPACE,   dbNamespace)
            .put(DB_QUERY_TEXT,  queryText)
            .put(DB2_MEMBER,     member)
            .put(DB2_QUERY_PLAN, planXml)
            .put(DB2_PLAN_HASH,  planHash);

        logger.logRecordBuilder()
            .setEventName("db.server.query_plan")
            .setSeverity(Severity.INFO)
            .setSeverityText("INFO")
            .setTimestamp(timestamp.toEpochMilli(), TimeUnit.MILLISECONDS)
            .setAllAttributes(b.build())
            .emit();
    }
}
