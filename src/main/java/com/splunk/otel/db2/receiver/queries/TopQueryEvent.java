package com.splunk.otel.db2.receiver.queries;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.Severity;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Typed builder for a {@code db.server.top_query} OTel log record.
 *
 * Each record represents one statement from the Db2 plan cache
 * ({@code MON_GET_PKG_CACHE_STMT}), ordered by total execution time.
 * SQL text is always obfuscated before emission.
 */
public final class TopQueryEvent {

    public static final AttributeKey<String> DB_SYSTEM        = AttributeKey.stringKey("db.system.name");
    public static final AttributeKey<String> DB_NAMESPACE     = AttributeKey.stringKey("db.namespace");
    public static final AttributeKey<String> DB_QUERY_TEXT    = AttributeKey.stringKey("db.query.text");
    public static final AttributeKey<Long>   DB2_MEMBER       = AttributeKey.longKey("db2.member");
    public static final AttributeKey<Long>   DB2_EXECUTIONS   = AttributeKey.longKey("db2.executions");
    public static final AttributeKey<Double> DB2_TOTAL_EXEC_TIME = AttributeKey.doubleKey("db2.total_exec_time");
    public static final AttributeKey<Double> DB2_AVG_EXEC_TIME   = AttributeKey.doubleKey("db2.avg_exec_time");
    public static final AttributeKey<Double> DB2_TOTAL_CPU_TIME  = AttributeKey.doubleKey("db2.total_cpu_time");
    public static final AttributeKey<Long>   DB2_ROWS_READ     = AttributeKey.longKey("db2.rows_read");
    public static final AttributeKey<Long>   DB2_ROWS_RETURNED = AttributeKey.longKey("db2.rows_returned");
    public static final AttributeKey<Long>   DB2_SORTS           = AttributeKey.longKey("db2.sorts");
    public static final AttributeKey<Long>   DB2_SORT_OVERFLOWS  = AttributeKey.longKey("db2.sort_overflows");
    public static final AttributeKey<Long>   DB2_ROWS_MODIFIED   = AttributeKey.longKey("db2.rows_modified");
    public static final AttributeKey<String> DB2_STMT_TYPE       = AttributeKey.stringKey("db2.stmt_type");

    // Required
    private final String dbNamespace;
    private final String queryText;
    private final long   member;
    private final long   executions;
    private final double totalExecTimeSec;

    // Optional
    private Double avgExecTimeSec;
    private Double totalCpuTimeSec;
    private Long   rowsRead;
    private Long   rowsReturned;
    private Long   rowsModified;
    private Long   sorts;
    private Long   sortOverflows;
    private String stmtType;

    private TopQueryEvent(String dbNamespace, String queryText, long member,
                          long executions, double totalExecTimeSec) {
        this.dbNamespace       = dbNamespace;
        this.queryText         = queryText;
        this.member            = member;
        this.executions        = executions;
        this.totalExecTimeSec  = totalExecTimeSec;
    }

    public static TopQueryEvent of(String dbNamespace, String queryText, long member,
                                   long executions, double totalExecTimeSec) {
        return new TopQueryEvent(dbNamespace, queryText, member, executions, totalExecTimeSec);
    }

    /** Pass average execution time already in seconds. */
    public TopQueryEvent avgExecTimeSec(double sec) {
        if (sec >= 0) this.avgExecTimeSec = sec;
        return this;
    }

    /** Pass total CPU time in microseconds. */
    public TopQueryEvent totalCpuTimeMicros(long us) {
        if (us >= 0) this.totalCpuTimeSec = us / 1_000_000.0;
        return this;
    }

    public TopQueryEvent rowsRead(long v)        { this.rowsRead       = v; return this; }
    public TopQueryEvent rowsReturned(long v)    { this.rowsReturned   = v; return this; }
    public TopQueryEvent rowsModified(long v)    { this.rowsModified   = v; return this; }
    public TopQueryEvent sorts(long v)           { this.sorts          = v; return this; }
    public TopQueryEvent sortOverflows(long v)   { this.sortOverflows  = v; return this; }
    public TopQueryEvent stmtType(String v)      { this.stmtType       = v; return this; }

    public void emit(Logger logger, Instant timestamp) {
        AttributesBuilder b = Attributes.builder()
            .put(DB_SYSTEM,           "db2")
            .put(DB_NAMESPACE,        dbNamespace)
            .put(DB_QUERY_TEXT,       queryText)
            .put(DB2_MEMBER,          member)
            .put(DB2_EXECUTIONS,      executions)
            .put(DB2_TOTAL_EXEC_TIME, totalExecTimeSec);

        if (avgExecTimeSec  != null) b.put(DB2_AVG_EXEC_TIME,    avgExecTimeSec);
        if (totalCpuTimeSec != null) b.put(DB2_TOTAL_CPU_TIME,   totalCpuTimeSec);
        if (rowsRead        != null) b.put(DB2_ROWS_READ,         rowsRead);
        if (rowsReturned    != null) b.put(DB2_ROWS_RETURNED,     rowsReturned);
        if (rowsModified    != null) b.put(DB2_ROWS_MODIFIED,     rowsModified);
        if (sorts           != null) b.put(DB2_SORTS,             sorts);
        if (sortOverflows   != null) b.put(DB2_SORT_OVERFLOWS,    sortOverflows);
        if (stmtType        != null) b.put(DB2_STMT_TYPE,         stmtType);

        logger.logRecordBuilder()
            .setEventName("db.server.top_query")
            .setSeverity(Severity.INFO)
            .setSeverityText("INFO")
            .setTimestamp(timestamp.toEpochMilli(), TimeUnit.MILLISECONDS)
            .setAllAttributes(b.build())
            .emit();
    }
}
