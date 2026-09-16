package com.splunk.otel.db2.receiver.queries;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.Severity;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Typed builder for a {@code db.server.blocking_session} OTel log record.
 *
 * Each record represents one blocked→blocker pair from {@code MON_LOCKWAITS}.
 * SQL text for both sides is obfuscated before emission.
 */
public final class BlockingSessionEvent {

    public static final AttributeKey<String> DB_SYSTEM              = AttributeKey.stringKey("db.system.name");
    public static final AttributeKey<String> DB_NAMESPACE           = AttributeKey.stringKey("db.namespace");
    public static final AttributeKey<Long>   DB2_MEMBER             = AttributeKey.longKey("db2.member");

    // Blocked session
    public static final AttributeKey<String> DB_QUERY_TEXT          = AttributeKey.stringKey("db.query.text");
    public static final AttributeKey<String> DB2_APP_HANDLE         = AttributeKey.stringKey("db2.session_id");
    public static final AttributeKey<String> DB2_ACTIVITY_STATE     = AttributeKey.stringKey("db2.activity_state");
    public static final AttributeKey<String> USER_NAME              = AttributeKey.stringKey("user.name");
    public static final AttributeKey<String> CLIENT_ADDRESS         = AttributeKey.stringKey("client.address");

    // Blocker session
    public static final AttributeKey<String> DB2_BLOCKER_APP_HANDLE = AttributeKey.stringKey("db2.blocking.blocker.session_id");
    public static final AttributeKey<String> DB2_BLOCKER_USER       = AttributeKey.stringKey("db2.blocking.blocker.user");
    public static final AttributeKey<String> DB2_BLOCKER_HOST       = AttributeKey.stringKey("db2.blocking.blocker.client_address");
    public static final AttributeKey<String> DB2_BLOCKER_QUERY_TEXT = AttributeKey.stringKey("db2.blocking.blocker.query_text");
    public static final AttributeKey<Boolean> DB2_BLOCKER_IS_IDLE   = AttributeKey.booleanKey("db2.blocking.blocker.is_idle");

    // Lock details
    public static final AttributeKey<String> DB2_LOCK_OBJECT_TYPE   = AttributeKey.stringKey("db2.blocking.lock.object_type");
    public static final AttributeKey<String> DB2_LOCK_MODE          = AttributeKey.stringKey("db2.blocking.lock.mode");
    public static final AttributeKey<String> DB2_LOCK_OBJECT_NAME   = AttributeKey.stringKey("db2.blocking.lock.object_name");
    public static final AttributeKey<String> DB2_LOCK_SCHEMA        = AttributeKey.stringKey("db2.blocking.lock.schema");
    public static final AttributeKey<String> DB2_BLOCKING_START     = AttributeKey.stringKey("db2.blocking.start_time");
    public static final AttributeKey<Double> DB2_WAIT_DURATION      = AttributeKey.doubleKey("db2.blocking.wait_duration");

    // Required
    private final String dbNamespace;
    private final long   member;
    private final String blockedAppHandle;
    private final String blockerAppHandle;

    // Optional — blocked side
    private String blockedQueryText;
    private String blockedUser;
    private String blockedHost;
    private String activityState;

    // Optional — blocker side
    private String blockerUser;
    private String blockerHost;
    private String blockerQueryText;
    private Boolean blockerIsIdle;

    // Optional — lock details
    private String lockObjectType;
    private String lockMode;
    private String lockObjectName;
    private String lockSchema;
    private String blockingStartTime;
    private Double waitDurationSec;

    private BlockingSessionEvent(String dbNamespace, long member,
                                  String blockedAppHandle, String blockerAppHandle) {
        this.dbNamespace       = dbNamespace;
        this.member            = member;
        this.blockedAppHandle  = blockedAppHandle;
        this.blockerAppHandle  = blockerAppHandle;
    }

    public static BlockingSessionEvent of(String dbNamespace, long member,
                                           String blockedAppHandle, String blockerAppHandle) {
        return new BlockingSessionEvent(dbNamespace, member, blockedAppHandle, blockerAppHandle);
    }

    public BlockingSessionEvent blockedQueryText(String v)  { this.blockedQueryText  = v; return this; }
    public BlockingSessionEvent blockedUser(String v)       { this.blockedUser        = v; return this; }
    public BlockingSessionEvent blockedHost(String v)       { this.blockedHost        = v; return this; }
    public BlockingSessionEvent activityState(String v)     { this.activityState      = v; return this; }
    public BlockingSessionEvent blockerUser(String v)       { this.blockerUser        = v; return this; }
    public BlockingSessionEvent blockerHost(String v)       { this.blockerHost        = v; return this; }
    public BlockingSessionEvent blockerQueryText(String v)  { this.blockerQueryText   = v; return this; }
    public BlockingSessionEvent blockerIsIdle(Boolean v)    { this.blockerIsIdle      = v; return this; }
    public BlockingSessionEvent lockObjectType(String v)    { this.lockObjectType     = v; return this; }
    public BlockingSessionEvent lockMode(String v)          { this.lockMode           = v; return this; }
    public BlockingSessionEvent lockObjectName(String v)    { this.lockObjectName     = v; return this; }
    public BlockingSessionEvent lockSchema(String v)        { this.lockSchema         = v; return this; }
    public BlockingSessionEvent blockingStartTime(String v) { this.blockingStartTime  = v; return this; }

    public BlockingSessionEvent waitDurationSeconds(double sec) {
        if (sec >= 0) this.waitDurationSec = sec;
        return this;
    }

    public void emit(Logger logger, Instant timestamp) {
        AttributesBuilder b = Attributes.builder()
            .put(DB_SYSTEM,        "ibm.db2")
            .put(DB_NAMESPACE,     dbNamespace)
            .put(DB2_MEMBER,       member)
            .put(DB2_APP_HANDLE,   blockedAppHandle)
            .put(DB2_BLOCKER_APP_HANDLE, blockerAppHandle);

        if (blockedQueryText  != null) b.put(DB_QUERY_TEXT,          blockedQueryText);
        if (blockedUser       != null) b.put(USER_NAME,              blockedUser);
        if (blockedHost       != null) b.put(CLIENT_ADDRESS,         blockedHost);
        if (activityState     != null) b.put(DB2_ACTIVITY_STATE,     activityState);
        if (blockerUser       != null) b.put(DB2_BLOCKER_USER,       blockerUser);
        if (blockerHost       != null) b.put(DB2_BLOCKER_HOST,       blockerHost);
        if (blockerQueryText  != null) b.put(DB2_BLOCKER_QUERY_TEXT, blockerQueryText);
        if (blockerIsIdle     != null) b.put(DB2_BLOCKER_IS_IDLE,   blockerIsIdle);
        if (lockObjectType    != null) b.put(DB2_LOCK_OBJECT_TYPE,   lockObjectType);
        if (lockMode          != null) b.put(DB2_LOCK_MODE,          lockMode);
        if (lockObjectName    != null) b.put(DB2_LOCK_OBJECT_NAME,   lockObjectName);
        if (lockSchema        != null) b.put(DB2_LOCK_SCHEMA,        lockSchema);
        if (blockingStartTime != null) b.put(DB2_BLOCKING_START,     blockingStartTime);
        if (waitDurationSec   != null) b.put(DB2_WAIT_DURATION,      waitDurationSec);

        logger.logRecordBuilder()
            .setEventName("db.server.blocking_session")
            .setSeverity(Severity.WARN)
            .setSeverityText("WARN")
            .setTimestamp(timestamp.toEpochMilli(), TimeUnit.MILLISECONDS)
            .setAllAttributes(b.build())
            .emit();
    }
}
