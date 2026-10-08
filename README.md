# splunk-otel-db2

A standalone OpenTelemetry receiver for IBM Db2. Collects metrics and active query samples from a Db2 database and exports them via OTLP gRPC to an OpenTelemetry Collector or Splunk Observability Cloud.

## Requirements

- Java 11+
- IBM Db2 9.7+ (current metric scrapers require 10.5+; 9.7–10.1 support planned via snapshot monitor fallback; integration-tested against 11.5)
- Docker (for integration tests)
- Helm 3.12+ (for the Helm chart; it uses `.Chart.IsRoot` to tell a standalone install from a subchart)

## Quick start

```bash
# Build
make build

# Configure — copy the example and fill in your values
cp testdata/config.yaml my-config.yaml
# Edit my-config.yaml with your DB2 host, credentials, and OTLP endpoint

# Run
DB2_USER=db2inst1 DB2_PASSWORD=secret ./gradlew run --args="my-config.yaml"
```

## Configuration

### Single database

```yaml
receivers:
  db2:
    hostname: "localhost"              # DB2 host
    hostport: 50000                    # DB2 port (default 50000)
    username: "${env:DB2_USER}"        # supports ${env:VAR} substitution
    password: "${env:DB2_PASSWORD}"
    database: "SAMPLE"                 # database name (uppercase)
    collection_interval: "30s"         # metrics collection interval
    connect_timeout_seconds: 10        # JDBC login + socket timeout (default: 10)
    events:
      db.server.top_query:
        enabled: false                 # set true to enable top query collection (opt-in)
      db.server.query_sample:
        enabled: false                 # set true to enable query sampling (opt-in)
    top_query_collection:
      collection_interval: "60s"      # how often to collect top queries (minimum 60s)
      top_query_count: 250            # top N queries to emit per interval (must be > 0)
      max_query_sample_count: 1000    # max rows fetched from MON_GET_PKG_CACHE_STMT (must be > 0)
      max_explain_each_interval: 0    # max EXPLAIN_FROM_SECTION calls per interval; 0 = plan disabled (default)
      query_plan_cache_size: 1000     # max entries in the client-side plan cache
      query_plan_cache_ttl: "1h"      # how long to cache a plan before re-explaining
      explain_schema: ""              # schema where explain tables live (default: USER)
    query_sample_collection:
      collection_interval: "10s"      # how often to sample active queries
      max_rows_per_query: 100         # max active sessions fetched per sample
    use_snapshot_monitor: false        # set true for the SNAP_GET_STMT query sample path,
                                       # and to add SNAP_GET_DB SQL activity metrics
    tls:
      insecure: true                   # set false to enable TLS

exporters:
  otlp:
    endpoint: "http://localhost:4317"
```

### Multiple databases

Use the `receiver/name` component ID syntax — the same pattern used by the OTel Collector. Each key under `receivers` is an independent pipeline with its own JDBC connections, schedulers, and OTel SDK instance. All pipelines share the same OTLP exporter endpoint.

```yaml
receivers:
  db2/prod:
    hostname: "db2-prod-1.example.com"
    hostport: 50000
    username: "${env:DB2_USER}"
    password: "${env:DB2_PASSWORD}"
    database: "PROD"
    collection_interval: "30s"
    connect_timeout_seconds: 10
    tls:
      insecure: false

  db2/analytics:
    hostname: "db2-prod-2.example.com"
    hostport: 50000
    username: "${env:DB2_USER2}"
    password: "${env:DB2_PASSWORD2}"
    database: "ANALYTICS"
    collection_interval: "60s"
    connect_timeout_seconds: 10
    tls:
      insecure: false

exporters:
  otlp:
    endpoint: "http://localhost:4317"
```

Each pipeline is identified by the `service.instance.id` resource attribute. For remote targets this is `host:port/database`. For loopback targets (`localhost`, `127.0.0.1`, `::1`, or blank) the configured hostname is replaced with the collector machine's own hostname, so co-located sidecars produce unique identities across the fleet.

### Identity overrides

Three fields give explicit control over the OTel resource identity, useful for multi-cluster deployments:

| Field | Config key | Purpose |
|---|---|---|
| `service_name` | `service_name` | Override the default `service.name` (`splunk-otel-db2`) |
| `service_namespace` | `service_namespace` | Set `service.namespace` (omitted when not configured) |
| `service_instance_id` | `service_instance_id` | Full override of `service.instance.id`; bypasses the auto-generated `host:port/database` |

All three support `${env:VAR}` substitution.

```yaml
receivers:
  db2/prod:
    hostname: "db2-prod-1.example.com"
    database: "PROD"
    username: "${env:DB2_USER}"
    password: "${env:DB2_PASSWORD}"
    service_name: "payments-db2"
    service_namespace: "payments"
    service_instance_id: "payments-db2-prod-1"

exporters:
  otlp:
    endpoint: "http://localhost:4317"
```

### Resource attribute overrides

Use `resource_attributes` to override individual resource attributes or add custom ones. Values support `${env:VAR}` substitution. Overrides are applied after the identity fields above.

```yaml
receivers:
  db2/prod:
    hostname: "db2-prod-1.example.com"
    database: "PROD"
    username: "${env:DB2_USER}"
    password: "${env:DB2_PASSWORD}"
    resource_attributes:
      deployment.environment: "production"
      team: "platform"

exporters:
  otlp:
    endpoint: "http://localhost:4317"
```

Standard attributes (`service.name`, `service.instance.id`, `server.address`, `db.system.name`, `db.namespace`) can be overridden. `server.port` must be an integer value. All overrides are applied on top of the defaults — unspecified attributes keep their automatically derived values.

Credential values can be literals or `${env:VAR_NAME}` references.

## Resource usage

Each monitored database uses 1–3 JDBC connections depending on which collectors are enabled:

| Collector | Connection | Always open? |
|---|---|---|
| Metrics (DatabaseQuery, BufferpoolQuery, TablespaceQuery, SqlActivityQuery, SortAndDirectIoQuery, TransactionLogQuery) | `metricsConnection` | Yes |
| Query sample + Blocking session (`events.db.server.query_sample.enabled: true`) | `querySampleConnection` | Optional |
| Top query + Plan (`events.db.server.top_query.enabled: true`) | `topQueryConnection` | Optional |

Plan collection (`max_explain_each_interval > 0`) reuses the top-query connection — it does **not** open a fourth connection.

There is no connection pool — the monitoring workload fires queries at intervals of 10–60 seconds, which is too infrequent for pooling to help. Connections are validated before each use (`isValid`) and reconnected automatically with exponential backoff (2s → 4s → 8s → 16s → 30s cap) on failure. A deadline-based fail-fast prevents every metric callback from blocking during the backoff window — only the first caller after the deadline attempts reconnection.

Heap grows with the number of configured databases: each database gets its own OTel SDK pipeline, metric readers, OTLP exporters, schedulers, and in-memory caches (top-query delta baseline, query-sample deduplication). At steady state expect roughly 50–80 MB base plus ~15–30 MB per additional database. The dominant per-cycle allocation is the `MON_GET_PKG_CACHE_STMT` result set (all matching rows are loaded; `top_query_count` limits how many are emitted after sorting) and the delta baseline map (bounded by `MAX_STALE_CYCLES` eviction).

There is no hard limit on the number of databases in a single process. Beyond ~20–30 databases, heap pressure becomes the practical ceiling — raise `-Xmx` or split databases across multiple processes as needed.

## Metrics

All metrics carry `db.namespace` (database name) and `db2.member` (member number) attributes.

### Database

| Metric | Type | Unit | Description |
|---|---|---|---|
| `db2.connection.count` | UpDownCounter | `{connection}` | Current connections |
| `db2.connection.active` | UpDownCounter | `{connection}` | Connections executing in the engine |
| `db2.connection.max` | Gauge | `{connection}` | Peak simultaneous connections |
| `db2.lock.wait.count` | Counter | `{wait}` | Total lock waits |
| `db2.lock.wait_time` | Counter | `s` | Total time spent waiting for locks |
| `db2.lock.timeout.count` | Counter | `{timeout}` | Lock timeouts |
| `db2.lock.escalation.count` | Counter | `{escalation}` | Lock escalations (row → table) |
| `db2.lock.space_used` | UpDownCounter | `By` | Lock list memory in use |
| `db2.deadlock.count` | Counter | `{deadlock}` | Total deadlocks |
| `db2.sort.count` | Counter | `{sort}` | Total sorts executed |
| `db2.sort.overflow.count` | Counter | `{sort}` | Sorts that spilled to disk |

### Buffer pool

Additional attributes: `db2.bufferpool` (pool name), `db2.read_type` (`data`, `index`, `xda`).

| Metric | Type | Unit | Description |
|---|---|---|---|
| `db2.buffer_pool.logical_reads` | Counter | `{read}` | Logical page reads |
| `db2.buffer_pool.physical_reads` | Counter | `{read}` | Physical page reads from disk |
| `db2.buffer_pool.writes` | Counter | `{write}` | Pages written to disk (data + index + XDA) |
| `db2.buffer_pool.read_time` | Counter | `s` | Time spent reading from disk |
| `db2.buffer_pool.write_time` | Counter | `s` | Time spent writing to disk |

### Tablespace

Additional attributes: `db2.tablespace`, `db2.tablespace.type`, `db2.tablespace.state`, `db2.tablespace.usage` (`capacity`, `used`, `free`).

| Metric | Type | Unit | Description |
|---|---|---|---|
| `db2.tablespace.size` | UpDownCounter | `By` | Tablespace size broken down by usage |

### SQL and row activity

Additional attributes: `db2.statement_type` (`dynamic`, `static`), `db2.operation_type` (`select`, `uid`, `ddl`), `db2.transaction_type` (`commit`, `rollback`), `db2.row_operation` (`inserted`, `updated`, `deleted`), `db2.internal` (`true` when initiated by the database manager rather than an application).

| Metric | Type | Unit | Description |
|---|---|---|---|
| `db2.sql.statement.count` | Counter | `{statement}` | SQL statements attempted, by how the statement was prepared |
| `db2.sql.operation.count` | Counter | `{statement}` | SQL statements executed, by the kind of operation performed |
| `db2.sql.failed.count` | Counter | `{statement}` | SQL statements that failed |
| `db2.transaction.count` | Counter | `{transaction}` | Transactions committed or rolled back |
| `db2.row.count` | Counter | `{row}` | Rows inserted, updated or deleted |
| `db2.row.modified.count` | Counter | `{row}` | Rows changed, including by triggers and referential integrity |
| `db2.row.read.count` | Counter | `{row}` | Rows read from tables |
| `db2.row.returned.count` | Counter | `{row}` | Rows returned to clients |
| `db2.sql.compilation.count` | Counter | `{compilation}` | SQL statement compilations |
| `db2.sql.compilation.time` | Counter | `s` | Time spent compiling SQL statements |
| `db2.sql.bind.count` | Counter | `{bind}` | Binds and precompiles attempted |
| `db2.sql.rebind.count` | Counter | `{rebind}` | Packages implicitly rebound |
| `db2.deadlock.rollback.count` | Counter | `{rollback}` | Internal rollbacks caused by a deadlock (requires `use_snapshot_monitor`) |

`db2.row.modified.count` is not the total of `db2.row.count`: DB2 counts rows changed by triggers and referential integrity enforcement there too, so it can be larger.

`db2.sql.statement.count` and `db2.sql.operation.count` classify the same statements two different ways — a dynamic `SELECT` is counted once in each — so they are separate metrics and must not be added together. For the same reason `db2.row.count` carries only the mutually exclusive insert/update/delete partition, with rows read and rows returned as their own metrics.

Enabling `use_snapshot_monitor` adds only `db2.deadlock.rollback.count`; the remaining snapshot columns duplicate values already available natively and are deliberately not emitted.

### Sort and direct I/O

Additional attribute: `disk.io.direction` (`read`, `write`) — the standard semantic-convention attribute, so it is not `db2`-prefixed.

| Metric | Type | Unit | Description |
|---|---|---|---|
| `db2.sort.heap.allocated` | UpDownCounter | `By` | Sort heap space allocated |
| `db2.sort.shared.heap.allocated` | UpDownCounter | `By` | Shared sort memory allocated |
| `db2.sort.shared.heap.max` | Gauge | `By` | Shared sort memory high watermark |
| `db2.sort.time` | Counter | `s` | Time spent performing section sorts |
| `db2.sort.active` | UpDownCounter | `{sort}` | Sorts currently active |
| `db2.hash_join.active` | UpDownCounter | `{hash_join}` | Hash joins currently active |
| `db2.olap_function.active` | UpDownCounter | `{function}` | OLAP functions currently active |
| `db2.direct.io.count` | Counter | `{operation}` | Read/write operations bypassing the buffer pool |
| `db2.direct.io.request.count` | Counter | `{request}` | Direct read/write requests |
| `db2.direct.io.time` | Counter | `s` | Time spent on direct reads/writes |

All sort memory metrics are reported in bytes. DB2 expresses these elements in 4KB pages: `db2.sort.shared.heap.*` state that explicitly, and for `db2.sort.heap.allocated` the same factor follows from its documented comparability with the `sheapthres` configuration parameter and from the sibling `SORT_HEAP_TOP` element.

### Transaction log

Additional attribute: `disk.io.direction` (`read`, `write`).

| Metric | Type | Unit | Description |
|---|---|---|---|
| `db2.log.space.available` | UpDownCounter | `By` | Active log space not held by uncommitted transactions |
| `db2.log.space.used` | UpDownCounter | `By` | Active log space currently used |
| `db2.log.space.used.max` | Gauge | `By` | Peak total log space used, including secondary |
| `db2.log.space.secondary.used.max` | Gauge | `By` | Peak secondary log space used |
| `db2.log.secondary_files.allocated` | UpDownCounter | `{file}` | Secondary log files currently in use |
| `db2.log.io.pages` | Counter | `{page}` | Log pages read from / written to disk |
| `db2.log.io.time` | Counter | `s` | Logger time spent reading from / writing to disk |
| `db2.log.io.count` | Counter | `{operation}` | Logger I/O requests |
| `db2.log.partial_page_io.count` | Counter | `{operation}` | Logger writes of partial log data |
| `db2.log.buffer_full.count` | Counter | `{wait}` | Times agents waited because the log buffer was full while copying records into it |
| `db2.log.buffer_hit.count` | Counter | `{read}` | Times an agent read log data from the log buffer |
| `db2.log.recovery.redo` | UpDownCounter | `By` | Log that would be redone for crash recovery |
| `db2.log.dirty_pages.held` | UpDownCounter | `By` | Log held by the oldest dirty page |
| `db2.log.hadr.wait.time` | Counter | `s` | Logger time waiting on HADR log replication |
| `db2.log.hadr.wait.count` | Counter | `{wait}` | Times the logger waited on HADR log replication |
| `db2.log.disk_wait.time` | Counter | `s` | Agent time waiting for log writes to disk |
| `db2.log.disk_wait.count` | Counter | `{wait}` | Times agents waited for log writes to disk |

`db2.log.space.secondary.used.max` is a subset of `db2.log.space.used.max`, not a separate pool — DB2's total log space includes secondary, so the two must not be added together.

Active log file numbers are identifiers rather than measurements and are not emitted as metrics.

## Query samples (log records)

Active statements are sampled at `query_sample_collection.collection_interval` and emitted as OTel log records (severity INFO). Disabled by default — set `events.db.server.query_sample.enabled: true` to opt in. SQL text is obfuscated before emission — literal values are replaced with `?`.

The log record's native `event_name` field is set to `db.server.query_sample` (not stored as an attribute).

| Attribute | Description |
|---|---|
| `db.system.name` | Always `ibm.db2` |
| `db.namespace` | Database name |
| `db.query.text` | Obfuscated SQL statement |
| `db.operation.name` | SQL operation type (`READ`, `WRITE`, `DDL`, …); omitted on snapshot path |
| `db2.session_id` | Session identifier (DB2 application handle) |
| `db2.activity_state` | Activity state (`EXECUTING`, `LOCKWAIT`, …) |
| `db2.session_status` | Coarse session lifecycle state derived from `ACTIVITY_STATE`: `active`, `waiting`, or `idle` |
| `db2.query_start` | Statement start timestamp |
| `db2.session.duration` | Wall-clock elapsed time in seconds |
| `db2.member` | Db2 member number |
| `db2.cpu_time` | CPU time in seconds (default path only; omitted on snapshot path) |
| `db2.wait_time` | Lock wait time in seconds (Db2 10.5+ default path only) |
| `db2.query_plan.hash` | Db2 EXECUTABLE_ID hex string identifying the active plan section; Db2 10.5+ default path only |
| `user.name` | Authenticated user ID |
| `client.address` | Client workstation name |

**Default path** (`use_snapshot_monitor: false`, Db2 9.7+): `SYSIBMADM.MON_CURRENT_SQL` view + `MON_GET_CONNECTION` TVF JOIN. No `DFT_MON_STMT` requirement. Filters out IDLE sessions and the receiver's own queries.

**Snapshot monitor fallback** (`use_snapshot_monitor: true`, Db2 9.7+): `SNAP_GET_APPL_INFO` + `SNAP_GET_STMT` + `SYSCAT.STATEMENTS`. Requires `DFT_MON_STMT ON`. Provides richer states (`LOCKWAIT`, `UOWWAIT`, etc.) and static SQL fallback.

### Trace correlation

If the sampled application propagates a W3C traceparent into its Db2 session's client accounting string, the query sample record is linked to that trace — `trace_id` and `span_id` are set as native log record fields (not attributes), so the sampled statement appears on the trace of the request that issued it.

The receiver needs no additional configuration and no new grants for this — `CLIENT_ACCTNG` comes from `MON_GET_CONNECTION`, which query sampling already reads — and it activates automatically whenever the value is present. Applications opt in by stamping the accounting string on the same connection they then query on, before each query:

```sql
CALL SYSPROC.WLM_SET_CLIENT_INFO(NULL, NULL, NULL, 'traceparent=00-<trace-id>-<span-id>-01', NULL);
```

The accounting string is parameter 4; parameter 5 is `client_workload` and rejects arbitrary text.

Note this is a privileged call on the *application's* side: Db2 grants no default `PUBLIC` privilege on `SYSPROC.WLM_SET_CLIENT_INFO`, so the application user needs `EXECUTE` on it or a sufficient authority. Where that is not available, `connection.setClientInfo("ClientAccountingInformation", ...)` over JDBC produces an identical `CLIENT_ACCTNG` value with no stored-procedure dependency, and is the better default for restricted application users.

`CLIENT_ACCTNG` is read from `MON_GET_CONNECTION` on the default path and from `SNAP_GET_APPL_INFO.TPMON_ACC_STR` on the snapshot path. Values that are absent or not a valid traceparent are ignored and the record is emitted unlinked; parsing follows the W3C grammar, so a malformed value never affects any other record. The prefix match is case-insensitive.

Only sessions that are currently in a request are linked — those reported as `active` or `waiting`. Because `CLIENT_ACCTNG` outlives the request that set it, an idle session still holds the last traceparent written to it, and linking that would parent the sample to a trace that has already finished. Lock-waiting sessions *are* linked: the statement is executing, just blocked. Sessions in states the receiver cannot positively identify as in-request are emitted unlinked.

**This takes ownership of `CLIENT_ACCTNG`.** The field has its own pre-existing uses — Db2 can assign workloads on `CURRENT CLIENT_ACCTNG`, and it is recorded in audit events — and the traceparent format above occupies the whole value, so it cannot coexist with an application's own accounting string. Adopting trace correlation means:

- A traced request **replaces** whatever accounting value the application would otherwise set, which can change WLM workload assignment and what appears in audit records. Check for workload definitions or audit policies that read `CLIENT_ACCTNG` before enabling it.
- An application that needs both has to decide what the field carries. Whether to define a structured value holding a traceparent alongside other fields is still open — see the design doc's open question on this.

**Connection pooling.** `CLIENT_ACCTNG` is connection-scoped and persists until it is replaced, so the stamp and the query must share one physical connection, and every checkout must write the field. Skipping the write leaves the previous borrower's traceparent in place and attributes this request's queries to that request's trace — worse than no linkage, because the result looks correct. On each checkout write:

- the current traceparent, for a traced request;
- otherwise the application's normal accounting value, if it has one — restoring it rather than blanking it keeps WLM and audit behaviour intact;
- an empty string only when the application has no accounting value of its own.

Clearing or restoring the field before returning the connection to the pool is a cheap extra safeguard.

The two stamping mechanisms have **opposite null semantics**, verified on 11.5.9.0:

| | `WLM_SET_CLIENT_INFO` (parameter 4) | JDBC `setClientInfo` |
|---|---|---|
| `NULL` / Java `null` | leaves the existing value **unchanged** | **clears** the value |
| `''` (empty string) | clears the value | clears the value |

So `NULL` is not a way to clear the field via the stored procedure, though `null` does clear it over JDBC. Use `''` for both if you want one code path that behaves the same either way.

A minimal Java example, stamping on borrow and clearing on release:

```java
Connection conn = pool.borrow();
try {
    SpanContext sc = Span.current().getSpanContext();
    String acctng = sc.isValid()
        ? String.format("traceparent=00-%s-%s-%s",
              sc.getTraceId(), sc.getSpanId(), sc.isSampled() ? "01" : "00")
        : "";   // "" clears; NULL would leave the previous request's value in place
    conn.setClientInfo("ClientAccountingInformation", acctng);

    // ... run the real query on this same connection ...
} finally {
    conn.setClientInfo("ClientAccountingInformation", "");
    pool.release(conn);
}
```

## Blocking sessions (log records)

Lock contention is detected automatically whenever query sampling is enabled. When a sampled session is waiting for a lock (`LOCKWAIT`), its `db.server.query_sample` record is enriched with blocking attributes and emitted at severity WARN. No separate flag is needed.

- `events.db.server.query_sample.enabled: true` enables both query samples and blocking detection.
- `query_sample_collection.collection_interval` controls the poll cadence for both.
- Both share the same `querySampleConnection`.

If the blocker is idle (holding an open transaction but not currently executing), its last statement is looked up from `MON_GET_PKG_CACHE_STMT` and attached as `db2.blocking.blocker.query_text`.

Blocking detection is available on the **default path only** (`use_snapshot_monitor: false`). The snapshot path emits query samples at INFO severity without blocking attributes.

```yaml
receivers:
  db2:
    hostname: "localhost"
    database: "SAMPLE"
    username: "${env:DB2_USER}"
    password: "${env:DB2_PASSWORD}"
    events:
      db.server.query_sample:
        enabled: true              # enables query sampling and blocking detection
    query_sample_collection:
      collection_interval: "10s"  # controls poll cadence for both
```

| Attribute | Description |
|---|---|
| `db.system.name` | Always `ibm.db2` |
| `db.namespace` | Database name |
| `db.query.text` | Obfuscated SQL of the **blocked** session |
| `db2.session_id` | Session identifier of the blocked session (DB2 application handle) |
| `db2.activity_state` | Activity state of the blocked session (e.g. `LOCK_WAIT READ_DML`) |
| `db2.member` | Db2 member number |
| `user.name` | Authenticated user of the blocked session |
| `client.address` | Client workstation of the blocked session |
| `db2.blocking.blocker.session_id` | Session identifier of the blocking session (DB2 application handle) |
| `db2.blocking.blocker.user` | Authenticated user of the blocker |
| `db2.blocking.blocker.client_address` | Client workstation of the blocker |
| `db2.blocking.blocker.query_text` | Obfuscated SQL of the blocker (from `MON_CURRENT_SQL`; idle blockers fall back to `MON_GET_PKG_CACHE_STMT`; omitted if unavailable) |
| `db2.blocking.blocker.is_idle` | `true` when the blocker has no active statement — it holds an open transaction but is not currently executing (omitted on snapshot path) |
| `db2.blocking.lock.object_type` | DB2 lock object type (`ROW`, `TABLE`, `TABLESPACE`, `PLAN`, `INTERNAL`, …) |
| `db2.blocking.lock.mode` | DB2 lock mode (`S`, `X`, `IX`, `IS`, `SIX`, `U`, `NS`, `NX`, …) |
| `db2.blocking.lock.object_name` | Name of the locked table or object |
| `db2.blocking.lock.schema` | Schema of the locked object |
| `db2.blocking.start_time` | Timestamp when the lock wait began |
| `db2.blocking.wait_duration` | Elapsed time since the lock wait began, in seconds |

**DB2-specific lock types** not present in SQL Server or PostgreSQL blocking detectors:

| Type | Meaning |
|---|---|
| `PLAN` | Package compile lock — acquired when DB2 compiles a static SQL package; can block concurrent compilations |
| `INTERNAL` | Internal latch — low-level engine synchronisation; rarely visible but can appear under high concurrency |
| `TABLESPACE` | Tablespace-level lock — acquired during backup, restore, or `REORG` operations |
| `NS` / `NX` modes | Next-key share/exclusive locks — used by range scans under `RR` isolation to prevent phantom reads |

**Required permissions** (blocking session detection):

```sql
GRANT SELECT ON TABLE SYSIBMADM.MON_LOCKWAITS TO USER db2mon;
GRANT SELECT ON TABLE SYSIBMADM.MON_CURRENT_SQL TO USER db2mon;
GRANT EXECUTE ON FUNCTION SYSPROC.MON_GET_CONNECTION TO USER db2mon;
GRANT EXECUTE ON FUNCTION SYSPROC.MON_GET_PKG_CACHE_STMT TO USER db2mon;  -- idle-blocker lookup
```

## Top queries (log records)

Top SQL statements are collected from the Db2 plan cache at `top_query_collection.collection_interval` and emitted as OTel log records (severity INFO). Disabled by default — set `events.db.server.top_query.enabled: true` to opt in. SQL text is obfuscated before emission — literal values are replaced with `?`.

Statements are ranked by execution elapsed time added during the current collection interval (per-interval delta of `STMT_EXEC_TIME`). **Note:** the SQL query first selects at most `max_query_sample_count` candidate statements from the plan cache ordered by cumulative lifetime elapsed time. The interval-delta ranking is then computed over that candidate set. A newly-hot statement outside the top-N cumulative candidates can therefore be missed in a single interval, but will appear once its cumulative cost enters the candidate window. The first collection after startup seeds the delta baseline and emits nothing. The lookback window is automatically set to 2× the configured interval (minimum 5 minutes) to ensure statements executed early in a long interval are not excluded before the next collection.

**Cache-miss classification:** when a plan-cache section has no baseline (first appearance or returning after eviction), its `INSERT_TIMESTAMP` is compared against the Db2 server clock recorded at the end of the previous scrape. Sections compiled after the previous scrape are treated as genuinely new and their current counters are emitted as the interval delta. Sections that predate the previous scrape are conservatively seeded — their baseline is established and nothing is emitted until the next cycle. This prevents lifetime cumulative totals from being reported as a single interval spike when an evicted section resumes execution. Both timestamps originate from the Db2 server clock, avoiding JVM/server timezone skew.

The log record's native `event_name` field is set to `db.server.top_query`.

| Attribute | Description |
|---|---|
| `db.system.name` | Always `ibm.db2` |
| `db.namespace` | Database name |
| `db.query.text` | Obfuscated SQL statement |
| `db2.member` | Db2 member number |
| `db2.num_executions` | Total executions added during this interval |
| `db2.total_elapsed_time` | Interval execution elapsed time in seconds |
| `db2.avg_exec_time` | Average execution time per execution this interval, in seconds |
| `db2.total_cpu_time` | Interval CPU time in seconds |
| `db2.rows_read` | Rows read during this interval |
| `db2.rows_returned` | Rows returned during this interval |
| `db2.rows_modified` | Rows inserted/updated/deleted during this interval |
| `db2.sorts` | Sorts performed during this interval |
| `db2.sort_overflows` | Sorts that spilled to disk during this interval |
| `db2.stmt_type` | Statement type: `D` = dynamic SQL, `S` = static SQL |
| `db2.query_plan` | JSON plan summary (operators + streams) — only when `max_explain_each_interval > 0` |
| `db2.query_plan.hash` | Db2 EXECUTABLE_ID hex string identifying the compiled plan section; changes on recompile — only when `max_explain_each_interval > 0` |

**Plan collection prerequisite** (`max_explain_each_interval > 0`): Db2 explain tables must be created once in the monitoring user's schema before enabling plan collection. Connect as the monitoring user and run:

```sql
CALL SYSPROC.SYSINSTALLOBJECTS('EXPLAIN', 'C', NULL, CURRENT SCHEMA)
```

This creates 10 explain tables (`EXPLAIN_INSTANCE`, `EXPLAIN_OPERATOR`, `EXPLAIN_STREAM`, etc.) in the user's schema. The tables are automatically owned by the monitoring user so no additional grants are needed. This is a one-time setup step — the tables persist across Db2 restarts.

The plan JSON format:

```json
{"operators":[{"id":1,"type":"RETURN","cost":198.6},{"id":2,"type":"NLJOIN","cost":198.6}],"streams":[{"id":1,"from":-1,"to":3,"cols":477}]}
```

## Required permissions

Db2 LUW authenticates users through the operating system (or an external LDAP/PAM provider) — there is no `CREATE USER` SQL command. Create the OS user first, then grant database authorities:

```bash
# Create the OS user on the Db2 server (Linux example)
useradd -m db2mon
echo 'db2mon:<password>' | chpasswd
```

```sql
-- Connect to the database as SYSADM or DBADM, then grant:

-- Basic connect
GRANT CONNECT ON DATABASE TO USER db2mon;

-- Stored procedure called unconditionally on every connection
GRANT EXECUTE ON PROCEDURE SYSPROC.WLM_SET_CLIENT_INFO TO USER db2mon;

-- Core metrics scrapers
GRANT EXECUTE ON FUNCTION SYSPROC.MON_GET_DATABASE TO USER db2mon;
GRANT EXECUTE ON FUNCTION SYSPROC.MON_GET_CONNECTION TO USER db2mon;
GRANT EXECUTE ON FUNCTION SYSPROC.MON_GET_BUFFERPOOL TO USER db2mon;
GRANT EXECUTE ON FUNCTION SYSPROC.MON_GET_TABLESPACE TO USER db2mon;
GRANT EXECUTE ON FUNCTION SYSPROC.MON_GET_TRANSACTION_LOG TO USER db2mon;

-- Top query collection (events.db.server.top_query.enabled: true)
GRANT EXECUTE ON FUNCTION SYSPROC.MON_GET_PKG_CACHE_STMT TO USER db2mon;

-- Explain plan enrichment (top_query_collection.max_explain_each_interval > 0)
-- EXPLAIN_FROM_SECTION reads the compiled plan from the package cache (no query re-execution).
-- Requires explain tables to be created once per schema — run as the monitoring user:
--   CALL SYSPROC.SYSINSTALLOBJECTS('EXPLAIN', 'C', NULL, CURRENT SCHEMA)
-- Then grant the procedure and the explain tables:
GRANT EXECUTE ON PROCEDURE SYSPROC.EXPLAIN_FROM_SECTION TO USER db2mon;
-- The monitoring user must also have INSERT/SELECT/DELETE on its own explain tables.
-- These are automatically granted when SYSINSTALLOBJECTS is run as that user.

-- Query sampling (events.db.server.query_sample.enabled: true, default path)
GRANT SELECT ON TABLE SYSIBMADM.MON_CURRENT_SQL TO USER db2mon;

-- Db2 10.5+ wait time attribute (db2.wait_time)
GRANT EXECUTE ON FUNCTION SYSPROC.MON_GET_ACTIVITY TO USER db2mon;

-- Blocking session detection (runs automatically with events.db.server.query_sample.enabled: true)
GRANT SELECT ON TABLE SYSIBMADM.MON_LOCKWAITS TO USER db2mon;
-- MON_CURRENT_SQL, MON_GET_CONNECTION, and MON_GET_PKG_CACHE_STMT are already granted above.

-- Snapshot monitor path (use_snapshot_monitor: true)
-- Note: blocking detection is NOT available on this path.
-- IMPORTANT: the EXECUTE grants below are necessary but NOT sufficient. Every SNAP_GET_*
-- function additionally requires the user to hold SYSMON, SYSCTRL, SYSMAINT or SYSADM
-- instance-level authority — see the note after this block.
-- GRANT EXECUTE ON FUNCTION SYSPROC.SNAP_GET_APPL_INFO TO USER db2mon;
-- GRANT EXECUTE ON FUNCTION SYSPROC.SNAP_GET_STMT TO USER db2mon;
-- GRANT SELECT ON TABLE SYSCAT.STATEMENTS TO USER db2mon;
-- SNAP_GET_DB supplies the SQL activity metrics that have no MON_GET_* equivalent
-- (see "SQL and row activity" under Metrics).
-- GRANT EXECUTE ON FUNCTION SYSPROC.SNAP_GET_DB TO USER db2mon;
-- Also requires DFT_MON_STMT ON. Run as SYSADM, then restart the instance:
--   db2 UPDATE DBM CFG USING DFT_MON_STMT ON
--   db2stop force && db2start
-- This setting takes effect only after the instance is cycled; without the restart
-- new connections inherit the old default and SNAP_GET_STMT returns no statement text.
```

> **Note**: `SYSIBMADM.MON_CURRENT_SQL` requires `DATAACCESS` or the individual `SELECT` grant shown above. Check with your DBA if queries fail with `SQLCODE=-551`.

### Snapshot monitor authority (`use_snapshot_monitor: true`)

The `SNAP_GET_*` functions are gated on **instance-level authority in addition to the `EXECUTE` grant**. DB2 requires one of `SYSMON`, `SYSCTRL`, `SYSMAINT` or `SYSADM`. A least-privilege monitoring user that has only been granted `EXECUTE` will silently receive no snapshot data — the receiver logs a warning and the snapshot-only metrics and dimensions are simply absent.

`SYSMON` is the least-privileged option and is the one to use. It is granted by OS group, not by SQL, so it needs a DBM CFG change and an instance restart:

```bash
# Run as the instance owner. Replace db2mon_grp with the OS group your monitoring user belongs to.
db2 UPDATE DBM CFG USING SYSMON_GROUP db2mon_grp
db2stop force && db2start
```

Verify the user actually holds it:

```sql
SELECT AUTHORITY, D_USER, D_GROUP
FROM TABLE(SYSPROC.AUTH_LIST_AUTHORITIES_FOR_AUTHID(CURRENT USER, 'U'))
WHERE AUTHORITY = 'SYSMON';
```

A `Y` in `D_USER` or `D_GROUP` means the authority is held. If both are `N`, the snapshot path will not return data regardless of the `EXECUTE` grants. This applies to the query sample snapshot path (`SNAP_GET_APPL_INFO`, `SNAP_GET_STMT`) as well as the SQL activity metrics (`SNAP_GET_DB`).

## Installation

### systemd (Linux)

The `packaging/systemd/` directory contains a hardened unit file. Set `otlp.endpoint` in `config.yaml` to wherever your OTel Collector is — same host, remote server, or any OTLP-compatible endpoint.

The receiver requires Java 11+. Install a JRE from your distribution's package manager — the Docker image's custom JRE is built against glibc (Debian) and can also be reused on glibc hosts.

```bash
# 1. Install a JRE (Java 11+) and copy the fat jar
sudo apt-get install -y openjdk-17-jre-headless   # Debian/Ubuntu
# sudo dnf install -y java-17-openjdk-headless     # RHEL/Fedora
sudo mkdir -p /opt/splunk-otel-db2
sudo cp build/libs/splunk-otel-db2-*.jar /opt/splunk-otel-db2/splunk-otel-db2.jar

# 2. Create the service user
sudo useradd -r -s /sbin/nologin -M splunk-otel-db2

# 3. Write config — set otlp.endpoint to your backend or local OTel Collector
sudo mkdir -p /etc/splunk-otel-db2
sudo tee /etc/splunk-otel-db2/config.yaml > /dev/null <<'EOF'
receivers:
  db2:
    hostname: "db2.example.com"
    hostport: 50000
    username: "${env:DB2_USER}"
    password: "${env:DB2_PASSWORD}"
    database: "SAMPLE"
    collection_interval: "30s"
    tls:
      insecure: true

exporters:
  otlp:
    # Send to an OTel Collector running on this host or elsewhere:
    endpoint: "http://localhost:4317"
    tls:
      insecure: true
EOF

# 4. Store credentials (readable only by the service user)
sudo tee /etc/splunk-otel-db2/env > /dev/null <<'EOF'
DB2_USER=db2mon
DB2_PASSWORD=<secret>
EOF
sudo chmod 640 /etc/splunk-otel-db2/env
sudo chown root:splunk-otel-db2 /etc/splunk-otel-db2/env

# 5. Install and start the service
sudo cp packaging/systemd/splunk-otel-db2.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now splunk-otel-db2
sudo journalctl -u splunk-otel-db2 -f
```

### Helm (Kubernetes)

The receiver exports via OTLP/gRPC. `otlp.endpoint` can point at an OTel Collector running anywhere — in the same cluster, on another host, or as a sidecar. Use a values file to avoid shell glob issues with array syntax.

**Example `my-values.yaml`:**

```yaml
receivers:
  - name: default
    hostname: "db2.example.com"
    port: 50000
    database: "PROD"
    collectionInterval: "30s"
    credentialsSecret: "my-db2-credentials"   # pre-existing Secret with DB2_USER / DB2_PASSWORD
    tls:
      insecure: true
    events:
      dbServerTopQuery:
        enabled: false
      dbServerQuerySample:
        enabled: false

otlp:
  # OTel Collector in the same cluster:
  # http://<helm-release-name>-opentelemetry-collector.<namespace>.svc.cluster.local:4317
  endpoint: "http://my-otelcol-opentelemetry-collector.monitoring.svc.cluster.local:4317"
  tls:
    insecure: true
```

```bash
helm repo add splunk-otel-db2 https://signalfx.github.io/splunk-otel-db2
helm repo update splunk-otel-db2
helm install my-db2 splunk-otel-db2/splunk-otel-db2 -f my-values.yaml
```

To install from a checkout instead, point at the chart directory: `helm install my-db2 packaging/helm -f my-values.yaml`.

`otlp.endpoint` is required for a standalone install. The chart can only derive an endpoint when it runs as a dependency of `splunk-otel-collector`, so rendering without one fails with a message saying so rather than defaulting to a name that does not resolve.

**OTel Collector outside the cluster** (external host or IP):

```yaml
otlp:
  # External OTel Collector reachable by hostname or IP:
  endpoint: "http://collector.example.com:4317"
  tls:
    insecure: true
```

**Credentials** — reference a pre-existing Secret (recommended for production):

```yaml
receivers:
  - name: default
    hostname: "db2.example.com"
    port: 50000
    database: "PROD"
    collectionInterval: "30s"
    credentialsSecret: "my-db2-credentials"   # Secret must have keys DB2_USER and DB2_PASSWORD
    tls:
      insecure: true
```

Or let the chart create one (not recommended for production):

```yaml
receivers:
  - name: default
    hostname: "db2.example.com"
    port: 50000
    database: "PROD"
    collectionInterval: "30s"
    username: "db2mon"
    password: "secret"
    tls:
      insecure: true
```

**Pulling from a private GHCR registry:**

```yaml
imagePullSecrets:
  - name: ghcr-credentials
```

### As a splunk-otel-collector subchart

The chart can be installed as a dependency of [splunk-otel-collector-chart](https://github.com/signalfx/splunk-otel-collector-chart), so one release deploys both the collector and the Db2 receiver. Add to that chart's `Chart.yaml`:

```yaml
  - name: splunk-otel-db2
    version: 0.1.0
    alias: db2
    repository: https://signalfx.github.io/splunk-otel-db2
    condition: db2.enabled
```

The alias must stay lowercase and hyphen-free — that chart uppercases aliases into resource names and hyphens break its template rendering.

Two further changes are required in that chart, and skipping either breaks it for **every** user of it, not just those enabling this receiver:

1. `values.yaml` must set `db2.enabled: false`. Helm ignores a `condition` whose values path does not resolve and installs the subchart anyway, so omitting the key deploys a Db2 receiver into every existing install on upgrade.
2. `values.schema.json` must declare a `db2` property. That schema sets `additionalProperties: false` at the top level, so an undeclared key makes the chart fail to render at its defaults with `additional properties 'db2' not allowed`.

Both are handled in [signalfx/splunk-otel-collector-chart#2641](https://github.com/signalfx/splunk-otel-collector-chart/pull/2641).

Values then nest under the alias, and `otlp.endpoint` can be omitted:

```yaml
db2:
  enabled: true
  receivers:
    - name: default
      hostname: "db2.example.com"
      port: 50000
      database: "PROD"
      collectionInterval: "30s"
      credentialsSecret: "my-db2-credentials"
      tls:
        insecure: true
```

**How the endpoint is chosen.** With `otlp.endpoint` empty, the chart derives it from the parent release name:

| `otlp.collectorTarget` | Service | Reachability |
| --- | --- | --- |
| `agent` (default) | `<release>-splunk-otel-collector-agent` | `internalTrafficPolicy: Local` — only an agent Pod on this Pod's own Node |
| `gateway` | `<release>-splunk-otel-collector` | cluster-wide |

The agent is the default because the agent DaemonSet is always deployed upstream while the gateway is opt-in. The tradeoff is that `Local` traffic policy means exports blackhole if this Pod's Node has no healthy agent, so prefer `collectorTarget: gateway` wherever the gateway is enabled.

Setting `gateway.enabled: true` on the parent does **not** switch the target on its own. Helm exposes parent values to a subchart only through `global`, so a top-level `gateway.enabled` is invisible here. Either set `db2.otlp.collectorTarget: gateway`, or have the parent pass `global.gateway.enabled: true`.

The derived name reproduces the collector chart's own `fullname` helper, including the rule that the chart-name suffix is dropped when the release name already contains it — release `my-splunk-otel-collector` resolves to `my-splunk-otel-collector-agent`, not `my-splunk-otel-collector-splunk-otel-collector-agent`.

**Set `db2.otlp.endpoint` explicitly if the parent release uses `fullnameOverride` or `nameOverride`.** Those keys live at the parent's top level, which `global` does not expose, so derivation cannot see them and would resolve to a Service that does not exist.

### Docker

Mount a config file and run. `otlp.endpoint` in the config points at wherever your OTel Collector is — same host, remote, or a sidecar container.

```bash
docker run --rm \
  -e DB2_USER=db2mon \
  -e DB2_PASSWORD=secret \
  -v /path/to/config.yaml:/etc/splunk-otel-db2/config.yaml:ro \
  ghcr.io/signalfx/splunk-otel-db2:latest
```

The config file controls everything — DB2 connection, OTLP endpoint, collection intervals. See the [Configuration](#configuration) section for the full schema.

### docker-compose

Two deployment modes depending on whether you run the OTel Collector yourself or want it bundled.

---

**Option A — Scraper only** (you already have an OTel Collector running elsewhere):

```bash
# 1. Clone the repo
git clone https://github.com/signalfx/splunk-otel-db2.git
cd splunk-otel-db2

# 2. Set required environment variables
export DB2_HOST=db2.example.com        # your DB2 hostname or IP
export DB2_PORT=50000                  # default 50000
export DB2_USER=db2mon
export DB2_PASSWORD=secret
export OTLP_ENDPOINT=http://your-collector:4317

# 3. Start the scraper
docker compose up --build -d

# 4. Check logs
docker compose logs -f receiver
```

---

**Option B — Scraper + OTel Collector** (collector runs alongside the scraper, forwards to Splunk Observability Cloud):

```bash
# 1. Clone the repo
git clone https://github.com/signalfx/splunk-otel-db2.git
cd splunk-otel-db2

# 2. Set required environment variables
export DB2_HOST=db2.example.com        # your DB2 hostname or IP
export DB2_PORT=50000                  # default 50000
export DB2_USER=db2mon
export DB2_PASSWORD=secret
export SPLUNK_REALM=us1                # your Splunk Observability Cloud realm
export SPLUNK_TOKEN=<your-ingest-token>

# 3. Start both the scraper and the collector
docker compose -f docker-compose.yml -f docker-compose.collector.yml --profile collector up --build -d

# 4. Check logs
docker compose -f docker-compose.yml -f docker-compose.collector.yml logs -f receiver otel-collector
```

> **Finding your realm and token**: in Splunk Observability Cloud go to **Settings → Access Tokens** for the ingest token, and **Settings → General** for the realm (e.g. `us1`, `eu0`).

## Development

```bash
make test                # unit tests
make integration-test    # live DB2 via Testcontainers (requires Docker)
make jar                 # build fat jar → build/libs/splunk-otel-db2-*.jar
make clean               # clean build outputs
```

### Test suites

| Gradle task | Description | Requires Docker |
|---|---|---|
| `./gradlew test` | Fast unit tests — mocked OTel SDK, no DB2 | No |
| `./gradlew integrationTest` | Live DB2 (Testcontainers) + in-process scraper with `InMemoryLogRecordExporter` | Yes |
| `./gradlew integrationTestSnapshot` | Snapshot monitor path; requires `DFT_MON_STMT ON` on the container | Yes |

**End-to-end OTel Collector test** (`Db2OtelCollectorEndToEndIntegrationTest`) is included in `integrationTest` and validates the full pipeline:

1. Starts a real IBM DB2 11.5 container via Testcontainers.
2. Starts an `otel/opentelemetry-collector-contrib` container with `verbosity: detailed` debug exporter.
3. Starts a `Db2Pipeline` connected to both — scraper exports via OTLP gRPC to the collector.
4. Creates a lock-wait workload (`INSERT` held uncommitted + `SELECT … WITH RR` blocks) so `MON_CURRENT_SQL` has an active query to capture.
5. Runs a cross-join probe query (`E2E_TOPQ_PROBE`) that appears in the top-query plan cache.
6. Asserts that the collector container logs contain both `db.server.query_sample` and `db.server.top_query` events with `db.system.name = ibm.db2` and the correct `db.namespace`.

## Resource attributes

The receiver sets the following OTel resource attributes on all telemetry. Any attribute can be overridden or extended using `resource_attributes` in the receiver config (see [Resource attribute overrides](#resource-attribute-overrides)).

| Attribute | Default value |
|---|---|
| `service.name` | `splunk-otel-db2` (override with `service_name`) |
| `service.instance.id` | `host:port/database`; loopback hosts replaced with collector hostname (override with `service_instance_id`) |
| `service.namespace` | not set (set with `service_namespace`) |
| `server.address` | configured hostname; loopback resolved to collector hostname |
| `host.name` | configured hostname (operator's typed value, preserved as-is) |
| `server.port` | configured port (integer) |
| `db.system.name` | `ibm.db2` |
| `db.namespace` | configured database name |

## Releases

Every merge to `main` publishes a chart and a matching image. The chart goes to the gh-pages Helm repo at `https://signalfx.github.io/splunk-otel-db2`; the image goes to `ghcr.io/signalfx/splunk-otel-db2`.

**The patch number is derived, not stored.** `version` in [packaging/helm/Chart.yaml](packaging/helm/Chart.yaml) is a *floor* that pins only major and minor. On each main merge, [next-version.sh](.github/scripts/next-version.sh) reads the published `index.yaml`, finds the highest patch already released in that major.minor series, and publishes the next one:

```
floor 0.1.0, nothing published yet   ->  0.1.0
floor 0.1.0, 0.1.0 published         ->  0.1.1
floor 0.1.0, 0.1.7 published         ->  0.1.8
```

Deriving it keeps CI from committing a version bump back to `main`, which would retrigger the release and loop. Preview what the next merge would publish:

```bash
make next-version
```

**To release a minor or major**, edit the floor in `Chart.yaml` — set it to `0.2.0` and the next merge publishes exactly that, then `0.2.1`, `0.2.2`, and so on. A floor *ahead* of what is published is rejected, because chart-releaser refuses to overwrite a release and its own error does not explain why.

The chart's `version` and `appVersion` are always the same value, and CI verifies the image for that `appVersion` exists before releasing the chart — a chart pointing at an image tag that was never pushed fails at install time, not at release time.

Pushing a `v<x>.<y>.<z>` tag overrides the derivation and publishes that exact version, which is the escape hatch for re-releasing a specific commit.

### One-time setup

The gh-pages Helm repo needs its branch to exist before the first release; chart-releaser will not create it:

```bash
git switch --orphan gh-pages && git commit --allow-empty -m "Initialize gh-pages" && git push -u origin gh-pages
```

Then enable GitHub Pages for the repository, serving from the `gh-pages` branch root.

## License

Apache 2.0 — see [LICENSE](LICENSE).
