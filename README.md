# splunk-otel-db2

A standalone OpenTelemetry receiver for IBM Db2. Collects metrics and active query samples from a Db2 database and exports them via OTLP gRPC to an OpenTelemetry Collector or Splunk Observability Cloud.

## Requirements

- Java 11+
- IBM Db2 9.7+ (current metric scrapers require 10.5+; 9.7–10.1 support planned via snapshot monitor fallback; integration-tested against 11.5)
- Docker (for integration tests)

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
    top_query_enabled: false           # set true to enable top query collection (opt-in)
    top_query_count: 250               # top N queries to emit per interval (must be > 0)
    top_query_interval: "60s"          # how often to collect top queries (minimum 60s)
    query_sample_enabled: false        # set true to enable query sampling (opt-in)
    max_query_sample_count: 1000       # max active statements sampled per cycle (must be > 0)
    query_sample_interval: "10s"       # how often to sample active queries
    use_snapshot_monitor: false        # set true to use SNAP_GET_STMT fallback path
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

Each pipeline is identified by the `service.instance.id` resource attribute (`host:port/database`), so metrics and logs from different databases are distinguishable in the backend.

Credential values can be literals or `${env:VAR_NAME}` references.

## Resource usage

Each monitored database uses 1–3 JDBC connections depending on which collectors are enabled:

| Collector | Connection | Always open? |
|---|---|---|
| Metrics (DatabaseQuery, BufferpoolQuery, TablespaceQuery) | `metricsConnection` | Yes |
| Query sample (`query_sample_enabled: true`) | `querySampleConnection` | Optional |
| Top query (`top_query_enabled: true`) | `topQueryConnection` | Optional |

There is no connection pool — the monitoring workload fires queries at intervals of 10–60 seconds, which is too infrequent for pooling to help. Connections are validated before each use (`isValid`) and reconnected automatically with exponential backoff (2s → 4s → 8s → 16s → 30s cap) on failure. A deadline-based fail-fast prevents every metric callback from blocking during the backoff window — only the first caller after the deadline attempts reconnection.

Heap grows with the number of configured databases: each database gets its own OTel SDK pipeline, metric readers, OTLP exporters, schedulers, and in-memory caches (top-query delta baseline, query-sample deduplication). At steady state expect roughly 50–80 MB base plus ~15–30 MB per additional database. The dominant per-cycle allocation is the `MON_GET_PKG_CACHE_STMT` result set (all matching rows are loaded; `top_query_count` limits how many are emitted after sorting) and the delta baseline map (bounded by `MAX_STALE_CYCLES` eviction).

There is no hard limit on the number of databases in a single process. Beyond ~20–30 databases, heap pressure becomes the practical ceiling — raise `-Xmx` or split databases across multiple processes as needed.

## Metrics

All metrics carry `db.namespace` (database name) and `db2.member` (member number) attributes.

### Database (`MON_GET_DATABASE`)

| Metric | Type | Unit | Description |
|---|---|---|---|
| `db2.connection.count` | UpDownCounter | `{connection}` | Current connections |
| `db2.connection.active` | UpDownCounter | `{connection}` | Connections executing in the engine |
| `db2.connection.max` | UpDownCounter | `{connection}` | Peak simultaneous connections |
| `db2.lock.wait.count` | Counter | `{wait}` | Total lock waits |
| `db2.lock.wait_time` | Counter | `s` | Total time spent waiting for locks |
| `db2.lock.timeout.count` | Counter | `{timeout}` | Lock timeouts |
| `db2.lock.escalation.count` | Counter | `{escalation}` | Lock escalations (row → table) |
| `db2.lock.space_used` | UpDownCounter | `By` | Lock list memory in use |
| `db2.deadlock.count` | Counter | `{deadlock}` | Total deadlocks |
| `db2.sort.count` | Counter | `{sort}` | Total sorts executed |
| `db2.sort.overflow.count` | Counter | `{sort}` | Sorts that spilled to disk |

### Buffer pool (`MON_GET_BUFFERPOOL`)

Additional attributes: `db2.bufferpool` (pool name), `db2.read_type` (`data`, `index`, `xda`).

| Metric | Type | Unit | Description |
|---|---|---|---|
| `db2.buffer_pool.logical_reads` | Counter | `{read}` | Logical page reads |
| `db2.buffer_pool.physical_reads` | Counter | `{read}` | Physical page reads from disk |
| `db2.buffer_pool.writes` | Counter | `{write}` | Pages written to disk (data + index + XDA) |
| `db2.buffer_pool.read_time` | Counter | `s` | Time spent reading from disk |
| `db2.buffer_pool.write_time` | Counter | `s` | Time spent writing to disk |

### Tablespace (`MON_GET_TABLESPACE`)

Additional attributes: `db2.tablespace`, `db2.tablespace.type`, `db2.tablespace.state`, `db2.tablespace.usage` (`capacity`, `used`, `free`).

| Metric | Type | Unit | Description |
|---|---|---|---|
| `db2.tablespace.size` | UpDownCounter | `By` | Tablespace size broken down by usage |

## Query samples (log records)

Active statements are sampled at `query_sample_interval` and emitted as OTel log records (severity INFO). Disabled by default — set `query_sample_enabled: true` to opt in. SQL text is obfuscated before emission — literal values are replaced with `?`.

The log record's native `event_name` field is set to `db.server.query_sample` (not stored as an attribute).

| Attribute | Description |
|---|---|
| `db.system.name` | Always `ibm.db2` |
| `db.namespace` | Database name |
| `db.query.text` | Obfuscated SQL statement |
| `db2.application_handle` | Application handle / agent ID |
| `db2.activity_state` | Activity state (`EXECUTING`, `LOCKWAIT`, …) |
| `db2.query_start` | Statement start timestamp |
| `db2.session.duration` | Wall-clock elapsed time in seconds |
| `db2.member` | Db2 member number |
| `db2.cpu_time` | CPU time in seconds (default path only) |
| `user.name` | Authenticated user ID |
| `client.address` | Client workstation name |

**Default path** (`use_snapshot_monitor: false`, Db2 9.7+): `SYSIBMADM.MON_CURRENT_SQL` view + `MON_GET_CONNECTION` TVF JOIN. No `DFT_MON_STMT` requirement. Filters out IDLE sessions and the receiver's own queries.

**Snapshot monitor fallback** (`use_snapshot_monitor: true`, Db2 9.7+): `SNAP_GET_APPL_INFO` + `SNAP_GET_STMT` + `SYSCAT.STATEMENTS`. Requires `DFT_MON_STMT ON`. Provides richer states (`LOCKWAIT`, `UOWWAIT`, etc.) and static SQL fallback.

## Top queries (log records)

Top SQL statements are collected from the Db2 plan cache at `top_query_interval` and emitted as OTel log records (severity INFO). Disabled by default — set `top_query_enabled: true` to opt in. SQL text is obfuscated before emission — literal values are replaced with `?`.

Statements are ranked by execution elapsed time added during the current collection interval (per-interval delta of `STMT_EXEC_TIME`). The first collection after startup seeds the delta baseline and emits nothing. The lookback window is automatically set to 2× the configured interval (minimum 5 minutes) to ensure statements executed early in a long interval are not excluded before the next collection.

**Cache-miss classification:** when a plan-cache section has no baseline (first appearance or returning after eviction), its `INSERT_TIMESTAMP` is compared against the Db2 server clock recorded at the end of the previous scrape. Sections compiled after the previous scrape are treated as genuinely new and their current counters are emitted as the interval delta. Sections that predate the previous scrape are conservatively seeded — their baseline is established and nothing is emitted until the next cycle. This prevents lifetime cumulative totals from being reported as a single interval spike when an evicted section resumes execution. Both timestamps originate from the Db2 server clock, avoiding JVM/server timezone skew.

The log record's native `event_name` field is set to `db.server.top_query`.

| Attribute | Description |
|---|---|
| `db.system.name` | Always `ibm.db2` |
| `db.namespace` | Database name |
| `db.query.text` | Obfuscated SQL statement (also set as log body) |
| `db2.member` | Db2 member number |
| `db2.executions` | Total executions added during this interval |
| `db2.total_exec_time` | Interval execution elapsed time in seconds |
| `db2.avg_exec_time` | Average execution time per execution this interval, in seconds |
| `db2.total_cpu_time` | Interval CPU time in seconds |
| `db2.rows_read` | Rows read during this interval |
| `db2.rows_returned` | Rows returned during this interval |
| `db2.rows_modified` | Rows inserted/updated/deleted during this interval |
| `db2.sorts` | Sorts performed during this interval |
| `db2.sort_overflows` | Sorts that spilled to disk during this interval |
| `db2.stmt_type` | Statement type: `D` = dynamic SQL, `S` = static SQL |

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

-- Top query collection (top_query_enabled: true)
GRANT EXECUTE ON FUNCTION SYSPROC.MON_GET_PKG_CACHE_STMT TO USER db2mon;

-- Query sampling (query_sample_enabled: true, default path)
GRANT SELECT ON TABLE SYSIBMADM.MON_CURRENT_SQL TO USER db2mon;

-- Db2 10.5+ wait time attribute (db2.wait_time)
GRANT EXECUTE ON FUNCTION SYSPROC.MON_GET_ACTIVITY TO USER db2mon;

-- Snapshot monitor path (use_snapshot_monitor: true)
-- GRANT EXECUTE ON FUNCTION SYSPROC.SNAP_GET_APPL_INFO TO USER db2mon;
-- GRANT EXECUTE ON FUNCTION SYSPROC.SNAP_GET_STMT TO USER db2mon;
-- GRANT SELECT ON TABLE SYSCAT.STATEMENTS TO USER db2mon;
-- Also requires DFT_MON_STMT ON. Run the following as SYSADM, then restart the instance:
--   db2 UPDATE DBM CFG USING DFT_MON_STMT ON
--   db2stop force && db2start
-- DFT_MON_STMT ON takes effect only after the instance is cycled; without the restart
-- new connections inherit the old default and SNAP_GET_STMT returns no statement text.
```

> **Note**: `SYSIBMADM.MON_CURRENT_SQL` requires `DATAACCESS` or the individual `SELECT` grant shown above. Check with your DBA if queries fail with `SQLCODE=-551`.

## Development

```bash
make test                # unit tests
make integration-test    # live DB2 via Testcontainers (requires Docker)
make jar                 # build fat jar → build/libs/splunk-otel-db2-*.jar
make clean               # clean build outputs
```

## Resource attributes

The receiver sets the following OTel resource attributes on all telemetry:

| Attribute | Value |
|---|---|
| `service.name` | `splunk-otel-db2` |
| `db.system.name` | `ibm.db2` |
| `db.namespace` | configured database name |
| `server.address` | configured hostname |
| `server.port` | configured port |
| `service.instance.id` | `host:port/database` |

## License

Apache 2.0 — see [LICENSE](LICENSE).
