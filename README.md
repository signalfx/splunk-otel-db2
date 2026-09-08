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
| `db.operation.name` | SQL operation type (`READ`, `WRITE`, `DDL`, …); omitted on snapshot path |
| `db2.application_handle` | Application handle / agent ID |
| `db2.activity_state` | Activity state (`EXECUTING`, `LOCKWAIT`, …) |
| `db2.query_start` | Statement start timestamp |
| `db2.session.duration` | Wall-clock elapsed time in seconds |
| `db2.member` | Db2 member number |
| `db2.cpu_time` | CPU time in seconds (default path only; omitted on snapshot path) |
| `db2.wait_time` | Lock wait time in seconds (Db2 10.5+ default path only) |
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
| `db.query.text` | Obfuscated SQL statement |
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
    topQueryEnabled: false
    querySampleEnabled: false

otlp:
  # OTel Collector in the same cluster:
  # http://<helm-release-name>-opentelemetry-collector.<namespace>.svc.cluster.local:4317
  endpoint: "http://my-otelcol-opentelemetry-collector.monitoring.svc.cluster.local:4317"
  tls:
    insecure: true
```

```bash
helm install my-db2 packaging/helm -f my-values.yaml
```

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
6. Asserts that the collector container logs contain both `db.server.query_sample` and `db.server.top_query` events with `db.system.name = db2` and the correct `db.namespace`.

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

## License

Apache 2.0 — see [LICENSE](LICENSE).
