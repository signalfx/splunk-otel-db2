# splunk-otel-db2

A standalone OpenTelemetry receiver for IBM Db2. Collects metrics from a Db2 database and exports them via OTLP gRPC to an OpenTelemetry Collector or Splunk Observability Cloud. Query log collection (top queries, active sessions) is planned for a future release.

## Requirements

- Java 11+
- IBM Db2 11.5+
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

```yaml
receivers:
  db2:
    hostname: "localhost"       # DB2 host
    hostport: 50000             # DB2 port (default 50000)
    username: "${env:DB2_USER}" # supports ${env:VAR} substitution
    password: "${env:DB2_PASSWORD}"
    database: "SAMPLE"          # database name (uppercase)
    collection_interval: "30s"  # metrics collection interval
    top_query_count: 250        # top N queries by exec time (future PR)
    max_query_sample_count: 1000
    query_sample_interval: "10s"
    top_query_interval: "60s"
    use_snapshot_monitor: false
    tls:
      insecure: true            # set false to enable TLS

exporters:
  otlp:
    endpoint: "http://localhost:4317"
```

Credential values can be literals or `${env:VAR_NAME}` references.

## Metrics

All metrics carry `db.namespace` (database name) and `db2.member` (member number) attributes.

### Database (`MON_GET_DATABASE`)

| Metric | Type | Unit | Description |
|---|---|---|---|
| `db2.connection.count` | UpDownCounter | `{connection}` | Current connections |
| `db2.connection.active` | UpDownCounter | `{connection}` | Connections executing in the engine |
| `db2.connection.max` | UpDownCounter | `{connection}` | Peak simultaneous connections |
| `db2.lock.wait.count` | Counter | `{wait}` | Total lock waits |
| `db2.lock.wait_time` | Counter | `ms` | Total time spent waiting for locks |
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
| `db2.buffer_pool.read_time` | Counter | `ms` | Time spent reading from disk |
| `db2.buffer_pool.write_time` | Counter | `ms` | Time spent writing to disk |

### Tablespace (`MON_GET_TABLESPACE`)

Additional attributes: `db2.tablespace`, `db2.tablespace.type`, `db2.tablespace.state`, `db2.tablespace.usage` (`capacity`, `used`, `free`).

| Metric | Type | Unit | Description |
|---|---|---|---|
| `db2.tablespace.size` | UpDownCounter | `By` | Tablespace size broken down by usage |

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
