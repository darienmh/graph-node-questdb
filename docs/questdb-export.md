# Streaming entities to QuestDB (ILP)

Graph Node can stream entity changes to [QuestDB](https://questdb.io/) as they
are committed, using the InfluxDB Line Protocol (ILP) over TCP. This is useful
for time-series analytics over indexed data without querying Postgres.

## How it works

As a subgraph indexes blocks, every entity change is normally written to
Postgres. When QuestDB export is enabled for a deployment, each committed block
batch additionally forwards its entity changes to QuestDB:

```
handlers → EntityCache → mods → WritableStore.transact_block_operations
   → (commit to Postgres) → QuestDB sink (background thread) → QuestDB (ILP/TCP)
```

Key properties:

- **After commit.** Changes are sent only after the batch is durably committed
  to Postgres, so QuestDB never receives data that failed to persist.
- **Best-effort.** Submission is non-blocking. A bounded in-memory queue feeds a
  dedicated writer thread; if QuestDB is slow or down, jobs are dropped and
  logged. Indexing is never stalled or failed because of QuestDB.
- **Append-only / reorgs.** Reverts (reorgs) are *not* replicated. Rows for
  reverted blocks remain in QuestDB. This matches QuestDB's append-only, ILP
  model.
- **All operations.** Inserts and overwrites are written with the full entity
  data; subgraph-level removals are written as `op=remove` tombstone rows
  (`id` + `block_number` only).

## Data model

For each entity change, one ILP row is written:

- **Table:** `<table_prefix><namespace>_<EntityType>`, where `<namespace>` is the
  deployment's Postgres schema (e.g. `sgd42`). Example: `sgd42_Transfer`.
- **Symbol:** `op` — one of `insert`, `overwrite`, `remove`.
- **Columns:** `id` (string), `block_number` (long), plus one column per entity
  field (for inserts/overwrites). The designated timestamp is the **block
  timestamp**.

Value type mapping:

| graph-node `Value` | ILP column |
| ------------------ | ---------- |
| `String`           | string     |
| `Int` / `Int8`     | i64        |
| `Bool`             | bool       |
| `BigDecimal`       | f64 (falls back to string if not finite) |
| `BigInt`           | string (may exceed i64) |
| `Bytes`            | string (hex) |
| `Timestamp`        | string     |
| `List`             | string     |
| `Null`             | column omitted |

## Configuration

Add a `[questdb]` section to the node configuration TOML file (the same file
passed via `--config`).

```toml
[questdb]
# ILP connection string for questdb-rs (TCP transport).
url = "tcp::addr=localhost:9009;"

# Optional: prefix prepended to every table name. Default: "".
table_prefix = "graph_"

# Optional: max pending write jobs before new jobs are dropped. Default: 10000.
queue_capacity = 10000

# Export rules. A deployment is exported if ANY rule matches it.
# `subgraph` is the deployment id (the Qm… hash) or "*" for all deployments.
# `entities` is a list of entity type names, or ["*"] for all entity types.
[[questdb.export]]
subgraph = "QmYourDeploymentHash"
entities = ["Transfer", "Swap"]

[[questdb.export]]
subgraph = "*"
entities = ["*"]
```

If the `[questdb]` section is absent, the feature is fully disabled with no
runtime overhead. Deployments that match no rule are never exported and incur no
per-block cost.

### Connection string

The `url` is passed verbatim to `questdb-rs`. For TCP use
`tcp::addr=host:port;` (default ILP port is `9009`). See the
[questdb-rs configuration docs](https://questdb.io/docs/reference/clients/rust/)
for TLS and authentication options.

## Operational notes

- The writer reconnects automatically with exponential backoff (1s up to 30s) if
  the connection drops.
- Dropped rows (queue full, connection failure) are counted and logged; they are
  not retried, by design, to keep indexing unblocked.
- Because export is append-only, downstream consumers should treat the data as
  an event log and deduplicate by `(id, block_number)` if needed, and be aware
  that rows from reverted blocks may be present.
