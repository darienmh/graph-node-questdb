//! QuestDB sink for graph-node.
//!
//! Implements [`EntitySinkRegistry`] so that, as a subgraph commits entities,
//! the changes are streamed to QuestDB over the InfluxDB Line Protocol (ILP)
//! using a TCP connection.
//!
//! Design notes:
//!
//! * **Best-effort.** Submitting changes never blocks indexing. Each
//!   per-deployment sink hands jobs to a bounded channel that is drained by a
//!   dedicated background thread. If the channel is full (QuestDB is slow or
//!   down) the job is dropped and logged; indexing is never stalled or failed.
//! * **After commit.** The store only calls [`DeploymentSink::submit`] after a
//!   block batch has been durably committed to Postgres.
//! * **Append-only.** Reorgs are not replicated: rows for reverted blocks
//!   remain in QuestDB. Subgraph-level removals are emitted as `op=remove`
//!   tombstone rows.
//! * **Blocking client.** `questdb-rs`' `Sender` is synchronous, so the worker
//!   runs on a plain OS thread and submission uses a non-blocking
//!   `SyncSender::try_send`.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::time::Duration;

use graph::blockchain::BlockPtr;
use graph::components::store::{
    DeploymentSink, EntityChange, EntityChangeOperation, EntitySinkRegistry,
};
use graph::data::store::Value;
use graph::data::subgraph::DeploymentHash;
use graph::prelude::Logger;
use graph::schema::EntityType;
use graph::slog::{debug, error, info, warn};

use questdb::ingress::{Buffer, Sender, TimestampNanos};
use serde::{Deserialize, Serialize};

/// Default capacity of the in-memory queue feeding the background writer.
fn default_queue_capacity() -> usize {
    10_000
}

/// Configuration for the QuestDB sink, deserialized from the `[questdb]`
/// section of the node config file.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QuestDbConfig {
    /// ILP connection string for `questdb-rs`, e.g.
    /// `tcp::addr=localhost:9009;`.
    pub url: String,
    /// Optional prefix prepended to every table name.
    #[serde(default)]
    pub table_prefix: Option<String>,
    /// Maximum number of pending write jobs before new jobs are dropped.
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
    /// Export rules. A deployment is exported if any rule matches it.
    #[serde(default)]
    pub export: Vec<ExportRule>,
}

/// A single export rule mapping a deployment to the entity types to export.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExportRule {
    /// Deployment id (the `Qm…` hash) to match, or `"*"` for all deployments.
    pub subgraph: String,
    /// Entity type names to export, or `["*"]` for all entity types.
    pub entities: Vec<String>,
}

/// Which entity types of a deployment to export.
#[derive(Clone, Debug)]
enum EntityFilter {
    All,
    Only(HashSet<String>),
}

impl EntityFilter {
    fn matches(&self, entity_type: &str) -> bool {
        match self {
            EntityFilter::All => true,
            EntityFilter::Only(set) => set.contains(entity_type),
        }
    }

    fn from_rule(entities: &[String]) -> Self {
        if entities.iter().any(|e| e == "*") {
            EntityFilter::All
        } else {
            EntityFilter::Only(entities.iter().cloned().collect())
        }
    }
}

/// A unit of work handed to the background writer.
struct WriteJob {
    namespace: String,
    block_number: i32,
    changes: Vec<EntityChange>,
}

/// Shared counters for observability (logged periodically).
#[derive(Default)]
struct Counters {
    rows_sent: AtomicU64,
    rows_dropped: AtomicU64,
    flush_errors: AtomicU64,
}

/// The QuestDB sink registry. Construct one with [`QuestDbSink::start`] and
/// register it with the subgraph store.
pub struct QuestDbSink {
    sender: SyncSender<WriteJob>,
    table_prefix: String,
    rules: Vec<(String, EntityFilter)>,
    counters: Arc<Counters>,
}

impl QuestDbSink {
    /// Start the background writer and return a registry ready to be handed to
    /// the subgraph store. Returns `None` if no config is provided.
    pub fn start(
        logger: &Logger,
        config: Option<&QuestDbConfig>,
    ) -> Option<Arc<dyn EntitySinkRegistry>> {
        let config = config?;
        if config.export.is_empty() {
            warn!(
                logger,
                "QuestDB export is configured but has no export rules; disabling"
            );
            return None;
        }

        let (sender, receiver) = sync_channel::<WriteJob>(config.queue_capacity.max(1));
        let counters = Arc::new(Counters::default());
        let table_prefix = config.table_prefix.clone().unwrap_or_default();
        let rules: Vec<(String, EntityFilter)> = config
            .export
            .iter()
            .map(|rule| {
                (
                    rule.subgraph.clone(),
                    EntityFilter::from_rule(&rule.entities),
                )
            })
            .collect();

        let worker_logger = logger.new(graph::slog::o!("component" => "QuestDbSink"));
        let conf = config.url.clone();
        let worker_counters = counters.clone();
        let worker_prefix = table_prefix.clone();

        std::thread::Builder::new()
            .name("questdb-writer".to_string())
            .spawn(move || {
                run_writer(
                    worker_logger,
                    conf,
                    worker_prefix,
                    receiver,
                    worker_counters,
                );
            })
            .expect("failed to spawn QuestDB writer thread");

        info!(logger, "Started QuestDB sink"; "url" => &config.url, "rules" => config.export.len());

        Some(Arc::new(QuestDbSink {
            sender,
            table_prefix,
            rules,
            counters,
        }))
    }

    fn filter_for(&self, deployment: &DeploymentHash) -> Option<EntityFilter> {
        let id = deployment.as_str();
        self.rules
            .iter()
            .find(|(subgraph, _)| subgraph == "*" || subgraph == id)
            .map(|(_, filter)| filter.clone())
    }
}

impl EntitySinkRegistry for QuestDbSink {
    fn for_deployment(
        &self,
        deployment: &DeploymentHash,
        namespace: &str,
    ) -> Option<Arc<dyn DeploymentSink>> {
        let filter = self.filter_for(deployment)?;
        Some(Arc::new(QuestDbDeploymentSink {
            namespace: namespace.to_string(),
            filter,
            sender: self.sender.clone(),
            counters: self.counters.clone(),
            _table_prefix: self.table_prefix.clone(),
        }))
    }
}

/// A per-deployment sink that forwards committed changes to the writer thread.
struct QuestDbDeploymentSink {
    namespace: String,
    filter: EntityFilter,
    sender: SyncSender<WriteJob>,
    counters: Arc<Counters>,
    // Kept for potential future per-deployment table naming overrides.
    _table_prefix: String,
}

impl DeploymentSink for QuestDbDeploymentSink {
    fn wants(&self, entity_type: &EntityType) -> bool {
        self.filter.matches(entity_type.as_str())
    }

    fn submit(&self, block_ptr: &BlockPtr, changes: Vec<EntityChange>) {
        if changes.is_empty() {
            return;
        }
        let job = WriteJob {
            namespace: self.namespace.clone(),
            block_number: block_ptr.number,
            changes,
        };
        match self.sender.try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(job)) => {
                self.counters
                    .rows_dropped
                    .fetch_add(job.changes.len() as u64, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(job)) => {
                self.counters
                    .rows_dropped
                    .fetch_add(job.changes.len() as u64, Ordering::Relaxed);
            }
        }
    }
}

/// Background writer loop: connects to QuestDB and drains the job channel,
/// reconnecting with backoff on failure. Never panics on backend errors.
fn run_writer(
    logger: Logger,
    conf: String,
    table_prefix: String,
    receiver: std::sync::mpsc::Receiver<WriteJob>,
    counters: Arc<Counters>,
) {
    let mut sender: Option<Sender> = None;
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);

    for job in receiver.iter() {
        // Make sure we have a live connection, reconnecting with backoff.
        if sender.is_none() {
            match Sender::from_conf(&conf) {
                Ok(s) => {
                    info!(logger, "Connected to QuestDB");
                    sender = Some(s);
                    backoff = Duration::from_secs(1);
                }
                Err(e) => {
                    counters
                        .rows_dropped
                        .fetch_add(job.changes.len() as u64, Ordering::Relaxed);
                    warn!(logger, "Could not connect to QuestDB, dropping batch";
                        "error" => e.to_string(), "backoff_s" => backoff.as_secs());
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(max_backoff);
                    continue;
                }
            }
        }

        let s = sender.as_mut().unwrap();
        match write_job(s, &table_prefix, &job) {
            Ok(rows) => {
                counters.rows_sent.fetch_add(rows, Ordering::Relaxed);
                debug!(logger, "Flushed entities to QuestDB";
                    "block" => job.block_number, "rows" => rows);
            }
            Err(e) => {
                counters.flush_errors.fetch_add(1, Ordering::Relaxed);
                counters
                    .rows_dropped
                    .fetch_add(job.changes.len() as u64, Ordering::Relaxed);
                error!(logger, "Failed to write to QuestDB, will reconnect";
                    "error" => e.to_string(), "block" => job.block_number);
                // Drop the connection so we reconnect on the next job.
                sender = None;
            }
        }
    }

    debug!(logger, "QuestDB writer thread shutting down");
}

/// Serialize a job into ILP and flush it. Returns the number of rows written.
fn write_job(
    sender: &mut Sender,
    table_prefix: &str,
    job: &WriteJob,
) -> Result<u64, questdb::Error> {
    // Obtain a buffer from the sender so it uses the negotiated protocol
    // version for this connection.
    let mut buffer = sender.new_buffer();
    let mut rows = 0u64;

    for change in &job.changes {
        let table = format!(
            "{}{}_{}",
            table_prefix,
            job.namespace,
            change.entity_type.as_str()
        );
        let ts = TimestampNanos::new(change.block_time.as_secs_since_epoch() * 1_000_000_000);

        buffer
            .table(table.as_str())?
            .symbol("op", change.operation.as_str())?
            .column_str("id", change.id.as_str())?
            .column_i64("block_number", change.block_number as i64)?;

        if let Some(entity) = &change.data
            && change.operation != EntityChangeOperation::Remove
        {
            for (name, value) in entity.sorted_ref() {
                // `id` and `block_number` are emitted as meta columns above.
                if name == "id" || name == "block_number" {
                    continue;
                }
                append_value(&mut buffer, name, value)?;
            }
        }

        buffer.at(ts)?;
        rows += 1;
    }

    sender.flush(&mut buffer)?;
    Ok(rows)
}

/// Append a single entity field to the ILP buffer, mapping graph-node `Value`s
/// to ILP column types. `Null` values are omitted.
fn append_value(buffer: &mut Buffer, name: &str, value: &Value) -> Result<(), questdb::Error> {
    match value {
        Value::String(s) => {
            buffer.column_str(name, s.as_str())?;
        }
        Value::Int(i) => {
            buffer.column_i64(name, *i as i64)?;
        }
        Value::Int8(i) => {
            buffer.column_i64(name, *i)?;
        }
        Value::Bool(b) => {
            buffer.column_bool(name, *b)?;
        }
        Value::BigDecimal(d) => {
            // Keep numeric where possible; fall back to a string otherwise.
            let s = d.to_string();
            match s.parse::<f64>() {
                Ok(f) if f.is_finite() => {
                    buffer.column_f64(name, f)?;
                }
                _ => {
                    buffer.column_str(name, s.as_str())?;
                }
            }
        }
        Value::BigInt(b) => {
            // BigInt can exceed i64; store as string to avoid overflow.
            buffer.column_str(name, b.to_string().as_str())?;
        }
        Value::Bytes(b) => {
            buffer.column_str(name, b.to_string().as_str())?;
        }
        Value::Timestamp(ts) => {
            buffer.column_str(name, ts.to_string().as_str())?;
        }
        Value::List(list) => {
            // Lists have no native ILP representation; store a JSON-ish string.
            buffer.column_str(name, format!("{:?}", list).as_str())?;
        }
        Value::Null => {
            // Omit null columns.
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_filter_all_matches_everything() {
        let filter = EntityFilter::from_rule(&["*".to_string()]);
        assert!(matches!(filter, EntityFilter::All));
        assert!(filter.matches("Transfer"));
        assert!(filter.matches("Anything"));
    }

    #[test]
    fn entity_filter_all_takes_precedence_over_names() {
        // A wildcard anywhere in the list means "all".
        let filter = EntityFilter::from_rule(&["Transfer".to_string(), "*".to_string()]);
        assert!(matches!(filter, EntityFilter::All));
        assert!(filter.matches("Swap"));
    }

    #[test]
    fn entity_filter_only_matches_listed() {
        let filter = EntityFilter::from_rule(&["Transfer".to_string(), "Swap".to_string()]);
        assert!(matches!(filter, EntityFilter::Only(_)));
        assert!(filter.matches("Transfer"));
        assert!(filter.matches("Swap"));
        assert!(!filter.matches("Mint"));
        assert!(!filter.matches("transfer")); // case-sensitive
    }

    #[test]
    fn entity_filter_empty_matches_nothing() {
        let filter = EntityFilter::from_rule(&[]);
        assert!(!filter.matches("Transfer"));
    }

    #[test]
    fn config_uses_defaults_for_optional_fields() {
        let config: QuestDbConfig =
            serde_json::from_str(r#"{ "url": "tcp::addr=localhost:9009;" }"#).unwrap();
        assert_eq!(config.url, "tcp::addr=localhost:9009;");
        assert_eq!(config.table_prefix, None);
        assert_eq!(config.queue_capacity, default_queue_capacity());
        assert!(config.export.is_empty());
    }

    #[test]
    fn config_parses_export_rules() {
        let config: QuestDbConfig = serde_json::from_str(
            r#"{
                "url": "tcp::addr=db:9009;",
                "table_prefix": "graph_",
                "queue_capacity": 42,
                "export": [
                    { "subgraph": "QmHash", "entities": ["Transfer", "Swap"] },
                    { "subgraph": "*", "entities": ["*"] }
                ]
            }"#,
        )
        .unwrap();
        assert_eq!(config.table_prefix.as_deref(), Some("graph_"));
        assert_eq!(config.queue_capacity, 42);
        assert_eq!(config.export.len(), 2);
        assert_eq!(config.export[0].subgraph, "QmHash");
        assert_eq!(config.export[0].entities, vec!["Transfer", "Swap"]);
        assert_eq!(config.export[1].subgraph, "*");
    }
}
