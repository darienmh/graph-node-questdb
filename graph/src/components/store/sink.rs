//! Abstractions for streaming committed entity changes to an external sink,
//! e.g. QuestDB over the InfluxDB Line Protocol (ILP).
//!
//! The sink is intentionally decoupled from any concrete backend: `graph`
//! only defines the traits and the data that crosses the boundary, while a
//! separate crate (e.g. `graph-store-questdb`) provides the implementation.
//!
//! The contract is **best-effort**: submitting changes must never block the
//! indexing pipeline and must never turn a backend failure into an indexing
//! failure. Implementations are expected to hand the data off to a background
//! task and return immediately, logging (and dropping) data on overload or
//! when the backend is unavailable.

use std::sync::Arc;

use crate::blockchain::{BlockPtr, BlockTime};
use crate::data::subgraph::DeploymentHash;
use crate::schema::EntityType;

use super::{BlockNumber, Entity};

/// The kind of change that was committed for an entity in a block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntityChangeOperation {
    /// A brand new entity version was inserted.
    Insert,
    /// An existing entity was overwritten with a new version.
    Overwrite,
    /// The entity was removed by the subgraph.
    Remove,
}

impl EntityChangeOperation {
    /// A stable, lowercase identifier suitable for use as a tag/symbol in the
    /// external store.
    pub fn as_str(&self) -> &'static str {
        match self {
            EntityChangeOperation::Insert => "insert",
            EntityChangeOperation::Overwrite => "overwrite",
            EntityChangeOperation::Remove => "remove",
        }
    }
}

/// A single committed entity change, tagged with the block it happened in.
///
/// These are derived from the raw, unfolded `EntityModification`s for a single
/// block, so the operation reflects exactly what the subgraph did (including
/// removals). For `Remove` there is no associated data.
#[derive(Clone, Debug)]
pub struct EntityChange {
    pub entity_type: EntityType,
    pub id: String,
    pub operation: EntityChangeOperation,
    /// The full entity for `Insert`/`Overwrite`; `None` for `Remove`.
    pub data: Option<Arc<Entity>>,
    pub block_number: BlockNumber,
    pub block_time: BlockTime,
}

/// A registry that hands out a per-deployment sink. Implemented by the
/// concrete backend.
pub trait EntitySinkRegistry: Send + Sync + 'static {
    /// Return a sink for `deployment` if it is configured for export.
    ///
    /// `namespace` is a stable, unique identifier for the deployment (the
    /// Postgres schema name) that the backend can use for table naming.
    ///
    /// Returning `None` means this deployment must not be exported, so callers
    /// can skip building any change data and incur no overhead.
    fn for_deployment(
        &self,
        deployment: &DeploymentHash,
        namespace: &str,
    ) -> Option<Arc<dyn DeploymentSink>>;
}

/// A per-deployment sink that receives committed entity changes.
pub trait DeploymentSink: Send + Sync + 'static {
    /// Whether `entity_type` should be exported. Used to avoid building change
    /// data for entities that would be dropped anyway.
    fn wants(&self, entity_type: &EntityType) -> bool;

    /// Submit changes that were committed for a block batch.
    ///
    /// This must not block; implementations hand the data off to a background
    /// task and return immediately. It is called only after the corresponding
    /// writes have been durably committed to the primary store.
    fn submit(&self, block_ptr: &BlockPtr, changes: Vec<EntityChange>);
}
