// src/ports.rs
use crate::cloudevents::CloudEvent;
use crate::core::Workload;
use crate::error::RevenantError;
use async_trait::async_trait;

/// Port for durable local storage of events.
#[async_trait]
pub trait DataRepository: Send + Sync {
    /// Persistently store a processed event.
    async fn store(&self, event: &CloudEvent) -> Result<(), RevenantError>;

    /// Retrieve a batch of unsynced workloads.
    async fn retrieve_pending_batch(
        &self,
        batch_size: usize,
    ) -> Result<Vec<Workload>, RevenantError>;

    /// Mark a set of workloads as successfully synced.
    async fn mark_as_synced(&self, workloads: &[Workload]) -> Result<(), RevenantError>;

    /// Increment the retry count for a set of workloads.
    async fn increment_retry_attempts(&self, workloads: &[Workload]) -> Result<(), RevenantError>;

    /// Get the number of pending workloads.
    async fn count_pending(&self) -> Result<i64, RevenantError>;
}

/// Port for synchronizing data with remote peers.
#[async_trait]
pub trait DataSyncer: Send + Sync {
    /// Send a batch of workloads to remote peers.
    /// Should return Ok(()) if the batch was successfully SENT.
    /// Acknowledgment of receipt is a separate concern.
    async fn sync_batch(&self, batch: &[Workload]) -> Result<(), RevenantError>;

    /// Check if the syncer is connected to any peers.
    async fn is_connected(&self) -> bool;
}

/// Port for the real-time processing logic (the "hot path").
pub trait EventProcessor: Send + Sync {
    /// Process an incoming CloudEvent immediately.
    /// Returns an optional new CloudEvent if processing yields a result to be stored and synced.
    fn process_event(&self, event: CloudEvent) -> Option<CloudEvent>;
}
