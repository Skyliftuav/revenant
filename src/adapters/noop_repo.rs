
use crate::core::Workload;
use crate::error::RevenantError;
use crate::ports::DataRepository;
use async_trait::async_trait;

/// A no-op repository that doesn't persist any data.
/// Useful for resource-constrained devices that prefer fire-and-forget semantics.
pub struct NoOpRepository;

impl NoOpRepository {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DataRepository for NoOpRepository {
    async fn store(&self, _event: &crate::cloudevents::CloudEvent) -> Result<(), RevenantError> {
        // No-op: don't store anything
        Ok(())
    }

    async fn retrieve_pending_batch(
        &self,
        _batch_size: usize,
    ) -> Result<Vec<Workload>, RevenantError> {
        // No-op: always return empty batch
        Ok(Vec::new())
    }

    async fn mark_as_synced(&self, _workloads: &[Workload]) -> Result<(), RevenantError> {
        // No-op: nothing to mark
        Ok(())
    }

    async fn increment_retry_attempts(&self, _workloads: &[Workload]) -> Result<(), RevenantError> {
        // No-op: nothing to increment
        Ok(())
    }

    async fn count_pending(&self) -> Result<i64, RevenantError> {
        // No-op: always 0 pending
        Ok(0)
    }
}
