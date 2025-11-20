use crate::cloudevents::CloudEvent;
use crate::error::RevenantError;
use crate::ports::{DataRepository, DataSyncer, EventProcessor, RealtimeSyncer};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::interval;
use uuid::Uuid;

/// Wraps a CloudEvent with metadata for persistence and synchronization.
#[derive(Debug, Clone)]
pub struct Workload {
    pub id: Uuid,
    pub event: CloudEvent,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub sync_attempts: i32,
    pub is_synced: bool,
}

/// Configuration for the Revenant service.
#[derive(Debug, Clone)]
pub struct RevenantConfig {
    pub sync_interval: Duration,
    pub batch_size: usize,
    pub max_retry_attempts: i32,
}

/// The central orchestration service for Revenant.
pub struct RevenantService {
    // Using an inner struct to hide the mutex and channel sender
    inner: Arc<Mutex<RevenantServiceInner>>,
}

struct RevenantServiceInner {
    processor: Arc<dyn EventProcessor>,
    repository: Arc<dyn DataRepository>,
    syncer: Arc<dyn DataSyncer>,
    realtime_syncer: Option<Arc<dyn RealtimeSyncer>>,
    config: RevenantConfig,
}

impl RevenantService {
    /// Creates a new RevenantService and starts its background tasks.
    pub fn new(
        processor: Arc<dyn EventProcessor>,
        repository: Arc<dyn DataRepository>,
        syncer: Arc<dyn DataSyncer>,
        realtime_syncer: Option<Arc<dyn RealtimeSyncer>>,
        config: RevenantConfig,
    ) -> Self {
        let inner = Arc::new(Mutex::new(RevenantServiceInner {
            processor,
            repository,
            syncer,
            realtime_syncer,
            config,
        }));

        // Spawn the background sync loop
        tokio::spawn(Self::run_sync_loop(inner.clone()));

        Self { inner }
    }

    /// Submits a CloudEvent for immediate processing and eventual synchronization.
    /// This is the primary entry point for the "hot path".
    pub async fn submit(&self, event: CloudEvent) -> Result<(), RevenantError> {
        let inner = self.inner.lock().await;

        // --- HOT PATH ---
        // 1. Process the event immediately.
        if let Some(processed_event) = inner.processor.process_event(event) {
            // 2. If processing yields a result, store it.
            inner.repository.store(&processed_event).await?;

            // 3. If a realtime syncer is configured, publish it immediately.
            if let Some(ref realtime) = inner.realtime_syncer {
                if let Err(e) = realtime.publish(&processed_event).await {
                    tracing::warn!("[Revenant] Realtime publish failed: {}", e);
                    // We do NOT fail the request here, as this is fire-and-forget.
                }
            }
        }

        Ok(())
    }

    /// The background task that periodically syncs data. This is the "cold path".
    async fn run_sync_loop(inner: Arc<Mutex<RevenantServiceInner>>) {
        let sync_interval = {
            let guard = inner.lock().await;
            guard.config.sync_interval
        };

        let mut interval = interval(sync_interval);

        loop {
            interval.tick().await;
            let guard = inner.lock().await;

            if let Err(e) = Self::sync_cycle(&guard).await {
                tracing::error!("[Revenant] Sync cycle failed: {}", e);
            }
        }
    }

    /// Performs a single cycle of retrieving, syncing, and updating workloads.
    async fn sync_cycle(inner: &RevenantServiceInner) -> Result<(), RevenantError> {
        // Only attempt to sync if we are connected.
        if !inner.syncer.is_connected().await {
            tracing::debug!("[Revenant] Syncer not connected. Skipping sync cycle.");
            return Ok(());
        }

        let batch = inner
            .repository
            .retrieve_pending_batch(inner.config.batch_size)
            .await?;

        if batch.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            "[Revenant] Found {} pending workloads to sync.",
            batch.len()
        );

        match inner.syncer.sync_batch(&batch).await {
            Ok(_) => {
                // If the batch was sent successfully, mark them as synced.
                // Note: This doesn't mean they were received, just that the network layer accepted them.
                // Acknowledgment is a higher-level concern we can add later.
                inner.repository.mark_as_synced(&batch).await?;
                tracing::debug!("[Revenant] Successfully synced batch of {}.", batch.len());
            }
            Err(e) => {
                tracing::error!(
                    "[Revenant] Sync batch failed: {}. Incrementing retry counts.",
                    e
                );
                // If sending failed, increment the retry counter for the batch.
                inner.repository.increment_retry_attempts(&batch).await?;
            }
        }

        Ok(())
    }
}
