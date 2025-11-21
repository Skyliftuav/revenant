use crate::cloudevents::CloudEvent;
use crate::error::RevenantError;
use crate::ports::RealtimeSyncer;
use async_trait::async_trait;
use redis::{AsyncCommands, Client};

/// A RealtimeSyncer implementation using Redis (or Valkey).
///
/// It publishes events to channels formatted as `{device_type}/{device_id}/{event_type}`.
pub struct RedisSyncer {
    client: Client,
    device_id: String,
    device_type: String,
}

impl RedisSyncer {
    /// Creates a new RedisSyncer.
    pub fn new(client: Client, device_id: String, device_type: String) -> Self {
        Self {
            client,
            device_id,
            device_type,
        }
    }
}

#[async_trait]
impl RealtimeSyncer for RedisSyncer {
    async fn publish(&self, event: &CloudEvent) -> Result<(), RevenantError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| RevenantError::Network(format!("Failed to get Redis connection: {}", e)))?;

        let topic_suffix = event.event_type.clone();
        let channel = format!("{}/{}/{}", self.device_type, self.device_id, topic_suffix);

        let payload = serde_json::to_string(event).map_err(RevenantError::Serialization)?;

        conn.publish::<_, _, ()>(channel, payload)
            .await
            .map_err(|e| RevenantError::Network(format!("Failed to publish to Redis: {}", e)))?;

        Ok(())
    }
}
