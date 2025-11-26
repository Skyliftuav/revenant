use crate::cloudevents::CloudEvent;
use crate::error::RevenantError;
use crate::ports::RealtimeSyncer;
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use lru::LruCache;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Client};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;

/// A RealtimeSyncer implementation using Redis (or Valkey).
///
/// It publishes events to channels formatted as `{device_type}/{device_id}/{event_type}`.
/// It subscribes to `{device_type}/{device_id}/commands` and `{device_type}/{device_id}/init` via Pub/Sub (QoS 0).
/// It polls `{device_type}/{device_id}/priority_commands` via Redis Streams (QoS 1).
pub struct RedisSyncer {
    client: Client,
    device_id: String,
    device_type: String,
    processed_priority_ids: Arc<Mutex<LruCache<String, ()>>>,
}

impl RedisSyncer {
    /// Creates a new RedisSyncer.
    pub fn new(client: Client, device_id: String, device_type: String) -> Self {
        Self {
            client,
            device_id,
            device_type,
            processed_priority_ids: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(200).unwrap(),
            ))),
        }
    }

    async fn poll_priority_commands(
        device_id: String,
        device_type: String,
        client: Client,
        event_tx: mpsc::Sender<CloudEvent>,
        processed_ids_lock: Arc<Mutex<LruCache<String, ()>>>,
    ) -> Result<(), RevenantError> {
        let stream_key = format!("{}/{}/priority_commands", device_type, device_id);
        let group_name = "revenant_group";
        let consumer_name = &device_id;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| RevenantError::Network(e.to_string()))?;

        // Create the consumer group if it doesn't exist.
        let _: redis::RedisResult<()> = conn
            .xgroup_create_mkstream(&stream_key, group_name, "$")
            .await;

        let options = StreamReadOptions::default()
            .group(group_name, consumer_name)
            .block(0)
            .count(1);

        loop {
            match conn
                .xread_options::<_, _, StreamReadReply>(&[&stream_key], &[">"], &options)
                .await
            {
                Ok(reply) => {
                    if let Some(stream) = reply.keys.get(0) {
                        for message in &stream.ids {
                            if let Some(redis::Value::Data(payload_bytes)) = message.map.get("data")
                            {
                                let json_payload_str = String::from_utf8_lossy(payload_bytes);

                                match serde_json::from_str::<CloudEvent>(&json_payload_str) {
                                    Ok(event) => {
                                        let mut cache = processed_ids_lock.lock().await;
                                        if cache.contains(&event.id) {
                                            tracing::warn!(event_id = %event.id, "Received duplicate priority command. Acknowledging and skipping.");
                                            let _: redis::RedisResult<()> = conn
                                                .xack(&stream_key, group_name, &[&message.id])
                                                .await;
                                            continue;
                                        }
                                        cache.put(event.id.clone(), ());
                                        drop(cache); // Release lock early

                                        if event_tx.send(event).await.is_err() {
                                            return Ok(());
                                        }

                                        let _: redis::RedisResult<()> = conn
                                            .xack(&stream_key, group_name, &[&message.id])
                                            .await;
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            "Failed to deserialize priority payload: {}",
                                            e
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Error reading from priority stream: {}. Reconnecting...", e);
                    return Err(RevenantError::Network(e.to_string()));
                }
            }
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
            .map_err(|e| {
                RevenantError::Network(format!("Failed to get Redis connection: {}", e))
            })?;

        let topic_suffix = event.event_type.clone();
        let channel = format!("{}/{}/{}", self.device_type, self.device_id, topic_suffix);

        let payload = serde_json::to_string(event).map_err(RevenantError::Serialization)?;

        conn.publish::<_, _, ()>(channel, payload)
            .await
            .map_err(|e| RevenantError::Network(format!("Failed to publish to Redis: {}", e)))?;

        Ok(())
    }

    async fn subscribe(&self) -> Result<BoxStream<'static, CloudEvent>, RevenantError> {
        let (tx, rx) = mpsc::channel(100);
        let client = self.client.clone();
        let device_id = self.device_id.clone();
        let device_type = self.device_type.clone();
        let processed_ids = self.processed_priority_ids.clone();

        tokio::spawn(async move {
            let command_channel = format!("{}/{}/commands", device_type, device_id);
            let init_channel = format!("{}/{}/init", device_type, device_id);
            let channels = vec![command_channel, init_channel];

            loop {
                // 1. Pub/Sub Listener
                let mut pubsub_conn = match client.get_async_pubsub().await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("Failed to get Redis connection for PubSub: {}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        continue;
                    }
                };

                if let Err(e) = pubsub_conn.subscribe(&channels).await {
                    tracing::error!("Failed to subscribe to channels: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }

                let mut pubsub_stream = pubsub_conn.on_message();

                // 2. Stream Poller (QoS 1)
                let stream_tx = tx.clone();
                let stream_client = client.clone();
                let stream_device_id = device_id.clone();
                let stream_device_type = device_type.clone();
                let stream_processed_ids = processed_ids.clone();

                let stream_handle = tokio::spawn(async move {
                    if let Err(e) = Self::poll_priority_commands(
                        stream_device_id,
                        stream_device_type,
                        stream_client,
                        stream_tx,
                        stream_processed_ids,
                    )
                    .await
                    {
                        tracing::error!("Stream poller failed: {}", e);
                    }
                });

                // 3. Main Loop
                loop {
                    tokio::select! {
                        msg = pubsub_stream.next() => {
                            match msg {
                                Some(m) => {
                                    let payload_result: Result<String, redis::RedisError> = m.get_payload();
                                    if let Ok(payload) = payload_result {
                                        match serde_json::from_str::<CloudEvent>(&payload) {
                                            Ok(event) => {
                                                if tx.send(event).await.is_err() {
                                                    stream_handle.abort();
                                                    return;
                                                }
                                            }
                                            Err(e) => tracing::error!("Failed to deserialize PubSub payload: {}", e),
                                        }
                                    } else if let Err(e) = payload_result {
                                        tracing::error!("Failed to get message payload: {}", e);
                                    }
                                }
                                None => {
                                    tracing::warn!("Pub/Sub message stream ended unexpectedly. Reconnecting...");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(ReceiverStream::new(rx).boxed())
    }
}
