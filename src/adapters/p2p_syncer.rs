use crate::core::Workload;
use crate::error::RevenantError;
use crate::ports::DataSyncer;
use crate::{cloudevents::CloudEvent, net::Multiaddr};
use async_trait::async_trait;
use futures::StreamExt;
use libp2p::SwarmBuilder;
use libp2p::{
    gossipsub, mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, PeerId,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

const DRONE_DATA_TOPIC: &str = "revenant-data";

#[derive(Clone, Debug, PartialEq)]
pub enum NodeRole {
    Cloud,
    Edge,
    Drone,
}

/// The P2pSyncer is our concrete implementation of the DataSyncer port.
/// It acts as the public API for the networking layer.
pub struct P2pSyncer {
    command_tx: mpsc::Sender<NetworkCommand>,
    connected_peers: Arc<AtomicUsize>,
}

/// Events produced by the network task for the application logic.
#[derive(Debug)]
pub enum NetworkEvent {
    Received(CloudEvent),
}

/// Commands sent from the P2pSyncer to the background network task.
enum NetworkCommand {
    Publish(Vec<u8>),
}

#[derive(NetworkBehaviour)]
struct P2pBehaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

impl P2pSyncer {
    /// Creates a new P2pSyncer and spawns the underlying libp2p network task.
    /// It now returns a receiver for incoming network events.
    pub async fn new(
        role: NodeRole,
        cloud_addr: Option<Multiaddr>,
    ) -> Result<(Self, mpsc::Receiver<NetworkEvent>), RevenantError> {
        let (command_tx, command_rx) = mpsc::channel(100);
        let (event_tx, event_rx) = mpsc::channel(100);
        let connected_peers = Arc::new(AtomicUsize::new(0));
        let connected_peers_clone = connected_peers.clone();

        tokio::spawn(async move {
            if let Err(e) = run_network_task(
                role,
                cloud_addr,
                command_rx,
                event_tx,
                connected_peers_clone,
            )
            .await
            {
                eprintln!("[P2pSyncer] Network task failed: {}", e);
            }
        });

        let syncer = Self {
            command_tx,
            connected_peers,
        };

        Ok((syncer, event_rx))
    }
}

#[async_trait]
impl DataSyncer for P2pSyncer {
    /// Sends a batch of workloads by serializing them and sending them to the network task.
    async fn sync_batch(&self, batch: &[Workload]) -> Result<(), RevenantError> {
        for workload in batch {
            let bytes =
                serde_json::to_vec(&workload.event).map_err(RevenantError::Serialization)?;

            self.command_tx
                .send(NetworkCommand::Publish(bytes))
                .await
                .map_err(|e| RevenantError::Network(e.to_string()))?;
        }
        Ok(())
    }

    /// Checks if we are connected to at least one other peer.
    async fn is_connected(&self) -> bool {
        self.connected_peers.load(Ordering::Relaxed) > 0
    }
}

/// The main background task that runs the libp2p swarm.
async fn run_network_task(
    role: NodeRole,
    cloud_addr: Option<Multiaddr>,
    mut command_rx: mpsc::Receiver<NetworkCommand>,
    event_tx: mpsc::Sender<NetworkEvent>,
    connected_peers: Arc<AtomicUsize>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let local_key = libp2p::identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    println!("[{role:?}] Local peer id: {local_peer_id}");

    let mut swarm = SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| {
            let message_id_fn = |message: &gossipsub::Message| {
                let mut s = DefaultHasher::new();
                message.data.hash(&mut s);
                gossipsub::MessageId::from(s.finish().to_string())
            };
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(10))
                .validation_mode(gossipsub::ValidationMode::Strict)
                .message_id_fn(message_id_fn)
                .build()
                .map_err(|e| e.to_string())?;

            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )
            .map_err(|e| e.to_string())?;

            let mdns = if role != NodeRole::Cloud {
                mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?
            } else {
                mdns::tokio::Behaviour::new(
                    mdns::Config {
                        query_interval: Duration::from_secs(0),
                        ..Default::default()
                    },
                    local_peer_id,
                )?
            };
            Ok(P2pBehaviour { gossipsub, mdns })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    let topic = gossipsub::IdentTopic::new(DRONE_DATA_TOPIC);
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    match role {
        NodeRole::Cloud => {
            swarm.listen_on("/ip4/0.0.0.0/tcp/25565".parse()?)?;
        }
        NodeRole::Edge => {
            swarm.listen_on("/ip4/0.0.0.0/tcp/25566".parse()?)?;
            if let Some(addr) = cloud_addr {
                swarm.dial(addr.deref().clone())?;
                println!("[Edge] Dialed cloud address: {}", addr.deref());
            } else {
                eprintln!("[Edge] No cloud address provided to dial.");
            };
        }
        NodeRole::Drone => {
            swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
        }
    }

    loop {
        tokio::select! {
            Some(command) = command_rx.recv() => {
                match command {
                    NetworkCommand::Publish(data) => {
                        let message_id = swarm.behaviour_mut().gossipsub.publish(topic.clone(), data);
                        match message_id {
                            Ok(id) => {
                                println!("[{role:?}] Published message with id: {:?}", id);
                            }
                            Err(e) => {
                                eprintln!("[{role:?}] Failed to publish message: {}", e);
                            }
                        }
                    }
                }
            },
            event = swarm.select_next_some() => {
                 match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        println!("[{role:?}] Listening on {address}");
                    }
                    SwarmEvent::ConnectionEstablished { .. } => {
                        let count = swarm.network_info().num_peers();
                        connected_peers.store(count, Ordering::Relaxed);
                        println!("[{role:?}] Connection established. Total peers: {}", count);
                    }
                    SwarmEvent::ConnectionClosed { .. } => {
                        let count = swarm.network_info().num_peers();
                        connected_peers.store(count, Ordering::Relaxed);
                        println!("[{role:?}] Connection closed. Total peers: {}", count);
                    }
                    SwarmEvent::Behaviour(P2pBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                        for (peer_id, _) in list {
                            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                        }
                    },
                    SwarmEvent::Behaviour(P2pBehaviourEvent::Gossipsub(gossipsub::Event::Message { message, .. })) => {
                        match serde_json::from_slice::<CloudEvent>(&message.data) {
                            Ok(event) => {
                                println!("[{role:?}] Received gossip message from {}", event.source);
                                if event_tx.send(NetworkEvent::Received(event)).await.is_err() {
                                    eprintln!("[{role:?}] Network event channel closed. Shutting down listener part.");
                                }
                            }
                            Err(e) => {
                                eprintln!("[{role:?}] Failed to deserialize CloudEvent from gossip: {}", e);
                            }
                        }
                    },
                    _ => {}
                }
            }
        }
    }
}

// Re-export NodeRole for convenience
pub use NodeRole as P2pNodeRole;
