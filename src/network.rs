// src/network.rs
use futures::StreamExt;
use libp2p::{
    gossipsub,
    mdns,
    noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp,
    yamux,
    Multiaddr,
    PeerId,
    SwarmBuilder,
    Transport, // <-- Import the Transport trait
};
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::sync::mpsc;

use crate::cloudevents::CloudEvent;

pub const DRONE_DATA_TOPIC: &str = "drone-data";

#[derive(Clone, Debug, PartialEq)]
pub enum NodeRole {
    Cloud,
    Edge,
    Drone,
}

#[derive(Debug)]
pub enum Command {
    Publish(CloudEvent),
}

#[derive(Debug)]
pub enum Event {
    Message(CloudEvent),
}

#[derive(NetworkBehaviour)]
pub struct MeshBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
}

pub async fn run_network_task(
    role: NodeRole,
    cloud_addr: Option<Multiaddr>,
    mut command_rx: mpsc::Receiver<Command>,
    event_tx: mpsc::Sender<Event>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let local_key = libp2p::identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    tracing::info!("[{role:?}] Local peer id: {local_peer_id}");

    let mut swarm = SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| {
            // This closure constructs the behaviour. It's called by the builder.
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
                .map_err(|e| e.to_string())?; // Errors in here must be convertible to a string

            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )
            .map_err(|e| e.to_string())?;

            let mdns = if role != NodeRole::Cloud {
                mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?
            } else {
                // The mdns::Config::disabled() method was removed.
                // We create a new behaviour with a config that is effectively disabled.
                // A better approach for larger apps would be to make mdns an Option in the behaviour struct.
                // For this example, we keep it simple.
                mdns::tokio::Behaviour::new(
                    mdns::Config {
                        // effectively disables active queries
                        query_interval: Duration::from_secs(0),
                        ..Default::default()
                    },
                    local_peer_id,
                )?
            };

            Ok(MeshBehaviour { gossipsub, mdns })
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
                swarm.dial(addr)?;
                println!("[Edge] Dialing cloud node.");
            }
        }
        NodeRole::Drone => {
            swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
        }
    }

    loop {
        tokio::select! {
            Some(command) = command_rx.recv() => {
                match command {
                    Command::Publish(event) => {
                        match serde_json::to_vec(&event) {
                        Ok(bytes) => {
                            if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic.clone(), bytes) {
                                tracing::error!("[{role:?}] Publish error: {e:?}");
                            }
                        }
                        Err(e) => {
                            tracing::error!("[{role:?}] Failed to serialize CloudEvent: {e}");
                        }
                    }                    }
                }
            },
            event = swarm.select_next_some() => {
                 match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        tracing::debug!("[{role:?}] Listening on {address}");
                    }
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                        for (peer_id, _multiaddr) in list {
                            tracing::debug!("[{role:?}] mDNS discovered: {peer_id}");
                            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                        }
                    },
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                        for (peer_id, _multiaddr) in list {
                            tracing::debug!("[{role:?}] mDNS expired: {peer_id}");
                            swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                        }
                    },
                    SwarmEvent::Behaviour(MeshBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                        message,
                        ..
                    })) => {
                        match serde_json::from_slice::<CloudEvent>(&message.data) {
                            Ok(event) => {
                                tracing::debug!("[{role:?}] Received CloudEvent from peer: {}", event.source);
                                if event_tx.send(Event::Message(event)).await.is_err() {
                                    tracing::error!("[{role:?}] Event receiver closed.");
                                    break;
                                }
                            }
                            Err(e) => {
                                tracing::error!("[{role:?}] Failed to deserialize CloudEvent: {e}");
                            }
                        }
                    },
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        tracing::info!("[{role:?}] Connected to {peer_id}");
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}
