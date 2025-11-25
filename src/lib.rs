// src/lib.rs

pub mod adapters;
pub mod cloudevents;
pub mod core;
pub mod error;
pub mod net;
pub mod ports;

// Re-export the primary public types for easy use by consumers.
pub use crate::cloudevents::{CloudEvent, CloudEventData};
pub use crate::core::{RevenantConfig, RevenantService};
pub use crate::error::RevenantError;
pub use crate::ports::{DataRepository, DataSyncer, EventProcessor, RealtimeSyncer};

// Re-export the concrete adapter implementations so users can construct them.
pub use crate::adapters::noop_repo::NoOpRepository;
pub use crate::adapters::p2p_syncer::{P2pNodeRole, P2pSyncer};
pub use crate::adapters::redis_syncer::RedisSyncer;
pub use crate::adapters::sqlite_repo::SqliteRepository;

// Re-export libp2p for consumers to use types like Keypair
pub use libp2p;
