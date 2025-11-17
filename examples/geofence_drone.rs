use clap::Parser;
use revenant::adapters::p2p_syncer::{NetworkEvent, P2pNodeRole, P2pSyncer};
use revenant::adapters::sqlite_repo::SqliteRepository;
use revenant::cloudevents::{CloudEvent, CloudEventData};
use revenant::core::{RevenantConfig, RevenantService};
use revenant::net::Multiaddr;
use revenant::ports::EventProcessor;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

// --- 1. Define our Business Logic ---

/// A simple processor that checks if a drone has breached a geofence.
struct GeofenceProcessor {
    // A simple rectangular geofence.
    min_lat: f64,
    max_lat: f64,
    min_lon: f64,
    max_lon: f64,
}

impl EventProcessor for GeofenceProcessor {
    /// This is our "hot path". It runs immediately for every event.
    fn process_event(&self, event: CloudEvent) -> Option<CloudEvent> {
        // We only care about telemetry events.
        if event.event_type != "com.drone.telemetry.v1" {
            // If we receive an alert event, we don't need to process it again.
            // In a real app, you might have different processors for different event types.
            return None;
        }

        if let Some(CloudEventData::EventData(data)) = event.data {
            let lat = data.get("latitude")?.as_f64()?;
            let lon = data.get("longitude")?.as_f64()?;

            // Check if the coordinates are outside our geofence.
            if lat < self.min_lat || lat > self.max_lat || lon < self.min_lon || lon > self.max_lon
            {
                println!(
                    "[Processor] GEOFENCE BREACH DETECTED at ({}, {})!",
                    lat, lon
                );

                // If a breach is detected, create a new "alert" event.
                // This new event will be stored and synced by Revenant.
                let alert_data = serde_json::json!({
                    "breach_latitude": lat,
                    "breach_longitude": lon,
                    "geofence_id": "central-park-boundary",
                    "severity": "critical",
                });

                return Some(
                    CloudEvent::new(
                        event.source, // Keep the original drone's ID as the source
                        "com.geofence.alert.v1".to_string(),
                        Some(CloudEventData::EventData(alert_data)),
                    )
                    .build(),
                );
            }
        }
        None
    }
}

// --- 2. Define Command Line Arguments ---

#[derive(Parser, Debug)]
#[clap(name = "revenant-geofence-example")]
struct Opts {
    #[clap(subcommand)]
    sub: Subcommand,
}

#[derive(Parser, Debug)]
enum Subcommand {
    Cloud,
    Edge {
        #[clap(long)]
        cloud_addr: Multiaddr,
    },
    Drone,
}

// --- 3. Main Application Entrypoint ---

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = Opts::parse();
    let (role, cloud_addr) = match opts.sub {
        Subcommand::Cloud => (P2pNodeRole::Cloud, None),
        Subcommand::Edge { cloud_addr } => (P2pNodeRole::Edge, Some(cloud_addr)),
        Subcommand::Drone => (P2pNodeRole::Drone, None),
    };

    // --- 4. Construct Revenant Components ---

    // The EventProcessor (our business logic)
    let processor = Arc::new(GeofenceProcessor {
        min_lat: 40.76,
        max_lat: 40.80,
        min_lon: -73.98,
        max_lon: -73.95,
    });

    // The DataRepository (local persistence)
    let db_path = format!("{}_events.db", format!("{:?}", role).to_lowercase());
    let repository = Arc::new(SqliteRepository::new(&db_path).await?);

    // The DataSyncer (P2P networking) and its event receiver
    let (syncer, mut event_rx) = P2pSyncer::new(role.clone(), cloud_addr).await?;
    let syncer = Arc::new(syncer);

    // The Revenant Configuration
    let config = RevenantConfig {
        sync_interval: Duration::from_secs(10),
        batch_size: 50,
        max_retry_attempts: 3,
    };

    // --- 5. Instantiate and Start the Revenant Service ---

    let revenant_service = Arc::new(RevenantService::new(
        processor,
        repository.clone(),
        syncer,
        config,
    ));

    println!("Revenant service initialized in {:?} mode.", role);
    println!("Database path: {}", db_path);

    // --- 6. Run Role-Specific Application Logic ---

    if role == P2pNodeRole::Drone {
        let drone_id = format!("drone-{}", Uuid::new_v4());
        println!("Running as Drone with ID: {}", drone_id);

        // Simulate a drone flying and periodically sending telemetry.
        let mut sequence = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(3)).await;

            let (lat, lon) = if sequence % 5 == 0 {
                (40.81, -73.99) // Outside the geofence
            } else {
                (40.78, -73.96) // Inside the geofence
            };

            let telemetry_data = serde_json::json!({
                "latitude": lat,
                "longitude": lon,
                "altitude": 100.0,
                "sequence": sequence,
            });

            let telemetry_event = CloudEvent::new(
                drone_id.clone(),
                "com.drone.telemetry.v1".to_string(),
                Some(CloudEventData::EventData(telemetry_data)),
            )
            .build();

            println!("[Drone App] Submitting telemetry #{}", sequence);

            if let Err(e) = revenant_service.submit(telemetry_event).await {
                eprintln!("[Drone App] Failed to submit event: {}", e);
            }

            sequence += 1;
        }
    } else {
        // Cloud and Edge nodes run a listener task.
        println!("Running as {:?}. Listening for incoming events...", role);

        let service_clone = revenant_service.clone();
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                if let NetworkEvent::Received(cloud_event) = event {
                    println!(
                        "[{:?} App] Received event from network. Submitting to local service.",
                        role
                    );

                    // Submit the received event to this node's own RevenantService.
                    // The service will run its own processor on the event.
                    // If the processor generates a new event, it will be stored locally.
                    if let Err(e) = service_clone.submit(cloud_event).await {
                        eprintln!(
                            "[{:?} App] Failed to submit received event: {}",
                            e, cloud_event
                        );
                    }
                }
            }
        });

        // The main task for cloud/edge can now just do periodic health checks.
        loop {
            tokio::time::sleep(Duration::from_secs(15)).await;
            let pending = repository.count_pending().await?;
            println!(
                "[{:?} App] Health Check: {} events pending sync.",
                role, pending
            );
        }
    }
}
