use crate::cloudevents::CloudEvent;
use crate::core::Workload;
use crate::error::RevenantError;
use crate::ports::DataRepository;
use async_trait::async_trait;
use tokio_rusqlite::Connection;
use uuid::Uuid;

pub struct SqliteRepository {
    conn: Connection,
}

impl SqliteRepository {
    pub async fn new(path: &str) -> Result<Self, RevenantError> {
        let conn = Connection::open(path).await?;
        // FIX: The closure now correctly takes `&mut conn`
        conn.call(|conn| Self::setup_schema(conn)).await?;
        Ok(Self { conn })
    }

    // FIX: Signature changed to take a mutable connection to match `conn.call`'s expectation.
    fn setup_schema(conn: &mut rusqlite::Connection) -> rusqlite::Result<()> {
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS workloads (
                id TEXT PRIMARY KEY,
                event_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                sync_attempts INTEGER NOT NULL DEFAULT 0,
                is_synced BOOLEAN NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_is_synced ON workloads(is_synced);
            ",
        )?;
        Ok(())
    }

    fn row_to_workload(row: &rusqlite::Row) -> rusqlite::Result<Workload> {
        let event_json: String = row.get("event_json")?;
        let event: CloudEvent = serde_json::from_str(&event_json).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let id_str: String = row.get("id")?;
        let id = Uuid::parse_str(&id_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })?;

        // FIX: Retrieve the timestamp as a string and then parse it.
        let created_at_str: String = row.get("created_at")?;
        let created_at = chrono::DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

        Ok(Workload {
            id,
            event,
            created_at,
            sync_attempts: row.get("sync_attempts")?,
            is_synced: row.get("is_synced")?,
        })
    }
}

#[async_trait]
impl DataRepository for SqliteRepository {
    async fn store(&self, event: &CloudEvent) -> Result<(), RevenantError> {
        let event_id = event.id.clone();
        let event_json = serde_json::to_string(event)?;
        let created_at_str = chrono::Utc::now().to_rfc3339();

        self.conn
            .call(move |conn| {
                // 'move' captures the owned variables above.
                conn.execute(
                    "INSERT INTO workloads (id, event_json, created_at) VALUES (?1, ?2, ?3)",
                    rusqlite::params![
                        event_id,       // Use the owned variable
                        event_json,     // Use the owned variable
                        created_at_str, // Use the owned variable
                    ],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn retrieve_pending_batch(
        &self,
        batch_size: usize,
    ) -> Result<Vec<Workload>, RevenantError> {
        let workloads = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT * FROM workloads WHERE is_synced = 0 ORDER BY created_at ASC LIMIT ?1",
                )?;
                let rows = stmt.query_map([batch_size], Self::row_to_workload)?;
                rows.collect()
            })
            .await?;
        Ok(workloads)
    }

    async fn mark_as_synced(&self, workloads: &[Workload]) -> Result<(), RevenantError> {
        let ids: Vec<String> = workloads.iter().map(|w| w.id.to_string()).collect();
        self.conn
            .call(move |conn| {
                let tx = conn.transaction()?;
                for id in &ids {
                    tx.execute("UPDATE workloads SET is_synced = 1 WHERE id = ?1", [id])?;
                }
                tx.commit()
            })
            .await?;
        Ok(())
    }

    async fn increment_retry_attempts(&self, workloads: &[Workload]) -> Result<(), RevenantError> {
        let ids: Vec<String> = workloads.iter().map(|w| w.id.to_string()).collect();
        self.conn
            .call(move |conn| {
                let tx = conn.transaction()?;
                for id in &ids {
                    tx.execute(
                        "UPDATE workloads SET sync_attempts = sync_attempts + 1 WHERE id = ?1",
                        [id],
                    )?;
                }
                tx.commit()
            })
            .await?;
        Ok(())
    }

    async fn count_pending(&self) -> Result<i64, RevenantError> {
        let count = self
            .conn
            .call(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM workloads WHERE is_synced = 0",
                    [],
                    |row| row.get(0),
                )
            })
            .await?;
        Ok(count)
    }
}
