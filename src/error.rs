// src/error.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RevenantError {
    #[error("Tokio database error: {0}")]
    TokioDatabase(#[from] tokio_rusqlite::Error),

    #[error("Core database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Operation failed: {0}")]
    Operation(String),
}
