mod collab_cache;
mod controller;

pub type WorkspaceId = uuid::Uuid;
pub type ObjectId = uuid::Uuid;

pub use controller::{Options, WorkspaceController};
use yrs::error::UpdateError;

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
  #[error("WebSocket error: {0}")]
  Ws(#[from] tokio_tungstenite::tungstenite::Error),
  #[error("HTTP error: {0}")]
  Http(#[from] tokio_tungstenite::tungstenite::http::Error),
  #[error("collab decoding error: {0}")]
  YrsDecoding(#[from] yrs::encoding::read::Error),
  #[error("failed to apply update: {0}")]
  ApplyUpdate(#[from] UpdateError),
  #[error("awareness state failure: {0}")]
  Awareness(#[from] yrs::sync::awareness::Error),
  #[error("controller is closed")]
  Closed,
}
