use crate::collab_indexer::DocumentIndexer;
use crate::vector::embedder::Embedder;
use app_error::AppError;
use appflowy_ai_client::dto::EmbeddingModel;
use async_trait::async_trait;
use collab::preclude::Collab;
use collab_entity::CollabType;
use database_entity::dto::{AFCollabEmbeddedChunk, AFCollabEmbeddings};
use infra::env_util::get_env_var;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

#[async_trait]
pub trait Indexer: Send + Sync {
  fn create_embedded_chunks_from_collab(
    &self,
    collab: &Collab,
    model: EmbeddingModel,
  ) -> Result<Vec<AFCollabEmbeddedChunk>, AppError>;

  fn create_embedded_chunks_from_text(
    &self,
    object_id: String,
    paragraphs: Vec<String>,
    model: EmbeddingModel,
  ) -> Result<Vec<AFCollabEmbeddedChunk>, AppError>;

  async fn embed(
    &self,
    embedder: &Embedder,
    content: Vec<AFCollabEmbeddedChunk>,
  ) -> Result<Option<AFCollabEmbeddings>, AppError>;
}

/// A structure responsible for resolving different [Indexer] types for different [CollabType]s,
/// including access permission checks for the specific workspaces.
pub struct IndexerProvider {
  indexer_cache: HashMap<CollabType, Arc<dyn Indexer>>,
}

impl IndexerProvider {
  pub fn new() -> Arc<Self> {
    let mut cache: HashMap<CollabType, Arc<dyn Indexer>> = HashMap::new();
    let enabled = get_env_var("APPFLOWY_INDEXER_ENABLED", "true")
      .parse::<bool>()
      .unwrap_or(true);

    info!("Indexer is enabled: {}", enabled);
    if enabled {
      cache.insert(CollabType::Document, Arc::new(DocumentIndexer));
    }
    Arc::new(Self {
      indexer_cache: cache,
    })
  }

  /// Returns indexer for a specific type of [Collab] object.
  /// If collab of given type is not supported or workspace it belongs to has indexing disabled,
  /// returns `None`.
  pub fn indexer_for(&self, collab_type: &CollabType) -> Option<Arc<dyn Indexer>> {
    self.indexer_cache.get(collab_type).cloned()
  }

  pub fn is_indexing_enabled(&self, collab_type: &CollabType) -> bool {
    self.indexer_cache.contains_key(collab_type)
  }
}
