use crate::collab_indexer::Indexer;
use crate::vector::embedder::Embedder;
use crate::vector::open_ai::group_paragraphs_by_max_content_len;
use anyhow::anyhow;
use app_error::AppError;
use appflowy_ai_client::dto::{
  EmbeddingEncodingFormat, EmbeddingInput, EmbeddingModel, EmbeddingOutput, EmbeddingRequest,
};
use async_trait::async_trait;
use collab::preclude::Collab;
use collab_document::document::DocumentBody;
use collab_entity::CollabType;
use database_entity::dto::{AFCollabEmbeddedChunk, AFCollabEmbeddings, EmbeddingContentType};
use serde_json::json;
use tracing::trace;
use twox_hash::xxhash64::Hasher;

pub struct DocumentIndexer;

#[async_trait]
impl Indexer for DocumentIndexer {
  fn create_embedded_chunks_from_collab(
    &self,
    collab: &Collab,
    model: EmbeddingModel,
  ) -> Result<Vec<AFCollabEmbeddedChunk>, AppError> {
    let object_id = collab.object_id().to_string();
    let document = DocumentBody::from_collab(collab).ok_or_else(|| {
      anyhow!(
        "Failed to get document body from collab `{}`: schema is missing required fields",
        object_id
      )
    })?;

    let paragraphs = document.paragraphs(collab.transact());
    self.create_embedded_chunks_from_text(object_id, paragraphs, model)
  }

  fn create_embedded_chunks_from_text(
    &self,
    object_id: String,
    paragraphs: Vec<String>,
    model: EmbeddingModel,
  ) -> Result<Vec<AFCollabEmbeddedChunk>, AppError> {
    split_text_into_chunks(object_id, paragraphs, CollabType::Document, model)
  }

  async fn embed(
    &self,
    embedder: &Embedder,
    mut content: Vec<AFCollabEmbeddedChunk>,
  ) -> Result<Option<AFCollabEmbeddings>, AppError> {
    if content.is_empty() {
      return Ok(None);
    }

    let contents: Vec<_> = content
      .iter()
      .map(|fragment| fragment.content.clone().unwrap_or_default())
      .collect();
    let resp = embedder
      .async_embed(EmbeddingRequest {
        input: EmbeddingInput::StringArray(contents),
        model: embedder.model().name().to_string(),
        encoding_format: EmbeddingEncodingFormat::Float,
        dimensions: EmbeddingModel::TextEmbedding3Small.default_dimensions(),
      })
      .await?;

    trace!(
      "[Embedding] request {} embeddings, received {} embeddings",
      content.len(),
      resp.data.len()
    );

    for embedding in resp.data {
      let param = &mut content[embedding.index as usize];
      if param.content.is_some() {
        // we only set the embedding if the content was not marked as unchanged
        let embedding: Vec<f32> = match embedding.embedding {
          EmbeddingOutput::Float(embedding) => embedding.into_iter().map(|f| f as f32).collect(),
          EmbeddingOutput::Base64(_) => {
            return Err(AppError::OpenError(
              "Unexpected base64 encoding".to_string(),
            ))
          },
        };
        param.embedding = Some(embedding);
      }
    }

    Ok(Some(AFCollabEmbeddings {
      tokens_consumed: resp.usage.total_tokens as u32,
      params: content,
    }))
  }
}

fn split_text_into_chunks(
  object_id: String,
  paragraphs: Vec<String>,
  collab_type: CollabType,
  embedding_model: EmbeddingModel,
) -> Result<Vec<AFCollabEmbeddedChunk>, AppError> {
  debug_assert!(matches!(
    embedding_model,
    EmbeddingModel::TextEmbedding3Small
  ));

  if paragraphs.is_empty() {
    return Ok(vec![]);
  }
  // We assume that every token is ~4 bytes. We're going to split document content into fragments
  // of ~2000 tokens each.
  let split_contents = group_paragraphs_by_max_content_len(paragraphs, 8000);
  let metadata =
    json!({"id": object_id, "source": "appflowy", "name": "document", "collab_type": collab_type });
  Ok(
    split_contents
      .into_iter()
      .enumerate()
      .map(|(index, content)| {
        let consistent_hash = Hasher::oneshot(0, content.as_bytes());
        AFCollabEmbeddedChunk {
          fragment_id: format!("{:x}", consistent_hash),
          object_id: object_id.clone(),
          content_type: EmbeddingContentType::PlainText,
          content: Some(content),
          embedding: None,
          metadata: metadata.clone(),
          fragment_index: index as i32,
          embedded_type: 0,
        }
      })
      .collect(),
  )
}
