use crate::collab_indexer::IndexerProvider;
use crate::entity::EmbeddingRecord;
use crate::metrics::EmbeddingMetrics;
use crate::queue::add_background_embed_task;
use crate::vector::embedder::Embedder;
use crate::vector::open_ai;
use app_error::AppError;
use appflowy_ai_client::dto::{EmbeddingRequest, OpenAIEmbeddingResponse};
use collab::preclude::Collab;
use collab_document::document::DocumentBody;
use collab_entity::CollabType;
use database::collab::CollabStorage;
use database::index::{
  get_collab_embedding_framgent_ids, update_collab_indexed_at, upsert_collab_embeddings,
};
use database::workspace::select_workspace_settings;
use infra::env_util::get_env_var;
use redis::aio::ConnectionManager;
use secrecy::{ExposeSecret, Secret};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::cmp::max;
use std::collections::HashSet;
use std::ops::DerefMut;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::RwLock as TokioRwLock;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{debug, error, info, instrument, trace, warn};
use uuid::Uuid;

pub struct IndexerScheduler {
  pub(crate) indexer_provider: Arc<IndexerProvider>,
  pub(crate) pg_pool: PgPool,
  pub(crate) storage: Arc<dyn CollabStorage>,
  #[allow(dead_code)]
  pub(crate) metrics: Arc<EmbeddingMetrics>,
  write_embedding_tx: UnboundedSender<EmbeddingRecord>,
  gen_embedding_tx: mpsc::Sender<UnindexedCollabTask>,
  config: IndexerConfiguration,
  redis_client: ConnectionManager,
}

#[derive(Debug)]
pub struct IndexerConfiguration {
  pub enable: bool,
  pub openai_api_key: Secret<String>,
  /// High watermark for the number of embeddings that can be buffered before being written to the database.
  pub embedding_buffer_size: usize,
}

impl IndexerScheduler {
  pub fn new(
    indexer_provider: Arc<IndexerProvider>,
    pg_pool: PgPool,
    storage: Arc<dyn CollabStorage>,
    metrics: Arc<EmbeddingMetrics>,
    config: IndexerConfiguration,
    redis_client: ConnectionManager,
  ) -> Arc<Self> {
    // Since threads often block while waiting for I/O, you can use more threads than CPU cores to improve concurrency.
    // A good rule of thumb is 2x to 10x the number of CPU cores
    let num_thread = max(
      get_env_var("APPFLOWY_INDEXER_SCHEDULER_NUM_THREAD", "50")
        .parse::<usize>()
        .unwrap_or(50),
      5,
    );

    info!("Indexer scheduler config: {:?}", config);
    let (write_embedding_tx, write_embedding_rx) = unbounded_channel::<EmbeddingRecord>();
    let (gen_embedding_tx, gen_embedding_rx) =
      mpsc::channel::<UnindexedCollabTask>(config.embedding_buffer_size);

    let this = Arc::new(Self {
      indexer_provider,
      pg_pool,
      storage,
      metrics,
      write_embedding_tx,
      gen_embedding_tx,
      config,
      redis_client,
    });

    info!(
      "Indexer scheduler is enabled: {}, num threads: {}",
      this.index_enabled(),
      num_thread
    );

    let latest_write_embedding_err = Arc::new(TokioRwLock::new(None));
    if this.index_enabled() {
      tokio::spawn(generate_embeddings_loop(
        gen_embedding_rx,
        Arc::downgrade(&this),
        num_thread,
        latest_write_embedding_err.clone(),
      ));

      tokio::spawn(spawn_pg_write_embeddings(
        write_embedding_rx,
        this.pg_pool.clone(),
        this.metrics.clone(),
        latest_write_embedding_err,
      ));
    }

    this
  }

  fn index_enabled(&self) -> bool {
    // if indexing is disabled, return false
    if !self.config.enable {
      return false;
    }

    // if openai api key is empty, return false
    if self.config.openai_api_key.expose_secret().is_empty() {
      return false;
    }

    true
  }

  pub fn is_indexing_enabled(&self, collab_type: &CollabType) -> bool {
    self.indexer_provider.is_indexing_enabled(collab_type)
  }

  pub(crate) fn create_embedder(&self) -> Result<Embedder, AppError> {
    if self.config.openai_api_key.expose_secret().is_empty() {
      return Err(AppError::AIServiceUnavailable(
        "OpenAI API key is empty".to_string(),
      ));
    }

    Ok(Embedder::OpenAI(open_ai::Embedder::new(
      self.config.openai_api_key.expose_secret().clone(),
    )))
  }

  pub async fn create_search_embeddings(
    &self,
    request: EmbeddingRequest,
  ) -> Result<OpenAIEmbeddingResponse, AppError> {
    let embedder = self.create_embedder()?;
    let embeddings = embedder.async_embed(request).await?;
    Ok(embeddings)
  }

  pub fn embed_in_background(
    &self,
    pending_collabs: Vec<UnindexedCollabTask>,
  ) -> Result<(), AppError> {
    if !self.index_enabled() {
      return Ok(());
    }

    let redis_client = self.redis_client.clone();
    tokio::spawn(add_background_embed_task(redis_client, pending_collabs));
    Ok(())
  }

  pub fn embed_immediately(&self, pending_collab: UnindexedCollabTask) -> Result<(), AppError> {
    if !self.index_enabled() {
      return Ok(());
    }
    if let Err(err) = self.gen_embedding_tx.try_send(pending_collab) {
      match err {
        TrySendError::Full(pending) => {
          warn!("[Embedding] Embedding queue is full, embedding in background");
          self.embed_in_background(vec![pending])?;
          self.metrics.record_failed_embed_count(1);
        },
        TrySendError::Closed(_) => {
          error!("Failed to send embedding record: channel closed");
        },
      }
    }

    Ok(())
  }

  pub fn index_pending_collab_one(
    &self,
    pending_collab: UnindexedCollabTask,
    background: bool,
  ) -> Result<(), AppError> {
    if !self.index_enabled() {
      return Ok(());
    }

    let indexer = self
      .indexer_provider
      .indexer_for(&pending_collab.collab_type);
    if indexer.is_none() {
      return Ok(());
    }

    if background {
      let _ = self.embed_in_background(vec![pending_collab]);
    } else {
      let _ = self.embed_immediately(pending_collab);
    }
    Ok(())
  }

  /// Index all pending collabs in the background
  pub fn index_pending_collabs(
    &self,
    mut pending_collabs: Vec<UnindexedCollabTask>,
  ) -> Result<(), AppError> {
    if !self.index_enabled() {
      return Ok(());
    }

    pending_collabs.retain(|collab| self.is_indexing_enabled(&collab.collab_type));
    if pending_collabs.is_empty() {
      return Ok(());
    }

    info!("indexing {} collabs in background", pending_collabs.len());
    let _ = self.embed_in_background(pending_collabs);

    Ok(())
  }

  pub async fn index_collab_immediately(
    &self,
    workspace_id: &str,
    object_id: &str,
    collab: &Collab,
    collab_type: &CollabType,
  ) -> Result<(), AppError> {
    if !self.index_enabled() {
      return Ok(());
    }

    if !self.is_indexing_enabled(collab_type) {
      return Ok(());
    }

    match collab_type {
      CollabType::Document => {
        let txn = collab.transact();
        let text = DocumentBody::from_collab(collab)
          .map(|body| body.paragraphs(txn))
          .unwrap_or_default();

        if !text.is_empty() {
          let pending = UnindexedCollabTask::new(
            Uuid::parse_str(workspace_id)?,
            object_id.to_string(),
            collab_type.clone(),
            UnindexedData::Paragraphs(text),
          );
          self.embed_immediately(pending)?;
        }
      },
      _ => {
        // TODO(nathan): support other collab types
      },
    }

    Ok(())
  }

  pub async fn can_index_workspace(&self, workspace_id: &str) -> Result<bool, AppError> {
    if !self.index_enabled() {
      return Ok(false);
    }

    let uuid = Uuid::parse_str(workspace_id)?;
    let settings = select_workspace_settings(&self.pg_pool, &uuid).await?;
    match settings {
      None => Ok(true),
      Some(settings) => Ok(!settings.disable_search_indexing),
    }
  }
}

async fn generate_embeddings_loop(
  mut rx: mpsc::Receiver<UnindexedCollabTask>,
  scheduler: Weak<IndexerScheduler>,
  buffer_size: usize,
  latest_write_embedding_err: Arc<TokioRwLock<Option<AppError>>>,
) {
  let mut buf = Vec::with_capacity(buffer_size);
  loop {
    let latest_error = latest_write_embedding_err.write().await.take();
    if let Some(err) = latest_error {
      if matches!(err, AppError::ActionTimeout(_)) {
        info!(
          "[Embedding] last write embedding task failed with timeout, waiting for 30s before retrying..."
        );
        tokio::time::sleep(Duration::from_secs(30)).await;
      }
    }

    let n = rx.recv_many(&mut buf, buffer_size).await;
    let scheduler = match scheduler.upgrade() {
      Some(scheduler) => scheduler,
      None => {
        error!("[Embedding] Failed to upgrade scheduler");
        break;
      },
    };

    if n == 0 {
      info!("[Embedding] Stop generating embeddings");
      break;
    }

    let start = Instant::now();
    let records = buf.drain(..n).collect::<Vec<_>>();
    trace!(
      "[Embedding] received {} embeddings to generate",
      records.len()
    );
    let metrics = scheduler.metrics.clone();
    let indexer_provider = scheduler.indexer_provider.clone();
    let write_embedding_tx = scheduler.write_embedding_tx.clone();
    let embedder = scheduler.create_embedder();
    match embedder {
      Ok(embedder) => {
        let params: Vec<_> = records
          .iter()
          .map(|r| (r.object_id.clone(), r.collab_type.clone()))
          .collect();
        let existing_embeddings =
          match get_collab_embedding_framgent_ids(&scheduler.pg_pool, params).await {
            Ok(existing_embeddings) => existing_embeddings,
            Err(err) => {
              error!("[Embedding] failed to get existing embeddings: {}", err);
              Default::default()
            },
          };
        let mut join_set = JoinSet::new();
        for record in records {
          if let Some(indexer) = indexer_provider.indexer_for(&record.collab_type) {
            metrics.record_embed_count(1);
            let paragraphs = match record.data {
              UnindexedData::Paragraphs(paragraphs) => paragraphs,
              UnindexedData::Text(text) => text.split('\n').map(|s| s.to_string()).collect(),
            };
            let embedder = embedder.clone();
            match indexer.create_embedded_chunks_from_text(
              record.object_id.clone(),
              paragraphs,
              embedder.model(),
            ) {
              Ok(mut chunks) => {
                if let Some(fragment_ids) = existing_embeddings.get(&record.object_id) {
                  for chunk in chunks.iter_mut() {
                    if fragment_ids.contains(&chunk.fragment_id) {
                      // we already had an embedding for this chunk
                      chunk.content = None;
                      chunk.embedding = None;
                    }
                  }
                }
                join_set.spawn(async move {
                  if chunks.is_empty() {
                    return Ok(None);
                  }

                  let result = indexer.embed(&embedder, chunks).await;
                  match result {
                    Ok(Some(embeddings)) => {
                      let record = EmbeddingRecord {
                        workspace_id: record.workspace_id,
                        object_id: record.object_id,
                        collab_type: record.collab_type,
                        tokens_used: embeddings.tokens_consumed,
                        contents: embeddings.params,
                      };
                      Ok(Some(record))
                    },
                    Ok(None) => Ok(None),
                    Err(err) => Err(err),
                  }
                });
              },
              Err(err) => {
                metrics.record_failed_embed_count(1);
                warn!(
                  "Failed to create embedded chunks for collab: {}, error:{}",
                  record.object_id, err
                );
                continue;
              },
            }
          }
        }
        while let Some(Ok(res)) = join_set.join_next().await {
          scheduler
            .metrics
            .record_gen_embedding_time(n as u32, start.elapsed().as_millis());
          match res {
            Ok(Some(record)) => {
              if let Err(err) = write_embedding_tx.send(record) {
                error!("Failed to send embedding record: {}", err);
              }
            },
            Ok(None) => debug!("No embedding for collab"),
            Err(err) => {
              metrics.record_failed_embed_count(1);
              warn!(
                "Failed to create embeddings content for collab, error:{}",
                err
              );
            },
          }
        }
      },
      Err(err) => error!("[Embedding] Failed to create embedder: {}", err),
    }
  }
}

const EMBEDDING_RECORD_BUFFER_SIZE: usize = 10;
pub async fn spawn_pg_write_embeddings(
  mut rx: UnboundedReceiver<EmbeddingRecord>,
  pg_pool: PgPool,
  metrics: Arc<EmbeddingMetrics>,
  latest_write_embedding_error: Arc<TokioRwLock<Option<AppError>>>,
) {
  let mut buf = Vec::with_capacity(EMBEDDING_RECORD_BUFFER_SIZE);
  loop {
    let n = rx.recv_many(&mut buf, EMBEDDING_RECORD_BUFFER_SIZE).await;
    if n == 0 {
      info!("Stop writing embeddings");
      break;
    }

    trace!("[Embedding] received {} embeddings to write", n);
    let start = Instant::now();
    let records = buf.drain(..n).collect::<Vec<_>>();
    for record in records.iter() {
      info!(
        "[Embedding] generate collab:{} embeddings, tokens used: {}",
        record.object_id, record.tokens_used
      );
    }

    let result = timeout(
      Duration::from_secs(20),
      batch_insert_records(&pg_pool, records),
    )
    .await
    .unwrap_or_else(|_| {
      Err(AppError::ActionTimeout(
        "timeout when writing embeddings".to_string(),
      ))
    });

    match result {
      Ok(_) => {
        trace!("[Embedding] save {} embeddings to disk", n);
        metrics.record_write_embedding_time(start.elapsed().as_millis());
      },
      Err(err) => {
        error!("Failed to write collab embedding to disk:{}", err);
        latest_write_embedding_error.write().await.replace(err);
      },
    }
  }
}

#[instrument(level = "trace", skip_all)]
pub(crate) async fn batch_insert_records(
  pg_pool: &PgPool,
  records: Vec<EmbeddingRecord>,
) -> Result<(), AppError> {
  let mut seen = HashSet::new();
  let records = records
    .into_iter()
    .filter(|record| seen.insert(record.object_id.clone()))
    .collect::<Vec<_>>();

  let mut txn = pg_pool.begin().await?;
  for record in records {
    update_collab_indexed_at(
      txn.deref_mut(),
      &record.object_id,
      &record.collab_type,
      chrono::Utc::now(),
    )
    .await?;

    upsert_collab_embeddings(
      &mut txn,
      &record.workspace_id,
      &record.object_id,
      record.collab_type,
      record.tokens_used,
      record.contents,
    )
    .await?;
  }
  txn.commit().await.map_err(|e| {
    error!("[Embedding] Failed to commit transaction: {:?}", e);
    e
  })?;

  Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnindexedCollabTask {
  pub workspace_id: Uuid,
  pub object_id: String,
  pub collab_type: CollabType,
  pub data: UnindexedData,
  pub created_at: i64,
}

impl UnindexedCollabTask {
  pub fn new(
    workspace_id: Uuid,
    object_id: String,
    collab_type: CollabType,
    data: UnindexedData,
  ) -> Self {
    Self {
      workspace_id,
      object_id,
      collab_type,
      data,
      created_at: chrono::Utc::now().timestamp(),
    }
  }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum UnindexedData {
  Text(String),
  Paragraphs(Vec<String>),
}

impl UnindexedData {
  pub fn is_empty(&self) -> bool {
    match self {
      UnindexedData::Text(text) => text.is_empty(),
      UnindexedData::Paragraphs(text) => text.is_empty(),
    }
  }
}
