use app_error::AppError;
use collab_entity::CollabType;
use database::index::{get_collab_embedding_framgent_ids, get_collabs_indexed_at};
use indexer::collab_indexer::IndexerProvider;
use indexer::entity::EmbeddingRecord;
use indexer::error::IndexerError;
use indexer::metrics::EmbeddingMetrics;
use indexer::queue::{
  ack_task, default_indexer_group_option, ensure_indexer_consumer_group,
  read_background_embed_tasks,
};
use indexer::scheduler::{spawn_pg_write_embeddings, UnindexedCollabTask, UnindexedData};
use indexer::thread_pool::ThreadPoolNoAbort;
use indexer::vector::embedder::Embedder;
use indexer::vector::open_ai;
use redis::aio::ConnectionManager;
use secrecy::{ExposeSecret, Secret};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{error, info, trace, warn};

pub struct BackgroundIndexerConfig {
  pub enable: bool,
  pub open_api_key: Secret<String>,
  pub tick_interval_secs: u64,
}

pub async fn run_background_indexer(
  pg_pool: PgPool,
  mut redis_client: ConnectionManager,
  embed_metrics: Arc<EmbeddingMetrics>,
  threads: Arc<ThreadPoolNoAbort>,
  config: BackgroundIndexerConfig,
) {
  if !config.enable {
    info!("Background indexer is disabled. Stop background indexer");
    return;
  }

  if config.open_api_key.expose_secret().is_empty() {
    error!("OpenAI API key is not set. Stop background indexer");
    return;
  }

  let indexer_provider = IndexerProvider::new();
  info!("Starting background indexer...");
  if let Err(err) = ensure_indexer_consumer_group(&mut redis_client).await {
    error!("Failed to ensure indexer consumer group: {:?}", err);
  }

  let latest_write_embedding_err = Arc::new(RwLock::new(None));
  let (write_embedding_tx, write_embedding_rx) = unbounded_channel::<EmbeddingRecord>();
  let write_embedding_task_fut = spawn_pg_write_embeddings(
    write_embedding_rx,
    pg_pool.clone(),
    embed_metrics.clone(),
    latest_write_embedding_err.clone(),
  );

  let process_tasks_task_fut = process_upcoming_tasks(
    pg_pool,
    &mut redis_client,
    embed_metrics,
    indexer_provider,
    threads,
    config,
    write_embedding_tx,
    latest_write_embedding_err,
  );

  tokio::select! {
    _ = write_embedding_task_fut => {
      error!("[Background Embedding] Write embedding task stopped");
    },
    _ = process_tasks_task_fut => {
      error!("[Background Embedding] Process tasks task stopped");
    },
  }
}

#[allow(clippy::too_many_arguments)]
async fn process_upcoming_tasks(
  pg_pool: PgPool,
  redis_client: &mut ConnectionManager,
  metrics: Arc<EmbeddingMetrics>,
  indexer_provider: Arc<IndexerProvider>,
  threads: Arc<ThreadPoolNoAbort>,
  config: BackgroundIndexerConfig,
  sender: UnboundedSender<EmbeddingRecord>,
  latest_write_embedding_err: Arc<RwLock<Option<AppError>>>,
) {
  let options = default_indexer_group_option(threads.current_num_threads());
  let mut interval = interval(Duration::from_secs(config.tick_interval_secs));
  interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
  interval.tick().await;

  loop {
    interval.tick().await;

    let latest_error = latest_write_embedding_err.write().await.take();
    if let Some(err) = latest_error {
      if matches!(err, AppError::ActionTimeout(_)) {
        info!(
          "[Background Embedding] last write embedding task failed with timeout, waiting for 30s before retrying..."
        );
        tokio::time::sleep(Duration::from_secs(15)).await;
      }
    }

    match read_background_embed_tasks(redis_client, &options).await {
      Ok(replay) => {
        let all_keys: Vec<String> = replay
          .keys
          .iter()
          .flat_map(|key| key.ids.iter().map(|stream_id| stream_id.id.clone()))
          .collect();

        for key in replay.keys {
          info!(
            "[Background Embedding] processing {} embedding tasks",
            key.ids.len()
          );

          let mut tasks: Vec<UnindexedCollabTask> = key
            .ids
            .into_iter()
            .filter_map(|stream_id| UnindexedCollabTask::try_from(&stream_id).ok())
            .collect();
          tasks.retain(|task| !task.data.is_empty());

          let collab_ids: Vec<(String, CollabType)> = tasks
            .iter()
            .map(|task| (task.object_id.clone(), task.collab_type.clone()))
            .collect();

          let indexed_collabs = get_collabs_indexed_at(&pg_pool, &collab_ids)
            .await
            .unwrap_or_default();

          let all_tasks_len = tasks.len();
          if !indexed_collabs.is_empty() {
            // Filter out tasks where `created_at` is less than `indexed_at`
            tasks.retain(|task| {
              indexed_collabs
                .get(&task.object_id)
                .map_or(true, |indexed_at| task.created_at > indexed_at.timestamp())
            });
          }

          if all_tasks_len != tasks.len() {
            info!("[Background Embedding] filter out {} tasks where `created_at` is less than `indexed_at`", all_tasks_len - tasks.len());
          }

          let start = Instant::now();
          let num_tasks = tasks.len();
          let existing_embeddings = get_collab_embedding_framgent_ids(&pg_pool, collab_ids)
            .await
            .unwrap_or_default();
          let mut join_set = JoinSet::new();
          for task in tasks {
            if let Some(indexer) = indexer_provider.indexer_for(&task.collab_type) {
              let embedder = create_embedder(&config);
              trace!(
                "[Background Embedding] processing task: {}, content:{:?}, collab_type: {}",
                task.object_id,
                task.data,
                task.collab_type
              );
              let paragraphs = match task.data {
                UnindexedData::Paragraphs(paragraphs) => paragraphs,
                UnindexedData::Text(text) => text.split('\n').map(|s| s.to_string()).collect(),
              };
              let mut chunks = match indexer.create_embedded_chunks_from_text(
                task.object_id.clone(),
                paragraphs,
                embedder.model(),
              ) {
                Ok(chunks) => chunks,
                Err(err) => {
                  warn!(
                    "[Background Embedding] failed to create embedded chunks for task: {}, error: {:?}",
                    task.object_id,
                    err
                  );
                  continue;
                },
              };
              if let Some(existing_chunks) = existing_embeddings.get(&task.object_id) {
                for chunk in chunks.iter_mut() {
                  if existing_chunks.contains(&chunk.fragment_id) {
                    chunk.content = None; // Clear content to mark unchanged chunk
                    chunk.embedding = None;
                  }
                }
              }
              join_set.spawn(async move {
                let embeddings = indexer.embed(&embedder, chunks).await.ok()?;
                embeddings.map(|embeddings| EmbeddingRecord {
                  workspace_id: task.workspace_id,
                  object_id: task.object_id,
                  collab_type: task.collab_type,
                  tokens_used: embeddings.tokens_consumed,
                  contents: embeddings.params,
                })
              });
            }
          }

          while let Some(Ok(result)) = join_set.join_next().await {
            match result {
              None => metrics.record_failed_embed_count(1),
              Some(record) => {
                metrics.record_embed_count(1);
                trace!(
                  "[Background Embedding] send {} embedding record to write task",
                  record.object_id
                );
                if let Err(err) = sender.send(record) {
                  trace!(
                    "[Background Embedding] failed to send embedding record to write task: {:?}",
                    err
                  );
                }
              },
            }
          }

          let cost = start.elapsed().as_millis();
          metrics.record_gen_embedding_time(num_tasks as u32, cost);
        }

        if !all_keys.is_empty() {
          match ack_task(redis_client, all_keys, true).await {
            Ok(_) => trace!("[Background embedding]: delete tasks from stream"),
            Err(err) => {
              error!("[Background Embedding] Failed to ack tasks: {:?}", err);
            },
          }
        }
      },
      Err(err) => {
        error!("[Background Embedding] Failed to read tasks: {:?}", err);
        if matches!(err, IndexerError::StreamGroupNotExist(_)) {
          if let Err(err) = ensure_indexer_consumer_group(redis_client).await {
            error!(
              "[Background Embedding] Failed to ensure indexer consumer group: {:?}",
              err
            );
          }
        }
      },
    }
  }
}

fn create_embedder(config: &BackgroundIndexerConfig) -> Embedder {
  Embedder::OpenAI(open_ai::Embedder::new(
    config.open_api_key.expose_secret().clone(),
  ))
}
