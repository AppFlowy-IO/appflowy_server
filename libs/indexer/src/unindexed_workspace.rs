use crate::collab_indexer::IndexerProvider;
use crate::entity::{EmbeddingRecord, UnindexedCollab};
use crate::scheduler::{batch_insert_records, IndexerScheduler};
use crate::vector::embedder::Embedder;
use appflowy_ai_client::dto::EmbeddingModel;
use collab::core::collab::DataSource;
use collab::core::origin::CollabOrigin;
use collab::preclude::Collab;
use collab_entity::CollabType;
use database::collab::{CollabStorage, GetCollabOrigin};
use database::index::{get_collab_embedding_framgent_ids, stream_collabs_without_embeddings};
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelIterator;
use sqlx::pool::PoolConnection;
use sqlx::Postgres;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::{error, info, trace};
use uuid::Uuid;

#[allow(dead_code)]
pub(crate) async fn index_workspace(scheduler: Arc<IndexerScheduler>, workspace_id: Uuid) {
  let mut retry_delay = Duration::from_secs(2);
  loop {
    let conn = scheduler.pg_pool.try_acquire();
    if conn.is_none() {
      tokio::time::sleep(retry_delay).await;
      // 4s, 8s, 16s, 32s, 60s
      retry_delay = retry_delay.saturating_mul(2);
      if retry_delay > Duration::from_secs(60) {
        error!("[Embedding] failed to acquire db connection for 1 minute, stop indexing");
        break;
      }
      continue;
    }

    retry_delay = Duration::from_secs(2);
    let mut conn = conn.unwrap();
    let mut stream =
      stream_unindexed_collabs(&mut conn, workspace_id, scheduler.storage.clone(), 50).await;

    let batch_size = 5;
    let mut unindexed_collabs = Vec::with_capacity(batch_size);
    while let Some(Ok(collab)) = stream.next().await {
      if unindexed_collabs.len() < batch_size {
        unindexed_collabs.push(collab);
        continue;
      }

      index_then_write_embedding_to_disk(&scheduler, std::mem::take(&mut unindexed_collabs)).await;
    }

    if !unindexed_collabs.is_empty() {
      index_then_write_embedding_to_disk(&scheduler, unindexed_collabs).await;
    }
  }
}

async fn index_then_write_embedding_to_disk(
  scheduler: &Arc<IndexerScheduler>,
  unindexed_collabs: Vec<UnindexedCollab>,
) {
  info!(
    "[Embedding] process batch {:?} embeddings",
    unindexed_collabs
      .iter()
      .map(|v| v.object_id.clone())
      .collect::<Vec<_>>()
  );

  if let Ok(embedder) = scheduler.create_embedder() {
    let start = Instant::now();
    let object_ids = unindexed_collabs
      .iter()
      .map(|v| (v.object_id.clone(), v.collab_type.clone()))
      .collect::<Vec<_>>();
    match get_collab_embedding_framgent_ids(&scheduler.pg_pool, object_ids).await {
      Ok(existing_embeddings) => {
        let embeddings = create_embeddings(
          embedder,
          &scheduler.indexer_provider,
          unindexed_collabs,
          existing_embeddings,
        )
        .await;
        scheduler
          .metrics
          .record_gen_embedding_time(embeddings.len() as u32, start.elapsed().as_millis());

        let write_start = Instant::now();
        let n = embeddings.len();
        match batch_insert_records(&scheduler.pg_pool, embeddings).await {
          Ok(_) => trace!(
            "[Embedding] upsert {} embeddings success, cost:{}ms",
            n,
            write_start.elapsed().as_millis()
          ),
          Err(err) => error!("{}", err),
        }

        scheduler
          .metrics
          .record_write_embedding_time(write_start.elapsed().as_millis());
        tokio::time::sleep(Duration::from_secs(5)).await;
      },
      Err(err) => error!("[Embedding] failed to get fragment ids: {}", err),
    }
  } else {
    trace!("[Embedding] no embeddings to process in this batch");
  }
}

async fn stream_unindexed_collabs(
  conn: &mut PoolConnection<Postgres>,
  workspace_id: Uuid,
  storage: Arc<dyn CollabStorage>,
  limit: i64,
) -> BoxStream<Result<UnindexedCollab, anyhow::Error>> {
  let cloned_storage = storage.clone();
  stream_collabs_without_embeddings(conn, workspace_id, limit)
    .await
    .map(move |result| {
      let storage = cloned_storage.clone();
      async move {
        match result {
          Ok(cid) => match cid.collab_type {
            CollabType::Document => {
              let collab = storage
                .get_encode_collab(GetCollabOrigin::Server, cid.clone().into(), false)
                .await?;

              Ok(Some(UnindexedCollab {
                workspace_id: cid.workspace_id,
                object_id: cid.object_id,
                collab_type: cid.collab_type,
                collab,
              }))
            },
            // TODO(nathan): support other collab types
            _ => Ok::<_, anyhow::Error>(None),
          },
          Err(e) => Err(e.into()),
        }
      }
    })
    .filter_map(|future| async {
      match future.await {
        Ok(Some(unindexed_collab)) => Some(Ok(unindexed_collab)),
        Ok(None) => None,
        Err(e) => Some(Err(e)),
      }
    })
    .boxed()
}
async fn create_embeddings(
  embedder: Embedder,
  indexer_provider: &Arc<IndexerProvider>,
  unindexed_records: Vec<UnindexedCollab>,
  existing_embeddings: HashMap<String, Vec<String>>,
) -> Vec<EmbeddingRecord> {
  // 1. use parallel iteration since computing text chunks is CPU-intensive task
  let records = compute_embedding_records(
    indexer_provider,
    embedder.model(),
    unindexed_records,
    existing_embeddings,
  );

  // 2. use tokio JoinSet to parallelize OpenAI calls (IO-bound)
  let mut join_set = JoinSet::new();
  for record in records {
    let indexer_provider = indexer_provider.clone();
    let embedder = embedder.clone();
    if let Some(indexer) = indexer_provider.indexer_for(&record.collab_type) {
      join_set.spawn(async move {
        match indexer.embed(&embedder, record.contents).await {
          Ok(embeddings) => embeddings.map(|embeddings| EmbeddingRecord {
            workspace_id: record.workspace_id,
            object_id: record.object_id,
            collab_type: record.collab_type,
            tokens_used: embeddings.tokens_consumed,
            contents: embeddings.params,
          }),
          Err(err) => {
            error!("Failed to embed collab: {}", err);
            None
          },
        }
      });
    }
  }

  let mut results = Vec::with_capacity(join_set.len());
  while let Some(Ok(Some(record))) = join_set.join_next().await {
    trace!(
      "[Embedding] generate collab:{} embeddings, tokens used: {}",
      record.object_id,
      record.tokens_used
    );
    results.push(record);
  }
  results
}

fn compute_embedding_records(
  indexer_provider: &IndexerProvider,
  model: EmbeddingModel,
  unindexed_records: Vec<UnindexedCollab>,
  existing_embeddings: HashMap<String, Vec<String>>,
) -> Vec<EmbeddingRecord> {
  unindexed_records
    .into_par_iter()
    .flat_map(|unindexed| {
      let indexer = indexer_provider.indexer_for(&unindexed.collab_type)?;
      let collab = Collab::new_with_source(
        CollabOrigin::Empty,
        &unindexed.object_id,
        DataSource::DocStateV1(unindexed.collab.doc_state.into()),
        vec![],
        false,
      )
      .ok()?;

      let mut chunks = indexer
        .create_embedded_chunks_from_collab(&collab, model)
        .ok()?;
      if chunks.is_empty() {
        trace!("[Embedding] {} has no embeddings", unindexed.object_id,);
        return Some(EmbeddingRecord::empty(
          unindexed.workspace_id,
          unindexed.object_id,
          unindexed.collab_type,
        ));
      }

      // compare chunks against existing fragment ids (which are content addressed) and mark these
      // which haven't changed as already embedded
      if let Some(existing_embeddings) = existing_embeddings.get(&unindexed.object_id) {
        for chunk in chunks.iter_mut() {
          if existing_embeddings.contains(&chunk.fragment_id) {
            chunk.content = None; // mark as already embedded
            chunk.embedding = None;
          }
        }
      }
      Some(EmbeddingRecord {
        workspace_id: unindexed.workspace_id,
        object_id: unindexed.object_id,
        collab_type: unindexed.collab_type,
        tokens_used: 0,
        contents: chunks,
      })
    })
    .collect()
}
