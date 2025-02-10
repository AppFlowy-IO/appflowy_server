use crate::collab::partition_key_from_collab_type;
use chrono::{DateTime, Utc};
use collab_entity::CollabType;
use database_entity::dto::{AFCollabEmbeddedChunk, IndexingStatus, QueryCollab, QueryCollabParams};
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use pgvector::Vector;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgHasArrayType, PgTypeInfo};
use sqlx::{Error, Executor, Postgres, Transaction};
use std::collections::HashMap;
use std::ops::DerefMut;
use uuid::Uuid;

pub async fn get_index_status<'a, E>(
  tx: E,
  workspace_id: &Uuid,
  object_id: &str,
  partition_key: i32,
) -> Result<IndexingStatus, sqlx::Error>
where
  E: Executor<'a, Database = Postgres>,
{
  let result = sqlx::query!(
    r#"
SELECT
  w.settings['disable_search_indexing']::boolean as disable_search_indexing,
  CASE
    WHEN w.settings['disable_search_indexing']::boolean THEN
      FALSE
    ELSE
      EXISTS (SELECT 1 FROM af_collab_embeddings m WHERE m.partition_key = $3 AND m.oid = $2)
  END as has_index
FROM af_workspace w
WHERE w.workspace_id = $1"#,
    workspace_id,
    object_id,
    partition_key
  )
  .fetch_one(tx)
  .await;
  match result {
    Ok(row) => {
      if row.disable_search_indexing.unwrap_or(false) {
        Ok(IndexingStatus::Disabled)
      } else if row.has_index.unwrap_or(false) {
        Ok(IndexingStatus::Indexed)
      } else {
        Ok(IndexingStatus::NotIndexed)
      }
    },
    Err(Error::RowNotFound) => {
      tracing::warn!(
        "open-collab event for {}/{} arrived before its workspace was created",
        workspace_id,
        object_id
      );
      Ok(IndexingStatus::NotIndexed)
    },
    Err(e) => Err(e),
  }
}

#[derive(sqlx::Type)]
#[sqlx(type_name = "af_fragment_v3", no_pg_array)]
struct Fragment {
  fragment_id: String,
  content_type: i32,
  contents: Option<String>,
  embedding: Option<Vector>,
  metadata: serde_json::Value,
  fragment_index: i32,
  embedded_type: i16,
}

impl From<AFCollabEmbeddedChunk> for Fragment {
  fn from(value: AFCollabEmbeddedChunk) -> Self {
    Fragment {
      fragment_id: value.fragment_id,
      content_type: value.content_type as i32,
      contents: value.content,
      embedding: value.embedding.map(Vector::from),
      metadata: value.metadata,
      fragment_index: value.fragment_index,
      embedded_type: value.embedded_type,
    }
  }
}

impl PgHasArrayType for Fragment {
  fn array_type_info() -> PgTypeInfo {
    PgTypeInfo::with_name("af_fragment_v3[]")
  }
}

pub async fn upsert_collab_embeddings(
  transaction: &mut Transaction<'_, Postgres>,
  workspace_id: &Uuid,
  object_id: &str,
  collab_type: CollabType,
  tokens_used: u32,
  records: Vec<AFCollabEmbeddedChunk>,
) -> Result<(), sqlx::Error> {
  let fragments = records.into_iter().map(Fragment::from).collect::<Vec<_>>();
  tracing::trace!(
    "[Embedding] upsert {} {} fragments",
    object_id,
    fragments.len()
  );
  sqlx::query(r#"CALL af_collab_embeddings_upsert($1, $2, $3, $4, $5::af_fragment_v3[])"#)
    .bind(*workspace_id)
    .bind(object_id)
    .bind(crate::collab::partition_key_from_collab_type(&collab_type))
    .bind(tokens_used as i32)
    .bind(fragments)
    .execute(transaction.deref_mut())
    .await?;
  Ok(())
}

pub async fn get_collab_embedding_framgent_ids<'a, E>(
  tx: E,
  collab_ids: Vec<(String, CollabType)>,
) -> Result<HashMap<String, Vec<String>>, sqlx::Error>
where
  E: Executor<'a, Database = Postgres>,
{
  let (oids, partition_keys): (Vec<String>, Vec<i32>) = collab_ids
    .into_iter()
    .map(|(object_id, collab_type)| (object_id, partition_key_from_collab_type(&collab_type)))
    .unzip();
  let records = sqlx::query!(
    r#"
      SELECT oid, fragment_id
      FROM af_collab_embeddings
      WHERE (oid, partition_key) = ANY (
         SELECT UNNEST($1::text[]), UNNEST($2::int[])
      )
    "#,
    &oids,
    &partition_keys
  )
  .fetch_all(tx)
  .await?;
  let mut fragment_ids_by_oid = HashMap::new();
  for record in records {
    fragment_ids_by_oid
      .entry(record.oid)
      .or_insert_with(Vec::new)
      .push(record.fragment_id);
  }
  Ok(fragment_ids_by_oid)
}

pub async fn stream_collabs_without_embeddings(
  conn: &mut PoolConnection<Postgres>,
  workspace_id: Uuid,
  limit: i64,
) -> BoxStream<sqlx::Result<CollabId>> {
  sqlx::query!(
    r#"
        SELECT c.workspace_id, c.oid, c.partition_key
        FROM af_collab c
        JOIN af_workspace w ON c.workspace_id = w.workspace_id
        WHERE c.workspace_id = $1
        AND NOT COALESCE(w.settings['disable_search_indexing']::boolean, false)
        AND c.indexed_at IS NULL
        ORDER BY c.updated_at DESC
        LIMIT $2
    "#,
    workspace_id,
    limit
  )
  .fetch(conn.deref_mut())
  .map(|row| {
    row.map(|r| CollabId {
      collab_type: CollabType::from(r.partition_key),
      workspace_id: r.workspace_id,
      object_id: r.oid,
    })
  })
  .boxed()
}

pub async fn update_collab_indexed_at<'a, E>(
  tx: E,
  object_id: &str,
  collab_type: &CollabType,
  indexed_at: DateTime<Utc>,
) -> Result<(), Error>
where
  E: Executor<'a, Database = Postgres>,
{
  let partition_key = partition_key_from_collab_type(collab_type);
  sqlx::query!(
    r#"
      UPDATE af_collab
      SET indexed_at = $1
      WHERE oid = $2 AND partition_key = $3
    "#,
    indexed_at,
    object_id,
    partition_key
  )
  .execute(tx)
  .await?;

  Ok(())
}

pub async fn get_collabs_indexed_at<'a, E>(
  executor: E,
  collab_ids: &[(String, CollabType)],
) -> Result<HashMap<String, DateTime<Utc>>, Error>
where
  E: Executor<'a, Database = Postgres>,
{
  let (oids, partition_keys): (Vec<String>, Vec<i32>) = collab_ids
    .iter()
    .map(|(object_id, collab_type)| {
      (
        object_id.clone(),
        partition_key_from_collab_type(collab_type),
      )
    })
    .unzip();

  let result = sqlx::query!(
    r#"
        SELECT oid, indexed_at
        FROM af_collab
        WHERE (oid, partition_key) = ANY (
            SELECT UNNEST($1::text[]), UNNEST($2::int[])
        )
        "#,
    &oids,
    &partition_keys
  )
  .fetch_all(executor)
  .await?;

  let map = result
    .into_iter()
    .filter_map(|r| {
      if let Some(indexed_at) = r.indexed_at {
        Some((r.oid, indexed_at))
      } else {
        None
      }
    })
    .collect::<HashMap<String, DateTime<Utc>>>();
  Ok(map)
}

#[derive(Debug, Clone)]
pub struct CollabId {
  pub collab_type: CollabType,
  pub workspace_id: Uuid,
  pub object_id: String,
}

impl From<CollabId> for QueryCollabParams {
  fn from(value: CollabId) -> Self {
    QueryCollabParams {
      workspace_id: value.workspace_id.to_string(),
      inner: QueryCollab {
        object_id: value.object_id,
        collab_type: value.collab_type,
      },
    }
  }
}
