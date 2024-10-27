use crate::import_worker::report::{ImportNotifier, ImportProgress, ImportResult};
use crate::s3_client::{download_file, AutoRemoveDownloadedFile, S3StreamResponse};
use anyhow::anyhow;
use aws_sdk_s3::primitives::ByteStream;

use crate::error::ImportError;
use crate::mailer::ImportNotionMailerParam;
use crate::s3_client::S3Client;

use bytes::Bytes;
use collab::core::origin::CollabOrigin;
use collab::entity::EncodedCollab;
use collab_database::workspace_database::WorkspaceDatabase;
use collab_entity::CollabType;
use collab_folder::{Folder, View, ViewLayout};
use collab_importer::imported_collab::ImportType;
use collab_importer::notion::page::CollabResource;
use collab_importer::notion::NotionImporter;
use collab_importer::util::FileId;
use database::collab::mem_cache::{cache_exp_secs_from_collab_type, CollabMemCache};
use database::collab::{insert_into_af_collab_bulk_for_user, select_blob_from_af_collab};
use database::resource_usage::{insert_blob_metadata_bulk, BulkInsertMeta};
use database::workspace::{
  delete_from_workspace, select_import_task, select_workspace_database_storage_id,
  update_import_task_status, update_updated_at_of_workspace_with_uid, update_workspace_status,
};
use database_entity::dto::CollabParams;

use crate::metric::ImportMetrics;
use async_zip::base::read::stream::{Ready, ZipFileReader};
use collab_importer::zip_tool::async_zip::async_unzip;
use collab_importer::zip_tool::sync_zip::sync_unzip;

use futures::stream::FuturesUnordered;
use futures::{stream, AsyncBufRead, StreamExt};
use infra::env_util::get_env_var;
use redis::aio::ConnectionManager;
use redis::streams::{
  StreamClaimOptions, StreamClaimReply, StreamId, StreamPendingReply, StreamReadOptions,
  StreamReadReply,
};
use redis::{AsyncCommands, RedisResult, Value};

use database::pg_row::AFImportTask;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use sqlx::types::chrono;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::env::temp_dir;
use std::fmt::Display;
use std::fs::Permissions;
use std::ops::DerefMut;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::task::spawn_local;
use tokio::time::interval;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::{error, info, trace, warn};
use uuid::Uuid;

const GROUP_NAME: &str = "import_task_group";
const CONSUMER_NAME: &str = "appflowy_worker";
pub async fn run_import_worker(
  pg_pool: PgPool,
  mut redis_client: ConnectionManager,
  metrics: Option<Arc<ImportMetrics>>,
  s3_client: Arc<dyn S3Client>,
  notifier: Arc<dyn ImportNotifier>,
  stream_name: &str,
  tick_interval_secs: u64,
) -> Result<(), ImportError> {
  info!("Starting importer worker");
  if let Err(err) = ensure_consumer_group(stream_name, GROUP_NAME, &mut redis_client)
    .await
    .map_err(ImportError::Internal)
  {
    error!("Failed to ensure consumer group: {:?}", err);
  }

  let storage_dir = temp_dir();
  process_un_acked_tasks(
    &storage_dir,
    &mut redis_client,
    &s3_client,
    &pg_pool,
    stream_name,
    GROUP_NAME,
    CONSUMER_NAME,
    notifier.clone(),
    &metrics,
  )
  .await;

  process_upcoming_tasks(
    &storage_dir,
    &mut redis_client,
    &s3_client,
    pg_pool,
    stream_name,
    GROUP_NAME,
    CONSUMER_NAME,
    notifier.clone(),
    tick_interval_secs,
    &metrics,
  )
  .await?;

  Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_un_acked_tasks(
  storage_dir: &Path,
  redis_client: &mut ConnectionManager,
  s3_client: &Arc<dyn S3Client>,
  pg_pool: &PgPool,
  stream_name: &str,
  group_name: &str,
  consumer_name: &str,
  notifier: Arc<dyn ImportNotifier>,
  metrics: &Option<Arc<ImportMetrics>>,
) {
  // when server restarts, we need to check if there are any unacknowledged tasks
  match get_un_ack_tasks(stream_name, group_name, consumer_name, redis_client).await {
    Ok(un_ack_tasks) => {
      info!("Found {} unacknowledged tasks", un_ack_tasks.len());
      for un_ack_task in un_ack_tasks {
        let context = TaskContext {
          storage_dir: storage_dir.to_path_buf(),
          redis_client: redis_client.clone(),
          s3_client: s3_client.clone(),
          pg_pool: pg_pool.clone(),
          notifier: notifier.clone(),
          metrics: metrics.clone(),
        };
        // Ignore the error here since the consume task will handle the error
        let _ = consume_task(
          context,
          un_ack_task.task,
          stream_name,
          group_name,
          un_ack_task.stream_id.id,
        )
        .await;
      }
    },
    Err(err) => error!("Failed to get unacknowledged tasks: {:?}", err),
  }
}

#[allow(clippy::too_many_arguments)]
async fn process_upcoming_tasks(
  storage_dir: &Path,
  redis_client: &mut ConnectionManager,
  s3_client: &Arc<dyn S3Client>,
  pg_pool: PgPool,
  stream_name: &str,
  group_name: &str,
  consumer_name: &str,
  notifier: Arc<dyn ImportNotifier>,
  interval_secs: u64,
  metrics: &Option<Arc<ImportMetrics>>,
) -> Result<(), ImportError> {
  let options = StreamReadOptions::default()
    .group(group_name, consumer_name)
    .count(10);
  let mut interval = interval(Duration::from_secs(interval_secs));
  interval.tick().await;

  loop {
    interval.tick().await;
    let tasks: StreamReadReply = match redis_client
      .xread_options(&[stream_name], &[">"], &options)
      .await
    {
      Ok(tasks) => tasks,
      Err(err) => {
        error!("Failed to read tasks from Redis stream: {:?}", err);
        continue;
      },
    };

    let mut task_handlers = FuturesUnordered::new();
    for stream_key in tasks.keys {
      // For each stream key, iterate through the stream entries
      for stream_id in stream_key.ids {
        match ImportTask::try_from(&stream_id) {
          Ok(import_task) => {
            let stream_name = stream_name.to_string();
            let group_name = group_name.to_string();
            let context = TaskContext {
              storage_dir: storage_dir.to_path_buf(),
              redis_client: redis_client.clone(),
              s3_client: s3_client.clone(),
              pg_pool: pg_pool.clone(),
              notifier: notifier.clone(),
              metrics: metrics.clone(),
            };
            task_handlers.push(spawn_local(async move {
              consume_task(
                context,
                import_task,
                &stream_name,
                &group_name,
                stream_id.id,
              )
              .await?;
              Ok::<(), ImportError>(())
            }));
          },
          Err(err) => {
            error!("Failed to deserialize task: {:?}", err);
          },
        }
      }
    }

    while let Some(result) = task_handlers.next().await {
      match result {
        Ok(Ok(())) => {},
        Ok(Err(e)) => error!("Task failed: {:?}", e),
        Err(e) => error!("Runtime error: {:?}", e),
      }
    }
  }
}
#[derive(Clone)]
struct TaskContext {
  storage_dir: PathBuf,
  redis_client: ConnectionManager,
  s3_client: Arc<dyn S3Client>,
  pg_pool: PgPool,
  notifier: Arc<dyn ImportNotifier>,
  metrics: Option<Arc<ImportMetrics>>,
}

#[allow(clippy::too_many_arguments)]
async fn consume_task(
  mut context: TaskContext,
  import_task: ImportTask,
  stream_name: &str,
  group_name: &str,
  entry_id: String,
) -> Result<(), ImportError> {
  if let ImportTask::Notion(task) = &import_task {
    if let Some(created_at_timestamp) = task.created_at {
      if is_record_expired(created_at_timestamp) {
        if let Ok(import_record) = select_import_task(&context.pg_pool, &task.task_id).await {
          handle_expired_task(
            &mut context,
            &import_record,
            task,
            stream_name,
            group_name,
            &entry_id,
          )
          .await?;
        }

        return Ok(());
      } else if !check_blob_existence(&context.s3_client, &task.s3_key).await? {
        trace!("[Import] {} file not found, re-add task", task.workspace_id);
        re_add_task(
          &mut context.redis_client,
          stream_name,
          group_name,
          import_task,
          &entry_id,
        )
        .await?;
        return Ok(());
      }
    }
  }

  process_and_ack_task(context, import_task, stream_name, group_name, &entry_id).await
}

async fn handle_expired_task(
  context: &mut TaskContext,
  import_record: &AFImportTask,
  task: &NotionImportTask,
  stream_name: &str,
  group_name: &str,
  entry_id: &str,
) -> Result<(), ImportError> {
  info!(
    "[Import]: {} import is expired, delete workspace",
    task.workspace_id
  );
  if let Err(err) = update_import_task_status(&import_record.task_id, 3, &context.pg_pool).await {
    error!("Failed to update import task status: {:?}", err);
  }
  if let Err(err) = context.s3_client.delete_blob(task.s3_key.as_str()).await {
    error!(
      "[Import]: {} failed to delete zip file from S3: {:?}",
      task.workspace_id, err
    );
  }
  remove_workspace(&import_record.workspace_id, &context.pg_pool).await;
  let _ = xack_task(&mut context.redis_client, stream_name, group_name, entry_id).await;
  notify_user(
    task,
    Err(ImportError::UploadFileExpire),
    context.notifier.clone(),
    &context.metrics,
  )
  .await?;
  Ok(())
}

async fn check_blob_existence(
  s3_client: &Arc<dyn S3Client>,
  s3_key: &str,
) -> Result<bool, ImportError> {
  s3_client.is_blob_exist(s3_key).await.map_err(|e| {
    error!("Failed to check blob existence: {:?}", e);
    ImportError::Internal(e.into())
  })
}

async fn process_and_ack_task(
  mut context: TaskContext,
  import_task: ImportTask,
  stream_name: &str,
  group_name: &str,
  entry_id: &str,
) -> Result<(), ImportError> {
  let result = process_task(context.clone(), import_task).await;
  xack_task(&mut context.redis_client, stream_name, group_name, entry_id)
    .await
    .ok();
  result
}

fn is_record_expired(timestamp: i64) -> bool {
  match DateTime::<Utc>::from_timestamp(timestamp, 0) {
    None => {
      info!("[Import] failed to parse timestamp: {}", timestamp);
      true
    },
    Some(created_at) => {
      let now = Utc::now();
      if created_at > now {
        error!(
          "[Import] created_at is in the future: {} > {}",
          created_at, now
        );
        return false;
      }

      let elapsed = now - created_at;
      let minutes = get_env_var("APPFLOWY_WORKER_IMPORT_TASK_EXPIRE_MINUTES", "30")
        .parse::<i64>()
        .unwrap_or(30);
      elapsed.num_minutes() >= minutes
    },
  }
}

async fn re_add_task(
  redis_client: &mut ConnectionManager,
  stream_name: &str,
  group_name: &str,
  task: ImportTask,
  entry_id: &str,
) -> Result<(), ImportError> {
  let task_str = serde_json::to_string(&task).map_err(|e| {
    error!("Failed to serialize task: {:?}", e);
    ImportError::Internal(e.into())
  })?;

  let mut pipeline = redis::pipe();
  pipeline
      .atomic() // Ensures the commands are executed atomically
      .cmd("XACK") // Acknowledge the task
      .arg(stream_name)
      .arg(group_name)
      .arg(entry_id)
      .ignore() // Ignore the result of XACK
      .cmd("XADD") // Re-add the task to the stream
      .arg(stream_name)
      .arg("*")
      .arg("task")
      .arg(task_str);

  let result: Result<(), redis::RedisError> = pipeline.query_async(redis_client).await;
  match result {
    Ok(_) => Ok(()),
    Err(err) => {
      error!(
        "Failed to execute transaction for re-adding task: {:?}",
        err
      );
      Err(ImportError::Internal(err.into()))
    },
  }
}

async fn xack_task(
  redis_client: &mut ConnectionManager,
  stream_name: &str,
  group_name: &str,
  entry_id: &str,
) -> Result<(), ImportError> {
  redis_client
    .xack(stream_name, group_name, &[entry_id])
    .await
    .map_err(|e| {
      error!("Failed to acknowledge task: {:?}", e);
      ImportError::Internal(e.into())
    })?;
  Ok(())
}

async fn process_task(
  mut context: TaskContext,
  import_task: ImportTask,
) -> Result<(), ImportError> {
  let retry_interval: u64 = get_env_var("APPFLOWY_WORKER_IMPORT_TASK_RETRY_INTERVAL", "10")
    .parse()
    .unwrap_or(10);

  let streaming = get_env_var("APPFLOWY_WORKER_IMPORT_TASK_STREAMING", "false")
    .parse()
    .unwrap_or(false);

  info!(
    "[Import]: Processing task: {}, retry interval: {}, streaming: {}",
    import_task, retry_interval, streaming
  );

  match import_task {
    ImportTask::Notion(task) => {
      // 1. download zip file
      let unzip_result = download_and_unzip_file_retry(
        &context.storage_dir,
        &task,
        &context.s3_client,
        3,
        Duration::from_secs(retry_interval),
        streaming,
        &context.metrics,
      )
      .await;

      trace!(
        "[Import]: {} download and unzip file result: {:?}",
        task.workspace_id,
        unzip_result
      );
      match unzip_result {
        Ok(unzip_dir_path) => {
          // 2. process unzip file
          let result = process_unzip_file(
            &task,
            &unzip_dir_path,
            &context.pg_pool,
            &mut context.redis_client,
            &context.s3_client,
          )
          .await;

          // If there is any errors when processing the unzip file, we will remove the workspace and notify the user.
          if result.is_err() {
            info!(
              "[Import]: failed to import notion file, delete workspace:{}",
              task.workspace_id
            );
            remove_workspace(&task.workspace_id, &context.pg_pool).await;
          }

          clean_up(&context.s3_client, &task).await;
          notify_user(&task, result, context.notifier, &context.metrics).await?;
        },
        Err(err) => {
          // If there is any errors when download or unzip the file, we will remove the file from S3 and notify the user.
          if let Err(err) = &context.s3_client.delete_blob(task.s3_key.as_str()).await {
            error!("Failed to delete zip file from S3: {:?}", err);
          }
          remove_workspace(&task.workspace_id, &context.pg_pool).await;
          clean_up(&context.s3_client, &task).await;
          notify_user(&task, Err(err), context.notifier, &context.metrics).await?;
        },
      }

      Ok(())
    },
    ImportTask::Custom(value) => {
      trace!("Custom task: {:?}", value);
      let result = ImportResult {
        user_name: "".to_string(),
        user_email: "".to_string(),
        is_success: true,
        value: Default::default(),
      };
      context
        .notifier
        .notify_progress(ImportProgress::Finished(result))
        .await;
      Ok(())
    },
  }
}
/// Retries the download and unzipping of a file from an S3 source.
///
/// This function attempts to download a zip file from an S3 bucket and unzip it to a local directory.
/// If the operation fails, it will retry up to `max_retries` times, waiting for `interval` between each attempt.
///
pub async fn download_and_unzip_file_retry(
  storage_dir: &Path,
  import_task: &NotionImportTask,
  s3_client: &Arc<dyn S3Client>,
  max_retries: usize,
  interval: Duration,
  streaming: bool,
  metrics: &Option<Arc<ImportMetrics>>,
) -> Result<PathBuf, ImportError> {
  let mut attempt = 0;
  loop {
    attempt += 1;
    match download_and_unzip_file(storage_dir, import_task, s3_client, streaming, metrics).await {
      Ok(result) => return Ok(result),
      Err(err) => {
        // If the Upload file not found error occurs, we will not retry.
        if matches!(err, ImportError::UploadFileNotFound) {
          return Err(err);
        }

        if attempt < max_retries && !err.is_file_not_found() {
          warn!(
            "{} attempt {} failed: {}. Retrying in {:?}...",
            import_task.workspace_id, attempt, err, interval
          );
          tokio::time::sleep(interval).await;
        } else {
          return Err(ImportError::Internal(anyhow!(
            "Failed after {} attempts: {}",
            attempt,
            err
          )));
        }
      },
    }
  }
}
/// Downloads a zip file from S3 and unzips it to the local directory.
///
/// This function fetches a zip file from an S3 source using the provided S3 client,
/// downloads it (if needed), and unzips the contents to the specified local directory.
///
async fn download_and_unzip_file(
  storage_dir: &Path,
  import_task: &NotionImportTask,
  s3_client: &Arc<dyn S3Client>,
  streaming: bool,
  metrics: &Option<Arc<ImportMetrics>>,
) -> Result<PathBuf, ImportError> {
  let S3StreamResponse {
    stream,
    content_type: _,
    content_length,
  } = s3_client
    .get_blob_stream(import_task.s3_key.as_str())
    .await?;

  let buffer_size = buffer_size_from_content_length(content_length);
  if let Some(metrics) = metrics {
    metrics.record_import_size_bytes(buffer_size);
  }
  if streaming {
    let zip_reader = get_zip_reader(buffer_size, StreamOrFile::Stream(stream)).await?;
    let unique_file_name = Uuid::new_v4().to_string();
    let output_file_path = storage_dir.join(unique_file_name);
    fs::create_dir_all(&output_file_path)
      .await
      .map_err(|err| ImportError::Internal(err.into()))?;
    fs::set_permissions(&output_file_path, Permissions::from_mode(0o777))
      .await
      .map_err(|err| {
        ImportError::Internal(anyhow!("Failed to set permissions for temp dir: {:?}", err))
      })?;
    let unzip_file = async_unzip(
      zip_reader.inner,
      output_file_path,
      Some(import_task.workspace_name.clone()),
    )
    .await?;
    Ok(unzip_file.unzip_dir_path)
  } else {
    let file = download_file(
      &import_task.workspace_id,
      storage_dir,
      stream,
      &import_task.md5_base64,
    )
    .await?;
    trace!(
      "[Import] {} start unzip file: {:?}",
      import_task.workspace_id,
      file.path_buf()
    );

    let file_path = file.path_buf().clone();
    let storage_dir = storage_dir.to_path_buf();
    let workspace_name = import_task.workspace_name.clone();
    let unzip_file =
      tokio::task::spawn_blocking(move || sync_unzip(file_path, storage_dir, Some(workspace_name)))
        .await
        .map_err(|err| ImportError::Internal(err.into()))??;

    trace!(
      "[Import] {} finish unzip file: {:?}",
      import_task.workspace_id,
      unzip_file.unzip_dir
    );
    Ok(unzip_file.unzip_dir)
  }
}

struct ZipReader {
  inner: ZipFileReader<Ready<Pin<Box<dyn AsyncBufRead + Unpin + Send>>>>,
  #[allow(dead_code)]
  file: Option<AutoRemoveDownloadedFile>,
}

#[allow(dead_code)]
enum StreamOrFile {
  Stream(Box<dyn AsyncBufRead + Unpin + Send>),
  File(AutoRemoveDownloadedFile),
}

/// Asynchronously returns a `ZipFileReader` that can read from a stream or a downloaded file, based on the environment setting.
///
/// This function checks whether streaming is enabled via the `APPFLOWY_WORKER_IMPORT_TASK_STREAMING` environment variable.
/// If streaming is enabled, it reads the zip file directly from the provided stream.
/// Otherwise, it first downloads the zip file to a local file and then reads from it.
///
async fn get_zip_reader(
  buffer_size: usize,
  stream_or_file: StreamOrFile,
) -> Result<ZipReader, ImportError> {
  match stream_or_file {
    StreamOrFile::Stream(stream) => {
      // Occasionally, we encounter the error 'unable to locate the end of central directory record'
      // when streaming a ZIP file to async-zip. This indicates that the ZIP reader couldn't find
      // the necessary end-of-file marker. The issue might occur if the entire ZIP file has not been
      // fully downloaded or buffered before the reader attempts to process the end-of-file information.
      let reader = futures::io::BufReader::with_capacity(buffer_size, stream);
      let boxed_reader: Pin<Box<dyn AsyncBufRead + Unpin + Send>> = Box::pin(reader);
      Ok(ZipReader {
        inner: async_zip::base::read::stream::ZipFileReader::new(boxed_reader),
        file: None,
      })
    },
    StreamOrFile::File(file) => {
      let handle = fs::File::open(&file)
        .await
        .map_err(|err| ImportError::Internal(err.into()))?;
      let reader = tokio::io::BufReader::with_capacity(buffer_size, handle).compat();
      let boxed_reader: Pin<Box<dyn AsyncBufRead + Unpin + Send>> = Box::pin(reader);
      Ok(ZipReader {
        inner: async_zip::base::read::stream::ZipFileReader::new(boxed_reader),
        // Make sure the lifetime of file is the same as zip reader.
        file: Some(file),
      })
    },
  }
}

/// Determines the buffer size based on the content length of the file.
/// If the buffer is too small, the zip reader will frequently pause to fetch more data,
/// causing delays. This can make the unzip process appear slower and can even cause premature
/// errors (like EOF) if there is a delay in fetching more data.
#[inline]
fn buffer_size_from_content_length(content_length: Option<i64>) -> usize {
  match content_length {
    Some(file_size) => {
      if file_size < 10 * 1024 * 1024 {
        3 * 1024 * 1024
      } else if file_size < 100 * 1024 * 1024 {
        5 * 1024 * 1024 // 5MB buffer
      } else {
        10 * 1024 * 1024 // 10MB buffer
      }
    },
    None => 3 * 1024 * 1024,
  }
}

async fn process_unzip_file(
  import_task: &NotionImportTask,
  unzip_dir_path: &PathBuf,
  pg_pool: &PgPool,
  redis_client: &mut ConnectionManager,
  s3_client: &Arc<dyn S3Client>,
) -> Result<(), ImportError> {
  let workspace_id =
    Uuid::parse_str(&import_task.workspace_id).map_err(|err| ImportError::Internal(err.into()))?;
  let notion_importer = NotionImporter::new(
    import_task.uid,
    unzip_dir_path,
    import_task.workspace_id.clone(),
    import_task.host.clone(),
  )
  .map_err(ImportError::ImportCollabError)?;

  trace!(
    "[Import]: {} start import notion data",
    import_task.workspace_id
  );
  let imported = notion_importer
    .import()
    .await
    .map_err(ImportError::ImportCollabError)?;
  let nested_views = imported.build_nested_views().await;
  trace!(
    "[Import]: {} imported nested views:{}",
    import_task.workspace_id,
    nested_views
  );

  // 1. Open the workspace folder
  let folder_collab =
    get_encode_collab_from_bytes(&imported.workspace_id, &CollabType::Folder, pg_pool).await?;
  let mut folder = Folder::from_collab_doc_state(
    import_task.uid,
    CollabOrigin::Server,
    folder_collab.into(),
    &imported.workspace_id,
    vec![],
  )
  .map_err(|err| ImportError::CannotOpenWorkspace(err.to_string()))?;

  // 2. Insert collabs' views into the folder
  trace!(
    "[Import]: {} insert views:{} to folder",
    import_task.workspace_id,
    nested_views.len()
  );
  folder.insert_nested_views(nested_views.into_inner());

  let mut resources = vec![];
  let mut collab_params_list = vec![];
  let mut database_view_ids_by_database_id: HashMap<String, Vec<String>> = HashMap::new();
  let mut orphan_view_ids = HashSet::new();
  let mem_cache = CollabMemCache::new(redis_client.clone());
  let timestamp = chrono::Utc::now().timestamp();

  // 3. Collect all collabs and resources
  let mut stream = imported.into_collab_stream().await;
  while let Some(imported_collab_info) = stream.next().await {
    trace!(
      "[Import]: {} imported collab: {}",
      import_task.workspace_id,
      imported_collab_info
    );
    resources.extend(imported_collab_info.resources);
    collab_params_list.extend(
      imported_collab_info
        .imported_collabs
        .into_iter()
        .map(|imported_collab| CollabParams {
          object_id: imported_collab.object_id,
          collab_type: imported_collab.collab_type,
          embeddings: None,
          encoded_collab_v1: Bytes::from(imported_collab.encoded_collab.encode_to_bytes().unwrap()),
        })
        .collect::<Vec<_>>(),
    );

    match imported_collab_info.import_type {
      ImportType::Database {
        database_id,
        view_ids,
        row_document_ids,
      } => {
        database_view_ids_by_database_id.insert(database_id, view_ids);
        orphan_view_ids.extend(row_document_ids);
      },
      ImportType::Document => {
        // do nothing
      },
    }
  }

  let w_database_id = select_workspace_database_storage_id(pg_pool, &import_task.workspace_id)
    .await
    .map_err(|err| {
      ImportError::Internal(anyhow!(
        "Failed to select workspace database storage id: {:?}",
        err
      ))
    })
    .map(|id| id.to_string())?;

  // 4. Edit workspace database collab and then encode workspace database collab
  if !database_view_ids_by_database_id.is_empty() {
    let w_db_collab =
      get_encode_collab_from_bytes(&w_database_id, &CollabType::WorkspaceDatabase, pg_pool).await?;
    let mut w_database = WorkspaceDatabase::from_collab_doc_state(
      &w_database_id,
      CollabOrigin::Server,
      w_db_collab.into(),
    )
    .map_err(|err| ImportError::CannotOpenWorkspace(err.to_string()))?;
    w_database.batch_add_database(database_view_ids_by_database_id);

    let w_database_collab = w_database.encode_collab_v1().map_err(|err| {
      ImportError::Internal(anyhow!(
        "Failed to encode workspace database collab: {:?}",
        err
      ))
    })?;
    // Update the workspace database cache because newly created workspace databases are cached in Redis.
    mem_cache
      .insert_encode_collab(
        &w_database_id,
        w_database_collab.clone(),
        timestamp,
        cache_exp_secs_from_collab_type(&CollabType::WorkspaceDatabase),
      )
      .await;

    trace!(
      "[Import]: {} did encode workspace database collab",
      import_task.workspace_id
    );
    let w_database_collab_params = CollabParams {
      object_id: w_database_id.clone(),
      collab_type: CollabType::WorkspaceDatabase,
      embeddings: None,
      encoded_collab_v1: Bytes::from(w_database_collab.encode_to_bytes().unwrap()),
    };
    collab_params_list.push(w_database_collab_params);
  }

  // 5. Insert orphan view to folder
  let orphan_views = orphan_view_ids
    .into_iter()
    .map(|orphan_view_id| {
      View::orphan_view(&orphan_view_id, ViewLayout::Document, Some(import_task.uid))
    })
    .collect::<Vec<_>>();
  if !orphan_views.is_empty() {
    folder.insert_views(orphan_views);
  }

  // 6. Encode Folder
  let folder_collab = folder
    .encode_collab_v1(|collab| CollabType::Folder.validate_require_data(collab))
    .map_err(|err| ImportError::Internal(err.into()))?;

  // Update the folder cache because newly created folders are cached in Redis.
  // Other collaboration objects do not use caching yet, so there is no need to insert them into Redis.
  mem_cache
    .insert_encode_collab(
      &import_task.workspace_id,
      folder_collab.clone(),
      timestamp,
      cache_exp_secs_from_collab_type(&CollabType::Folder),
    )
    .await;

  let folder_collab_params = CollabParams {
    object_id: import_task.workspace_id.clone(),
    collab_type: CollabType::Folder,
    embeddings: None,
    encoded_collab_v1: Bytes::from(folder_collab.encode_to_bytes().unwrap()),
  };
  trace!(
    "[Import]: {} did encode folder collab",
    import_task.workspace_id
  );
  collab_params_list.push(folder_collab_params);

  let upload_resources = process_resources(resources).await;

  // 7. Start a transaction to insert all collabs
  let mut transaction = pg_pool.begin().await.map_err(|err| {
    ImportError::Internal(anyhow!(
      "Failed to start transaction when importing data: {:?}",
      err
    ))
  })?;

  trace!(
    "[Import]: {} insert collabs into database",
    import_task.workspace_id
  );

  // 8. write all collab to disk
  insert_into_af_collab_bulk_for_user(
    &mut transaction,
    &import_task.uid,
    &import_task.workspace_id,
    &collab_params_list,
  )
  .await
  .map_err(|err| {
    ImportError::Internal(anyhow!(
      "Failed to insert collabs into database when importing data: {:?}",
      err
    ))
  })?;

  trace!(
    "[Import]: {} update task:{} status to completed",
    import_task.workspace_id,
    import_task.task_id,
  );
  update_import_task_status(&import_task.task_id, 1, transaction.deref_mut())
    .await
    .map_err(|err| {
      ImportError::Internal(anyhow!(
        "Failed to update import task status when importing data: {:?}",
        err
      ))
    })?;

  trace!(
    "[Import]: {} set is_initialized to true",
    import_task.workspace_id,
  );
  update_workspace_status(transaction.deref_mut(), &workspace_id, true)
    .await
    .map_err(|err| {
      ImportError::Internal(anyhow!(
        "Failed to update workspace status when importing data: {:?}",
        err
      ))
    })?;

  // Set the workspace's updated_at to the earliest possible timestamp, as it is created by an import task
  // and not actively updated by a user. This ensures that when sorting workspaces by updated_at to find
  // the most recent, the imported workspace doesn't appear as the most recently visited workspace.
  let updated_at = DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now);
  update_updated_at_of_workspace_with_uid(
    transaction.deref_mut(),
    import_task.uid,
    &workspace_id,
    updated_at,
  )
  .await
  .map_err(|err| {
    ImportError::Internal(anyhow!(
      "Failed to update workspace updated_at when importing data: {:?}",
      err
    ))
  })?;

  // insert metadata into database
  let metas = upload_resources
    .iter()
    .map(|res| res.meta.clone())
    .collect::<Vec<_>>();
  let affected_rows = insert_blob_metadata_bulk(transaction.deref_mut(), &workspace_id, metas)
    .await
    .map_err(|err| {
      ImportError::Internal(anyhow!(
        "Failed to insert blob metadata into database when importing data: {:?}",
        err
      ))
    })?;

  if affected_rows != upload_resources.len() as u64 {
    warn!(
      "[Import]: {}, Affected rows: {}, upload resources: {}",
      import_task.workspace_id,
      affected_rows,
      upload_resources.len()
    );
  }

  let result = transaction.commit().await.map_err(|err| {
    ImportError::Internal(anyhow!(
      "Failed to commit transaction when importing data: {:?}",
      err
    ))
  });

  if result.is_err() {
    let _ = mem_cache.remove_encode_collab(&w_database_id).await;
    let _ = mem_cache
      .remove_encode_collab(&import_task.workspace_id)
      .await;

    return result;
  }

  // 9. after inserting all collabs, upload all files to S3
  trace!("[Import]: {} upload files to s3", import_task.workspace_id,);
  batch_upload_files_to_s3(&import_task.workspace_id, s3_client, upload_resources)
    .await
    .map_err(|err| ImportError::Internal(anyhow!("Failed to upload files to S3: {:?}", err)))?;

  // 10. delete zip file regardless of success or failure
  match fs::remove_dir_all(unzip_dir_path).await {
    Ok(_) => trace!(
      "[Import]: {} deleted unzip file: {:?}",
      import_task.workspace_id,
      unzip_dir_path
    ),
    Err(err) => error!("Failed to delete unzip file: {:?}", err),
  }

  Ok(())
}

async fn clean_up(s3_client: &Arc<dyn S3Client>, task: &NotionImportTask) {
  if let Err(err) = s3_client.delete_blob(task.s3_key.as_str()).await {
    error!("Failed to delete zip file from S3: {:?}", err);
  }
}

async fn remove_workspace(workspace_id: &str, pg_pool: &PgPool) {
  if let Ok(workspace_id) = Uuid::from_str(workspace_id) {
    if let Err(err) = delete_from_workspace(pg_pool, &workspace_id).await {
      error!(
        "Failed to delete workspace: {:?} when fail to import notion file",
        err
      );
    }
  }
}

async fn notify_user(
  import_task: &NotionImportTask,
  result: Result<(), ImportError>,
  notifier: Arc<dyn ImportNotifier>,
  metrics: &Option<Arc<ImportMetrics>>,
) -> Result<(), ImportError> {
  let task_id = import_task.task_id.to_string();
  let (error, error_detail) = match result {
    Ok(_) => {
      info!("[Import]: successfully imported:{}", import_task);
      if let Some(metrics) = metrics {
        metrics.incr_import_success_count(1);
      }
      (None, None)
    },
    Err(err) => {
      error!(
        "[Import]: failed to import:{}: error:{:?}",
        import_task, err
      );
      if let Some(metrics) = metrics {
        metrics.incr_import_fail_count(1);
      }
      let (error, error_detail) = err.report(&task_id);
      (Some(error), Some(error_detail))
    },
  };

  let is_success = error.is_none();

  let value = serde_json::to_value(ImportNotionMailerParam {
    import_task_id: task_id,
    user_name: import_task.user_name.clone(),
    import_file_name: import_task.workspace_name.clone(),
    workspace_id: import_task.workspace_id.clone(),
    workspace_name: import_task.workspace_name.clone(),
    open_workspace: false,
    error,
    error_detail,
  })
  .unwrap();

  notifier
    .notify_progress(ImportProgress::Finished(ImportResult {
      user_name: import_task.user_name.clone(),
      user_email: import_task.user_email.clone(),
      is_success,
      value,
    }))
    .await;
  Ok(())
}

async fn batch_upload_files_to_s3(
  workspace_id: &str,
  client: &Arc<dyn S3Client>,
  resources: Vec<UploadCollabResource>,
) -> Result<(), anyhow::Error> {
  // Create a stream of upload tasks
  let upload_stream = stream::iter(resources.into_iter().map(|res| async move {
    match upload_file_to_s3(
      client,
      workspace_id,
      &res.object_id,
      &res.meta.file_id,
      &res.meta.file_type,
      &res.file_path,
    )
    .await
    {
      Ok(_) => {
        trace!("Successfully uploaded: {}", res);
        Ok(())
      },
      Err(e) => {
        error!("Failed to upload {}: {:?}", res, e);
        Err(e)
      },
    }
  }))
  .buffer_unordered(5);
  let results: Vec<_> = upload_stream.collect().await;
  let errors: Vec<_> = results.into_iter().filter_map(Result::err).collect();
  if errors.is_empty() {
    Ok(())
  } else {
    Err(anyhow!("Some uploads failed: {:?}", errors))
  }
}

async fn upload_file_to_s3(
  client: &Arc<dyn S3Client>,
  workspace_id: &str,
  object_id: &str,
  file_id: &str,
  file_type: &str,
  file_path: &str,
) -> Result<(), anyhow::Error> {
  let path = Path::new(file_path);
  if !path.exists() {
    return Err(anyhow!("File does not exist: {:?}", path));
  }

  let object_key = format!("{}/{}/{}", workspace_id, object_id, file_id);
  let byte_stream = ByteStream::from_path(path).await?;
  client
    .put_blob(&object_key, byte_stream, Some(file_type))
    .await?;
  Ok(())
}

async fn get_encode_collab_from_bytes(
  object_id: &str,
  collab_type: &CollabType,
  pg_pool: &PgPool,
) -> Result<EncodedCollab, ImportError> {
  let bytes = select_blob_from_af_collab(pg_pool, collab_type, object_id)
    .await
    .map_err(|err| ImportError::Internal(err.into()))?;
  tokio::task::spawn_blocking(move || match EncodedCollab::decode_from_bytes(&bytes) {
    Ok(encoded_collab) => Ok(encoded_collab),
    Err(err) => Err(ImportError::Internal(anyhow!(
      "Failed to decode collab from bytes: {:?}",
      err
    ))),
  })
  .await
  .map_err(|err| ImportError::Internal(err.into()))?
}

/// Ensure the consumer group exists, if not, create it.
async fn ensure_consumer_group(
  stream_key: &str,
  group_name: &str,
  redis_client: &mut ConnectionManager,
) -> Result<(), anyhow::Error> {
  let result: RedisResult<()> = redis_client
    .xgroup_create_mkstream(stream_key, group_name, "0")
    .await;

  if let Err(redis_error) = result {
    if let Some(code) = redis_error.code() {
      if code == "BUSYGROUP" {
        return Ok(()); // Group already exists, considered as success.
      }
    }
    error!("Error when creating consumer group: {:?}", redis_error);
    return Err(redis_error.into());
  }

  Ok(())
}

struct UnAckTask {
  stream_id: StreamId,
  task: ImportTask,
}

async fn get_un_ack_tasks(
  stream_key: &str,
  group_name: &str,
  consumer_name: &str,
  redis_client: &mut ConnectionManager,
) -> Result<Vec<UnAckTask>, anyhow::Error> {
  let reply: StreamPendingReply = redis_client.xpending(stream_key, group_name).await?;
  match reply {
    StreamPendingReply::Empty => Ok(vec![]),
    StreamPendingReply::Data(pending) => {
      let opts = StreamClaimOptions::default()
        .idle(500)
        .with_force()
        .retry(2);

      // If the start_id and end_id are the same, we only need to claim one message.
      let mut ids = Vec::with_capacity(2);
      ids.push(pending.start_id.clone());
      if pending.start_id != pending.end_id {
        ids.push(pending.end_id);
      }

      let result: StreamClaimReply = redis_client
        .xclaim_options(stream_key, group_name, consumer_name, 500, &ids, opts)
        .await?;

      let tasks = result
        .ids
        .into_iter()
        .filter_map(|stream_id| {
          ImportTask::try_from(&stream_id)
            .map(|task| UnAckTask { stream_id, task })
            .ok()
        })
        .collect::<Vec<_>>();

      trace!("Claimed tasks: {}", tasks.len());
      Ok(tasks)
    },
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotionImportTask {
  pub uid: i64,
  pub user_name: String,
  pub user_email: String,
  pub task_id: Uuid,
  pub workspace_id: String,
  pub workspace_name: String,
  pub s3_key: String,
  pub host: String,
  #[serde(default)]
  pub created_at: Option<i64>,
  #[serde(default)]
  pub md5_base64: Option<String>,
}

impl Display for NotionImportTask {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "NotionImportTask {{ task_id: {}, workspace_id: {}, workspace_name: {}, user_name: {}, user_email: {} }}",
      self.task_id, self.workspace_id, self.workspace_name, self.user_name, self.user_email
    )
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ImportTask {
  // boxing the large fields to reduce the total size of the enum
  Notion(Box<NotionImportTask>),
  Custom(serde_json::Value),
}

impl Display for ImportTask {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      ImportTask::Notion(task) => write!(
        f,
        "NotionImportTask {{ workspace_id: {}, workspace_name: {} }}",
        task.workspace_id, task.workspace_name
      ),
      ImportTask::Custom(value) => write!(f, "CustomTask {{ {} }}", value),
    }
  }
}

impl TryFrom<&StreamId> for ImportTask {
  type Error = ImportError;

  fn try_from(stream_id: &StreamId) -> Result<Self, Self::Error> {
    let task_str = match stream_id.map.get("task") {
      Some(value) => match value {
        Value::Data(data) => String::from_utf8_lossy(data).to_string(),
        _ => {
          error!("Unexpected value type for task field: {:?}", value);
          return Err(ImportError::Internal(anyhow!(
            "Unexpected value type for task field: {:?}",
            value
          )));
        },
      },
      None => {
        error!("Task field not found in Redis stream entry");
        return Err(ImportError::Internal(anyhow!(
          "Task field not found in Redis stream entry"
        )));
      },
    };

    from_str::<ImportTask>(&task_str).map_err(|err| ImportError::Internal(err.into()))
  }
}

async fn process_resources(resources: Vec<CollabResource>) -> Vec<UploadCollabResource> {
  let upload_resources_stream = stream::iter(resources)
    .flat_map(|resource| {
      let object_id = resource.object_id.clone();
      stream::iter(resource.files.into_iter().map(move |file_path| {
        let object_id = object_id.clone();
        let path = PathBuf::from(file_path.clone());
        async move {
          match insert_meta_from_path(&object_id, &path).await {
            Ok(meta) => Some(UploadCollabResource {
              object_id,
              file_path,
              meta,
            }),
            Err(_) => None,
          }
        }
      }))
    })
    // buffer_unordered method limits how many futures (tasks) are run concurrently.
    .buffer_unordered(20);

  upload_resources_stream
    .filter_map(|result| async { result })
    .collect::<Vec<UploadCollabResource>>()
    .await
}

struct UploadCollabResource {
  object_id: String,
  file_path: String,
  meta: BulkInsertMeta,
}

impl Display for UploadCollabResource {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "UploadCollabResource {{ object_id: {}, file_path: {}, file_size: {} }}",
      self.object_id, self.file_path, self.meta.file_size
    )
  }
}

async fn insert_meta_from_path(
  object_id: &str,
  path: &PathBuf,
) -> Result<BulkInsertMeta, ImportError> {
  let file_id = FileId::from_path(path).await?;
  let object_id = object_id.to_string();
  let file_type = mime_guess::from_path(path)
    .first_or_octet_stream()
    .to_string();
  let file_size = fs::metadata(path)
    .await
    .map_err(|err| ImportError::Internal(err.into()))?
    .len() as i64;

  Ok(BulkInsertMeta {
    object_id,
    file_id,
    file_type,
    file_size,
  })
}
