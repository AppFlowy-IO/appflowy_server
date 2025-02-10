use std::collections::HashMap;
use std::sync::Arc;

use actix_web::web::Data;
use app_error::AppError;
use appflowy_collaborate::collab::storage::CollabAccessControlStorage;
use chrono::DateTime;
use chrono::Utc;
use collab::preclude::Collab;
use collab_database::database::gen_field_id;
use collab_database::database::gen_row_id;
use collab_database::entity::FieldType;
use collab_database::fields::Field;
use collab_database::fields::TypeOptions;
use collab_database::rows::meta_id_from_row_id;
use collab_database::rows::CreateRowParams;
use collab_database::rows::DatabaseRowBody;
use collab_database::rows::Row;
use collab_database::rows::RowDetail;
use collab_database::rows::RowId;
use collab_database::rows::RowMetaKey;
use collab_database::views::OrderObjectPosition;
use collab_database::workspace_database::WorkspaceDatabase;
use collab_database::workspace_database::WorkspaceDatabaseBody;
use collab_document::document::Document;
use collab_entity::CollabType;
use collab_entity::EncodedCollab;
use collab_folder::hierarchy_builder::NestedChildViewBuilder;
use collab_folder::Folder;
use collab_folder::SectionItem;
use collab_folder::{CollabOrigin, SpaceInfo};
use collab_rt_entity::user::RealtimeUser;
use database::collab::select_last_updated_database_row_ids;
use database::collab::select_workspace_database_oid;
use database::collab::{CollabStorage, GetCollabOrigin};
use database::publish::select_published_view_ids_for_workspace;
use database::publish::select_published_view_ids_with_publish_info_for_workspace;
use database::publish::select_workspace_id_for_publish_namespace;
use database_entity::dto::QueryCollab;
use database_entity::dto::QueryCollabResult;
use database_entity::dto::{CollabParams, WorkspaceCollabIdentify};
use shared_entity::dto::workspace_dto::AFDatabase;
use shared_entity::dto::workspace_dto::AFDatabaseField;
use shared_entity::dto::workspace_dto::AFDatabaseRow;
use shared_entity::dto::workspace_dto::AFDatabaseRowDetail;
use shared_entity::dto::workspace_dto::AFInsertDatabaseField;
use shared_entity::dto::workspace_dto::DatabaseRowUpdatedItem;
use shared_entity::dto::workspace_dto::FavoriteFolderView;
use shared_entity::dto::workspace_dto::FolderViewMinimal;
use shared_entity::dto::workspace_dto::PublishedViewInfo;
use shared_entity::dto::workspace_dto::RecentFolderView;
use shared_entity::dto::workspace_dto::TrashFolderView;
use sqlx::PgPool;
use std::ops::DerefMut;
use yrs::Map;

use crate::api::metrics::AppFlowyWebMetrics;
use crate::api::ws::RealtimeServerAddr;
use crate::biz::collab::folder_view::check_if_view_is_space;
use crate::biz::collab::utils::get_database_row_doc_changes;
use crate::biz::workspace::ops::broadcast_update_with_timeout;
use crate::biz::workspace::page_view::update_workspace_folder_data;
use access_control::collab::CollabAccessControl;
use anyhow::Context;
use database_entity::dto::{
  AFCollabMember, InsertCollabMemberParams, QueryCollabMembers, UpdateCollabMemberParams,
};
use shared_entity::dto::workspace_dto::{FolderView, PublishedView};
use sqlx::types::Uuid;
use std::collections::HashSet;
use tracing::{event, trace};
use validator::Validate;

use super::folder_view::collab_folder_to_folder_view;
use super::folder_view::section_items_to_favorite_folder_view;
use super::folder_view::section_items_to_recent_folder_view;
use super::folder_view::section_items_to_trash_folder_view;
use super::folder_view::to_dto_folder_view_miminal;
use super::publish_outline::collab_folder_to_published_outline;
use super::utils::collab_to_bin;
use super::utils::create_row_document;
use super::utils::field_by_id_name_uniq;
use super::utils::get_latest_collab;
use super::utils::get_latest_collab_database_body;
use super::utils::get_latest_collab_database_row_body;
use super::utils::get_latest_collab_folder;
use super::utils::get_row_details_serde;
use super::utils::type_option_reader_by_id;
use super::utils::type_options_serde;
use super::utils::write_to_database_row;
use super::utils::CreatedRowDocument;
use super::utils::DocChanges;
use super::utils::DEFAULT_SPACE_ICON;
use super::utils::DEFAULT_SPACE_ICON_COLOR;

/// Create a new collab member
/// If the collab member already exists, return [AppError::RecordAlreadyExists]
/// If the collab member does not exist, create a new one
pub async fn create_collab_member(
  pg_pool: &PgPool,
  params: &InsertCollabMemberParams,
  collab_access_control: Arc<dyn CollabAccessControl>,
) -> Result<(), AppError> {
  params.validate()?;

  let mut transaction = pg_pool
    .begin()
    .await
    .context("acquire transaction to insert collab member")?;

  if database::collab::is_collab_member_exists(
    params.uid,
    &params.object_id,
    transaction.deref_mut(),
  )
  .await?
  {
    return Err(AppError::RecordAlreadyExists(format!(
      "Collab member with uid {} and object_id {} already exists",
      params.uid, params.object_id
    )));
  }

  trace!("Inserting collab member: {:?}", params);
  database::collab::insert_collab_member(
    params.uid,
    &params.object_id,
    &params.access_level,
    &mut transaction,
  )
  .await?;

  collab_access_control
    .update_access_level_policy(&params.uid, &params.object_id, params.access_level)
    .await?;

  transaction
    .commit()
    .await
    .context("fail to commit the transaction to insert collab member")?;
  Ok(())
}

pub async fn upsert_collab_member(
  pg_pool: &PgPool,
  _user_uuid: &Uuid,
  params: &UpdateCollabMemberParams,
  collab_access_control: Arc<dyn CollabAccessControl>,
) -> Result<(), AppError> {
  params.validate()?;
  let mut transaction = pg_pool
    .begin()
    .await
    .context("acquire transaction to upsert collab member")?;

  collab_access_control
    .update_access_level_policy(&params.uid, &params.object_id, params.access_level)
    .await?;

  database::collab::insert_collab_member(
    params.uid,
    &params.object_id,
    &params.access_level,
    &mut transaction,
  )
  .await?;

  transaction
    .commit()
    .await
    .context("fail to commit the transaction to upsert collab member")?;
  Ok(())
}

pub async fn get_collab_member(
  pg_pool: &PgPool,
  params: &WorkspaceCollabIdentify,
) -> Result<AFCollabMember, AppError> {
  params.validate()?;
  let collab_member =
    database::collab::select_collab_member(&params.uid, &params.object_id, pg_pool).await?;
  Ok(collab_member)
}

pub async fn delete_collab_member(
  pg_pool: &PgPool,
  params: &WorkspaceCollabIdentify,
  collab_access_control: Arc<dyn CollabAccessControl>,
) -> Result<(), AppError> {
  params.validate()?;
  let mut transaction = pg_pool
    .begin()
    .await
    .context("acquire transaction to remove collab member")?;
  event!(
    tracing::Level::DEBUG,
    "Deleting member:{} from {}",
    params.uid,
    params.object_id
  );
  database::collab::delete_collab_member(params.uid, &params.object_id, &mut transaction).await?;

  collab_access_control
    .remove_access_level(&params.uid, &params.object_id)
    .await?;

  transaction
    .commit()
    .await
    .context("fail to commit the transaction to remove collab member")?;
  Ok(())
}

pub async fn get_collab_member_list(
  pg_pool: &PgPool,
  params: &QueryCollabMembers,
) -> Result<Vec<AFCollabMember>, AppError> {
  params.validate()?;
  let collab_member = database::collab::select_collab_members(&params.object_id, pg_pool).await?;
  Ok(collab_member)
}

pub async fn get_user_favorite_folder_views(
  collab_storage: &CollabAccessControlStorage,
  pg_pool: &PgPool,
  uid: i64,
  workspace_id: Uuid,
) -> Result<Vec<FavoriteFolderView>, AppError> {
  let folder = get_latest_collab_folder(
    collab_storage,
    GetCollabOrigin::User { uid },
    &workspace_id.to_string(),
  )
  .await?;
  let publish_view_ids = select_published_view_ids_for_workspace(pg_pool, workspace_id).await?;
  let publish_view_ids: HashSet<String> = publish_view_ids
    .into_iter()
    .map(|id| id.to_string())
    .collect();
  let deleted_section_item_ids: Vec<String> = folder
    .get_my_trash_sections()
    .iter()
    .map(|s| s.id.clone())
    .collect();
  let favorite_section_items: Vec<SectionItem> = folder
    .get_my_favorite_sections()
    .into_iter()
    .filter(|s| !deleted_section_item_ids.contains(&s.id))
    .collect();
  Ok(section_items_to_favorite_folder_view(
    &favorite_section_items,
    &folder,
    &publish_view_ids,
  ))
}

pub async fn get_user_recent_folder_views(
  collab_storage: &CollabAccessControlStorage,
  pg_pool: &PgPool,
  uid: i64,
  workspace_id: Uuid,
) -> Result<Vec<RecentFolderView>, AppError> {
  let folder = get_latest_collab_folder(
    collab_storage,
    GetCollabOrigin::User { uid },
    &workspace_id.to_string(),
  )
  .await?;
  let deleted_section_item_ids: Vec<String> = folder
    .get_my_trash_sections()
    .iter()
    .map(|s| s.id.clone())
    .collect();
  let recent_section_items: Vec<SectionItem> = folder
    .get_my_recent_sections()
    .into_iter()
    .filter(|s| !deleted_section_item_ids.contains(&s.id))
    .collect();
  let publish_view_ids = select_published_view_ids_for_workspace(pg_pool, workspace_id).await?;
  let publish_view_ids: HashSet<String> = publish_view_ids
    .into_iter()
    .map(|id| id.to_string())
    .collect();
  Ok(section_items_to_recent_folder_view(
    &recent_section_items,
    &folder,
    &publish_view_ids,
  ))
}

pub async fn get_user_trash_folder_views(
  collab_storage: &CollabAccessControlStorage,
  uid: i64,
  workspace_id: Uuid,
) -> Result<Vec<TrashFolderView>, AppError> {
  let folder = get_latest_collab_folder(
    collab_storage,
    GetCollabOrigin::User { uid },
    &workspace_id.to_string(),
  )
  .await?;
  let section_items = folder.get_my_trash_sections();
  Ok(section_items_to_trash_folder_view(&section_items, &folder))
}

#[allow(clippy::too_many_arguments)]
fn patch_old_workspace_folder(
  user: RealtimeUser,
  workspace_id: &str,
  folder: &mut Folder,
  child_view_id_without_space: &[String],
) -> Result<Vec<u8>, AppError> {
  let encoded_update = {
    let space_id = Uuid::new_v4().to_string();

    let space_view = NestedChildViewBuilder::new(user.uid, workspace_id.to_string())
      .with_view_id(space_id.clone())
      .with_name("General")
      .with_extra(|extra| {
        extra
          .with_space_info(SpaceInfo {
            space_icon: Some(DEFAULT_SPACE_ICON.to_string()),
            space_icon_color: Some(DEFAULT_SPACE_ICON_COLOR.to_string()),
            ..Default::default()
          })
          .build()
      })
      .build()
      .view;
    let mut txn = folder.collab.transact_mut();
    folder.body.views.insert(&mut txn, space_view, None);
    for (i, current_view_id) in child_view_id_without_space.iter().enumerate() {
      let previous_view_id = if i == 0 {
        None
      } else {
        Some(child_view_id_without_space[i - 1].clone())
      };
      folder
        .body
        .move_nested_view(&mut txn, current_view_id, &space_id, previous_view_id);
    }
    txn.encode_update_v1()
  };
  Ok(encoded_update)
}

async fn fix_old_workspace_folder(
  appflowy_web_metrics: &AppFlowyWebMetrics,
  server: Data<RealtimeServerAddr>,
  user: RealtimeUser,
  mut folder: Folder,
  workspace_id: Uuid,
) -> Result<Folder, AppError> {
  let root_view = folder.get_view(&workspace_id.to_string()).ok_or_else(|| {
    AppError::InvalidRequest(format!(
      "Failed to get view for workspace_id: {}",
      workspace_id
    ))
  })?;
  let direct_workspace_children: Vec<String> = root_view
    .children
    .iter()
    .map(|view_id| view_id.to_string())
    .collect();
  let has_at_least_one_space = direct_workspace_children
    .iter()
    .filter_map(|view_id| folder.get_view(view_id))
    .any(|view| check_if_view_is_space(&view));
  if !has_at_least_one_space {
    let folder_update = patch_old_workspace_folder(
      user.clone(),
      &workspace_id.to_string(),
      &mut folder,
      &direct_workspace_children,
    )?;
    update_workspace_folder_data(
      appflowy_web_metrics,
      server,
      user,
      workspace_id,
      folder_update,
    )
    .await?;
  }
  Ok(folder)
}

#[allow(clippy::too_many_arguments)]
pub async fn get_user_workspace_structure(
  appflowy_web_metrics: &AppFlowyWebMetrics,
  server: Data<RealtimeServerAddr>,
  collab_storage: &CollabAccessControlStorage,
  pg_pool: &PgPool,
  user: RealtimeUser,
  workspace_id: Uuid,
  depth: u32,
  root_view_id: &str,
) -> Result<FolderView, AppError> {
  let depth_limit = 10;
  if depth > depth_limit {
    return Err(AppError::InvalidRequest(format!(
      "Depth {} is too large (limit: {})",
      depth, depth_limit
    )));
  }
  let folder = get_latest_collab_folder(
    collab_storage,
    GetCollabOrigin::User { uid: user.uid },
    &workspace_id.to_string(),
  )
  .await?;
  let patched_folder =
    fix_old_workspace_folder(appflowy_web_metrics, server, user, folder, workspace_id).await?;

  let publish_view_ids = select_published_view_ids_for_workspace(pg_pool, workspace_id).await?;
  let publish_view_ids: HashSet<String> = publish_view_ids
    .into_iter()
    .map(|id| id.to_string())
    .collect();
  collab_folder_to_folder_view(
    workspace_id,
    root_view_id,
    &patched_folder,
    depth,
    &publish_view_ids,
  )
}

pub async fn get_latest_workspace_database(
  collab_storage: &CollabAccessControlStorage,
  pg_pool: &PgPool,
  collab_origin: GetCollabOrigin,
  workspace_id: Uuid,
) -> Result<(String, WorkspaceDatabase), AppError> {
  let workspace_database_oid = select_workspace_database_oid(pg_pool, &workspace_id).await?;
  let workspace_database_collab = get_latest_collab(
    collab_storage,
    collab_origin,
    &workspace_id.to_string(),
    &workspace_database_oid,
    CollabType::WorkspaceDatabase,
  )
  .await?;

  let workspace_database = WorkspaceDatabase::open(workspace_database_collab)
    .map_err(|err| AppError::Unhandled(format!("failed to open workspace database: {}", err)))?;
  Ok((workspace_database_oid, workspace_database))
}

pub async fn get_published_view(
  collab_storage: &CollabAccessControlStorage,
  publish_namespace: String,
  pg_pool: &PgPool,
) -> Result<PublishedView, AppError> {
  let workspace_id = select_workspace_id_for_publish_namespace(pg_pool, &publish_namespace).await?;
  let folder = get_latest_collab_folder(
    collab_storage,
    GetCollabOrigin::Server,
    &workspace_id.to_string(),
  )
  .await?;
  let publish_view_ids_with_publish_info =
    select_published_view_ids_with_publish_info_for_workspace(pg_pool, workspace_id).await?;
  let publish_view_id_to_info_map: HashMap<String, PublishedViewInfo> =
    publish_view_ids_with_publish_info
      .into_iter()
      .map(|pv| {
        (
          pv.view_id.to_string(),
          PublishedViewInfo {
            publisher_email: pv.publisher_email.clone(),
            publish_name: pv.publish_name.clone(),
            publish_timestamp: pv.publish_timestamp,
            comments_enabled: pv.comments_enabled,
            duplicate_enabled: pv.duplicate_enabled,
          },
        )
      })
      .collect();

  let published_view: PublishedView = collab_folder_to_published_outline(
    &workspace_id.to_string(),
    &folder,
    &publish_view_id_to_info_map,
  )?;
  Ok(published_view)
}

pub async fn list_database(
  pg_pool: &PgPool,
  collab_storage: &CollabAccessControlStorage,
  uid: i64,
  workspace_uuid_str: String,
) -> Result<Vec<AFDatabase>, AppError> {
  let workspace_uuid: Uuid = workspace_uuid_str.as_str().parse()?;
  let ws_db_oid = select_workspace_database_oid(pg_pool, &workspace_uuid).await?;

  let mut ws_body_collab = get_latest_collab(
    collab_storage,
    GetCollabOrigin::Server,
    &workspace_uuid_str,
    &ws_db_oid,
    CollabType::WorkspaceDatabase,
  )
  .await?;

  let ws_body = WorkspaceDatabaseBody::open(&mut ws_body_collab).map_err(|e| {
    AppError::Internal(anyhow::anyhow!(
      "Failed to open workspace database body: {:?}",
      e
    ))
  })?;
  let db_metas = ws_body.get_all_meta(&ws_body_collab.transact());

  let folder = get_latest_collab_folder(
    collab_storage,
    GetCollabOrigin::User { uid },
    &workspace_uuid_str,
  )
  .await?;

  let trash = folder
    .get_all_trash_sections()
    .into_iter()
    .map(|s| s.id)
    .collect::<HashSet<_>>();

  let mut af_databases = Vec::with_capacity(db_metas.len());
  for db_meta in db_metas {
    let id = db_meta.database_id;
    let mut views: Vec<FolderViewMinimal> = Vec::new();
    for linked_view_id in db_meta.linked_views {
      if !trash.contains(&linked_view_id) {
        if let Some(folder_view) = folder.get_view(&linked_view_id) {
          views.push(to_dto_folder_view_miminal(&folder_view));
        };
      }
    }
    if !views.is_empty() {
      af_databases.push(AFDatabase { id, views });
    }
  }

  Ok(af_databases)
}

pub async fn list_database_row_ids(
  collab_storage: &CollabAccessControlStorage,
  workspace_uuid_str: &str,
  database_uuid_str: &str,
) -> Result<Vec<AFDatabaseRow>, AppError> {
  let (db_collab, db_body) =
    get_latest_collab_database_body(collab_storage, workspace_uuid_str, database_uuid_str).await?;
  // get any view_id
  let txn = db_collab.transact();
  let iid = db_body.get_inline_view_id(&txn);

  let iview = db_body.views.get_view(&txn, &iid).ok_or_else(|| {
    AppError::Internal(anyhow::anyhow!("Failed to get inline view, iid: {}", iid))
  })?;

  let db_rows = iview
    .row_orders
    .into_iter()
    .map(|row_order| AFDatabaseRow {
      id: row_order.id.to_string(),
    })
    .collect();

  Ok(db_rows)
}

#[allow(clippy::too_many_arguments)]
pub async fn insert_database_row(
  collab_storage: Arc<CollabAccessControlStorage>,
  pg_pool: &PgPool,
  workspace_uuid_str: &str,
  database_uuid_str: &str,
  uid: i64,
  new_db_row_id: Option<&str>,
  cell_value_by_id: HashMap<String, serde_json::Value>,
  row_doc_content: Option<String>,
) -> Result<String, AppError> {
  let new_db_row_id: RowId = new_db_row_id
    .map(|id| RowId::from(id.to_string()))
    .unwrap_or_else(gen_row_id);

  let creation_time = Utc::now();

  let mut new_db_row_collab =
    Collab::new_with_origin(CollabOrigin::Empty, new_db_row_id.clone(), vec![], false);
  let new_db_row_body = DatabaseRowBody::create(
    new_db_row_id.clone(),
    &mut new_db_row_collab,
    Row::empty(new_db_row_id.clone(), database_uuid_str),
  );
  new_db_row_body.update(&mut new_db_row_collab.transact_mut(), |row_update| {
    row_update.set_created_at(Utc::now().timestamp());
  });

  let new_row_doc_creation: Option<(String, CreatedRowDocument)> = match row_doc_content {
    Some(row_doc_content) if !row_doc_content.is_empty() => {
      // update row to indicate that the document is not empty
      let is_document_empty_id =
        meta_id_from_row_id(&new_db_row_id.parse()?, RowMetaKey::IsDocumentEmpty);
      new_db_row_body.get_meta().insert(
        &mut new_db_row_collab.transact_mut(),
        is_document_empty_id,
        false,
      );

      // get document id
      let new_doc_id = new_db_row_body
        .document_id(&new_db_row_collab.transact())
        .map_err(|err| AppError::Internal(anyhow::anyhow!("Failed to get document id: {:?}", err)))?
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Failed to get document id")))?;

      let created_row_doc = create_row_document(
        workspace_uuid_str,
        uid,
        &new_doc_id,
        &collab_storage,
        row_doc_content,
      )
      .await?;
      Some((new_doc_id, created_row_doc))
    },
    _ => None,
  };

  let (mut db_collab, db_body) =
    get_latest_collab_database_body(&collab_storage, workspace_uuid_str, database_uuid_str).await?;
  write_to_database_row(
    &db_body,
    &mut new_db_row_collab.transact_mut(),
    &new_db_row_body,
    cell_value_by_id,
    creation_time.timestamp(),
  )
  .await?;

  // Create new row order
  let ts_now = creation_time.timestamp();
  let row_order = db_body
    .create_row(CreateRowParams {
      id: new_db_row_id.clone(),
      database_id: database_uuid_str.to_string(),
      cells: new_db_row_body
        .cells(&new_db_row_collab.transact())
        .unwrap_or_default(),
      height: 30,
      visibility: true,
      row_position: OrderObjectPosition::End,
      created_at: ts_now,
      modified_at: ts_now,
    })
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create row: {:?}", e)))?;

  let new_db_row_ec_v1 = collab_to_bin(new_db_row_collab, CollabType::DatabaseRow).await?;

  // For each database view, add the new row order
  let db_collab_update = {
    let mut txn = db_collab.transact_mut();
    let mut db_views = db_body.views.get_all_views(&txn);
    for db_view in db_views.iter_mut() {
      db_view.row_orders.push(row_order.clone());
    }
    db_body.views.clear(&mut txn);
    for view in db_views {
      db_body.views.insert_view(&mut txn, view);
    }

    txn.encode_update_v1()
  };
  let updated_db_collab = collab_to_bin(db_collab, CollabType::Database).await?;

  let mut db_txn = pg_pool.begin().await?;

  // handle row document (if provided)
  if let Some((doc_id, created_doc)) = new_row_doc_creation {
    // insert document
    collab_storage
      .upsert_new_collab_with_transaction(
        workspace_uuid_str,
        &uid,
        CollabParams {
          object_id: doc_id,
          encoded_collab_v1: created_doc.doc_ec_bytes.into(),
          collab_type: CollabType::Document,
        },
        &mut db_txn,
        "inserting new database row document from server",
      )
      .await?;

    // update folder and broadcast
    collab_storage
      .upsert_new_collab_with_transaction(
        workspace_uuid_str,
        &uid,
        CollabParams {
          object_id: workspace_uuid_str.to_string(),
          encoded_collab_v1: created_doc.updated_folder.into(),
          collab_type: CollabType::Folder,
        },
        &mut db_txn,
        "inserting updated folder from server",
      )
      .await?;
    broadcast_update_with_timeout(
      collab_storage.clone(),
      workspace_uuid_str.to_string(),
      created_doc.folder_updates,
    )
    .await;
  };

  // insert row
  collab_storage
    .upsert_new_collab_with_transaction(
      workspace_uuid_str,
      &uid,
      CollabParams {
        object_id: new_db_row_id.to_string(),
        encoded_collab_v1: new_db_row_ec_v1.into(),
        collab_type: CollabType::DatabaseRow,
      },
      &mut db_txn,
      "inserting new database row from server",
    )
    .await?;

  // update database
  collab_storage
    .upsert_new_collab_with_transaction(
      workspace_uuid_str,
      &uid,
      CollabParams {
        object_id: database_uuid_str.to_string(),
        encoded_collab_v1: updated_db_collab.into(),
        collab_type: CollabType::Database,
      },
      &mut db_txn,
      "inserting updated database from server",
    )
    .await?;

  db_txn.commit().await?;
  broadcast_update_with_timeout(
    collab_storage,
    database_uuid_str.to_string(),
    db_collab_update,
  )
  .await;
  Ok(new_db_row_id.to_string())
}

#[allow(clippy::too_many_arguments)]
pub async fn upsert_database_row(
  collab_storage: Arc<CollabAccessControlStorage>,
  pg_pool: &PgPool,
  workspace_uuid_str: &str,
  database_uuid_str: &str,
  uid: i64,
  row_id: &str,
  cell_value_by_id: HashMap<String, serde_json::Value>,
  row_doc_content: Option<String>,
) -> Result<(), AppError> {
  let (mut db_row_collab, db_row_body) =
    match get_latest_collab_database_row_body(&collab_storage, workspace_uuid_str, row_id).await {
      Ok(res) => res,
      Err(err) => match err {
        AppError::RecordNotFound(_) => {
          return insert_database_row(
            collab_storage,
            pg_pool,
            workspace_uuid_str,
            database_uuid_str,
            uid,
            Some(row_id),
            cell_value_by_id,
            row_doc_content,
          )
          .await
          .map(|_id| {});
        },
        _ => return Err(err),
      },
    };

  // At this point, db row exists,
  // so we modify it, put into storage and broadcast change
  let (_db_collab, db_body) =
    get_latest_collab_database_body(&collab_storage, workspace_uuid_str, database_uuid_str).await?;
  let mut db_row_txn = db_row_collab.transact_mut();
  write_to_database_row(
    &db_body,
    &mut db_row_txn,
    &db_row_body,
    cell_value_by_id,
    Utc::now().timestamp(),
  )
  .await?;

  // determine if there are any document changes
  let doc_changes: Option<(String, DocChanges)> = get_database_row_doc_changes(
    &collab_storage,
    workspace_uuid_str,
    row_doc_content,
    &db_row_body,
    &mut db_row_txn,
    row_id,
    uid,
  )
  .await?;

  // finalize update for database row
  let db_row_collab_updates = db_row_txn.encode_update_v1();
  drop(db_row_txn);
  let db_row_ec_v1 = collab_to_bin(db_row_collab, CollabType::DatabaseRow).await?;

  // write to disk and broadcast changes
  let mut db_txn = pg_pool.begin().await?;
  collab_storage
    .upsert_new_collab_with_transaction(
      workspace_uuid_str,
      &uid,
      CollabParams {
        object_id: row_id.to_string(),
        encoded_collab_v1: db_row_ec_v1.into(),
        collab_type: CollabType::DatabaseRow,
      },
      &mut db_txn,
      "inserting new database row from server",
    )
    .await?;
  broadcast_update_with_timeout(
    collab_storage.clone(),
    row_id.to_string(),
    db_row_collab_updates,
  )
  .await;

  // handle document changes
  if let Some((doc_id, doc_changes)) = doc_changes {
    match doc_changes {
      DocChanges::Update(updated_doc, doc_update) => {
        collab_storage
          .upsert_new_collab_with_transaction(
            workspace_uuid_str,
            &uid,
            CollabParams {
              object_id: doc_id.clone(),
              encoded_collab_v1: updated_doc.into(),
              collab_type: CollabType::Document,
            },
            &mut db_txn,
            "updating database row document from server",
          )
          .await?;
        broadcast_update_with_timeout(collab_storage, doc_id, doc_update).await;
      },
      DocChanges::Insert(created_doc) => {
        let CreatedRowDocument {
          updated_folder,
          folder_updates,
          doc_ec_bytes,
        } = created_doc;

        // insert document
        collab_storage
          .upsert_new_collab_with_transaction(
            workspace_uuid_str,
            &uid,
            CollabParams {
              object_id: doc_id,
              encoded_collab_v1: doc_ec_bytes.into(),
              collab_type: CollabType::Document,
            },
            &mut db_txn,
            "inserting new database row document from server",
          )
          .await?;

        // update folder and broadcast
        collab_storage
          .upsert_new_collab_with_transaction(
            workspace_uuid_str,
            &uid,
            CollabParams {
              object_id: workspace_uuid_str.to_string(),
              encoded_collab_v1: updated_folder.into(),
              collab_type: CollabType::Folder,
            },
            &mut db_txn,
            "inserting updated folder from server",
          )
          .await?;
        broadcast_update_with_timeout(
          collab_storage,
          workspace_uuid_str.to_string(),
          folder_updates,
        )
        .await;
      },
    }
  }

  db_txn.commit().await?;
  Ok(())
}

pub async fn get_database_fields(
  collab_storage: &CollabAccessControlStorage,
  workspace_uuid_str: &str,
  database_uuid_str: &str,
) -> Result<Vec<AFDatabaseField>, AppError> {
  let (db_collab, db_body) =
    get_latest_collab_database_body(collab_storage, workspace_uuid_str, database_uuid_str).await?;

  let all_fields = db_body.fields.get_all_fields(&db_collab.transact());
  let mut acc = Vec::with_capacity(all_fields.len());
  for field in all_fields {
    let field_type = FieldType::from(field.field_type);
    acc.push(AFDatabaseField {
      id: field.id,
      name: field.name,
      field_type: format!("{:?}", field_type),
      type_option: type_options_serde(&field.type_options, &field_type),
      is_primary: field.is_primary,
    });
  }
  Ok(acc)
}

// inserts a new field into the database
// returns the id of the field created
pub async fn add_database_field(
  uid: i64,
  collab_storage: Arc<CollabAccessControlStorage>,
  pg_pool: &PgPool,
  workspace_id: &str,
  database_id: &str,
  insert_field: AFInsertDatabaseField,
) -> Result<String, AppError> {
  let (mut db_collab, db_body) =
    get_latest_collab_database_body(&collab_storage, workspace_id, database_id).await?;

  let new_id = gen_field_id();
  let mut type_options = TypeOptions::new();
  let type_option_data = insert_field
    .type_option_data
    .unwrap_or(serde_json::json!({}));
  match serde_json::from_value(type_option_data) {
    Ok(tod) => type_options.insert(insert_field.field_type.to_string(), tod),
    Err(err) => {
      return Err(AppError::InvalidRequest(format!(
        "Failed to parse type option: {:?}",
        err
      )));
    },
  };

  let new_field = Field {
    id: new_id.clone(),
    name: insert_field.name,
    field_type: insert_field.field_type,
    type_options,
    ..Default::default()
  };

  let db_collab_update = {
    let mut yrs_txn = db_collab.transact_mut();
    db_body.create_field(
      &mut yrs_txn,
      None,
      new_field,
      &OrderObjectPosition::End,
      &HashMap::new(),
    );
    yrs_txn.encode_update_v1()
  };
  let updated_db_collab = collab_to_bin(db_collab, CollabType::Database).await?;

  let mut pg_txn = pg_pool.begin().await?;
  collab_storage
    .upsert_new_collab_with_transaction(
      workspace_id,
      &uid,
      CollabParams {
        object_id: database_id.to_string(),
        encoded_collab_v1: updated_db_collab.into(),
        collab_type: CollabType::Database,
      },
      &mut pg_txn,
      "inserting updated database from server",
    )
    .await?;

  pg_txn.commit().await?;
  broadcast_update_with_timeout(collab_storage, database_id.to_string(), db_collab_update).await;

  Ok(new_id)
}

pub async fn list_database_row_ids_updated(
  collab_storage: &CollabAccessControlStorage,
  pg_pool: &PgPool,
  workspace_uuid_str: &str,
  database_uuid_str: &str,
  after: &DateTime<Utc>,
) -> Result<Vec<DatabaseRowUpdatedItem>, AppError> {
  let row_ids = list_database_row_ids(collab_storage, workspace_uuid_str, database_uuid_str)
    .await?
    .into_iter()
    .map(|row| row.id)
    .collect::<Vec<String>>();

  let workspace_uuid: Uuid = workspace_uuid_str.parse()?;
  let updated_row_ids =
    select_last_updated_database_row_ids(pg_pool, &workspace_uuid, &row_ids, after).await?;
  Ok(updated_row_ids)
}

pub async fn list_database_row_details(
  collab_storage: &CollabAccessControlStorage,
  uid: i64,
  workspace_uuid_str: String,
  database_uuid_str: String,
  row_ids: &[&str],
  unsupported_field_types: &[FieldType],
  with_doc: bool,
) -> Result<Vec<AFDatabaseRowDetail>, AppError> {
  let (database_collab, db_body) =
    get_latest_collab_database_body(collab_storage, &workspace_uuid_str, &database_uuid_str)
      .await?;

  let all_fields: Vec<Field> = db_body
    .fields
    .get_all_fields(&database_collab.transact())
    .into_iter()
    .filter(|field| !unsupported_field_types.contains(&FieldType::from(field.field_type)))
    .collect();
  if all_fields.is_empty() {
    return Ok(vec![]);
  }

  let type_option_reader_by_id = type_option_reader_by_id(&all_fields);
  let field_by_id = field_by_id_name_uniq(all_fields);
  let query_collabs: Vec<QueryCollab> = row_ids
    .iter()
    .map(|id| QueryCollab {
      object_id: id.to_string(),
      collab_type: CollabType::DatabaseRow,
    })
    .collect();
  let mut db_row_details = collab_storage
    .batch_get_collab(&uid, &workspace_uuid_str, query_collabs, true)
    .await
    .into_iter()
    .flat_map(|(id, result)| match result {
      QueryCollabResult::Success { encode_collab_v1 } => {
        let ec = match EncodedCollab::decode_from_bytes(&encode_collab_v1) {
          Ok(ec) => ec,
          Err(err) => {
            tracing::error!("Failed to decode encoded collab: {:?}", err);
            return None;
          },
        };
        let collab =
          match Collab::new_with_source(CollabOrigin::Server, &id, ec.into(), vec![], false) {
            Ok(collab) => collab,
            Err(err) => {
              tracing::error!("Failed to create collab: {:?}", err);
              return None;
            },
          };
        let row_detail = match RowDetail::from_collab(&collab) {
          Some(row_detail) => row_detail,
          None => {
            tracing::error!("Failed to get row detail from collab: {:?}", collab);
            return None;
          },
        };

        let has_doc = !row_detail.meta.is_document_empty;
        let cells = get_row_details_serde(row_detail, &field_by_id, &type_option_reader_by_id);
        Some(AFDatabaseRowDetail {
          id,
          cells,
          has_doc,
          doc: None,
        })
      },
      QueryCollabResult::Failed { error } => {
        tracing::warn!("Failed to get collab: {:?}", error);
        None
      },
    })
    .collect::<Vec<AFDatabaseRowDetail>>();

  // Fill in the document content if requested and exists
  if with_doc {
    let doc_id_by_row_id = db_row_details
      .iter()
      .filter(|row| row.has_doc)
      .flat_map(|row| {
        row.id.parse::<Uuid>().ok().map(|row_uuid| {
          (
            row.id.clone(),
            meta_id_from_row_id(&row_uuid, RowMetaKey::DocumentId),
          )
        })
      })
      .collect::<HashMap<_, _>>();
    let query_db_docs = doc_id_by_row_id
      .values()
      .map(|doc_id| QueryCollab {
        object_id: doc_id.to_string(),
        collab_type: CollabType::Document,
      })
      .collect::<Vec<_>>();
    let mut query_res = collab_storage
      .batch_get_collab(&uid, &workspace_uuid_str, query_db_docs, true)
      .await;
    for row_detail in &mut db_row_details {
      if let Err(err) = fill_in_db_row_doc(row_detail, &doc_id_by_row_id, &mut query_res) {
        tracing::error!("Failed to fill in document content: {:?}", err);
      };
    }
  }

  Ok(db_row_details)
}

fn fill_in_db_row_doc(
  row_detail: &mut AFDatabaseRowDetail,
  doc_id_by_row_id: &HashMap<String, String>,
  query_res: &mut HashMap<String, QueryCollabResult>,
) -> Result<(), AppError> {
  let doc_id = doc_id_by_row_id.get(&row_detail.id).ok_or_else(|| {
    AppError::Internal(anyhow::anyhow!(
      "Failed to get document id for row id: {}",
      row_detail.id
    ))
  })?;
  let res = query_res.remove(doc_id.as_str()).ok_or_else(|| {
    AppError::Internal(anyhow::anyhow!(
      "Failed to get document collab for row id: {}",
      row_detail.id
    ))
  })?;

  let ec_bytes = match res {
    QueryCollabResult::Success { encode_collab_v1 } => encode_collab_v1,
    QueryCollabResult::Failed { error } => return Err(AppError::Internal(anyhow::anyhow!(error))),
  };
  let ec = EncodedCollab::decode_from_bytes(&ec_bytes)?;
  let doc_collab = Collab::new_with_source(CollabOrigin::Server, doc_id, ec.into(), vec![], false)
    .map_err(|err| {
      AppError::Internal(anyhow::anyhow!(
        "Failed to create document collab: {:?}",
        err
      ))
    })?;
  let doc = Document::open(doc_collab)
    .map_err(|err| AppError::Internal(anyhow::anyhow!("Failed to open document: {:?}", err)))?;
  let plain_text = doc.paragraphs().join("");
  row_detail.doc = Some(plain_text);
  Ok(())
}
