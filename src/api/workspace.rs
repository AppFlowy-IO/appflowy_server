use crate::api::util::{client_version_from_headers, realtime_user_for_web_request, PayloadReader};
use crate::api::util::{compress_type_from_header_value, device_id_from_headers};
use crate::api::ws::RealtimeServerAddr;
use crate::biz;
use crate::biz::collab::ops::{
  get_user_favorite_folder_views, get_user_recent_folder_views, get_user_trash_folder_views,
};
use crate::biz::user::user_verify::verify_token;
use crate::biz::workspace;
use crate::biz::workspace::ops::{
  create_comment_on_published_view, create_reaction_on_comment, get_comments_on_published_view,
  get_reactions_on_published_view, remove_comment_on_published_view, remove_reaction_on_comment,
};
use crate::biz::workspace::page_view::{
  create_page, create_space, delete_all_pages_from_trash, delete_trash, get_page_view_collab,
  move_page, move_page_to_trash, publish_page, restore_all_pages_from_trash,
  restore_page_from_trash, unpublish_page, update_page, update_page_collab_data, update_space,
};
use crate::biz::workspace::publish::get_workspace_default_publish_view_info_meta;
use crate::biz::workspace::quick_note::{
  create_quick_note, delete_quick_note, list_quick_notes, update_quick_note,
};
use crate::domain::compression::{
  blocking_decompress, decompress, CompressionType, X_COMPRESSION_TYPE,
};
use crate::state::AppState;
use access_control::act::Action;
use actix_web::web::{Bytes, Path, Payload};
use actix_web::web::{Data, Json, PayloadConfig};
use actix_web::{web, HttpResponse, ResponseError, Scope};
use actix_web::{HttpRequest, Result};
use anyhow::{anyhow, Context};
use app_error::AppError;
use appflowy_collaborate::actix_ws::entities::{ClientHttpStreamMessage, ClientHttpUpdateMessage};
use authentication::jwt::{Authorization, OptionalUserUuid, UserUuid};
use bytes::BytesMut;
use chrono::{DateTime, Duration, Utc};
use collab::core::collab::DataSource;
use collab::core::origin::CollabOrigin;
use collab::entity::EncodedCollab;
use collab::preclude::Collab;
use collab_database::entity::FieldType;
use collab_document::document::Document;
use collab_entity::CollabType;
use collab_folder::timestamp;
use collab_rt_entity::collab_proto::{CollabDocStateParams, PayloadCompressionType};
use collab_rt_entity::realtime_proto::HttpRealtimeMessage;
use collab_rt_entity::user::RealtimeUser;
use collab_rt_entity::RealtimeMessage;
use collab_rt_protocol::collab_from_encode_collab;
use database::collab::{CollabStorage, GetCollabOrigin};
use database::user::select_uid_from_email;
use database_entity::dto::PublishCollabItem;
use database_entity::dto::PublishInfo;
use database_entity::dto::*;
use indexer::scheduler::{UnindexedCollabTask, UnindexedData};
use prost::Message as ProstMessage;
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use shared_entity::dto::publish_dto::DuplicatePublishedPageResponse;
use shared_entity::dto::workspace_dto::*;
use shared_entity::response::AppResponseError;
use shared_entity::response::{AppResponse, JsonAppResponse};
use sqlx::types::uuid;
use std::io::Cursor;
use std::time::Instant;
use tokio_stream::StreamExt;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, event, instrument, trace};
use uuid::Uuid;
use validator::Validate;

pub const WORKSPACE_ID_PATH: &str = "workspace_id";
pub const COLLAB_OBJECT_ID_PATH: &str = "object_id";

pub const WORKSPACE_PATTERN: &str = "/api/workspace";
pub const WORKSPACE_MEMBER_PATTERN: &str = "/api/workspace/{workspace_id}/member";
pub const WORKSPACE_INVITE_PATTERN: &str = "/api/workspace/{workspace_id}/invite";
pub const COLLAB_PATTERN: &str = "/api/workspace/{workspace_id}/collab/{object_id}";
pub const V1_COLLAB_PATTERN: &str = "/api/workspace/v1/{workspace_id}/collab/{object_id}";
pub const WORKSPACE_PUBLISH_PATTERN: &str = "/api/workspace/{workspace_id}/publish";
pub const WORKSPACE_PUBLISH_NAMESPACE_PATTERN: &str =
  "/api/workspace/{workspace_id}/publish-namespace";

pub fn workspace_scope() -> Scope {
  web::scope("/api/workspace")
    .service(
      web::resource("")
        .route(web::get().to(list_workspace_handler))
        .route(web::post().to(create_workspace_handler))
        .route(web::patch().to(patch_workspace_handler)),
    )
    .service(
      web::resource("/{workspace_id}/invite").route(web::post().to(post_workspace_invite_handler)), // invite members to workspace
    )
    .service(
      web::resource("/invite").route(web::get().to(get_workspace_invite_handler)), // show invites for user
    )
    .service(
      web::resource("/invite/{invite_id}").route(web::get().to(get_workspace_invite_by_id_handler)),
    )
    .service(
      web::resource("/accept-invite/{invite_id}")
        .route(web::post().to(post_accept_workspace_invite_handler)), // accept invitation to workspace
    )
    .service(web::resource("/{workspace_id}").route(web::delete().to(delete_workspace_handler)))
    .service(
      web::resource("/{workspace_id}/settings")
        .route(web::get().to(get_workspace_settings_handler))
        .route(web::post().to(post_workspace_settings_handler)),
    )
    .service(web::resource("/{workspace_id}/open").route(web::put().to(open_workspace_handler)))
    .service(web::resource("/{workspace_id}/leave").route(web::post().to(leave_workspace_handler)))
    .service(
      web::resource("/{workspace_id}/member")
        .route(web::get().to(get_workspace_members_handler))
        .route(web::put().to(update_workspace_member_handler))
        .route(web::delete().to(remove_workspace_member_handler)),
    )
    .service(
      web::resource("/{workspace_id}/member/user/{user_id}")
        .route(web::get().to(get_workspace_member_handler)),
    )
    .service(
      web::resource("/{workspace_id}/collab/{object_id}")
        .app_data(
          PayloadConfig::new(5 * 1024 * 1024), // 5 MB
        )
        .route(web::post().to(create_collab_handler))
        .route(web::get().to(get_collab_handler))
        .route(web::put().to(update_collab_handler))
        .route(web::delete().to(delete_collab_handler)),
    )
    .service(
      web::resource("/v1/{workspace_id}/collab/{object_id}")
        .route(web::get().to(v1_get_collab_handler)),
    )
    .service(
      web::resource("/v1/{workspace_id}/collab/{object_id}/full-sync")
        .route(web::post().to(collab_full_sync_handler)),
    )
    .service(
      web::resource("/v1/{workspace_id}/collab/{object_id}/web-update")
        .route(web::post().to(post_web_update_handler)),
    )
    .service(
      web::resource("/{workspace_id}/collab/{object_id}/member")
        .route(web::post().to(add_collab_member_handler))
        .route(web::get().to(get_collab_member_handler))
        .route(web::put().to(update_collab_member_handler))
        .route(web::delete().to(remove_collab_member_handler)),
    )
    .service(
      web::resource("/{workspace_id}/collab/{object_id}/embed-info")
        .route(web::get().to(get_collab_embed_info_handler)),
    )
    .service(
      web::resource("/{workspace_id}/collab/embed-info/list")
        .route(web::post().to(batch_get_collab_embed_info_handler)),
    )
    .service(web::resource("/{workspace_id}/space").route(web::post().to(post_space_handler)))
    .service(
      web::resource("/{workspace_id}/space/{view_id}").route(web::patch().to(update_space_handler)),
    )
    .service(
      web::resource("/{workspace_id}/page-view").route(web::post().to(post_page_view_handler)),
    )
    .service(
      web::resource("/{workspace_id}/page-view/{view_id}")
        .route(web::get().to(get_page_view_handler))
        .route(web::patch().to(update_page_view_handler)),
    )
    .service(
      web::resource("/{workspace_id}/page-view/{view_id}/move")
        .route(web::post().to(move_page_handler)),
    )
    .service(
      web::resource("/{workspace_id}/page-view/{view_id}/move-to-trash")
        .route(web::post().to(move_page_to_trash_handler)),
    )
    .service(
      web::resource("/{workspace_id}/page-view/{view_id}/restore-from-trash")
        .route(web::post().to(restore_page_from_trash_handler)),
    )
    .service(
      web::resource("/{workspace_id}/restore-all-pages-from-trash")
        .route(web::post().to(restore_all_pages_from_trash_handler)),
    )
    .service(
      web::resource("/{workspace_id}/delete-all-pages-from-trash")
        .route(web::post().to(delete_all_pages_from_trash_handler)),
    )
    .service(
      web::resource("/{workspace_id}/page-view/{view_id}/publish")
        .route(web::post().to(publish_page_handler)),
    )
    .service(
      web::resource("/{workspace_id}/page-view/{view_id}/unpublish")
        .route(web::post().to(unpublish_page_handler)),
    )
    .service(
      web::resource("/{workspace_id}/batch/collab")
        .route(web::post().to(batch_create_collab_handler)),
    )
    .service(
      web::resource("/{workspace_id}/usage").route(web::get().to(get_workspace_usage_handler)),
    )
    .service(
      web::resource("/{workspace_id}/{object_id}/snapshot")
        .route(web::get().to(get_collab_snapshot_handler))
        .route(web::post().to(create_collab_snapshot_handler)),
    )
    .service(
      web::resource("/{workspace_id}/{object_id}/snapshot/list")
        .route(web::get().to(get_all_collab_snapshot_list_handler)),
    )
    .service(
      web::resource("/published/{publish_namespace}")
        .route(web::get().to(get_default_published_collab_info_meta_handler)),
    )
    .service(
      web::resource("/v1/published/{publish_namespace}/{publish_name}")
        .route(web::get().to(get_v1_published_collab_handler)),
    )
    .service(
      web::resource("/published/{publish_namespace}/{publish_name}/blob")
        .route(web::get().to(get_published_collab_blob_handler)),
    )
    .service(
      web::resource("{workspace_id}/published-duplicate")
        .route(web::post().to(post_published_duplicate_handler)),
    )
    .service(
      web::resource("/{workspace_id}/published-info")
        .route(web::get().to(list_published_collab_info_handler)),
    )
    .service(
      // deprecated since 0.7.4
      web::resource("/published-info/{view_id}")
        .route(web::get().to(get_published_collab_info_handler)),
    )
    .service(
      web::resource("/v1/published-info/{view_id}")
        .route(web::get().to(get_v1_published_collab_info_handler)),
    )
    .service(
      web::resource("/published-info/{view_id}/comment")
        .route(web::get().to(get_published_collab_comment_handler))
        .route(web::post().to(post_published_collab_comment_handler))
        .route(web::delete().to(delete_published_collab_comment_handler)),
    )
    .service(
      web::resource("/published-info/{view_id}/reaction")
        .route(web::get().to(get_published_collab_reaction_handler))
        .route(web::post().to(post_published_collab_reaction_handler))
        .route(web::delete().to(delete_published_collab_reaction_handler)),
    )
    .service(
      web::resource("/{workspace_id}/publish-namespace")
        .route(web::put().to(put_publish_namespace_handler))
        .route(web::get().to(get_publish_namespace_handler)),
    )
    .service(
      web::resource("/{workspace_id}/publish-default")
        .route(web::put().to(put_workspace_default_published_view_handler))
        .route(web::delete().to(delete_workspace_default_published_view_handler))
        .route(web::get().to(get_workspace_published_default_info_handler)),
    )
    .service(
      web::resource("/{workspace_id}/publish")
        .route(web::post().to(post_publish_collabs_handler))
        .route(web::delete().to(delete_published_collabs_handler))
        .route(web::patch().to(patch_published_collabs_handler)),
    )
    .service(
      web::resource("/{workspace_id}/folder").route(web::get().to(get_workspace_folder_handler)),
    )
    .service(web::resource("/{workspace_id}/recent").route(web::get().to(get_recent_views_handler)))
    .service(
      web::resource("/{workspace_id}/favorite").route(web::get().to(get_favorite_views_handler)),
    )
    .service(web::resource("/{workspace_id}/trash").route(web::get().to(get_trash_views_handler)))
    .service(
      web::resource("/{workspace_id}/trash/{view_id}")
        .route(web::delete().to(delete_page_from_trash_handler)),
    )
    .service(
      web::resource("/published-outline/{publish_namespace}")
        .route(web::get().to(get_workspace_publish_outline_handler)),
    )
    .service(
      web::resource("/{workspace_id}/collab/{object_id}/member/list")
        .route(web::get().to(get_collab_member_list_handler)),
    )
    .service(
      web::resource("/{workspace_id}/collab_list")
      .route(web::get().to(batch_get_collab_handler))
      // Web browser can't carry payload when using GET method, so for browser compatibility, we use POST method
      .route(web::post().to(batch_get_collab_handler)),
    )
    .service(web::resource("/{workspace_id}/database").route(web::get().to(list_database_handler)))
    .service(
      web::resource("/{workspace_id}/database/{database_id}/row")
        .route(web::get().to(list_database_row_id_handler))
        .route(web::post().to(post_database_row_handler))
        .route(web::put().to(put_database_row_handler)),
    )
    .service(
      web::resource("/{workspace_id}/database/{database_id}/fields")
        .route(web::get().to(get_database_fields_handler))
        .route(web::post().to(post_database_fields_handler)),
    )
    .service(
      web::resource("/{workspace_id}/database/{database_id}/row/updated")
        .route(web::get().to(list_database_row_id_updated_handler)),
    )
    .service(
      web::resource("/{workspace_id}/database/{database_id}/row/detail")
        .route(web::get().to(list_database_row_details_handler)),
    )
    .service(
      web::resource("/{workspace_id}/quick-note")
        .route(web::get().to(list_quick_notes_handler))
        .route(web::post().to(post_quick_note_handler)),
    )
    .service(
      web::resource("/{workspace_id}/quick-note/{quick_note_id}")
        .route(web::put().to(update_quick_note_handler))
        .route(web::delete().to(delete_quick_note_handler)),
    )
}

pub fn collab_scope() -> Scope {
  web::scope("/api/realtime").service(
    web::resource("post/stream")
      .app_data(
        PayloadConfig::new(10 * 1024 * 1024), // 10 MB
      )
      .route(web::post().to(post_realtime_message_stream_handler)),
  )
}

// Adds a workspace for user, if success, return the workspace id
#[instrument(skip_all, err)]
async fn create_workspace_handler(
  uuid: UserUuid,
  state: Data<AppState>,
  create_workspace_param: Json<CreateWorkspaceParam>,
) -> Result<Json<AppResponse<AFWorkspace>>> {
  let workspace_name = create_workspace_param
    .into_inner()
    .workspace_name
    .unwrap_or_else(|| format!("workspace_{}", chrono::Utc::now().timestamp()));

  let uid = state.user_cache.get_user_uid(&uuid).await?;
  let new_workspace = workspace::ops::create_workspace_for_user(
    &state.pg_pool,
    state.workspace_access_control.clone(),
    &state.collab_access_control_storage,
    &uuid,
    uid,
    &workspace_name,
  )
  .await?;

  Ok(AppResponse::Ok().with_data(new_workspace).into())
}

// Edit existing workspace
#[instrument(skip_all, err)]
async fn patch_workspace_handler(
  uuid: UserUuid,
  state: Data<AppState>,
  params: Json<PatchWorkspaceParam>,
) -> Result<Json<AppResponse<()>>> {
  let uid = state.user_cache.get_user_uid(&uuid).await?;
  state
    .workspace_access_control
    .enforce_action(&uid, &params.workspace_id.to_string(), Action::Write)
    .await?;
  let params = params.into_inner();
  workspace::ops::patch_workspace(
    &state.pg_pool,
    &params.workspace_id,
    params.workspace_name.as_deref(),
    params.workspace_icon.as_deref(),
  )
  .await?;
  Ok(AppResponse::Ok().into())
}

async fn delete_workspace_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<()>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id.to_string(), Action::Delete)
    .await?;
  workspace::ops::delete_workspace_for_user(
    state.pg_pool.clone(),
    *workspace_id,
    state.bucket_storage.clone(),
  )
  .await?;
  Ok(AppResponse::Ok().into())
}

/// Get all user owned and shared workspaces
#[instrument(skip_all, err)]
async fn list_workspace_handler(
  uuid: UserUuid,
  state: Data<AppState>,
  query: web::Query<QueryWorkspaceParam>,
) -> Result<JsonAppResponse<Vec<AFWorkspace>>> {
  let QueryWorkspaceParam {
    include_member_count,
    include_role,
  } = query.into_inner();

  let workspaces = workspace::ops::get_all_user_workspaces(
    &state.pg_pool,
    &uuid,
    include_member_count.unwrap_or(false),
    include_role.unwrap_or(false),
  )
  .await?;
  Ok(AppResponse::Ok().with_data(workspaces).into())
}

#[instrument(skip(payload, state), err)]
async fn post_workspace_invite_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  payload: Json<Vec<WorkspaceMemberInvitation>>,
  state: Data<AppState>,
) -> Result<JsonAppResponse<()>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_role(&uid, &workspace_id.to_string(), AFRole::Owner)
    .await?;

  let invitations = payload.into_inner();
  workspace::ops::invite_workspace_members(
    &state.mailer,
    &state.gotrue_admin,
    &state.pg_pool,
    &state.gotrue_client,
    &user_uuid,
    &workspace_id,
    invitations,
    state.config.appflowy_web_url.as_deref(),
    &state.config.admin_frontend_path_prefix,
  )
  .await?;
  Ok(AppResponse::Ok().into())
}

async fn get_workspace_invite_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  query: web::Query<WorkspaceInviteQuery>,
) -> Result<JsonAppResponse<Vec<AFWorkspaceInvitation>>> {
  let query = query.into_inner();
  let res =
    workspace::ops::list_workspace_invitations_for_user(&state.pg_pool, &user_uuid, query.status)
      .await?;
  Ok(AppResponse::Ok().with_data(res).into())
}

async fn get_workspace_invite_by_id_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  invite_id: web::Path<Uuid>,
) -> Result<JsonAppResponse<AFWorkspaceInvitation>> {
  let invite_id = invite_id.into_inner();
  let res =
    workspace::ops::get_workspace_invitations_for_user(&state.pg_pool, &user_uuid, &invite_id)
      .await?;
  Ok(AppResponse::Ok().with_data(res).into())
}

async fn post_accept_workspace_invite_handler(
  auth: Authorization,
  invite_id: web::Path<Uuid>,
  state: Data<AppState>,
) -> Result<JsonAppResponse<()>> {
  let _is_new = verify_token(&auth.token, state.as_ref()).await?;
  let user_uuid = auth.uuid()?;
  let user_uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let invite_id = invite_id.into_inner();
  workspace::ops::accept_workspace_invite(
    &state.pg_pool,
    state.workspace_access_control.clone(),
    user_uid,
    &user_uuid,
    &invite_id,
  )
  .await?;
  Ok(AppResponse::Ok().into())
}

#[instrument(skip_all, err, fields(user_uuid))]
async fn get_workspace_settings_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  workspace_id: web::Path<Uuid>,
) -> Result<JsonAppResponse<AFWorkspaceSettings>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id.to_string(), Action::Read)
    .await?;
  let settings = workspace::ops::get_workspace_settings(&state.pg_pool, &workspace_id).await?;
  Ok(AppResponse::Ok().with_data(settings).into())
}

#[instrument(level = "info", skip_all, err, fields(user_uuid))]
async fn post_workspace_settings_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  workspace_id: web::Path<Uuid>,
  data: Json<AFWorkspaceSettingsChange>,
) -> Result<JsonAppResponse<AFWorkspaceSettings>> {
  let data = data.into_inner();
  trace!("workspace settings: {:?}", data);
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id.to_string(), Action::Write)
    .await?;
  let settings =
    workspace::ops::update_workspace_settings(&state.pg_pool, &workspace_id, data).await?;
  Ok(AppResponse::Ok().with_data(settings).into())
}

#[instrument(skip_all, err)]
async fn get_workspace_members_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  workspace_id: web::Path<Uuid>,
) -> Result<JsonAppResponse<Vec<AFWorkspaceMember>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_role(&uid, &workspace_id.to_string(), AFRole::Member)
    .await?;
  let members = workspace::ops::get_workspace_members(&state.pg_pool, &workspace_id)
    .await?
    .into_iter()
    .map(|member| AFWorkspaceMember {
      name: member.name,
      email: member.email,
      role: member.role,
      avatar_url: None,
    })
    .collect();

  Ok(AppResponse::Ok().with_data(members).into())
}

#[instrument(skip_all, err)]
async fn remove_workspace_member_handler(
  user_uuid: UserUuid,
  payload: Json<WorkspaceMembers>,
  state: Data<AppState>,
  workspace_id: web::Path<Uuid>,
) -> Result<JsonAppResponse<()>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_role(&uid, &workspace_id.to_string(), AFRole::Owner)
    .await?;

  let member_emails = payload
    .into_inner()
    .0
    .into_iter()
    .map(|member| member.0)
    .collect::<Vec<String>>();
  workspace::ops::remove_workspace_members(
    &state.pg_pool,
    &workspace_id,
    &member_emails,
    state.workspace_access_control.clone(),
  )
  .await?;

  Ok(AppResponse::Ok().into())
}

#[instrument(skip_all, err)]
async fn get_workspace_member_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<(Uuid, i64)>,
) -> Result<JsonAppResponse<AFWorkspaceMember>> {
  let (workspace_id, user_uuid_to_retrieved) = path.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  // Guest users can not get workspace members
  state
    .workspace_access_control
    .enforce_role(&uid, &workspace_id.to_string(), AFRole::Member)
    .await?;
  let member_row =
    workspace::ops::get_workspace_member(&user_uuid_to_retrieved, &state.pg_pool, &workspace_id)
      .await?;
  let member = AFWorkspaceMember {
    name: member_row.name,
    email: member_row.email,
    role: member_row.role,
    avatar_url: None,
  };

  Ok(AppResponse::Ok().with_data(member).into())
}

#[instrument(level = "debug", skip_all, err)]
async fn open_workspace_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  workspace_id: web::Path<Uuid>,
) -> Result<JsonAppResponse<AFWorkspace>> {
  let workspace_id = workspace_id.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id.to_string(), Action::Read)
    .await?;
  let workspace = workspace::ops::open_workspace(&state.pg_pool, &user_uuid, &workspace_id).await?;
  Ok(AppResponse::Ok().with_data(workspace).into())
}

#[instrument(level = "debug", skip_all, err)]
async fn leave_workspace_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  workspace_id: web::Path<Uuid>,
) -> Result<JsonAppResponse<()>> {
  let workspace_id = workspace_id.into_inner();
  workspace::ops::leave_workspace(
    &state.pg_pool,
    &workspace_id,
    &user_uuid,
    state.workspace_access_control.clone(),
  )
  .await?;
  Ok(AppResponse::Ok().into())
}

#[instrument(level = "debug", skip_all, err)]
async fn update_workspace_member_handler(
  user_uuid: UserUuid,
  payload: Json<WorkspaceMemberChangeset>,
  state: Data<AppState>,
  workspace_id: web::Path<Uuid>,
) -> Result<JsonAppResponse<()>> {
  let workspace_id = workspace_id.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_role(&uid, &workspace_id.to_string(), AFRole::Owner)
    .await?;

  let changeset = payload.into_inner();

  if changeset.role.is_some() {
    let changeset_uid = select_uid_from_email(&state.pg_pool, &changeset.email)
      .await
      .map_err(AppResponseError::from)?;
    workspace::ops::update_workspace_member(
      &changeset_uid,
      &state.pg_pool,
      &workspace_id,
      &changeset,
      state.workspace_access_control.clone(),
    )
    .await?;
  }

  Ok(AppResponse::Ok().into())
}

#[instrument(skip(state, payload))]
async fn create_collab_handler(
  user_uuid: UserUuid,
  payload: Bytes,
  state: Data<AppState>,
  req: HttpRequest,
) -> Result<Json<AppResponse<()>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let params = match req.headers().get(X_COMPRESSION_TYPE) {
    None => serde_json::from_slice::<CreateCollabParams>(&payload).map_err(|err| {
      AppError::InvalidRequest(format!(
        "Failed to parse CreateCollabParams from JSON: {}",
        err
      ))
    })?,
    Some(_) => match compress_type_from_header_value(req.headers())? {
      CompressionType::Brotli { buffer_size } => {
        let decompress_data = blocking_decompress(payload.to_vec(), buffer_size).await?;
        CreateCollabParams::from_bytes(&decompress_data).map_err(|err| {
          AppError::InvalidRequest(format!(
            "Failed to parse CreateCollabParams with brotli decompression data: {}",
            err
          ))
        })?
      },
    },
  };

  let (params, workspace_id) = params.split();

  if params.object_id == workspace_id {
    // Only the object with [CollabType::Folder] can have the same object_id as workspace_id. But
    // it should use create workspace API
    return Err(
      AppError::InvalidRequest("object_id cannot be the same as workspace_id".to_string()).into(),
    );
  }

  let collab = collab_from_encode_collab(&params.object_id, &params.encoded_collab_v1)
    .await
    .map_err(|err| {
      AppError::NoRequiredData(format!(
        "Failed to create collab from encoded collab: {}",
        err
      ))
    })?;

  if let Err(err) = params.collab_type.validate_require_data(&collab) {
    return Err(
      AppError::NoRequiredData(format!(
        "collab doc state is not correct:{},{}",
        params.object_id, err
      ))
      .into(),
    );
  }

  if state
    .indexer_scheduler
    .can_index_workspace(&workspace_id)
    .await?
  {
    if let Ok(paragraphs) = Document::open(collab).map(|doc| doc.paragraphs()) {
      let workspace_id_uuid =
        Uuid::parse_str(&workspace_id).map_err(|err| AppError::Internal(err.into()))?;
      let pending = UnindexedCollabTask::new(
        workspace_id_uuid,
        params.object_id.clone(),
        params.collab_type.clone(),
        UnindexedData::Paragraphs(paragraphs),
      );
      state
        .indexer_scheduler
        .index_pending_collab_one(pending, false)?;
    }
  }

  let mut transaction = state
    .pg_pool
    .begin()
    .await
    .context("acquire transaction to upsert collab")
    .map_err(AppError::from)?;
  let start = Instant::now();

  let action = format!("Create new collab: {}", params);
  state
    .collab_access_control_storage
    .upsert_new_collab_with_transaction(&workspace_id, &uid, params, &mut transaction, &action)
    .await?;

  transaction
    .commit()
    .await
    .context("fail to commit the transaction to upsert collab")
    .map_err(AppError::from)?;
  state.metrics.collab_metrics.observe_pg_tx(start.elapsed());

  Ok(Json(AppResponse::Ok()))
}

#[instrument(skip(state, payload), err)]
async fn batch_create_collab_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  mut payload: Payload,
  state: Data<AppState>,
  req: HttpRequest,
) -> Result<Json<AppResponse<()>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let workspace_id_uuid = workspace_id.into_inner();
  let workspace_id = workspace_id_uuid.to_string();
  let compress_type = compress_type_from_header_value(req.headers())?;
  event!(tracing::Level::DEBUG, "start decompressing collab list");

  let mut payload_buffer = Vec::new();
  let mut offset_len_list = Vec::new();
  let mut current_offset = 0;
  let start = Instant::now();
  while let Some(item) = payload.next().await {
    if let Ok(bytes) = item {
      payload_buffer.extend_from_slice(&bytes);
      while current_offset + 4 <= payload_buffer.len() {
        // The length of the next frame is determined by the first 4 bytes
        let size = u32::from_be_bytes([
          payload_buffer[current_offset],
          payload_buffer[current_offset + 1],
          payload_buffer[current_offset + 2],
          payload_buffer[current_offset + 3],
        ]) as usize;

        // Ensure there is enough data for the frame (4 bytes for size + `size` bytes for data)
        if current_offset + 4 + size > payload_buffer.len() {
          break;
        }

        // Collect the (offset, len) for the current frame (data starts at current_offset + 4)
        offset_len_list.push((current_offset + 4, size));
        current_offset += 4 + size;
      }
    }
  }
  // Perform decompression and processing in a Rayon thread pool
  let mut collab_params_list = tokio::task::spawn_blocking(move || match compress_type {
    CompressionType::Brotli { buffer_size } => offset_len_list
      .into_par_iter()
      .filter_map(|(offset, len)| {
        let compressed_data = &payload_buffer[offset..offset + len];
        match decompress(compressed_data.to_vec(), buffer_size) {
          Ok(decompressed_data) => {
            let params = CollabParams::from_bytes(&decompressed_data).ok()?;
            if params.validate().is_ok() {
              let encoded_collab =
                EncodedCollab::decode_from_bytes(&params.encoded_collab_v1).ok()?;
              let collab = Collab::new_with_source(
                CollabOrigin::Empty,
                &params.object_id,
                DataSource::DocStateV1(encoded_collab.doc_state.to_vec()),
                vec![],
                false,
              )
              .ok()?;

              match params.collab_type.validate_require_data(&collab) {
                Ok(_) => {
                  match params.collab_type {
                    CollabType::Document => {
                      let index_text = Document::open(collab).map(|doc| doc.paragraphs());
                      Some((Some(index_text), params))
                    },
                    _ => {
                      // TODO(nathan): support other types
                      Some((None, params))
                    },
                  }
                },
                Err(_) => None,
              }
            } else {
              None
            }
          },
          Err(err) => {
            error!("Failed to decompress data: {:?}", err);
            None
          },
        }
      })
      .collect::<Vec<_>>(),
  })
  .await
  .map_err(|_| AppError::InvalidRequest("Failed to decompress data".to_string()))?;

  if collab_params_list.is_empty() {
    return Err(AppError::InvalidRequest("Empty collab params list".to_string()).into());
  }

  let total_size = collab_params_list
    .iter()
    .fold(0, |acc, x| acc + x.1.encoded_collab_v1.len());
  tracing::info!(
    "decompressed {} collab objects in {:?}",
    collab_params_list.len(),
    start.elapsed()
  );

  let mut pending_undexed_collabs = vec![];
  if state
    .indexer_scheduler
    .can_index_workspace(&workspace_id)
    .await?
  {
    pending_undexed_collabs = collab_params_list
      .iter_mut()
      .filter(|p| {
        state
          .indexer_scheduler
          .is_indexing_enabled(&p.1.collab_type)
      })
      .flat_map(|value| match std::mem::take(&mut value.0) {
        None => None,
        Some(text) => text
          .map(|paragraphs| {
            UnindexedCollabTask::new(
              workspace_id_uuid,
              value.1.object_id.clone(),
              value.1.collab_type.clone(),
              UnindexedData::Paragraphs(paragraphs),
            )
          })
          .ok(),
      })
      .collect::<Vec<_>>();
  }

  let collab_params_list = collab_params_list
    .into_iter()
    .map(|(_, params)| params)
    .collect::<Vec<_>>();

  let start = Instant::now();
  state
    .collab_access_control_storage
    .batch_insert_new_collab(&workspace_id, &uid, collab_params_list)
    .await?;

  tracing::info!(
    "inserted collab objects to disk in {:?}, total size:{}",
    start.elapsed(),
    total_size
  );

  // Must after batch_insert_new_collab
  if !pending_undexed_collabs.is_empty() {
    state
      .indexer_scheduler
      .index_pending_collabs(pending_undexed_collabs)?;
  }

  Ok(Json(AppResponse::Ok()))
}

// Deprecated
async fn get_collab_handler(
  user_uuid: UserUuid,
  payload: Json<QueryCollabParams>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<CollabResponse>>> {
  let uid = state
    .user_cache
    .get_user_uid(&user_uuid)
    .await
    .map_err(AppResponseError::from)?;
  let params = payload.into_inner();
  let object_id = params.object_id.clone();
  let encode_collab = state
    .collab_access_control_storage
    .get_encode_collab(GetCollabOrigin::User { uid }, params, true)
    .await
    .map_err(AppResponseError::from)?;

  let resp = CollabResponse {
    encode_collab,
    object_id,
  };

  Ok(Json(AppResponse::Ok().with_data(resp)))
}

async fn v1_get_collab_handler(
  user_uuid: UserUuid,
  path: web::Path<(String, String)>,
  query: web::Query<CollabTypeParam>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<CollabResponse>>> {
  let (workspace_id, object_id) = path.into_inner();
  let collab_type = query.into_inner().collab_type;
  let uid = state
    .user_cache
    .get_user_uid(&user_uuid)
    .await
    .map_err(AppResponseError::from)?;

  let param = QueryCollabParams {
    workspace_id,
    inner: QueryCollab {
      object_id: object_id.clone(),
      collab_type,
    },
  };

  let encode_collab = state
    .collab_access_control_storage
    .get_encode_collab(GetCollabOrigin::User { uid }, param, true)
    .await
    .map_err(AppResponseError::from)?;

  let resp = CollabResponse {
    encode_collab,
    object_id,
  };

  Ok(Json(AppResponse::Ok().with_data(resp)))
}

#[instrument(level = "debug", skip_all)]
async fn post_web_update_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, Uuid)>,
  payload: Json<UpdateCollabWebParams>,
  state: Data<AppState>,
  server: Data<RealtimeServerAddr>,
  req: HttpRequest,
) -> Result<Json<AppResponse<()>>> {
  let uid = state
    .user_cache
    .get_user_uid(&user_uuid)
    .await
    .map_err(AppResponseError::from)?;
  let (workspace_id, object_id) = path.into_inner();
  state
    .collab_access_control
    .enforce_action(
      &workspace_id.to_string(),
      &uid,
      &object_id.to_string(),
      Action::Write,
    )
    .await?;
  let user = realtime_user_for_web_request(req.headers(), uid)?;
  trace!("create onetime web realtime user: {}", user);

  let payload = payload.into_inner();
  let collab_type = payload.collab_type.clone();

  update_page_collab_data(
    &state.metrics.appflowy_web_metrics,
    server,
    user,
    workspace_id,
    object_id,
    collab_type,
    payload.doc_state,
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn post_space_handler(
  user_uuid: UserUuid,
  path: web::Path<Uuid>,
  payload: Json<CreateSpaceParams>,
  state: Data<AppState>,
  server: Data<RealtimeServerAddr>,
  req: HttpRequest,
) -> Result<Json<AppResponse<Space>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let workspace_uuid = path.into_inner();
  let user = realtime_user_for_web_request(req.headers(), uid)?;
  let space = create_space(
    &state.metrics.appflowy_web_metrics,
    server,
    user,
    &state.pg_pool,
    &state.collab_access_control_storage,
    workspace_uuid,
    &payload.space_permission,
    &payload.name,
    &payload.space_icon,
    &payload.space_icon_color,
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(space)))
}

async fn update_space_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, String)>,
  payload: Json<UpdateSpaceParams>,
  state: Data<AppState>,
  server: Data<RealtimeServerAddr>,
  req: HttpRequest,
) -> Result<Json<AppResponse<Space>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let (workspace_uuid, view_id) = path.into_inner();
  let user = realtime_user_for_web_request(req.headers(), uid)?;
  update_space(
    &state.metrics.appflowy_web_metrics,
    server,
    user,
    &state.collab_access_control_storage,
    workspace_uuid,
    &view_id,
    &payload.space_permission,
    &payload.name,
    &payload.space_icon,
    &payload.space_icon_color,
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn post_page_view_handler(
  user_uuid: UserUuid,
  path: web::Path<Uuid>,
  payload: Json<CreatePageParams>,
  state: Data<AppState>,
  server: Data<RealtimeServerAddr>,
  req: HttpRequest,
) -> Result<Json<AppResponse<Page>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let workspace_uuid = path.into_inner();
  let user = realtime_user_for_web_request(req.headers(), uid)?;
  let page = create_page(
    &state.metrics.appflowy_web_metrics,
    server,
    user,
    &state.pg_pool,
    &state.collab_access_control_storage,
    workspace_uuid,
    &payload.parent_view_id,
    &payload.layout,
    payload.name.as_deref(),
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(page)))
}

async fn move_page_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, String)>,
  payload: Json<MovePageParams>,
  state: Data<AppState>,
  server: Data<RealtimeServerAddr>,
  req: HttpRequest,
) -> Result<Json<AppResponse<()>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let (workspace_uuid, view_id) = path.into_inner();
  let user = realtime_user_for_web_request(req.headers(), uid)?;
  move_page(
    &state.metrics.appflowy_web_metrics,
    server,
    user,
    &state.collab_access_control_storage,
    workspace_uuid,
    &view_id,
    &payload.new_parent_view_id,
    payload.prev_view_id.clone(),
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn move_page_to_trash_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, String)>,
  state: Data<AppState>,
  server: Data<RealtimeServerAddr>,
  req: HttpRequest,
) -> Result<Json<AppResponse<()>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let (workspace_uuid, view_id) = path.into_inner();
  let user = realtime_user_for_web_request(req.headers(), uid)?;
  move_page_to_trash(
    &state.metrics.appflowy_web_metrics,
    server,
    user,
    &state.collab_access_control_storage,
    workspace_uuid,
    &view_id,
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn restore_page_from_trash_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, String)>,
  state: Data<AppState>,
  server: Data<RealtimeServerAddr>,
  req: HttpRequest,
) -> Result<Json<AppResponse<()>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let (workspace_uuid, view_id) = path.into_inner();
  let user = realtime_user_for_web_request(req.headers(), uid)?;
  restore_page_from_trash(
    &state.metrics.appflowy_web_metrics,
    server,
    user,
    &state.collab_access_control_storage,
    workspace_uuid,
    &view_id,
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn restore_all_pages_from_trash_handler(
  user_uuid: UserUuid,
  path: web::Path<Uuid>,
  state: Data<AppState>,
  server: Data<RealtimeServerAddr>,
  req: HttpRequest,
) -> Result<Json<AppResponse<()>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let workspace_uuid = path.into_inner();
  let user = realtime_user_for_web_request(req.headers(), uid)?;
  restore_all_pages_from_trash(
    &state.metrics.appflowy_web_metrics,
    server,
    user,
    &state.collab_access_control_storage,
    workspace_uuid,
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn delete_page_from_trash_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, String)>,
  state: Data<AppState>,
  server: Data<RealtimeServerAddr>,
  req: HttpRequest,
) -> Result<Json<AppResponse<()>>> {
  let uid = state
    .user_cache
    .get_user_uid(&user_uuid)
    .await
    .map_err(AppResponseError::from)?;
  let (workspace_id, view_id) = path.into_inner();
  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id.to_string(), Action::Write)
    .await?;
  let user = realtime_user_for_web_request(req.headers(), uid)?;
  delete_trash(
    &state.metrics.appflowy_web_metrics,
    server,
    user,
    &state.collab_access_control_storage,
    workspace_id,
    &view_id,
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn delete_all_pages_from_trash_handler(
  user_uuid: UserUuid,
  path: web::Path<Uuid>,
  state: Data<AppState>,
  server: Data<RealtimeServerAddr>,
  req: HttpRequest,
) -> Result<Json<AppResponse<()>>> {
  let uid = state
    .user_cache
    .get_user_uid(&user_uuid)
    .await
    .map_err(AppResponseError::from)?;
  let workspace_id = path.into_inner();
  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id.to_string(), Action::Write)
    .await?;
  let user = realtime_user_for_web_request(req.headers(), uid)?;
  delete_all_pages_from_trash(
    &state.metrics.appflowy_web_metrics,
    server,
    user,
    &state.collab_access_control_storage,
    workspace_id,
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn publish_page_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, String)>,
  payload: Json<PublishPageParams>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<()>>> {
  let (workspace_id, view_id) = path.into_inner();
  let uid = state
    .user_cache
    .get_user_uid(&user_uuid)
    .await
    .map_err(AppResponseError::from)?;
  state
    .workspace_access_control
    .enforce_role(&uid, &workspace_id.to_string(), AFRole::Member)
    .await?;
  let PublishPageParams {
    publish_name,
    visible_database_view_ids,
    comments_enabled,
    duplicate_enabled,
  } = payload.into_inner();
  publish_page(
    &state.pg_pool,
    &state.collab_access_control_storage,
    state.published_collab_store.as_ref(),
    uid,
    *user_uuid,
    workspace_id,
    &view_id,
    visible_database_view_ids,
    publish_name,
    comments_enabled.unwrap_or(true),
    duplicate_enabled.unwrap_or(true),
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn unpublish_page_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, Uuid)>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<()>>> {
  let (workspace_uuid, view_uuid) = path.into_inner();
  let uid = state
    .user_cache
    .get_user_uid(&user_uuid)
    .await
    .map_err(AppResponseError::from)?;
  state
    .workspace_access_control
    .enforce_role(&uid, &workspace_uuid.to_string(), AFRole::Member)
    .await?;
  unpublish_page(
    state.published_collab_store.as_ref(),
    workspace_uuid,
    *user_uuid,
    view_uuid,
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn update_page_view_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, String)>,
  payload: Json<UpdatePageParams>,
  state: Data<AppState>,
  server: Data<RealtimeServerAddr>,
  req: HttpRequest,
) -> Result<Json<AppResponse<()>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let (workspace_uuid, view_id) = path.into_inner();
  let icon = payload.icon.as_ref();
  let extra = payload
    .extra
    .as_ref()
    .map(|json_value| json_value.to_string());
  let user = realtime_user_for_web_request(req.headers(), uid)?;
  update_page(
    &state.metrics.appflowy_web_metrics,
    server,
    user,
    &state.collab_access_control_storage,
    workspace_uuid,
    &view_id,
    &payload.name,
    icon,
    extra.as_ref(),
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn get_page_view_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, String)>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<PageCollab>>> {
  let (workspace_uuid, view_id) = path.into_inner();
  let uid = state
    .user_cache
    .get_user_uid(&user_uuid)
    .await
    .map_err(AppResponseError::from)?;

  let page_collab = get_page_view_collab(
    &state.pg_pool,
    &state.collab_access_control_storage,
    uid,
    workspace_uuid,
    &view_id,
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(page_collab)))
}

#[instrument(level = "trace", skip_all, err)]
async fn get_collab_snapshot_handler(
  payload: Json<QuerySnapshotParams>,
  path: web::Path<(String, String)>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<SnapshotData>>> {
  let (workspace_id, object_id) = path.into_inner();
  let data = state
    .collab_access_control_storage
    .get_collab_snapshot(&workspace_id.to_string(), &object_id, &payload.snapshot_id)
    .await
    .map_err(AppResponseError::from)?;

  Ok(Json(AppResponse::Ok().with_data(data)))
}

#[instrument(level = "trace", skip_all, err)]
async fn create_collab_snapshot_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  payload: Json<CollabType>,
) -> Result<Json<AppResponse<AFSnapshotMeta>>> {
  let (workspace_id, object_id) = path.into_inner();
  let collab_type = payload.into_inner();
  let uid = state
    .user_cache
    .get_user_uid(&user_uuid)
    .await
    .map_err(AppResponseError::from)?;
  let data = state
    .collab_access_control_storage
    .get_encode_collab(
      GetCollabOrigin::User { uid },
      QueryCollabParams::new(&object_id, collab_type.clone(), &workspace_id),
      true,
    )
    .await?
    .doc_state;

  let meta = state
    .collab_access_control_storage
    .create_snapshot(InsertSnapshotParams {
      object_id,
      workspace_id,
      doc_state: data,
      collab_type,
    })
    .await?;

  Ok(Json(AppResponse::Ok().with_data(meta)))
}

#[instrument(level = "trace", skip(path, state), err)]
async fn get_all_collab_snapshot_list_handler(
  _user_uuid: UserUuid,
  path: web::Path<(String, String)>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<AFSnapshotMetas>>> {
  let (workspace_id, object_id) = path.into_inner();
  let data = state
    .collab_access_control_storage
    .get_collab_snapshot_list(&workspace_id, &object_id)
    .await
    .map_err(AppResponseError::from)?;
  Ok(Json(AppResponse::Ok().with_data(data)))
}

#[instrument(level = "debug", skip(payload, state), err)]
async fn batch_get_collab_handler(
  user_uuid: UserUuid,
  path: Path<String>,
  state: Data<AppState>,
  payload: Json<BatchQueryCollabParams>,
) -> Result<Json<AppResponse<BatchQueryCollabResult>>> {
  let workspace_id = path.into_inner();
  let uid = state
    .user_cache
    .get_user_uid(&user_uuid)
    .await
    .map_err(AppResponseError::from)?;
  let result = BatchQueryCollabResult(
    state
      .collab_access_control_storage
      .batch_get_collab(&uid, &workspace_id, payload.into_inner().0, false)
      .await,
  );
  Ok(Json(AppResponse::Ok().with_data(result)))
}

#[instrument(skip(state, payload), err)]
async fn update_collab_handler(
  user_uuid: UserUuid,
  payload: Json<CreateCollabParams>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<()>>> {
  let (params, workspace_id) = payload.into_inner().split();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;

  let create_params = CreateCollabParams::from((workspace_id.to_string(), params));
  let (params, workspace_id) = create_params.split();
  if state
    .indexer_scheduler
    .can_index_workspace(&workspace_id)
    .await?
  {
    let workspace_id_uuid =
      Uuid::parse_str(&workspace_id).map_err(|err| AppError::Internal(err.into()))?;

    match params.collab_type {
      CollabType::Document => {
        let collab = collab_from_encode_collab(&params.object_id, &params.encoded_collab_v1)
          .await
          .map_err(|err| {
            AppError::InvalidRequest(format!(
              "Failed to create collab from encoded collab: {}",
              err
            ))
          })?;
        params
          .collab_type
          .validate_require_data(&collab)
          .map_err(|err| {
            AppError::NoRequiredData(format!(
              "collab doc state is not correct:{},{}",
              params.object_id, err
            ))
          })?;

        if let Ok(paragraphs) = Document::open(collab).map(|doc| doc.paragraphs()) {
          if !paragraphs.is_empty() {
            let pending = UnindexedCollabTask::new(
              workspace_id_uuid,
              params.object_id.clone(),
              params.collab_type.clone(),
              UnindexedData::Paragraphs(paragraphs),
            );
            state
              .indexer_scheduler
              .index_pending_collab_one(pending, true)?;
          }
        }
      },
      _ => {
        // TODO(nathan): support other collab type
      },
    }
  }

  state
    .collab_access_control_storage
    .queue_insert_or_update_collab(&workspace_id, &uid, params, false)
    .await?;
  Ok(AppResponse::Ok().into())
}

#[instrument(level = "info", skip(state, payload), err)]
async fn delete_collab_handler(
  user_uuid: UserUuid,
  payload: Json<DeleteCollabParams>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<()>>> {
  let payload = payload.into_inner();
  payload.validate().map_err(AppError::from)?;

  let uid = state
    .user_cache
    .get_user_uid(&user_uuid)
    .await
    .map_err(AppResponseError::from)?;

  state
    .collab_access_control_storage
    .delete_collab(&payload.workspace_id, &uid, &payload.object_id)
    .await
    .map_err(AppResponseError::from)?;

  Ok(AppResponse::Ok().into())
}

#[instrument(level = "debug", skip(state, payload), err)]
async fn add_collab_member_handler(
  payload: Json<InsertCollabMemberParams>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<()>>> {
  let payload = payload.into_inner();
  if !state
    .collab_cache
    .is_exist(&payload.workspace_id, &payload.object_id)
    .await?
  {
    return Err(
      AppError::RecordNotFound(format!(
        "Fail to insert collab member. The Collab with object_id {} does not exist",
        payload.object_id
      ))
      .into(),
    );
  }

  biz::collab::ops::create_collab_member(
    &state.pg_pool,
    &payload,
    state.collab_access_control.clone(),
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

#[instrument(level = "debug", skip(state, payload), err)]
async fn update_collab_member_handler(
  user_uuid: UserUuid,
  payload: Json<UpdateCollabMemberParams>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<()>>> {
  let payload = payload.into_inner();

  if !state
    .collab_cache
    .is_exist(&payload.workspace_id, &payload.object_id)
    .await?
  {
    return Err(
      AppError::RecordNotFound(format!(
        "Fail to update collab member. The Collab with object_id {} does not exist",
        payload.object_id
      ))
      .into(),
    );
  }
  biz::collab::ops::upsert_collab_member(
    &state.pg_pool,
    &user_uuid,
    &payload,
    state.collab_access_control.clone(),
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}
#[instrument(level = "debug", skip(state, payload), err)]
async fn get_collab_member_handler(
  payload: Json<WorkspaceCollabIdentify>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<AFCollabMember>>> {
  let payload = payload.into_inner();
  let member = biz::collab::ops::get_collab_member(&state.pg_pool, &payload).await?;
  Ok(Json(AppResponse::Ok().with_data(member)))
}

#[instrument(skip(state, payload), err)]
async fn remove_collab_member_handler(
  payload: Json<WorkspaceCollabIdentify>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<()>>> {
  let payload = payload.into_inner();
  biz::collab::ops::delete_collab_member(
    &state.pg_pool,
    &payload,
    state.collab_access_control.clone(),
  )
  .await?;

  Ok(Json(AppResponse::Ok()))
}

async fn put_workspace_default_published_view_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  payload: Json<UpdateDefaultPublishView>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<()>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_role(&uid, &workspace_id.to_string(), AFRole::Owner)
    .await?;
  let new_default_pub_view_id = payload.into_inner().view_id;
  biz::workspace::publish::set_workspace_default_publish_view(
    &state.pg_pool,
    &workspace_id,
    &new_default_pub_view_id,
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn delete_workspace_default_published_view_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<()>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_role(&uid, &workspace_id.to_string(), AFRole::Owner)
    .await?;
  biz::workspace::publish::unset_workspace_default_publish_view(&state.pg_pool, &workspace_id)
    .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn get_workspace_published_default_info_handler(
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<PublishInfo>>> {
  let workspace_id = workspace_id.into_inner();
  let info =
    biz::workspace::publish::get_workspace_default_publish_view_info(&state.pg_pool, &workspace_id)
      .await?;
  Ok(Json(AppResponse::Ok().with_data(info)))
}

async fn put_publish_namespace_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  payload: Json<UpdatePublishNamespace>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<()>>> {
  let workspace_id = workspace_id.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_role(&uid, &workspace_id.to_string(), AFRole::Owner)
    .await?;
  let UpdatePublishNamespace {
    old_namespace,
    new_namespace,
  } = payload.into_inner();
  biz::workspace::publish::set_workspace_namespace(
    &state.pg_pool,
    &workspace_id,
    &old_namespace,
    &new_namespace,
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn get_publish_namespace_handler(
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<String>>> {
  let workspace_id = workspace_id.into_inner();
  let namespace =
    biz::workspace::publish::get_workspace_publish_namespace(&state.pg_pool, &workspace_id).await?;
  Ok(Json(AppResponse::Ok().with_data(namespace)))
}

async fn get_default_published_collab_info_meta_handler(
  publish_namespace: web::Path<String>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<PublishInfoMeta<serde_json::Value>>>> {
  let publish_namespace = publish_namespace.into_inner();
  let (info, meta) =
    get_workspace_default_publish_view_info_meta(&state.pg_pool, &publish_namespace).await?;
  Ok(Json(
    AppResponse::Ok().with_data(PublishInfoMeta { info, meta }),
  ))
}

async fn get_v1_published_collab_handler(
  path_param: web::Path<(String, String)>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<serde_json::Value>>> {
  let (workspace_namespace, publish_name) = path_param.into_inner();
  let metadata = state
    .published_collab_store
    .get_collab_metadata(&workspace_namespace, &publish_name)
    .await?;
  Ok(Json(AppResponse::Ok().with_data(metadata)))
}

async fn get_published_collab_blob_handler(
  path_param: web::Path<(String, String)>,
  state: Data<AppState>,
) -> Result<Vec<u8>> {
  let (publish_namespace, publish_name) = path_param.into_inner();
  let collab_data = state
    .published_collab_store
    .get_collab_blob_by_publish_namespace(&publish_namespace, &publish_name)
    .await?;
  Ok(collab_data)
}

async fn post_published_duplicate_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<String>,
  state: Data<AppState>,
  params: Json<PublishedDuplicate>,
) -> Result<Json<AppResponse<DuplicatePublishedPageResponse>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id.to_string(), Action::Write)
    .await?;
  let params = params.into_inner();
  let root_view_id_for_duplicate =
    biz::workspace::publish_dup::duplicate_published_collab_to_workspace(
      &state.pg_pool,
      state.bucket_client.clone(),
      state.collab_access_control_storage.clone(),
      uid,
      params.published_view_id,
      workspace_id.into_inner(),
      params.dest_view_id,
    )
    .await?;

  Ok(Json(AppResponse::Ok().with_data(
    DuplicatePublishedPageResponse {
      view_id: root_view_id_for_duplicate,
    },
  )))
}

async fn list_published_collab_info_handler(
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<Vec<PublishInfoView>>>> {
  let publish_infos = biz::workspace::publish::list_collab_publish_info(
    state.published_collab_store.as_ref(),
    &state.collab_access_control_storage,
    &workspace_id.into_inner(),
  )
  .await?;

  Ok(Json(AppResponse::Ok().with_data(publish_infos)))
}

// Deprecated since 0.7.4
async fn get_published_collab_info_handler(
  view_id: web::Path<Uuid>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<PublishInfo>>> {
  let view_id = view_id.into_inner();
  let collab_data = state
    .published_collab_store
    .get_collab_publish_info(&view_id)
    .await?;
  if collab_data.unpublished_timestamp.is_some() {
    return Err(AppError::RecordNotFound("Collab is unpublished".to_string()).into());
  }
  Ok(Json(AppResponse::Ok().with_data(collab_data)))
}

async fn get_v1_published_collab_info_handler(
  view_id: web::Path<Uuid>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<PublishInfo>>> {
  let view_id = view_id.into_inner();
  let collab_data = state
    .published_collab_store
    .get_collab_publish_info(&view_id)
    .await?;
  Ok(Json(AppResponse::Ok().with_data(collab_data)))
}

async fn get_published_collab_comment_handler(
  view_id: web::Path<Uuid>,
  optional_user_uuid: OptionalUserUuid,
  state: Data<AppState>,
) -> Result<JsonAppResponse<GlobalComments>> {
  let view_id = view_id.into_inner();
  let comments =
    get_comments_on_published_view(&state.pg_pool, &view_id, &optional_user_uuid).await?;
  let resp = GlobalComments { comments };
  Ok(Json(AppResponse::Ok().with_data(resp)))
}

async fn post_published_collab_comment_handler(
  user_uuid: UserUuid,
  view_id: web::Path<Uuid>,
  state: Data<AppState>,
  data: Json<CreateGlobalCommentParams>,
) -> Result<JsonAppResponse<()>> {
  let view_id = view_id.into_inner();
  create_comment_on_published_view(
    &state.pg_pool,
    &view_id,
    &data.reply_comment_id,
    &data.content,
    &user_uuid,
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn delete_published_collab_comment_handler(
  user_uuid: UserUuid,
  view_id: web::Path<Uuid>,
  state: Data<AppState>,
  data: Json<DeleteGlobalCommentParams>,
) -> Result<JsonAppResponse<()>> {
  let view_id = view_id.into_inner();
  remove_comment_on_published_view(&state.pg_pool, &view_id, &data.comment_id, &user_uuid).await?;
  Ok(Json(AppResponse::Ok()))
}

async fn get_published_collab_reaction_handler(
  view_id: web::Path<Uuid>,
  query: web::Query<GetReactionQueryParams>,
  state: Data<AppState>,
) -> Result<JsonAppResponse<Reactions>> {
  let view_id = view_id.into_inner();
  let reactions =
    get_reactions_on_published_view(&state.pg_pool, &view_id, &query.comment_id).await?;
  let resp = Reactions { reactions };
  Ok(Json(AppResponse::Ok().with_data(resp)))
}

async fn post_published_collab_reaction_handler(
  user_uuid: UserUuid,
  view_id: web::Path<Uuid>,
  data: Json<CreateReactionParams>,
  state: Data<AppState>,
) -> Result<JsonAppResponse<()>> {
  let view_id = view_id.into_inner();
  create_reaction_on_comment(
    &state.pg_pool,
    &data.comment_id,
    &view_id,
    &data.reaction_type,
    &user_uuid,
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn delete_published_collab_reaction_handler(
  user_uuid: UserUuid,
  data: Json<DeleteReactionParams>,
  state: Data<AppState>,
) -> Result<JsonAppResponse<()>> {
  remove_reaction_on_comment(
    &state.pg_pool,
    &data.comment_id,
    &data.reaction_type,
    &user_uuid,
  )
  .await?;
  Ok(Json(AppResponse::Ok()))
}

// FIXME: This endpoint currently has a different behaviour from the publish page endpoint,
// as it doesn't accept parameters. We will need to deprecate this endpoint and use a new
// one that accepts parameters.
async fn post_publish_collabs_handler(
  workspace_id: web::Path<Uuid>,
  user_uuid: UserUuid,
  payload: Payload,
  state: Data<AppState>,
) -> Result<Json<AppResponse<()>>> {
  let workspace_id = workspace_id.into_inner();

  let mut accumulator = Vec::<PublishCollabItem<serde_json::Value, Vec<u8>>>::new();
  let mut payload_reader: PayloadReader = PayloadReader::new(payload);

  loop {
    let meta: PublishCollabMetadata<serde_json::Value> = {
      let meta_len = payload_reader.read_u32_little_endian().await?;
      if meta_len > 4 * 1024 * 1024 {
        // 4MiB Limit for metadata
        return Err(AppError::InvalidRequest(String::from("metadata too large")).into());
      }
      if meta_len == 0 {
        break;
      }

      let mut meta_buffer = vec![0; meta_len as usize];
      payload_reader.read_exact(&mut meta_buffer).await?;
      serde_json::from_slice(&meta_buffer)?
    };

    let data = {
      let data_len = payload_reader.read_u32_little_endian().await?;
      if data_len > 32 * 1024 * 1024 {
        // 32MiB Limit for data
        return Err(AppError::InvalidRequest(String::from("data too large")).into());
      }
      let mut data_buffer = vec![0; data_len as usize];
      payload_reader.read_exact(&mut data_buffer).await?;
      data_buffer
    };

    // Set comments_enabled and duplicate_enabled to true by default, as this is the default
    // behaviour for the older web version.
    accumulator.push(PublishCollabItem {
      meta,
      data,
      comments_enabled: true,
      duplicate_enabled: true,
    });
  }

  if accumulator.is_empty() {
    return Err(
      AppError::InvalidRequest(String::from("did not receive any data to publish")).into(),
    );
  }
  state
    .published_collab_store
    .publish_collabs(accumulator, &workspace_id, &user_uuid)
    .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn patch_published_collabs_handler(
  workspace_id: web::Path<Uuid>,
  user_uuid: UserUuid,
  state: Data<AppState>,
  patches: Json<Vec<PatchPublishedCollab>>,
) -> Result<Json<AppResponse<()>>> {
  let workspace_id = workspace_id.into_inner();
  if patches.is_empty() {
    return Err(AppError::InvalidRequest("No patches provided".to_string()).into());
  }
  state
    .published_collab_store
    .patch_collabs(&workspace_id, &user_uuid, &patches)
    .await?;
  Ok(Json(AppResponse::Ok()))
}

async fn delete_published_collabs_handler(
  workspace_id: web::Path<Uuid>,
  user_uuid: UserUuid,
  state: Data<AppState>,
  view_ids: Json<Vec<Uuid>>,
) -> Result<Json<AppResponse<()>>> {
  let workspace_id = workspace_id.into_inner();
  let view_ids = view_ids.into_inner();
  if view_ids.is_empty() {
    return Err(AppError::InvalidRequest("No view_ids provided".to_string()).into());
  }
  state
    .published_collab_store
    .unpublish_collabs(&workspace_id, &view_ids, &user_uuid)
    .await?;
  Ok(Json(AppResponse::Ok()))
}

#[instrument(level = "debug", skip(state, payload), err)]
async fn get_collab_member_list_handler(
  payload: Json<QueryCollabMembers>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<AFCollabMembers>>> {
  let members =
    biz::collab::ops::get_collab_member_list(&state.pg_pool, &payload.into_inner()).await?;
  Ok(Json(AppResponse::Ok().with_data(AFCollabMembers(members))))
}

#[instrument(level = "info", skip_all, err)]
async fn post_realtime_message_stream_handler(
  user_uuid: UserUuid,
  mut payload: Payload,
  server: Data<RealtimeServerAddr>,
  state: Data<AppState>,
  req: HttpRequest,
) -> Result<Json<AppResponse<()>>> {
  let device_id = device_id_from_headers(req.headers())
    .map(|s| s.to_string())
    .unwrap_or_else(|_| "".to_string());
  let uid = state
    .user_cache
    .get_user_uid(&user_uuid)
    .await
    .map_err(AppResponseError::from)?;

  let mut bytes = BytesMut::new();
  while let Some(item) = payload.next().await {
    bytes.extend_from_slice(&item?);
  }

  let device_id = device_id.to_string();

  let message = parser_realtime_msg(bytes.freeze(), req.clone()).await?;
  let stream_message = ClientHttpStreamMessage {
    uid,
    device_id,
    message,
  };

  // When the server is under heavy load, try_send may fail. In client side, it will retry to send
  // the message later.
  match server.try_send(stream_message) {
    Ok(_) => return Ok(Json(AppResponse::Ok())),
    Err(err) => Err(
      AppError::Internal(anyhow!(
        "Failed to send message to websocket server, error:{}",
        err
      ))
      .into(),
    ),
  }
}

async fn get_workspace_usage_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<WorkspaceUsage>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_role(&uid, &workspace_id.to_string(), AFRole::Owner)
    .await?;
  let res =
    biz::workspace::ops::get_workspace_document_total_bytes(&state.pg_pool, &workspace_id).await?;
  Ok(Json(AppResponse::Ok().with_data(res)))
}

async fn get_workspace_folder_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
  server: Data<RealtimeServerAddr>,
  query: web::Query<QueryWorkspaceFolder>,
  req: HttpRequest,
) -> Result<Json<AppResponse<FolderView>>> {
  let depth = query.depth.unwrap_or(1);
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let user = realtime_user_for_web_request(req.headers(), uid)?;
  let workspace_id = workspace_id.into_inner();
  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id.to_string(), Action::Read)
    .await?;
  let root_view_id = if let Some(root_view_id) = query.root_view_id.as_ref() {
    root_view_id.to_string()
  } else {
    workspace_id.to_string()
  };
  let folder_view = biz::collab::ops::get_user_workspace_structure(
    &state.metrics.appflowy_web_metrics,
    server,
    &state.collab_access_control_storage,
    &state.pg_pool,
    user,
    workspace_id,
    depth,
    &root_view_id,
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(folder_view)))
}

async fn get_recent_views_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<RecentSectionItems>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let workspace_id = workspace_id.into_inner();
  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id.to_string(), Action::Read)
    .await?;
  let folder_views = get_user_recent_folder_views(
    &state.collab_access_control_storage,
    &state.pg_pool,
    uid,
    workspace_id,
  )
  .await?;
  let section_items = RecentSectionItems {
    views: folder_views,
  };
  Ok(Json(AppResponse::Ok().with_data(section_items)))
}

async fn get_favorite_views_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<FavoriteSectionItems>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let workspace_id = workspace_id.into_inner();
  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id.to_string(), Action::Read)
    .await?;
  let folder_views = get_user_favorite_folder_views(
    &state.collab_access_control_storage,
    &state.pg_pool,
    uid,
    workspace_id,
  )
  .await?;
  let section_items = FavoriteSectionItems {
    views: folder_views,
  };
  Ok(Json(AppResponse::Ok().with_data(section_items)))
}

async fn get_trash_views_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<TrashSectionItems>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let workspace_id = workspace_id.into_inner();
  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id.to_string(), Action::Read)
    .await?;
  let folder_views =
    get_user_trash_folder_views(&state.collab_access_control_storage, uid, workspace_id).await?;
  let section_items = TrashSectionItems {
    views: folder_views,
  };
  Ok(Json(AppResponse::Ok().with_data(section_items)))
}

async fn get_workspace_publish_outline_handler(
  publish_namespace: web::Path<String>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<PublishedView>>> {
  let published_view = biz::collab::ops::get_published_view(
    &state.collab_access_control_storage,
    publish_namespace.into_inner(),
    &state.pg_pool,
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(published_view)))
}

async fn list_database_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<String>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<Vec<AFDatabase>>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let workspace_id = workspace_id.into_inner();
  let dbs = biz::collab::ops::list_database(
    &state.pg_pool,
    &state.collab_access_control_storage,
    uid,
    workspace_id,
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(dbs)))
}

async fn list_database_row_id_handler(
  user_uuid: UserUuid,
  path_param: web::Path<(String, String)>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<Vec<AFDatabaseRow>>>> {
  let (workspace_id, db_id) = path_param.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;

  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id, Action::Read)
    .await?;

  let db_rows = biz::collab::ops::list_database_row_ids(
    &state.collab_access_control_storage,
    &workspace_id,
    &db_id,
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(db_rows)))
}

async fn post_database_row_handler(
  user_uuid: UserUuid,
  path_param: web::Path<(String, String)>,
  state: Data<AppState>,
  add_database_row: Json<AddDatatabaseRow>,
) -> Result<Json<AppResponse<String>>> {
  let (workspace_id, db_id) = path_param.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id, Action::Write)
    .await?;

  let AddDatatabaseRow { cells, document } = add_database_row.into_inner();

  let new_db_row_id = biz::collab::ops::insert_database_row(
    state.collab_access_control_storage.clone(),
    &state.pg_pool,
    &workspace_id,
    &db_id,
    uid,
    None,
    cells,
    document,
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(new_db_row_id)))
}

async fn put_database_row_handler(
  user_uuid: UserUuid,
  path_param: web::Path<(String, String)>,
  state: Data<AppState>,
  upsert_db_row: Json<UpsertDatatabaseRow>,
) -> Result<Json<AppResponse<String>>> {
  let (workspace_id, db_id) = path_param.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id, Action::Write)
    .await?;

  let UpsertDatatabaseRow {
    pre_hash,
    cells,
    document,
  } = upsert_db_row.into_inner();

  let row_id = {
    let mut hasher = Sha256::new();
    hasher.update(&workspace_id);
    hasher.update(&db_id);
    hasher.update(pre_hash);
    let hash = hasher.finalize();
    Uuid::from_bytes([
      // take 16 out of 32 bytes
      hash[0], hash[1], hash[2], hash[3], hash[4], hash[5], hash[6], hash[7], hash[8], hash[9],
      hash[10], hash[11], hash[12], hash[13], hash[14], hash[15],
    ])
  };
  let row_id_str = row_id.to_string();

  biz::collab::ops::upsert_database_row(
    state.collab_access_control_storage.clone(),
    &state.pg_pool,
    &workspace_id,
    &db_id,
    uid,
    &row_id_str,
    cells,
    document,
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(row_id_str)))
}

async fn get_database_fields_handler(
  user_uuid: UserUuid,
  path_param: web::Path<(String, String)>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<Vec<AFDatabaseField>>>> {
  let (workspace_id, db_id) = path_param.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id, Action::Read)
    .await?;

  let db_fields = biz::collab::ops::get_database_fields(
    &state.collab_access_control_storage,
    &workspace_id,
    &db_id,
  )
  .await?;

  Ok(Json(AppResponse::Ok().with_data(db_fields)))
}

async fn post_database_fields_handler(
  user_uuid: UserUuid,
  path_param: web::Path<(String, String)>,
  state: Data<AppState>,
  field: Json<AFInsertDatabaseField>,
) -> Result<Json<AppResponse<String>>> {
  let (workspace_id, db_id) = path_param.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id, Action::Write)
    .await?;

  let field_id = biz::collab::ops::add_database_field(
    uid,
    state.collab_access_control_storage.clone(),
    &state.pg_pool,
    &workspace_id,
    &db_id,
    field.into_inner(),
  )
  .await?;

  Ok(Json(AppResponse::Ok().with_data(field_id)))
}

async fn list_database_row_id_updated_handler(
  user_uuid: UserUuid,
  path_param: web::Path<(String, String)>,
  state: Data<AppState>,
  param: web::Query<ListDatabaseRowUpdatedParam>,
) -> Result<Json<AppResponse<Vec<DatabaseRowUpdatedItem>>>> {
  let (workspace_id, db_id) = path_param.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;

  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id, Action::Read)
    .await?;

  // Default to 1 hour ago
  let after: DateTime<Utc> = param
    .after
    .unwrap_or_else(|| Utc::now() - Duration::hours(1));

  let db_rows = biz::collab::ops::list_database_row_ids_updated(
    &state.collab_access_control_storage,
    &state.pg_pool,
    &workspace_id,
    &db_id,
    &after,
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(db_rows)))
}

async fn list_database_row_details_handler(
  user_uuid: UserUuid,
  path_param: web::Path<(String, String)>,
  state: Data<AppState>,
  param: web::Query<ListDatabaseRowDetailParam>,
) -> Result<Json<AppResponse<Vec<AFDatabaseRowDetail>>>> {
  let (workspace_id, db_id) = path_param.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let list_db_row_query = param.into_inner();
  let with_doc = list_db_row_query.with_doc.unwrap_or_default();
  let row_ids = list_db_row_query.into_ids();

  if let Err(e) = Uuid::parse_str(&workspace_id) {
    return Err(
      AppError::InvalidRequest(format!("invalid workspace id `{}`: {}", db_id, e)).into(),
    );
  }
  if let Err(e) = Uuid::parse_str(&db_id) {
    return Err(AppError::InvalidRequest(format!("invalid database id `{}`: {}", db_id, e)).into());
  }

  for id in row_ids.iter() {
    if let Err(e) = Uuid::parse_str(id) {
      return Err(AppError::InvalidRequest(format!("invalid row id `{}`: {}", id, e)).into());
    }
  }

  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id, Action::Read)
    .await?;

  static UNSUPPORTED_FIELD_TYPES: &[FieldType] = &[FieldType::Relation];

  let db_rows = biz::collab::ops::list_database_row_details(
    &state.collab_access_control_storage,
    uid,
    workspace_id,
    db_id,
    &row_ids,
    UNSUPPORTED_FIELD_TYPES,
    with_doc,
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(db_rows)))
}

#[inline]
async fn parser_realtime_msg(
  payload: Bytes,
  req: HttpRequest,
) -> Result<RealtimeMessage, AppError> {
  let HttpRealtimeMessage {
    device_id: _,
    payload,
  } =
    HttpRealtimeMessage::decode(payload.as_ref()).map_err(|err| AppError::Internal(err.into()))?;
  let payload = match req.headers().get(X_COMPRESSION_TYPE) {
    None => payload,
    Some(_) => match compress_type_from_header_value(req.headers())? {
      CompressionType::Brotli { buffer_size } => {
        let decompressed_data = blocking_decompress(payload, buffer_size).await?;
        event!(
          tracing::Level::TRACE,
          "Decompress realtime http message with len: {}",
          decompressed_data.len()
        );
        decompressed_data
      },
    },
  };
  let message = Message::from(payload);
  match message {
    Message::Binary(bytes) => {
      let realtime_msg = tokio::task::spawn_blocking(move || {
        RealtimeMessage::decode(&bytes).map_err(|err| {
          AppError::InvalidRequest(format!("Failed to parse RealtimeMessage: {}", err))
        })
      })
      .await
      .map_err(AppError::from)??;
      Ok(realtime_msg)
    },
    _ => Err(AppError::InvalidRequest(format!(
      "Unsupported message type: {:?}",
      message
    ))),
  }
}

#[instrument(level = "debug", skip_all)]
async fn get_collab_embed_info_handler(
  path: web::Path<(String, String)>,
  query: web::Query<CollabTypeParam>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<AFCollabEmbedInfo>>> {
  let (_, object_id) = path.into_inner();
  let collab_type = query.into_inner().collab_type;
  let info = database::collab::select_collab_embed_info(&state.pg_pool, &object_id, collab_type)
    .await
    .map_err(AppResponseError::from)?
    .ok_or_else(|| {
      AppError::RecordNotFound(format!(
        "Embedding for given object:{} not found",
        object_id
      ))
    })?;
  Ok(Json(AppResponse::Ok().with_data(info)))
}

#[instrument(level = "debug", skip_all)]
async fn batch_get_collab_embed_info_handler(
  state: Data<AppState>,
  payload: Json<RepeatedEmbeddedCollabQuery>,
) -> Result<Json<AppResponse<RepeatedAFCollabEmbedInfo>>> {
  let payload = payload.into_inner();
  let info = database::collab::batch_select_collab_embed(&state.pg_pool, payload.0)
    .await
    .map_err(AppResponseError::from)?;
  Ok(Json(AppResponse::Ok().with_data(info)))
}

#[instrument(level = "debug", skip_all, err)]
async fn collab_full_sync_handler(
  user_uuid: UserUuid,
  body: Bytes,
  path: web::Path<(Uuid, Uuid)>,
  state: Data<AppState>,
  server: Data<RealtimeServerAddr>,
  req: HttpRequest,
) -> Result<HttpResponse> {
  if body.is_empty() {
    return Err(AppError::InvalidRequest("body is empty".to_string()).into());
  }

  // when the payload size exceeds the limit, we consider it as an invalid payload.
  const MAX_BODY_SIZE: usize = 1024 * 1024 * 50; // 50MB
  if body.len() > MAX_BODY_SIZE {
    error!("Unexpected large body size: {}", body.len());
    return Err(
      AppError::InvalidRequest(format!("body size exceeds limit: {}", MAX_BODY_SIZE)).into(),
    );
  }

  let (workspace_id, object_id) = path.into_inner();
  let params = CollabDocStateParams::decode(&mut Cursor::new(body)).map_err(|err| {
    AppError::InvalidRequest(format!("Failed to parse CollabDocStateParams: {}", err))
  })?;

  if params.doc_state.is_empty() {
    return Err(AppError::InvalidRequest("doc state is empty".to_string()).into());
  }

  let collab_type = CollabType::from(params.collab_type);
  let compression_type = PayloadCompressionType::try_from(params.compression).map_err(|err| {
    AppError::InvalidRequest(format!("Failed to parse PayloadCompressionType: {}", err))
  })?;

  let doc_state = match compression_type {
    PayloadCompressionType::None => params.doc_state,
    PayloadCompressionType::Zstd => tokio::task::spawn_blocking(move || {
      zstd::decode_all(&*params.doc_state)
        .map_err(|err| AppError::InvalidRequest(format!("Failed to decompress doc_state: {}", err)))
    })
    .await
    .map_err(AppError::from)??,
  };

  let sv = match compression_type {
    PayloadCompressionType::None => params.sv,
    PayloadCompressionType::Zstd => tokio::task::spawn_blocking(move || {
      zstd::decode_all(&*params.sv)
        .map_err(|err| AppError::InvalidRequest(format!("Failed to decompress sv: {}", err)))
    })
    .await
    .map_err(AppError::from)??,
  };

  let app_version = client_version_from_headers(req.headers())
    .map(|s| s.to_string())
    .unwrap_or_else(|_| "".to_string());
  let device_id = device_id_from_headers(req.headers())
    .map(|s| s.to_string())
    .unwrap_or_else(|_| "".to_string());

  let uid = state
    .user_cache
    .get_user_uid(&user_uuid)
    .await
    .map_err(AppResponseError::from)?;

  let user = RealtimeUser {
    uid,
    device_id,
    connect_at: timestamp(),
    session_id: Uuid::new_v4().to_string(),
    app_version,
  };

  let (tx, rx) = tokio::sync::oneshot::channel();
  let message = ClientHttpUpdateMessage {
    user,
    workspace_id: workspace_id.to_string(),
    object_id: object_id.to_string(),
    collab_type,
    update: Bytes::from(doc_state),
    state_vector: Some(Bytes::from(sv)),
    return_tx: Some(tx),
  };

  server
    .try_send(message)
    .map_err(|err| AppError::Internal(anyhow!("Failed to send message to server: {}", err)))?;

  match rx
    .await
    .map_err(|err| AppError::Internal(anyhow!("Failed to receive message from server: {}", err)))?
  {
    Ok(Some(data)) => {
      let encoded = tokio::task::spawn_blocking(move || zstd::encode_all(Cursor::new(data), 3))
        .await
        .map_err(|err| AppError::Internal(anyhow!("Failed to compress data: {}", err)))??;
      Ok(HttpResponse::Ok().body(encoded))
    },
    Ok(None) => Ok(HttpResponse::InternalServerError().finish()),
    Err(err) => Ok(err.error_response()),
  }
}

async fn post_quick_note_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
  data: Json<CreateQuickNoteParams>,
) -> Result<JsonAppResponse<QuickNote>> {
  let workspace_id = workspace_id.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_role(&uid, &workspace_id.to_string(), AFRole::Member)
    .await?;
  let data = data.into_inner();
  let quick_note = create_quick_note(&state.pg_pool, uid, workspace_id, data.data.as_ref()).await?;
  Ok(Json(AppResponse::Ok().with_data(quick_note)))
}

async fn list_quick_notes_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
  query: web::Query<ListQuickNotesQueryParams>,
) -> Result<JsonAppResponse<QuickNotes>> {
  let workspace_id = workspace_id.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_role(&uid, &workspace_id.to_string(), AFRole::Member)
    .await?;
  let ListQuickNotesQueryParams {
    search_term,
    offset,
    limit,
  } = query.into_inner();
  let quick_notes = list_quick_notes(
    &state.pg_pool,
    uid,
    workspace_id,
    search_term,
    offset,
    limit,
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(quick_notes)))
}

async fn update_quick_note_handler(
  user_uuid: UserUuid,
  path_param: web::Path<(Uuid, Uuid)>,
  state: Data<AppState>,
  data: Json<UpdateQuickNoteParams>,
) -> Result<JsonAppResponse<()>> {
  let (workspace_id, quick_note_id) = path_param.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_role(&uid, &workspace_id.to_string(), AFRole::Member)
    .await?;
  update_quick_note(&state.pg_pool, quick_note_id, &data.data).await?;
  Ok(Json(AppResponse::Ok()))
}

async fn delete_quick_note_handler(
  user_uuid: UserUuid,
  path_param: web::Path<(Uuid, Uuid)>,
  state: Data<AppState>,
) -> Result<JsonAppResponse<()>> {
  let (workspace_id, quick_note_id) = path_param.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_role(&uid, &workspace_id.to_string(), AFRole::Member)
    .await?;
  delete_quick_note(&state.pg_pool, quick_note_id).await?;
  Ok(Json(AppResponse::Ok()))
}
