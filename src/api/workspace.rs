use crate::biz::workspace;
use crate::component::auth::jwt::UserUuid;
use crate::state::{AppState, Storage};
use actix_web::web::{Data, Json};
use actix_web::Result;
use actix_web::{web, Scope};
use database_entity::*;
use shared_entity::data::{AppResponse, JsonAppResponse};
use shared_entity::dto::workspace_dto::*;
use sqlx::types::uuid;

use tracing::{debug, instrument};
use tracing_actix_web::RequestId;

use crate::biz;
use crate::component::storage_proxy::CollabStorageProxy;
use database::collab::CollabStorage;
use database_entity::database_error::DatabaseError;
use shared_entity::app_error::AppError;
use shared_entity::error_code::ErrorCode;
use uuid::Uuid;

pub const WORKSPACE_ID_PATH: &str = "workspace_id";
pub const COLLAB_OBJECT_ID_PATH: &str = "object_id";

const SCOPE_PATH: &str = "/api/workspace";
const WORKSPACE_LIST_RESOURCE: &str = "list";

pub fn workspace_scope() -> Scope {
  web::scope(SCOPE_PATH)
    .service(web::resource(WORKSPACE_LIST_RESOURCE).route(web::get().to(list_handler)))
    .service(
      web::resource("{workspace_id}/member")
        .route(web::get().to(list_workspace_members_handler))
        .route(web::post().to(add_workspace_members_handler))
        .route(web::put().to(update_workspace_member_handler))
        .route(web::delete().to(remove_workspace_member_handler)),
    )
    .service(
      web::resource("{workspace_id}/collab/{object_id}")
        .route(web::post().to(create_collab_handler))
        .route(web::get().to(get_collab_handler))
        .route(web::put().to(update_collab_handler))
        .route(web::delete().to(delete_collab_handler)),
    )
    .service(web::resource("list").route(web::get().to(batch_get_collab_handler)))
    .service(web::resource("snapshot").route(web::get().to(retrieve_snapshot_data_handler)))
    .service(web::resource("snapshots").route(web::get().to(retrieve_snapshots_handler)))
}

#[instrument(skip_all, err)]
async fn list_handler(
  uuid: UserUuid,
  state: Data<AppState>,
) -> Result<JsonAppResponse<AFWorkspaces>> {
  let workspaces = workspace::ops::get_workspaces(&state.pg_pool, &uuid).await?;
  Ok(AppResponse::Ok().with_data(workspaces).into())
}

#[instrument(skip(payload, state), err)]
async fn add_workspace_members_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  payload: Json<CreateWorkspaceMembers>,
  state: Data<AppState>,
) -> Result<JsonAppResponse<()>> {
  let create_members = payload.into_inner();
  workspace::ops::add_workspace_members(
    &state.pg_pool,
    &user_uuid,
    &workspace_id,
    create_members.0,
  )
  .await?;
  Ok(AppResponse::Ok().into())
}

#[instrument(skip_all, err)]
async fn list_workspace_members_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  workspace_id: web::Path<Uuid>,
) -> Result<JsonAppResponse<Vec<AFWorkspaceMember>>> {
  let ws_members =
    workspace::ops::get_workspace_members(&state.pg_pool, &user_uuid, &workspace_id).await?;
  Ok(AppResponse::Ok().with_data(ws_members).into())
}

#[instrument(skip_all, err)]
async fn remove_workspace_member_handler(
  user_uuid: UserUuid,
  payload: Json<WorkspaceMembers>,
  state: Data<AppState>,
  workspace_id: web::Path<Uuid>,
) -> Result<JsonAppResponse<()>> {
  let member_emails = payload
    .into_inner()
    .0
    .into_iter()
    .map(|member| member.0)
    .collect();
  workspace::ops::remove_workspace_members(
    &user_uuid,
    &state.pg_pool,
    workspace_id.into_inner(),
    member_emails,
  )
  .await?;
  Ok(AppResponse::Ok().into())
}

#[instrument(skip_all, err)]
async fn update_workspace_member_handler(
  payload: Json<WorkspaceMemberChangeset>,
  state: Data<AppState>,
  workspace_id: web::Path<Uuid>,
) -> Result<JsonAppResponse<()>> {
  let workspace_id = workspace_id.into_inner();
  let changeset = payload.into_inner();
  workspace::ops::update_workspace_member(&state.pg_pool, &workspace_id, changeset).await?;
  Ok(AppResponse::Ok().into())
}

#[instrument(skip(state, payload), err)]
async fn create_collab_handler(
  user_uuid: UserUuid,
  required_id: RequestId,
  payload: Json<InsertCollabParams>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<()>>> {
  biz::collab::ops::create_collab(&state.pg_pool, &user_uuid, &payload.into_inner()).await?;
  Ok(Json(AppResponse::Ok()))
}

#[instrument(skip(storage, payload), err)]
async fn get_collab_handler(
  user_uuid: UserUuid,
  required_id: RequestId,
  payload: Json<QueryCollabParams>,
  storage: Data<Storage<CollabStorageProxy>>,
) -> Result<Json<AppResponse<RawData>>> {
  let data = storage
    .collab_storage
    .get_collab(payload.into_inner())
    .await
    .map_err(|err| match &err {
      DatabaseError::RecordNotFound => AppError::new(ErrorCode::RecordNotFound, err.to_string()),
      _ => AppError::new(ErrorCode::DatabaseError, err.to_string()),
    })?;

  debug!("Returned data length: {}", data.len());
  Ok(Json(AppResponse::Ok().with_data(data)))
}

#[instrument(skip(storage, payload), err)]
async fn batch_get_collab_handler(
  user_uuid: UserUuid,
  required_id: RequestId,
  payload: Json<BatchQueryCollabParams>,
  storage: Data<Storage<CollabStorageProxy>>,
) -> Result<Json<AppResponse<BatchQueryCollabResult>>> {
  let result = BatchQueryCollabResult(
    storage
      .collab_storage
      .batch_get_collab(payload.into_inner().0)
      .await,
  );
  Ok(Json(AppResponse::Ok().with_data(result)))
}

#[instrument(skip(state, payload), err)]
async fn update_collab_handler(
  user_uuid: UserUuid,
  required_id: RequestId,
  payload: Json<InsertCollabParams>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<()>>> {
  biz::collab::ops::upsert_collab(&state.pg_pool, &user_uuid, &payload.into_inner()).await?;
  Ok(AppResponse::Ok().into())
}

#[instrument(level = "info", skip(state, payload), err)]
async fn delete_collab_handler(
  user_uuid: UserUuid,
  required_id: RequestId,
  payload: Json<DeleteCollabParams>,
  state: Data<AppState>,
) -> Result<Json<AppResponse<()>>> {
  biz::collab::ops::delete_collab(&state.pg_pool, &user_uuid, &payload.into_inner()).await?;
  Ok(AppResponse::Ok().into())
}

async fn retrieve_snapshot_data_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  payload: Json<QuerySnapshotParams>,
) -> Result<Json<AppResponse<RawData>>> {
  let data =
    biz::collab::ops::get_collab_snapshot(&state.pg_pool, &user_uuid, &payload.into_inner())
      .await?;
  Ok(Json(AppResponse::Ok().with_data(data)))
}

#[tracing::instrument(level = "debug", skip_all)]
async fn retrieve_snapshots_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  payload: Json<QueryObjectSnapshotParams>,
) -> Result<Json<AppResponse<AFCollabSnapshots>>> {
  let data =
    biz::collab::ops::get_all_collab_snapshot(&state.pg_pool, &user_uuid, &payload.into_inner())
      .await?;
  Ok(Json(AppResponse::Ok().with_data(data)))
}
