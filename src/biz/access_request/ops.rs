use std::ops::DerefMut;
use std::sync::Arc;

use crate::biz::collab::utils::get_latest_collab_folder;
use crate::mailer::AFCloudMailer;
use crate::{
  biz::collab::folder_view::{to_dto_view_icon, to_dto_view_layout},
  mailer::{WorkspaceAccessRequestApprovedMailerParam, WorkspaceAccessRequestMailerParam},
};
use access_control::workspace::WorkspaceAccessControl;
use anyhow::Context;
use app_error::AppError;
use appflowy_collaborate::collab::storage::CollabAccessControlStorage;
use database::{
  access_request::{
    insert_new_access_request, select_access_request_by_request_id, update_access_request_status,
  },
  collab::GetCollabOrigin,
  pg_row::AFAccessRequestStatusColumn,
  workspace::upsert_workspace_member_with_txn,
};
use database_entity::dto::AFRole;
use shared_entity::dto::access_request_dto::{AccessRequest, AccessRequestView};
use sqlx::PgPool;
use uuid::Uuid;

// Test
pub async fn create_access_request(
  pg_pool: &PgPool,
  mailer: AFCloudMailer,
  appflowy_web_url: &str,
  workspace_id: Uuid,
  view_id: Uuid,
  uid: i64,
) -> Result<Uuid, AppError> {
  let request_id = insert_new_access_request(pg_pool, workspace_id, view_id, uid).await?;
  let access_request = select_access_request_by_request_id(pg_pool, request_id).await?;
  let cloned_mailer = mailer.clone();
  let approve_url = format!(
    "{}/app/approve-request?request_id={}",
    appflowy_web_url, request_id
  );
  let email = access_request.workspace.owner_email.clone();
  let recipient_name = access_request.workspace.owner_name.clone();
  // use default icon until we have workspace icon
  let workspace_icon_url =
    "https://miro.medium.com/v2/resize:fit:2400/1*mTPfm7CwU31-tLhtLNkyJw.png".to_string();
  let user_icon_url =
    "https://cdn.pixabay.com/photo/2015/10/05/22/37/blank-profile-picture-973460_1280.png"
      .to_string();
  tokio::spawn(async move {
    if let Err(err) = cloned_mailer
      .send_workspace_access_request(
        &recipient_name,
        &email,
        WorkspaceAccessRequestMailerParam {
          user_icon_url,
          username: access_request.requester.name,
          workspace_name: access_request.workspace.workspace_name,
          workspace_icon_url,
          workspace_member_count: access_request.workspace.member_count.unwrap_or(0),
          approve_url,
        },
      )
      .await
    {
      tracing::error!("Failed to send access request email: {:?}", err);
    };
  });
  Ok(request_id)
}

pub async fn get_access_request(
  pg_pool: &PgPool,
  collab_storage: &CollabAccessControlStorage,
  access_request_id: Uuid,
  user_uid: i64,
) -> Result<AccessRequest, AppError> {
  let access_request_with_view_id =
    select_access_request_by_request_id(pg_pool, access_request_id).await?;

  if access_request_with_view_id.workspace.owner_uid != user_uid {
    return Err(AppError::NotEnoughPermissions);
  }
  let folder = get_latest_collab_folder(
    collab_storage,
    GetCollabOrigin::Server,
    &access_request_with_view_id
      .workspace
      .workspace_id
      .to_string(),
  )
  .await?;
  let view = folder.get_view(&access_request_with_view_id.view_id.to_string());
  let access_request_view = view
    .map(|v| AccessRequestView {
      view_id: v.id.clone(),
      name: v.name.clone(),
      icon: v.icon.as_ref().map(|icon| to_dto_view_icon(icon.clone())),
      layout: to_dto_view_layout(&v.layout),
    })
    .ok_or(AppError::MissingView(format!(
      "the view {} is missing",
      access_request_with_view_id.view_id
    )))?;
  let access_request = AccessRequest {
    request_id: access_request_with_view_id.request_id,
    workspace: access_request_with_view_id.workspace,
    requester: access_request_with_view_id.requester,
    view: access_request_view,
    status: access_request_with_view_id.status,
    created_at: access_request_with_view_id.created_at,
  };
  Ok(access_request)
}

#[allow(clippy::too_many_arguments)]
pub async fn approve_or_reject_access_request(
  pg_pool: &PgPool,
  workspace_access_control: Arc<dyn WorkspaceAccessControl>,
  mailer: AFCloudMailer,
  appflowy_web_url: &str,
  request_id: Uuid,
  uid: i64,
  is_approved: bool,
) -> Result<(), AppError> {
  let access_request = select_access_request_by_request_id(pg_pool, request_id).await?;
  workspace_access_control
    .enforce_role(
      &uid,
      &access_request.workspace.workspace_id.to_string(),
      AFRole::Owner,
    )
    .await?;

  let mut txn = pg_pool.begin().await.context("approving request")?;
  let role = AFRole::Member;
  if is_approved {
    upsert_workspace_member_with_txn(
      &mut txn,
      &access_request.workspace.workspace_id,
      &access_request.requester.email,
      role.clone(),
    )
    .await?;
    workspace_access_control
      .insert_role(
        &access_request.requester.uid,
        &access_request.workspace.workspace_id,
        role.clone(),
      )
      .await?;
    let cloned_mailer = mailer.clone();
    let launch_workspace_url = format!(
      "{}/app/{}",
      appflowy_web_url, &access_request.workspace.workspace_id
    );

    // use default icon until we have workspace icon
    let workspace_icon_url =
      "https://miro.medium.com/v2/resize:fit:2400/1*mTPfm7CwU31-tLhtLNkyJw.png".to_string();
    tokio::spawn(async move {
      if let Err(err) = cloned_mailer
        .send_workspace_access_request_approval_notification(
          &access_request.requester.name,
          &access_request.requester.email,
          WorkspaceAccessRequestApprovedMailerParam {
            workspace_name: access_request.workspace.workspace_name,
            workspace_icon_url,
            workspace_member_count: access_request.workspace.member_count.unwrap_or(0),
            launch_workspace_url,
          },
        )
        .await
      {
        tracing::error!(
          "Failed to send access request approved notification email: {:?}",
          err
        );
      };
    });
  }
  let status = if is_approved {
    AFAccessRequestStatusColumn::Approved
  } else {
    AFAccessRequestStatusColumn::Rejected
  };
  update_access_request_status(txn.deref_mut(), request_id, status).await?;
  txn.commit().await.context("committing transaction")?;
  Ok(())
}
