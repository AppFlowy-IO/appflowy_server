use crate::biz::collab::member_listener::{CollabMemberAction, CollabMemberChange};
use crate::biz::collab::ops::require_user_can_edit;
use crate::component::auth::jwt::UserUuid;
use crate::middleware::access_control_mw::{AccessControlService, AccessResource};
use anyhow::Error;
use async_trait::async_trait;
use database_entity::AFRole;
use realtime::collaborate::CollabPermission;
use shared_entity::app_error::AppError;
use sqlx::PgPool;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::trace;
use uuid::Uuid;

#[derive(Clone)]
pub struct CollabAccessControl;

#[async_trait]
impl AccessControlService for CollabAccessControl {
  fn resource(&self) -> AccessResource {
    AccessResource::Collab
  }

  async fn check_collab_permission(
    &self,
    workspace_id: &Uuid,
    oid: &str,
    user_uuid: &UserUuid,
    pg_pool: &PgPool,
  ) -> Result<(), AppError> {
    trace!(
      "Collab access control: oid: {:?}, user_uuid: {:?}",
      oid,
      user_uuid
    );
    require_user_can_edit(pg_pool, workspace_id, user_uuid, oid).await
  }
}

type RoleStatusByOid = HashMap<String, RoleStatus>;

/// Use to check if the user is allowed to send or receive the [CollabMessage]
pub struct CollabPermissionImpl {
  pg_pool: PgPool,
  role_by_uid: Arc<RwLock<HashMap<i64, RoleStatusByOid>>>,
}

#[derive(Clone, Debug)]
enum RoleStatus {
  Invalid,
  Valid(AFRole),
}

impl CollabPermissionImpl {
  pub fn new(pg_pool: PgPool, mut listener: broadcast::Receiver<CollabMemberChange>) -> Self {
    let role_by_uid = Arc::new(RwLock::new(HashMap::new()));

    // Update the role of the user when the role of the collab member is changed
    let cloned_role_by_uid = role_by_uid.clone();
    tokio::spawn(async move {
      while let Ok(change) = listener.recv().await {
        match change.action_type {
          CollabMemberAction::Insert | CollabMemberAction::Update => {
            let mut outer_map = cloned_role_by_uid.write().await;
            let inner_map = outer_map.entry(change.uid).or_insert_with(HashMap::new);
            inner_map.insert(change.oid.clone(), RoleStatus::Valid(change.role));
          },
          CollabMemberAction::Delete => {
            if let Some(mut inner_map) = cloned_role_by_uid.write().await.get_mut(&change.uid) {
              inner_map.insert(change.oid.clone(), RoleStatus::Invalid);
            }
          },
        }
      }
    });

    Self {
      pg_pool,
      role_by_uid,
    }
  }

  async fn get_role_state(&self, uid: i64, oid: &str) -> Option<RoleStatus> {
    self
      .role_by_uid
      .read()
      .await
      .get(&uid)
      .map(|map| map.get(oid).cloned())?
  }

  async fn load_role_state(&self, uid: i64, oid: &str) -> Result<RoleStatus, Error> {
    todo!()
  }

  #[inline]
  async fn is_user_can_edit_collab(&self, uid: i64, oid: &str) -> Result<bool, Error> {
    match self.get_role_state(uid, oid).await {
      None => {
        self.load_role_state(uid, oid).await;
      },
      Some(status) => {},
    }

    todo!()
  }
}

#[async_trait]
impl CollabPermission for CollabPermissionImpl {
  #[inline]
  async fn is_allowed_send_by_user(&self, uid: i64, oid: &str) -> Result<bool, Error> {
    self.is_user_can_edit_collab(uid, oid).await
  }

  #[inline]
  async fn is_allowed_recv_by_user(&self, uid: i64, oid: &str) -> Result<bool, Error> {
    self.is_user_can_edit_collab(uid, oid).await
  }
}
