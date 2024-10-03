use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::Result;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use tokio::sync::Notify;
use tokio::time::interval;
use tracing::{error, info, trace};

use access_control::collab::RealtimeAccessControl;
use collab_rt_entity::user::{RealtimeUser, UserDevice};
use collab_rt_entity::MessageByObjectId;
use collab_stream::client::CollabRedisStream;
use database::collab::CollabStorage;

use crate::client::client_msg_router::ClientMessageRouter;
use crate::command::{spawn_collaboration_command, CLCommandReceiver};
use crate::config::get_env_var;
use crate::connect_state::ConnectState;
use crate::error::{CreateGroupFailedReason, RealtimeError};
use crate::group::cmd::{GroupCommand, GroupCommandRunner, GroupCommandSender};
use crate::group::manager::GroupManager;
use crate::indexer::IndexerProvider;
use crate::metrics::spawn_metrics;
use crate::rt_server::collaboration_runtime::COLLAB_RUNTIME;
use crate::state::RedisConnectionManager;
use crate::{CollabRealtimeMetrics, RealtimeClientWebsocketSink};

#[derive(Clone)]
pub struct CollaborationServer<S, AC> {
  /// Keep track of all collab groups
  group_manager: Arc<GroupManager<S, AC>>,
  connect_state: ConnectState,
  group_sender_by_object_id: Arc<DashMap<String, GroupCommandSender>>,
  storage: Arc<S>,
  #[allow(dead_code)]
  metrics: Arc<CollabRealtimeMetrics>,
  enable_custom_runtime: bool,
}

impl<S, AC> CollaborationServer<S, AC>
where
  S: CollabStorage,
  AC: RealtimeAccessControl,
{
  #[allow(clippy::too_many_arguments)]
  pub async fn new(
    storage: Arc<S>,
    access_control: AC,
    metrics: Arc<CollabRealtimeMetrics>,
    command_recv: CLCommandReceiver,
    redis_connection_manager: RedisConnectionManager,
    group_persistence_interval: Duration,
    edit_state_max_count: u32,
    edit_state_max_secs: i64,
    indexer_provider: Arc<IndexerProvider>,
  ) -> Result<Self, RealtimeError> {
    let enable_custom_runtime = get_env_var("APPFLOWY_COLLABORATE_MULTI_THREAD", "false")
      .parse::<bool>()
      .unwrap_or(false);

    if enable_custom_runtime {
      info!("CollaborationServer with custom runtime");
    } else {
      info!("CollaborationServer with actix-web runtime");
    }

    let connect_state = ConnectState::new();
    let access_control = Arc::new(access_control);
    let collab_stream = CollabRedisStream::new_with_connection_manager(redis_connection_manager);
    let group_manager = Arc::new(
      GroupManager::new(
        storage.clone(),
        access_control.clone(),
        metrics.clone(),
        collab_stream,
        group_persistence_interval,
        edit_state_max_count,
        edit_state_max_secs,
        indexer_provider.clone(),
      )
      .await?,
    );
    let group_sender_by_object_id: Arc<DashMap<String, GroupCommandSender>> =
      Arc::new(Default::default());

    spawn_period_check_inactive_group(Arc::downgrade(&group_manager), &group_sender_by_object_id);

    spawn_collaboration_command(
      command_recv,
      &group_sender_by_object_id,
      Arc::downgrade(&group_manager),
    );

    spawn_metrics(metrics.clone(), storage.clone());

    spawn_handle_unindexed_collabs(indexer_provider, storage.clone());

    Ok(Self {
      storage,
      group_manager,
      connect_state,
      group_sender_by_object_id,
      metrics,
      enable_custom_runtime,
    })
  }

  /// Handles a new user connection, replacing any existing connection for the same user.
  ///
  /// - Creates a new client stream for the connected user.
  /// - Replaces any existing user connection with the new one, signaling the old connection
  ///   if it's replaced.
  /// - Removes the old user connection from all collaboration groups.
  ///
  pub fn handle_new_connection(
    &self,
    connected_user: RealtimeUser,
    conn_sink: impl RealtimeClientWebsocketSink,
  ) -> Pin<Box<dyn Future<Output = Result<(), RealtimeError>>>> {
    let new_client_router = ClientMessageRouter::new(conn_sink);
    let group_manager = self.group_manager.clone();
    let connect_state = self.connect_state.clone();
    let metrics_calculate = self.metrics.clone();
    let storage = self.storage.clone();

    Box::pin(async move {
      storage
        .add_connected_user(connected_user.uid, &connected_user.device_id)
        .await;

      if let Some(old_user) = connect_state.handle_user_connect(connected_user, new_client_router) {
        // Remove the old user from all collaboration groups.
        group_manager.remove_user(&old_user).await;
      }
      metrics_calculate
        .connected_users
        .set(connect_state.number_of_connected_users() as i64);
      Ok(())
    })
  }

  /// Handles a user's disconnection from the collaboration server.
  ///
  /// Steps:
  /// 1. Checks if the disconnecting user's session matches the stored session.
  ///    - If yes, proceeds with removal.
  ///    - If not, exits without action.
  /// 2. Removes the user from collaboration groups and client streams.
  pub fn handle_disconnect(
    &self,
    disconnect_user: RealtimeUser,
  ) -> Pin<Box<dyn Future<Output = Result<(), RealtimeError>>>> {
    let group_manager = self.group_manager.clone();
    let connect_state = self.connect_state.clone();
    let metrics_calculate = self.metrics.clone();
    let storage = self.storage.clone();

    Box::pin(async move {
      trace!("[realtime]: disconnect => {}", disconnect_user);
      let was_removed = connect_state.handle_user_disconnect(&disconnect_user);
      if was_removed.is_some() {
        storage
          .remove_connected_user(disconnect_user.uid, &disconnect_user.device_id)
          .await;

        metrics_calculate
          .connected_users
          .set(connect_state.number_of_connected_users() as i64);

        group_manager.remove_user(&disconnect_user).await;
      }

      Ok(())
    })
  }

  #[inline]
  pub fn handle_client_message(
    &self,
    user: RealtimeUser,
    message_by_oid: MessageByObjectId,
  ) -> Pin<Box<dyn Future<Output = Result<(), RealtimeError>>>> {
    let group_sender_by_object_id = self.group_sender_by_object_id.clone();
    let client_msg_router_by_user = self.connect_state.client_message_routers.clone();
    let group_manager = self.group_manager.clone();
    let enable_custom_runtime = self.enable_custom_runtime;

    Box::pin(async move {
      for (object_id, collab_messages) in message_by_oid {
        let old_sender = group_sender_by_object_id
          .get(&object_id)
          .map(|entry| entry.value().clone());

        let sender = match old_sender {
          Some(sender) => sender,
          None => match group_sender_by_object_id.entry(object_id.clone()) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
              let (new_sender, recv) = tokio::sync::mpsc::channel(2000);
              let notify = Arc::new(Notify::new());
              let runner = GroupCommandRunner {
                group_manager: group_manager.clone(),
                msg_router_by_user: client_msg_router_by_user.clone(),
                recv: Some(recv),
              };

              let object_id = entry.key().clone();
              let clone_notify = notify.clone();
              if enable_custom_runtime {
                COLLAB_RUNTIME.spawn(runner.run(object_id, clone_notify));
              } else {
                tokio::spawn(runner.run(object_id, clone_notify));
              }

              entry.insert(new_sender.clone());

              // wait for the runner to be ready to handle the message.
              notify.notified().await;
              new_sender
            },
          },
        };

        let cloned_user = user.clone();
        // Create a new task to send a message to the group command runner without waiting for the
        // result. This approach is used to prevent potential issues with the actor's mailbox in
        // single-threaded runtimes (like actix-web actors). By spawning a task, the actor can
        // immediately proceed to process the next message.
        tokio::spawn(async move {
          let (tx, rx) = tokio::sync::oneshot::channel();
          match sender
            .send(GroupCommand::HandleClientCollabMessage {
              user: cloned_user,
              object_id,
              collab_messages,
              ret: tx,
            })
            .await
          {
            Ok(_) => {
              if let Ok(Err(err)) = rx.await {
                if !matches!(
                  err,
                  RealtimeError::CreateGroupFailed(
                    CreateGroupFailedReason::CollabWorkspaceIdNotMatch { .. }
                  )
                ) {
                  error!("Handle client collab message fail: {}", err);
                }
              }
            },
            Err(err) => {
              // it should not happen. Because the receiver is always running before acquiring the sender.
              // Otherwise, the GroupCommandRunner might not be ready to handle the message.
              error!("Send message to group fail: {}", err);
            },
          }
        });
      }

      Ok(())
    })
  }

  pub fn get_user_by_device(&self, user_device: &UserDevice) -> Option<RealtimeUser> {
    self
      .connect_state
      .user_by_device
      .get(user_device)
      .map(|entry| entry.value().clone())
  }
}

fn spawn_handle_unindexed_collabs(
  indexer_provider: Arc<IndexerProvider>,
  storage: Arc<dyn CollabStorage>,
) {
  tokio::spawn(IndexerProvider::handle_unindexed_collabs(
    indexer_provider,
    storage,
  ));
}

fn spawn_period_check_inactive_group<S, AC>(
  weak_groups: Weak<GroupManager<S, AC>>,
  group_sender_by_object_id: &Arc<DashMap<String, GroupCommandSender>>,
) where
  S: CollabStorage,
  AC: RealtimeAccessControl,
{
  let mut interval = interval(Duration::from_secs(20));
  let cloned_group_sender_by_object_id = group_sender_by_object_id.clone();
  tokio::spawn(async move {
    // when appflowy-collaborate start, wait for 60 seconds to start the check. Since no groups will
    // be inactive in the first 60 seconds.
    tokio::time::sleep(Duration::from_secs(60)).await;

    loop {
      interval.tick().await;
      if let Some(groups) = weak_groups.upgrade() {
        let inactive_group_ids = groups.get_inactive_groups().await;
        for id in inactive_group_ids {
          cloned_group_sender_by_object_id.remove(&id);
        }
      } else {
        break;
      }
    }
  });
}

/// When the CollaborationServer operates within an actix-web actor, utilizing tokio::spawn for
/// task execution confines all tasks to the same thread, attributable to the actor's reliance on a
/// single-threaded Tokio runtime. To circumvent this limitation and enable task execution across
/// multiple threads, we've incorporated a multi-thread feature.
///
/// When appflowy-collaborate is deployed as a standalone service, we can use tokio multi-thread.
mod collaboration_runtime {
  use std::io;

  use lazy_static::lazy_static;
  use tokio::runtime;
  use tokio::runtime::Runtime;

  lazy_static! {
    pub(crate) static ref COLLAB_RUNTIME: Runtime = default_tokio_runtime().unwrap();
  }

  pub fn default_tokio_runtime() -> io::Result<Runtime> {
    runtime::Builder::new_multi_thread()
      .thread_name("collab-rt")
      .enable_io()
      .enable_time()
      .build()
  }
}
