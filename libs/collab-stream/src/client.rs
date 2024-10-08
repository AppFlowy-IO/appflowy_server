use crate::collab_update_sink::{AwarenessUpdateSink, CollabUpdateSink};
use crate::error::StreamError;
use crate::lease::{Lease, LeaseAcquisition};
use crate::model::{
  AwarenessStreamUpdate, AwarenessStreamUpdateBatch, CollabStreamUpdate, CollabStreamUpdateBatch,
  CollabUpdateEvent, MessageId,
};
use crate::pubsub::{CollabStreamPub, CollabStreamSub};
use crate::stream_group::{StreamConfig, StreamGroup};
use futures::Stream;
use redis::aio::ConnectionManager;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use std::time::Duration;
use tracing::error;

pub const CONTROL_STREAM_KEY: &str = "af_collab_control";

#[derive(Clone)]
pub struct CollabRedisStream {
  connection_manager: ConnectionManager,
}

impl CollabRedisStream {
  pub const LEASE_TTL: Duration = Duration::from_secs(60);

  pub async fn new(redis_client: redis::Client) -> Result<Self, redis::RedisError> {
    let connection_manager = redis_client.get_connection_manager().await?;
    Ok(Self::new_with_connection_manager(connection_manager))
  }

  pub fn new_with_connection_manager(connection_manager: ConnectionManager) -> Self {
    Self { connection_manager }
  }

  pub async fn lease(
    &self,
    workspace_id: &str,
    object_id: &str,
  ) -> Result<Option<LeaseAcquisition>, StreamError> {
    let lease_key = format!("af:{}:{}:snapshot_lease", workspace_id, object_id);
    self
      .connection_manager
      .lease(lease_key, Self::LEASE_TTL)
      .await
  }

  pub async fn collab_control_stream(
    &self,
    key: &str,
    group_name: &str,
  ) -> Result<StreamGroup, StreamError> {
    let mut group = StreamGroup::new_with_config(
      key.to_string(),
      group_name,
      self.connection_manager.clone(),
      StreamConfig::new().with_max_len(1000),
    );

    // don't return error when create consumer group failed
    if let Err(err) = group.ensure_consumer_group().await {
      error!("Failed to ensure consumer group: {}", err);
    }

    Ok(group)
  }

  pub async fn collab_update_stream_group(
    &self,
    workspace_id: &str,
    oid: &str,
    group_name: &str,
  ) -> Result<StreamGroup, StreamError> {
    let stream_key = format!("af_collab_update-{}-{}", workspace_id, oid);
    let mut group = StreamGroup::new_with_config(
      stream_key,
      group_name,
      self.connection_manager.clone(),
      StreamConfig::new()
        // 2000 messages
        .with_max_len(2000)
        // 12 hours
        .with_expire_time(60 * 60 * 12),
    );
    group.ensure_consumer_group().await?;
    Ok(group)
  }

  pub fn collab_update_sink(&self, workspace_id: &str, object_id: &str) -> CollabUpdateSink {
    let stream_key = CollabStreamUpdate::stream_key(workspace_id, object_id);
    CollabUpdateSink::new(self.connection_manager.clone(), stream_key)
  }

  pub fn awareness_update_sink(&self, workspace_id: &str, object_id: &str) -> AwarenessUpdateSink {
    let stream_key = AwarenessStreamUpdate::stream_key(workspace_id, object_id);
    AwarenessUpdateSink::new(self.connection_manager.clone(), stream_key)
  }

  pub fn collab_updates(
    &self,
    workspace_id: &str,
    object_id: &str,
    since: Option<MessageId>,
  ) -> impl Stream<Item = Result<(MessageId, CollabStreamUpdate), StreamError>> {
    // use `:` separator as it adheres to Redis naming conventions
    let mut conn = self.connection_manager.clone();
    let stream_key = CollabStreamUpdate::stream_key(workspace_id, object_id);
    let read_options = StreamReadOptions::default().count(100);
    let mut since = since.unwrap_or_default();
    async_stream::try_stream! {
      loop {
        let last_id = since.to_string();
        let batch: CollabStreamUpdateBatch = conn
          .xread_options(&[&stream_key], &[&last_id], &read_options)
          .await?;
        for (message_id, update) in batch.updates {
          since = since.max(message_id);
          yield (message_id, update);
        }
      }
    }
  }

  pub fn awareness_updates(
    &self,
    workspace_id: &str,
    object_id: &str,
    since: Option<MessageId>,
  ) -> impl Stream<Item = Result<AwarenessStreamUpdate, StreamError>> {
    // use `:` separator as it adheres to Redis naming conventions
    let mut conn = self.connection_manager.clone();
    let stream_key = AwarenessStreamUpdate::stream_key(workspace_id, object_id);
    let read_options = StreamReadOptions::default().count(100);
    let mut since = since.unwrap_or_default();
    async_stream::try_stream! {
      loop {
        let last_id = since.to_string();
        let batch: AwarenessStreamUpdateBatch = conn
          .xread_options(&[&stream_key], &[&last_id], &read_options)
          .await?;
        for (message_id, update) in batch.updates {
          since = since.max(message_id);
          yield update;
        }
      }
    }
  }
}
