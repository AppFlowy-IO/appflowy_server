use crate::error::RealtimeError;
use anyhow::anyhow;
use app_error::AppError;
use arc_swap::ArcSwap;
use collab::core::collab::DataSource;
use collab::core::origin::CollabOrigin;
use collab::entity::EncodedCollab;
use collab::lock::RwLock;
use collab::preclude::Collab;
use collab_entity::CollabType;
use collab_rt_entity::user::RealtimeUser;
use collab_rt_entity::{
  AckCode, AwarenessSync, BroadcastSync, CollabAck, MessageByObjectId, MsgId,
};
use collab_rt_entity::{ClientCollabMessage, CollabMessage};
use collab_rt_protocol::{Message, MessageReader, RTProtocolError, SyncMessage};
use collab_stream::client::CollabRedisStream;
use collab_stream::collab_update_sink::{AwarenessUpdateSink, CollabUpdateSink};

use crate::metrics::CollabRealtimeMetrics;
use bytes::Bytes;
use collab_document::document::DocumentBody;
use collab_stream::error::StreamError;
use collab_stream::model::{AwarenessStreamUpdate, CollabStreamUpdate, MessageId, UpdateFlags};
use dashmap::DashMap;
use database::collab::{CollabStorage, GetCollabOrigin};
use database_entity::dto::{CollabParams, QueryCollabParams};
use futures::{pin_mut, Sink, Stream};
use futures_util::{SinkExt, StreamExt};
use indexer::scheduler::{IndexerScheduler, UnindexedCollabTask, UnindexedData};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, trace, warn};
use uuid::Uuid;
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{ReadTxn, StateVector, Update};

/// A group used to manage a single [Collab] object
pub struct CollabGroup {
  state: Arc<CollabGroupState>,
}

/// Inner state of [CollabGroup] that's private and hidden behind Arc, so that it can be moved into
/// tasks.
struct CollabGroupState {
  workspace_id: String,
  object_id: String,
  collab_type: CollabType,
  /// A list of subscribers to this group. Each subscriber will receive updates from the
  /// broadcast.
  subscribers: DashMap<RealtimeUser, Subscription>,
  persister: CollabPersister,
  metrics: Arc<CollabRealtimeMetrics>,
  /// Cancellation token triggered when current collab group is about to be stopped.
  /// This will also shut down all subsequent [Subscription]s.
  shutdown: CancellationToken,
  last_activity: ArcSwap<Instant>,
  seq_no: AtomicU32,
  /// The most recent state vector from a redis update.
  state_vector: RwLock<StateVector>,
}

impl Drop for CollabGroup {
  fn drop(&mut self) {
    // we're going to use state shutdown to cancel subsequent tasks
    self.state.shutdown.cancel();
  }
}

impl CollabGroup {
  #[allow(clippy::too_many_arguments)]
  pub fn new<S>(
    uid: i64,
    workspace_id: String,
    object_id: String,
    collab_type: CollabType,
    metrics: Arc<CollabRealtimeMetrics>,
    storage: Arc<S>,
    collab_redis_stream: Arc<CollabRedisStream>,
    persistence_interval: Duration,
    prune_grace_period: Duration,
    state_vector: StateVector,
    indexer_scheduler: Arc<IndexerScheduler>,
  ) -> Result<Self, StreamError>
  where
    S: CollabStorage,
  {
    let is_new_collab = state_vector.is_empty();
    let persister = CollabPersister::new(
      uid,
      workspace_id.clone(),
      object_id.clone(),
      collab_type.clone(),
      storage,
      collab_redis_stream,
      indexer_scheduler,
      metrics.clone(),
      prune_grace_period,
    );

    let state = Arc::new(CollabGroupState {
      workspace_id,
      object_id,
      collab_type,
      subscribers: DashMap::new(),
      metrics,
      shutdown: CancellationToken::new(),
      persister,
      last_activity: ArcSwap::new(Instant::now().into()),
      seq_no: AtomicU32::new(0),
      state_vector: state_vector.into(),
    });

    /*
     NOTE: we don't want to pass `Weak<CollabGroupState>` to tasks and terminate them when they
     cannot be upgraded since we want to be sure that ie. when collab group is to be removed,
     that we're going to call for a final save of the document state.

     For that we use `CancellationToken` instead, which is racing against internal loops of child
     tasks and triggered when this `CollabGroup` is dropped.
    */

    // setup task used to receive collab updates from Redis
    {
      let state = state.clone();
      tokio::spawn(async move {
        if let Err(err) = Self::inbound_task(state).await {
          tracing::warn!("failed to receive collab update: {}", err);
        }
      });
    }

    // setup task used to receive awareness updates from Redis
    {
      let state = state.clone();
      tokio::spawn(async move {
        if let Err(err) = Self::inbound_awareness_task(state).await {
          tracing::warn!("failed to receive awareness update: {}", err);
        }
      });
    }

    // setup periodic snapshot
    {
      tokio::spawn(Self::snapshot_task(
        state.clone(),
        persistence_interval,
        is_new_collab,
      ));
    }

    Ok(Self { state })
  }

  #[inline]
  pub fn workspace_id(&self) -> &str {
    &self.state.workspace_id
  }

  #[inline]
  #[allow(dead_code)]
  pub fn object_id(&self) -> &str {
    &self.state.object_id
  }

  pub fn is_cancelled(&self) -> bool {
    self.state.shutdown.is_cancelled()
  }

  /// Task used to receive collab updates from Redis.
  async fn inbound_task(state: Arc<CollabGroupState>) -> Result<(), RealtimeError> {
    let updates = state.persister.collab_redis_stream.live_collab_updates(
      &state.workspace_id,
      &state.object_id,
      None,
    );
    pin_mut!(updates);
    loop {
      tokio::select! {
        _ = state.shutdown.cancelled() => {
          match state.persister.trim_awareness().await {
            Ok(_) => (),
            Err(err) => warn!("unable to trim awareness due to {}", err),
          };
          break;
        }
        res = updates.next() => {
          match res {
            Some(Ok((message_id, update))) => {
              state.metrics.observe_collab_stream_latency(message_id.timestamp_ms);
              Self::handle_inbound_update(&state, update).await;
            },
            Some(Err(err)) => {
              tracing::warn!("failed to handle incoming update for collab `{}`: {}", state.object_id, err);
              break;
            },
            None => {
              break;
            }
          }
        }
      }
    }
    Ok(())
  }

  async fn handle_inbound_update(state: &CollabGroupState, update: CollabStreamUpdate) {
    // update state vector based on incoming message
    match Update::decode_v1(&update.data) {
      Ok(update) => state
        .state_vector
        .write()
        .await
        .merge(update.state_vector()),
      Err(err) => {
        tracing::error!(
          "received malformed update for collab `{}`: {}",
          state.object_id,
          err
        );
        return;
      },
    }

    let seq_num = state.seq_no.fetch_add(1, Ordering::SeqCst) + 1;
    tracing::trace!(
      "broadcasting collab update from {} ({} bytes) - seq_num: {}",
      update.sender,
      update.data.len(),
      seq_num
    );
    let payload = Message::Sync(SyncMessage::Update(update.data)).encode_v1();
    let message = BroadcastSync::new(update.sender, state.object_id.clone(), payload, seq_num);
    for mut e in state.subscribers.iter_mut() {
      let subscription = e.value_mut();
      if message.origin == subscription.collab_origin {
        continue; // don't send update to its sender
      }

      if let Err(err) = subscription.sink.send(message.clone().into()).await {
        tracing::debug!(
          "failed to send collab `{}` update to `{}`: {}",
          state.object_id,
          subscription.collab_origin,
          err
        );
      }

      state.last_activity.store(Arc::new(Instant::now()));
    }
  }

  /// Task used to receive awareness updates from Redis.
  async fn inbound_awareness_task(state: Arc<CollabGroupState>) -> Result<(), RealtimeError> {
    let updates = state.persister.collab_redis_stream.awareness_updates(
      &state.workspace_id,
      &state.object_id,
      None,
    );
    pin_mut!(updates);
    loop {
      tokio::select! {
        _ = state.shutdown.cancelled() => {
          break;
        }
        res = updates.next() => {
          match res {
            Some(Ok(awareness_update)) => {
              Self::handle_inbound_awareness(&state, awareness_update).await;
            },
            Some(Err(err)) => {
              tracing::warn!("failed to handle incoming update for collab `{}`: {}", state.object_id, err);
              break;
            },
            None => {
              break;
            }
          }
        }
      }
    }
    Ok(())
  }

  async fn handle_inbound_awareness(state: &CollabGroupState, update: AwarenessStreamUpdate) {
    tracing::trace!(
      "broadcasting awareness update from {} ({} bytes)",
      update.sender,
      update.data.len()
    );
    let sender = update.sender;
    let message = AwarenessSync::new(
      state.object_id.clone(),
      Message::Awareness(update.data).encode_v1(),
      CollabOrigin::Empty,
    );
    for mut e in state.subscribers.iter_mut() {
      let subscription = e.value_mut();
      if sender == subscription.collab_origin {
        continue; // don't send update to its sender
      }

      if let Err(err) = subscription.sink.send(message.clone().into()).await {
        tracing::debug!(
          "failed to send awareness `{}` update to `{}`: {}",
          state.object_id,
          subscription.collab_origin,
          err
        );
      }

      state.last_activity.store(Arc::new(Instant::now()));
    }
  }

  async fn snapshot_task(state: Arc<CollabGroupState>, interval: Duration, is_new_collab: bool) {
    if is_new_collab {
      tracing::trace!("persisting new collab for {}", state.object_id);
      if let Err(err) = state.persister.save().await {
        tracing::warn!(
          "failed to persist new document `{}`: {}",
          state.object_id,
          err
        );
      }
    }

    let mut snapshot_tick = tokio::time::interval(interval);
    // if saving took longer than snapshot_tick, just skip it over and try in the next round
    snapshot_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
      tokio::select! {
        _ = snapshot_tick.tick() => {
          if let Err(err) = state.persister.save().await {
            tracing::warn!("failed to persist collab `{}/{}`: {}", state.workspace_id, state.object_id, err);
          }
        },
        _ = state.shutdown.cancelled() => {
          if let Err(err) = state.persister.save().await {
            tracing::warn!("failed to persist collab on shutdown `{}/{}`: {}", state.workspace_id, state.object_id, err);
          }
          break;
        }
      }
    }
  }

  /// Generate embedding for the current Collab immediately
  ///
  pub async fn generate_embeddings(&self) -> Result<(), AppError> {
    let collab = self
      .encode_collab()
      .await
      .map_err(|e| AppError::Internal(e.into()))?;
    let collab = Collab::new_with_source(
      CollabOrigin::Server,
      self.object_id(),
      DataSource::DocStateV1(collab.doc_state.into()),
      vec![],
      false,
    )
    .map_err(|e| AppError::Internal(e.into()))?;
    let workspace_id = &self.state.workspace_id;
    let object_id = &self.state.object_id;
    let collab_type = &self.state.collab_type;
    self
      .state
      .persister
      .indexer_scheduler
      .index_collab_immediately(workspace_id, object_id, &collab, collab_type)
      .await
  }

  pub async fn calculate_missing_update(
    &self,
    state_vector: StateVector,
  ) -> Result<Vec<u8>, RealtimeError> {
    {
      // first check if we need to send any updates
      let collab_sv = self.state.state_vector.read().await;
      if *collab_sv <= state_vector {
        return Ok(vec![]);
      }
    }

    let encoded_collab = self.encode_collab().await?;
    let collab = Collab::new_with_source(
      CollabOrigin::Server,
      self.object_id(),
      DataSource::DocStateV1(encoded_collab.doc_state.into()),
      vec![],
      false,
    )?;
    let update = collab.transact().encode_state_as_update_v1(&state_vector);
    Ok(update)
  }

  pub async fn encode_collab(&self) -> Result<EncodedCollab, RealtimeError> {
    let snapshot = self.state.persister.load_compact().await?;
    let encode_collab = snapshot.collab.encode_collab_v1(|collab| {
      self
        .state
        .collab_type
        .validate_require_data(collab)
        .map_err(|err| RealtimeError::CollabSchemaError(err.to_string()))
    })?;
    Ok(encode_collab)
  }

  pub fn contains_user(&self, user: &RealtimeUser) -> bool {
    self.state.subscribers.contains_key(user)
  }

  pub fn remove_user(&self, user: &RealtimeUser) {
    if self.state.subscribers.remove(user).is_some() {
      trace!(
        "{} remove subscriber from group: {}",
        self.state.object_id,
        user
      );
    }
  }

  pub fn user_count(&self) -> usize {
    self.state.subscribers.len()
  }

  pub fn modified_at(&self) -> Instant {
    *self.state.last_activity.load_full()
  }

  /// Subscribes a new connection to the broadcast group for collaborative activities.
  ///
  pub fn subscribe<Sink, Stream>(
    &self,
    user: &RealtimeUser,
    subscriber_origin: CollabOrigin,
    sink: Sink,
    stream: Stream,
  ) where
    Sink: SubscriptionSink + Clone + 'static,
    Stream: SubscriptionStream + 'static,
  {
    // create new subscription for new subscriber
    let subscriber_shutdown = self.state.shutdown.child_token();

    tokio::spawn(Self::receive_from_client_task(
      self.state.clone(),
      sink.clone(),
      stream,
      subscriber_origin.clone(),
    ));

    let sub = Subscription::new(sink, subscriber_origin, subscriber_shutdown);
    if self
      .state
      .subscribers
      .insert((*user).clone(), sub)
      .is_some()
    {
      tracing::warn!("{}: remove old subscriber: {}", &self.state.object_id, user);
    }

    if cfg!(debug_assertions) {
      trace!(
        "{}: add new subscriber, current group member: {}",
        &self.state.object_id,
        self.user_count(),
      );
    }

    trace!(
      "[realtime]:{} new subscriber:{}, connect at:{}, connected members: {}",
      self.state.object_id,
      user.user_device(),
      user.connect_at,
      self.state.subscribers.len(),
    );
  }

  async fn receive_from_client_task<Sink, Stream>(
    state: Arc<CollabGroupState>,
    mut sink: Sink,
    mut stream: Stream,
    origin: CollabOrigin,
  ) where
    Sink: SubscriptionSink + 'static,
    Stream: SubscriptionStream + 'static,
  {
    loop {
      tokio::select! {
        _ = state.shutdown.cancelled() => {
          break;
        }
        msg = stream.next() => {
          match msg {
            None => break,
            Some(msg) => if let Err(err) =  Self::handle_messages(&state, &mut sink, msg).await {
              tracing::warn!(
                "collab `{}` failed to handle message from `{}`: {}",
                state.object_id,
                origin,
                err
              );

            }
          }
        }
      }
    }
  }

  async fn handle_messages<Sink>(
    state: &CollabGroupState,
    sink: &mut Sink,
    msg: MessageByObjectId,
  ) -> Result<(), RealtimeError>
  where
    Sink: SubscriptionSink + 'static,
  {
    for (message_object_id, messages) in msg.0 {
      if state.object_id != message_object_id {
        error!(
          "Expect object id:{} but got:{}",
          state.object_id, message_object_id
        );
        continue;
      }
      for message in messages {
        match Self::handle_client_message(state, message).await {
          Ok(response) => {
            trace!("[realtime]: sending response: {}", response);
            match sink.send(response.into()).await {
              Ok(()) => {},
              Err(err) => {
                trace!("[realtime]: send failed: {}", err);
                break;
              },
            }
          },
          Err(err) => {
            error!(
              "Error handling collab message for object_id: {}: {}",
              message_object_id, err
            );
            break;
          },
        }
      }
    }
    Ok(())
  }

  /// Handle the message sent from the client
  async fn handle_client_message(
    state: &CollabGroupState,
    collab_msg: ClientCollabMessage,
  ) -> Result<CollabAck, RealtimeError> {
    let msg_id = collab_msg.msg_id();
    let message_origin = collab_msg.origin().clone();

    // If the payload is empty, we don't need to apply any updates .
    // Currently, only the ping message should has an empty payload.
    if collab_msg.payload().is_empty() {
      if !matches!(collab_msg, ClientCollabMessage::ClientCollabStateCheck(_)) {
        error!("receive unexpected empty payload message:{}", collab_msg);
      }
      return Ok(CollabAck::new(
        message_origin,
        state.object_id.to_string(),
        msg_id,
        state.seq_no.load(Ordering::SeqCst),
      ));
    }

    trace!(
      "Applying client updates: {}, origin:{}",
      collab_msg,
      message_origin
    );

    let payload = collab_msg.payload();

    // Spawn a blocking task to handle the message
    let result = Self::handle_message(state, payload, &message_origin, msg_id).await;

    match result {
      Ok(inner_result) => match inner_result {
        Some(response) => Ok(response),
        None => Err(RealtimeError::UnexpectedData("No ack response")),
      },
      Err(err) => Err(RealtimeError::Internal(anyhow!(
        "fail to handle message:{}",
        err
      ))),
    }
  }

  async fn handle_message(
    state: &CollabGroupState,
    payload: &[u8],
    message_origin: &CollabOrigin,
    msg_id: MsgId,
  ) -> Result<Option<CollabAck>, RealtimeError> {
    let mut decoder = DecoderV1::from(payload);
    let reader = MessageReader::new(&mut decoder);
    let mut ack_response = None;
    for msg in reader {
      match msg {
        Ok(msg) => {
          match Self::handle_protocol_message(state, message_origin, msg).await {
            Ok(payload) => {
              // One ClientCollabMessage can have multiple Yrs [Message] in it, but we only need to
              // send one ack back to the client.
              if ack_response.is_none() {
                ack_response = Some(
                  CollabAck::new(
                    CollabOrigin::Server,
                    state.object_id.to_string(),
                    msg_id,
                    state.seq_no.load(Ordering::SeqCst),
                  )
                  .with_payload(payload.unwrap_or_default()),
                );
              }
            },
            Err(err) => {
              tracing::warn!("[realtime]: failed to handled message: {}", msg_id);
              state.metrics.apply_update_failed_count.inc();

              let code = Self::ack_code_from_error(&err);
              let payload = match err {
                RTProtocolError::MissUpdates {
                  state_vector_v1,
                  reason: _,
                } => state_vector_v1.unwrap_or_default(),
                _ => vec![],
              };

              ack_response = Some(
                CollabAck::new(
                  CollabOrigin::Server,
                  state.object_id.to_string(),
                  msg_id,
                  state.seq_no.load(Ordering::SeqCst),
                )
                .with_code(code)
                .with_payload(payload),
              );

              break;
            },
          }
        },
        Err(e) => {
          error!("{} => parse sync message failed: {:?}", state.object_id, e);
          break;
        },
      }
    }
    Ok(ack_response)
  }

  async fn handle_protocol_message(
    state: &CollabGroupState,
    origin: &CollabOrigin,
    msg: Message,
  ) -> Result<Option<Vec<u8>>, RTProtocolError> {
    match msg {
      Message::Sync(msg) => match msg {
        SyncMessage::SyncStep1(sv) => Self::handle_sync_step1(state, &sv).await,
        SyncMessage::SyncStep2(update) => Self::handle_sync_step2(state, origin, update).await,
        SyncMessage::Update(update) => Self::handle_update(state, origin, update).await,
      },
      //FIXME: where is the QueryAwareness protocol?
      Message::Awareness(update) => Self::handle_awareness_update(state, origin, update).await,
      Message::Auth(_reason) => Ok(None),
      Message::Custom(_msg) => Ok(None),
    }
  }

  async fn handle_sync_step1(
    state: &CollabGroupState,
    remote_sv: &StateVector,
  ) -> Result<Option<Vec<u8>>, RTProtocolError> {
    if let Ok(sv) = state.state_vector.try_read() {
      // we optimistically try to obtain state vector lock for a fast track:
      // if we remote sv is up-to-date with current one, we don't need to do anything
      match sv.partial_cmp(remote_sv) {
        Some(std::cmp::Ordering::Equal) => return Ok(None), // client and server are in sync
        Some(std::cmp::Ordering::Less) => {
          // server is behind client
          let msg = Message::Sync(SyncMessage::SyncStep1(sv.clone()));
          return Ok(Some(msg.encode_v1()));
        },
        Some(std::cmp::Ordering::Greater) | None => { /* server has some new updates */ },
      }
    }

    // we need to reconstruct document state on the server side
    tracing::debug!("loading collab {}", state.object_id);
    let snapshot = state
      .persister
      .load_compact()
      .await
      .map_err(|err| RTProtocolError::Internal(err.into()))?;

    // prepare document state update and state vector
    let tx = snapshot.collab.transact();
    let doc_state = tx.encode_state_as_update_v1(remote_sv);
    let local_sv = tx.state_vector();
    drop(tx);

    // Retrieve the latest document state from the client after they return online from offline editing.
    tracing::trace!("sending missing data to client ({} bytes)", doc_state.len());
    let mut encoder = EncoderV1::new();
    Message::Sync(SyncMessage::SyncStep2(doc_state)).encode(&mut encoder);
    //FIXME: this should never happen as response to sync step 1 from the client, but rather be
    //  send when a connection is established
    Message::Sync(SyncMessage::SyncStep1(local_sv)).encode(&mut encoder);
    Ok(Some(encoder.to_vec()))
  }

  async fn handle_sync_step2(
    state: &CollabGroupState,
    origin: &CollabOrigin,
    update: Vec<u8>,
  ) -> Result<Option<Vec<u8>>, RTProtocolError> {
    state.metrics.collab_size.observe(update.len() as f64);

    let start = tokio::time::Instant::now();
    // we try to decode update to make sure it's not malformed and to extract state vector
    let (update, decoded_update) = if update.len() <= collab_rt_protocol::LARGE_UPDATE_THRESHOLD {
      let decoded_update = Update::decode_v1(&update)?;
      (update, decoded_update)
    } else {
      tokio::task::spawn_blocking(move || {
        let decoded_update = Update::decode_v1(&update)?;
        Ok::<(Vec<u8>, yrs::Update), yrs::encoding::read::Error>((update, decoded_update))
      })
      .await
      .map_err(|err| RTProtocolError::Internal(err.into()))??
    };
    let missing_updates = {
      let state_vector = state.state_vector.read().await;
      match state_vector.partial_cmp(&decoded_update.state_vector_lower()) {
        None | Some(std::cmp::Ordering::Less) => Some(state_vector.clone()),
        _ => None,
      }
    };

    if let Some(missing_updates) = missing_updates {
      let msg = Message::Sync(SyncMessage::SyncStep1(missing_updates));
      tracing::debug!("subscriber {} send update with missing data", origin);
      Ok(Some(msg.encode_v1()))
    } else {
      state
        .persister
        .send_update(origin.clone(), update)
        .await
        .map_err(|err| RTProtocolError::Internal(err.into()))?;
      let elapsed = start.elapsed();

      state
        .metrics
        .load_collab_time
        .observe(elapsed.as_millis() as f64);

      Ok(None)
    }
  }

  async fn handle_update(
    state: &CollabGroupState,
    origin: &CollabOrigin,
    update: Vec<u8>,
  ) -> Result<Option<Vec<u8>>, RTProtocolError> {
    Self::handle_sync_step2(state, origin, update).await
  }

  async fn handle_awareness_update(
    state: &CollabGroupState,
    origin: &CollabOrigin,
    update: Vec<u8>,
  ) -> Result<Option<Vec<u8>>, RTProtocolError> {
    state
      .persister
      .send_awareness(origin, update)
      .await
      .map_err(|err| RTProtocolError::Internal(err.into()))?;
    Ok(None)
  }

  #[inline]
  fn ack_code_from_error(error: &RTProtocolError) -> AckCode {
    match error {
      RTProtocolError::YrsTransaction(_) => AckCode::Retry,
      RTProtocolError::YrsApplyUpdate(_) => AckCode::CannotApplyUpdate,
      RTProtocolError::YrsEncodeState(_) => AckCode::EncodeStateAsUpdateFail,
      RTProtocolError::MissUpdates { .. } => AckCode::MissUpdate,
      _ => AckCode::Internal,
    }
  }

  /// Check if the group is active. A group is considered active if it has at least one
  /// subscriber
  pub fn is_inactive(&self) -> bool {
    let modified_at = self.modified_at();
    let elapsed_secs = modified_at.elapsed().as_secs();
    // Mark the group as inactive if it has been inactive for more than 3 hours, regardless of the number of subscribers.
    // Otherwise, return `true` only if there are no subscribers remaining in the group.
    // If a client modifies a group that has already been marked as inactive (removed),
    // the client will automatically send an initialization sync to reinitialize the group.
    const MAXIMUM_SECS: u64 = 3 * 60 * 60;
    if elapsed_secs > MAXIMUM_SECS {
      info!(
        "Group:{}:{} is inactive for {} seconds, subscribers: {}",
        self.state.object_id,
        self.state.collab_type,
        modified_at.elapsed().as_secs(),
        self.state.subscribers.len()
      );
      true
    } else {
      self.state.subscribers.is_empty()
    }
  }
}

pub trait SubscriptionSink:
  Sink<CollabMessage, Error = RealtimeError> + Send + Sync + Unpin
{
}
impl<T> SubscriptionSink for T where
  T: Sink<CollabMessage, Error = RealtimeError> + Send + Sync + Unpin
{
}

pub trait SubscriptionStream: Stream<Item = MessageByObjectId> + Send + Sync + Unpin {}
impl<T> SubscriptionStream for T where T: Stream<Item = MessageByObjectId> + Send + Sync + Unpin {}

struct Subscription {
  collab_origin: CollabOrigin,
  sink: Box<dyn SubscriptionSink>,
  shutdown: CancellationToken,
}

impl Subscription {
  fn new<S>(sink: S, collab_origin: CollabOrigin, shutdown: CancellationToken) -> Self
  where
    S: SubscriptionSink + 'static,
  {
    Subscription {
      sink: Box::new(sink),
      collab_origin,
      shutdown,
    }
  }
}

impl Drop for Subscription {
  fn drop(&mut self) {
    tracing::trace!("closing subscription: {}", self.collab_origin);
    self.shutdown.cancel();
  }
}

struct CollabPersister {
  uid: i64,
  workspace_id: String,
  object_id: String,
  collab_type: CollabType,
  storage: Arc<dyn CollabStorage>,
  collab_redis_stream: Arc<CollabRedisStream>,
  indexer_scheduler: Arc<IndexerScheduler>,
  metrics: Arc<CollabRealtimeMetrics>,
  update_sink: CollabUpdateSink,
  awareness_sink: AwarenessUpdateSink,
  /// A grace period for prunning Redis collab updates. Instead of deleting all messages we
  /// read right away, we give 1min for other potential client to catch up.
  prune_grace_period: Duration,
}

impl CollabPersister {
  #[allow(clippy::too_many_arguments)]
  pub fn new(
    uid: i64,
    workspace_id: String,
    object_id: String,
    collab_type: CollabType,
    storage: Arc<dyn CollabStorage>,
    collab_redis_stream: Arc<CollabRedisStream>,
    indexer_scheduler: Arc<IndexerScheduler>,
    metrics: Arc<CollabRealtimeMetrics>,
    prune_grace_period: Duration,
  ) -> Self {
    let update_sink = collab_redis_stream.collab_update_sink(&workspace_id, &object_id);
    let awareness_sink = collab_redis_stream.awareness_update_sink(&workspace_id, &object_id);
    Self {
      uid,
      workspace_id,
      object_id,
      collab_type,
      storage,
      collab_redis_stream,
      indexer_scheduler,
      metrics,
      update_sink,
      awareness_sink,
      prune_grace_period,
    }
  }

  async fn send_update(
    &self,
    sender: CollabOrigin,
    update: Vec<u8>,
  ) -> Result<MessageId, StreamError> {
    let len = update.len();
    // send updates to redis queue
    let update = CollabStreamUpdate::new(update, sender, UpdateFlags::default());
    let msg_id = self.update_sink.send(&update).await?;
    tracing::trace!(
      "persisted update from {} ({} bytes) - msg id: {}",
      update.sender,
      len,
      msg_id
    );
    Ok(msg_id)
  }

  async fn send_awareness(
    &self,
    sender_session: &CollabOrigin,
    awareness_update: Vec<u8>,
  ) -> Result<MessageId, StreamError> {
    // send awareness updates to redis queue:
    // QUESTION: is it needed? Maybe we could reuse update_sink?
    let len = awareness_update.len();
    let update = AwarenessStreamUpdate {
      data: awareness_update,
      sender: sender_session.clone(),
    };
    let msg_id = self.awareness_sink.send(&update).await?;
    tracing::trace!(
      "persisted awareness from {} ({} bytes) - msg id: {}",
      update.sender,
      len,
      msg_id
    );
    Ok(msg_id)
  }

  /// Loads collab without its history. Used for handling y-sync protocol messages.
  async fn load_compact(&self) -> Result<CollabSnapshot, RealtimeError> {
    tracing::trace!("requested to load compact collab {}", self.object_id);
    // 1. Try to load the latest snapshot from storage
    let start = Instant::now();
    let mut collab = match self.load_collab_full().await? {
      Some(collab) => collab,
      None => Collab::new_with_origin(CollabOrigin::Server, self.object_id.clone(), vec![], false),
    };
    self.metrics.load_collab_count.inc();

    // 2. consume all Redis updates on top of it (keep redis msg id)
    let mut last_message_id = None;
    let mut tx = collab.transact_mut();
    let updates = self
      .collab_redis_stream
      .current_collab_updates(
        &self.workspace_id,
        &self.object_id,
        None, //TODO: store Redis last msg id somewhere in doc state snapshot and replay from there
      )
      .await?;
    let mut i = 0;
    for (message_id, update) in updates {
      i += 1;
      let update: Update = update.into_update()?;
      tx.apply_update(update)
        .map_err(|err| RTProtocolError::YrsApplyUpdate(err.to_string()))?;
      last_message_id = Some(message_id); //TODO: shouldn't this happen before decoding?
      self.metrics.apply_update_count.inc();
    }
    drop(tx);
    tracing::trace!(
      "loaded collab compact state: {} replaying {} updates",
      self.object_id,
      i
    );
    self
      .metrics
      .load_collab_time
      .observe(start.elapsed().as_millis() as f64);

    // now we have the most recent version of the document
    let snapshot = CollabSnapshot {
      collab,
      last_message_id,
    };
    Ok(snapshot)
  }

  /// Returns a collab state (with GC turned off), but only if there were any pending updates
  /// waiting to be merged into main document state.
  async fn load_if_changed(&self) -> Result<Option<CollabSnapshot>, RealtimeError> {
    // 1. load pending Redis updates
    let updates = self
      .collab_redis_stream
      .current_collab_updates(&self.workspace_id, &self.object_id, None)
      .await?;

    let start = Instant::now();
    let mut i = 0;
    let mut collab = None;
    let mut last_message_id = None;
    for (message_id, update) in updates {
      i += 1;
      let update: Update = update.into_update()?;
      if collab.is_none() {
        collab = Some(match self.load_collab_full().await? {
          Some(collab) => collab,
          None => {
            Collab::new_with_origin(CollabOrigin::Server, self.object_id.clone(), vec![], false)
          },
        })
      };
      let collab = collab.as_mut().unwrap();
      collab
        .transact_mut()
        .apply_update(update)
        .map_err(|err| RTProtocolError::YrsApplyUpdate(err.to_string()))?;
      last_message_id = Some(message_id); //TODO: shouldn't this happen before decoding?
      self.metrics.apply_update_count.inc();
    }

    // if there were no Redis updates, collab is still not initialized
    match collab {
      Some(collab) => {
        self.metrics.load_full_collab_count.inc();
        let elapsed = start.elapsed();
        self
          .metrics
          .load_collab_time
          .observe(elapsed.as_millis() as f64);
        tracing::trace!(
          "loaded collab full state: {} replaying {} updates in {:?}",
          self.object_id,
          i,
          elapsed
        );
        {
          let tx = collab.transact();
          if tx.store().pending_update().is_some() || tx.store().pending_ds().is_some() {
            tracing::trace!(
              "loaded collab {} is incomplete: has pending data",
              self.object_id
            );
          }
        }
        Ok(Some(CollabSnapshot {
          collab,
          last_message_id,
        }))
      },
      None => Ok(None),
    }
  }

  async fn save(&self) -> Result<(), RealtimeError> {
    // load collab but only if there were pending updates in Redis
    if let Some(mut snapshot) = self.load_if_changed().await? {
      tracing::debug!("requesting save for collab {}", self.object_id);
      if let Some(message_id) = snapshot.last_message_id {
        // non-nil message_id means that we had to update the most recent collab state snapshot
        // with new updates from Redis. This means that our snapshot state is newer than the last
        // persisted one in the database
        self.save_attempt(&mut snapshot.collab, message_id).await?;
      }
    } else {
      tracing::trace!("collab {} state has not changed", self.object_id);
    }
    Ok(())
  }

  /// Tries to save provided `snapshot`. This snapshot is expected to have **GC turned off**, as
  /// first it will try to save it as a historical snapshot (will all updates available), then it
  /// will generate another (compact) snapshot variant that will be used as main one for loading
  /// for the sake of y-sync protocol.
  async fn save_attempt(
    &self,
    collab: &mut Collab,
    message_id: MessageId,
  ) -> Result<(), RealtimeError> {
    // try to acquire snapshot lease - it's possible that multiple web services will try to
    // perform snapshot at the same time, so we'll use lease to let only one of them atm.
    if let Some(mut lease) = self
      .collab_redis_stream
      .lease(&self.workspace_id, &self.object_id)
      .await?
    {
      let doc_state_light = collab
        .transact()
        .encode_state_as_update_v1(&StateVector::default());
      let light_len = doc_state_light.len();
      self.write_collab(doc_state_light).await?;

      match self.collab_type {
        CollabType::Document => {
          let txn = collab.transact();
          if let Some(text) = DocumentBody::from_collab(collab).map(|body| body.paragraphs(txn)) {
            self.index_collab_content(text);
          }
        },
        _ => {
          // TODO(nathan): support other collab type
        },
      }

      tracing::debug!(
        "persisted collab {} snapshot at {}: {} bytes",
        self.object_id,
        message_id,
        light_len
      );

      // 3. finally we can drop Redis messages
      let now = SystemTime::UNIX_EPOCH.elapsed().unwrap().as_millis();
      let msg_id = MessageId {
        timestamp_ms: (now - self.prune_grace_period.as_millis()) as u64,
        sequence_number: 0,
      };
      let stream_key = CollabStreamUpdate::stream_key(&self.workspace_id, &self.object_id);
      self
        .collab_redis_stream
        .prune_update_stream(&stream_key, msg_id)
        .await?;

      let _ = lease.release().await;
    }

    Ok(())
  }

  async fn trim_awareness(&self) -> Result<(), RealtimeError> {
    let stream_key = AwarenessStreamUpdate::stream_key(&self.workspace_id, &self.object_id);
    self
      .collab_redis_stream
      .prune_awareness_stream(&stream_key)
      .await?;
    Ok(())
  }

  async fn write_collab(&self, doc_state_v1: Vec<u8>) -> Result<(), RealtimeError> {
    let encoded_collab = EncodedCollab::new_v1(Default::default(), doc_state_v1)
      .encode_to_bytes()
      .map(Bytes::from)
      .map_err(|err| RealtimeError::BincodeEncode(err.to_string()))?;
    self
      .metrics
      .collab_size
      .observe(encoded_collab.len() as f64);
    let params = CollabParams::new(&self.object_id, self.collab_type.clone(), encoded_collab);
    self
      .storage
      .queue_insert_or_update_collab(&self.workspace_id, &self.uid, params, true)
      .await
      .map_err(|err| RealtimeError::Internal(err.into()))?;
    Ok(())
  }

  fn index_collab_content(&self, paragraphs: Vec<String>) {
    if let Ok(workspace_id) = Uuid::parse_str(&self.workspace_id) {
      let indexed_collab = UnindexedCollabTask::new(
        workspace_id,
        self.object_id.clone(),
        self.collab_type.clone(),
        UnindexedData::Paragraphs(paragraphs),
      );
      if let Err(err) = self
        .indexer_scheduler
        .index_pending_collab_one(indexed_collab, false)
      {
        tracing::warn!(
          "failed to index collab `{}/{}`: {}",
          self.workspace_id,
          self.object_id,
          err
        );
      }
    }
  }

  async fn load_collab_full(&self) -> Result<Option<Collab>, RealtimeError> {
    // we didn't find a snapshot, or we want a lightweight collab version
    let params = QueryCollabParams::new(
      self.object_id.clone(),
      self.collab_type.clone(),
      self.workspace_id.clone(),
    );
    let result = self
      .storage
      .get_encode_collab(GetCollabOrigin::Server, params, false)
      .await;
    let doc_state = match result {
      Ok(encoded_collab) => encoded_collab.doc_state,
      Err(AppError::RecordNotFound(_)) => return Ok(None),
      Err(err) => return Err(RealtimeError::Internal(err.into())),
    };

    let collab: Collab = Collab::new_with_source(
      CollabOrigin::Server,
      &self.object_id,
      DataSource::DocStateV1(doc_state.into()),
      vec![],
      false,
    )?;
    Ok(Some(collab))
  }
}

pub struct CollabSnapshot {
  pub collab: Collab,
  pub last_message_id: Option<MessageId>,
}
