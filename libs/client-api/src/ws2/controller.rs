use super::{ConnectionError, ObjectId, WorkspaceId};
use crate::ws2::collab_cache::CollabCache;
use bytes::Bytes;
use collab::core::collab::CollabContext;
use collab::preclude::Collab;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use smallvec::{smallvec, ExtendFromSlice, SmallVec};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::error::SendError;
use tokio::time::MissedTickBehavior;
use tokio::{join, try_join};
use tokio_stream::wrappers::WatchStream;
use tokio_tungstenite::tungstenite::http;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::{tungstenite, MaybeTlsStream};
use tokio_util::sync::CancellationToken;
use yrs::block::ClientID;
use yrs::encoding::write::Write;
use yrs::sync::SyncMessage;
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{AsyncTransact, ReadTxn, Transaction, TransactionMut, Update};

type WsConn = tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
type CollabUpdateSink = tokio::sync::broadcast::Sender<Message>;
type CollabUpdateStream = tokio::sync::broadcast::Receiver<Message>;

#[derive(Clone)]
struct Message {
  object_id: ObjectId,
  sync_message: yrs::sync::Message,
}

impl Message {
  fn new(object_id: ObjectId, sync_message: yrs::sync::Message) -> Self {
    Message {
      object_id,
      sync_message,
    }
  }

  fn into_bytes(self) -> Vec<u8> {
    let mut encoder = EncoderV1::new();
    encoder.write_all(self.object_id.as_bytes());
    self.sync_message.encode(&mut encoder);
    encoder.to_vec()
  }

  fn from_bytes(data: &[u8]) -> Result<Self, yrs::encoding::read::Error> {
    let object_id = ObjectId::from_slice(data)
      .map_err(|_| yrs::encoding::read::Error::EndOfBuffer(data.len()))?;
    let mut decoder = DecoderV1::from(&data[16..]);
    let sync_message = yrs::sync::Message::decode(&mut decoder)?;
    Ok(Self {
      object_id,
      sync_message,
    })
  }
}

pub struct WorkspaceController {
  inner: Arc<Inner>,
}

impl WorkspaceController {
  pub fn new(options: Options) -> Self {
    WorkspaceController {
      inner: Inner::new(options),
    }
  }

  pub fn status(&self) -> ConnectionStatus {
    self.inner.status_rx.borrow().clone()
  }

  pub fn observe_connection_state(&self) -> WatchStream<ConnectionStatus> {
    WatchStream::new(self.inner.status_rx.clone())
  }

  pub async fn connect(&self) -> Result<(), ConnectionError> {
    {
      let status = self.inner.status_rx.borrow();
      match &*status {
        ConnectionStatus::Connected { .. } => return Ok(()), // already connected
        ConnectionStatus::Connecting { .. } => { /* we're already connecting */ },
        ConnectionStatus::Disconnected { .. } => {
          // send signal to inner that we want to connect
          let cancel = self.inner.shutdown.child_token();
          self
            .inner
            .status_tx
            .send(ConnectionStatus::Connecting { cancel })
            .unwrap();
        },
      }
    }
    self
      .inner
      .status_rx
      .clone()
      .wait_for(|s| matches!(s, ConnectionStatus::Connected { .. }))
      .await
      .unwrap();
    Ok(())
  }

  #[inline]
  pub async fn disconnect(&self) -> Result<(), ConnectionError> {
    self.disconnect_with(None).await
  }

  pub async fn disconnect_with(&self, reason: Option<String>) -> Result<(), ConnectionError> {
    let status = self.inner.status_rx.borrow();
    match &*status {
      ConnectionStatus::Disconnected { .. } => Ok(()),
      ConnectionStatus::Connecting { cancel } | ConnectionStatus::Connected { cancel } => {
        cancel.cancel();
        self.inner.set_disconnected(reason);
        Ok(())
      },
    }
  }

  async fn send_message(ws: &mut WsConn, message: Bytes) -> Result<(), ConnectionError> {
    ws.send(tungstenite::Message::binary(message)).await?;
    Ok(())
  }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Options {
  pub url: String,
  pub workspace_id: WorkspaceId,
  pub auth_token: String,
  pub device_id: String,
  pub client_id: ClientID,
}

struct Inner {
  options: Options,
  status_tx: tokio::sync::watch::Sender<ConnectionStatus>,
  status_rx: tokio::sync::watch::Receiver<ConnectionStatus>,
  message_tx: CollabUpdateSink,
  shutdown: CancellationToken,
  collab_cache: CollabCache,
}

impl Inner {
  const RECONNECT_DELAY: Duration = Duration::from_secs(5);
  const PING_INTERVAL: Duration = Duration::from_secs(4);

  fn new(options: Options) -> Arc<Self> {
    let (status_tx, status_rx) = tokio::sync::watch::channel(ConnectionStatus::default());
    let (message_tx, message_rx) = tokio::sync::broadcast::channel(1000);
    let workspace_id = options.workspace_id.clone();
    let inner = Arc::new(Inner {
      options,
      status_rx,
      status_tx,
      message_tx,
      shutdown: CancellationToken::new(),
      collab_cache: CollabCache::new(workspace_id),
    });
    tokio::spawn(Self::remote_receiver_loop(inner.clone(), message_rx));
    inner
  }

  async fn remote_receiver_loop(inner: Arc<Inner>, mut message_rx: CollabUpdateStream) {
    let mut status_rx = inner.status_rx.clone();
    while status_rx.changed().await.is_ok() {
      // if status was changed to connecting, we retry creating connection
      // otherwise skip the loop iteration and wait for the next status change until we received
      // connecting signal
      let cancel = match status_rx.borrow_and_update().clone() {
        ConnectionStatus::Connecting { cancel } => cancel,
        _ if inner.shutdown.is_cancelled() => {
          tracing::info!("connection manager has been closed");
          inner.set_disconnected(None);
          return;
        },
        _ => continue,
      };
      // connection status changed to connecting => try to establish connection
      match Self::establish_connection(&inner.options, cancel.clone()).await {
        Ok(Some(conn)) => {
          // successfully made a connection
          tracing::debug!("successfully connected to {}", inner.options.url);
          inner.set_connected(cancel.clone());
          let rx = message_rx.resubscribe();
          match Self::handle_connection(inner.clone(), conn, rx, cancel.clone()).await {
            Ok(()) => break, // connection closed gracefully
            Err(err) => {
              // error while sending messages
              tracing::error!("failed to handle messages: {}", err);
            },
          }
        },
        Ok(None) => {
          inner.set_disconnected(None); // connection establishing has been cancelled midway
          break;
        },
        Err(err) => {
          // failed to make a connection, wait and retry
          tracing::error!("failed to establish WebSocket v2 connection: {}", err);
          inner.set_disconnected(Some(err.to_string()));
          tokio::time::sleep(Self::RECONNECT_DELAY).await;
          inner.request_reconnect(); // go to the next loop iteration and retry
        },
      }

      if inner.shutdown.is_cancelled() {
        tracing::info!("connection manager has been closed");
        inner.set_disconnected(None);
        return;
      }
    }
  }

  async fn establish_connection(
    options: &Options,
    cancel: CancellationToken,
  ) -> Result<Option<WsConn>, ConnectionError> {
    let url = format!("{}/ws/v2/{}", options.url, options.workspace_id);
    let req = http::Request::builder()
      .uri(url)
      .header("authorization", &options.auth_token)
      .header("device-id", &options.device_id)
      .header("client-id", &options.client_id.to_string())
      .header("sec-websocket-key", "foo")
      .header("upgrade", "websocket")
      .header("connection", "upgrade")
      .header("sec-websocket-version", 13)
      .body(())?;
    let fut = tokio_tungstenite::connect_async(req);
    tokio::select! {
      res = fut => {
        let (stream, _resp) = res?;
        Ok(Some(stream.into()))
      }
      _ = cancel.cancelled() => {
        Ok(None)
      }
    }
  }

  async fn handle_connection(
    inner: Arc<Inner>,
    mut conn: WsConn,
    messages: CollabUpdateStream,
    cancel: CancellationToken,
  ) -> Result<(), ConnectionError> {
    let (sink, stream) = conn.split();
    let reply_tx = inner.message_tx.clone();
    let send_messages_loop = tokio::spawn(Self::send_messages_loop(sink, messages, cancel.clone()));
    let receive_messages_loop =
      tokio::spawn(Self::receive_messages_loop(inner, stream, reply_tx, cancel));
    let (send_res, recv_res) = try_join!(send_messages_loop, receive_messages_loop).unwrap();
    // if connection ended on its own, check if it was not due to errors
    send_res?;
    recv_res?;
    Ok(())
  }

  /// Loop task used to handle messages received from the server.
  async fn receive_messages_loop(
    inner: Arc<Inner>,
    mut stream: SplitStream<WsConn>,
    mut reply_tx: CollabUpdateSink,
    cancel: CancellationToken,
  ) -> Result<(), ConnectionError> {
    let mut buf = Vec::new();
    while let Some(res) = stream.next().await {
      match res? {
        tungstenite::Message::Binary(bytes) => {
          inner.handle_server_message(bytes, &mut reply_tx)?;
        },
        tungstenite::Message::Text(_) => {
          tracing::warn!("text messages are not supported")
        },
        tungstenite::Message::Ping(_) => { /* do nothing */ },
        tungstenite::Message::Pong(_) => { /* do nothing */ },
        tungstenite::Message::Frame(frame) => {
          buf.extend_from_slice(frame.payload().as_slice());
          if frame.header().is_final {
            let bytes = std::mem::take(&mut buf);
            inner.handle_server_message(bytes, &mut reply_tx)?;
          }
        },
        tungstenite::Message::Close(close) => {
          match close {
            None => tracing::info!("received close request from server"),
            Some(frame) => tracing::info!(
              "received close request from server: {} - {}",
              frame.code,
              frame.reason
            ),
          }
          cancel.cancel();
          break;
        },
      }
    }
    Ok(())
  }

  fn handle_server_message(
    &self,
    data: Vec<u8>,
    reply_tx: &mut CollabUpdateSink,
  ) -> Result<(), ConnectionError> {
    let msg = Message::from_bytes(&data)?;
    let object_id = msg.object_id;
    match msg.sync_message {
      yrs::sync::Message::Sync(SyncMessage::SyncStep1(sv)) => {
        let collab = self.collab_cache.get(object_id)?;
        let tx = collab.transact();
        let update = tx.encode_state_as_update_v1(&sv);
        let reply = Message::new(
          object_id,
          yrs::sync::Message::Sync(SyncMessage::SyncStep2(update)),
        );
        reply_tx.send(reply)?;
      },
      yrs::sync::Message::Sync(SyncMessage::SyncStep2(update))
      | yrs::sync::Message::Sync(SyncMessage::Update(update)) => {
        let mut collab = self.collab_cache.get_mut(object_id)?;
        {
          let mut tx = collab.transact_mut();
          let update = Update::decode_v1(&update)?;
          tx.apply_update(update)?;
        }
        // if after applying update we have detached blocks, we need to ask for missing updates
        Self::check_missing_updates(collab.transact(), object_id, reply_tx)?;
      },
      yrs::sync::Message::Auth(Some(deny_reason)) => {
        tracing::info!(
          "removing collab from local storage {} - reason: {}",
          object_id,
          deny_reason
        );
        self.collab_cache.remove(&object_id)?;
      },
      yrs::sync::Message::Auth(None) => { /* do nothing */ },
      yrs::sync::Message::AwarenessQuery => {
        let collab = self.collab_cache.get(object_id)?;
        let awareness = collab.get_awareness().update()?;
        let reply = Message::new(object_id, yrs::sync::Message::Awareness(awareness));
        reply_tx.send(reply)?;
      },
      yrs::sync::Message::Awareness(awareness_update) => {
        let mut collab = self.collab_cache.get_mut(object_id)?;
        let awareness = collab.get_mut_awareness();
        awareness.apply_update(awareness_update)?
      },
      yrs::sync::Message::Custom(tag, _) => {
        tracing::warn!("unknown message with tag {tag}")
      },
    }
    Ok(())
  }

  fn check_missing_updates(
    tx: Transaction,
    object_id: ObjectId,
    reply_tx: &mut CollabUpdateSink,
  ) -> Result<(), ConnectionError> {
    let store = tx.store();
    if store.pending_update().is_some() || store.pending_ds().is_some() {
      let sv = tx.state_vector();
      let reply = Message::new(
        object_id,
        yrs::sync::Message::Sync(SyncMessage::SyncStep1(sv)),
      );
      reply_tx.send(reply)?;
    }
    Ok(())
  }

  /// Loop task used to handle messages to be sent to the server.
  async fn send_messages_loop(
    mut sink: SplitSink<WsConn, tungstenite::protocol::Message>,
    mut rx: CollabUpdateStream,
    cancel: CancellationToken,
  ) -> Result<(), ConnectionError> {
    let mut keep_alive = tokio::time::interval(Self::PING_INTERVAL);
    keep_alive.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
      tokio::select! {
        res = rx.recv() => {
          if let Ok(msg) = res {
            if let yrs::sync::Message::Sync(SyncMessage::Update(update)) = msg.sync_message {
              // try to eagerly fetch more updates if possible: this way we can merge multiple
              // updates and send them as one bigger message
              let (m1, m2) = Self::eager_prefetch(&mut rx, msg.object_id, update)?;
              sink
                .send(tungstenite::Message::Binary(m1.into_bytes()))
                .await?;
              if let Some(msg) = m2 {
                sink
                  .send(tungstenite::Message::Binary(msg.into_bytes()))
                  .await?;
              }
            } else {
              sink
                .send(tungstenite::Message::Binary(msg.into_bytes()))
                .await?;
            }
            sink.flush().await?;
            keep_alive.reset();
          } else {
            break;
          }
        },
        _ = keep_alive.tick() => {
          // we didn't receive any message for some time, so we send a ping to keep connection alive
          sink.send(tungstenite::Message::Ping(Vec::default())).await?;
          sink.flush().await?;
        }
        _ = cancel.cancelled() => {
          break;
        }
      }
    }
    Ok(())
  }

  /// Given initial [SyncMessage::Update] payload, try to prefetch (without blocking or awaiting) as
  /// many bending messages from the collab stream as possible.
  ///
  /// This is so that we can even the difference between frequent updates coming from the user with
  /// slower responding server by merging a lot of small updates together into a bigger one.
  ///
  /// Returns a compacted update message and (optionally) the first message after it which couldn't
  /// be compacted because it's of different type or doesn't belong to the same collab.
  fn eager_prefetch(
    rx: &mut CollabUpdateStream,
    current_oid: ObjectId,
    buf: Vec<u8>,
  ) -> Result<(Message, Option<Message>), ConnectionError> {
    const SIZE_THRESHOLD: usize = 64 * 1024;
    let mut size_hint = buf.len();
    let mut updates: SmallVec<[Vec<u8>; 1]> = smallvec![buf];
    let mut other = None;
    // try to eagerly fetch more updates if they are already in the queue
    while let Ok(msg) = rx.try_recv() {
      match msg.sync_message {
        yrs::sync::Message::Sync(SyncMessage::Update(update)) if msg.object_id == current_oid => {
          size_hint += update.len();
          // we stack updates together until we reach a non-update message
          updates.push(update);

          if size_hint >= SIZE_THRESHOLD {
            break; // potential size of the update may be over threshold, stop here and send what we have
          }
        },
        _ => {
          // other type of message, we cannot compact updates anymore,
          // so we just prepend the update message and then add new one and send them
          // all together
          other = Some(msg);
          break;
        },
      }
    }
    let compacted = if updates.len() == 1 {
      std::mem::take(&mut updates[0])
    } else {
      tracing::debug!("Compacting {} updates ({} bytes)", updates.len(), size_hint);
      yrs::merge_updates_v1(updates)? // try to compact updates together
    };
    let compacted = Message::new(
      current_oid,
      yrs::sync::Message::Sync(SyncMessage::Update(compacted)),
    );
    Ok((compacted, other))
  }

  /// Connection status change: requesting connection but not yet connected.
  /// Note: DO NOT call it outside the given context - it doesn't cancel current connection.
  fn request_reconnect(&self) {
    let cancel = self.shutdown.child_token();
    self
      .status_tx
      .send(ConnectionStatus::Connecting { cancel })
      .unwrap();
  }

  /// Connection status change: connected to server and ready to operate.
  fn set_connected(&self, cancel: CancellationToken) {
    self
      .status_tx
      .send(ConnectionStatus::Connected { cancel })
      .unwrap();
  }

  /// Connection status change: disconnected from the server, either on demand (reason `None`) or
  /// due to some failure (reason provided).
  fn set_disconnected(&self, reason: Option<String>) {
    self
      .status_tx
      .send(ConnectionStatus::Disconnected {
        reason: reason.map(Into::into),
      })
      .unwrap();
  }
}

impl Drop for Inner {
  fn drop(&mut self) {
    self.shutdown.cancel();
  }
}

#[derive(Debug, Clone)]
pub enum ConnectionStatus {
  Disconnected { reason: Option<Arc<str>> },
  Connecting { cancel: CancellationToken },
  Connected { cancel: CancellationToken },
}

impl Default for ConnectionStatus {
  fn default() -> Self {
    ConnectionStatus::Disconnected { reason: None }
  }
}

impl Display for ConnectionStatus {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    match self {
      ConnectionStatus::Disconnected { reason: None } => write!(f, "disconnected"),
      ConnectionStatus::Disconnected {
        reason: Some(reason),
      } => write!(f, "disconnected: {}", reason),
      ConnectionStatus::Connecting { .. } => write!(f, "connecting"),
      ConnectionStatus::Connected { .. } => write!(f, "connected"),
    }
  }
}

impl From<SendError<Message>> for ConnectionError {
  fn from(_: SendError<Message>) -> Self {
    ConnectionError::Closed
  }
}
