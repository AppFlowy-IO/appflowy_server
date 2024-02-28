use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::collab_sync::sink_queue::{MessageState, SinkQueue};
use crate::collab_sync::{SyncError, SyncObject};
use futures_util::SinkExt;

use crate::af_spawn;
use crate::collab_sync::sink_config::SinkConfig;
use realtime_entity::collab_msg::{CollabSinkMessage, MsgId, ServerCollabMessage};
use tokio::sync::{oneshot, watch, Mutex};
use tokio::time::interval;
use tracing::{debug, error, trace, warn};

#[derive(Clone, Debug)]
pub enum SinkState {
  Init,
  /// The sink is syncing the messages to the remote.
  Syncing,
  /// All the messages are synced to the remote.
  Finished,
  Pause,
}

impl SinkState {
  pub fn is_init(&self) -> bool {
    matches!(self, SinkState::Init)
  }
}

/// Use to sync the [Msg] to the remote.
pub struct CollabSink<Sink, Msg> {
  uid: i64,
  /// The [Sink] is used to send the messages to the remote. It might be a websocket sink or
  /// other sink that implements the [SinkExt] trait.
  sender: Arc<Mutex<Sink>>,
  /// The [SinkQueue] is used to queue the messages that are waiting to be sent to the
  /// remote. It will merge the messages if possible.
  message_queue: Arc<parking_lot::Mutex<SinkQueue<Msg>>>,
  msg_id_counter: Arc<DefaultMsgIdCounter>,
  /// The [watch::Sender] is used to notify the [CollabSinkRunner] to process the pending messages.
  /// Sending `false` will stop the [CollabSinkRunner].
  notifier: Arc<watch::Sender<bool>>,
  config: SinkConfig,
  state_notifier: Arc<watch::Sender<SinkState>>,
  pause: AtomicBool,
  object: SyncObject,
}

impl<Sink, Msg> Drop for CollabSink<Sink, Msg> {
  fn drop(&mut self) {
    trace!("Drop CollabSink {}", self.object.object_id);
    let _ = self.notifier.send(true);
  }
}

impl<E, Sink, Msg> CollabSink<Sink, Msg>
where
  E: Into<anyhow::Error> + Send + Sync + 'static,
  Sink: SinkExt<Msg, Error = E> + Send + Sync + Unpin + 'static,
  Msg: CollabSinkMessage,
{
  pub fn new(
    uid: i64,
    object: SyncObject,
    sink: Sink,
    notifier: watch::Sender<bool>,
    sync_state_tx: watch::Sender<SinkState>,
    config: SinkConfig,
    pause: bool,
  ) -> Self {
    let msg_id_counter = DefaultMsgIdCounter::new();
    let notifier = Arc::new(notifier);
    let state_notifier = Arc::new(sync_state_tx);
    let sender = Arc::new(Mutex::new(sink));
    let msg_queue = SinkQueue::new(uid);
    let message_queue = Arc::new(parking_lot::Mutex::new(msg_queue));
    let msg_id_counter = Arc::new(msg_id_counter);
    Self {
      uid,
      sender,
      message_queue,
      msg_id_counter,
      notifier,
      state_notifier,
      config,
      pause: AtomicBool::new(pause),
      object,
    }
  }

  /// Put the message into the queue and notify the sink to process the next message.
  /// After the [Msg] was pushed into the [SinkQueue]. The queue will pop the next msg base on
  /// its priority. And the message priority is determined by the [Msg] that implement the [Ord] and
  /// [PartialOrd] trait. Check out the [CollabMessage] for more details.
  ///
  pub fn queue_msg(&self, f: impl FnOnce(MsgId) -> Msg) {
    {
      let mut msg_queue = self.message_queue.lock();
      let msg_id = self.msg_id_counter.next();
      let msg = f(msg_id);
      msg_queue.push_msg(msg_id, msg);
      drop(msg_queue);
    }

    self.notify();
  }

  /// When queue the init message, the sink will clear all the pending messages and send the init
  /// message immediately.
  pub fn queue_init_sync(&self, f: impl FnOnce(MsgId) -> Msg) {
    // When the client is connected, remove all pending messages and send the init message.
    {
      let mut msg_queue = self.message_queue.lock();
      // if there is an init message in the queue, return;
      if let Some(msg) = msg_queue.peek() {
        if msg.get_msg().is_init_msg() {
          return;
        }
      }
      msg_queue.clear();
      let msg_id = self.msg_id_counter.next();
      let msg = f(msg_id);
      msg_queue.push_msg(msg_id, msg);
      drop(msg_queue);
    }

    self.notify();
  }

  pub fn can_queue_init_sync(&self) -> bool {
    let msg_queue = self.message_queue.lock();
    if let Some(msg) = msg_queue.peek() {
      if msg.get_msg().is_init_msg() {
        return false;
      }
    }
    true
  }

  pub fn clear(&self) {
    self.message_queue.lock().clear();
  }

  pub fn pause(&self) {
    self.pause.store(true, Ordering::SeqCst);
    let _ = self.state_notifier.send(SinkState::Pause);
  }

  pub fn resume(&self) {
    self.pause.store(false, Ordering::SeqCst);
    self.notify();
  }

  /// Notify the sink to process the next message and mark the current message as done.
  pub async fn ack_msg(&self, msg: &ServerCollabMessage) -> bool {
    // the msg_id will be None if the message is [ServerBroadcast] or [ServerAwareness]
    match msg.msg_id() {
      None => true,
      Some(msg_id) => {
        match self.message_queue.lock().peek_mut() {
          None => false,
          Some(mut pending_msg) => {
            // In most cases, the msg_id of the pending_msg is the same as the passed-in msg_id. However,
            // due to network issues, the client might send multiple messages with the same msg_id.
            // Therefore, the msg_id might not always match the msg_id of the pending_msg.
            if pending_msg.msg_id() != msg_id {
              return false;
            }

            let is_done = pending_msg.set_state(self.uid, MessageState::Done);
            if is_done {
              self.notify();
            }
            is_done
          },
        }
      },
    }
  }

  async fn process_next_msg(&self) -> Result<(), SyncError> {
    if self.pause.load(Ordering::SeqCst) {
      return Ok(());
    }

    self.send_msg_immediately().await;
    Ok(())
  }

  async fn send_msg_immediately(&self) -> Option<()> {
    let (tx, rx) = oneshot::channel();
    let collab_msg = {
      let (mut msg_queue, mut sending_msg) = match self.message_queue.try_lock() {
        None => {
          // If acquire the lock failed, try to notify again after 100ms
          retry_later(Arc::downgrade(&self.notifier));
          None
        },
        Some(mut msg_queue) => msg_queue.pop().map(|sending_msg| (msg_queue, sending_msg)),
      }?;
      if sending_msg.state().is_done() {
        // Notify to process the next pending message
        self.notify();
        return None;
      }

      // Do nothing if the message is still processing.
      if sending_msg.state().is_processing() {
        return None;
      }

      let mut merged_msg = vec![];
      // If the message can merge other messages, try to merge the next message until the
      // message is not mergeable.
      if sending_msg.can_merge() {
        while let Some(pending_msg) = msg_queue.pop() {
          // If the message is not mergeable, push the message back to the queue and break the loop.
          match sending_msg.merge(&pending_msg, &self.config.maximum_payload_size) {
            Ok(continue_merge) => {
              merged_msg.push(pending_msg.msg_id());
              if !continue_merge {
                break;
              }
            },
            Err(err) => {
              msg_queue.push(pending_msg);
              error!("Failed to merge message: {}", err);
              break;
            },
          }
        }
      }

      sending_msg.set_ret(tx);
      sending_msg.set_state(self.uid, MessageState::Processing);

      let _ = self.state_notifier.send(SinkState::Syncing);
      let collab_msg = sending_msg.get_msg().clone();
      msg_queue.push(sending_msg);
      collab_msg
    };

    let payload_len = collab_msg.payload_len();
    match self.sender.try_lock() {
      Ok(mut sender) => {
        debug!("Sending {}", collab_msg);
        if let Err(err) = sender.send(collab_msg).await {
          error!("Failed to send error: {:?}", err.into());
          return None;
        }
      },
      Err(_) => {
        warn!("Failed to acquire the lock of the sink, retry later");
        retry_later(Arc::downgrade(&self.notifier));
        return None;
      },
    }
    let timeout_duration = calculate_timeout(payload_len, self.config.send_timeout);
    // Wait for the message to be acked.
    // If the message is not acked within the timeout, resend the message.
    match tokio::time::timeout(timeout_duration, rx).await {
      Ok(result) => {
        match result {
          Ok(_) => match self.message_queue.try_lock() {
            None => warn!("Failed to acquire the lock of the msg_queue"),
            Some(mut msg_queue) => {
              let msg = msg_queue.pop();
              trace!(
                "{:?}: Pending messages: {}",
                msg.map(|msg| msg.object_id().to_owned()),
                msg_queue.len()
              );
              if msg_queue.is_empty() {
                if let Err(e) = self.state_notifier.send(SinkState::Finished) {
                  error!("send sink state failed: {}", e);
                }
              }
            },
          },
          Err(err) => {
            // the error might be caused by the sending message was removed from the queue.
            trace!("pending message oneshot channel error: {}", err)
          },
        }

        self.notify()
      },
      Err(_) => {
        if let Some(mut pending_msg) = self.message_queue.lock().peek_mut() {
          pending_msg.set_state(self.uid, MessageState::Timeout);
        }
        self.notify();
      },
    }
    None
  }

  /// Notify the sink to process the next message.
  pub(crate) fn notify(&self) {
    let _ = self.notifier.send(false);
  }
}

fn retry_later(weak_notifier: Weak<watch::Sender<bool>>) {
  af_spawn(async move {
    interval(Duration::from_millis(100)).tick().await;
    if let Some(notifier) = weak_notifier.upgrade() {
      let _ = notifier.send(false);
    }
  });
}

pub struct CollabSinkRunner<Msg>(PhantomData<Msg>);

impl<Msg> CollabSinkRunner<Msg> {
  /// The runner will stop if the [CollabSink] was dropped or the notifier was closed.
  pub async fn run<E, Sink>(
    weak_sink: Weak<CollabSink<Sink, Msg>>,
    mut notifier: watch::Receiver<bool>,
  ) where
    E: Into<anyhow::Error> + Send + Sync + 'static,
    Sink: SinkExt<Msg, Error = E> + Send + Sync + Unpin + 'static,
    Msg: CollabSinkMessage,
  {
    if let Some(sink) = weak_sink.upgrade() {
      sink.notify();
    }
    loop {
      // stops the runner if the notifier was closed.
      if notifier.changed().await.is_err() {
        break;
      }

      // stops the runner if the value of notifier is `true`
      if *notifier.borrow() {
        break;
      }

      if let Some(sync_sink) = weak_sink.upgrade() {
        let _ = sync_sink.process_next_msg().await;
      } else {
        break;
      }
    }
  }
}

fn calculate_timeout(payload_len: usize, default: Duration) -> Duration {
  match payload_len {
    0..=40959 => default,
    40960..=1048576 => Duration::from_secs(10),
    1048577..=2097152 => Duration::from_secs(20),
    2097153..=4194304 => Duration::from_secs(50),
    _ => Duration::from_secs(160),
  }
}

pub trait MsgIdCounter: Send + Sync + 'static {
  /// Get the next message id. The message id should be unique.
  fn next(&self) -> MsgId;
}

#[derive(Debug, Default)]
pub struct DefaultMsgIdCounter(Arc<AtomicU64>);

impl DefaultMsgIdCounter {
  pub fn new() -> Self {
    Self::default()
  }
  fn next(&self) -> MsgId {
    self.0.fetch_add(1, Ordering::SeqCst)
  }
}
