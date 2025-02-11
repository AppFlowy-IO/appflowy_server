use crate::WorkspaceId;
use actix::{Actor, Handler, Recipient, ResponseFuture};
use bytes::Bytes;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use yrs::block::ClientID;

pub struct WsServer {
  sessions: HashMap<ClientID, Recipient<super::session::Message>>,
  workspaces: HashMap<WorkspaceId, HashSet<ClientID>>,
}

impl WsServer {
  pub fn new() -> Self {
    tracing::trace!("new ws server");
    Self {
      sessions: HashMap::new(),
      workspaces: HashMap::new(),
    }
  }
}

impl Actor for WsServer {
  type Context = actix::Context<Self>;
}

impl Handler<Join> for WsServer {
  type Result = ();

  fn handle(&mut self, msg: Join, _ctx: &mut Self::Context) -> Self::Result {
    self.sessions.insert(msg.session_id, msg.addr);
    match self.workspaces.entry(msg.workspace) {
      Entry::Occupied(mut e) => {
        if e.get_mut().insert(msg.session_id) {
          tracing::trace!("attached session `{}`", msg.session_id);
        }
      },
      Entry::Vacant(e) => {
        let sessions = e.insert(HashSet::new());
        if sessions.insert(msg.session_id) {
          tracing::trace!("attached session `{}`", msg.session_id);
        }
      },
    }
  }
}

impl Handler<Leave> for WsServer {
  type Result = ();

  fn handle(&mut self, msg: Leave, _ctx: &mut Self::Context) -> Self::Result {
    if self.sessions.remove(&msg.session_id).is_some() {
      let mut empty_topics = Vec::new();
      for (doc_id, sessions) in self.workspaces.iter_mut() {
        sessions.remove(&msg.session_id);
        tracing::trace!(
          "detached session `{}` from doc `{}`",
          msg.session_id,
          doc_id
        );
        if sessions.is_empty() {
          empty_topics.push(doc_id.clone());
        }
      }

      for topic in empty_topics {
        self.workspaces.remove(&topic);
      }
    }
  }
}

impl Handler<ClientMessage> for WsServer {
  type Result = ResponseFuture<()>;

  fn handle(&mut self, msg: ClientMessage, _ctx: &mut Self::Context) -> Self::Result {
    let mut tasks = Vec::new();
    let doc_id = msg.doc_id();
    let sessions = self.workspaces.entry(doc_id).or_default();
    if sessions.insert(msg.sender) {
      tracing::trace!("attaching `{}` to document `{}`", msg.sender, doc_id);
    }
    for session in sessions.iter() {
      if session != &msg.sender {
        if let Some(recipient) = self.sessions.get(session) {
          tracing::trace!("sending message to `{}`: {} bytes", session, msg.data.len());
          let fut = recipient.send(super::session::Message(msg.data.clone()));
          tasks.push(fut);
        }
      }
    }

    Box::pin(async {
      //FIXME: for some reason tokio::try_join(tasks) doesn't consider tasks to be Futures
      for t in tasks {
        if let Err(err) = t.await {
          tracing::warn!("failed to send message: {}", err);
        }
      }
    })
  }
}

#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct Join {
  /// Current client session identifier.
  pub session_id: ClientID,
  /// Actix WebSocket session actor address.
  pub addr: Recipient<super::session::Message>,
  /// List of topics to join.
  pub workspace: WorkspaceId,
}

#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct Leave {
  /// Current client session identifier.
  pub session_id: ClientID,
}

#[derive(Debug, actix::Message)]
#[rtype(result = "()")]
pub struct ClientMessage {
  pub sender: ClientID,
  pub data: Bytes,
}

impl ClientMessage {
  pub fn doc_id(&self) -> Uuid {
    Uuid::from_slice(&self.data[..16]).unwrap()
  }
}
