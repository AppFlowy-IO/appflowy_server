use tokio::sync::broadcast::{channel, Receiver, Sender};

pub struct ConnectStateNotify {
  pub(crate) state: ConnectState,
  sender: Sender<ConnectState>,
}

impl ConnectStateNotify {
  pub(crate) fn new() -> Self {
    let (sender, _) = channel(100);
    Self {
      state: ConnectState::Disconnected,
      sender,
    }
  }

  pub(crate) fn set_state(&mut self, state: ConnectState) {
    if self.state != state {
      tracing::trace!("[🙂Client]: connect state changed to {:?}", state);
      self.state = state.clone();
      let _ = self.sender.send(state);
    }
  }

  pub(crate) fn subscribe(&self) -> Receiver<ConnectState> {
    self.sender.subscribe()
  }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum ConnectState {
  PingTimeout,
  Connecting,
  Connected,
  Disconnected,
}

impl ConnectState {
  #[allow(dead_code)]
  pub(crate) fn is_connecting(&self) -> bool {
    matches!(self, ConnectState::Connecting)
  }

  pub(crate) fn is_connected(&self) -> bool {
    matches!(self, ConnectState::Connected)
  }

  #[allow(dead_code)]
  pub(crate) fn is_timeout(&self) -> bool {
    matches!(self, ConnectState::PingTimeout)
  }

  #[allow(dead_code)]
  pub(crate) fn is_disconnected(&self) -> bool {
    matches!(self, ConnectState::Disconnected)
  }
}
