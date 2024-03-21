use crate::error::RealtimeError;
use actix::{Message, Recipient};

use serde_repr::{Deserialize_repr, Serialize_repr};
use std::fmt::Debug;

pub use realtime_entity::message::RealtimeMessage;

#[derive(Debug, Message, Clone)]
#[rtype(result = "Result<(), RealtimeError>")]
pub struct Connect<U> {
  pub socket: Recipient<RealtimeMessage>,
  pub user: U,
  pub session_id: String,
}

#[derive(Debug, Message, Clone)]
#[rtype(result = "Result<(), RealtimeError>")]
pub struct Disconnect<U> {
  pub user: U,
  pub session_id: String,
}

#[derive(Debug, Message, Clone)]
#[rtype(result = "Result<(), RealtimeError>")]
pub struct DisconnectByServer;
#[derive(Debug, Clone, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum BusinessID {
  CollabId = 1,
}

#[derive(Debug, Message, Clone)]
#[rtype(result = "Result<(), RealtimeError>")]
pub struct ClientMessage<U> {
  pub user: U,
  pub message: RealtimeMessage,
}

#[derive(Message)]
#[rtype(result = "Result<(), RealtimeError>")]
pub struct ClientStreamMessage {
  pub uid: i64,
  pub device_id: String,
  pub message: RealtimeMessage,
}
