use crate::Oid;
use collab_entity::CollabType;
use collab_stream::model::MessageId;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use yrs::encoding::read::Read;
use yrs::encoding::write::Write;
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::StateVector;

const TAG_MANIFEST: u8 = 1;
const TAG_UPDATE: u8 = 2;
const TAG_AWARENESS: u8 = 3;
const TAG_PERMISSION: u8 = 4;

#[repr(u8)]
#[derive(Debug, Clone)]
pub enum ClientMessage {
  /// Essentially yjs sync step 1 + Redis last message id.
  Manifest {
    /// Collab identifier.
    id: Oid,
    /// Yrs state vector for a given collab.
    state_vector: StateVector,
    /// Redis last message id received by the client.
    last_msg_id: String,
  } = TAG_MANIFEST,
  Update {
    /// Collab identifier.
    id: Oid,
    /// Collab type.
    collab_type: CollabType,
    /// Yrs [yrs::Update], encoded using lib0 v1 encoding.
    update: Vec<u8>,
  } = TAG_UPDATE,
  AwarenessUpdate {
    /// Collab identifier.
    id: Oid,
    /// Yrs [yrs::sync::awareness::AwarenessUpdate], encoded using lib0 v1 encoding.
    update: Vec<u8>,
  } = TAG_AWARENESS,
}

impl ClientMessage {
  pub fn object_id(&self) -> &Oid {
    match self {
      ClientMessage::Manifest { id, .. } => id,
      ClientMessage::Update { id, .. } => id,
      ClientMessage::AwarenessUpdate { id, .. } => id,
    }
  }

  pub fn from_bytes(&self, bytes: &[u8]) -> Result<Self, yrs::encoding::read::Error> {
    let mut dec = DecoderV1::from(bytes);
    let id = Oid::from_str(dec.read_string()?).unwrap();
    match dec.read_u8()? {
      TAG_MANIFEST => {
        let last_msg_id = dec.read_string()?.to_string();
        let state_vector = StateVector::decode(&mut dec)?;
        Ok(ClientMessage::Manifest {
          id,
          last_msg_id,
          state_vector,
        })
      },
      TAG_UPDATE => {
        let collab_type = CollabType::from(dec.read_var::<i32>()?);
        let update = dec.read_buf()?.to_vec();
        Ok(ClientMessage::Update {
          id,
          collab_type,
          update,
        })
      },
      TAG_AWARENESS => {
        let update = dec.read_buf()?.to_vec();
        Ok(ClientMessage::AwarenessUpdate { id, update })
      },
      _ => Err(yrs::encoding::read::Error::UnexpectedValue),
    }
  }

  pub fn into_bytes(&self) -> Vec<u8> {
    //TODO: use some well know, but efficient binary format?
    let mut enc = EncoderV1::new();
    match &self {
      ClientMessage::Manifest {
        id,
        last_msg_id,
        state_vector,
      } => {
        enc.write_string(&id.to_string());
        enc.write_u8(TAG_MANIFEST);
        enc.write_string(last_msg_id);
        state_vector.encode(&mut enc);
      },
      ClientMessage::Update {
        id,
        collab_type,
        update,
      } => {
        enc.write_string(&id.to_string());
        enc.write_u8(TAG_UPDATE);
        enc.write_var(collab_type.clone() as i32);
        enc.write_buf(update);
      },
      ClientMessage::AwarenessUpdate { id, update } => {
        enc.write_string(&id.to_string());
        enc.write_u8(TAG_AWARENESS);
        enc.write_buf(update);
      },
    }
    enc.to_vec()
  }
}

#[repr(u8)]
#[derive(Debug, Clone)]
pub enum ServerMessage {
  /// Essentially yjs sync step 1 + Redis last message id.
  Manifest {
    /// Collab identifier.
    id: Oid,
    /// Yrs state vector for a given collab.
    state_vector: StateVector,
    /// Redis last message id received by the client.
    last_msg_id: String,
  } = TAG_MANIFEST,
  Update {
    /// Collab identifier.
    id: Oid,
    /// Collab type.
    collab_type: CollabType,
    /// Yrs [yrs::Update], encoded using lib0 v1 encoding.
    update: Vec<u8>,
    /// Redis last message id, under which this update was stored in Redis.
    last_message_id: String,
  } = TAG_UPDATE,
  AwarenessUpdate {
    /// Collab identifier.
    id: Oid,
    /// Yrs [yrs::sync::awareness::AwarenessUpdate], encoded using lib0 v1 encoding.
    update: Vec<u8>,
  } = TAG_AWARENESS,
  PermissionDenied {
    /// Collab identifier.
    id: Oid,
    /// Reason for the permission denial.
    reason: String,
  } = TAG_PERMISSION,
}

impl ServerMessage {
  pub fn object_id(&self) -> &Oid {
    match self {
      ServerMessage::Manifest { id, .. } => id,
      ServerMessage::Update { id, .. } => id,
      ServerMessage::AwarenessUpdate { id, .. } => id,
      ServerMessage::PermissionDenied { id, .. } => id,
    }
  }

  pub fn from_bytes(&self, bytes: &[u8]) -> Result<Self, yrs::encoding::read::Error> {
    let mut dec = DecoderV1::from(bytes);
    let id = Oid::from_str(dec.read_string()?).unwrap();
    match dec.read_u8()? {
      TAG_MANIFEST => {
        let last_msg_id = dec.read_string()?.to_string();
        let state_vector = StateVector::decode(&mut dec)?;
        Ok(ServerMessage::Manifest {
          id,
          last_msg_id,
          state_vector,
        })
      },
      TAG_UPDATE => {
        let collab_type = CollabType::from(dec.read_var::<i32>()?);
        let update = dec.read_buf()?.to_vec();
        let last_message_id = dec.read_string()?.to_string();
        Ok(ServerMessage::Update {
          id,
          collab_type,
          update,
          last_message_id,
        })
      },
      TAG_AWARENESS => {
        let update = dec.read_buf()?.to_vec();
        Ok(ServerMessage::AwarenessUpdate { id, update })
      },
      TAG_PERMISSION => {
        let reason = dec.read_string()?.to_string();
        Ok(ServerMessage::PermissionDenied { id, reason })
      },
      _ => Err(yrs::encoding::read::Error::UnexpectedValue),
    }
  }

  pub fn into_bytes(&self) -> Vec<u8> {
    //TODO: use some well know, but efficient binary format?
    let mut enc = EncoderV1::new();
    match &self {
      ServerMessage::Manifest {
        id,
        last_msg_id,
        state_vector,
      } => {
        enc.write_string(&id.to_string());
        enc.write_u8(TAG_MANIFEST);
        enc.write_string(last_msg_id);
        state_vector.encode(&mut enc);
      },
      ServerMessage::Update {
        id,
        collab_type,
        update,
        last_message_id,
      } => {
        enc.write_string(&id.to_string());
        enc.write_u8(TAG_UPDATE);
        enc.write_var(collab_type.clone() as i32);
        enc.write_buf(update);
        enc.write_string(last_message_id);
      },
      ServerMessage::AwarenessUpdate { id, update } => {
        enc.write_string(&id.to_string());
        enc.write_u8(TAG_AWARENESS);
        enc.write_buf(update);
      },
      ServerMessage::PermissionDenied { id, reason } => {
        enc.write_string(&id.to_string());
        enc.write_u8(TAG_PERMISSION);
        enc.write_string(reason);
      },
    }
    enc.to_vec()
  }
}

#[cfg(test)]
mod test {
  use serde::{Deserialize, Serialize};
  use serde_repr::{Deserialize_repr, Serialize_repr};
  use yrs::StateVector;

  #[test]
  fn serde_test() {
    #[repr(u8)]
    #[derive(Serialize, Deserialize)]
    enum Message {
      A(String, String) = 1,
      B(String, Vec<u8>) = 2,
    }

    let msg = Message::A("foo".to_string(), "bar".to_string());

    println!("{}", serde_json::to_string(&msg).unwrap())
  }
}
