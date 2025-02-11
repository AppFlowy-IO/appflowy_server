use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use crate::state::AppState;
use actix::Addr;
use actix_http::header::AUTHORIZATION;
use actix_web::web::{Data, Path, Payload};
use actix_web::{get, web, HttpRequest, HttpResponse, Result, Scope};
use actix_web_actors::ws;
use app_error::AppError;
use appflowy_collaborate::actix_ws::client::rt_client::RealtimeClient;
use appflowy_collaborate::actix_ws::server::RealtimeServerActor;
use appflowy_collaborate::collab::storage::CollabAccessControlStorage;
use appflowy_ws::WsSession;
use authentication::jwt::{authorization_from_token, UserUuid};
use collab_rt_entity::user::{AFUserChange, RealtimeUser, UserMessage};
use collab_rt_entity::RealtimeMessage;
use database_entity::dto::AFRole;
use secrecy::Secret;
use semver::Version;
use shared_entity::response::AppResponseError;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, instrument, trace};
use uuid::Uuid;
use yrs::block::ClientID;

pub fn ws_scope() -> Scope {
  web::scope("/ws")
    //.service(establish_ws_connection)
    .service(web::resource("/v1").route(web::get().to(establish_ws_connection_v1)))
    .service(web::resource("/v2/{workspace_id}").route(web::get().to(establish_ws_connection_v2)))
}
const MAX_FRAME_SIZE: usize = 65_536; // 64 KiB

pub type RealtimeServerAddr = Addr<RealtimeServerActor<CollabAccessControlStorage>>;

/// This function will not be used after the 0.5.0 of the client.
#[instrument(skip_all, err)]
#[get("/{token}/{device_id}")]
pub async fn establish_ws_connection(
  request: HttpRequest,
  payload: Payload,
  path: Path<(String, String)>,
  state: Data<AppState>,
  jwt_secret: Data<Secret<String>>,
  server: Data<RealtimeServerAddr>,
) -> Result<HttpResponse> {
  let (access_token, device_id) = path.into_inner();
  let client_version = Version::new(0, 5, 0);
  let connect_at = chrono::Utc::now().timestamp();
  start_connect(
    &request,
    payload,
    &state,
    &jwt_secret,
    server,
    access_token,
    device_id,
    client_version,
    connect_at,
  )
  .await
}

#[instrument(skip_all, err)]
pub async fn establish_ws_connection_v2(
  request: HttpRequest,
  stream: Payload,
  state: Data<AppState>,
  path: Path<Uuid>,
  jwt_secret: Data<Secret<String>>,
  server: Data<Addr<appflowy_ws::WsServer>>,
  web::Query(query_params): web::Query<HashMap<String, String>>,
) -> Result<HttpResponse> {
  let workspace_id = path.into_inner();
  // Try to parse the connect info from the request body
  // If it fails, try to parse it from the query params
  let ConnectionManifest {
    access_token,
    client_version,
    device_id,
    client_id,
  } = match ConnectionManifest::parse_from(&request) {
    Ok(info) => info,
    Err(_) => {
      trace!("Failed to parse connect info from request body. Trying to parse from query params.");
      ConnectionManifest::parse_from(&query_params)?
    },
  };
  if client_version < state.config.websocket.min_client_version_v2 {
    return Err(AppError::Connect("Client version is too low".to_string()).into());
  }
  let auth = authorization_from_token(&access_token, &jwt_secret)?;
  let user_uuid = UserUuid::from_auth(auth)?;
  match state.user_cache.get_user_uid(&user_uuid).await {
    Ok(user_id) => {
      state
        .workspace_access_control
        .enforce_role(&user_id, &workspace_id.to_string(), AFRole::Member)
        .await?;

      let server = server.get_ref().clone();
      let session = WsSession::new(client_id, user_id, device_id, workspace_id, server);
      ws::WsResponseBuilder::new(session, &request, stream)
        .frame_size(MAX_FRAME_SIZE)
        .start()
    },
    Err(err) => {
      if err.is_record_not_found() {
        return Ok(HttpResponse::NotFound().json("user not found"));
      }
      Err(AppResponseError::from(err).into())
    },
  }
}

struct ConnectionManifest {
  access_token: String,
  client_version: Version,
  device_id: String,
  client_id: ClientID,
}

impl ConnectionManifest {
  fn parse_from<T: ExtractParameter>(source: &T) -> Result<Self, AppError> {
    let access_token: String = source.extract_param(AUTHORIZATION.as_str())?;
    let client_version: Version = source.extract_param(CLIENT_VERSION)?;
    let device_id: String = source.extract_param(DEVICE_ID)?;
    let client_id: u64 = source.extract_param(CLIENT_ID)?;

    Ok(Self {
      access_token,
      client_version,
      device_id,
      client_id,
    })
  }
}

#[instrument(skip_all, err)]
pub async fn establish_ws_connection_v1(
  request: HttpRequest,
  payload: Payload,
  state: Data<AppState>,
  jwt_secret: Data<Secret<String>>,
  server: Data<RealtimeServerAddr>,
  web::Query(query_params): web::Query<HashMap<String, String>>,
) -> Result<HttpResponse> {
  // Try to parse the connect info from the request body
  // If it fails, try to parse it from the query params
  let ConnectInfo {
    access_token,
    client_version,
    device_id,
    connect_at,
  } = match ConnectInfo::parse_from(&request) {
    Ok(info) => info,
    Err(_) => {
      trace!("Failed to parse connect info from request body. Trying to parse from query params.");
      ConnectInfo::parse_from(&query_params)?
    },
  };

  if client_version < state.config.websocket.min_client_version {
    return Err(AppError::Connect("Client version is too low".to_string()).into());
  }

  start_connect(
    &request,
    payload,
    &state,
    &jwt_secret,
    server,
    access_token,
    device_id,
    client_version,
    connect_at,
  )
  .await
}

#[allow(clippy::too_many_arguments)]
#[inline]
async fn start_connect(
  request: &HttpRequest,
  payload: Payload,
  state: &Data<AppState>,
  jwt_secret: &Data<Secret<String>>,
  server: Data<RealtimeServerAddr>,
  access_token: String,
  device_id: String,
  client_app_version: Version,
  connect_at: i64,
) -> Result<HttpResponse> {
  let auth = authorization_from_token(access_token.as_str(), jwt_secret)?;
  let user_uuid = UserUuid::from_auth(auth)?;
  let result = state.user_cache.get_user_uid(&user_uuid).await;

  match result {
    Ok(uid) => {
      debug!(
        "🚀new websocket connect: uid={}, device_id={}, client_version:{}",
        uid, device_id, client_app_version
      );

      let session_id = uuid::Uuid::new_v4().to_string();
      let realtime_user = RealtimeUser::new(
        uid,
        device_id,
        session_id,
        connect_at,
        client_app_version.to_string(),
      );
      let (tx, external_source) = mpsc::channel(100);
      let client = RealtimeClient::new(
        realtime_user,
        server.get_ref().clone(),
        Duration::from_secs(state.config.websocket.heartbeat_interval as u64),
        Duration::from_secs(state.config.websocket.client_timeout as u64),
        client_app_version,
        external_source,
        10,
      );

      // Receive user change notifications and send them to the client.
      listen_on_user_change(state, uid, tx);

      match ws::WsResponseBuilder::new(client, request, payload)
        .frame_size(MAX_FRAME_SIZE * 2)
        .start()
      {
        Ok(response) => Ok(response),
        Err(e) => {
          error!("🔴ws connection error: {:?}", e);
          Err(e)
        },
      }
    },
    Err(err) => {
      if err.is_record_not_found() {
        return Ok(HttpResponse::NotFound().json("user not found"));
      }
      Err(AppResponseError::from(err).into())
    },
  }
}

fn listen_on_user_change(state: &Data<AppState>, uid: i64, tx: Sender<RealtimeMessage>) {
  let mut user_change_recv = state.pg_listeners.subscribe_user_change(uid);
  actix::spawn(async move {
    while let Some(notification) = user_change_recv.recv().await {
      // Extract the user object from the notification payload.
      if let Some(user) = notification.payload {
        trace!("Receive user change: {:?}", user);
        // Since bincode serialization is used for RealtimeMessage but does not support the
        // Serde `deserialize_any` method, the user metadata is serialized into a JSON string.
        // This step ensures compatibility and flexibility for the metadata field.
        let metadata = serde_json::to_string(&user.metadata).ok();
        // Construct a UserMessage with the user's details, including the serialized metadata.
        let msg = UserMessage::ProfileChange(AFUserChange {
          uid: user.uid,
          name: user.name,
          email: user.email,
          metadata,
        });
        if tx.send(RealtimeMessage::User(msg)).await.is_err() {
          break;
        }
      }
    }
  });
}

struct ConnectInfo {
  access_token: String,
  client_version: Version,
  device_id: String,
  connect_at: i64,
}

const CLIENT_VERSION: &str = "client-version";
const DEVICE_ID: &str = "device-id";
const CLIENT_ID: &str = "client-id";
const CONNECT_AT: &str = "connect-at";

// Trait for parameter extraction
trait ExtractParameter {
  fn extract_param<S: FromStr>(&self, key: &str) -> Result<S, AppError>;
}

// Implement the trait for HashMap<String, String>
impl ExtractParameter for HashMap<String, String> {
  fn extract_param<S: FromStr>(&self, key: &str) -> Result<S, AppError> {
    let value = self.get(key).ok_or_else(|| {
      AppError::InvalidRequest(format!("Parameter with given key '{}' not found", key))
    })?;
    S::from_str(&value)
      .map_err(|_| AppError::InvalidRequest(format!("Failed to parse value for key '{}'", key)))
  }
}

// Implement the trait for HttpRequest
impl ExtractParameter for HttpRequest {
  fn extract_param<S: FromStr>(&self, key: &str) -> Result<S, AppError> {
    let value = self.headers().get(key).ok_or_else(|| {
      AppError::InvalidRequest(format!("Header with given key:{} not found", key))
    })?;
    let value = value.to_str().map_err(|_| {
      AppError::InvalidRequest(format!("Invalid header value for given key:{}", key))
    })?;
    S::from_str(value)
      .map_err(|_| AppError::InvalidRequest(format!("Failed to parse value for header '{}'", key)))
  }
}

impl ConnectInfo {
  fn parse_from<T: ExtractParameter>(source: &T) -> Result<Self, AppError> {
    let access_token: String = source.extract_param(AUTHORIZATION.as_str())?;
    let client_version: Version = source.extract_param(CLIENT_VERSION)?;
    let device_id: String = source.extract_param(DEVICE_ID)?;
    let connect_at = source
      .extract_param::<i64>(CONNECT_AT)
      .unwrap_or_else(|_| chrono::Utc::now().timestamp());

    Ok(Self {
      access_token,
      client_version,
      device_id,
      connect_at,
    })
  }
}
