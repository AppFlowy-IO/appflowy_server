use client_api::{Client, ClientConfiguration};
use lazy_static::lazy_static;
use std::borrow::Cow;
use std::env;
use tracing::warn;
use uuid::Uuid;

#[cfg(not(target_arch = "wasm32"))]
lazy_static! {
  pub static ref LOCALHOST_URL: Cow<'static, str> =
    get_env_var("LOCALHOST_URL", "http://localhost:8000");
  pub static ref LOCALHOST_WS: Cow<'static, str> =
    get_env_var("LOCALHOST_WS", "ws://localhost:8000/ws/v1");
  pub static ref LOCALHOST_WS_V2: Cow<'static, str> =
    get_env_var("LOCALHOST_WS", "ws://localhost:8000/ws/v2/");
  pub static ref LOCALHOST_GOTRUE: Cow<'static, str> =
    get_env_var("LOCALHOST_GOTRUE", "http://localhost:9999");
}

// Use following configuration when using local server with nginx
//
// #[cfg(not(target_arch = "wasm32"))]
// lazy_static! {
//   pub static ref LOCALHOST_URL: Cow<'static, str> =
//     get_env_var("LOCALHOST_URL", "http://localhost");
//   pub static ref LOCALHOST_WS: Cow<'static, str> =
//     get_env_var("LOCALHOST_WS", "ws://localhost/ws/v1");
//   pub static ref LOCALHOST_GOTRUE: Cow<'static, str> =
//     get_env_var("LOCALHOST_GOTRUE", "http://localhost/gotrue");
// }

// The env vars are not available in wasm32-unknown-unknown
#[cfg(target_arch = "wasm32")]
lazy_static! {
  pub static ref LOCALHOST_URL: Cow<'static, str> = Cow::Owned("http://localhost".to_string());
  pub static ref LOCALHOST_WS: Cow<'static, str> = Cow::Owned("ws://localhost/ws/v1".to_string());
  pub static ref LOCALHOST_GOTRUE: Cow<'static, str> =
    Cow::Owned("http://localhost/gotrue".to_string());
}

#[allow(dead_code)]
fn get_env_var<'default>(key: &str, default: &'default str) -> Cow<'default, str> {
  dotenvy::dotenv().ok();
  match env::var(key) {
    Ok(value) => Cow::Owned(value),
    Err(_) => {
      warn!("could not read env var {}: using default: {}", key, default);
      Cow::Borrowed(default)
    },
  }
}

/// Return a client that connects to the local host. It requires to run the server locally.
/// ```shell
/// ./script/run_local_server.sh
/// ```
pub fn localhost_client() -> Client {
  let device_id = Uuid::new_v4().to_string();
  localhost_client_with_device_id(&device_id)
}

pub fn localhost_client_with_device_id(device_id: &str) -> Client {
  Client::new(
    &LOCALHOST_URL,
    &LOCALHOST_WS,
    &LOCALHOST_WS_V2,
    &LOCALHOST_GOTRUE,
    device_id,
    ClientConfiguration::default(),
    "0.9.0",
  )
}

pub async fn workspace_id_from_client(c: &Client) -> String {
  c.get_workspaces()
    .await
    .unwrap()
    .first()
    .unwrap()
    .workspace_id
    .to_string()
}
