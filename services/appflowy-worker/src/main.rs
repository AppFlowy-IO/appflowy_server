mod application;
mod config;
pub mod error;
pub mod notion_import;
pub(crate) mod s3_client;

use crate::application::run_server;
use crate::config::Config;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let listener = TcpListener::bind("0.0.0.0:4001").await.unwrap();
  let config = Config::from_env().expect("failed to load config");
  run_server(listener, config).await
}
