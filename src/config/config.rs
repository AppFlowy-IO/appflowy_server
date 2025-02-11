use std::fmt::Display;
use std::str::FromStr;

use anyhow::Context;
use secrecy::{ExposeSecret, Secret};
use semver::Version;
use serde::Deserialize;
use sqlx::postgres::{PgConnectOptions, PgSslMode};

use infra::env_util::{get_env_var, get_env_var_opt};
use mailer::config::MailerSetting;

#[derive(Clone, Debug)]
pub struct Config {
  pub app_env: Environment,
  pub access_control: AccessControlSetting,
  pub db_settings: DatabaseSetting,
  pub gotrue: GoTrueSetting,
  pub application: ApplicationSetting,
  pub websocket: WebsocketSetting,
  pub redis_uri: Secret<String>,
  pub redis_worker_count: usize,
  pub s3: S3Setting,
  pub appflowy_ai: AppFlowyAISetting,
  pub collab: CollabSetting,
  pub published_collab: PublishedCollabSetting,
  pub mailer: MailerSetting,
  pub apple_oauth: AppleOAuthSetting,
  pub appflowy_web_url: Option<String>,
  pub admin_frontend_path_prefix: String,
}

#[derive(serde::Deserialize, Clone, Debug)]

pub struct AccessControlSetting {
  pub is_enabled: bool,
  pub enable_workspace_access_control: bool,
  pub enable_collab_access_control: bool,
  pub enable_realtime_access_control: bool,
}

#[derive(serde::Deserialize, Clone, Debug)]
pub struct AppleOAuthSetting {
  pub client_id: String,
  pub client_secret: Secret<String>,
}

#[derive(serde::Deserialize, Clone, Debug)]
pub struct CasbinSetting {
  pub pool_size: u32,
}

#[derive(serde::Deserialize, Clone, Debug)]
pub struct S3Setting {
  pub create_bucket: bool,
  pub use_minio: bool,
  pub minio_url: String,
  pub access_key: String,
  pub secret_key: Secret<String>,
  pub bucket: String,
  pub region: String,
  pub presigned_url_endpoint: Option<String>,
}

#[derive(serde::Deserialize, Clone, Debug)]
pub struct GoTrueSetting {
  pub base_url: String,
  pub ext_url: String, // public url
  pub jwt_secret: Secret<String>,
  pub admin_email: String,
  pub admin_password: Secret<String>,
}

#[derive(serde::Deserialize, Clone, Debug)]
pub struct AppFlowyAISetting {
  pub port: Secret<String>,
  pub host: Secret<String>,
}

impl AppFlowyAISetting {
  pub fn url(&self) -> String {
    format!(
      "http://{}:{}",
      self.host.expose_secret(),
      self.port.expose_secret()
    )
  }
}

// We are using 127.0.0.1 as our host in address, we are instructing our
// application to only accept connections coming from the same machine. However,
// request from the hose machine which is not seen as local by our Docker image.
//
// Using 0.0.0.0 as host to instruct our application to accept connections from
// any network interface. So using 127.0.0.1 for our local development and set
// it to 0.0.0.0 in our Docker images.
//
#[derive(Clone, Debug)]
pub struct ApplicationSetting {
  pub port: u16,
  pub host: String,
}

#[derive(Clone, Debug)]
pub struct DatabaseSetting {
  pub pg_conn_opts: PgConnectOptions,
  pub require_ssl: bool,
  /// PostgreSQL has a maximum of 115 connections to the database, 15 connections are reserved to
  /// the super user to maintain the integrity of the PostgreSQL database, and 100 PostgreSQL
  /// connections are reserved for system applications.
  /// When we exceed the limit of the database connection, then it shows an error message.
  pub max_connections: u32,
}

impl Display for DatabaseSetting {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let masked_pg_conn_opts = self.pg_conn_opts.clone().password("********");
    write!(
      f,
      "DatabaseSetting {{ pg_conn_opts: {:?}, require_ssl: {}, max_connections: {} }}",
      masked_pg_conn_opts, self.require_ssl, self.max_connections
    )
  }
}

impl DatabaseSetting {
  pub fn pg_connect_options(&self) -> PgConnectOptions {
    let ssl_mode = if self.require_ssl {
      PgSslMode::Require
    } else {
      PgSslMode::Prefer
    };
    let options = self.pg_conn_opts.clone();
    options.ssl_mode(ssl_mode)
  }
}

#[derive(Clone, Debug)]
pub struct CollabSetting {
  pub group_persistence_interval_secs: u64,
  pub group_prune_grace_period_secs: u64,
  pub edit_state_max_count: u32,
  pub edit_state_max_secs: i64,
  pub s3_collab_threshold: u64,
}

#[derive(Clone, Debug)]
pub enum PublishedCollabStorageBackend {
  Postgres,
  S3WithPostgresBackup,
}

#[derive(Clone, Debug)]
pub struct PublishedCollabSetting {
  pub storage_backend: PublishedCollabStorageBackend,
}

impl TryFrom<&str> for PublishedCollabStorageBackend {
  type Error = anyhow::Error;

  fn try_from(value: &str) -> Result<Self, Self::Error> {
    match value {
      "postgres" => Ok(PublishedCollabStorageBackend::Postgres),
      "s3_with_postgres_backup" => Ok(PublishedCollabStorageBackend::S3WithPostgresBackup),
      _ => Err(anyhow::anyhow!("Invalid PublishedCollabStorageBackend")),
    }
  }
}

// Default values favor local development.
pub fn get_configuration() -> Result<Config, anyhow::Error> {
  let config = Config {
    app_env: get_env_var("APPFLOWY_ENVIRONMENT", "local")
      .parse()
      .context("fail to get APPFLOWY_ENVIRONMENT")?,
    access_control: AccessControlSetting {
      is_enabled: get_env_var("APPFLOWY_ACCESS_CONTROL", "false")
        .parse()
        .context("fail to get APPFLOWY_ACCESS_CONTROL")?,
      enable_workspace_access_control: get_env_var("APPFLOWY_ACCESS_CONTROL_WORKSPACE", "true")
        .parse()
        .context("fail to get APPFLOWY_ACCESS_CONTROL_WORKSPACE")?,
      enable_collab_access_control: get_env_var("APPFLOWY_ACCESS_CONTROL_COLLAB", "true")
        .parse()
        .context("fail to get APPFLOWY_ACCESS_CONTROL_COLLAB")?,
      enable_realtime_access_control: get_env_var("APPFLOWY_ACCESS_CONTROL_REALTIME", "true")
        .parse()
        .context("fail to get APPFLOWY_ACCESS_CONTROL_REALTIME")?,
    },
    db_settings: DatabaseSetting {
      pg_conn_opts: PgConnectOptions::from_str(&get_env_var(
        "APPFLOWY_DATABASE_URL",
        "postgres://postgres:password@localhost:5432/postgres",
      ))?,
      require_ssl: get_env_var("APPFLOWY_DATABASE_REQUIRE_SSL", "false")
        .parse()
        .context("fail to get APPFLOWY_DATABASE_REQUIRE_SSL")?,
      max_connections: get_env_var("APPFLOWY_DATABASE_MAX_CONNECTIONS", "40")
        .parse()
        .context("fail to get APPFLOWY_DATABASE_MAX_CONNECTIONS")?,
    },
    gotrue: GoTrueSetting {
      base_url: get_env_var("APPFLOWY_GOTRUE_BASE_URL", "http://localhost:9999"),
      ext_url: get_env_var("APPFLOWY_GOTRUE_EXT_URL", "http://localhost:9999"),
      jwt_secret: get_env_var("APPFLOWY_GOTRUE_JWT_SECRET", "hello456").into(),
      admin_email: get_env_var("APPFLOWY_GOTRUE_ADMIN_EMAIL", "admin@example.com"),
      admin_password: get_env_var("APPFLOWY_GOTRUE_ADMIN_PASSWORD", "password").into(),
    },
    application: ApplicationSetting {
      port: get_env_var("APPFLOWY_APPLICATION_PORT", "8000").parse()?,
      host: get_env_var("APPFLOWY_APPLICATION_HOST", "0.0.0.0"),
    },
    websocket: WebsocketSetting {
      heartbeat_interval: get_env_var("APPFLOWY_WEBSOCKET_HEARTBEAT_INTERVAL", "6").parse()?,
      client_timeout: get_env_var("APPFLOWY_WEBSOCKET_CLIENT_TIMEOUT", "60").parse()?,
      min_client_version: get_env_var("APPFLOWY_WEBSOCKET_CLIENT_MIN_VERSION", "0.5.0").parse()?,
      min_client_version_v2: get_env_var("APPFLOWY_WEBSOCKET_CLIENT_MIN_VERSION_V2", "0.10.0")
        .parse()?,
    },
    redis_uri: get_env_var("APPFLOWY_REDIS_URI", "redis://localhost:6379").into(),
    redis_worker_count: get_env_var("APPFLOWY_REDIS_WORKERS", "60").parse()?,
    s3: S3Setting {
      create_bucket: get_env_var("APPFLOWY_S3_CREATE_BUCKET", "true")
        .parse()
        .context("fail to get APPFLOWY_S3_CREATE_BUCKET")?,
      use_minio: get_env_var("APPFLOWY_S3_USE_MINIO", "true")
        .parse()
        .context("fail to get APPFLOWY_S3_USE_MINIO")?,
      minio_url: get_env_var("APPFLOWY_S3_MINIO_URL", "http://localhost:9000"),
      access_key: get_env_var("APPFLOWY_S3_ACCESS_KEY", "minioadmin"),
      secret_key: get_env_var("APPFLOWY_S3_SECRET_KEY", "minioadmin").into(),
      bucket: get_env_var("APPFLOWY_S3_BUCKET", "appflowy"),
      region: get_env_var("APPFLOWY_S3_REGION", ""),
      presigned_url_endpoint: get_env_var_opt("APPFLOWY_S3_PRESIGNED_URL_ENDPOINT"),
    },
    appflowy_ai: AppFlowyAISetting {
      port: get_env_var("AI_SERVER_PORT", "5001").into(),
      host: get_env_var("AI_SERVER_HOST", "localhost").into(),
    },
    collab: CollabSetting {
      group_persistence_interval_secs: get_env_var(
        "APPFLOWY_COLLAB_GROUP_PERSISTENCE_INTERVAL",
        "60",
      )
      .parse()?,
      group_prune_grace_period_secs: get_env_var("APPFLOWY_COLLAB_GROUP_GRACE_PERIOD_SECS", "60")
        .parse()?,
      edit_state_max_count: get_env_var("APPFLOWY_COLLAB_EDIT_STATE_MAX_COUNT", "100").parse()?,
      edit_state_max_secs: get_env_var("APPFLOWY_COLLAB_EDIT_STATE_MAX_SECS", "60").parse()?,
      s3_collab_threshold: get_env_var("APPFLOWY_COLLAB_S3_THRESHOLD", "8000").parse()?,
    },
    published_collab: PublishedCollabSetting {
      storage_backend: get_env_var("APPFLOWY_PUBLISHED_COLLAB_STORAGE_BACKEND", "postgres")
        .as_str()
        .try_into()?,
    },
    mailer: MailerSetting {
      smtp_host: get_env_var("APPFLOWY_MAILER_SMTP_HOST", "smtp.gmail.com"),
      smtp_port: get_env_var("APPFLOWY_MAILER_SMTP_PORT", "465").parse()?,
      smtp_username: get_env_var("APPFLOWY_MAILER_SMTP_USERNAME", "sender@example.com"),
      smtp_email: get_env_var("APPFLOWY_MAILER_SMTP_EMAIL", "sender@example.com"),
      smtp_password: get_env_var("APPFLOWY_MAILER_SMTP_PASSWORD", "password").into(),
      smtp_tls_kind: get_env_var("APPFLOWY_MAILER_SMTP_TLS_KIND", "wrapper"),
    },
    apple_oauth: AppleOAuthSetting {
      client_id: get_env_var("APPFLOWY_APPLE_OAUTH_CLIENT_ID", ""),
      client_secret: get_env_var("APPFLOWY_APPLE_OAUTH_CLIENT_SECRET", "").into(),
    },
    appflowy_web_url: get_env_var_opt("APPFLOWY_WEB_URL"),
    admin_frontend_path_prefix: get_env_var("APPFLOWY_ADMIN_FRONTEND_PATH_PREFIX", ""),
  };
  Ok(config)
}

/// The possible runtime environment for our application.
#[derive(Clone, Debug, Deserialize)]
pub enum Environment {
  Local,
  Production,
}

impl Environment {
  pub fn as_str(&self) -> &'static str {
    match self {
      Environment::Local => "local",
      Environment::Production => "production",
    }
  }
}

impl FromStr for Environment {
  type Err = anyhow::Error;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s.to_lowercase().as_str() {
      "local" => Ok(Self::Local),
      "production" => Ok(Self::Production),
      other => anyhow::bail!(
        "{} is not a supported environment. Use either `local` or `production`.",
        other
      ),
    }
  }
}

#[derive(Clone, Debug)]
pub struct WebsocketSetting {
  pub heartbeat_interval: u8,
  pub client_timeout: u8,
  pub min_client_version: Version,
  pub min_client_version_v2: Version,
}
