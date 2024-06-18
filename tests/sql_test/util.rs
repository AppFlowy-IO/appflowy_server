use appflowy_cloud::middleware::feature_migration::FeatureMigrationSource;
use lazy_static::lazy_static;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use snowflake::Snowflake;
use sqlx::PgPool;
use sqlx_core::migrate::Migrator;
use tokio::sync::RwLock;
use uuid::Uuid;

pub async fn setup_db(pool: &PgPool) -> anyhow::Result<()> {
  // Have to manually create schema and tables managed by gotrue but referenced by our
  // migration scripts.
  sqlx::query(r#"create schema auth"#).execute(pool).await?;
  sqlx::query(
    r#"
      CREATE TABLE auth.users(
        id uuid NOT NULL UNIQUE,
        deleted_at timestamptz null,
        CONSTRAINT users_pkey PRIMARY KEY (id)
      )
    "#,
  )
  .execute(pool)
  .await?;

  Migrator::new(FeatureMigrationSource::new("./migrations"))
    .await
    .unwrap()
    .set_ignore_missing(true)
    .run(pool)
    .await
    .unwrap();
  Ok(())
}

pub async fn insert_auth_user(pool: &PgPool, user_uuid: Uuid) -> anyhow::Result<()> {
  sqlx::query(
    r#"
      INSERT INTO auth.users (id)
      VALUES ($1)
    "#,
  )
  .bind(user_uuid)
  .execute(pool)
  .await?;
  Ok(())
}

lazy_static! {
  pub static ref ID_GEN: RwLock<Snowflake> = RwLock::new(Snowflake::new(1));
}

pub async fn test_create_user(
  pool: &PgPool,
  user_uuid: Uuid,
  email: &str,
  name: &str,
) -> anyhow::Result<TestUser> {
  insert_auth_user(pool, user_uuid).await.unwrap();
  let uid = ID_GEN.write().await.next_id();
  let workspace_id = database::user::create_user(pool, uid, &user_uuid, email, name)
    .await
    .unwrap();

  Ok(TestUser {
    uid,
    workspace_id: workspace_id.to_string(),
  })
}

#[derive(Clone)]
pub struct TestUser {
  pub uid: i64,
  pub workspace_id: String,
}

pub fn generate_random_bytes(size: usize) -> Vec<u8> {
  let s: String = thread_rng()
    .sample_iter(&Alphanumeric)
    .take(size)
    .map(char::from)
    .collect();
  s.into_bytes()
}
