use sqlx::{
  types::{uuid, Uuid},
  PgPool,
};

use storage_entity::{AFUserProfileView, AFWorkspace};

pub async fn create_user_if_not_exists(
  pool: &PgPool,
  gotrue_uuid: &uuid::Uuid,
  email: &str,
) -> Result<(), sqlx::Error> {
  sqlx::query!(
    r#"
        INSERT INTO af_user (uuid, email)
        SELECT $1, $2
        WHERE NOT EXISTS (
            SELECT 1 FROM public.af_user WHERE email = $2
        )
        AND NOT EXISTS (
            SELECT 1 FROM public.af_user WHERE uuid = $1
        )
        "#,
    gotrue_uuid,
    email
  )
  .execute(pool)
  .await?;
  Ok(())
}

pub async fn get_user_id(pool: &PgPool, gotrue_uuid: &uuid::Uuid) -> Result<i64, sqlx::Error> {
  let uid = sqlx::query!(
    r#"
      SELECT uid FROM af_user WHERE uuid = $1
    "#,
    gotrue_uuid
  )
  .fetch_one(pool)
  .await?
  .uid;
  Ok(uid)
}

pub async fn select_all_workspaces_owned(
  pool: &PgPool,
  owner_uuid: &Uuid,
) -> Result<Vec<AFWorkspace>, sqlx::Error> {
  sqlx::query_as!(
    AFWorkspace,
    r#"
        SELECT * FROM public.af_workspace WHERE owner_uid = (
            SELECT uid FROM public.af_user WHERE uuid = $1
            )
        "#,
    owner_uuid
  )
  .fetch_all(pool)
  .await
}

pub async fn select_user_is_workspace_owner(
  pg_pool: &PgPool,
  user_uuid: &Uuid,
  workspace_uuid: &Uuid,
) -> Result<bool, sqlx::Error> {
  let exists = sqlx::query_scalar!(
    r#"
        SELECT EXISTS(
          SELECT 1 FROM public.af_workspace
          WHERE workspace_id = $1 AND owner_uid = (
            SELECT uid FROM public.af_user WHERE uuid = $2
          )
        )
        "#,
    workspace_uuid,
    user_uuid
  )
  .fetch_one(pg_pool)
  .await?;

  Ok(exists.unwrap_or(false))
}

pub async fn insert_workspace_members(
  pool: &PgPool,
  workspace_id: &uuid::Uuid,
  members: &[i64],
) -> Result<(), sqlx::Error> {
  sqlx::query_as!(
    AFWorkspace,
    r#"
        INSERT INTO public.af_workspace_member (workspace_id, uid)
        SELECT $1, unnest($2::bigint[])
        ON CONFLICT (workspace_id, uid)
        DO NOTHING
        "#,
    workspace_id,
    members
  )
  .execute(pool)
  .await?;
  Ok(())
}

pub async fn delete_workspace_members(
  pool: &PgPool,
  workspace_id: &uuid::Uuid,
  members: &[i64],
) -> Result<(), sqlx::Error> {
  sqlx::query!(
    r#"
        DELETE FROM public.af_workspace_member
        WHERE workspace_id = $1 AND uid = ANY($2::bigint[])
        "#,
    workspace_id,
    &members
  )
  .execute(pool)
  .await?;
  Ok(())
}

pub async fn select_user_profile_view_by_uuid(
  pool: &PgPool,
  user_uuid: &Uuid,
) -> Result<Option<AFUserProfileView>, sqlx::Error> {
  sqlx::query_as!(
    AFUserProfileView,
    r#"
        SELECT *
        FROM public.af_user_profile_view WHERE uuid = $1
        "#,
    user_uuid
  )
  .fetch_optional(pool)
  .await
}
