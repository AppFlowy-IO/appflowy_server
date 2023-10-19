use crate::error::RenderError;
use crate::session::UserSession;
use askama::Template;
use axum::extract::{Path, State};
use axum::response::Result;
use axum::{response::Html, routing::get, Router};
use gotrue_entity::User;

use crate::{templates, AppState};

pub fn router() -> Router<AppState> {
  Router::new()
    .route("/", get(home_handler))
    .route("/home", get(home_handler))
    .route("/login", get(login_handler))
    .route("/admin", get(admin_handler))
    .route("/admin/users", get(admin_users_handler))
    .route("/admin/users/:user_id", get(admin_user_details_handler))
}

pub async fn login_handler() -> Result<Html<String>, RenderError> {
  render_template(templates::Login {})
}

pub async fn home_handler(
  State(state): State<AppState>,
  session: UserSession,
) -> Result<Html<String>, RenderError> {
  match state.gotrue_client.user_info(&session.access_token).await {
    Ok(user) => render_template(templates::Home {
      email: &user.email,
      is_admin: is_admin(&user),
    }),
    Err(err) => {
      tracing::error!("Error getting user info: {:?}", err);
      login_handler().await
    },
  }
}

pub async fn admin_handler(_: UserSession) -> Result<Html<String>, RenderError> {
  render_template(templates::Admin {})
}

pub async fn admin_users_handler(
  State(state): State<AppState>,
  session: UserSession,
) -> Result<Html<String>, RenderError> {
  let users = state
    .gotrue_client
    .admin_list_user(&session.access_token)
    .await
    .map_or_else(
      |err| {
        tracing::error!("Error getting user list: {:?}", err);
        vec![]
      },
      |r| r.users,
    )
    .into_iter()
    .filter(|user| user.deleted_at.is_none())
    .collect::<Vec<_>>();

  render_template(templates::Users { users: &users })
}

pub async fn admin_user_details_handler(
  State(state): State<AppState>,
  session: UserSession,
  Path(user_id): Path<String>,
) -> Result<Html<String>, RenderError> {
  let users = state
    .gotrue_client
    .admin_user_details(&session.access_token, &user_id)
    .await
    .unwrap(); // TODO: handle error

  render_template(templates::UserDetails { user: &users })
}

fn render_template<T>(x: T) -> Result<Html<String>, RenderError>
where
  T: Template,
{
  let s = x.render()?;
  Ok(Html(s))
}

fn is_admin(user: &User) -> bool {
  user.role == "supabase_admin"
}
