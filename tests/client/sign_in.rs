use client_api::Client;
use shared_entity::error_code::ErrorCode;

use crate::client::{
  constants::LOCALHOST_URL,
  utils::{generate_unique_email, REGISTERED_EMAIL, REGISTERED_PASSWORD},
};

#[tokio::test]
async fn sign_in_unknown_user() {
  let email = generate_unique_email();
  let password = "Hello123!";
  let mut c = Client::from(reqwest::Client::new(), LOCALHOST_URL);
  let err = c.sign_in_password(&email, password).await.unwrap_err();
  assert_eq!(err.code, ErrorCode::OAuthError);
  assert!(!err.message.is_empty());
}

#[tokio::test]
async fn sign_in_wrong_password() {
  let mut c = Client::from(reqwest::Client::new(), LOCALHOST_URL);

  let email = generate_unique_email();
  let password = "Hello123!";

  c.sign_up(&email, password).await.unwrap();

  let wrong_password = "Hllo123!";
  let err = c
    .sign_in_password(&email, wrong_password)
    .await
    .unwrap_err();
  assert_eq!(err.code, ErrorCode::OAuthError);
  assert!(!err.message.is_empty());
}

#[tokio::test]
async fn sign_in_unconfirmed_email() {
  let mut c = Client::from(reqwest::Client::new(), LOCALHOST_URL);

  let email = generate_unique_email();
  let password = "Hello123!";

  c.sign_up(&email, password).await.unwrap();

  let err = c.sign_in_password(&email, password).await.unwrap_err();
  assert_eq!(err.code, ErrorCode::OAuthError);
  assert!(!err.message.is_empty());
}

#[tokio::test]
async fn sign_in_success() {
  let mut c = Client::from(reqwest::Client::new(), LOCALHOST_URL);
  c.sign_in_password(&REGISTERED_EMAIL, &REGISTERED_PASSWORD)
    .await
    .unwrap();
  let token = c.token().unwrap();
  assert!(token.user.confirmed_at.is_some());

  let workspaces = c.workspaces().await.unwrap();
  assert!(!workspaces.0.is_empty());
  let profile = c.profile().await.unwrap();
  let latest_workspace = workspaces.get_latest(profile);
  assert!(latest_workspace.is_some());
}

#[tokio::test]
async fn sign_in_with_url() {
  let url_str = "appflowy-flutter://#access_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2OTQ1ODIyMjMsInN1YiI6Ijk5MGM2NDNjLTMyMWEtNGNmMi04OWY1LTNhNmJhZGFjMTg5NCIsImVtYWlsIjoiNG5uaWhpbGF0ZWRAZ21haWwuY29tIiwicGhvbmUiOiIiLCJhcHBfbWV0YWRhdGEiOnsicHJvdmlkZXIiOiJnb29nbGUiLCJwcm92aWRlcnMiOlsiZ29vZ2xlIl19LCJ1c2VyX21ldGFkYXRhIjp7ImF2YXRhcl91cmwiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS9BQ2c4b2NJdGZpa28xX0lpMmZiNzM4VnpGekViLVBqT0NCY3FUQzdrNjVIX0hnRTQwOVk9czk2LWMiLCJlbWFpbCI6IjRubmloaWxhdGVkQGdtYWlsLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJmdWxsX25hbWUiOiJmdSB6aXhpYW5nIiwiaXNzIjoiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vdXNlcmluZm8vdjIvbWUiLCJuYW1lIjoiZnUgeml4aWFuZyIsInBpY3R1cmUiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS9BQ2c4b2NJdGZpa28xX0lpMmZiNzM4VnpGekViLVBqT0NCY3FUQzdrNjVIX0hnRTQwOVk9czk2LWMiLCJwcm92aWRlcl9pZCI6IjEwMTQ5OTYxMDMxOTYxNjE0NTcyNSIsInN1YiI6IjEwMTQ5OTYxMDMxOTYxNjE0NTcyNSJ9LCJyb2xlIjoiIn0.I-7j-Tdj62P56zhzEqvBc7cHMldv5MA_MM7xtrBibbE&expires_in=3600&provider_token=ya29.a0AfB_byCovXs1CUiC9_f9VBTupQPsIxwh9aSlOg0PLYJvv1x1zvVfssrQfW6_Aq9no7EKpCzFUCLElOvK1Xz4x4K5r7tug79tr5b1yiOoUMWTeWTXyV61fZHQbZ9vscAiyKYtq5NqYTiytHcQEFlKr7UMfu6BTbKsUwaCgYKAaISARISFQGOcNnC0Vsx2QCAXgYO3XbfcF91WQ0169&refresh_token=Hi3Jc3I_pj9YrexcR91i5g&token_type=bearer";
  let mut c = Client::from(reqwest::Client::new(), LOCALHOST_URL);
  match c.sign_in_url(url_str).await {
    Ok(()) => panic!("should not be ok"),
    Err(e) => {
      assert_eq!(e.code, ErrorCode::OAuthError);
      assert!(e.message.starts_with("Invalid token: token is expired by"));
    },
  }
}
