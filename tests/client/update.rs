use shared_entity::error_code::ErrorCode;

use crate::client::utils::{generate_unique_email, REGISTERED_USERS, REGISTERED_USERS_MUTEX};
use crate::client_api_client;

#[tokio::test]
async fn update_but_not_logged_in() {
  let mut c = client_api_client();
  let new_email = generate_unique_email();
  let new_password = "Hello123!";
  let res = c.update(&new_email, new_password).await;
  assert!(res.is_err());
}

#[tokio::test]
async fn update_password_same_password() {
  let _guard = REGISTERED_USERS_MUTEX.lock().await;

  let user = &REGISTERED_USERS[0];
  let mut c = client_api_client();
  c.sign_in_password(&user.email, &user.password)
    .await
    .unwrap();
  let err = c.update(&user.email, &user.password).await.err().unwrap();
  assert_eq!(err.code, ErrorCode::InvalidPassword);
  assert_eq!(
    err.message,
    "New password should be different from the old password."
  );
}

#[tokio::test]
async fn update_password_and_revert() {
  let _guard = REGISTERED_USERS_MUTEX.lock().await;

  let new_password = "Hello456!";
  let user = &REGISTERED_USERS[0];
  {
    // change password to new_password
    let mut c = client_api_client();
    c.sign_in_password(&user.email, &user.password)
      .await
      .unwrap();
    c.update(&user.email, new_password).await.unwrap();
  }
  {
    // revert password to old_password
    let mut c = client_api_client();
    c.sign_in_password(&user.email, new_password).await.unwrap();
    c.update(&user.email, &user.password).await.unwrap();
  }
}
