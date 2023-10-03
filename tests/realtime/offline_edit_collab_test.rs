use crate::client::utils::generate_unique_registered_user;
use crate::realtime::test_client::{assert_client_collab, assert_remote_collab, TestClient};
use std::time::Duration;

use collab_define::CollabType;
use serde_json::json;
use shared_entity::dto::QueryCollabParams;
use shared_entity::error_code::ErrorCode;
use sqlx::types::uuid;

#[tokio::test]
async fn ws_reconnect_sync_test() {
  let object_id = uuid::Uuid::new_v4().to_string();
  let collab_type = CollabType::Document;

  let mut test_client = TestClient::new_user().await;
  let workspace_id = test_client.current_workspace_id().await;
  test_client
    .create_collab(&workspace_id, &object_id, collab_type.clone())
    .await;

  // Disconnect the client and edit the collab. The updates will not be sent to the server.
  test_client.disconnect().await;
  for i in 0..=5 {
    test_client
      .collab_by_object_id
      .get_mut(&object_id)
      .unwrap()
      .collab
      .lock()
      .insert(&i.to_string(), i.to_string());
  }

  // it will return RecordNotFound error when trying to get the collab from the server
  let err = test_client
    .api_client
    .get_collab(QueryCollabParams {
      object_id: object_id.clone(),
      collab_type: collab_type.clone(),
    })
    .await
    .unwrap_err();
  assert_eq!(err.code, ErrorCode::RecordNotFound);

  // After reconnect the collab should be synced to the server.
  test_client.reconnect().await;
  // Wait for the messages to be sent
  test_client.wait_object_sync_complete(&object_id).await;

  assert_remote_collab(
    &test_client.api_client,
    &object_id,
    &collab_type,
    10,
    json!( {
      "0": "0",
      "1": "1",
      "2": "2",
      "3": "3",
      "4": "4",
      "5": "5",
    }),
  )
  .await;
}

#[tokio::test]
async fn edit_document_with_one_client_online_and_other_offline_test() {
  let object_id = uuid::Uuid::new_v4().to_string();
  let collab_type = CollabType::Document;
  let registered_user = generate_unique_registered_user().await;

  let mut client_1 = TestClient::user_with_new_device(registered_user.clone()).await;
  let workspace_id = client_1.current_workspace_id().await;
  client_1
    .create_collab(&workspace_id, &object_id, collab_type.clone())
    .await;
  client_1
    .collab_by_object_id
    .get_mut(&object_id)
    .unwrap()
    .collab
    .lock()
    .insert("name", "work");
  client_1.wait_object_sync_complete(&object_id).await;

  let mut client_2 = TestClient::user_with_new_device(registered_user.clone()).await;
  client_2
    .create_collab(&workspace_id, &object_id, collab_type.clone())
    .await;
  tokio::time::sleep(Duration::from_millis(1000)).await;

  client_2.disconnect().await;
  client_2
    .collab_by_object_id
    .get_mut(&object_id)
    .unwrap()
    .collab
    .lock()
    .insert("name", "workspace");

  client_2.reconnect().await;
  client_2.wait_object_sync_complete(&object_id).await;

  let expected_json = json!({
    "name": "workspace"
  });
  assert_client_collab(&mut client_1, &object_id, expected_json.clone()).await;
  assert_client_collab(&mut client_2, &object_id, expected_json.clone()).await;
}

#[tokio::test]
async fn edit_document_with_both_clients_offline_then_online_sync_test() {
  let object_id = uuid::Uuid::new_v4().to_string();
  let collab_type = CollabType::Document;

  let mut client_1 = TestClient::new_user().await;
  let workspace_id = client_1.current_workspace_id().await;
  client_1
    .create_collab(&workspace_id, &object_id, collab_type.clone())
    .await;
  client_1.disconnect().await;

  let mut client_2 = TestClient::new_user().await;
  client_2
    .create_collab(&workspace_id, &object_id, collab_type.clone())
    .await;
  client_2.disconnect().await;

  for i in 0..10 {
    if i % 2 == 0 {
      client_1
        .collab_by_object_id
        .get_mut(&object_id)
        .unwrap()
        .collab
        .lock()
        .insert(&i.to_string(), format!("Task {}", i));
    } else {
      client_2
        .collab_by_object_id
        .get_mut(&object_id)
        .unwrap()
        .collab
        .lock()
        .insert(&i.to_string(), format!("Task {}", i));
    }
  }

  tokio::join!(client_1.reconnect(), client_2.reconnect());
  tokio::join!(
    client_1.wait_object_sync_complete(&object_id),
    client_2.wait_object_sync_complete(&object_id)
  );

  let expected_json = json!({
    "0": "Task 0",
    "1": "Task 1",
    "2": "Task 2",
    "3": "Task 3",
    "4": "Task 4",
    "5": "Task 5",
    "6": "Task 6",
    "7": "Task 7",
    "8": "Task 8",
    "9": "Task 9"
  });
  assert_client_collab(&mut client_1, &object_id, expected_json.clone()).await;
  assert_client_collab(&mut client_2, &object_id, expected_json.clone()).await;
}
