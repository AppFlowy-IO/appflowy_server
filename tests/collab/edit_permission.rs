use crate::collab::util::generate_random_string;
use assert_json_diff::{assert_json_eq, assert_json_include};
use client_api_test_util::{
  assert_client_collab_include_value, assert_client_collab_within_secs, assert_server_collab,
  TestClient,
};
use collab_entity::CollabType;
use database_entity::dto::{AFAccessLevel, AFRole};

use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

#[tokio::test]
async fn recv_updates_without_permission_test() {
  let collab_type = CollabType::Empty;
  let mut client_1 = TestClient::new_user().await;
  let mut client_2 = TestClient::new_user().await;

  let workspace_id = client_1.workspace_id().await;
  let object_id = client_1
    .create_and_edit_collab(&workspace_id, collab_type.clone())
    .await;

  client_2
    .open_collab(&workspace_id, &object_id, collab_type.clone())
    .await;

  // Edit the collab from client 1 and then the server will broadcast to client 2. But the client 2
  // is not the member of the collab, so the client 2 will not receive the update.
  client_1
    .collabs
    .get_mut(&object_id)
    .unwrap()
    .collab
    .lock()
    .insert("name", "AppFlowy");
  client_1
    .wait_object_sync_complete(&object_id)
    .await
    .unwrap();
  assert_client_collab_within_secs(&mut client_2, &object_id, "name", json!({}), 60).await;
}

// #[tokio::test]
// async fn recv_remote_updates_with_readonly_permission_test() {
//   let collab_type = CollabType::Empty;
//   let mut client_1 = TestClient::new_user().await;
//   let mut client_2 = TestClient::new_user().await;
//
//   let workspace_id = client_1.workspace_id().await;
//   let object_id = client_1
//     .create_and_edit_collab(&workspace_id, collab_type.clone())
//     .await;
//
//   // Add client 2 as the member of the collab then the client 2 will receive the update.
//   client_1
//     .add_collab_member(
//       &workspace_id,
//       &object_id,
//       &client_2,
//       AFAccessLevel::ReadOnly,
//     )
//     .await;
//
//   client_2
//     .open_collab(&workspace_id, &object_id, collab_type.clone())
//     .await;
//
//   // Edit the collab from client 1 and then the server will broadcast to client 2
//   client_1
//     .collabs
//     .get_mut(&object_id)
//     .unwrap()
//     .collab
//     .lock()
//     .insert("name", "AppFlowy");
//   client_1
//     .wait_object_sync_complete(&object_id)
//     .await
//     .unwrap();
//
//   let expected = json!({
//     "name": "AppFlowy"
//   });
//   assert_client_collab_within_secs(&mut client_2, &object_id, "name", expected.clone(), 60).await;
//   assert_server_collab(
//     &workspace_id,
//     &mut client_1.api_client,
//     &object_id,
//     &collab_type,
//     10,
//     expected,
//   )
//   .await
//   .unwrap();
// }

// #[tokio::test]
// async fn init_sync_with_readonly_permission_test() {
//   let collab_type = CollabType::Empty;
//   let mut client_1 = TestClient::new_user().await;
//   let mut client_2 = TestClient::new_user().await;
//
//   let workspace_id = client_1.workspace_id().await;
//   let object_id = client_1
//     .create_and_edit_collab(&workspace_id, collab_type.clone())
//     .await;
//   client_1
//     .collabs
//     .get_mut(&object_id)
//     .unwrap()
//     .collab
//     .lock()
//     .insert("name", "AppFlowy");
//   client_1
//     .wait_object_sync_complete(&object_id)
//     .await
//     .unwrap();
//   sleep(Duration::from_secs(2)).await;
//
//   //
//   let expected = json!({
//     "name": "AppFlowy"
//   });
//   assert_server_collab(
//     &workspace_id,
//     &mut client_1.api_client,
//     &object_id,
//     &collab_type,
//     10,
//     expected.clone(),
//   )
//   .await
//   .unwrap();
//
//   // Add client 2 as the member of the collab with readonly permission.
//   // client 2 can pull the latest updates via the init sync. But it's not allowed to send local changes.
//   client_1
//     .add_collab_member(
//       &workspace_id,
//       &object_id,
//       &client_2,
//       AFAccessLevel::ReadOnly,
//     )
//     .await;
//   client_2
//     .open_collab(&workspace_id, &object_id, collab_type.clone())
//     .await;
//   assert_client_collab_include_value(&mut client_2, &object_id, expected)
//     .await
//     .unwrap();
// }

#[tokio::test]
async fn edit_collab_with_readonly_permission_test() {
  let collab_type = CollabType::Empty;
  let mut client_1 = TestClient::new_user().await;
  let mut client_2 = TestClient::new_user().await;

  let workspace_id = client_1.workspace_id().await;
  let object_id = client_1
    .create_and_edit_collab(&workspace_id, collab_type.clone())
    .await;

  // Add client 2 as the member of the collab then the client 2 will receive the update.
  client_1
    .add_collab_member(
      &workspace_id,
      &object_id,
      &client_2,
      AFAccessLevel::ReadOnly,
    )
    .await;

  client_2
    .open_collab(&workspace_id, &object_id, collab_type.clone())
    .await;

  // client 2 edit the collab and then the server will reject the update which mean the
  // collab in the server will not be updated.
  client_2
    .collabs
    .get_mut(&object_id)
    .unwrap()
    .collab
    .lock()
    .insert("name", "AppFlowy");
  assert_client_collab_include_value(
    &mut client_2,
    &object_id,
    json!({
      "name": "AppFlowy"
    }),
  )
  .await
  .unwrap();

  assert_server_collab(
    &workspace_id,
    &mut client_1.api_client,
    &object_id,
    &collab_type,
    5,
    json!({}),
  )
  .await
  .unwrap();
}

#[tokio::test]
async fn edit_collab_with_read_and_write_permission_test() {
  let collab_type = CollabType::Empty;
  let mut client_1 = TestClient::new_user().await;
  let mut client_2 = TestClient::new_user().await;

  let workspace_id = client_1.workspace_id().await;
  let object_id = client_1
    .create_and_edit_collab(&workspace_id, collab_type.clone())
    .await;

  // Add client 2 as the member of the collab then the client 2 will receive the update.
  client_1
    .add_collab_member(
      &workspace_id,
      &object_id,
      &client_2,
      AFAccessLevel::ReadAndWrite,
    )
    .await;

  client_2
    .open_collab(&workspace_id, &object_id, collab_type.clone())
    .await;

  // client 2 edit the collab and then the server will broadcast the update
  client_2
    .collabs
    .get_mut(&object_id)
    .unwrap()
    .collab
    .lock()
    .insert("name", "AppFlowy");
  client_2
    .wait_object_sync_complete(&object_id)
    .await
    .unwrap();

  let expected = json!({
    "name": "AppFlowy"
  });
  assert_client_collab_include_value(&mut client_2, &object_id, expected.clone())
    .await
    .unwrap();

  assert_server_collab(
    &workspace_id,
    &mut client_1.api_client,
    &object_id,
    &collab_type,
    5,
    expected,
  )
  .await
  .unwrap();
}

#[tokio::test]
async fn edit_collab_with_full_access_permission_test() {
  let collab_type = CollabType::Empty;
  let mut client_1 = TestClient::new_user().await;
  let mut client_2 = TestClient::new_user().await;

  let workspace_id = client_1.workspace_id().await;
  let object_id = client_1
    .create_and_edit_collab(&workspace_id, collab_type.clone())
    .await;

  // Add client 2 as the member of the collab then the client 2 will receive the update.
  client_1
    .add_collab_member(
      &workspace_id,
      &object_id,
      &client_2,
      AFAccessLevel::FullAccess,
    )
    .await;

  client_2
    .open_collab(&workspace_id, &object_id, collab_type.clone())
    .await;

  // client 2 edit the collab and then the server will broadcast the update
  client_2
    .collabs
    .get_mut(&object_id)
    .unwrap()
    .collab
    .lock()
    .insert("name", "AppFlowy");

  let expected = json!({
    "name": "AppFlowy"
  });
  client_2
    .wait_object_sync_complete(&object_id)
    .await
    .unwrap();
  assert_client_collab_within_secs(&mut client_2, &object_id, "name", expected.clone(), 30).await;

  assert_server_collab(
    &workspace_id,
    &mut client_1.api_client,
    &object_id,
    &collab_type,
    5,
    expected,
  )
  .await
  .unwrap();
}

#[tokio::test]
async fn edit_collab_with_full_access_then_readonly_permission() {
  let collab_type = CollabType::Empty;
  let mut client_1 = TestClient::new_user().await;
  let mut client_2 = TestClient::new_user().await;

  let workspace_id = client_1.workspace_id().await;
  let object_id = client_1
    .create_and_edit_collab(&workspace_id, collab_type.clone())
    .await;

  // Add client 2 as the member of the collab then the client 2 will receive the update.
  client_1
    .add_collab_member(
      &workspace_id,
      &object_id,
      &client_2,
      AFAccessLevel::FullAccess,
    )
    .await;

  // client 2 edit the collab and then the server will broadcast the update
  {
    client_2
      .open_collab(&workspace_id, &object_id, collab_type.clone())
      .await;
    client_2
      .collabs
      .get_mut(&object_id)
      .unwrap()
      .collab
      .lock()
      .insert("title", "hello world");
    client_2
      .wait_object_sync_complete(&object_id)
      .await
      .unwrap();
  }

  // update the permission from full access to readonly, then the server will reject the subsequent
  // updates generated by client 2
  {
    client_1
      .update_collab_member_access_level(
        &workspace_id,
        &object_id,
        &client_2,
        AFAccessLevel::ReadOnly,
      )
      .await;
    client_2
      .collabs
      .get_mut(&object_id)
      .unwrap()
      .collab
      .lock()
      .insert("subtitle", "Writing Rust, fun");
  }

  assert_client_collab_include_value(
    &mut client_2,
    &object_id,
    json!({
      "title": "hello world",
      "subtitle": "Writing Rust, fun"
    }),
  )
  .await
  .unwrap();
  assert_server_collab(
    &workspace_id,
    &mut client_1.api_client,
    &object_id,
    &collab_type,
    5,
    json!({
      "title": "hello world"
    }),
  )
  .await
  .unwrap();
}

#[tokio::test]
async fn multiple_user_with_read_and_write_permission_edit_same_collab_test() {
  let mut tasks = Vec::new();
  let mut owner = TestClient::new_user().await;
  let object_id = Uuid::new_v4().to_string();
  let collab_type = CollabType::Empty;
  let workspace_id = owner.workspace_id().await;
  owner
    .create_and_edit_collab_with_data(object_id.clone(), &workspace_id, collab_type.clone(), None)
    .await;

  let arc_owner = Arc::new(owner);

  // simulate multiple users edit the same collab. All of them have read and write permission
  for i in 0..3 {
    let owner = arc_owner.clone();
    let object_id = object_id.clone();
    let collab_type = collab_type.clone();
    let workspace_id = workspace_id.clone();
    let task = tokio::spawn(async move {
      let mut new_member = TestClient::new_user().await;
      // sleep 2 secs to make sure it do not trigger register user too fast in gotrue
      sleep(Duration::from_secs(i % 3)).await;

      owner
        .invite_and_accepted_workspace_member(&workspace_id, &new_member, AFRole::Member)
        .await
        .unwrap();
      owner
        .add_collab_member(
          &workspace_id,
          &object_id,
          &new_member,
          AFAccessLevel::ReadAndWrite,
        )
        .await;

      new_member
        .open_collab(&workspace_id, &object_id, collab_type.clone())
        .await;

      // generate random string and insert it to the collab
      let random_str = generate_random_string(200);
      new_member
        .collabs
        .get_mut(&object_id)
        .unwrap()
        .collab
        .lock()
        .insert(&i.to_string(), random_str.clone());
      new_member
        .wait_object_sync_complete(&object_id)
        .await
        .unwrap();
      (random_str, new_member)
    });
    tasks.push(task);
  }

  let results = futures::future::join_all(tasks).await;
  let mut expected_json = HashMap::new();
  let mut clients = vec![];
  for (index, result) in results.into_iter().enumerate() {
    let (s, client) = result.unwrap();
    clients.push(client);
    expected_json.insert(index.to_string(), s);
  }

  // wait 5 seconds to make sure all the server broadcast the updates to all the clients
  sleep(Duration::from_secs(5)).await;

  // all the clients should have the same collab object
  assert_json_eq!(
    json!(expected_json),
    arc_owner
      .collabs
      .get(&object_id)
      .unwrap()
      .collab
      .to_json_value()
  );

  for client in clients {
    assert_json_include!(
      actual: json!(expected_json),
      expected: client
        .collabs
        .get(&object_id)
        .unwrap()
        .collab
        .to_json_value()
    );
  }
}

#[tokio::test]
async fn multiple_user_with_read_only_permission_edit_same_collab_test() {
  let mut tasks = Vec::new();
  let mut owner = TestClient::new_user().await;
  let collab_type = CollabType::Empty;
  let workspace_id = owner.workspace_id().await;
  let object_id = owner
    .create_and_edit_collab(&workspace_id, collab_type.clone())
    .await;

  let arc_owner = Arc::new(owner);

  for i in 0..5 {
    let owner = arc_owner.clone();
    let object_id = object_id.clone();
    let collab_type = collab_type.clone();
    let workspace_id = workspace_id.clone();
    let task = tokio::spawn(async move {
      let mut new_user = TestClient::new_user().await;
      // sleep 2 secs to make sure it do not trigger register user too fast in gotrue
      sleep(Duration::from_secs(i % 2)).await;
      owner
        .add_collab_member(
          &workspace_id,
          &object_id,
          &new_user,
          AFAccessLevel::ReadOnly,
        )
        .await;

      new_user
        .open_collab(&workspace_id, &object_id, collab_type.clone())
        .await;

      let random_str = generate_random_string(200);
      new_user
        .collabs
        .get_mut(&object_id)
        .unwrap()
        .collab
        .lock()
        .insert(&i.to_string(), random_str.clone());

      // wait 3 seconds to let the client try to send the update to the server
      // can't use want_object_sync_complete because the client do not have permission to send the update
      sleep(Duration::from_secs(3)).await;
      (random_str, new_user)
    });
    tasks.push(task);
  }

  let results = futures::future::join_all(tasks).await;
  for (index, result) in results.into_iter().enumerate() {
    let (s, client) = result.unwrap();
    let value = client
      .collabs
      .get(&object_id)
      .unwrap()
      .collab
      .to_json_value();

    assert_json_eq!(json!({index.to_string(): s}), value,);
  }
  // all the clients should have the same collab object
  assert_json_eq!(
    json!({}),
    arc_owner
      .collabs
      .get(&object_id)
      .unwrap()
      .collab
      .to_json_value(),
  );
}
