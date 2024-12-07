use collab::preclude::{GetString, Text, Transact, WriteTxn};
use collab_entity::CollabType;

use client_api_test::{TestClient, TestCollab};
use database_entity::dto::AFRole;

#[tokio::test]
async fn test_sync_collab() {
  let mut client_1 = TestClient::new_user().await;
  let mut client_2 = TestClient::new_user().await;

  let workspace_id = client_1.workspace_id().await;
  let object_id = client_1
    .create_and_edit_collab(&workspace_id, CollabType::Unknown)
    .await;

  // user 1 invites user 2 to the workspace
  client_1
    .invite_and_accepted_workspace_member(&workspace_id, &client_2, AFRole::Member)
    .await
    .unwrap();

  client_2
    .open_collab(&workspace_id, &object_id, CollabType::Unknown)
    .await;

  //TODO: user 1 disconnects from the server

  // user 1 edits the collab
  {
    let collab_1 = &client_1.collabs[&object_id];
    let mut lock = collab_1.mutex_collab.lock();
    let doc = lock.get_doc();
    let mut txn = doc.transact_mut();
    let txt = txn.get_or_insert_text("text");
    txt.insert(&mut txn, 0, "1");
  }

  // user 2 opens collab (out-of-sync with user 1)
  client_2
    .open_collab(&workspace_id, &object_id, CollabType::Unknown)
    .await;

  // user 2 edits the collab
  {
    let collab_2 = &client_2.collabs[&object_id];
    let mut lock = collab_2.mutex_collab.lock();
    let doc = lock.get_doc();
    let mut txn = doc.transact_mut();
    let txt = txn.get_or_insert_text("text");
    txt.insert(&mut txn, 0, "2");
  }
  client_2
    .wait_object_sync_complete(&object_id)
    .await
    .unwrap();

  //TODO: user 1 re-connects to the server
  client_1
    .wait_object_sync_complete(&object_id)
    .await
    .unwrap();

  // user 1 should see the changes made by user 2
  let str = get_text(&client_1.collabs[&object_id]);
  assert!(str == "12" || str == "21", "1st should see 2nd's edits");

  tokio::time::sleep(std::time::Duration::from_secs(2)).await;

  // user 2 should see the changes made by user 1
  let str = get_text(&client_2.collabs[&object_id]);
  assert!(str == "12" || str == "21", "2nd should see 1st's edits");
}

fn get_text(collab: &TestCollab) -> String {
  let mut lock = collab.mutex_collab.lock();
  let doc = lock.get_doc();
  let mut txn = doc.transact_mut();
  let txt = txn.get_or_insert_text("text");
  txt.get_string(&txn)
}
