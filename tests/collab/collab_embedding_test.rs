use crate::collab::util::empty_document_editor;
use client_api_test::TestClient;
use collab_entity::CollabType;
use database_entity::dto::CreateCollabParams;
use uuid::Uuid;

#[tokio::test]
async fn query_collab_embedding_after_create_test() {
  let object_id = Uuid::new_v4().to_string();
  let mut editor = empty_document_editor(&object_id);
  let contents = vec![
    "AppFlowy is an open-source project.",
    "It is an alternative to tools like Notion.",
    "AppFlowy provides full control of your data.",
    "The project is built using Flutter for the frontend.",
    "Rust powers AppFlowy's backend for safety and performance.",
    "AppFlowy supports both personal and collaborative workflows.",
    "It is customizable and self-hostable.",
    "Users can create documents, databases, and workflows with AppFlowy.",
    "The community contributes actively to AppFlowy's development.",
    "AppFlowy aims to be fast, reliable, and feature-rich.",
  ];
  editor.insert_paragraphs(contents.into_iter().map(|s| s.to_string()).collect());

  let test_client = TestClient::new_user().await;
  let workspace_id = test_client.workspace_id().await;
  let params = CreateCollabParams {
    workspace_id: workspace_id.clone(),
    object_id: object_id.clone(),
    encoded_collab_v1: editor.encode_collab().encode_to_bytes().unwrap(),
    collab_type: CollabType::Document,
  };
  test_client.api_client.create_collab(params).await.unwrap();
  test_client
    .wait_until_get_embedding(&workspace_id, &object_id)
    .await;
}

#[tokio::test]
async fn document_full_sync_then_search_test() {
  let object_id = Uuid::new_v4().to_string();
  let mut local_document = empty_document_editor(&object_id);
  let test_client = TestClient::new_user().await;
  let workspace_id = test_client.workspace_id().await;
  let doc_state = local_document.encode_collab().encode_to_bytes().unwrap();
  let params = CreateCollabParams {
    workspace_id: workspace_id.clone(),
    object_id: object_id.clone(),
    encoded_collab_v1: doc_state,
    collab_type: CollabType::Document,
  };
  test_client.api_client.create_collab(params).await.unwrap();
  test_client
    .insert_view_to_general_space(
      &workspace_id,
      &object_id,
      "AppFlowy",
      collab_folder::ViewLayout::Document,
    )
    .await;

  let contents = vec![
    "AppFlowy is an open-source project.",
    "It is an alternative to tools like Notion.",
    "AppFlowy provides full control of your data.",
    "The project is built using Flutter for the frontend.",
    "Rust powers AppFlowy's backend for safety and performance.",
    "AppFlowy supports both personal and collaborative workflows.",
    "It is customizable and self-hostable.",
    "Users can create documents, databases, and workflows with AppFlowy.",
    "The community contributes actively to AppFlowy's development.",
    "AppFlowy aims to be fast, reliable, and feature-rich.",
  ];
  local_document.insert_paragraphs(contents.into_iter().map(|s| s.to_string()).collect());
  let encode_collab = local_document.encode_collab();

  // After full sync, two document should be the same
  test_client
    .api_client
    .collab_full_sync(
      &workspace_id,
      &object_id,
      CollabType::Document,
      encode_collab.doc_state.to_vec(),
      encode_collab.state_vector.to_vec(),
    )
    .await
    .unwrap();

  let remote_document = test_client
    .create_document_collab(&workspace_id, &object_id)
    .await;
  let remote_plain_text = remote_document.paragraphs().join("");
  let local_plain_text = local_document.document.paragraphs().join("");
  assert_eq!(local_plain_text, remote_plain_text);

  let search_result = test_client
    .wait_unit_get_search_result(&workspace_id, "workflows", 1)
    .await;
  assert_eq!(search_result.len(), 1);
  assert_eq!(search_result[0].preview, Some("AppFlowy is an open-source project. It is an alternative to tools like Notion. AppFlowy provides full control of your data. The project is built using Flutter for the frontend. Rust powers AppFlowy's ".to_string()));
}
