use crate::collab::util::{
  alex_banker_story, alex_software_engineer_story, empty_document_editor,
  snowboarding_in_japan_plan, TestDocumentEditor,
};
use client_api_test::{ai_test_enabled, collect_answer, TestClient};
use collab_entity::CollabType;
use database_entity::dto::CreateCollabParams;
use futures_util::future::join_all;
use shared_entity::dto::chat_dto::{CreateChatMessageParams, CreateChatParams, UpdateChatParams};
use shared_entity::dto::workspace_dto::EmbeddedCollabQuery;
use std::sync::Arc;
use uuid::Uuid;

struct TestDoc {
  object_id: String,
  editor: TestDocumentEditor,
}

impl TestDoc {
  fn new(contents: Vec<&'static str>) -> Self {
    let object_id = Uuid::new_v4().to_string();
    let mut editor = empty_document_editor(&object_id);
    editor.insert_paragraphs(contents.into_iter().map(|s| s.to_string()).collect());

    Self { object_id, editor }
  }
}

#[tokio::test]
async fn chat_with_multiple_selected_source_test() {
  if !ai_test_enabled() {
    return;
  }

  let docs = vec![
    TestDoc::new(alex_software_engineer_story()),
    TestDoc::new(snowboarding_in_japan_plan()),
  ];

  let test_client = Arc::new(TestClient::new_user().await);
  let workspace_id = test_client.workspace_id().await;

  // Use futures' join_all to run async tasks concurrently
  let tasks: Vec<_> = docs
    .iter()
    .map(|doc| {
      let params = CreateCollabParams {
        workspace_id: workspace_id.clone(),
        object_id: doc.object_id.clone(),
        encoded_collab_v1: doc.editor.encode_collab().encode_to_bytes().unwrap(),
        collab_type: CollabType::Document,
      };
      let cloned_test_client = Arc::clone(&test_client);
      async move {
        // Create collaboration and wait for embedding in parallel
        cloned_test_client
          .api_client
          .create_collab(params)
          .await
          .unwrap();
      }
    })
    .collect();
  join_all(tasks).await;

  // batch query the collab embedding info
  let query = docs
    .iter()
    .map(|doc| EmbeddedCollabQuery {
      collab_type: CollabType::Document,
      object_id: doc.object_id.clone(),
    })
    .collect();
  test_client
    .wait_until_all_embedding(&workspace_id, query)
    .await;

  // create chat
  let chat_id = uuid::Uuid::new_v4().to_string();
  let params = CreateChatParams {
    chat_id: chat_id.clone(),
    name: "my first chat".to_string(),
    rag_ids: vec![],
  };
  test_client
    .api_client
    .create_chat(&workspace_id, params)
    .await
    .unwrap();

  // use alex_software_engineer_story as chat context
  let params = UpdateChatParams {
    name: None,
    metadata: None,
    rag_ids: Some(vec![docs[0].object_id.clone()]),
  };
  test_client
    .api_client
    .update_chat_settings(&workspace_id, &chat_id, params)
    .await
    .unwrap();

  // ask question that relate to the plan to Japan. The chat doesn't know any plan to Japan because
  // I have added the snowboarding_in_japan_plan as a chat context.
  let answer = ask_question(
    &test_client,
    &workspace_id,
    &chat_id,
    "When do we take off to Japan? Just tell me the date, and if you don't know, Just say you don’t know the date for the trip to Japan",
  )
  .await;
  let expected_unknown_japan_answer = r#"I don’t know the date for your trip to Japan"#;
  test_client
    .assert_similarity(
      &workspace_id,
      &answer,
      expected_unknown_japan_answer,
      0.7,
      true,
    )
    .await;

  // update chat context to snowboarding_in_japan_plan
  let params = UpdateChatParams {
    name: None,
    metadata: None,
    rag_ids: Some(vec![docs[0].object_id.clone(), docs[1].object_id.clone()]),
  };
  test_client
    .api_client
    .update_chat_settings(&workspace_id, &chat_id, params)
    .await
    .unwrap();
  let answer = ask_question(
    &test_client,
    &workspace_id,
    &chat_id,
    "when do we take off to Japan? Just tell me the date",
  )
  .await;
  let expected = r#"
  You take off to Japan on **January 7th**
  "#;
  test_client
    .assert_similarity(&workspace_id, &answer, expected, 0.8, false)
    .await;

  // Ask question for alex to make sure two documents are treated as chat context
  let answer = ask_question(
    &test_client,
    &workspace_id,
    &chat_id,
    "Can you list the sports Alex enjoys? Please provide just the names, separated by commas",
  )
  .await;
  let expected = r#"Tennis, basketball, cycling, badminton, snowboarding."#;
  test_client
    .assert_similarity(&workspace_id, &answer, expected, 0.8, true)
    .await;

  // remove the Japan plan and check the response. After remove the Japan plan, the chat should not
  // know about the plan to Japan.
  let params = UpdateChatParams {
    name: None,
    metadata: None,
    rag_ids: Some(vec![]),
  };
  test_client
    .api_client
    .update_chat_settings(&workspace_id, &chat_id, params)
    .await
    .unwrap();
  let answer = ask_question(
    &test_client,
    &workspace_id,
    &chat_id,
    "When do we take off to Japan? Just tell me the date, and if you don't know, Just say you don’t know the date for the trip to Japan",
  )
  .await;
  test_client
    .assert_similarity(
      &workspace_id,
      &answer,
      expected_unknown_japan_answer,
      0.7,
      true,
    )
    .await;
}

#[tokio::test]
async fn chat_with_selected_source_override_test() {
  if !ai_test_enabled() {
    return;
  }
  let object_id = Uuid::new_v4().to_string();
  let mut editor = empty_document_editor(&object_id);
  let contents = alex_software_engineer_story();
  editor.insert_paragraphs(contents.into_iter().map(|s| s.to_string()).collect());
  let encode_collab = editor.encode_collab();

  let test_client = TestClient::new_user().await;
  let workspace_id = test_client.workspace_id().await;
  let params = CreateCollabParams {
    workspace_id: workspace_id.clone(),
    object_id: object_id.clone(),
    encoded_collab_v1: encode_collab.encode_to_bytes().unwrap(),
    collab_type: CollabType::Document,
  };
  test_client.api_client.create_collab(params).await.unwrap();
  test_client
    .wait_until_get_embedding(&workspace_id, &object_id)
    .await;

  // chat with document
  let chat_id = uuid::Uuid::new_v4().to_string();
  let params = CreateChatParams {
    chat_id: chat_id.clone(),
    name: "my first chat".to_string(),
    rag_ids: vec![object_id.clone()],
  };

  // create a chat
  test_client
    .api_client
    .create_chat(&workspace_id, params)
    .await
    .unwrap();

  // ask question to check the chat is using document embedding or not
  let answer = ask_question(
    &test_client,
    &workspace_id,
    &chat_id,
    "What are some of the sports Alex enjoys, and what are his experiences with them",
  )
  .await;
  let expected = r#"
  Alex enjoys a variety of sports that keep him active and engaged:
	1.	Tennis: Learned in Singapore, he plays on weekends with friends.
	2.	Basketball: Enjoys casual play, though specific details aren’t provided.
	3.	Cycling: Brought his bike to Singapore and looks forward to exploring parks.
	4.	Badminton: Enjoys it, though details aren’t given.
	5.	Snowboarding: Had an unforgettable experience on challenging slopes in Lake Tahoe.
Overall, Alex balances his work as a software programmer with his passion for sports, finding excitement and freedom in each activity.
  "#;
  test_client
    .assert_similarity(&workspace_id, &answer, expected, 0.8, true)
    .await;

  // remove all content for given document
  editor.clear();

  // Simulate insert new content
  let contents = alex_banker_story();
  editor.insert_paragraphs(contents.into_iter().map(|s| s.to_string()).collect());
  let text = editor.document.paragraphs().join("");
  let expected = alex_banker_story().join("");
  assert_eq!(text, expected);

  // full sync
  let encode_collab = editor.encode_collab();
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

  // after full sync, chat with the same question. After update the document content, the chat
  // should not reply with previous context.
  let answer = ask_question(
    &test_client,
    &workspace_id,
    &chat_id,
    "What are some of the sports Alex enjoys, and what are his experiences with them",
  )
  .await;
  let expected = r#"
 Alex does not enjoy sports or physical activities. Instead, he prefers to relax and finds joy in
 exploring delicious food and trying new restaurants. For Alex, food is a form of relaxation and self-care,
 making it his favorite way to unwind rather than engaging in sports. While he may not have experiences with sports,
  he certainly has many experiences in the culinary world, where he enjoys savoring flavors and discovering new dishes
  "#;
  test_client
    .assert_similarity(&workspace_id, &answer, expected, 0.8, true)
    .await;
}

async fn ask_question(
  test_client: &TestClient,
  workspace_id: &str,
  chat_id: &str,
  question: &str,
) -> String {
  let params = CreateChatMessageParams::new_user(question);
  let question = test_client
    .api_client
    .create_question(workspace_id, chat_id, params)
    .await
    .unwrap();
  let answer_stream = test_client
    .api_client
    .stream_answer_v2(workspace_id, chat_id, question.message_id)
    .await
    .unwrap();
  collect_answer(answer_stream).await
}
