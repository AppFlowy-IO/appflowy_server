use crate::http::log_request_id;
use crate::Client;
use bytes::Bytes;
use client_api_entity::{
  ChatMessage, CreateAnswerMessageParams, CreateChatMessageParams, CreateChatParams, MessageCursor,
  RepeatedChatMessage, UpdateChatMessageContentParams,
};
use futures_core::{ready, Stream};
use pin_project::pin_project;
use reqwest::Method;
use serde_json::Value;
use shared_entity::dto::ai_dto::{
  CreateTextChatContext, RepeatedRelatedQuestion, STEAM_ANSWER_KEY, STEAM_METADATA_KEY,
};
use shared_entity::response::{AppResponse, AppResponseError};
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::error;

impl Client {
  /// Create a new chat
  pub async fn create_chat(
    &self,
    workspace_id: &str,
    params: CreateChatParams,
  ) -> Result<(), AppResponseError> {
    let url = format!("{}/api/chat/{workspace_id}", self.base_url);
    let resp = self
      .http_client_with_auth(Method::POST, &url)
      .await?
      .json(&params)
      .send()
      .await?;
    log_request_id(&resp);
    AppResponse::<()>::from_response(resp).await?.into_error()
  }

  /// Delete a chat for given chat_id
  pub async fn delete_chat(
    &self,
    workspace_id: &str,
    chat_id: &str,
  ) -> Result<(), AppResponseError> {
    let url = format!("{}/api/chat/{workspace_id}/{chat_id}", self.base_url);
    let resp = self
      .http_client_with_auth(Method::DELETE, &url)
      .await?
      .send()
      .await?;
    log_request_id(&resp);
    AppResponse::<()>::from_response(resp).await?.into_error()
  }

  /// Save a question message to a chat
  pub async fn save_question(
    &self,
    workspace_id: &str,
    chat_id: &str,
    params: CreateChatMessageParams,
  ) -> Result<ChatMessage, AppResponseError> {
    let url = format!(
      "{}/api/chat/{workspace_id}/{chat_id}/message/question",
      self.base_url
    );
    let resp = self
      .http_client_with_auth(Method::POST, &url)
      .await?
      .json(&params)
      .send()
      .await?;
    log_request_id(&resp);
    AppResponse::<ChatMessage>::from_response(resp)
      .await?
      .into_data()
  }

  /// save an answer message to a chat
  pub async fn save_answer(
    &self,
    workspace_id: &str,
    chat_id: &str,
    params: CreateAnswerMessageParams,
  ) -> Result<ChatMessage, AppResponseError> {
    let url = format!(
      "{}/api/chat/{workspace_id}/{chat_id}/message/answer",
      self.base_url
    );
    let resp = self
      .http_client_with_auth(Method::POST, &url)
      .await?
      .json(&params)
      .send()
      .await?;
    log_request_id(&resp);
    AppResponse::<ChatMessage>::from_response(resp)
      .await?
      .into_data()
  }

  /// Ask AI with a question for given question's message_id
  pub async fn stream_answer(
    &self,
    workspace_id: &str,
    chat_id: &str,
    question_message_id: i64,
  ) -> Result<impl Stream<Item = Result<Bytes, AppResponseError>>, AppResponseError> {
    let url = format!(
      "{}/api/chat/{workspace_id}/{chat_id}/{question_message_id}/answer/stream",
      self.base_url
    );
    let resp = self
      .http_client_with_auth(Method::GET, &url)
      .await?
      .send()
      .await?;
    log_request_id(&resp);
    AppResponse::<()>::answer_response_stream(resp).await
  }

  pub async fn stream_answer_v2(
    &self,
    workspace_id: &str,
    chat_id: &str,
    question_message_id: i64,
  ) -> Result<QuestionStream, AppResponseError> {
    let url = format!(
      "{}/api/chat/{workspace_id}/{chat_id}/{question_message_id}/v2/answer/stream",
      self.base_url
    );
    let resp = self
      .http_client_with_auth(Method::GET, &url)
      .await?
      .send()
      .await?;
    log_request_id(&resp);
    let stream = AppResponse::<serde_json::Value>::json_response_stream(resp).await?;
    Ok(QuestionStream::new(stream))
  }

  /// Generate an answer for given question's message_id. The same as ask_question but return ChatMessage
  /// instead of stream of Bytes
  pub async fn get_answer(
    &self,
    workspace_id: &str,
    chat_id: &str,
    question_message_id: i64,
  ) -> Result<ChatMessage, AppResponseError> {
    let url = format!(
      "{}/api/chat/{workspace_id}/{chat_id}/{question_message_id}/answer",
      self.base_url
    );
    let resp = self
      .http_client_with_auth(Method::GET, &url)
      .await?
      .send()
      .await?;
    log_request_id(&resp);
    AppResponse::<ChatMessage>::from_response(resp)
      .await?
      .into_data()
  }

  /// Update chat message content. It will override the content of the message.
  /// A message can be a question or an answer
  pub async fn update_chat_message(
    &self,
    workspace_id: &str,
    chat_id: &str,
    params: UpdateChatMessageContentParams,
  ) -> Result<(), AppResponseError> {
    let url = format!(
      "{}/api/chat/{workspace_id}/{chat_id}/message",
      self.base_url
    );
    let resp = self
      .http_client_with_auth(Method::PUT, &url)
      .await?
      .json(&params)
      .send()
      .await?;
    log_request_id(&resp);
    AppResponse::<()>::from_response(resp).await?.into_error()
  }

  /// Get related question for a chat message. The message_d should be the question's id
  pub async fn get_chat_related_question(
    &self,
    workspace_id: &str,
    chat_id: &str,
    message_id: i64,
  ) -> Result<RepeatedRelatedQuestion, AppResponseError> {
    let url = format!(
      "{}/api/chat/{workspace_id}/{chat_id}/{message_id}/related_question",
      self.base_url
    );
    let resp = self
      .http_client_with_auth(Method::GET, &url)
      .await?
      .send()
      .await?;
    log_request_id(&resp);
    AppResponse::<RepeatedRelatedQuestion>::from_response(resp)
      .await?
      .into_data()
  }

  /// Return list of chat messages for a chat
  pub async fn get_chat_messages(
    &self,
    workspace_id: &str,
    chat_id: &str,
    offset: MessageCursor,
    limit: u64,
  ) -> Result<RepeatedChatMessage, AppResponseError> {
    let mut url = format!("{}/api/chat/{workspace_id}/{chat_id}", self.base_url);
    let mut query_params = vec![("limit", limit.to_string())];
    match offset {
      MessageCursor::Offset(offset_value) => {
        query_params.push(("offset", offset_value.to_string()));
      },
      MessageCursor::AfterMessageId(message_id) => {
        query_params.push(("after", message_id.to_string()));
      },
      MessageCursor::BeforeMessageId(message_id) => {
        query_params.push(("before", message_id.to_string()));
      },
      MessageCursor::NextBack => {},
    }
    let query = serde_urlencoded::to_string(&query_params).unwrap();
    url = format!("{}?{}", url, query);
    let resp = self
      .http_client_with_auth(Method::GET, &url)
      .await?
      .send()
      .await?;
    AppResponse::<RepeatedChatMessage>::from_response(resp)
      .await?
      .into_data()
  }

  /// It's no longer used in the frontend application since 0.6.0 version.
  pub async fn create_question_answer(
    &self,
    workspace_id: &str,
    chat_id: &str,
    params: CreateChatMessageParams,
  ) -> Result<impl Stream<Item = Result<ChatMessage, AppResponseError>>, AppResponseError> {
    let url = format!(
      "{}/api/chat/{workspace_id}/{chat_id}/message",
      self.base_url
    );
    let resp = self
      .http_client_with_auth(Method::POST, &url)
      .await?
      .json(&params)
      .send()
      .await?;
    log_request_id(&resp);
    AppResponse::<ChatMessage>::json_response_stream(resp).await
  }

  pub async fn create_chat_context(
    &self,
    workspace_id: &str,
    params: CreateTextChatContext,
  ) -> Result<(), AppResponseError> {
    let url = format!(
      "{}/api/chat/{workspace_id}/{}/context/text",
      self.base_url, params.chat_id
    );
    let resp = self
      .http_client_with_auth(Method::POST, &url)
      .await?
      .json(&params)
      .send()
      .await?;
    log_request_id(&resp);
    AppResponse::<()>::from_response(resp).await?.into_error()
  }
}

#[pin_project]
pub struct QuestionStream {
  stream: Pin<Box<dyn Stream<Item = Result<serde_json::Value, AppResponseError>> + Send>>,
  buffer: Vec<u8>,
}

impl QuestionStream {
  pub fn new<S>(stream: S) -> Self
  where
    S: Stream<Item = Result<serde_json::Value, AppResponseError>> + Send + 'static,
  {
    QuestionStream {
      stream: Box::pin(stream),
      buffer: Vec::new(),
    }
  }
}

pub enum QuestionStreamValue {
  Answer { value: String },
  Metadata { value: serde_json::Value },
}

impl Stream for QuestionStream {
  type Item = Result<QuestionStreamValue, AppResponseError>;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = self.project();

    loop {
      match ready!(this.stream.as_mut().poll_next(cx)) {
        Some(Ok(value)) => {
          if let Value::Object(mut value) = value {
            if let Some(metadata) = value.remove(STEAM_METADATA_KEY) {
              return Poll::Ready(Some(Ok(QuestionStreamValue::Metadata { value: metadata })));
            }

            if let Some(answer) = value
              .remove(STEAM_ANSWER_KEY)
              .and_then(|s| s.as_str().map(ToString::to_string))
            {
              return Poll::Ready(Some(Ok(QuestionStreamValue::Answer { value: answer })));
            }

            error!("Invalid streaming value: {:?}", value);
          }
        },
        Some(Err(err)) => return Poll::Ready(Some(Err(err))),
        None => return Poll::Ready(None),
      }
    }
  }
}
