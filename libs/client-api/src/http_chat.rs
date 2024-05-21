use crate::http::log_request_id;
use crate::Client;
use database_entity::dto::{
  ChatMessage, CreateChatMessageParams, CreateChatParams, MessageOffset, RepeatedChatMessage,
};
use reqwest::Method;
use shared_entity::response::{AppResponse, AppResponseError};

impl Client {
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

  pub async fn create_chat_message(
    &self,
    workspace_id: &str,
    chat_id: &str,
    params: CreateChatMessageParams,
  ) -> Result<ChatMessage, AppResponseError> {
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
    AppResponse::<ChatMessage>::from_response(resp)
      .await?
      .into_data()
  }

  pub async fn get_chat_messages(
    &self,
    workspace_id: &str,
    chat_id: &str,
    offset: MessageOffset,
    limit: u64,
  ) -> Result<RepeatedChatMessage, AppResponseError> {
    let mut url = format!("{}/api/chat/{workspace_id}/{chat_id}", self.base_url);
    let mut query_params = vec![("limit", limit.to_string())];
    match offset {
      MessageOffset::Offset(offset_value) => {
        query_params.push(("offset", offset_value.to_string()));
      },
      MessageOffset::AfterMessageId(message_id) => {
        query_params.push(("after", message_id.to_string()));
      },
      MessageOffset::BeforeMessageId(message_id) => {
        query_params.push(("before", message_id.to_string()));
      },
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
}
