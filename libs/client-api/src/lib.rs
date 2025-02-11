mod http;
mod http_ai;
mod http_billing;

mod http_access_request;
mod http_blob;
mod http_collab;
mod http_member;
mod http_publish;
mod http_quick_note;
mod http_search;
mod http_template;
mod http_view;
pub use http::*;

#[cfg(feature = "collab-sync")]
pub mod collab_sync;

mod http_chat;
mod http_file;
mod http_settings;
pub mod notify;
mod ping;
mod retry;
pub mod ws;
pub mod ws2;

pub mod error {
  pub use shared_entity::response::AppResponseError;
  pub use shared_entity::response::ErrorCode;
}

// Export all dto entities that will be used in the frontend application
pub mod entity {
  #[cfg(not(target_arch = "wasm32"))]
  pub use crate::http_chat::{QuestionStream, QuestionStreamValue};
  pub use client_api_entity::*;
}

#[cfg(feature = "template")]
pub mod template {
  pub use workspace_template;
}
