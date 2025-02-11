mod actors;
pub mod messages;

pub use actors::server::WsServer;
pub use actors::session::WsSession;

/// A unique workspace identifier.
pub type WorkspaceId = uuid::Uuid;

/// A unique [collab::preclude::Collab] identifier.
pub type Oid = uuid::Uuid;
