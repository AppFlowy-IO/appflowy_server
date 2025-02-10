use crate::error::EntityError;
use crate::error::EntityError::{DeserializationError, InvalidData};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use collab_entity::proto;
use collab_entity::CollabType;
use infra::validate::{validate_not_empty_payload, validate_not_empty_str};
use prost::Message;
use serde::{Deserialize, Serialize};

use serde_repr::{Deserialize_repr, Serialize_repr};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Display;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use tracing::error;
use uuid::Uuid;
use validator::Validate;

/// The default compression level of ZSTD-compressed collabs.
pub const ZSTD_COMPRESSION_LEVEL: i32 = 3;

#[derive(Debug, Clone, Validate, Serialize, Deserialize)]
pub struct CreateCollabParams {
  #[validate(custom(function = "validate_not_empty_str"))]
  pub workspace_id: String,

  #[validate(custom(function = "validate_not_empty_str"))]
  pub object_id: String,

  #[validate(custom(function = "validate_not_empty_payload"))]
  pub encoded_collab_v1: Vec<u8>,

  pub collab_type: CollabType,
}

impl From<(String, CollabParams)> for CreateCollabParams {
  fn from((workspace_id, collab_params): (String, CollabParams)) -> Self {
    Self {
      workspace_id,
      object_id: collab_params.object_id,
      encoded_collab_v1: collab_params.encoded_collab_v1.to_vec(),
      collab_type: collab_params.collab_type,
    }
  }
}

impl CreateCollabParams {
  pub fn split(self) -> (CollabParams, String) {
    (
      CollabParams {
        object_id: self.object_id,
        encoded_collab_v1: Bytes::from(self.encoded_collab_v1),
        collab_type: self.collab_type,
      },
      self.workspace_id,
    )
  }

  pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
    bincode::serialize(self)
  }
  pub fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
    bincode::deserialize(bytes)
  }
}

pub struct CollabIndexParams {}

pub struct PendingCollabWrite {
  pub workspace_id: String,
  pub uid: i64,
  pub params: CollabParams,
}

impl PendingCollabWrite {
  pub fn new(workspace_id: String, uid: i64, params: CollabParams) -> Self {
    PendingCollabWrite {
      workspace_id,
      uid,
      params,
    }
  }
}

#[derive(Debug, Clone, Validate, Serialize, Deserialize, PartialEq)]
pub struct CollabParams {
  #[validate(custom(function = "validate_not_empty_str"))]
  pub object_id: String,
  #[validate(custom(function = "validate_not_empty_payload"))]
  pub encoded_collab_v1: Bytes,
  pub collab_type: CollabType,
}

impl Display for CollabParams {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "object_id: {}, collab_type: {:?}, size:{}",
      self.object_id,
      self.collab_type,
      self.encoded_collab_v1.len()
    )
  }
}

impl CollabParams {
  pub fn new<T: ToString, B: Into<Bytes>>(
    object_id: T,
    collab_type: CollabType,
    encoded_collab_v1: B,
  ) -> Self {
    let object_id = object_id.to_string();
    Self {
      object_id,
      collab_type,
      encoded_collab_v1: encoded_collab_v1.into(),
    }
  }

  pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
    bincode::serialize(self)
  }

  pub fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
    match bincode::deserialize(bytes) {
      Ok(value) => Ok(value),
      Err(_) => {
        // fallback to deserialize into older version
        let old: CollabParamsV0 = bincode::deserialize(bytes)?;
        Ok(Self {
          object_id: old.object_id,
          encoded_collab_v1: old.encoded_collab_v1.into(),
          collab_type: old.collab_type,
        })
      },
    }
  }

  pub fn to_proto(&self) -> proto::collab::CollabParams {
    proto::collab::CollabParams {
      object_id: self.object_id.clone(),
      encoded_collab: self.encoded_collab_v1.to_vec(),
      collab_type: self.collab_type.to_proto() as i32,
      embeddings: None,
    }
  }

  pub fn to_protobuf_bytes(&self) -> Vec<u8> {
    self.to_proto().encode_to_vec()
  }

  pub fn from_protobuf_bytes(bytes: &[u8]) -> Result<Self, EntityError> {
    match proto::collab::CollabParams::decode(bytes) {
      Ok(proto) => Self::try_from(proto),
      Err(err) => Err(DeserializationError(err.to_string())),
    }
  }
}

impl TryFrom<proto::collab::CollabParams> for CollabParams {
  type Error = EntityError;

  fn try_from(proto: proto::collab::CollabParams) -> Result<Self, Self::Error> {
    let collab_type_proto = proto::collab::CollabType::try_from(proto.collab_type).unwrap();
    let collab_type = CollabType::from_proto(&collab_type_proto);
    Ok(Self {
      object_id: proto.object_id,
      encoded_collab_v1: Bytes::from(proto.encoded_collab),
      collab_type,
    })
  }
}

#[derive(Serialize, Deserialize)]
struct CollabParamsV0 {
  object_id: String,
  encoded_collab_v1: Vec<u8>,
  collab_type: CollabType,
}

#[derive(Debug, Clone, Validate, Serialize, Deserialize)]
pub struct BatchCreateCollabParams {
  #[validate(custom(function = "validate_not_empty_str"))]
  pub workspace_id: String,
  pub params_list: Vec<CollabParams>,
}

impl BatchCreateCollabParams {
  pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
    bincode::serialize(self)
  }

  pub fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
    bincode::deserialize(bytes)
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateCollabWebParams {
  pub doc_state: Vec<u8>,
  pub collab_type: CollabType,
}

#[derive(Debug, Clone, Validate, Serialize, Deserialize)]
pub struct DeleteCollabParams {
  #[validate(custom(function = "validate_not_empty_str"))]
  pub object_id: String,
  #[validate(custom(function = "validate_not_empty_str"))]
  pub workspace_id: String,
}

#[derive(Debug, Clone, Validate)]
pub struct InsertSnapshotParams {
  #[validate(custom(function = "validate_not_empty_str"))]
  pub object_id: String,
  #[validate(custom(function = "validate_not_empty_payload"))]
  pub doc_state: Bytes,
  #[validate(custom(function = "validate_not_empty_str"))]
  pub workspace_id: String,
  pub collab_type: CollabType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
  pub object_id: String,
  pub encoded_collab_v1: Vec<u8>,
  pub workspace_id: String,
}

#[derive(Debug, Clone, Validate, Serialize, Deserialize)]
pub struct QuerySnapshotParams {
  pub snapshot_id: i64,
}

#[derive(Debug, Clone, Validate, Serialize, Deserialize)]
pub struct QueryCollabParams {
  #[validate(custom(function = "validate_not_empty_str"))]
  pub workspace_id: String,
  #[validate(nested)]
  pub inner: QueryCollab,
}

impl Display for QueryCollabParams {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "workspace_id: {}, object_id: {}, collab_type: {:?}",
      self.workspace_id, self.object_id, self.collab_type
    )
  }
}

impl QueryCollabParams {
  pub fn new<T1: Into<String>, T2: Into<String>>(
    object_id: T1,
    collab_type: CollabType,
    workspace_id: T2,
  ) -> Self {
    let workspace_id = workspace_id.into();
    let object_id = object_id.into();
    let inner = QueryCollab {
      object_id,
      collab_type,
    };
    Self {
      workspace_id,
      inner,
    }
  }
}

impl Deref for QueryCollabParams {
  type Target = QueryCollab;

  fn deref(&self) -> &Self::Target {
    &self.inner
  }
}

#[derive(Debug, Clone, Validate, Serialize, Deserialize)]
pub struct QueryCollab {
  #[validate(custom(function = "validate_not_empty_str"))]
  pub object_id: String,
  pub collab_type: CollabType,
}
impl QueryCollab {
  pub fn new<T: ToString>(object_id: T, collab_type: CollabType) -> Self {
    let object_id = object_id.to_string();
    Self {
      object_id,
      collab_type,
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchQueryCollabParams(pub Vec<QueryCollab>);

impl Deref for BatchQueryCollabParams {
  type Target = Vec<QueryCollab>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl DerefMut for BatchQueryCollabParams {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AFSnapshotMeta {
  pub snapshot_id: i64,
  pub object_id: String,
  pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AFSnapshotMetas(pub Vec<AFSnapshotMeta>);

#[derive(Debug, Clone, Deserialize)]
pub struct QueryObjectSnapshotParams {
  pub object_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AFBlobRecord {
  pub file_id: String,
}

impl AFBlobRecord {
  pub fn new(file_id: String) -> Self {
    Self { file_id }
  }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum QueryCollabResult {
  Success { encode_collab_v1: Vec<u8> },
  Failed { error: String },
}

#[derive(Serialize, Deserialize)]
pub struct BatchQueryCollabResult(pub HashMap<String, QueryCollabResult>);

#[derive(Serialize, Deserialize)]
pub struct WorkspaceUsage {
  pub total_document_size: i64,
}

#[derive(Debug, Clone, Validate, Serialize, Deserialize)]
pub struct InsertCollabMemberParams {
  pub uid: i64,
  #[validate(custom(function = "validate_not_empty_str"))]
  pub workspace_id: String,
  #[validate(custom(function = "validate_not_empty_str"))]
  pub object_id: String,
  pub access_level: AFAccessLevel,
}

pub type UpdateCollabMemberParams = InsertCollabMemberParams;

#[derive(Debug, Clone, Validate, Serialize, Deserialize)]
pub struct WorkspaceCollabIdentify {
  pub uid: i64,
  #[validate(custom(function = "validate_not_empty_str"))]
  pub workspace_id: String,
  #[validate(custom(function = "validate_not_empty_str"))]
  pub object_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct UpdatePublishNamespace {
  pub old_namespace: String,
  pub new_namespace: String,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateDefaultPublishView {
  pub view_id: Uuid,
}

#[derive(Serialize, Deserialize)]
pub struct DefaultPublishViewInfoMeta {
  pub info: PublishInfo,
  pub meta: serde_json::Value,
}

#[derive(Debug, Clone, Validate, Serialize, Deserialize)]
pub struct QueryCollabMembers {
  #[validate(custom(function = "validate_not_empty_str"))]
  pub workspace_id: String,
  #[validate(custom(function = "validate_not_empty_str"))]
  pub object_id: String,
}

#[derive(Debug, Clone, Validate, Serialize, Deserialize)]
pub struct QueryWorkspaceMember {
  #[validate(custom(function = "validate_not_empty_str"))]
  pub workspace_id: String,

  pub uid: i64,
}

#[derive(Serialize, Deserialize)]
pub struct AFCollabMember {
  pub uid: i64,
  pub oid: String,
  pub permission: AFPermission,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AFCollabEmbedInfo {
  pub object_id: String,
  /// The timestamp when the object's embeddings updated
  pub indexed_at: DateTime<Utc>,
  /// The timestamp when the object's data updated
  pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RepeatedAFCollabEmbedInfo(pub Vec<AFCollabEmbedInfo>);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PublishInfo {
  pub namespace: String,
  pub publish_name: String,
  pub view_id: Uuid,
  #[serde(default)]
  pub publisher_email: String,
  #[serde(default)]
  pub publish_timestamp: DateTime<Utc>,
  #[serde(default)]
  pub unpublished_timestamp: Option<DateTime<Utc>>,
  #[serde(default = "default_comments_enabled")]
  pub comments_enabled: bool,
  #[serde(default = "default_duplicate_enabled")]
  pub duplicate_enabled: bool,
}

fn default_comments_enabled() -> bool {
  true
}

fn default_duplicate_enabled() -> bool {
  true
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PublishInfoMeta<Meta> {
  pub info: PublishInfo,
  pub meta: Meta,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone, Hash)]
#[repr(i32)]
pub enum AFRole {
  Owner = 1,
  Member = 2,
  Guest = 3,
}

impl AFRole {
  /// The user can create a [Collab] if the user is [AFRole::Owner] or [AFRole::Member] of the workspace.
  pub fn can_create_collab(&self) -> bool {
    matches!(self, AFRole::Owner | AFRole::Member)
  }
}

impl From<i32> for AFRole {
  fn from(value: i32) -> Self {
    // Can't modify the value of the enum
    match value {
      1 => AFRole::Owner,
      2 => AFRole::Member,
      3 => AFRole::Guest,
      _ => {
        error!("Invalid role id: {}", value);
        AFRole::Guest
      },
    }
  }
}

impl From<&str> for AFRole {
  fn from(value: &str) -> Self {
    match i32::from_str(value) {
      Ok(value) => value.into(),
      Err(_) => AFRole::Guest,
    }
  }
}

impl From<AFRole> for i32 {
  fn from(role: AFRole) -> Self {
    role as i32
  }
}

impl From<&AFRole> for i32 {
  fn from(role: &AFRole) -> Self {
    role.clone() as i32
  }
}

impl PartialOrd for AFRole {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for AFRole {
  fn cmp(&self, other: &Self) -> Ordering {
    let left = i32::from(self);
    let right = i32::from(other);
    // lower value has higher priority
    left.cmp(&right).reverse()
  }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AFPermission {
  /// The permission id
  pub id: i32,
  pub name: String,
  pub access_level: AFAccessLevel,
  pub description: String,
}

#[derive(Deserialize_repr, Serialize_repr, Eq, PartialEq, Debug, Clone, Copy)]
#[repr(i32)]
pub enum AFAccessLevel {
  // Can't modify the value of the enum
  ReadOnly = 10,
  ReadAndComment = 20,
  ReadAndWrite = 30,
  FullAccess = 50,
}

impl AFAccessLevel {
  pub fn can_write(&self) -> bool {
    match self {
      AFAccessLevel::ReadOnly | AFAccessLevel::ReadAndComment => false,
      AFAccessLevel::ReadAndWrite | AFAccessLevel::FullAccess => true,
    }
  }

  pub fn can_delete(&self) -> bool {
    match self {
      AFAccessLevel::ReadOnly | AFAccessLevel::ReadAndComment | AFAccessLevel::ReadAndWrite => {
        false
      },
      AFAccessLevel::FullAccess => true,
    }
  }
}

impl From<&AFRole> for AFAccessLevel {
  fn from(value: &AFRole) -> Self {
    match value {
      AFRole::Owner => AFAccessLevel::FullAccess,
      AFRole::Member => AFAccessLevel::ReadAndWrite,
      AFRole::Guest => AFAccessLevel::ReadOnly,
    }
  }
}

impl From<i32> for AFAccessLevel {
  fn from(value: i32) -> Self {
    // Can't modify the value of the enum
    match value {
      10 => AFAccessLevel::ReadOnly,
      20 => AFAccessLevel::ReadAndComment,
      30 => AFAccessLevel::ReadAndWrite,
      50 => AFAccessLevel::FullAccess,
      _ => {
        error!("Invalid access level: {}", value);
        AFAccessLevel::ReadOnly
      },
    }
  }
}

impl From<&str> for AFAccessLevel {
  fn from(value: &str) -> Self {
    match i32::from_str(value) {
      Ok(value) => AFAccessLevel::from(value),
      Err(_) => AFAccessLevel::ReadOnly,
    }
  }
}

impl From<AFAccessLevel> for i32 {
  fn from(level: AFAccessLevel) -> Self {
    level as i32
  }
}

impl From<&AFAccessLevel> for i32 {
  fn from(level: &AFAccessLevel) -> Self {
    *level as i32
  }
}

impl PartialOrd for AFAccessLevel {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for AFAccessLevel {
  fn cmp(&self, other: &Self) -> Ordering {
    let left = i32::from(self);
    let right = i32::from(other);
    left.cmp(&right)
  }
}

#[derive(Serialize, Deserialize)]
pub struct AFCollabMembers(pub Vec<AFCollabMember>);

pub type RawData = Vec<u8>;

#[derive(Serialize, Deserialize)]
pub struct AFUserProfile {
  pub uid: i64,
  pub uuid: Uuid,
  pub email: Option<String>,
  pub password: Option<String>,
  pub name: Option<String>,
  pub metadata: Option<serde_json::Value>,
  pub encryption_sign: Option<String>,
  pub latest_workspace_id: Uuid,
  pub updated_at: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AFWorkspace {
  pub workspace_id: Uuid,
  pub database_storage_id: Uuid,
  pub owner_uid: i64,
  pub owner_name: String,
  #[serde(default)]
  pub owner_email: String,
  pub workspace_type: i32,
  pub workspace_name: String,
  pub created_at: DateTime<Utc>,
  pub icon: String,
  pub member_count: Option<i64>,
  #[serde(default)]
  pub role: Option<AFRole>, // role of the user requesting the workspace
}

#[derive(Serialize, Deserialize)]
pub struct AFWorkspaceSettings {
  #[serde(default)]
  pub disable_search_indexing: bool,

  #[serde(default)]
  pub ai_model: String,
}

impl Default for AFWorkspaceSettings {
  fn default() -> Self {
    Self {
      disable_search_indexing: false,
      ai_model: "".to_string(),
    }
  }
}

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct AFWorkspaceSettingsChange {
  #[serde(skip_serializing_if = "Option::is_none")]
  pub disable_search_indexing: Option<bool>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub ai_model: Option<String>,
}

impl AFWorkspaceSettingsChange {
  pub fn new() -> Self {
    Self {
      disable_search_indexing: None,
      ai_model: None,
    }
  }
  pub fn disable_search_indexing(mut self, disable_search_indexing: bool) -> Self {
    self.disable_search_indexing = Some(disable_search_indexing);
    self
  }
  pub fn ai_model(mut self, ai_model: String) -> Self {
    self.ai_model = Some(ai_model);
    self
  }
}

#[derive(Serialize, Deserialize)]
pub struct AFUserWorkspaceInfo {
  pub user_profile: AFUserProfile,
  pub visiting_workspace: AFWorkspace,
  pub workspaces: Vec<AFWorkspace>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct AFWorkspaceMember {
  pub name: String,
  pub email: String,
  pub role: AFRole,
  pub avatar_url: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct AFWorkspaceInvitation {
  pub invite_id: Uuid,
  pub workspace_id: Uuid,
  pub workspace_name: Option<String>,
  pub inviter_email: Option<String>,
  pub inviter_name: Option<String>,
  pub status: AFWorkspaceInvitationStatus,
  pub updated_at: DateTime<Utc>,
  pub inviter_icon: Option<String>,
  pub workspace_icon: String,
  pub member_count: Option<i64>, // use unwrap_or(0) to get the value
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
#[repr(i16)]
pub enum AFWorkspaceInvitationStatus {
  Pending = 0,
  Accepted = 1,
  Rejected = 2,
}

impl From<i16> for AFWorkspaceInvitationStatus {
  fn from(value: i16) -> Self {
    match value {
      0 => AFWorkspaceInvitationStatus::Pending,
      1 => AFWorkspaceInvitationStatus::Accepted,
      2 => AFWorkspaceInvitationStatus::Rejected,
      _ => {
        error!("Invalid role id: {}", value);
        AFWorkspaceInvitationStatus::Pending
      },
    }
  }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AFCollabEmbeddedChunk {
  pub fragment_id: String,
  pub object_id: String,
  pub content_type: EmbeddingContentType,
  pub content: Option<String>,
  pub embedding: Option<Vec<f32>>,
  pub metadata: serde_json::Value,
  pub fragment_index: i32,
  pub embedded_type: i16,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AFCollabEmbeddings {
  pub tokens_consumed: u32,
  pub params: Vec<AFCollabEmbeddedChunk>,
}

/// Type of content stored by the embedding.
/// Currently only plain text of the document is supported.
/// In the future, we might support other kinds like i.e. PDF, images or image-extracted text.
#[repr(i32)]
#[derive(Debug, Copy, Clone, Serialize_repr, Deserialize_repr, Eq, PartialEq)]
pub enum EmbeddingContentType {
  /// The plain text representation of the document.
  PlainText = 0,
}

impl EmbeddingContentType {
  pub fn from_proto(proto: proto::collab::EmbeddingContentType) -> Result<Self, EntityError> {
    match proto {
      proto::collab::EmbeddingContentType::PlainText => Ok(EmbeddingContentType::PlainText),
      proto::collab::EmbeddingContentType::Unknown => Err(InvalidData(format!(
        "{} is not a supported embedding type",
        proto.as_str_name()
      ))),
    }
  }

  pub fn to_proto(&self) -> proto::collab::EmbeddingContentType {
    match self {
      EmbeddingContentType::PlainText => proto::collab::EmbeddingContentType::PlainText,
    }
  }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PublishCollabMetadata<Metadata> {
  pub view_id: uuid::Uuid,
  pub publish_name: String,
  pub metadata: Metadata,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PublishCollabKey {
  pub workspace_id: uuid::Uuid,
  pub view_id: uuid::Uuid,
}

#[derive(Debug)]
pub struct PublishCollabItem<Meta, Data> {
  pub meta: PublishCollabMetadata<Meta>,
  pub data: Data,
  pub comments_enabled: bool,
  pub duplicate_enabled: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PatchPublishedCollab {
  pub view_id: Uuid,
  pub publish_name: Option<String>,
  pub comments_enabled: Option<bool>,
  pub duplicate_enabled: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GlobalComments {
  pub comments: Vec<GlobalComment>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AFWebUser {
  pub uuid: Uuid,
  pub name: String,
  pub avatar_url: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GlobalComment {
  pub user: Option<AFWebUser>,
  pub created_at: DateTime<Utc>,
  pub last_updated_at: DateTime<Utc>,
  pub content: String,
  pub reply_comment_id: Option<Uuid>,
  pub comment_id: Uuid,
  pub is_deleted: bool,
  pub can_be_deleted: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateGlobalCommentParams {
  pub content: String,
  pub reply_comment_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeleteGlobalCommentParams {
  pub comment_id: Uuid,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Reactions {
  pub reactions: Vec<Reaction>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Reaction {
  pub reaction_type: String,
  pub react_users: Vec<AFWebUser>,
  pub comment_id: Uuid,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetReactionQueryParams {
  pub comment_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateReactionParams {
  pub reaction_type: String,
  pub comment_id: Uuid,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeleteReactionParams {
  pub reaction_type: String,
  pub comment_id: Uuid,
}

/// Indexing status of a document.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum IndexingStatus {
  /// Indexing is disabled for that document.
  Disabled,
  /// Indexing is enabled, but the document has never been indexed.
  NotIndexed,
  /// Indexing is enabled and the document has been indexed.
  Indexed,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TemplateCategories {
  pub categories: Vec<TemplateCategory>,
}

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug, Copy, Clone)]
#[repr(i32)]
pub enum TemplateCategoryType {
  UseCase = 0,
  Feature = 1,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TemplateCategory {
  pub id: Uuid,
  pub name: String,
  pub icon: String,
  pub bg_color: String,
  pub description: String,
  pub category_type: TemplateCategoryType,
  pub priority: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TemplateCategoryMinimal {
  pub id: Uuid,
  pub name: String,
  pub icon: String,
  pub bg_color: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateTemplateCategoryParams {
  pub name: String,
  pub icon: String,
  pub bg_color: String,
  pub description: String,
  pub category_type: TemplateCategoryType,
  pub priority: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetTemplateCategoriesQueryParams {
  pub name_contains: Option<String>,
  pub category_type: Option<TemplateCategoryType>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateTemplateCategoryParams {
  pub name: String,
  pub icon: String,
  pub bg_color: String,
  pub description: String,
  pub category_type: TemplateCategoryType,
  pub priority: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TemplateCreators {
  pub creators: Vec<TemplateCreator>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AccountLink {
  pub link_type: String,
  pub url: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TemplateCreator {
  pub id: Uuid,
  pub name: String,
  pub avatar_url: String,
  pub account_links: Vec<AccountLink>,
  pub number_of_templates: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TemplateCreatorMinimal {
  pub id: Uuid,
  pub name: String,
  pub avatar_url: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateTemplateCreatorParams {
  pub name: String,
  pub avatar_url: String,
  pub account_links: Vec<AccountLink>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateTemplateCreatorParams {
  pub name: String,
  pub avatar_url: String,
  pub account_links: Vec<AccountLink>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetTemplateCreatorsQueryParams {
  pub name_contains: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Template {
  pub view_id: Uuid,
  pub created_at: DateTime<Utc>,
  pub last_updated_at: DateTime<Utc>,
  pub name: String,
  pub description: String,
  pub about: String,
  pub view_url: String,
  pub categories: Vec<TemplateCategory>,
  pub creator: TemplateCreator,
  pub is_new_template: bool,
  pub is_featured: bool,
  pub related_templates: Vec<TemplateMinimal>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TemplateWithPublishInfo {
  #[serde(flatten)]
  pub template: Template,
  pub publish_info: PublishInfo,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TemplateMinimal {
  pub view_id: Uuid,
  pub created_at: DateTime<Utc>,
  pub last_updated_at: DateTime<Utc>,
  pub name: String,
  pub description: String,
  pub view_url: String,
  pub creator: TemplateCreatorMinimal,
  pub categories: Vec<TemplateCategoryMinimal>,
  pub is_new_template: bool,
  pub is_featured: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TemplateMinimalWithPublishInfo {
  #[serde(flatten)]
  pub template: TemplateMinimal,
  pub publish_info: PublishInfo,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Templates {
  pub templates: Vec<TemplateMinimalWithPublishInfo>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateTemplateParams {
  pub view_id: Uuid,
  pub name: String,
  pub description: String,
  pub about: String,
  pub view_url: String,
  pub category_ids: Vec<Uuid>,
  pub creator_id: Uuid,
  pub is_new_template: bool,
  pub is_featured: bool,
  pub related_view_ids: Vec<Uuid>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateTemplateParams {
  pub name: String,
  pub description: String,
  pub about: String,
  pub view_url: String,
  pub category_ids: Vec<Uuid>,
  pub creator_id: Uuid,
  pub is_new_template: bool,
  pub is_featured: bool,
  pub related_view_ids: Vec<Uuid>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetTemplatesQueryParams {
  pub category_id: Option<Uuid>,
  pub is_featured: Option<bool>,
  pub is_new_template: Option<bool>,
  pub name_contains: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TemplateGroup {
  pub category: TemplateCategoryMinimal,
  pub templates: Vec<TemplateMinimal>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TemplateGroupWithPublishInfo {
  pub category: TemplateCategoryMinimal,
  pub templates: Vec<TemplateMinimalWithPublishInfo>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TemplateHomePage {
  pub featured_templates: Vec<TemplateMinimalWithPublishInfo>,
  pub new_templates: Vec<TemplateMinimalWithPublishInfo>,
  pub template_groups: Vec<TemplateGroupWithPublishInfo>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TemplateHomePageQueryParams {
  pub per_count: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AvatarImageSource {
  pub file_id: String,
}

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug, Copy, Clone)]
#[repr(i32)]
pub enum AccessRequestStatus {
  Pending = 0,
  Approved = 1,
  Rejected = 2,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AccessRequestWithViewId {
  pub request_id: Uuid,
  pub workspace: AFWorkspace,
  pub requester: AccessRequesterInfo,
  pub view_id: Uuid,
  pub status: AccessRequestStatus,
  pub created_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AccessRequesterInfo {
  pub uid: i64,
  pub uuid: Uuid,
  pub email: String,
  pub name: String,
  pub avatar_url: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AccessRequestMinimal {
  pub request_id: Uuid,
  pub workspace_id: Uuid,
  pub requester_id: Uuid,
  pub view_id: Uuid,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateAccessRequestParams {
  pub workspace_id: Uuid,
  pub view_id: Uuid,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ApproveAccessRequestParams {
  pub is_approved: bool,
}

#[derive(Debug, Clone, Validate, Serialize, Deserialize)]
pub struct CreateImportTask {
  #[validate(custom(function = "validate_not_empty_str"))]
  pub workspace_name: String,
  pub content_length: u64,
}

/// Create a import task
/// Upload the import zip file to the presigned url
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateImportTaskResponse {
  pub task_id: String,
  pub presigned_url: String,
}

#[derive(Debug)]
pub struct WorkspaceNamespace {
  pub workspace_id: Uuid,
  pub namespace: String,
  pub is_original: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct QuickNote {
  pub id: Uuid,
  pub data: serde_json::Value,
  pub created_at: DateTime<Utc>,
  pub last_updated_at: DateTime<Utc>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct QuickNotes {
  pub quick_notes: Vec<QuickNote>,
  pub has_more: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateQuickNoteParams {
  pub data: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateQuickNoteParams {
  pub data: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListQuickNotesQueryParams {
  pub search_term: Option<String>,
  pub offset: Option<i32>,
  pub limit: Option<i32>,
}

#[cfg(test)]
mod test {
  use crate::dto::{CollabParams, CollabParamsV0};

  use bytes::Bytes;
  use collab_entity::CollabType;

  use uuid::Uuid;

  #[test]
  fn collab_params_serialization_from_old_format() {
    let v0 = CollabParamsV0 {
      object_id: Uuid::new_v4().to_string(),
      collab_type: CollabType::Document,
      encoded_collab_v1: vec![
        7, 0, 0, 0, 0, 0, 0, 0, 1, 209, 196, 206, 243, 15, 1, 26, 4, 0, 0, 0, 0, 0, 0, 1, 1, 209,
        196, 206, 243, 15, 0, 40, 1, 4, 100, 97, 116, 97, 5, 116, 105, 116, 108, 101, 1, 119, 128,
        8, 120, 118, 88, 114, 83, 79, 105, 70, 69, 84, 70, 97, 66, 79, 57, 53, 111, 122, 87, 110,
        54, 106, 71, 87, 66, 104, 120, 114, 79, 70, 74, 111, 109, 119, 68, 119, 114, 114, 89, 66,
        103, 79, 72, 114, 102, 51, 87, 55, 79, 76, 110, 85, 120, 69, 113, 121, 104, 121, 107, 82,
        117, 82, 113, 73, 104, 89, 72, 84, 114, 105, 122, 56, 122, 72, 90, 67, 110, 97, 120, 83,
        114, 85, 113, 71, 98, 52, 50, 110, 87, 116, 105, 55, 107, 82, 103, 72, 68, 101, 89, 118,
        82, 112, 114, 73, 122, 87, 76, 68, 120, 101, 57, 55, 87, 68, 77, 113, 107, 120, 104, 65,
        71, 103, 48, 51, 49, 110, 119, 111, 106, 70, 108, 114, 102, 83, 99, 89, 110, 73, 50, 98,
        118, 54, 68, 88, 111, 118, 74, 107, 121, 119, 103, 102, 98, 107, 51, 78, 99, 103, 51, 73,
        78, 106, 67, 85, 97, 54, 114, 50, 103, 83, 55, 70, 57, 122, 106, 115, 103, 121, 88, 68, 97,
        101, 50, 68, 84, 70, 84, 72, 87, 112, 105, 102, 71, 108, 52, 48, 114, 106, 113, 56, 119,
        110, 86, 71, 54, 65, 99, 99, 109, 85, 107, 105, 89, 86, 100, 77, 75, 69, 73, 54, 107, 122,
        48, 80, 54, 50, 99, 80, 100, 115, 78, 90, 49, 70, 90, 106, 117, 106, 98, 111, 88, 65, 105,
        83, 108, 82, 105, 90, 73, 73, 90, 102, 116, 77, 117, 79, 81, 79, 85, 53, 71, 72, 65, 49,
        119, 118, 88, 97, 98, 122, 52, 122, 77, 70, 85, 112, 100, 115, 67, 89, 107, 114, 88, 87,
        101, 65, 79, 86, 102, 77, 102, 106, 100, 117, 74, 57, 89, 82, 65, 103, 72, 100, 120, 89,
        75, 54, 103, 70, 89, 122, 75, 122, 53, 119, 78, 76, 83, 68, 90, 101, 115, 109, 116, 117,
        65, 54, 53, 48, 97, 52, 85, 51, 57, 111, 74, 90, 73, 77, 117, 105, 80, 116, 57, 70, 84,
        118, 76, 122, 111, 82, 116, 68, 51, 83, 71, 108, 78, 77, 71, 102, 68, 84, 110, 114, 80,
        106, 79, 65, 75, 114, 118, 116, 98, 57, 72, 84, 108, 101, 76, 109, 48, 110, 54, 102, 97,
        83, 89, 77, 66, 88, 69, 119, 78, 71, 89, 75, 53, 114, 80, 56, 72, 55, 122, 112, 116, 82,
        52, 117, 121, 113, 67, 53, 72, 48, 101, 83, 81, 76, 76, 87, 110, 53, 81, 106, 67, 100, 103,
        55, 85, 109, 115, 103, 55, 110, 121, 87, 121, 117, 98, 72, 121, 85, 106, 57, 66, 90, 89,
        54, 50, 122, 84, 69, 103, 57, 52, 67, 102, 50, 82, 114, 74, 84, 115, 87, 97, 87, 113, 109,
        88, 105, 113, 97, 100, 113, 57, 87, 114, 70, 57, 120, 108, 80, 122, 52, 113, 119, 53, 48,
        69, 73, 78, 90, 55, 120, 65, 67, 122, 89, 111, 84, 57, 88, 79, 112, 105, 76, 76, 51, 77,
        52, 119, 84, 98, 67, 54, 101, 105, 89, 72, 80, 99, 119, 90, 109, 56, 105, 49, 68, 57, 102,
        111, 75, 65, 68, 74, 87, 106, 69, 65, 85, 71, 104, 85, 75, 117, 66, 70, 105, 72, 100, 75,
        121, 86, 74, 81, 56, 49, 73, 82, 85, 98, 120, 122, 74, 88, 107, 116, 110, 73, 101, 75, 87,
        57, 76, 53, 120, 100, 117, 112, 99, 72, 49, 105, 122, 115, 113, 103, 109, 85, 122, 113, 50,
        70, 76, 118, 76, 121, 88, 55, 110, 84, 78, 120, 99, 78, 70, 122, 117, 66, 98, 65, 75, 112,
        50, 116, 112, 84, 73, 113, 106, 77, 106, 68, 114, 99, 76, 78, 53, 109, 117, 66, 88, 100,
        68, 76, 77, 113, 67, 101, 108, 120, 49, 117, 50, 56, 89, 118, 88, 82, 74, 55, 99, 111, 78,
        77, 77, 87, 111, 121, 50, 52, 73, 116, 120, 73, 81, 72, 53, 107, 70, 50, 111, 67, 97, 122,
        88, 103, 114, 76, 97, 105, 109, 113, 105, 89, 77, 79, 70, 74, 90, 70, 122, 117, 100, 105,
        113, 121, 55, 55, 89, 49, 85, 51, 103, 120, 98, 79, 80, 121, 66, 57, 73, 98, 114, 70, 100,
        113, 83, 101, 106, 65, 82, 121, 49, 83, 73, 122, 84, 80, 103, 121, 84, 110, 117, 74, 117,
        70, 90, 116, 104, 56, 57, 109, 82, 71, 100, 68, 106, 70, 75, 80, 83, 77, 110, 113, 120,
        102, 68, 119, 118, 109, 84, 113, 77, 79, 119, 112, 114, 107, 118, 85, 78, 65, 104, 106, 98,
        53, 81, 98, 80, 71, 122, 107, 52, 83, 67, 121, 68, 78, 108, 107, 55, 109, 121, 105, 51,
        120, 100, 110, 51, 108, 122, 65, 117, 77, 83, 78, 55, 121, 86, 76, 109, 78, 103, 88, 68,
        75, 48, 81, 101, 120, 112, 69, 78, 86, 88, 116, 78, 48, 97, 105, 55, 116, 79, 104, 118, 86,
        89, 101, 74, 112, 82, 87, 82, 115, 84, 54, 55, 97, 111, 76, 56, 109, 120, 53, 73, 55, 85,
        114, 82, 49, 90, 66, 76, 76, 99, 81, 72, 49, 105, 71, 79, 109, 110, 87, 90, 68, 111, 75,
        108, 116, 86, 81, 103, 109, 112, 120, 100, 52, 70, 79, 111, 121, 57, 111, 76, 65, 111, 83,
        67, 56, 48, 120, 53, 87, 56, 72, 83, 83, 51, 103, 114, 49, 88, 85, 53, 111, 51, 90, 86,
        121, 121, 86, 74, 116, 112, 104, 116, 120, 84, 113, 85, 71, 90, 75, 85, 86, 119, 122, 75,
        49, 122, 78, 101, 118, 82, 54, 90, 55, 72, 114, 53, 84, 72, 115, 102, 104, 48, 103, 48, 99,
        65, 101, 65, 111, 122, 89, 106, 89, 117, 49, 110, 79, 115, 78, 80, 71, 53, 107, 112, 54,
        48, 121, 55, 119, 116, 119, 76, 73, 105, 110, 102, 79, 101, 72, 82, 105, 80, 117, 80, 88,
        70, 84, 51, 53, 98, 108, 74, 84, 121, 112, 76, 76, 98, 119, 102, 80, 122, 51, 120, 53, 54,
        113, 68, 0, 0,
      ],
    };
    let data = bincode::serialize(&v0).unwrap();
    let collab_params = CollabParams::from_bytes(&data).unwrap();
    assert_eq!(collab_params.object_id, v0.object_id);
    assert_eq!(collab_params.collab_type, v0.collab_type);
    assert_eq!(collab_params.encoded_collab_v1, v0.encoded_collab_v1);
  }

  #[test]
  fn deserialization_using_protobuf() {
    let collab_params_with_embeddings = CollabParams {
      object_id: "object_id".to_string(),
      collab_type: CollabType::Document,
      encoded_collab_v1: Bytes::default(),
    };

    let protobuf_encoded = collab_params_with_embeddings.to_protobuf_bytes();
    let collab_params_decoded = CollabParams::from_protobuf_bytes(&protobuf_encoded).unwrap();
    assert_eq!(collab_params_with_embeddings, collab_params_decoded);
  }

  #[test]
  fn deserialize_collab_params_without_embeddings() {
    let collab_params = CollabParams {
      object_id: "object_id".to_string(),
      collab_type: CollabType::Document,
      encoded_collab_v1: Bytes::from(vec![1, 2, 3]),
    };

    let protobuf_encoded = collab_params.to_protobuf_bytes();
    let collab_params_decoded = CollabParams::from_protobuf_bytes(&protobuf_encoded).unwrap();
    assert_eq!(collab_params, collab_params_decoded);
  }
}
