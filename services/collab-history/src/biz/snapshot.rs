use crate::biz::persistence::HistoryPersistence;
use crate::error::HistoryError;
use collab::core::collab::{MutexCollab, WeakMutexCollab};
use collab::preclude::{ReadTxn, Snapshot, StateVector};
use collab_entity::CollabType;
use std::ops::Deref;
use std::sync::atomic::{AtomicI64, AtomicU32};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{error, warn};

pub struct SnapshotGenerator {
  collab: WeakMutexCollab,
  collab_type: CollabType,
  apply_update_count: AtomicU32,
  pending_snapshots: Arc<RwLock<Vec<CollabSnapshot>>>,
}

impl SnapshotGenerator {
  pub fn new(collab: WeakMutexCollab, collab_type: CollabType) -> Self {
    Self {
      collab,
      collab_type,
      apply_update_count: Default::default(),
      pending_snapshots: Default::default(),
    }
  }

  pub async fn take_pending_snapshots(&self) -> Vec<CollabSnapshot> {
    let mut pending_snapshots = self.pending_snapshots.write().await;
    std::mem::take(&mut *pending_snapshots)
  }

  pub fn did_apply_update(&self, _update: &[u8]) {
    let prev_apply_update_count = self
      .apply_update_count
      .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    // keep it simple for now. we just compare the update count to determine if we need to generate a snapshot.
    // in the future, we can use a more sophisticated algorithm to determine when to generate a snapshot.
    if prev_apply_update_count + 1 >= gen_snapshot_threshold(&self.collab_type) {
      let pending_snapshots = self.pending_snapshots.clone();
      let weak_collab = self.collab.clone();
      tokio::spawn(async move {
        if let Some(collab) = weak_collab.upgrade() {
          attempt_gen_snapshot(collab, pending_snapshots, 3, Duration::from_secs(2)).await;
        } else {
          warn!("collab is dropped. cannot generate snapshot")
        }
      });
      self
        .apply_update_count
        .store(0, std::sync::atomic::Ordering::SeqCst);
    }
  }
}

#[inline]
fn gen_snapshot_threshold(collab_type: &CollabType) -> u32 {
  match collab_type {
    CollabType::Document => 100,
    CollabType::Database => 20,
    CollabType::WorkspaceDatabase => 20,
    CollabType::Folder => 20,
    CollabType::DatabaseRow => 10,
    CollabType::UserAwareness => 50,
    CollabType::Empty => {
      if cfg!(debug_assertions) {
        1
      } else {
        10
      }
    },
  }
}

// Assume gen_snapshot and other relevant functions and types are defined elsewhere.
// Helper function to perform the snapshot generation with retries.
async fn attempt_gen_snapshot(
  collab: MutexCollab,
  pending_snapshots: Arc<RwLock<Vec<CollabSnapshot>>>,
  max_retries: usize,
  delay: Duration,
) {
  let mut retries = 0;
  while retries < max_retries {
    match gen_snapshot(&collab, 1) {
      Ok(snapshot) => {
        pending_snapshots.write().await.push(snapshot);
        return;
      },
      Err(err) => {
        error!(
          "Failed to generate snapshot on attempt {}: {:?}",
          retries + 1,
          err
        );
        retries += 1;
        sleep(delay * retries as u32).await; // Exponential backoff
      },
    }
  }
  warn!("Exceeded maximum retry attempts for snapshot generation");
}

pub fn gen_snapshot(mutex_collab: &MutexCollab, uid: i64) -> Result<CollabSnapshot, HistoryError> {
  let lock_guard = mutex_collab.lock();
  let txn = lock_guard.try_transaction()?;
  let snapshot = txn.snapshot();
  drop(txn);

  let timestamp = chrono::Utc::now().timestamp();
  Ok(CollabSnapshot::new(uid, snapshot, timestamp))
}

/// Represents the state of a collaborative object (Collab) at a specific timestamp.
/// This is used to revert a Collab to a past state using the closest preceding
/// `CollabStateSnapshot`. When reverting to a specific `CollabSnapshot`,
///
/// locating the nearest `CollabStateSnapshot` whose `created_at` timestamp
/// is less than or equal to the `CollabSnapshot`'s `created_at`.
/// This `CollabStateSnapshot` is then used to restore the Collab's state to the snapshot's timestamp.
pub struct CollabStateSnapshot {
  pub snapshot_id: String,
  /// Unique identifier of the collaborative document.
  pub object_id: String,
  /// Binary representation of the Collab's state.
  pub doc_state_v2: Vec<u8>,
  pub state_vector: StateVector,
  /// Timestamp indicating when this snapshot was created, measured in milliseconds since the Unix epoch.
  pub created_at: i64,
  /// This field specifies the ID of another snapshot that the current snapshot depends on. If present,
  /// it indicates that the current document's state is built upon or derived from the state of the
  /// specified dependency snapshot.
  pub dependency_snapshot_id: Option<String>,
}

impl CollabStateSnapshot {
  pub fn new(
    object_id: String,
    doc_state_v2: Vec<u8>,
    state_vector: StateVector,
    created_at: i64,
  ) -> Self {
    let snapshot_id = uuid::Uuid::new_v4().to_string();
    Self {
      snapshot_id,
      object_id,
      doc_state_v2,
      state_vector,
      created_at,
      dependency_snapshot_id: None,
    }
  }
}

/// Captures a significant version of a collaborative object (Collab), marking a specific point in time.
/// This snapshot is identified by a unique ID and linked to a specific `CollabStateSnapshot`.
/// It represents a milestone or version of the Collab that can be referenced or reverted to.
pub struct CollabSnapshot {
  /// Unique identifier for the snapshot, typically representing a version or revision number.
  pub uid: i64,
  /// Snapshot data capturing the Collab's state at the time of the snapshot.
  pub snapshot: Snapshot,
  /// Timestamp indicating when this snapshot was created, measured in milliseconds since the Unix epoch.
  pub created_at: i64,
}
impl Deref for CollabSnapshot {
  type Target = Snapshot;

  fn deref(&self) -> &Self::Target {
    &self.snapshot
  }
}

impl CollabSnapshot {
  pub fn new(uid: i64, snapshot: Snapshot, created_at: i64) -> Self {
    Self {
      uid,
      snapshot,
      created_at,
    }
  }
}
