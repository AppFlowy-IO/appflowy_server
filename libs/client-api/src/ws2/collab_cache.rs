use crate::ws2::{ConnectionError, ObjectId, WorkspaceId};
use collab::core::collab::CollabContext;
use collab_plugins::local_storage::kv::doc::CollabKVAction;
use collab_plugins::local_storage::kv::KVTransactionDB;
use collab_plugins::local_storage::rocksdb::kv_impl::KVTransactionDBRocksdbImpl;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

pub(super) struct CollabCache {
  uid: i64,
  workspace_id: WorkspaceId,
  active: DashMap<ObjectId, CollabContext>,
  store: KVTransactionDBRocksdbImpl,
}

impl CollabCache {
  pub fn new(workspace_id: WorkspaceId, store: KVTransactionDBRocksdbImpl) -> Self {
    CollabCache {
      workspace_id,
      active: DashMap::new(),
      store,
    }
  }

  pub fn get(&self, object_id: ObjectId) -> Result<&CollabContext, ConnectionError> {
    let entry = self.active.entry(object_id);
    match entry {
      Entry::Occupied(e) => Ok(e.get()),
      Entry::Vacant(e) => {
        let tx = self.store.read_txn();
        let doc = tx.load_doc(self.uid, &self.workspace_id, &object_id)?;
        todo!()
      },
    }
  }

  pub fn get_mut(&self, object_id: ObjectId) -> Result<&mut CollabContext, ConnectionError> {
    let entry = self.active.entry(object_id);
    match entry {
      Entry::Occupied(mut e) => Ok(e.get_mut()),
      Entry::Vacant(e) => {
        todo!()
      },
    }
  }

  pub fn remove(&self, object_id: &ObjectId) -> Result<(), ConnectionError> {
    self.active.remove(object_id);
    //TODO: remove from persistent store
    Ok(())
  }
}
