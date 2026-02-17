use crate::catalog::Catalog;
use crate::storage::keyspace::KeyspaceSnapshot;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct SnapshotReadView {
    pub keyspace: Arc<KeyspaceSnapshot>,
    pub catalog: Arc<Catalog>,
    pub seq: u64,
}
