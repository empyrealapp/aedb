use super::{CommitDelta, VersionStore};
use crate::commit::validation::Mutation;
use crate::storage::keyspace::Keyspace;
use std::sync::Arc;

#[test]
fn publish_delta_reuses_shared_arc_instance() {
    let mut store = VersionStore::new(8, 4, 0);
    store.bootstrap(
        0,
        Keyspace::default().snapshot(),
        crate::catalog::Catalog::default(),
    );
    let delta = Arc::new(CommitDelta {
        seq: 7,
        mutations: vec![Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        }],
        row_changes: Vec::new(),
    });

    assert!(!store.publish_delta(7, Arc::clone(&delta)));
    let deltas = store.deltas_since(0, 7).expect("delta range");

    assert_eq!(deltas.len(), 1);
    assert!(Arc::ptr_eq(&delta, &deltas[0]));
}

#[test]
fn publish_delta_requests_full_snapshot_after_interval() {
    let mut store = VersionStore::new(8, 2, 0);
    store.bootstrap(
        0,
        Keyspace::default().snapshot(),
        crate::catalog::Catalog::default(),
    );

    let first = Arc::new(CommitDelta {
        seq: 1,
        mutations: Vec::new(),
        row_changes: Vec::new(),
    });
    let second = Arc::new(CommitDelta {
        seq: 2,
        mutations: Vec::new(),
        row_changes: Vec::new(),
    });

    assert!(!store.publish_delta(1, first));
    assert!(store.publish_delta(2, second));

    store.publish_full(
        2,
        Keyspace::default().snapshot(),
        crate::catalog::Catalog::default(),
    );

    let third = Arc::new(CommitDelta {
        seq: 3,
        mutations: Vec::new(),
        row_changes: Vec::new(),
    });
    assert!(!store.publish_delta(3, third));
}

#[test]
fn publish_deltas_batches_prune_and_snapshot_interval_accounting() {
    let mut store = VersionStore::new(8, 3, 0);
    store.bootstrap(
        0,
        Keyspace::default().snapshot(),
        crate::catalog::Catalog::default(),
    );
    let first = Arc::new(CommitDelta {
        seq: 1,
        mutations: Vec::new(),
        row_changes: Vec::new(),
    });
    let second = Arc::new(CommitDelta {
        seq: 2,
        mutations: Vec::new(),
        row_changes: Vec::new(),
    });
    assert!(!store.publish_deltas([(1, first), (2, second)]));

    let third = Arc::new(CommitDelta {
        seq: 3,
        mutations: Vec::new(),
        row_changes: Vec::new(),
    });
    assert!(store.publish_deltas([(3, third)]));
    assert_eq!(store.deltas_since(0, 3).expect("delta range").len(), 3);
}
