use super::SnapshotManager;
use crate::catalog::Catalog;
use crate::error::AedbError;
use crate::snapshot::reader::SnapshotReadView;
use crate::storage::keyspace::Keyspace;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn view(seq: u64) -> SnapshotReadView {
    SnapshotReadView {
        keyspace: Arc::new(Keyspace::default().snapshot()),
        catalog: Arc::new(Catalog::default()),
        seq,
    }
}

#[test]
fn gc_reclaims_only_released_snapshots() {
    let mut mgr = SnapshotManager::default();
    let h1 = mgr.acquire(view(1));
    let h2 = mgr.acquire(view(2));
    let h3 = mgr.acquire(view(3));
    mgr.release(h1);
    mgr.release(h2);
    let res = mgr.gc();
    assert_eq!(res.reclaimed_snapshots, 2);
    assert!(mgr.get(h3).is_some());
    assert!(mgr.get(h1).is_none());
}

#[test]
fn leak_detection_warns_for_old_snapshot() {
    let mut mgr = SnapshotManager::default();
    let _ = mgr.acquire(view(1));
    thread::sleep(Duration::from_millis(5));
    let warnings = mgr.check_leaks(1);
    assert!(!warnings.is_empty());
}

#[test]
fn wal_segment_reclamation_rules() {
    let mut mgr = SnapshotManager::default();
    let h = mgr.acquire(view(4));
    let segments = vec![1, 2, 3, 4, 5];

    let none = mgr.eligible_segment_reclaims(&segments, 4, 5);
    assert!(none.is_empty());

    mgr.release(h);
    let _ = mgr.gc();
    let eligible = mgr.eligible_segment_reclaims(&segments, 4, 5);
    assert_eq!(eligible, vec![1, 2, 3, 4]);
}

#[test]
fn bounded_acquire_and_expiry_guardrails() {
    let mut mgr = SnapshotManager::default();
    let _h1 = mgr.acquire_bounded(view(1), 1).expect("first");
    let err = mgr.acquire_bounded(view(2), 1).expect_err("bounded");
    assert!(matches!(err, AedbError::QueueFull));

    let mut mgr = SnapshotManager::default();
    let h = mgr.acquire(view(1));
    thread::sleep(Duration::from_millis(3));
    let expired = mgr.get_checked(h, 1).expect_err("expired");
    assert!(matches!(expired, AedbError::SnapshotExpired));
}
