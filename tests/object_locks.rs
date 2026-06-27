//! Integration coverage for transaction-scoped object locks.
//!
//! These tests exercise the locking primitives against a real `AedbInstance`,
//! verifying the success criteria from the spec: exclusive modification of a
//! logical object, non-blocking MVCC readers, transaction-scoped release, and
//! that locks survive neither rollback nor restart.

use aedb::AedbInstance;
use aedb::config::AedbConfig;
use aedb::error::AedbError;
use aedb::locks::LockKey;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

async fn open(dir: &std::path::Path) -> Arc<AedbInstance> {
    let db = AedbInstance::open_anonymous(Default::default(), dir).expect("open");
    Arc::new(db)
}

/// Reopen with retry: a detached reactive-processor loop can transiently hold a
/// strong ref to the prior instance, so the directory lock may not be free for
/// a few milliseconds after drop.
async fn reopen(config: AedbConfig, dir: &std::path::Path) -> Arc<AedbInstance> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match AedbInstance::open_anonymous(config.clone(), dir) {
            Ok(db) => return Arc::new(db),
            Err(AedbError::Unavailable { .. }) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(err) => panic!("reopen: {err:?}"),
        }
    }
}

#[tokio::test]
async fn lock_serializes_modifications_while_reads_stay_concurrent() {
    let dir = tempdir().expect("temp");
    let db = open(dir.path()).await;
    db.create_project("p").await.expect("project");
    db.kv_set("p", "app", b"program:1/state".to_vec(), b"v0".to_vec())
        .await
        .expect("seed");

    let key = LockKey::new("p", "app", b"program:1".to_vec());

    // Execution A acquires the program lock and begins its critical section.
    let mut tx_a = db.lock_scope();
    tx_a.lock(key.clone()).expect("A acquires lock");

    // Execution B cannot acquire the same program lock concurrently.
    let mut tx_b = db.lock_scope();
    assert_eq!(
        tx_b.try_lock(key.clone()).expect("try_lock ok"),
        None,
        "second execution is locked out of the same program"
    );

    // Snapshot reads are unaffected — acquiring a consistent snapshot never
    // consults the lock table, so it succeeds while A holds the program lock.
    db.snapshot_probe(aedb::query::plan::ConsistencyMode::AtLatest)
        .await
        .expect("snapshot read is not blocked by the object lock");

    // A performs its guarded write and commits the data, then releases.
    db.kv_set("p", "app", b"program:1/state".to_vec(), b"v1".to_vec())
        .await
        .expect("guarded write");
    tx_a.commit();

    // Now B can take the lock.
    assert!(
        tx_b.try_lock(key.clone()).expect("try_lock ok").is_some(),
        "lock is available once A commits"
    );
}

#[tokio::test]
async fn lock_released_on_rollback() {
    let dir = tempdir().expect("temp");
    let db = open(dir.path()).await;
    let key = LockKey::global("workflow:7");

    let mut tx = db.lock_scope();
    tx.lock(key.clone()).expect("acquire");
    assert!(db.lock_manager().is_locked(&key));
    tx.rollback();

    assert!(
        !db.lock_manager().is_locked(&key),
        "rollback releases the lock"
    );
}

#[tokio::test]
async fn lock_released_when_scope_dropped() {
    let dir = tempdir().expect("temp");
    let db = open(dir.path()).await;
    let key = LockKey::global("doc:99");
    {
        let mut tx = db.lock_scope();
        tx.lock(key.clone()).expect("acquire");
        // dropped here without commit/rollback
    }
    assert!(
        !db.lock_manager().is_locked(&key),
        "dropping the scope releases the lock"
    );
}

#[tokio::test]
async fn blocking_acquire_times_out_under_contention() {
    let dir = tempdir().expect("temp");
    let db = open(dir.path()).await;
    let key = LockKey::global("k");

    let mut holder = db.lock_scope();
    holder.lock(key.clone()).expect("acquire");

    let mut waiter = db.lock_scope();
    let err = waiter
        .lock_with_timeout(key.clone(), Duration::from_millis(50))
        .unwrap_err();
    assert!(
        matches!(err, AedbError::Timeout),
        "blocking acquire degrades to a timeout rather than hanging"
    );
}

#[tokio::test]
async fn lock_all_acquires_multiple_objects_atomically() {
    let dir = tempdir().expect("temp");
    let db = open(dir.path()).await;

    let mut tx = db.lock_scope();
    tx.lock_all(vec![
        LockKey::global("a"),
        LockKey::global("b"),
        LockKey::global("c"),
    ])
    .expect("acquire all");
    assert_eq!(db.lock_manager().held_count(), 3);
    tx.commit();
    assert_eq!(db.lock_manager().held_count(), 0);
}

#[tokio::test]
async fn locks_do_not_survive_restart() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig::default();
    let key = LockKey::global("program:1");
    {
        let db = open(dir.path()).await;
        // Persist some durable state so the store can be reopened.
        db.create_project("p").await.expect("project");
        let mut tx = db.lock_scope();
        tx.lock(key.clone()).expect("acquire");
        assert!(db.lock_manager().is_locked(&key));
        // Forget the scope so its Drop does not run — simulate a crash that
        // leaves the lock "held" right up until the process dies.
        std::mem::forget(tx);
        assert!(db.lock_manager().is_locked(&key));
        // Flush the manifest and release the directory lock for re-open.
        db.shutdown().await.expect("shutdown");
    }

    // A fresh instance over the same directory starts with an empty lock table.
    let db2 = reopen(config, dir.path()).await;
    assert!(
        !db2.lock_manager().is_locked(&key),
        "in-memory locks are discarded on restart"
    );
    assert_eq!(db2.lock_manager().held_count(), 0);

    // The runtime can re-acquire the lock as execution resumes.
    let mut tx = db2.lock_scope();
    assert!(tx.try_lock(key).expect("try_lock ok").is_some());
}
