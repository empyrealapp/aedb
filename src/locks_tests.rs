use crate::error::AedbError;
use crate::locks::{LockKey, LockManager};
use std::sync::Arc;
use std::time::Duration;

fn mgr() -> Arc<LockManager> {
    Arc::new(LockManager::new(Duration::from_millis(200)))
}

#[test]
fn exclusive_within_scope() {
    let m = mgr();
    let mut a = m.begin();
    let mut b = m.begin();
    let key = LockKey::global("program:1");
    assert!(a.lock(key.clone()).is_ok());
    // Second scope cannot take it.
    assert_eq!(b.try_lock(key.clone()).unwrap(), None);
    assert!(m.is_locked(&key));
}

#[test]
fn released_on_commit_and_drop() {
    let m = mgr();
    let key = LockKey::global("doc:7");
    {
        let mut a = m.begin();
        a.lock(key.clone()).unwrap();
        a.commit();
    }
    assert!(!m.is_locked(&key));

    {
        let mut a = m.begin();
        a.lock(key.clone()).unwrap();
        // dropped without commit/rollback
    }
    assert!(!m.is_locked(&key));
    assert_eq!(m.held_count(), 0);
}

#[test]
fn rollback_releases() {
    let m = mgr();
    let key = LockKey::global("wf:9");
    let mut a = m.begin();
    a.lock(key.clone()).unwrap();
    a.rollback();
    assert!(!m.is_locked(&key));
}

#[test]
fn reentrant_same_owner() {
    let m = mgr();
    let mut a = m.begin();
    let key = LockKey::global("k");
    let t1 = a.lock(key.clone()).unwrap();
    let t2 = a.lock(key.clone()).unwrap();
    assert_eq!(t1, t2, "re-lock returns the same fencing token");
    assert_eq!(m.held_count(), 1);
}

#[test]
fn try_lock_after_release_succeeds() {
    let m = mgr();
    let key = LockKey::global("k");
    let mut a = m.begin();
    a.lock(key.clone()).unwrap();
    let mut b = m.begin();
    assert_eq!(b.try_lock(key.clone()).unwrap(), None);
    a.commit();
    assert!(b.try_lock(key.clone()).unwrap().is_some());
}

#[test]
fn blocking_lock_times_out() {
    let m = mgr();
    let key = LockKey::global("k");
    let mut a = m.begin();
    a.lock(key.clone()).unwrap();
    let mut b = m.begin();
    let err = b
        .lock_with_timeout(key.clone(), Duration::from_millis(50))
        .unwrap_err();
    assert!(matches!(err, AedbError::Timeout));
}

#[test]
fn blocking_lock_wakes_on_release() {
    let m = mgr();
    let key = LockKey::global("k");
    let mut a = m.begin();
    a.lock(key.clone()).unwrap();

    let m2 = Arc::clone(&m);
    let key2 = key.clone();
    let handle = std::thread::spawn(move || {
        let mut b = m2.begin();
        b.lock_with_timeout(key2, Duration::from_secs(5)).unwrap();
        assert!(b.holds(&LockKey::global("k")));
    });
    // Give the waiter time to park, then release.
    std::thread::sleep(Duration::from_millis(50));
    a.commit();
    handle.join().unwrap();
}

#[test]
fn lock_all_is_atomic_on_failure() {
    let m = mgr();
    // Pre-hold the middle key with another scope.
    let blocker_key = LockKey::global("b");
    let mut blocker = m.begin();
    blocker.lock(blocker_key.clone()).unwrap();

    let mut a = m.begin();
    let err = a
        .lock_all_with_timeout(
            vec![
                LockKey::global("a"),
                LockKey::global("b"),
                LockKey::global("c"),
            ],
            Duration::from_millis(50),
        )
        .unwrap_err();
    assert!(matches!(err, AedbError::Timeout));
    // None of a/b/c should remain held by `a`.
    assert!(!a.holds(&LockKey::global("a")));
    assert!(!a.holds(&LockKey::global("c")));
    // Only the blocker's key remains in the manager.
    assert_eq!(m.held_count(), 1);
    assert!(m.is_locked(&blocker_key));
}

#[test]
fn lock_all_succeeds_and_releases_together() {
    let m = mgr();
    let mut a = m.begin();
    a.lock_all(vec![
        LockKey::global("x"),
        LockKey::global("y"),
        LockKey::global("z"),
    ])
    .unwrap();
    assert_eq!(m.held_count(), 3);
    a.commit();
    assert_eq!(m.held_count(), 0);
}

#[test]
fn lock_all_preserves_preexisting_holds_on_failure() {
    let m = mgr();
    let mut a = m.begin();
    a.lock(LockKey::global("x")).unwrap();

    // Another scope holds "y"; lock_all over {x, y} should fail but keep x.
    let mut blocker = m.begin();
    blocker.lock(LockKey::global("y")).unwrap();

    let err = a
        .lock_all_with_timeout(
            vec![LockKey::global("x"), LockKey::global("y")],
            Duration::from_millis(50),
        )
        .unwrap_err();
    assert!(matches!(err, AedbError::Timeout));
    assert!(a.holds(&LockKey::global("x")), "pre-existing hold preserved");
}

#[test]
fn unlock_releases_single_key() {
    let m = mgr();
    let mut a = m.begin();
    a.lock(LockKey::global("x")).unwrap();
    a.lock(LockKey::global("y")).unwrap();
    a.unlock(&LockKey::global("x"));
    assert!(!m.is_locked(&LockKey::global("x")));
    assert!(m.is_locked(&LockKey::global("y")));
}

#[test]
fn distinct_owners_per_begin() {
    let m = mgr();
    let a = m.begin();
    let b = m.begin();
    assert_ne!(a.owner_id(), b.owner_id());
}

#[test]
fn deterministic_ordering_no_deadlock() {
    // Two threads acquire {p, q} in opposite request order. Because lock_all
    // sorts, they acquire in the same global order and one simply waits for the
    // other — no deadlock, both eventually succeed.
    let m = mgr();
    let p = LockKey::global("p");
    let q = LockKey::global("q");

    let m1 = Arc::clone(&m);
    let (p1, q1) = (p.clone(), q.clone());
    let t1 = std::thread::spawn(move || {
        for _ in 0..50 {
            let mut tx = m1.begin();
            tx.lock_all_with_timeout(vec![p1.clone(), q1.clone()], Duration::from_secs(5))
                .unwrap();
            tx.commit();
        }
    });
    let m2 = Arc::clone(&m);
    let (p2, q2) = (p.clone(), q.clone());
    let t2 = std::thread::spawn(move || {
        for _ in 0..50 {
            let mut tx = m2.begin();
            tx.lock_all_with_timeout(vec![q2.clone(), p2.clone()], Duration::from_secs(5))
                .unwrap();
            tx.commit();
        }
    });
    t1.join().unwrap();
    t2.join().unwrap();
    assert_eq!(m.held_count(), 0);
}
