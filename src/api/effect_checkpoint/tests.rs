use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::commit::validation::Mutation;
use crate::{AedbConfig, AedbInstance, CompleteEffect, ConsistencyMode, EffectClaim, EffectStatus};
use tempfile::tempdir;

async fn open() -> (tempfile::TempDir, AedbInstance) {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    (dir, db)
}

fn credit(key: &str) -> Vec<Mutation> {
    vec![Mutation::KvSet {
        project_id: "arcana".into(),
        scope_id: "app".into(),
        key: key.as_bytes().to_vec(),
        value: b"credited".to_vec(),
    }]
}

#[tokio::test]
async fn begin_then_complete_then_duplicate() {
    let (_dir, db) = open().await;

    // First claim is fresh.
    let c1 = db.begin_effect("arcana", "app", "dep-1").await.unwrap();
    assert_eq!(c1, EffectClaim::Fresh);

    // Re-claiming a pending key reports it is in progress with bumped attempts.
    let c2 = db.begin_effect("arcana", "app", "dep-1").await.unwrap();
    assert_eq!(c2, EffectClaim::InProgress { attempts: 2 });

    // Completing applies the internal mutation and marks committed.
    let done = db
        .complete_effect(
            "arcana",
            "app",
            "dep-1",
            r#"{"tx":"0xabc"}"#.into(),
            credit("c1"),
        )
        .await
        .unwrap();
    assert!(matches!(done, CompleteEffect::Applied(_)));
    let v = db
        .kv_get_no_auth("arcana", "app", b"c1", ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(v.map(|e| e.value), Some(b"credited".to_vec()));

    // After completion, begin reports AlreadyCommitted with the recorded result.
    let c3 = db.begin_effect("arcana", "app", "dep-1").await.unwrap();
    assert_eq!(
        c3,
        EffectClaim::AlreadyCommitted {
            result_json: r#"{"tx":"0xabc"}"#.into()
        }
    );
}

#[tokio::test]
async fn complete_is_exactly_once() {
    let (_dir, db) = open().await;

    let first = db
        .complete_effect("arcana", "app", "dep-2", "{}".into(), credit("once"))
        .await
        .unwrap();
    assert!(matches!(first, CompleteEffect::Applied(_)));

    // A replayed completion does NOT re-apply the internal mutation.
    let second = db
        .complete_effect(
            "arcana",
            "app",
            "dep-2",
            "{}".into(),
            // A different mutation that, if applied, would be observable.
            vec![Mutation::KvSet {
                project_id: "arcana".into(),
                scope_id: "app".into(),
                key: b"once".to_vec(),
                value: b"DOUBLED".to_vec(),
            }],
        )
        .await
        .unwrap();
    assert!(matches!(second, CompleteEffect::AlreadyCommitted { .. }));

    // The value is still the original credit, proving the second body was skipped.
    let v = db
        .kv_get_no_auth("arcana", "app", b"once", ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(v.map(|e| e.value), Some(b"credited".to_vec()));
}

#[tokio::test]
async fn concurrent_complete_applies_exactly_one() {
    let (_dir, db) = open().await;
    let db = Arc::new(db);
    let applied = Arc::new(AtomicUsize::new(0));
    let already = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for worker in 0..12u8 {
        let db = Arc::clone(&db);
        let applied = Arc::clone(&applied);
        let already = Arc::clone(&already);
        handles.push(tokio::spawn(async move {
            let muts = vec![Mutation::KvSet {
                project_id: "arcana".into(),
                scope_id: "app".into(),
                key: b"winner".to_vec(),
                value: vec![worker],
            }];
            match db
                .complete_effect("arcana", "app", "race", "{}".into(), muts)
                .await
                .unwrap()
            {
                CompleteEffect::Applied(_) => applied.fetch_add(1, Ordering::SeqCst),
                CompleteEffect::AlreadyCommitted { .. } => already.fetch_add(1, Ordering::SeqCst),
            };
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(applied.load(Ordering::SeqCst), 1, "exactly one applied");
    assert_eq!(already.load(Ordering::SeqCst), 11, "rest are duplicates");
    // Exactly one winner value was written.
    let v = db
        .kv_get_no_auth("arcana", "app", b"winner", ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert!(v.is_some());
}

#[tokio::test]
async fn status_reflects_lifecycle() {
    let (_dir, db) = open().await;
    assert!(
        db.effect_status("arcana", "app", "dep-3", ConsistencyMode::AtLatest)
            .await
            .unwrap()
            .is_none()
    );

    db.begin_effect("arcana", "app", "dep-3").await.unwrap();
    let pending = db
        .effect_status("arcana", "app", "dep-3", ConsistencyMode::AtLatest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.status, EffectStatus::Pending);
    assert_eq!(pending.attempts, 1);

    db.complete_effect("arcana", "app", "dep-3", r#"{"ok":true}"#.into(), vec![])
        .await
        .unwrap();
    let committed = db
        .effect_status("arcana", "app", "dep-3", ConsistencyMode::AtLatest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed.status, EffectStatus::Committed);
    assert_eq!(committed.result_json, r#"{"ok":true}"#);
}

#[tokio::test]
async fn rejects_empty_key_and_bad_json() {
    let (_dir, db) = open().await;
    assert!(db.begin_effect("arcana", "app", "").await.is_err());
    assert!(
        db.complete_effect("arcana", "app", "k", "not json".into(), vec![])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn survives_restart() {
    let dir = tempdir().expect("temp");
    {
        let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
        db.create_project("arcana").await.expect("project");
        db.create_scope("arcana", "app").await.expect("scope");
        db.complete_effect("arcana", "app", "persist", r#"{"x":1}"#.into(), vec![])
            .await
            .unwrap();
        db.shutdown().await.expect("graceful shutdown");
    }
    let db2 = AedbInstance::open(AedbConfig::default(), dir.path()).expect("reopen");
    let claim = db2.begin_effect("arcana", "app", "persist").await.unwrap();
    assert_eq!(
        claim,
        EffectClaim::AlreadyCommitted {
            result_json: r#"{"x":1}"#.into()
        }
    );
}
