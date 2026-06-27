use crate::commit::validation::Mutation;
use crate::{AedbConfig, AedbInstance, ConsistencyMode, FencedCommit, LeaseOutcome, RenewOutcome};
use tempfile::tempdir;

const TTL: u64 = 60_000_000; // 60s — comfortably live for a test.

async fn open() -> (tempfile::TempDir, AedbInstance) {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    (dir, db)
}

fn held(outcome: LeaseOutcome) -> crate::MonitorLease {
    match outcome {
        LeaseOutcome::Acquired(l) => l,
        LeaseOutcome::Held { .. } => panic!("expected Acquired, got Held"),
    }
}

#[tokio::test]
async fn acquire_is_exclusive_while_live() {
    let (_dir, db) = open().await;
    let l1 = held(
        db.lease_acquire("arcana", "app", "keeper:cleanup", "w1", TTL)
            .await
            .unwrap(),
    );
    assert_eq!(l1.fencing_token, 1);

    match db
        .lease_acquire("arcana", "app", "keeper:cleanup", "w2", TTL)
        .await
        .unwrap()
    {
        LeaseOutcome::Held { owner_id, .. } => assert_eq!(owner_id, "w1"),
        LeaseOutcome::Acquired(_) => panic!("w2 should not acquire a live lease"),
    }
}

#[tokio::test]
async fn reacquire_by_same_owner_is_allowed() {
    let (_dir, db) = open().await;
    let l1 = held(
        db.lease_acquire("arcana", "app", "k", "w1", TTL)
            .await
            .unwrap(),
    );
    let l2 = held(
        db.lease_acquire("arcana", "app", "k", "w1", TTL)
            .await
            .unwrap(),
    );
    assert!(l2.fencing_token > l1.fencing_token);
}

#[tokio::test]
async fn expired_lease_taken_over_and_old_owner_fenced() {
    let (_dir, db) = open().await;
    let l1 = held(
        db.lease_acquire("arcana", "app", "k", "w1", 0)
            .await
            .unwrap(),
    );
    let l2 = held(
        db.lease_acquire("arcana", "app", "k", "w2", TTL)
            .await
            .unwrap(),
    );
    assert_eq!(l2.fencing_token, 2);

    let fenced = db
        .lease_guarded_commit("arcana", "app", "k", l1.fencing_token, vec![kv("x", "1")])
        .await
        .unwrap();
    assert!(matches!(fenced, FencedCommit::LeaseLost));
}

#[tokio::test]
async fn renew_extends_and_stale_token_is_lost() {
    let (_dir, db) = open().await;
    let l1 = held(
        db.lease_acquire("arcana", "app", "k", "w1", TTL)
            .await
            .unwrap(),
    );
    match db
        .lease_renew("arcana", "app", "k", l1.fencing_token, TTL)
        .await
        .unwrap()
    {
        RenewOutcome::Renewed(_) => {}
        RenewOutcome::LeaseLost => panic!("owner should renew its own lease"),
    }
    // A different token (e.g. an old holder) cannot renew.
    assert!(matches!(
        db.lease_renew("arcana", "app", "k", 999, TTL)
            .await
            .unwrap(),
        RenewOutcome::LeaseLost
    ));
}

#[tokio::test]
async fn guarded_commit_applies_while_held() {
    let (_dir, db) = open().await;
    let l = held(
        db.lease_acquire("arcana", "app", "k", "w1", TTL)
            .await
            .unwrap(),
    );
    let out = db
        .lease_guarded_commit(
            "arcana",
            "app",
            "k",
            l.fencing_token,
            vec![kv("counter", "42")],
        )
        .await
        .unwrap();
    assert!(matches!(out, FencedCommit::Applied(_)));

    let entry = db
        .kv_get_no_auth("arcana", "app", b"counter", ConsistencyMode::AtLatest)
        .await
        .unwrap()
        .expect("value present");
    assert_eq!(entry.value, b"42");
}

#[tokio::test]
async fn release_is_idempotent_and_frees_the_lease() {
    let (_dir, db) = open().await;
    let l = held(
        db.lease_acquire("arcana", "app", "k", "w1", TTL)
            .await
            .unwrap(),
    );
    db.lease_release("arcana", "app", "k", l.fencing_token)
        .await
        .unwrap();
    // Idempotent second release is a no-op.
    db.lease_release("arcana", "app", "k", l.fencing_token)
        .await
        .unwrap();
    // Now another owner can take it.
    let l2 = held(
        db.lease_acquire("arcana", "app", "k", "w2", TTL)
            .await
            .unwrap(),
    );
    assert_eq!(l2.owner_id, "w2");
}

#[tokio::test]
async fn status_reflects_owner_and_meta() {
    let (_dir, db) = open().await;
    assert!(
        db.lease_status("arcana", "app", "k", ConsistencyMode::AtLatest)
            .await
            .unwrap()
            .is_none()
    );
    db.lease_acquire("arcana", "app", "k", "w1", TTL)
        .await
        .unwrap();
    let rec = db
        .lease_status("arcana", "app", "k", ConsistencyMode::AtLatest)
        .await
        .unwrap()
        .expect("present");
    assert_eq!(rec.owner_id.as_deref(), Some("w1"));
    assert_eq!(rec.fencing_token, 1);
}

fn kv(key: &str, value: &str) -> Mutation {
    Mutation::KvSet {
        project_id: "arcana".into(),
        scope_id: "app".into(),
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    }
}
