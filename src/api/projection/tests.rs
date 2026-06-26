use std::sync::Arc;

use crate::{AedbConfig, AedbInstance, ConsistencyMode};
use tempfile::tempdir;

async fn open() -> (tempfile::TempDir, AedbInstance) {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    (dir, db)
}

#[tokio::test]
async fn latest_per_key_put_get_overwrite() {
    let (_dir, db) = open().await;
    db.projection_put("arcana", "app", "status", "protocol", r#"{"paused":false}"#)
        .await
        .unwrap();
    let v = db
        .projection_get(
            "arcana",
            "app",
            "status",
            "protocol",
            ConsistencyMode::AtLatest,
        )
        .await
        .unwrap();
    assert_eq!(v.as_deref(), Some(r#"{"paused":false}"#));

    // Overwrite -> latest only.
    db.projection_put("arcana", "app", "status", "protocol", r#"{"paused":true}"#)
        .await
        .unwrap();
    let v = db
        .projection_get(
            "arcana",
            "app",
            "status",
            "protocol",
            ConsistencyMode::AtLatest,
        )
        .await
        .unwrap();
    assert_eq!(v.as_deref(), Some(r#"{"paused":true}"#));
}

#[tokio::test]
async fn list_and_remove() {
    let (_dir, db) = open().await;
    for acct in ["alice", "bob", "carol"] {
        db.projection_put(
            "arcana",
            "app",
            "positions",
            acct,
            &format!(r#"{{"acct":"{acct}"}}"#),
        )
        .await
        .unwrap();
    }
    let entries = db
        .projection_list("arcana", "app", "positions", 100, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    let keys: Vec<_> = entries.iter().map(|e| e.entity_key.clone()).collect();
    assert_eq!(keys, vec!["alice", "bob", "carol"]);

    db.projection_remove("arcana", "app", "positions", "bob")
        .await
        .unwrap();
    let entries = db
        .projection_list("arcana", "app", "positions", 100, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    let keys: Vec<_> = entries.iter().map(|e| e.entity_key.clone()).collect();
    assert_eq!(keys, vec!["alice", "carol"]);
}

#[tokio::test]
async fn list_views_are_isolated() {
    let (_dir, db) = open().await;
    db.projection_put("arcana", "app", "viewA", "k", "1")
        .await
        .unwrap();
    db.projection_put("arcana", "app", "viewB", "k", "2")
        .await
        .unwrap();
    let a = db
        .projection_list("arcana", "app", "viewA", 100, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].value_json, "1");
}

#[tokio::test]
async fn series_keeps_last_n() {
    let (_dir, db) = open().await;
    for i in 0..10 {
        db.series_append("arcana", "app", "nav", &format!(r#"{{"nav":{i}}}"#), 3)
            .await
            .unwrap();
    }
    // Oldest first.
    let oldest_first = db
        .series_read(
            "arcana",
            "app",
            "nav",
            100,
            false,
            ConsistencyMode::AtLatest,
        )
        .await
        .unwrap();
    assert_eq!(
        oldest_first,
        vec![r#"{"nav":7}"#, r#"{"nav":8}"#, r#"{"nav":9}"#]
    );

    // Newest first, limited.
    let newest = db
        .series_read("arcana", "app", "nav", 2, true, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(newest, vec![r#"{"nav":9}"#, r#"{"nav":8}"#]);
}

#[tokio::test]
async fn series_empty_read() {
    let (_dir, db) = open().await;
    let pts = db
        .series_read(
            "arcana",
            "app",
            "missing",
            10,
            false,
            ConsistencyMode::AtLatest,
        )
        .await
        .unwrap();
    assert!(pts.is_empty());
}

#[tokio::test]
async fn concurrent_series_append_loses_no_points() {
    let (_dir, db) = open().await;
    let db = Arc::new(db);
    let mut handles = Vec::new();
    for i in 0..20 {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            db.series_append("arcana", "app", "log", &format!(r#"{{"i":{i}}}"#), 100)
                .await
                .unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let pts = db
        .series_read(
            "arcana",
            "app",
            "log",
            1000,
            false,
            ConsistencyMode::AtLatest,
        )
        .await
        .unwrap();
    assert_eq!(pts.len(), 20, "all concurrent appends retained");
}

#[tokio::test]
async fn rejects_bad_input() {
    let (_dir, db) = open().await;
    assert!(
        db.projection_put("arcana", "app", "", "k", "1")
            .await
            .is_err()
    );
    assert!(
        db.projection_put("arcana", "app", "v", "k", "not json")
            .await
            .is_err()
    );
    assert!(
        db.series_append("arcana", "app", "s", "{}", 0)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn series_survives_restart() {
    let dir = tempdir().expect("temp");
    {
        let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
        db.create_project("arcana").await.expect("project");
        db.create_scope("arcana", "app").await.expect("scope");
        db.series_append("arcana", "app", "nav", r#"{"nav":1}"#, 5)
            .await
            .unwrap();
        db.shutdown().await.expect("shutdown");
    }
    let db2 = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("reopen");
    let pts = db2
        .series_read("arcana", "app", "nav", 10, false, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(pts, vec![r#"{"nav":1}"#]);
}
