use crate::engine_interface::{EffectBatch, EffectEvent};
use crate::{AedbConfig, AedbInstance, ConsistencyMode, EventOrder, EventQuery};
use tempfile::tempdir;

async fn open() -> (tempfile::TempDir, AedbInstance) {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    (dir, db)
}

async fn emit(db: &AedbInstance, topic: &str, key: &str, payload: &str) -> u64 {
    db.emit_event("arcana", "app", topic, key.into(), payload.into())
        .await
        .expect("emit")
        .commit_seq
}

fn keys(page: &crate::EventStreamPage) -> Vec<String> {
    page.events.iter().map(|e| e.event_key.clone()).collect()
}

#[tokio::test]
async fn filters_by_topic() {
    let (_dir, db) = open().await;
    emit(&db, "deposit", "d1", "{}").await;
    emit(&db, "trade", "t1", "{}").await;
    emit(&db, "deposit", "d2", "{}").await;

    let q = EventQuery::new().topic("deposit").limit(10);
    let page = db
        .query_events(&q, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(keys(&page), vec!["d1", "d2"]);
}

#[tokio::test]
async fn limit_zero_is_empty() {
    let (_dir, db) = open().await;
    emit(&db, "deposit", "d1", "{}").await;
    let q = EventQuery::new().limit(0);
    let page = db
        .query_events(&q, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert!(page.events.is_empty());
    assert_eq!(page.next_commit_seq, None);
}

#[tokio::test]
async fn ascending_cursor_pagination_covers_all() {
    let (_dir, db) = open().await;
    for i in 0..5 {
        emit(&db, "deposit", &format!("d{i}"), "{}").await;
    }

    let mut seen = Vec::new();
    let mut cursor: Option<u64> = None;
    loop {
        let mut q = EventQuery::new().topic("deposit").limit(2);
        if let Some(c) = cursor {
            q = q.after_commit_seq(c);
        }
        let page = db
            .query_events(&q, ConsistencyMode::AtLatest)
            .await
            .unwrap();
        seen.extend(keys(&page));
        match page.next_commit_seq {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    assert_eq!(seen, vec!["d0", "d1", "d2", "d3", "d4"]);
}

#[tokio::test]
async fn pagination_never_splits_a_commit() {
    let (_dir, db) = open().await;
    emit(&db, "deposit", "a", "{}").await;
    // Three events committed atomically share one commit_seq.
    db.commit_effect_batch(
        "arcana",
        "app",
        EffectBatch {
            preconditions: Vec::new(),
            effects: Vec::new(),
            events: vec![
                EffectEvent {
                    event_name: "deposit".into(),
                    event_key: "b".into(),
                    data_json: "{}".into(),
                },
                EffectEvent {
                    event_name: "deposit".into(),
                    event_key: "c".into(),
                    data_json: "{}".into(),
                },
                EffectEvent {
                    event_name: "deposit".into(),
                    event_key: "d".into(),
                    data_json: "{}".into(),
                },
            ],
        },
    )
    .await
    .expect("batch");
    emit(&db, "deposit", "e", "{}").await;

    // limit=2 lands inside the 3-event commit; the page must include the whole
    // commit rather than splitting it (which would skip events on resume).
    let q = EventQuery::new().topic("deposit").limit(2);
    let page1 = db
        .query_events(&q, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(keys(&page1), vec!["a", "b", "c", "d"]);
    let cursor = page1.next_commit_seq.expect("more pages");

    let q2 = EventQuery::new()
        .topic("deposit")
        .after_commit_seq(cursor)
        .limit(2);
    let page2 = db
        .query_events(&q2, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(keys(&page2), vec!["e"]);
    assert_eq!(page2.next_commit_seq, None);
}

#[tokio::test]
async fn latest_n_by_topic_is_newest_first() {
    let (_dir, db) = open().await;
    for i in 0..4 {
        emit(&db, "nav", &format!("n{i}"), "{}").await;
    }
    let latest = db
        .latest_events_by_topic("nav", 2, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    let keys: Vec<_> = latest.iter().map(|e| e.event_key.clone()).collect();
    assert_eq!(keys, vec!["n3", "n2"]);
}

#[tokio::test]
async fn descending_cursor_pagination_covers_all() {
    let (_dir, db) = open().await;
    for i in 0..5 {
        emit(&db, "nav", &format!("n{i}"), "{}").await;
    }
    let mut seen = Vec::new();
    let mut cursor: Option<u64> = None;
    loop {
        let mut q = EventQuery::new().topic("nav").descending().limit(2);
        if let Some(c) = cursor {
            q = q.before_commit_seq(c);
        }
        let page = db
            .query_events(&q, ConsistencyMode::AtLatest)
            .await
            .unwrap();
        seen.extend(keys(&page));
        match page.next_commit_seq {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    assert_eq!(seen, vec!["n4", "n3", "n2", "n1", "n0"]);
}

#[tokio::test]
async fn filters_by_payload_field() {
    let (_dir, db) = open().await;
    emit(&db, "deposit", "d1", r#"{"recipient":"0xabc","block":100}"#).await;
    emit(&db, "deposit", "d2", r#"{"recipient":"0xdef","block":101}"#).await;
    emit(&db, "deposit", "d3", r#"{"recipient":"0xabc","block":102}"#).await;

    let q = EventQuery::new()
        .topic("deposit")
        .where_field("recipient", "0xabc")
        .limit(10);
    let page = db
        .query_events(&q, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(keys(&page), vec!["d1", "d3"]);

    // Numeric field rendered as its canonical string.
    let q2 = EventQuery::new()
        .topic("deposit")
        .where_field("block", "101")
        .limit(10);
    let page2 = db
        .query_events(&q2, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(keys(&page2), vec!["d2"]);
}

#[tokio::test]
async fn filters_by_time_range() {
    let (_dir, db) = open().await;
    emit(&db, "deposit", "d1", "{}").await;
    emit(&db, "deposit", "d2", "{}").await;
    emit(&db, "deposit", "d3", "{}").await;

    let all = db
        .query_events(
            &EventQuery::new().topic("deposit").limit(10),
            ConsistencyMode::AtLatest,
        )
        .await
        .unwrap();
    let cutoff = all.events[1].ts_micros;

    let q = EventQuery::new()
        .topic("deposit")
        .time_range(Some(cutoff), None)
        .limit(10);
    let page = db
        .query_events(&q, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert!(!page.events.is_empty());
    assert!(page.events.iter().all(|e| e.ts_micros >= cutoff));
}

#[tokio::test]
async fn commit_seq_range_bounds() {
    let (_dir, db) = open().await;
    let mut seqs = Vec::new();
    for i in 0..5 {
        seqs.push(emit(&db, "deposit", &format!("d{i}"), "{}").await);
    }
    // (seqs[0], seqs[3]) exclusive on both ends -> d1, d2.
    let q = EventQuery::new()
        .topic("deposit")
        .after_commit_seq(seqs[0])
        .before_commit_seq(seqs[3])
        .limit(10);
    let page = db
        .query_events(&q, ConsistencyMode::AtLatest)
        .await
        .unwrap();
    assert_eq!(keys(&page), vec!["d1", "d2"]);
}

#[tokio::test]
async fn secure_mode_rejects_anonymous_query() {
    let dir = tempdir().expect("temp");
    let db =
        AedbInstance::open_production(AedbConfig::production([7u8; 32]), dir.path()).expect("open");
    let q = EventQuery::new().limit(10);
    let err = db.query_events(&q, ConsistencyMode::AtLatest).await;
    assert!(matches!(err, Err(crate::AedbError::PermissionDenied(_))));
}

#[tokio::test]
async fn order_default_is_ascending() {
    assert_eq!(EventQuery::new().order, EventOrder::Ascending);
}
