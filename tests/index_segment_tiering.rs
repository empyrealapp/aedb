//! End-to-end secondary-index cold tiering: open the engine with index segment
//! eviction enabled and a tiny memory budget so committed index postings are
//! evicted to disk segments, then verify every read and write path stays
//! correct over the cold index tier: index equality query, secondary-index
//! maintenance on an evicted-posting update, unique-constraint enforcement
//! against an evicted posting, delete-then-reinsert of a unique value (tombstone
//! path), and recovery.

use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::{ColumnDef, IndexType};
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::config::{AedbConfig, StorageMode};
use aedb::query::plan::{ConsistencyMode, Query, QueryOptions, col, lit};
use std::sync::Arc;
use tempfile::tempdir;

fn tiering_config() -> AedbConfig {
    AedbConfig {
        storage_mode: StorageMode::DiskBacked,
        // Evict both rows and index postings to disk; with a tiny budget the
        // index postings (which dwarf the rows once there are enough of them)
        // must move to the cold segment tier for commits to stay under budget.
        table_row_segment_eviction_enabled: true,
        index_segment_eviction_enabled: true,
        table_row_spill_enabled: false,
        max_memory_estimate_bytes: 48 * 1024,
        ..AedbConfig::default()
    }
}

async fn create_schema(db: &AedbInstance) {
    db.commit(Mutation::Ddl(DdlOperation::CreateProject {
        project_id: "p".into(),
        owner_id: None,
        if_not_exists: true,
    }))
    .await
    .expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateScope {
        project_id: "p".into(),
        scope_id: "app".into(),
        owner_id: None,
        if_not_exists: true,
    }))
    .await
    .expect("scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        owner_id: None,
        if_not_exists: true,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "tag".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "n".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        index_name: "by_tag".into(),
        if_not_exists: true,
        columns: vec!["tag".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("tag index");
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        index_name: "uniq_n".into(),
        if_not_exists: true,
        columns: vec!["n".into()],
        index_type: IndexType::UniqueHash,
        partial_filter: None,
    }))
    .await
    .expect("n unique index");
}

async fn upsert(
    db: &AedbInstance,
    id: i64,
    tag: &str,
    n: i64,
) -> Result<(), aedb::error::AedbError> {
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        primary_key: vec![Value::Integer(id)],
        row: Row {
            values: vec![
                Value::Integer(id),
                Value::Text(tag.into()),
                Value::Integer(n),
            ],
        },
    })
    .await
    .map(|_| ())
}

async fn query_tag(db: &AedbInstance, tag: &str) -> usize {
    db.query_no_auth(
        "p",
        "app",
        Query::select(&["id"])
            .from("items")
            .where_(col("tag").eq(lit(tag)))
            .limit(10_000),
        QueryOptions {
            consistency: ConsistencyMode::AtLatest,
            ..QueryOptions::default()
        },
    )
    .await
    .expect("tag query")
    .rows
    .len()
}

async fn scan_count(db: &AedbInstance) -> usize {
    db.query_no_auth(
        "p",
        "app",
        Query::select(&["id"]).from("items").limit(10_000),
        QueryOptions {
            consistency: ConsistencyMode::AtLatest,
            ..QueryOptions::default()
        },
    )
    .await
    .expect("scan")
    .rows
    .len()
}

#[tokio::test]
async fn index_segment_tiering_keeps_all_read_paths_correct() {
    let dir = tempdir().expect("temp");
    let config = tiering_config();
    let db = Arc::new(AedbInstance::open_anonymous(config.clone(), dir.path()).expect("open"));
    create_schema(&db).await;

    for i in 0..200i64 {
        upsert(&db, i, &format!("tag{}", i % 5), i)
            .await
            .expect("upsert");
    }

    // Index eviction actually happened: resident memory is far below what the
    // two indexes' 200 postings each would occupy if they stayed in RAM.
    // The full dataset (200 rows + 400 index postings) would occupy far more
    // than the budget if resident, so a bounded footprint proves both tiers
    // evicted to disk.
    let m = db.operational_metrics().await;
    assert!(
        m.keyspace_resident_bytes < 48 * 1024,
        "rows and index postings must be evicted to the cold tier (resident={})",
        m.keyspace_resident_bytes
    );

    // Segment reclamation must NOT delete live cold index/row segments.
    db.reclaim_unused_kv_segments()
        .await
        .expect("reclaim unused segments");

    // Equality query pages cold postings back from the index segment tier
    // (still intact after reclamation).
    assert_eq!(
        query_tag(&db, "tag2").await,
        40,
        "index query over evicted postings survives reclamation"
    );
    assert_eq!(scan_count(&db).await, 200);

    // Update a row's indexed column while its posting is cold: the old posting
    // must be tombstoned (suppressed) and the new one indexed.
    upsert(&db, 42, "moved", 42).await.expect("update");
    assert_eq!(query_tag(&db, "tag2").await, 39, "old posting tombstoned");
    assert_eq!(query_tag(&db, "moved").await, 1, "new posting visible");

    // Unique constraint must see the evicted posting: re-using n=99 is rejected.
    let dup = upsert(&db, 9999, "dup", 99).await;
    assert!(
        dup.is_err(),
        "unique violation against an evicted posting must be rejected"
    );
    assert_eq!(scan_count(&db).await, 200, "rejected insert did not land");

    // Delete the holder of n=7, then reinsert a different row with n=7: the
    // tombstone must let the value be reused (no false unique violation).
    db.commit(Mutation::Delete {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        primary_key: vec![Value::Integer(7)],
    })
    .await
    .expect("delete");
    assert_eq!(scan_count(&db).await, 199);
    upsert(&db, 5007, "reused", 7)
        .await
        .expect("reusing a freed unique value must succeed across the cold tier");
    assert_eq!(scan_count(&db).await, 200);

    // Recovery restores every live row and its indexes from checkpoint + WAL.
    db.shutdown().await.expect("shutdown");
    drop(db);
    let db2 = AedbInstance::open_anonymous(config, dir.path()).expect("reopen");
    assert_eq!(scan_count(&db2).await, 200, "all live rows recovered");
    assert_eq!(query_tag(&db2, "moved").await, 1, "index recovered");
    assert_eq!(
        query_tag(&db2, "reused").await,
        1,
        "reused unique value recovered"
    );
    // The previously-evicted unique value is still enforced after recovery.
    assert!(
        upsert(&db2, 12345, "dup2", 42).await.is_err(),
        "unique enforcement holds after recovery"
    );
}
