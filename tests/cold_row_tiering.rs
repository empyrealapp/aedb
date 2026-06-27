//! End-to-end cold-row tiering: open the engine with eviction enabled and a
//! tiny memory budget (and payload spill disabled) so committed table rows are
//! evicted whole to disk segments, then verify every read and write path stays
//! correct: point query, full/filtered/index scans, secondary-index maintenance
//! on an evicted-row update, unique-constraint enforcement against an evicted
//! row, delete, and recovery.

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
        table_row_segment_eviction_enabled: true,
        // Disable payload spill so the cold-row *segment* tier is the only
        // mechanism that relieves memory pressure.
        table_row_spill_enabled: false,
        max_memory_estimate_bytes: 16 * 1024,
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
    // Secondary index on tag and a unique index on n.
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
async fn cold_row_tiering_keeps_all_read_paths_correct() {
    let dir = tempdir().expect("temp");
    let config = tiering_config();
    let db = Arc::new(AedbInstance::open_anonymous(config.clone(), dir.path()).expect("open"));
    create_schema(&db).await;

    for i in 0..200i64 {
        upsert(&db, i, &format!("tag{}", i % 5), i)
            .await
            .expect("upsert");
    }

    // Eviction actually happened: resident memory is far below the full dataset.
    let m = db.operational_metrics().await;
    assert!(
        m.keyspace_resident_bytes < 12 * 1024,
        "rows must be evicted to the cold tier (resident={})",
        m.keyspace_resident_bytes
    );

    // Point query pages an evicted row back.
    let one = db
        .query_no_auth(
            "p",
            "app",
            Query::select(&["n"])
                .from("items")
                .where_(col("id").eq(lit(42i64)))
                .limit(1),
            QueryOptions {
                consistency: ConsistencyMode::AtLatest,
                ..QueryOptions::default()
            },
        )
        .await
        .expect("point query");
    assert_eq!(one.rows.len(), 1);
    assert_eq!(one.rows[0].values[0], Value::Integer(42));

    assert_eq!(scan_count(&db).await, 200, "full scan returns evicted rows");
    assert_eq!(
        query_tag(&db, "tag2").await,
        40,
        "index query over evicted rows"
    );

    // Update an evicted row's indexed column: old index entry must be removed.
    // Row 42 had tag = "tag2"; move it to a fresh tag.
    upsert(&db, 42, "moved", 42)
        .await
        .expect("update evicted row");
    assert_eq!(query_tag(&db, "tag2").await, 39, "old index entry removed");
    assert_eq!(query_tag(&db, "moved").await, 1, "new index entry added");
    assert_eq!(
        scan_count(&db).await,
        200,
        "no duplicate from updating evicted row"
    );

    // Unique constraint must see the evicted row: re-using n=99 (row 99) fails.
    let dup = upsert(&db, 9999, "dup", 99).await;
    assert!(
        dup.is_err(),
        "unique violation against an evicted row must be rejected"
    );
    assert_eq!(scan_count(&db).await, 200, "rejected insert did not land");

    // Delete an evicted row.
    db.commit(Mutation::Delete {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        primary_key: vec![Value::Integer(7)],
    })
    .await
    .expect("delete");
    assert_eq!(scan_count(&db).await, 199, "deleted evicted row is gone");
    assert_eq!(
        query_tag(&db, "tag2").await,
        38,
        "deleted row removed from index too"
    );

    // Build a NEW index after rows are already in the cold tier: the index
    // build must scan the cold rows too, or index lookups would miss them.
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        index_name: "by_n".into(),
        if_not_exists: true,
        columns: vec!["n".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("post-eviction index build");
    let by_n = db
        .query_no_auth(
            "p",
            "app",
            Query::select(&["id"])
                .from("items")
                .where_(col("n").eq(lit(150i64)))
                .limit(10),
            QueryOptions {
                consistency: ConsistencyMode::AtLatest,
                ..QueryOptions::default()
            },
        )
        .await
        .expect("query by new index");
    assert_eq!(by_n.rows.len(), 1, "post-eviction index covers cold rows");
    assert_eq!(by_n.rows[0].values[0], Value::Integer(150));

    // Recovery restores every live row with correct values and indexes.
    db.shutdown().await.expect("shutdown");
    drop(db);
    let db2 = AedbInstance::open_anonymous(config, dir.path()).expect("reopen");
    assert_eq!(scan_count(&db2).await, 199, "all live rows recovered");
    assert_eq!(query_tag(&db2, "moved").await, 1, "index recovered");
}
