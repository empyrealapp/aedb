//! Regression tests for correctness bugs found during the security/correctness
//! audit. Each test fails against the pre-fix code and passes after the fix.
#![allow(deprecated)]

use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::{ColumnDef, IndexType};
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::query::plan::{Aggregate, Expr, Query, QueryOptions};
use tempfile::tempdir;

async fn open_db(project: &str) -> AedbInstance {
    let dir = tempdir().expect("temp");
    // Leak the tempdir so the data directory outlives the instance for the test.
    let path = Box::leak(Box::new(dir)).path().to_path_buf();
    let db = AedbInstance::open(Default::default(), &path).expect("open");
    db.create_project(project).await.expect("project");
    db
}

/// A range/`LIKE`/`BETWEEN` predicate must never select a Hash index (which
/// cannot serve ordered scans). Before the fix the planner picked the Hash
/// index, its range scan returned no rows, and — because the predicate was
/// marked exact — no residual full-scan filter recovered them, so the query
/// silently returned zero rows.
#[tokio::test]
async fn hash_index_range_predicate_returns_all_matching_rows() {
    let db = open_db("hx").await;

    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "hx".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "score".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");

    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "hx".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        index_name: "by_score".into(),
        if_not_exists: false,
        columns: vec!["score".into()],
        index_type: IndexType::Hash,
        partial_filter: None,
    }))
    .await
    .expect("hash index");

    for i in 0..20_i64 {
        db.commit(Mutation::Upsert {
            project_id: "hx".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row::from_values(vec![Value::Integer(i), Value::Integer(i)]),
        })
        .await
        .expect("seed");
    }

    let gt = db
        .query(
            "hx",
            "app",
            Query::select(&["id", "score"])
                .from("items")
                .where_(Expr::Gt("score".into(), Value::Integer(10))),
        )
        .await
        .expect("range query on hash index");
    // scores 11..=19 -> 9 rows. Pre-fix this returned 0.
    assert_eq!(
        gt.rows.len(),
        9,
        "hash-indexed range predicate dropped rows"
    );
    for row in &gt.rows {
        match row.values[1] {
            Value::Integer(s) => assert!(s > 10, "row {s} violates predicate"),
            ref other => panic!("unexpected score {other:?}"),
        }
    }

    // Equality on the same Hash index must still work (point lookup is valid).
    let eq = db
        .query(
            "hx",
            "app",
            Query::select(&["id"])
                .from("items")
                .where_(Expr::Eq("score".into(), Value::Integer(7))),
        )
        .await
        .expect("eq query on hash index");
    assert_eq!(eq.rows.len(), 1);
}

/// SQL three-valued logic: `NULL = NULL` is never true, so NULL join keys must
/// not match. Before the fix the non-PK hash-join inserted NULL keys into the
/// probe map, so NULL-keyed left and right rows joined into spurious rows.
#[tokio::test]
async fn equi_join_null_keys_do_not_match() {
    let db = open_db("jn").await;

    for table in ["left_t", "right_t"] {
        db.commit(Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "jn".into(),
            scope_id: "app".into(),
            table_name: table.into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                // Non-PK, nullable join key -> forces the hash-join path.
                ColumnDef {
                    name: "k".into(),
                    col_type: ColumnType::Integer,
                    nullable: true,
                },
            ],
            primary_key: vec!["id".into()],
        }))
        .await
        .expect("table");
    }

    // left: (1, NULL), (2, 5)   right: (10, NULL), (11, 5)
    let seed = |table: &str, id: i64, k: Value| Mutation::Upsert {
        project_id: "jn".into(),
        scope_id: "app".into(),
        table_name: table.into(),
        primary_key: vec![Value::Integer(id)],
        row: Row::from_values(vec![Value::Integer(id), k]),
    };
    db.commit(seed("left_t", 1, Value::Null)).await.expect("l1");
    db.commit(seed("left_t", 2, Value::Integer(5)))
        .await
        .expect("l2");
    db.commit(seed("right_t", 10, Value::Null))
        .await
        .expect("r1");
    db.commit(seed("right_t", 11, Value::Integer(5)))
        .await
        .expect("r2");

    let joined = db
        .query_with_options(
            "jn",
            "app",
            Query::select(&["x.id", "y.id"])
                .from("left_t")
                .alias("x")
                .inner_join("right_t", "x.k", "k")
                .with_last_join_alias("y"),
            QueryOptions {
                allow_full_scan: true,
                ..QueryOptions::default()
            },
        )
        .await
        .expect("join query");

    // Only (2,5) x (11,5) should match. Pre-fix the NULL rows also matched -> 2 rows.
    assert_eq!(joined.rows.len(), 1, "NULL join keys must not match");
    assert_eq!(joined.rows[0].values[0], Value::Integer(2));
    assert_eq!(joined.rows[0].values[1], Value::Integer(11));
}

/// SUM/AVG must account for all numeric types (not just Integer/U8), and MIN
/// must ignore NULLs rather than treating NULL as the smallest value.
#[tokio::test]
async fn aggregates_handle_floats_and_ignore_nulls() {
    let db = open_db("ag").await;

    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "ag".into(),
        scope_id: "app".into(),
        table_name: "t".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "amount".into(),
                col_type: ColumnType::Float,
                nullable: false,
            },
            ColumnDef {
                name: "opt".into(),
                col_type: ColumnType::Integer,
                nullable: true,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");

    let rows = [
        (1_i64, 1.5_f64, Value::Integer(10)),
        (2, 2.5, Value::Null),
        (3, 3.0, Value::Integer(5)),
    ];
    for (id, amount, opt) in rows {
        db.commit(Mutation::Upsert {
            project_id: "ag".into(),
            scope_id: "app".into(),
            table_name: "t".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![Value::Integer(id), Value::Float(amount), opt]),
        })
        .await
        .expect("seed");
    }

    let agg = db
        .query(
            "ag",
            "app",
            Query::select(&["*"])
                .from("t")
                .aggregate(Aggregate::Sum("amount".into()))
                .aggregate(Aggregate::Avg("amount".into()))
                .aggregate(Aggregate::Min("opt".into()))
                .aggregate(Aggregate::Max("opt".into())),
        )
        .await
        .expect("aggregate query");

    assert_eq!(agg.rows.len(), 1);
    let vals = &agg.rows[0].values;

    // SUM over a float column was silently 0 pre-fix.
    match vals[0] {
        Value::Float(v) => assert!((v - 7.0).abs() < 1e-9, "SUM(amount) = {v}, expected 7.0"),
        ref other => panic!("SUM should be Float, got {other:?}"),
    }
    // AVG skipped floats entirely pre-fix -> Null.
    match vals[1] {
        Value::Float(v) => assert!((v - 7.0 / 3.0).abs() < 1e-9, "AVG(amount) = {v}"),
        ref other => panic!("AVG should be Float, got {other:?}"),
    }
    // MIN ignored NULLs? Pre-fix NULL (smallest) won -> Null.
    assert_eq!(vals[2], Value::Integer(5), "MIN(opt) must ignore NULL");
    assert_eq!(vals[3], Value::Integer(10), "MAX(opt)");
}
