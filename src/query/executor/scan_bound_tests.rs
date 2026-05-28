use super::execute_query_with_options;
use super::tests::setup;
use crate::catalog::schema::ColumnDef;
use crate::catalog::types::{ColumnType, Row, Value};
use crate::query::error::QueryError;
use crate::query::plan::{Aggregate, Order, Query, QueryOptions};

#[test]
fn bounded_scan_is_enforced_when_full_scan_not_allowed() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    let err = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"]).from("users"),
        &QueryOptions::default(),
        1,
        10_000,
        None,
    )
    .expect_err("should reject full scan");
    assert!(matches!(err, QueryError::InvalidQuery { .. }));
}

#[test]
fn default_execute_query_rejects_unbounded_full_scan() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    let err = super::execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"]).from("users"),
    )
    .expect_err("default execute_query should reject unbounded full scan");
    assert!(matches!(err, QueryError::InvalidQuery { .. }));
}

#[test]
fn non_join_page_size_is_capped_by_max_scan_rows() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    let result = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .order_by("id", Order::Asc)
            .limit(50),
        &QueryOptions::default(),
        9,
        10,
        None,
    )
    .expect("bounded page");
    assert_eq!(result.rows.len(), 10);
    assert!(result.cursor.is_some());
    assert!(result.rows_examined <= 100);
}

#[test]
fn aggregate_limit_does_not_bypass_scan_bound() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    let err = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .aggregate(Aggregate::Count)
            .limit(1),
        &QueryOptions::default(),
        7,
        10,
        None,
    )
    .expect_err("aggregate should still honor scan bound");
    assert!(matches!(err, QueryError::ScanBoundExceeded { .. }));
}

#[test]
fn cursor_does_not_bypass_scan_bound() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    let first = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .order_by("id", Order::Asc)
            .limit(5),
        &QueryOptions::default(),
        9,
        100,
        None,
    )
    .expect("first page");
    let err = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .order_by("id", Order::Asc)
            .limit(5),
        &QueryOptions {
            cursor: first.cursor,
            ..QueryOptions::default()
        },
        9,
        10,
        None,
    )
    .expect_err("cursor path should still honor scan bound");
    assert!(matches!(err, QueryError::ScanBoundExceeded { .. }));
}

#[test]
fn join_scan_bound_uses_cardinality_aware_estimate_when_right_side_is_primary_key() {
    let (mut keyspace, mut catalog) = setup();
    catalog
        .create_table(
            "A",
            "app",
            "profiles",
            vec![
                ColumnDef {
                    name: "user_id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "country".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            vec!["user_id".into()],
        )
        .expect("profiles table");
    for i in 0..50 {
        keyspace.upsert_row(
            "A",
            "app",
            "profiles",
            vec![Value::Integer(i)],
            Row::from_values(vec![Value::Integer(i), Value::Text("US".into())]),
            1,
        );
    }
    let snapshot = keyspace.snapshot();
    let result = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.country"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .limit(10),
        &QueryOptions::default(),
        1,
        1_000,
        None,
    )
    .expect("primary-key join should stay within scan bound");
    assert_eq!(result.rows.len(), 10);
    assert!(result.rows_examined <= 50);
}
