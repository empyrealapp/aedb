use super::execute_query_with_options;
use super::join::enforce_materialization_budget;
use super::tests::{execute_query, setup};
use crate::catalog::schema::ColumnDef;
use crate::catalog::types::{ColumnType, Row, Value};
use crate::query::error::QueryError;
use crate::query::plan::{Aggregate, Expr, Order, Query, QueryOptions};

#[test]
fn materialization_budget_rejects_oversized_row_set() {
    let rows: Vec<Row> = (0..1000)
        .map(|i| Row {
            values: vec![Value::Integer(i), Value::Text("x".repeat(256).into())],
        })
        .collect();

    // A generous budget admits the set.
    assert!(enforce_materialization_budget(&rows, u64::MAX).is_ok());

    // A tiny budget rejects it with a structured, machine-readable error.
    let err = enforce_materialization_budget(&rows, 64).unwrap_err();
    match err {
        QueryError::MaterializationBudgetExceeded {
            estimated_bytes,
            max_bytes,
        } => {
            assert_eq!(max_bytes, 64);
            assert!(estimated_bytes > 64);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn materialization_budget_admits_empty_and_small_sets() {
    assert!(enforce_materialization_budget(&[], 0).is_ok());
    let small = vec![Row {
        values: vec![Value::Integer(1)],
    }];
    assert!(enforce_materialization_budget(&small, 1024).is_ok());
}

fn add_profiles_table(catalog: &mut crate::catalog::Catalog, primary_key: Vec<String>) {
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
            primary_key,
        )
        .expect("profiles table");
}

fn seed_profiles(
    keyspace: &mut crate::storage::keyspace::Keyspace,
    ids: impl Iterator<Item = i64>,
    country_for_id: impl Fn(i64) -> &'static str,
) {
    for id in ids {
        keyspace.upsert_row(
            "A",
            "app",
            "profiles",
            vec![Value::Integer(id)],
            Row::from_values(vec![
                Value::Integer(id),
                Value::Text(country_for_id(id).into()),
            ]),
            1,
        );
    }
}

#[test]
fn inner_join_returns_matching_rows() {
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 0..50, |_| "US");

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.country"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .limit(100),
    )
    .expect("join query");
    assert_eq!(result.rows.len(), 50);
}

#[test]
fn join_on_right_primary_key_uses_bounded_probe_path() {
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 0..50, |_| "US");

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
            .limit(100),
        &QueryOptions::default(),
        1,
        100,
        None,
    )
    .expect("pk join should respect bounded probe path");
    assert_eq!(result.rows.len(), 50);
    assert!(result.rows_examined <= 50);
}

#[test]
fn join_aggregate_count_and_having_are_applied() {
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(
        &mut keyspace,
        0..50,
        |id| {
            if id % 2 == 0 { "US" } else { "CA" }
        },
    );

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["p.country", "count_star"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .group_by(&["p.country"])
            .aggregate(Aggregate::Count)
            .having(Expr::Gt("count_star".into(), Value::Integer(20)))
            .order_by("count_star", Order::Desc)
            .limit(10),
    )
    .expect("join aggregate query");

    assert_eq!(result.rows.len(), 2);
    for row in result.rows {
        assert!(matches!(row.values[1], Value::Integer(25)));
    }
}

#[test]
fn left_join_supports_global_table_reference() {
    let (mut keyspace, mut catalog) = setup();
    catalog.create_project("_global").expect("global project");
    catalog
        .create_table(
            "_global",
            "app",
            "users",
            vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            vec!["id".into()],
        )
        .expect("global users");
    for i in 0..20 {
        keyspace.upsert_row(
            "_global",
            "app",
            "users",
            vec![Value::Integer(i)],
            Row::from_values(vec![Value::Integer(i), Value::Text(format!("g{i}").into())]),
            1,
        );
    }
    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "g.name"])
            .from("users")
            .alias("u")
            .left_join("_global.users", "u.id", "id")
            .with_last_join_alias("g")
            .limit(5),
    )
    .expect("left join");
    assert_eq!(result.rows.len(), 5);
}

#[test]
fn join_query_supports_cursor_pagination() {
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 0..50, |_| "US");
    let snapshot = keyspace.snapshot();

    let first = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .order_by("u.id", Order::Asc)
            .limit(5),
        &QueryOptions::default(),
        7,
        10_000,
        None,
    )
    .expect("first page");
    assert_eq!(first.rows.len(), 5);
    assert!(first.cursor.is_some());

    let options = QueryOptions {
        cursor: first.cursor,
        ..QueryOptions::default()
    };
    let second = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.country"])
            .from("users")
            .alias("u")
            .inner_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .limit(5),
        &options,
        7,
        10_000,
        None,
    )
    .expect("join cursor page");
    assert_eq!(second.rows.len(), 5);
    assert!(second.cursor.is_some());

    let first_ids: Vec<Value> = first.rows.iter().map(|r| r.values[0].clone()).collect();
    let second_ids: Vec<Value> = second.rows.iter().map(|r| r.values[0].clone()).collect();
    assert!(
        first_ids
            .iter()
            .all(|id| !second_ids.iter().any(|other| other == id))
    );
}

#[test]
fn right_join_includes_unmatched_right_rows_with_nulls() {
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 90..110, |_| "US");

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.user_id"])
            .from("users")
            .alias("u")
            .right_join("profiles", "u.id", "user_id")
            .with_last_join_alias("p")
            .order_by("p.user_id", Order::Asc)
            .limit(200),
    )
    .expect("right join");

    assert_eq!(result.rows.len(), 20);
    let unmatched = result
        .rows
        .iter()
        .filter(|r| matches!(r.values[0], Value::Null))
        .count();
    assert_eq!(unmatched, 10);
}

#[test]
fn cross_join_cardinality_and_limit_are_correct() {
    let (mut keyspace, mut catalog) = setup();
    add_profiles_table(&mut catalog, vec!["user_id".into()]);
    seed_profiles(&mut keyspace, 0..5, |_| "US");

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["u.id", "p.user_id"])
            .from("users")
            .alias("u")
            .cross_join("profiles")
            .with_last_join_alias("p")
            .limit(123),
    )
    .expect("cross join");
    assert_eq!(result.rows.len(), 123);
}
