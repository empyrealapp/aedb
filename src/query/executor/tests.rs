use super::execute_query_with_options;
use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::schema::{ColumnDef, IndexType};
use crate::catalog::types::{ColumnType, Row, Value};
use crate::query::error::QueryError;
use crate::query::plan::{Aggregate, Expr, Order, Query, QueryOptions, ScalarExpr, col, lit};
use crate::storage::index::extract_index_key_encoded;
use crate::storage::keyspace::{Keyspace, SecondaryIndex};

pub(super) fn execute_query(
    snapshot: &crate::storage::keyspace::KeyspaceSnapshot,
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    query: Query,
) -> Result<super::QueryResult, QueryError> {
    execute_query_with_options(
        snapshot,
        catalog,
        project_id,
        scope_id,
        query,
        &QueryOptions {
            allow_full_scan: true,
            ..QueryOptions::default()
        },
        0,
        usize::MAX,
        None,
    )
}

pub(super) fn setup() -> (Keyspace, Catalog) {
    let mut keyspace = Keyspace::default();
    let mut catalog = Catalog::default();
    catalog.create_project("A").expect("project A");
    catalog.create_project("B").expect("project B");
    for p in ["A", "B"] {
        catalog
            .create_table(
                p,
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
                    ColumnDef {
                        name: "age".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "email".into(),
                        col_type: ColumnType::Text,
                        nullable: true,
                    },
                ],
                vec!["id".into()],
            )
            .expect("table");
    }
    for i in 0..100 {
        keyspace.upsert_row(
            "A",
            "app",
            "users",
            vec![Value::Integer(i)],
            Row {
                values: vec![
                    Value::Integer(i),
                    Value::Text(format!("u{i}").into()),
                    Value::Integer(18 + (i % 50)),
                    if i == 0 {
                        Value::Null
                    } else if i % 2 == 0 {
                        Value::Text(format!("u{i}@gmail.com").into())
                    } else {
                        Value::Text(format!("u{i}@example.com").into())
                    },
                ],
            },
            i as u64 + 1,
        );
        keyspace.upsert_row(
            "B",
            "app",
            "users",
            vec![Value::Integer(i)],
            Row {
                values: vec![
                    Value::Integer(i),
                    Value::Text(format!("b{i}").into()),
                    Value::Integer(99),
                    Value::Text(format!("b{i}@other.com").into()),
                ],
            },
            i as u64 + 10_000,
        );
    }
    catalog
        .create_index(
            "A",
            "app",
            "users",
            "by_age",
            vec!["age".into()],
            IndexType::BTree,
            None,
        )
        .expect("create index");
    catalog
        .create_index(
            "A",
            "app",
            "users",
            "by_name",
            vec!["name".into()],
            IndexType::BTree,
            None,
        )
        .expect("create name index");
    let schema = catalog
        .tables
        .get(&(namespace_key("A", "app"), "users".to_string()))
        .expect("schema")
        .clone();
    let table = keyspace
        .table_by_namespace_key_mut(&namespace_key("A", "app"), "users")
        .expect("table");
    let mut secondary_index = SecondaryIndex::default();
    for (pk, stored) in &table.rows {
        let row = stored.resident().expect("resident row");
        let age_key =
            extract_index_key_encoded(row, &schema, &["age".into()]).expect("age index key");
        secondary_index.insert(age_key, pk.clone());
    }
    table.indexes.insert("by_age".into(), secondary_index);
    let mut by_name = SecondaryIndex::default();
    for (pk, stored) in &table.rows {
        let row = stored.resident().expect("resident row");
        let key =
            extract_index_key_encoded(row, &schema, &["name".into()]).expect("name index key");
        by_name.insert(key, pk.clone());
    }
    table.indexes.insert("by_name".into(), by_name);
    (keyspace, catalog)
}

#[test]
fn query_correctness_suite() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();

    let all = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"]).from("users"),
    )
    .expect("all");
    assert_eq!(all.rows.len(), 100);

    let filtered = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .where_(Expr::Gt("age".into(), Value::Integer(30))),
    )
    .expect("filtered");
    assert!(
        filtered
            .rows
            .iter()
            .all(|r| matches!(r.values[2], Value::Integer(v) if v > 30))
    );

    let ordered = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .order_by("age", Order::Desc)
            .order_by("id", Order::Asc),
    )
    .expect("ordered");
    for w in ordered.rows.windows(2) {
        assert!(w[0].values[2] >= w[1].values[2]);
    }

    let limited = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"]).from("users").limit(5),
    )
    .expect("limit");
    assert_eq!(limited.rows.len(), 5);

    let counted = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .aggregate(Aggregate::Count),
    )
    .expect("count");
    assert_eq!(counted.rows[0].values[0], Value::Integer(100));

    let grouped = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .group_by(&["age"])
            .aggregate(Aggregate::Count),
    )
    .expect("grouped");
    assert!(!grouped.rows.is_empty());

    let compound = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"]).from("users").where_(
            Expr::Gt("age".into(), Value::Integer(30))
                .and(Expr::Like("email".into(), "%@gmail.com".into())),
        ),
    )
    .expect("compound");
    assert!(compound.rows.iter().all(|r| {
        matches!(&r.values[2], Value::Integer(v) if *v > 30)
            && matches!(&r.values[3], Value::Text(s) if s.ends_with("@gmail.com"))
    }));

    let project_b = execute_query(
        &snapshot,
        &catalog,
        "B",
        "app",
        Query::select(&["*"])
            .from("users")
            .where_(Expr::Eq("age".into(), Value::Integer(99))),
    )
    .expect("project B");
    assert_eq!(project_b.rows.len(), 100);
}

#[test]
fn builder_supports_not_is_not_null_and_like_underscore() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();

    let query = Query::select(&["id", "email"]).from("users").where_(
        col("email")
            .is_not_null()
            .and(col("name").like(lit("u_")))
            .and(col("age").gt(lit(20)).not().not()),
    );
    let result = execute_query(&snapshot, &catalog, "A", "app", query).expect("query");
    assert!(!result.rows.is_empty());
    assert!(
        result
            .rows
            .iter()
            .all(|r| matches!(&r.values[1], Value::Text(_)))
    );
}

#[test]
fn having_filters_post_aggregation() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();

    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["age", "count_star"])
            .from("users")
            .group_by(&["age"])
            .aggregate(Aggregate::Count)
            .having(Expr::Gt("count_star".into(), Value::Integer(1))),
    )
    .expect("having");

    assert!(
        result
            .rows
            .iter()
            .all(|r| matches!(r.values[1], Value::Integer(v) if v > 1))
    );
}

#[test]
fn distinct_dedupes_projected_rows() {
    // users.age = 18 + (id % 50) over 100 ids => 50 distinct ages, each twice.
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["age"]).from("users").distinct().limit(1000),
    )
    .expect("distinct");
    assert_eq!(result.rows.len(), 50);
    let mut ages: Vec<Value> = result.rows.iter().map(|r| r.values[0].clone()).collect();
    ages.sort();
    ages.dedup();
    assert_eq!(ages.len(), 50, "rows must be unique");
}

#[test]
fn distinct_applies_offset_and_limit_after_dedup() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["age"])
            .from("users")
            .order_by("age", Order::Asc)
            .distinct()
            .limit(10)
            .offset(5),
    )
    .expect("distinct page");
    assert_eq!(result.rows.len(), 10);
    // 50 distinct ages 18..=67 sorted; offset 5 => first is 23.
    assert!(matches!(result.rows[0].values[0], Value::Integer(23)));
    assert!(result.truncated);
}

#[test]
fn distinct_rejects_cursor_pagination() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    // Obtain a real cursor from a first (non-distinct) page.
    let first = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id"])
            .from("users")
            .order_by("id", Order::Asc)
            .limit(5),
        &QueryOptions::default(),
        1,
        10_000,
        None,
    )
    .expect("first page");
    let options = QueryOptions {
        cursor: first.cursor,
        ..QueryOptions::default()
    };
    let err = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id"])
            .from("users")
            .order_by("id", Order::Asc)
            .distinct()
            .limit(5),
        &options,
        1,
        10_000,
        None,
    )
    .expect_err("distinct + cursor must be rejected");
    assert!(matches!(err, QueryError::InvalidQuery { .. }));
}

#[test]
fn computed_projection_appends_arithmetic_column() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id", "age"])
            .from("users")
            .where_(col("id").eq(lit(0)))
            .compute(
                "age_plus_10",
                ScalarExpr::col("age").add(ScalarExpr::lit(10i64)),
            ),
    )
    .expect("computed projection");

    assert_eq!(result.rows.len(), 1);
    // [id = 0, age = 18, age_plus_10 = 28]
    assert_eq!(result.rows[0].values.len(), 3);
    assert!(matches!(result.rows[0].values[2], Value::Integer(28)));
}

#[test]
fn computed_projection_rejected_with_aggregates() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    let err = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["count_star"])
            .from("users")
            .aggregate(Aggregate::Count)
            .compute("bad", ScalarExpr::col("age").add(ScalarExpr::lit(1i64))),
    )
    .expect_err("computed + aggregate must be rejected");
    assert!(matches!(err, QueryError::InvalidQuery { .. }));
}

#[test]
fn computed_division_and_null_semantics() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id"])
            .from("users")
            .where_(col("id").eq(lit(0)))
            .compute("exact", ScalarExpr::lit(10i64).div(ScalarExpr::lit(2i64)))
            .compute("inexact", ScalarExpr::lit(10i64).div(ScalarExpr::lit(3i64)))
            .compute(
                "div_zero",
                ScalarExpr::lit(10i64).div(ScalarExpr::lit(0i64)),
            )
            .compute(
                "null_operand",
                ScalarExpr::col("email").add(ScalarExpr::lit(1i64)),
            ),
    )
    .expect("division semantics");

    assert_eq!(result.rows.len(), 1);
    let v = &result.rows[0].values;
    // [id, exact, inexact, div_zero, null_operand]
    assert!(matches!(v[1], Value::Integer(5)), "exact int division");
    assert!(
        matches!(v[2], Value::Float(f) if (f - 10.0 / 3.0).abs() < 1e-9),
        "inexact division is float"
    );
    assert!(matches!(v[3], Value::Null), "division by zero is null");
    // id 0 has a NULL email, so arithmetic on it is NULL.
    assert!(matches!(v[4], Value::Null), "null operand propagates");
}
