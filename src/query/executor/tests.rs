use super::execute_query_with_options;
use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::schema::{ColumnDef, IndexType, TableSchema};
use crate::catalog::types::{ColumnType, Row, Value};
use crate::query::error::QueryError;
use crate::query::plan::{Aggregate, Expr, Order, Query, QueryOptions, col, lit};
use crate::storage::encoded_key::EncodedKey;
use crate::storage::index::extract_index_key_encoded;
use crate::storage::keyspace::{Keyspace, NamespaceId, SecondaryIndex};

fn execute_query(
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
    )
}

fn setup() -> (Keyspace, Catalog) {
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
    for (pk, row) in &table.rows {
        let age_key =
            extract_index_key_encoded(row, &schema, &["age".into()]).expect("age index key");
        secondary_index.insert(age_key, pk.clone());
    }
    table.indexes.insert("by_age".into(), secondary_index);
    let mut by_name = SecondaryIndex::default();
    for (pk, row) in &table.rows {
        let key =
            extract_index_key_encoded(row, &schema, &["name".into()]).expect("name index key");
        by_name.insert(key, pk.clone());
    }
    table.indexes.insert("by_name".into(), by_name);
    (keyspace, catalog)
}

fn validation_schema_with_columns(count: usize) -> TableSchema {
    TableSchema {
        project_id: "A".into(),
        scope_id: "app".into(),
        table_name: "wide".into(),
        owner_id: None,
        columns: (0..count)
            .map(|idx| ColumnDef {
                name: format!("c{idx}"),
                col_type: ColumnType::Integer,
                nullable: false,
            })
            .collect(),
        primary_key: vec!["c0".into()],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
    }
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
fn index_backed_range_scan_reduces_examined_rows() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();

    let full = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"]).from("users"),
    )
    .expect("full");
    let ranged = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"]).from("users").where_(Expr::Between(
            "age".into(),
            Value::Integer(40),
            Value::Integer(41),
        )),
    )
    .expect("range");
    assert!(ranged.rows.len() < full.rows.len());
    assert!(ranged.rows_examined < full.rows_examined);
}

#[test]
fn primary_key_eq_uses_point_lookup_path() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();

    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id", "name"])
            .from("users")
            .where_(Expr::Eq("id".into(), Value::Integer(42)))
            .limit(1),
    )
    .expect("pk point query");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], Value::Integer(42));
    assert_eq!(result.rows_examined, 1);
}

#[test]
fn primary_key_eq_fast_path_preserves_type_validation() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();

    let err = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id"])
            .from("users")
            .where_(Expr::Eq("id".into(), Value::Text("wrong".into())))
            .limit(1),
    )
    .expect_err("type mismatch should be preserved");

    assert!(matches!(err, QueryError::TypeMismatch { column, .. } if column == "id"));
}

#[test]
fn primary_key_with_non_pk_eq_falls_back_to_general_path() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();

    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id", "name"])
            .from("users")
            .where_(
                Expr::Eq("id".into(), Value::Integer(42))
                    .and(Expr::Eq("age".into(), Value::Integer(60))),
            )
            .limit(1),
    )
    .expect("mixed eq query");

    assert_eq!(result.rows.len(), 1);
    assert!(result.rows_examined > 1);
}

#[test]
fn use_index_hint_selects_async_projection() {
    let mut keyspace = Keyspace::default();
    let mut catalog = Catalog::default();
    catalog.create_project("A").expect("project A");
    catalog
        .create_table(
            "A",
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
        .expect("table");
    keyspace.upsert_row(
        "A",
        "app",
        "users",
        vec![Value::Integer(1)],
        Row {
            values: vec![Value::Integer(1), Value::Text("alice".into())],
        },
        1,
    );
    keyspace.insert_async_projection(
        NamespaceId::Project(namespace_key("A", "app")),
        "users".into(),
        "users_view".into(),
        crate::storage::keyspace::AsyncProjectionData {
            rows: {
                let mut rows = im::OrdMap::new();
                rows.insert(
                    EncodedKey::from_values(&[Value::Integer(9)]),
                    Row {
                        values: vec![Value::Integer(9), Value::Text("projection".into())],
                    },
                );
                rows
            },
            materialized_seq: 123,
        },
    );
    let snapshot = keyspace.snapshot();

    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"]).from("users").use_index("users_view"),
    )
    .expect("hint query");

    assert_eq!(result.materialized_seq, Some(123));
    assert_eq!(result.rows[0].values[0], Value::Integer(9));
}

#[test]
fn in_and_like_prefix_can_use_index_path() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();

    let by_in = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"]).from("users").where_(Expr::In(
            "age".into(),
            vec![Value::Integer(40), Value::Integer(41)],
        )),
    )
    .expect("in");
    assert!(
        by_in
            .rows
            .iter()
            .all(|r| { matches!(r.values[2], Value::Integer(40) | Value::Integer(41)) })
    );

    let by_prefix = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .where_(Expr::Like("name".into(), "u1%".into())),
    )
    .expect("prefix like");
    assert!(
        by_prefix
            .rows
            .iter()
            .all(|r| matches!(&r.values[1], Value::Text(s) if s.starts_with("u1")))
    );
}

#[test]
fn and_or_predicates_compose_index_row_sets() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();

    let and_query = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id", "name", "age"]).from("users").where_(
            Expr::Eq("age".into(), Value::Integer(40)).and(Expr::Like("name".into(), "u2%".into())),
        ),
    )
    .expect("and query");
    assert!(and_query.rows.iter().all(|r| {
        matches!(r.values[2], Value::Integer(40))
            && matches!(&r.values[1], Value::Text(name) if name.starts_with("u2"))
    }));

    let or_query = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id", "name"])
            .from("users")
            .where_(
                Expr::Eq("name".into(), Value::Text("u1".into()))
                    .or(Expr::Like("name".into(), "u2%".into())),
            )
            .order_by("id", Order::Asc),
    )
    .expect("or query");
    assert!(or_query.rows.iter().all(|r| match &r.values[1] {
        Value::Text(name) => name == "u1" || name.starts_with("u2"),
        _ => false,
    }));
    assert!(!or_query.rows.is_empty());
}

#[test]
fn composite_index_respects_leftmost_prefix_rule() {
    let (mut keyspace, mut catalog) = setup();
    catalog
        .create_index(
            "A",
            "app",
            "users",
            "by_age_name",
            vec!["age".into(), "name".into()],
            IndexType::BTree,
            None,
        )
        .expect("composite index");
    let schema = catalog
        .tables
        .get(&(namespace_key("A", "app"), "users".to_string()))
        .expect("schema")
        .clone();
    let table = keyspace
        .table_by_namespace_key_mut(&namespace_key("A", "app"), "users")
        .expect("table");
    let mut by_age_name = SecondaryIndex::default();
    for (pk, row) in &table.rows {
        let key = extract_index_key_encoded(row, &schema, &["age".into(), "name".into()])
            .expect("composite key");
        by_age_name.insert(key, pk.clone());
    }
    table.indexes.insert("by_age_name".into(), by_age_name);

    let snapshot = keyspace.snapshot();

    let good = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id", "name", "age"])
            .from("users")
            .where_(Expr::Eq("age".into(), Value::Integer(40))),
    )
    .expect("leftmost predicate should use composite index");
    assert!(
        good.rows_examined < 100,
        "leftmost-prefix query should avoid full scan"
    );

    let bad = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id", "name", "age"])
            .from("users")
            .where_(Expr::Eq(
                "email".into(),
                Value::Text("u1@example.com".into()),
            )),
    )
    .expect("non-leftmost predicate falls back");
    assert!(
        bad.rows_examined >= good.rows_examined,
        "non-leftmost query should not be better than leftmost"
    );
}

#[test]
fn partial_index_only_indexes_matching_rows() {
    let (mut keyspace, mut catalog) = setup();
    catalog
        .create_index(
            "A",
            "app",
            "users",
            "adults_only",
            vec!["age".into()],
            IndexType::BTree,
            Some(Expr::Gte("age".into(), Value::Integer(50))),
        )
        .expect("partial index");
    let schema = catalog
        .tables
        .get(&(namespace_key("A", "app"), "users".to_string()))
        .expect("schema")
        .clone();
    let table = keyspace
        .table_by_namespace_key_mut(&namespace_key("A", "app"), "users")
        .expect("table");
    let mut adults_only = SecondaryIndex {
        partial_filter: Some(Expr::Gte("age".into(), Value::Integer(50))),
        ..SecondaryIndex::default()
    };
    for (pk, row) in &table.rows {
        if adults_only
            .should_include_row(row, &schema, "users")
            .expect("partial eval")
        {
            let key = extract_index_key_encoded(row, &schema, &["age".into()]).expect("index key");
            adults_only.insert(key, pk.clone());
        }
    }
    table.indexes.insert("adults_only".into(), adults_only);

    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id", "age"])
            .from("users")
            .where_(Expr::Gte("age".into(), Value::Integer(50))),
    )
    .expect("partial query");
    assert!(!result.rows.is_empty());
    assert!(
        result
            .rows
            .iter()
            .all(|r| matches!(r.values[1], Value::Integer(v) if v >= 50))
    );
}

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
    )
    .expect_err("cursor path should still honor scan bound");
    assert!(matches!(err, QueryError::ScanBoundExceeded { .. }));
}

#[test]
fn join_scan_bound_uses_cardinality_aware_estimate_when_right_side_is_primary_key() {
    let (keyspace, mut catalog) = setup();
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
    let mut keyspace = keyspace;
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
    )
    .expect("primary-key join should stay within scan bound");
    assert_eq!(result.rows.len(), 10);
    assert!(result.rows_examined <= 50);
}

#[test]
fn type_mismatch_rejected_at_plan_time() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    let err = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .where_(Expr::Gt("age".into(), Value::Text("oops".into()))),
    )
    .expect_err("type mismatch");
    assert!(matches!(err, QueryError::TypeMismatch { .. }));
}

#[test]
fn cursor_pagination_returns_stable_pages() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    let mut options = QueryOptions::default();
    let mut all = Vec::new();
    loop {
        let page = execute_query_with_options(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"])
                .from("users")
                .order_by("id", Order::Asc)
                .limit(10),
            &options,
            42,
            10_000,
        )
        .expect("page");
        all.extend(page.rows.clone());
        if let Some(cursor) = page.cursor {
            options.cursor = Some(cursor);
        } else {
            break;
        }
    }
    assert_eq!(all.len(), 100);
    for (i, row) in all.iter().enumerate().take(100) {
        assert_eq!(row.values[0], Value::Integer(i as i64));
    }
}

#[test]
fn inner_join_returns_matching_rows() {
    let (keyspace, mut catalog) = setup();
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
    let mut keyspace = keyspace;
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
    let (keyspace, mut catalog) = setup();
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
    let mut keyspace = keyspace;
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
            .limit(100),
        &QueryOptions::default(),
        1,
        100,
    )
    .expect("pk join should respect bounded probe path");
    assert_eq!(result.rows.len(), 50);
    assert!(result.rows_examined <= 50);
}

#[test]
fn join_aggregate_count_and_having_are_applied() {
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
            Row::from_values(vec![
                Value::Integer(i),
                Value::Text(if i % 2 == 0 { "US" } else { "CA" }.into()),
            ]),
            1,
        );
    }
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
fn invalid_cursor_is_rejected() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    let options = QueryOptions {
        cursor: Some("xyz".into()),
        ..QueryOptions::default()
    };
    let err = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .order_by("id", Order::Asc),
        &options,
        42,
        10_000,
    )
    .expect_err("invalid cursor should fail");
    assert!(matches!(err, QueryError::InvalidQuery { .. }));
}

#[test]
fn uppercase_hex_cursor_is_accepted() {
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
            .limit(10),
        &QueryOptions::default(),
        42,
        10_000,
    )
    .expect("first page");
    let cursor = first
        .cursor
        .expect("first page should include cursor")
        .to_ascii_uppercase();
    let options = QueryOptions {
        cursor: Some(cursor),
        ..QueryOptions::default()
    };
    let second = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["*"])
            .from("users")
            .order_by("id", Order::Asc)
            .limit(10),
        &options,
        42,
        10_000,
    )
    .expect("uppercase cursor should decode");
    assert_eq!(second.rows.len(), 10);
}

#[test]
fn validate_query_rejects_too_many_order_by_columns() {
    let schema = validation_schema_with_columns(33);
    let mut query = Query::select(&["*"]).from("wide");
    query.order_by = (0..33).map(|idx| (format!("c{idx}"), Order::Asc)).collect();

    let err = super::validate::validate_query(&schema, &query).expect_err("too many order by");
    assert!(matches!(err, QueryError::InvalidQuery { .. }));
}

#[test]
fn validate_query_rejects_too_many_group_by_columns() {
    let schema = validation_schema_with_columns(33);
    let mut query = Query::select(&["c0"]).from("wide");
    query.group_by = (0..33).map(|idx| format!("c{idx}")).collect();

    let err = super::validate::validate_query(&schema, &query).expect_err("too many group by");
    assert!(matches!(err, QueryError::InvalidQuery { .. }));
}

#[test]
fn validate_query_rejects_too_many_aggregates() {
    let schema = validation_schema_with_columns(1);
    let mut query = Query::select(&["count_star"]).from("wide");
    query.aggregates = (0..33).map(|_| Aggregate::Count).collect();

    let err = super::validate::validate_query(&schema, &query).expect_err("too many aggregates");
    assert!(matches!(err, QueryError::InvalidQuery { .. }));
}

#[test]
fn cursor_snapshot_mismatch_is_rejected() {
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
            .limit(10),
        &QueryOptions::default(),
        42,
        10_000,
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
        Query::select(&["*"])
            .from("users")
            .order_by("id", Order::Asc)
            .limit(10),
        &options,
        43,
        10_000,
    )
    .expect_err("snapshot mismatch");
    assert!(matches!(err, QueryError::InvalidQuery { .. }));
}

#[test]
fn join_query_supports_cursor_pagination() {
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
    for i in 90..110 {
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
    for i in 0..5 {
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

#[test]
fn descending_cursor_pagination_is_stable() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    let mut options = QueryOptions::default();
    let mut all = Vec::new();
    loop {
        let page = execute_query_with_options(
            &snapshot,
            &catalog,
            "A",
            "app",
            Query::select(&["*"])
                .from("users")
                .order_by("id", Order::Desc)
                .limit(11),
            &options,
            55,
            10_000,
        )
        .expect("page");
        all.extend(page.rows.clone());
        if let Some(cursor) = page.cursor {
            options.cursor = Some(cursor);
        } else {
            break;
        }
    }
    assert_eq!(all.len(), 100);
    for w in all.windows(2) {
        assert!(w[0].values[0] > w[1].values[0]);
    }
}

#[test]
fn top_k_sort_matches_full_multi_column_ordering() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();

    let limited = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id", "age"])
            .from("users")
            .order_by("age", Order::Desc)
            .order_by("id", Order::Asc)
            .limit(7),
    )
    .expect("limited ordered query");
    let full = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id", "age"])
            .from("users")
            .order_by("age", Order::Desc)
            .order_by("id", Order::Asc),
    )
    .expect("full ordered query");

    let expected: Vec<Row> = full.rows.into_iter().take(7).collect();
    assert_eq!(limited.rows, expected);
}

#[test]
fn contradictory_pk_equalities_return_empty_result() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    let result = execute_query(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["id"])
            .from("users")
            .where_(
                Expr::Eq("id".into(), Value::Integer(1))
                    .and(Expr::Eq("id".into(), Value::Integer(2))),
            )
            .limit(10),
    )
    .expect("query");
    assert!(result.rows.is_empty());
}
