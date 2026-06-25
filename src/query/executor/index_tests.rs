use super::execute_query_with_options;
use super::indexing::indexed_pks_for_predicate_limited;
use super::tests::{execute_query, setup};
use crate::catalog::namespace_key;
use crate::catalog::schema::{ColumnDef, IndexType};
use crate::catalog::types::{ColumnType, Row, Value};
use crate::query::error::QueryError;
use crate::query::plan::{Expr, Order, Query, QueryOptions};
use crate::storage::encoded_key::EncodedKey;
use crate::storage::index::extract_index_key_encoded;
use crate::storage::keyspace::{Keyspace, NamespaceId, SecondaryIndex};

fn install_composite_age_name_index(
    keyspace: &mut Keyspace,
    catalog: &crate::catalog::Catalog,
    index_name: &str,
) {
    let ns = namespace_key("A", "app");
    let schema = catalog
        .tables
        .get(&(ns.clone(), "users".to_string()))
        .expect("schema")
        .clone();
    let table = keyspace
        .table_by_namespace_key_mut(&ns, "users")
        .expect("table");
    let mut age_name_index = SecondaryIndex::default();
    for (pk, stored) in &table.rows {
        let row = stored.resident().expect("resident row");
        let key = extract_index_key_encoded(row, &schema, &["age".into(), "name".into()])
            .expect("composite key");
        age_name_index.insert(key, pk.clone());
    }
    table.indexes.insert(index_name.into(), age_name_index);
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
fn exact_index_predicate_limit_caps_candidate_materialization() {
    let (mut keyspace, catalog) = setup();
    let ns = namespace_key("A", "app");
    let schema = catalog
        .tables
        .get(&(ns.clone(), "users".to_string()))
        .expect("schema")
        .clone();
    let table = keyspace
        .table_by_namespace_key_mut(&ns, "users")
        .expect("table");
    for i in 100..300 {
        let pk = vec![Value::Integer(i)];
        let row = Row {
            values: vec![
                Value::Integer(i),
                Value::Text(format!("hot{i}").into()),
                Value::Integer(42),
                Value::Text(format!("hot{i}@example.com").into()),
            ],
        };
        let encoded_pk = EncodedKey::from_values(&pk);
        let age_key =
            extract_index_key_encoded(&row, &schema, &["age".into()]).expect("age index key");
        table
            .indexes
            .get_mut("by_age")
            .expect("age index")
            .insert(age_key, encoded_pk);
        table.rows.insert(EncodedKey::from_values(&pk), row.into());
    }
    let snapshot = keyspace.snapshot();
    let table = snapshot.table("A", "app", "users").expect("snapshot table");

    let exact = indexed_pks_for_predicate_limited(
        &catalog,
        "A",
        "app",
        "users",
        table,
        &Expr::Eq("age".into(), Value::Integer(42)),
        Some(7),
    )
    .expect("index lookup")
    .expect("indexed");
    assert!(exact.predicate_exact);
    assert_eq!(exact.pks.len(), 7);

    let residual = indexed_pks_for_predicate_limited(
        &catalog,
        "A",
        "app",
        "users",
        table,
        &Expr::Eq("age".into(), Value::Integer(42))
            .and(Expr::Eq("email".into(), Value::Text("missing".into()))),
        Some(7),
    )
    .expect("residual lookup")
    .expect("indexed");
    assert!(!residual.predicate_exact);
    assert!(
        residual.pks.len() > 7,
        "residual predicates must not cap candidates before filtering"
    );
}

#[test]
fn non_exact_like_index_predicate_keeps_residual_filter() {
    let (keyspace, catalog) = setup();
    let snapshot = keyspace.snapshot();
    let table = snapshot.table("A", "app", "users").expect("snapshot table");
    let predicate = Expr::Like("name".into(), "u1%7%".into());

    let indexed = indexed_pks_for_predicate_limited(
        &catalog,
        "A",
        "app",
        "users",
        table,
        &predicate,
        Some(1),
    )
    .expect("index lookup")
    .expect("indexed");
    assert!(!indexed.predicate_exact);
    assert!(
        indexed.pks.len() > 1,
        "non-exact LIKE candidates must not be capped before residual filtering"
    );

    let result = execute_query_with_options(
        &snapshot,
        &catalog,
        "A",
        "app",
        Query::select(&["name"]).from("users").where_(predicate),
        &QueryOptions {
            allow_full_scan: false,
            ..QueryOptions::default()
        },
        0,
        10_000,
        None,
    )
    .expect("like query");

    assert_eq!(
        result.rows,
        vec![Row {
            values: vec![Value::Text("u17".into())]
        }]
    );
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
    let mut catalog = crate::catalog::Catalog::default();
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
    install_composite_age_name_index(&mut keyspace, &catalog, "by_age_name");

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
fn simple_leftmost_eq_can_use_composite_index_without_single_column_index() {
    let (mut keyspace, mut catalog) = setup();
    let ns = namespace_key("A", "app");
    catalog
        .indexes
        .remove(&(ns.clone(), "users".to_string(), "by_age".to_string()));
    keyspace
        .table_by_namespace_key_mut(&ns, "users")
        .expect("table")
        .indexes
        .remove("by_age");
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
    install_composite_age_name_index(&mut keyspace, &catalog, "by_age_name");

    let result = execute_query(
        &keyspace.snapshot(),
        &catalog,
        "A",
        "app",
        Query::select(&["id", "name", "age"])
            .from("users")
            .where_(Expr::Eq("age".into(), Value::Integer(40))),
    )
    .expect("leftmost eq should use composite index");

    assert!(!result.rows.is_empty());
    assert!(
        result.rows_examined < 100,
        "leftmost eq should avoid full scan via composite index"
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
    for (pk, stored) in &table.rows {
        let row = stored.resident().expect("resident row");
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
