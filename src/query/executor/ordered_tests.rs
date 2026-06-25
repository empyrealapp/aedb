use super::{execute_query_with_options, explain_access_path_for_query};
use crate::QueryResult;
use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::schema::IndexType;
use crate::catalog::types::Value;
use crate::query::plan::{Order, Query, QueryOptions};
use crate::storage::index::extract_index_key_encoded;
use crate::storage::keyspace::SecondaryIndex;

const PROJECT_ID: &str = "A";
const SCOPE_ID: &str = "app";
const TABLE_NAME: &str = "users";

fn no_full_scan_options() -> QueryOptions {
    QueryOptions {
        allow_full_scan: false,
        ..QueryOptions::default()
    }
}

fn result_ids(result: &QueryResult) -> Vec<Value> {
    result
        .rows
        .iter()
        .map(|row| row.values[0].clone())
        .collect()
}

fn build_runtime_index(
    keyspace: &mut crate::storage::keyspace::Keyspace,
    catalog: &Catalog,
    index_name: &str,
    columns: &[&str],
) {
    let ns = namespace_key(PROJECT_ID, SCOPE_ID);
    let schema = catalog
        .tables
        .get(&(ns.clone(), TABLE_NAME.to_string()))
        .expect("users schema")
        .clone();
    let columns = columns
        .iter()
        .map(|column| column.to_string())
        .collect::<Vec<_>>();
    let table = keyspace
        .table_by_namespace_key_mut(&ns, TABLE_NAME)
        .expect("users table");
    let mut runtime_index = SecondaryIndex::default();
    for (pk, stored) in &table.rows {
        let row = stored.resident().expect("resident row");
        let key = extract_index_key_encoded(row, &schema, &columns).expect("runtime index key");
        runtime_index.insert(key, pk.clone());
    }
    table.indexes.insert(index_name.into(), runtime_index);
}

fn add_email_index(keyspace: &mut crate::storage::keyspace::Keyspace, catalog: &Catalog) {
    build_runtime_index(keyspace, catalog, "by_email", &["email"]);
}

fn replace_age_index_with_composite(
    keyspace: &mut crate::storage::keyspace::Keyspace,
    catalog: &mut Catalog,
) {
    let ns = namespace_key(PROJECT_ID, SCOPE_ID);
    catalog
        .indexes
        .remove(&(ns.clone(), TABLE_NAME.to_string(), "by_age".to_string()));
    catalog
        .create_index(
            PROJECT_ID,
            SCOPE_ID,
            TABLE_NAME,
            "by_age_name",
            vec!["age".into(), "name".into()],
            IndexType::BTree,
            None,
        )
        .expect("create age/name index");
    let table = keyspace
        .table_by_namespace_key_mut(&ns, TABLE_NAME)
        .expect("users table");
    table.indexes.remove("by_age");
    build_runtime_index(keyspace, catalog, "by_age_name", &["age", "name"]);
}

#[test]
fn order_by_limit_uses_index_order_without_full_sort() {
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();

    let asc = execute_query_with_options(
        &snapshot,
        &catalog,
        PROJECT_ID,
        SCOPE_ID,
        Query::select(&["id", "age"])
            .from(TABLE_NAME)
            .order_by("age", Order::Asc)
            .limit(5),
        &no_full_scan_options(),
        0,
        10_000,
        None,
    )
    .expect("ascending ordered index query");

    assert_eq!(asc.rows.len(), 5);
    assert!(
        asc.rows_examined <= 6,
        "ordered index path should read only the page window, examined={}",
        asc.rows_examined
    );
    assert!(
        asc.rows
            .windows(2)
            .all(|rows| rows[0].values[1] <= rows[1].values[1])
    );

    let desc = execute_query_with_options(
        &snapshot,
        &catalog,
        PROJECT_ID,
        SCOPE_ID,
        Query::select(&["id", "age"])
            .from(TABLE_NAME)
            .order_by("age", Order::Desc)
            .limit(5),
        &no_full_scan_options(),
        0,
        10_000,
        None,
    )
    .expect("descending ordered index query");

    assert_eq!(desc.rows.len(), 5);
    assert!(
        desc.rows_examined <= 6,
        "descending ordered index path should read only the page window, examined={}",
        desc.rows_examined
    );
    assert!(
        desc.rows
            .windows(2)
            .all(|rows| rows[0].values[1] >= rows[1].values[1])
    );

    let explain = explain_access_path_for_query(
        &snapshot,
        &catalog,
        PROJECT_ID,
        SCOPE_ID,
        &Query::select(&["id", "age"])
            .from(TABLE_NAME)
            .order_by("age", Order::Desc)
            .limit(5),
        &no_full_scan_options(),
    )
    .expect("ordered index explain");
    assert_eq!(explain.selected_indexes, vec!["by_age".to_string()]);
    assert!(
        explain
            .plan_trace
            .iter()
            .any(|line| line.contains("ordered row source"))
    );

    let ordered_offset_result = execute_query_with_options(
        &snapshot,
        &catalog,
        PROJECT_ID,
        SCOPE_ID,
        Query::select(&["id", "age"])
            .from(TABLE_NAME)
            .order_by("age", Order::Asc)
            .limit(3)
            .offset(4),
        &no_full_scan_options(),
        0,
        10_000,
        None,
    )
    .expect("ordered index offset query");
    assert_eq!(
        result_ids(&ordered_offset_result),
        vec![Value::Integer(2), Value::Integer(52), Value::Integer(3)]
    );
    assert_eq!(ordered_offset_result.rows_examined, 4);
    assert!(ordered_offset_result.truncated);
    assert!(ordered_offset_result.cursor.is_none());
}

#[test]
fn ordered_predicate_range_uses_index_window_without_sort() {
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();

    let asc = execute_query_with_options(
        &snapshot,
        &catalog,
        PROJECT_ID,
        SCOPE_ID,
        Query::select(&["id", "age"])
            .from(TABLE_NAME)
            .where_(crate::query::plan::Expr::Between(
                "age".into(),
                Value::Integer(25),
                Value::Integer(35),
            ))
            .order_by("age", Order::Asc)
            .limit(4)
            .offset(3),
        &no_full_scan_options(),
        0,
        10_000,
        None,
    )
    .expect("ascending ordered predicate query");

    assert_eq!(asc.rows.len(), 4);
    assert_eq!(
        asc.rows_examined, 5,
        "ordered predicate path should only read the requested page plus lookahead"
    );
    assert!(
        asc.rows
            .windows(2)
            .all(|rows| rows[0].values[1] <= rows[1].values[1])
    );
    assert!(
        asc.rows
            .iter()
            .all(|row| matches!(row.values[1], Value::Integer(age) if (25..=35).contains(&age)))
    );

    let desc = execute_query_with_options(
        &snapshot,
        &catalog,
        PROJECT_ID,
        SCOPE_ID,
        Query::select(&["id", "age"])
            .from(TABLE_NAME)
            .where_(crate::query::plan::Expr::Between(
                "age".into(),
                Value::Integer(25),
                Value::Integer(35),
            ))
            .order_by("age", Order::Desc)
            .limit(4),
        &no_full_scan_options(),
        0,
        10_000,
        None,
    )
    .expect("descending ordered predicate query");

    assert_eq!(desc.rows.len(), 4);
    assert_eq!(
        desc.rows_examined, 5,
        "descending ordered predicate path should only read the page plus lookahead"
    );
    assert!(
        desc.rows
            .windows(2)
            .all(|rows| rows[0].values[1] >= rows[1].values[1])
    );
    assert!(
        desc.rows
            .iter()
            .all(|row| matches!(row.values[1], Value::Integer(age) if (25..=35).contains(&age)))
    );

    let explain = explain_access_path_for_query(
        &snapshot,
        &catalog,
        PROJECT_ID,
        SCOPE_ID,
        &Query::select(&["id", "age"])
            .from(TABLE_NAME)
            .where_(crate::query::plan::Expr::Between(
                "age".into(),
                Value::Integer(25),
                Value::Integer(35),
            ))
            .order_by("age", Order::Desc)
            .limit(4),
        &no_full_scan_options(),
    )
    .expect("ordered predicate explain");
    assert_eq!(explain.selected_indexes, vec!["by_age".to_string()]);
    assert!(explain.order_satisfied_by_row_source);
    assert!(!explain.residual_filter_required);
}

#[test]
fn nullable_order_column_keeps_sort_stage() {
    let (mut keyspace, mut catalog) = super::tests::setup();
    catalog
        .create_index(
            PROJECT_ID,
            SCOPE_ID,
            TABLE_NAME,
            "by_email",
            vec!["email".into()],
            IndexType::BTree,
            None,
        )
        .expect("create email index");
    add_email_index(&mut keyspace, &catalog);
    let snapshot = keyspace.snapshot();

    let query = Query::select(&["id", "email"])
        .from(TABLE_NAME)
        .order_by("email", Order::Asc)
        .limit(3);

    let result = execute_query_with_options(
        &snapshot,
        &catalog,
        PROJECT_ID,
        SCOPE_ID,
        query.clone(),
        &no_full_scan_options(),
        0,
        10_000,
        None,
    )
    .expect("nullable order query");

    assert_eq!(
        result_ids(&result),
        vec![Value::Integer(0), Value::Integer(10), Value::Integer(11)]
    );

    let explain = explain_access_path_for_query(
        &snapshot,
        &catalog,
        PROJECT_ID,
        SCOPE_ID,
        &query,
        &no_full_scan_options(),
    )
    .expect("nullable order explain");
    assert!(
        explain.selected_indexes.is_empty(),
        "nullable ordering must use the general scan + sort path because index key ordering places NULL differently"
    );
    assert!(
        !explain
            .plan_trace
            .iter()
            .any(|line| line.contains("ordered row source"))
    );
}

#[test]
fn composite_leftmost_order_index_keeps_sort_stage() {
    let (mut keyspace, mut catalog) = super::tests::setup();
    replace_age_index_with_composite(&mut keyspace, &mut catalog);
    let snapshot = keyspace.snapshot();

    let query = Query::select(&["id", "age"])
        .from(TABLE_NAME)
        .order_by("age", Order::Asc)
        .limit(5);
    let result = execute_query_with_options(
        &snapshot,
        &catalog,
        PROJECT_ID,
        SCOPE_ID,
        query.clone(),
        &no_full_scan_options(),
        0,
        10_000,
        None,
    )
    .expect("composite leftmost order query");

    assert_eq!(result.rows.len(), 5);
    assert!(
        result.rows_examined > result.rows.len(),
        "composite index tie ordering must not be treated as satisfying single-column ORDER BY"
    );

    let explain = explain_access_path_for_query(
        &snapshot,
        &catalog,
        PROJECT_ID,
        SCOPE_ID,
        &query,
        &no_full_scan_options(),
    )
    .expect("composite leftmost order explain");
    assert!(
        explain.selected_indexes.is_empty(),
        "composite leftmost index cannot satisfy ORDER BY without changing cursor tie ordering"
    );
    assert!(
        !explain
            .plan_trace
            .iter()
            .any(|line| line.contains("ordered row source"))
    );
}

#[test]
fn multi_column_order_by_keeps_sort_stage() {
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();

    let result = execute_query_with_options(
        &snapshot,
        &catalog,
        PROJECT_ID,
        SCOPE_ID,
        Query::select(&["id", "age"])
            .from(TABLE_NAME)
            .order_by("age", Order::Asc)
            .order_by("id", Order::Desc)
            .limit(4),
        &no_full_scan_options(),
        0,
        10_000,
        None,
    )
    .expect("multi-column order query");

    assert_eq!(result.rows.len(), 4);
    assert!(
        result.rows_examined > result.rows.len(),
        "multi-column ordering must not use the ordered-index row source"
    );
    assert_eq!(
        result_ids(&result),
        vec![
            Value::Integer(50),
            Value::Integer(0),
            Value::Integer(51),
            Value::Integer(1),
        ]
    );

    let explain = explain_access_path_for_query(
        &snapshot,
        &catalog,
        PROJECT_ID,
        SCOPE_ID,
        &Query::select(&["id", "age"])
            .from(TABLE_NAME)
            .order_by("age", Order::Asc)
            .order_by("id", Order::Desc)
            .limit(4),
        &no_full_scan_options(),
    )
    .expect("multi-column order explain");
    assert!(
        explain.selected_indexes.is_empty(),
        "multi-column ordering requires the general scan + sort path"
    );
    assert!(
        !explain
            .plan_trace
            .iter()
            .any(|line| line.contains("ordered row source"))
    );
}

#[test]
fn zero_limit_ordered_index_query_reads_no_rows() {
    let (keyspace, catalog) = super::tests::setup();
    let snapshot = keyspace.snapshot();

    let result = execute_query_with_options(
        &snapshot,
        &catalog,
        PROJECT_ID,
        SCOPE_ID,
        Query::select(&["id", "age"])
            .from(TABLE_NAME)
            .order_by("age", Order::Asc)
            .limit(0),
        &no_full_scan_options(),
        0,
        10_000,
        None,
    )
    .expect("zero limit ordered index query");

    assert!(result.rows.is_empty());
    assert_eq!(result.rows_examined, 0);
    assert!(!result.truncated);
    assert!(result.cursor.is_none());
}
