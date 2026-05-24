use super::tests::{execute_query, setup};
use crate::catalog::schema::{ColumnDef, TableSchema};
use crate::catalog::types::{ColumnType, Value};
use crate::query::error::QueryError;
use crate::query::plan::{Aggregate, Expr, Order, Query};

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
