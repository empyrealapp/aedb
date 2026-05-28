use crate::catalog::schema::{ColumnDef, TableSchema};
use crate::catalog::types::{ColumnType, Value};
use crate::query::plan::{Query, col};
use crate::query::planner::{ExecutionStage, build_physical_plan};

fn schema() -> TableSchema {
    TableSchema {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        owner_id: None,
        columns: vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        primary_key: vec!["id".into()],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
    }
}

#[test]
fn exact_index_predicate_skips_filter_stage() {
    let query = Query::select(&["id"])
        .from("users")
        .where_(col("id").eq(Value::Integer(1)));

    let plan =
        build_physical_plan(&schema(), &query, Some("by_id".into()), 1, false).expect("plan");

    assert!(!plan.stages.contains(&ExecutionStage::Filter));
    assert!(!plan.plan.has_residual_filter);
}

#[test]
fn residual_predicate_keeps_filter_stage() {
    let query = Query::select(&["id"])
        .from("users")
        .where_(col("id").eq(Value::Integer(1)));

    let plan = build_physical_plan(&schema(), &query, Some("by_id".into()), 1, true).expect("plan");

    assert!(plan.stages.contains(&ExecutionStage::Filter));
    assert!(plan.plan.has_residual_filter);
}
