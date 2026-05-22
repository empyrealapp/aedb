use crate::catalog::schema::TableSchema;
use crate::query::error::QueryError;
use crate::query::plan::{Query, QueryPlan};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalPlanNode {
    Scan {
        table: String,
        index: Option<String>,
    },
    Filter {
        expr: bool,
        child: Box<PhysicalPlanNode>,
    },
    Sort {
        columns: Vec<String>,
        child: Box<PhysicalPlanNode>,
    },
    Aggregate {
        group_by: Vec<String>,
        aggregate_count: usize,
        child: Box<PhysicalPlanNode>,
    },
    Project {
        columns: Vec<String>,
        child: Box<PhysicalPlanNode>,
    },
    Limit {
        limit: usize,
        child: Box<PhysicalPlanNode>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedQuery {
    pub plan: QueryPlan,
    pub root: PhysicalPlanNode,
    pub stages: Vec<ExecutionStage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStage {
    Scan,
    Filter,
    Sort,
    Aggregate,
    Having,
    Project,
    Limit,
}

pub fn build_physical_plan(
    schema: &TableSchema,
    query: &Query,
    index_used: Option<String>,
    estimated_scan_rows: u64,
    has_residual_filter: bool,
) -> Result<PlannedQuery, QueryError> {
    let mut node = PhysicalPlanNode::Scan {
        table: query.table.clone(),
        index: index_used.clone(),
    };
    let mut stages = vec![ExecutionStage::Scan];

    if has_residual_filter {
        node = PhysicalPlanNode::Filter {
            expr: true,
            child: Box::new(node),
        };
        stages.push(ExecutionStage::Filter);
    }
    if !query.order_by.is_empty() {
        node = PhysicalPlanNode::Sort {
            columns: query.order_by.iter().map(|(c, _)| c.clone()).collect(),
            child: Box::new(node),
        };
        stages.push(ExecutionStage::Sort);
    }
    if !query.aggregates.is_empty() {
        node = PhysicalPlanNode::Aggregate {
            group_by: query.group_by.clone(),
            aggregate_count: query.aggregates.len(),
            child: Box::new(node),
        };
        stages.push(ExecutionStage::Aggregate);
    }
    if query.having.is_some() {
        node = PhysicalPlanNode::Filter {
            expr: true,
            child: Box::new(node),
        };
        stages.push(ExecutionStage::Having);
    }
    if !query.select.is_empty() && query.select[0] != "*" {
        node = PhysicalPlanNode::Project {
            columns: query.select.clone(),
            child: Box::new(node),
        };
        stages.push(ExecutionStage::Project);
    }
    if let Some(limit) = query.limit {
        node = PhysicalPlanNode::Limit {
            limit,
            child: Box::new(node),
        };
        stages.push(ExecutionStage::Limit);
    }

    let output_columns = if !query.select.is_empty() && query.select[0] != "*" {
        query.select.clone()
    } else {
        schema.columns.iter().map(|c| c.name.clone()).collect()
    };

    Ok(PlannedQuery {
        plan: QueryPlan {
            output_columns,
            index_used,
            estimated_scan_rows,
            has_residual_filter,
        },
        root: node,
        stages,
    })
}

#[cfg(test)]
mod tests {
    use super::{ExecutionStage, build_physical_plan};
    use crate::catalog::schema::{ColumnDef, TableSchema};
    use crate::catalog::types::{ColumnType, Value};
    use crate::query::plan::Query;

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
            .where_(crate::query::plan::col("id").eq(Value::Integer(1)));

        let plan =
            build_physical_plan(&schema(), &query, Some("by_id".into()), 1, false).expect("plan");

        assert!(!plan.stages.contains(&ExecutionStage::Filter));
        assert!(!plan.plan.has_residual_filter);
    }

    #[test]
    fn residual_predicate_keeps_filter_stage() {
        let query = Query::select(&["id"])
            .from("users")
            .where_(crate::query::plan::col("id").eq(Value::Integer(1)));

        let plan =
            build_physical_plan(&schema(), &query, Some("by_id".into()), 1, true).expect("plan");

        assert!(plan.stages.contains(&ExecutionStage::Filter));
        assert!(plan.plan.has_residual_filter);
    }
}
