use crate::catalog::types::Row;
use crate::query::error::QueryError;
use crate::query::operators::{
    AggregateOperator, FilterOperator, LimitOperator, Operator, ScanOperator, SortOperator,
    compile_expr,
};
use crate::query::plan::Query;
use crate::query::planner::{ExecutionStage, PlannedQuery};

use super::aggregate::{aggregate_col_idx, aggregate_output_name};

pub(super) struct OperatorPipeline<'a> {
    pub root: Box<dyn Operator + Send + 'a>,
    pub selected_indices: Option<Vec<usize>>,
    pub row_columns: Vec<String>,
}

pub(super) struct OperatorPipelineRequest<'a> {
    pub row_source: Box<dyn Iterator<Item = Row> + Send + 'a>,
    pub physical_plan: &'a PlannedQuery,
    pub query: &'a Query,
    pub columns: Vec<String>,
    pub row_source_satisfies_order: bool,
    pub cursor_absent: bool,
    pub row_source_window_limit: usize,
}

pub(super) fn build_operator_pipeline(
    request: OperatorPipelineRequest<'_>,
) -> Result<OperatorPipeline<'_>, QueryError> {
    let OperatorPipelineRequest {
        row_source,
        physical_plan,
        query,
        columns,
        row_source_satisfies_order,
        cursor_absent,
        row_source_window_limit,
    } = request;
    let mut root: Box<dyn Operator + Send + '_> = Box::new(ScanOperator::new(row_source));
    let mut selected_indices: Option<Vec<usize>> = None;
    let mut row_columns = columns.clone();
    for stage in &physical_plan.stages {
        match stage {
            ExecutionStage::Scan => {}
            ExecutionStage::Limit => {
                // DISTINCT deduplicates before applying limit, so the stream must
                // not be limited here.
                if cursor_absent
                    && !query.distinct
                    && query.order_by.is_empty()
                    && query.aggregates.is_empty()
                    && query.having.is_none()
                {
                    root = Box::new(LimitOperator::new(root, row_source_window_limit));
                }
            }
            ExecutionStage::Filter => {
                if let Some(predicate) = query.predicate.as_ref() {
                    let compiled = compile_expr(predicate, &columns, &query.table)?;
                    root = Box::new(FilterOperator::new(root, compiled));
                }
            }
            ExecutionStage::Sort => {
                if row_source_satisfies_order {
                    continue;
                }
                let order_by = query
                    .order_by
                    .iter()
                    .map(|(order_col, order)| {
                        row_columns
                            .iter()
                            .position(|c| c == order_col)
                            .map(|idx| (idx, *order))
                            .ok_or_else(|| QueryError::ColumnNotFound {
                                table: query.table.clone(),
                                column: order_col.clone(),
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                // DISTINCT needs every row before dedup, so no top-k truncation.
                let top_k_limit = if cursor_absent && !query.distinct {
                    Some(row_source_window_limit)
                } else {
                    None
                };
                root = Box::new(SortOperator::new_with_limit(root, order_by, top_k_limit));
            }
            ExecutionStage::Aggregate => {
                let group_by_idx = query
                    .group_by
                    .iter()
                    .map(|name| {
                        columns.iter().position(|c| c == name).ok_or_else(|| {
                            QueryError::ColumnNotFound {
                                table: query.table.clone(),
                                column: name.clone(),
                            }
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let agg_col_idx = query
                    .aggregates
                    .iter()
                    .map(|agg| aggregate_col_idx(agg, &columns))
                    .collect::<Result<Vec<_>, _>>()?;
                root = Box::new(AggregateOperator::new(
                    root,
                    query.aggregates.clone(),
                    group_by_idx,
                    agg_col_idx,
                ));
                row_columns = query.group_by.clone();
                row_columns.extend(query.aggregates.iter().map(aggregate_output_name));
            }
            ExecutionStage::Having => {
                if query.aggregates.is_empty() {
                    return Err(QueryError::InvalidQuery {
                        reason: "having requires aggregate or group_by".into(),
                    });
                }
                if let Some(having) = query.having.as_ref() {
                    let compiled = compile_expr(having, &row_columns, &query.table)?;
                    root = Box::new(FilterOperator::new(root, compiled));
                }
            }
            ExecutionStage::Project => {
                selected_indices = Some(
                    query
                        .select
                        .iter()
                        .map(|col| {
                            row_columns.iter().position(|c| c == col).ok_or_else(|| {
                                QueryError::ColumnNotFound {
                                    table: query.table.clone(),
                                    column: col.clone(),
                                }
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
        }
    }
    Ok(OperatorPipeline {
        root,
        selected_indices,
        row_columns,
    })
}
