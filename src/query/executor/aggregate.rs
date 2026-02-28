use crate::query::error::QueryError;
use crate::query::plan::Aggregate;

pub(super) fn aggregate_col_idx(
    agg: &Aggregate,
    columns: &[String],
) -> Result<Option<usize>, QueryError> {
    let column_index = match agg {
        Aggregate::Count => return Ok(None),
        Aggregate::Sum(col) | Aggregate::Min(col) | Aggregate::Max(col) | Aggregate::Avg(col) => {
            columns
                .iter()
                .position(|c| c == col)
                .ok_or_else(|| QueryError::ColumnNotFound {
                    table: "".to_string(),
                    column: col.clone(),
                })?
        }
    };
    Ok(Some(column_index))
}

pub(super) fn aggregate_output_name(agg: &Aggregate) -> String {
    match agg {
        Aggregate::Count => "count_star".to_string(),
        Aggregate::Sum(col) => format!("sum_{col}"),
        Aggregate::Min(col) => format!("min_{col}"),
        Aggregate::Max(col) => format!("max_{col}"),
        Aggregate::Avg(col) => format!("avg_{col}"),
    }
}
