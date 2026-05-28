use super::QueryResult;
use super::cursor::CursorToken;
use super::predicate::extract_primary_key_values;
use super::read_set::ReadSetCollector;
use super::validate;
use crate::catalog::schema::TableSchema;
use crate::catalog::types::Row;
use crate::query::error::QueryError;
use crate::query::plan::Query;
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::{KeyspaceSnapshot, TableData};

pub(super) struct PrimaryKeyPointQueryRequest<'a, 'r> {
    pub snapshot: &'a KeyspaceSnapshot,
    pub schema: &'a TableSchema,
    pub table: Option<&'a TableData>,
    pub project_id: &'a str,
    pub scope_id: &'a str,
    pub query: &'a Query,
    pub cursor_state: &'a Option<CursorToken>,
    pub snapshot_seq: u64,
    pub read_set: Option<&'r mut ReadSetCollector>,
}

pub(super) fn try_primary_key_point_query(
    request: PrimaryKeyPointQueryRequest<'_, '_>,
) -> Result<Option<QueryResult>, QueryError> {
    if request.cursor_state.is_some()
        || request.query.predicate.is_none()
        || request.query.offset.unwrap_or(0) > 0
        || !request.query.group_by.is_empty()
        || !request.query.aggregates.is_empty()
        || request.query.having.is_some()
        || !request.query.order_by.is_empty()
    {
        return Ok(None);
    }
    if request.query.limit == Some(0) {
        return Ok(Some(QueryResult {
            rows: Vec::new(),
            rows_examined: 0,
            cursor: None,
            truncated: false,
            snapshot_seq: request.snapshot_seq,
            materialized_seq: None,
            split_recommended: false,
        }));
    }

    let Some(predicate) = request.query.predicate.as_ref() else {
        return Ok(None);
    };
    validate::validate_expr_types(request.schema, predicate)?;
    let Some(primary_key) = extract_primary_key_values(predicate, &request.schema.primary_key)
    else {
        return Ok(None);
    };

    if let Some(collector) = request.read_set {
        collector.record_point(
            request.snapshot,
            request.project_id,
            request.scope_id,
            &request.query.table,
            primary_key.clone(),
        );
    }

    let selected_indices = resolve_selected_indices(request.schema, request.query)?;
    let encoded_pk = EncodedKey::from_values(&primary_key);
    let maybe_row = request.table.and_then(|t| t.rows.get(&encoded_pk));
    let rows = match maybe_row {
        Some(row) => vec![project_selected_row(row, selected_indices.as_deref())],
        None => Vec::new(),
    };

    Ok(Some(QueryResult {
        rows,
        rows_examined: 1,
        cursor: None,
        truncated: false,
        snapshot_seq: request.snapshot_seq,
        materialized_seq: None,
        split_recommended: false,
    }))
}

fn resolve_selected_indices(
    schema: &TableSchema,
    query: &Query,
) -> Result<Option<Vec<usize>>, QueryError> {
    if query.select.len() == 1 && query.select[0] == "*" {
        return Ok(None);
    }
    let mut indices = Vec::with_capacity(query.select.len());
    for col in &query.select {
        let column_index = schema
            .columns
            .iter()
            .position(|c| c.name == *col)
            .ok_or_else(|| QueryError::ColumnNotFound {
                table: query.table.clone(),
                column: col.clone(),
            })?;
        indices.push(column_index);
    }
    Ok(Some(indices))
}

fn project_selected_row(row: &Row, selected_indices: Option<&[usize]>) -> Row {
    match selected_indices {
        Some(indices) => Row {
            values: indices.iter().map(|idx| row.values[*idx].clone()).collect(),
        },
        None => row.clone(),
    }
}
