use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::catalog::schema::{IndexType, TableSchema};
use crate::query::error::QueryError;
use crate::query::plan::{Expr, Order, Query};
use crate::storage::encoded_key::EncodedKey;
use crate::storage::keyspace::TableData;
use std::ops::Bound;

use super::index_lookup::{IndexLookup, extract_indexable_predicate};

pub(super) struct OrderedIndexScan {
    pub index_name: String,
    pub pks: Vec<EncodedKey>,
}

pub(super) struct OrderedIndexSelection<'a> {
    pub index_name: &'a str,
}

pub(super) struct OrderedIndexScanRequest<'a> {
    pub catalog: &'a Catalog,
    pub project_id: &'a str,
    pub scope_id: &'a str,
    pub schema: &'a TableSchema,
    pub query: &'a Query,
    pub table: &'a TableData,
    pub segment_store: Option<&'a crate::storage::kv_segment::KvSegmentStore>,
    pub offset: usize,
    pub limit: usize,
    pub has_cursor: bool,
}

pub(super) struct OrderedPredicateIndexScanRequest<'a> {
    pub catalog: &'a Catalog,
    pub project_id: &'a str,
    pub scope_id: &'a str,
    pub schema: &'a TableSchema,
    pub query: &'a Query,
    pub table: &'a TableData,
    pub predicate: &'a Expr,
    pub segment_store: Option<&'a crate::storage::kv_segment::KvSegmentStore>,
    pub offset: usize,
    pub limit: usize,
    pub has_cursor: bool,
}

pub(super) struct OrderedPredicateIndexSelectionRequest<'a> {
    pub catalog: &'a Catalog,
    pub project_id: &'a str,
    pub scope_id: &'a str,
    pub schema: &'a TableSchema,
    pub query: &'a Query,
    pub table: &'a TableData,
    pub predicate: &'a Expr,
    pub has_cursor: bool,
}

struct OrderedIndexSelectionWithOrder<'a> {
    index_name: &'a str,
    order: Order,
}

pub(super) fn ordered_index_scan_for_query(
    request: OrderedIndexScanRequest<'_>,
) -> Result<Option<OrderedIndexScan>, QueryError> {
    let Some(selection) = ordered_index_selection_for_query_with_order(
        request.catalog,
        request.project_id,
        request.scope_id,
        request.schema,
        request.query,
        request.table,
        request.has_cursor,
    ) else {
        return Ok(None);
    };
    let Some(secondary_index) = request.table.indexes.get(selection.index_name) else {
        return Ok(None);
    };
    Ok(Some(OrderedIndexScan {
        index_name: selection.index_name.to_string(),
        pks: secondary_index.tier_scan_prefix_window_ordered(
            None,
            request.offset,
            request.limit,
            matches!(selection.order, Order::Desc),
            request.segment_store,
        )?,
    }))
}

pub(super) fn ordered_index_selection_for_query<'a>(
    catalog: &'a Catalog,
    project_id: &str,
    scope_id: &str,
    schema: &TableSchema,
    query: &Query,
    table: &'a TableData,
    has_cursor: bool,
) -> Option<OrderedIndexSelection<'a>> {
    let selection = ordered_index_selection_for_query_with_order(
        catalog, project_id, scope_id, schema, query, table, has_cursor,
    )?;
    Some(OrderedIndexSelection {
        index_name: selection.index_name,
    })
}

pub(super) fn ordered_predicate_index_scan_for_query(
    request: OrderedPredicateIndexScanRequest<'_>,
) -> Result<Option<OrderedIndexScan>, QueryError> {
    let Some(selection) =
        ordered_predicate_index_selection_for_query(OrderedPredicateIndexSelectionRequest {
            catalog: request.catalog,
            project_id: request.project_id,
            scope_id: request.scope_id,
            schema: request.schema,
            query: request.query,
            table: request.table,
            predicate: request.predicate,
            has_cursor: request.has_cursor,
        })
    else {
        return Ok(None);
    };
    let Some(secondary_index) = request.table.indexes.get(selection.index_name) else {
        return Ok(None);
    };
    let reverse = matches!(selection.order, Order::Desc);
    let store = request.segment_store;
    let pks = match selection.lookup {
        OrderedPredicateLookup::Eq(key) => secondary_index.tier_scan_range_window_ordered(
            Bound::Included(key.clone()),
            Bound::Included(key),
            request.offset,
            request.limit,
            reverse,
            store,
        )?,
        OrderedPredicateLookup::Range(start, end) => secondary_index
            .tier_scan_range_window_ordered(
                start,
                end,
                request.offset,
                request.limit,
                reverse,
                store,
            )?,
    };
    Ok(Some(OrderedIndexScan {
        index_name: selection.index_name.to_string(),
        pks,
    }))
}

pub(super) fn ordered_predicate_index_selection_name_for_query<'a>(
    request: OrderedPredicateIndexSelectionRequest<'a>,
) -> Option<&'a str> {
    ordered_predicate_index_selection_for_query(request).map(|selection| selection.index_name)
}

struct OrderedPredicateIndexSelection<'a> {
    index_name: &'a str,
    order: Order,
    lookup: OrderedPredicateLookup,
}

enum OrderedPredicateLookup {
    Eq(EncodedKey),
    Range(Bound<EncodedKey>, Bound<EncodedKey>),
}

fn ordered_predicate_index_selection_for_query<'a>(
    request: OrderedPredicateIndexSelectionRequest<'a>,
) -> Option<OrderedPredicateIndexSelection<'a>> {
    let query = request.query;
    if !query_can_use_ordered_predicate_index_scan(query, request.has_cursor) {
        return None;
    }
    let (order_col, order) = query.order_by.first()?;
    if !order_column_is_non_nullable(request.schema, order_col) {
        return None;
    }
    let lookup = extract_indexable_predicate(request.predicate)?;
    if !lookup.predicate_exact() {
        return None;
    }
    let lookup = match lookup {
        IndexLookup::Eq { column, value, .. } if column == order_col => {
            OrderedPredicateLookup::Eq(EncodedKey::from_values(std::slice::from_ref(value)))
        }
        IndexLookup::Range { column, bounds, .. } if column == order_col => {
            OrderedPredicateLookup::Range(bounds.0, bounds.1)
        }
        _ => return None,
    };
    let index_name = ordered_index_name_for_column(
        request.catalog,
        request.project_id,
        request.scope_id,
        &query.table,
        request.table,
        order_col,
    )?;
    Some(OrderedPredicateIndexSelection {
        index_name,
        order: *order,
        lookup,
    })
}

fn ordered_index_selection_for_query_with_order<'a>(
    catalog: &'a Catalog,
    project_id: &str,
    scope_id: &str,
    schema: &TableSchema,
    query: &Query,
    table: &'a TableData,
    has_cursor: bool,
) -> Option<OrderedIndexSelectionWithOrder<'a>> {
    if !query_can_use_ordered_index_scan(query, has_cursor) {
        return None;
    }

    let (order_col, order) = query.order_by.first()?;
    if !order_column_is_non_nullable(schema, order_col) {
        return None;
    }
    let index_name = ordered_index_name_for_column(
        catalog,
        project_id,
        scope_id,
        &query.table,
        table,
        order_col,
    )?;
    Some(OrderedIndexSelectionWithOrder {
        index_name,
        order: *order,
    })
}

fn query_can_use_ordered_predicate_index_scan(query: &Query, has_cursor: bool) -> bool {
    !has_cursor
        && query.predicate.is_some()
        && query.limit.is_some()
        && query.group_by.is_empty()
        && query.aggregates.is_empty()
        && query.having.is_none()
        && query.order_by.len() == 1
}

fn order_column_is_non_nullable(schema: &TableSchema, order_col: &str) -> bool {
    schema
        .columns
        .iter()
        .find(|column| column.name == order_col)
        .is_some_and(|column| !column.nullable)
}

fn query_can_use_ordered_index_scan(query: &Query, has_cursor: bool) -> bool {
    !has_cursor
        && query.predicate.is_none()
        && query.limit.is_some()
        && query.group_by.is_empty()
        && query.aggregates.is_empty()
        && query.having.is_none()
        && query.order_by.len() == 1
}

fn ordered_index_name_for_column<'a>(
    catalog: &'a Catalog,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    table: &TableData,
    order_col: &str,
) -> Option<&'a str> {
    let ns = namespace_key(project_id, scope_id);
    catalog
        .indexes
        .iter()
        .find_map(|((p, t, index_name), index_def)| {
            (p == &ns
                && t == table_name
                && index_def.partial_filter.is_none()
                && matches!(index_def.index_type, IndexType::BTree | IndexType::Art)
                && index_def.columns.as_slice() == [order_col]
                && table.indexes.contains_key(index_name))
            .then_some(index_name.as_str())
        })
}
