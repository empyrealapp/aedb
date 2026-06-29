use super::indexing::indexed_pks_for_predicate_with_trace;
use super::join;
use super::ordered_scan::{
    OrderedPredicateIndexSelectionRequest, ordered_index_selection_for_query,
    ordered_predicate_index_selection_name_for_query,
};
use super::predicate::extract_primary_key_values;
use crate::catalog::Catalog;
use crate::catalog::namespace_key;
use crate::query::error::QueryError;
use crate::query::plan::{Query, QueryOptions};
use crate::storage::keyspace::KeyspaceSnapshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AccessPathDiagnostics {
    pub selected_indexes: Vec<String>,
    pub predicate_evaluation_path: crate::PredicateEvaluationPath,
    pub plan_trace: Vec<String>,
    pub order_satisfied_by_row_source: bool,
    pub residual_filter_required: bool,
}

impl AccessPathDiagnostics {
    fn new(
        selected_indexes: Vec<String>,
        predicate_evaluation_path: crate::PredicateEvaluationPath,
        plan_trace: Vec<String>,
    ) -> Self {
        Self {
            selected_indexes,
            predicate_evaluation_path,
            plan_trace,
            order_satisfied_by_row_source: false,
            residual_filter_required: false,
        }
    }

    fn with_order_satisfied_by_row_source(mut self) -> Self {
        self.order_satisfied_by_row_source = true;
        self
    }

    fn with_residual_filter_required(mut self) -> Self {
        self.residual_filter_required = true;
        self
    }

    fn with_residual_filter_if(self, required: bool) -> Self {
        if required {
            self.with_residual_filter_required()
        } else {
            self
        }
    }
}

pub(crate) fn explain_access_path_for_query(
    snapshot: &KeyspaceSnapshot,
    catalog: &Catalog,
    project_id: &str,
    scope_id: &str,
    query: &Query,
    options: &QueryOptions,
) -> Result<AccessPathDiagnostics, QueryError> {
    if !query.joins.is_empty() {
        let mut trace = Vec::new();
        trace.push("join query: predicate evaluation happens after join execution".to_string());
        if query.predicate.is_some() {
            trace.push("post-join filter stage evaluates query predicate".to_string());
        }
        let diagnostics = AccessPathDiagnostics::new(
            Vec::new(),
            crate::PredicateEvaluationPath::JoinExecution,
            trace,
        );
        return Ok(diagnostics.with_residual_filter_if(query.predicate.is_some()));
    }

    let mut selected_indexes = Vec::new();
    let mut trace = Vec::new();
    let mut predicate_evaluation_path = crate::PredicateEvaluationPath::None;

    let mut effective_options = options.clone();
    if effective_options.async_index.is_none() {
        effective_options.async_index = query.use_index.clone();
    }

    if let Some(async_index) = &effective_options.async_index {
        selected_indexes.push(async_index.clone());
        trace.push(format!(
            "selected async index projection '{async_index}' as row source"
        ));
        predicate_evaluation_path = crate::PredicateEvaluationPath::AsyncIndexProjection;
        if query.predicate.is_some() {
            trace.push("query predicate is evaluated as filter on projected rows".to_string());
        }
        let diagnostics =
            AccessPathDiagnostics::new(selected_indexes, predicate_evaluation_path, trace);
        return Ok(diagnostics.with_residual_filter_if(query.predicate.is_some()));
    }

    let (exec_project_id, exec_scope_id, exec_table_name) =
        join::resolve_table_ref(project_id, scope_id, &query.table);
    let mut query = query.clone();
    query.table = exec_table_name;
    let table_key = (
        namespace_key(&exec_project_id, &exec_scope_id),
        query.table.clone(),
    );
    let schema = catalog
        .tables
        .get(&table_key)
        .ok_or_else(|| QueryError::TableNotFound {
            project_id: exec_project_id.clone(),
            table: query.table.clone(),
        })?;
    let table = snapshot.table(&exec_project_id, &exec_scope_id, &query.table);

    if let Some(predicate) = query.predicate.as_ref() {
        if let Some(table) = table
            && let Some(index_name) = ordered_predicate_index_selection_name_for_query(
                OrderedPredicateIndexSelectionRequest {
                    catalog,
                    project_id: &exec_project_id,
                    scope_id: &exec_scope_id,
                    schema,
                    query: &query,
                    table,
                    predicate,
                    has_cursor: options.cursor.is_some(),
                },
            )
        {
            selected_indexes.push(index_name.to_string());
            trace.push(format!(
                "selected secondary index '{index_name}' as ordered predicate row source"
            ));
            trace.push("index lookup fully satisfies predicate; no residual filter needed".into());
            return Ok(AccessPathDiagnostics::new(
                selected_indexes,
                crate::PredicateEvaluationPath::SecondaryIndexLookup,
                trace,
            )
            .with_order_satisfied_by_row_source());
        }

        if query.limit != Some(0)
            && query.group_by.is_empty()
            && query.aggregates.is_empty()
            && query.having.is_none()
            && query.order_by.is_empty()
            && options.cursor.is_none()
            && extract_primary_key_values(predicate, &schema.primary_key).is_some()
        {
            trace.push("primary-key equality predicate detected; using direct row lookup".into());
            return Ok(AccessPathDiagnostics::new(
                selected_indexes,
                crate::PredicateEvaluationPath::PrimaryKeyEqLookup,
                trace,
            ));
        }

        if let Some(table) = table
            && let Some(indexed) = indexed_pks_for_predicate_with_trace(
                catalog,
                &exec_project_id,
                &exec_scope_id,
                &query.table,
                table,
                snapshot.kv_segment_store.as_deref(),
                predicate,
            )?
        {
            if !indexed.selected_indexes.is_empty() {
                selected_indexes.extend(indexed.selected_indexes.clone());
                predicate_evaluation_path = crate::PredicateEvaluationPath::SecondaryIndexLookup;
            } else {
                predicate_evaluation_path = crate::PredicateEvaluationPath::FullScanFilter;
            }
            trace.extend(indexed.plan_trace);
            if matches!(
                predicate_evaluation_path,
                crate::PredicateEvaluationPath::FullScanFilter
            ) {
                trace.push(
                    "no matching secondary index; evaluating predicate during table scan"
                        .to_string(),
                );
            } else if indexed.predicate_exact {
                trace.push(
                    "index lookup fully satisfies predicate; no residual filter needed".to_string(),
                );
            } else {
                trace.push(
                    "residual predicate is evaluated on rows returned by index lookup".to_string(),
                );
            }
            let diagnostics =
                AccessPathDiagnostics::new(selected_indexes, predicate_evaluation_path, trace);
            return Ok(if indexed.predicate_exact {
                diagnostics
            } else {
                diagnostics.with_residual_filter_required()
            });
        }

        trace.push("predicate not indexable for current schema/index set".to_string());
        return Ok(AccessPathDiagnostics::new(
            selected_indexes,
            crate::PredicateEvaluationPath::FullScanFilter,
            trace,
        )
        .with_residual_filter_required());
    }

    if let Some(table) = table
        && let Some(selection) = ordered_index_selection_for_query(
            catalog,
            &exec_project_id,
            &exec_scope_id,
            schema,
            &query,
            table,
            options.cursor.is_some(),
        )
    {
        selected_indexes.push(selection.index_name.to_string());
        trace.push(format!(
            "selected secondary index '{}' as ordered row source",
            selection.index_name
        ));
        return Ok(
            AccessPathDiagnostics::new(selected_indexes, predicate_evaluation_path, trace)
                .with_order_satisfied_by_row_source(),
        );
    }

    trace.push("no predicate supplied; full table scan path".to_string());
    Ok(AccessPathDiagnostics::new(
        selected_indexes,
        predicate_evaluation_path,
        trace,
    ))
}
