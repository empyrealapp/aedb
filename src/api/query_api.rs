use crate::catalog::namespace_key;
use crate::catalog::types::Value;
use crate::commit::tx::{ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use crate::commit::validation::{Mutation, TableUpdateExpr};
use crate::error::AedbError;
use crate::query::error::QueryError;
use crate::query::executor::QueryResult;
use crate::query::plan::{ConsistencyMode, Expr, Order, Query, QueryOptions};
use crate::query_authorization::{ensure_external_caller_allowed, ensure_query_caller_allowed};
use crate::query_runtime::{
    QueryExecutionContext, execute_query_against_view, explain_query_against_view,
    resolve_query_table_ref,
};
use crate::{
    AedbInstance, CallerContext, CommitResult, ListPageResult, ListWithTotalRequest,
    LookupThenHydrateRequest, MutateWhereReturningResult, QueryBatchItem, QueryDiagnostics,
    QueryWithDiagnosticsResult, ReadOnlySqlAdapter, ReadTx, SqlTransactionPlan,
    UpdateWhereExprRequest, UpdateWhereRequest,
};
use std::time::Instant;

impl AedbInstance {
    /// Run a query with caller context + options and capture the read-set it
    /// touched. Mirrors [`Self::query_with_options_as`].
    pub async fn query_with_options_capturing_as(
        &self,
        caller: Option<&CallerContext>,
        project_id: &str,
        scope_id: &str,
        query: Query,
        mut options: QueryOptions,
    ) -> Result<(QueryResult, ReadSet), QueryError> {
        if self.require_authenticated_calls && caller.is_none() {
            return Err(QueryError::PermissionDenied {
                permission: "authenticated caller required in secure mode".into(),
                scope: "anonymous".into(),
            });
        }
        if let Some(caller) = caller {
            ensure_query_caller_allowed(caller)?;
        }
        if options.async_index.is_none() {
            options.async_index = query.use_index.clone();
        }
        self.normalize_query_cursor_options(&mut options)?;

        let view = self
            .snapshot_for_consistency(options.consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot = &view.keyspace;
        let catalog = &view.catalog;
        let seq = view.seq;

        let query = crate::lib_helpers::authorize_and_bind_query_for_caller(
            project_id,
            scope_id,
            query,
            &options,
            caller,
            catalog.as_ref(),
        )?;

        let mut collector = crate::query::executor::ReadSetCollector::new();
        let mut result = crate::query::executor::execute_query_with_options_capturing(
            crate::query::executor::CapturingQueryExecutionRequest {
                snapshot: snapshot.as_ref(),
                catalog: catalog.as_ref(),
                project_id,
                scope_id,
                query,
                options: &options,
                snapshot_seq: seq,
                max_scan_rows: self._config.max_scan_rows,
                read_set: Some(&mut collector),
            },
        )?;
        self.sign_query_result_cursor(&mut result)?;
        Ok((result, collector.into_inner()))
    }

    pub(crate) async fn query_unchecked(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryResult, QueryError> {
        self.query_with_options_as(None, project_id, scope_id, query, options)
            .await
    }

    /// Run a query without a caller identity.
    ///
    /// This is unavailable for instances opened with [`Self::open_secure`] or
    /// [`Self::open_production`]. Prefer [`Self::query_with_options_as`] in
    /// services.
    pub async fn query_no_auth(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryResult, QueryError> {
        if self.require_authenticated_calls {
            return Err(QueryError::PermissionDenied {
                permission: "query_no_auth is unavailable in secure mode".into(),
                scope: "anonymous".into(),
            });
        }
        self.query_unchecked(project_id, scope_id, query, options)
            .await
    }

    /// Convenience anonymous query with default [`QueryOptions`].
    ///
    /// Ergonomic entry point for embedding consumers that run un-authenticated
    /// reads (e.g. an app storage layer built on top of AEDB). Delegates to
    /// [`Self::query_no_auth`], so it is likewise unavailable in secure mode.
    /// Prefer [`Self::query_with_options_as`] when a caller identity is
    /// required, or [`Self::begin_read_tx`] for a repeatable point-in-time
    /// snapshot serving many reads.
    pub async fn query(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
    ) -> Result<QueryResult, QueryError> {
        self.query_no_auth(project_id, scope_id, query, QueryOptions::default())
            .await
    }

    /// Convenience anonymous query with explicit [`QueryOptions`]. See
    /// [`Self::query`].
    pub async fn query_with_options(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryResult, QueryError> {
        self.query_no_auth(project_id, scope_id, query, options)
            .await
    }

    pub async fn query_with_options_as(
        &self,
        caller: Option<&CallerContext>,
        project_id: &str,
        scope_id: &str,
        query: Query,
        mut options: QueryOptions,
    ) -> Result<QueryResult, QueryError> {
        if self.require_authenticated_calls && caller.is_none() {
            return Err(QueryError::PermissionDenied {
                permission: "authenticated caller required in secure mode".into(),
                scope: "anonymous".into(),
            });
        }
        if let Some(caller) = caller {
            ensure_query_caller_allowed(caller)?;
        }

        if options.async_index.is_none() {
            options.async_index = query.use_index.clone();
        }

        self.normalize_query_cursor_options(&mut options)?;

        let snapshot_started = Instant::now();
        let view = self
            .snapshot_for_consistency(options.consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot_micros = snapshot_started.elapsed().as_micros() as u64;
        let started = Instant::now();
        let execute_started = Instant::now();
        let table = query.table.clone();
        let result = execute_query_against_view(
            QueryExecutionContext {
                view: &view,
                project_id,
                scope_id,
                options: &options,
                caller,
                max_scan_rows: self._config.max_scan_rows,
                cursor_signing_key: self._config.cursor_signing_key(),
            },
            query,
        );
        tokio::task::yield_now().await;
        let execute_micros = execute_started.elapsed().as_micros() as u64;
        let units = result
            .as_ref()
            .map(|res| res.rows_examined)
            .unwrap_or_default();
        self.maybe_log_read_phase(
            "query_with_options",
            options.consistency,
            snapshot_micros,
            execute_micros,
            units,
            result.is_ok(),
        );
        let mut result = result?;
        self.sign_query_result_cursor(&mut result)?;
        let result = Ok(result);
        self.emit_query_telemetry(started, project_id, scope_id, &table, view.seq, &result);
        result
    }

    pub async fn begin_read_tx(
        &self,
        consistency: ConsistencyMode,
    ) -> Result<ReadTx<'_>, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode; use begin_read_tx_as".into(),
            ));
        }
        let lease = self.acquire_snapshot(consistency).await?;
        Ok(ReadTx {
            db: self,
            lease,
            caller: None,
        })
    }

    pub async fn begin_read_tx_as(
        &self,
        caller: CallerContext,
        consistency: ConsistencyMode,
    ) -> Result<ReadTx<'_>, AedbError> {
        ensure_query_caller_allowed(&caller).map_err(|err| match err {
            QueryError::PermissionDenied { permission, .. } => {
                AedbError::PermissionDenied(permission)
            }
            other => AedbError::Validation(other.to_string()),
        })?;
        let lease = self.acquire_snapshot(consistency).await?;
        Ok(ReadTx {
            db: self,
            lease,
            caller: Some(caller),
        })
    }

    pub async fn query_page_stable(
        &self,
        project_id: &str,
        scope_id: &str,
        mut query: Query,
        cursor: Option<String>,
        page_size: usize,
        consistency: ConsistencyMode,
    ) -> Result<QueryResult, QueryError> {
        if query.joins.is_empty() && query.order_by.is_empty() {
            let (_, catalog, _) = self.executor.snapshot_state().await;
            let (q_project, q_scope, q_table) =
                resolve_query_table_ref(project_id, scope_id, &query.table);
            if let Some(schema) = catalog
                .tables
                .get(&(namespace_key(&q_project, &q_scope), q_table))
            {
                for pk in &schema.primary_key {
                    query = query.order_by(pk, Order::Asc);
                }
            }
        }
        query.limit = Some(page_size.max(1));
        self.query_no_auth(
            project_id,
            scope_id,
            query,
            QueryOptions {
                consistency,
                cursor,
                ..QueryOptions::default()
            },
        )
        .await
    }

    pub async fn query_batch(
        &self,
        project_id: &str,
        scope_id: &str,
        items: Vec<QueryBatchItem>,
        consistency: ConsistencyMode,
    ) -> Result<Vec<QueryResult>, QueryError> {
        let tx = self
            .begin_read_tx(consistency)
            .await
            .map_err(QueryError::from)?;
        tx.query_batch(project_id, scope_id, items).await
    }

    pub async fn query_batch_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        items: Vec<QueryBatchItem>,
        consistency: ConsistencyMode,
    ) -> Result<Vec<QueryResult>, QueryError> {
        let tx = self
            .begin_read_tx_as(caller, consistency)
            .await
            .map_err(QueryError::from)?;
        tx.query_batch(project_id, scope_id, items).await
    }

    pub async fn exists(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        consistency: ConsistencyMode,
    ) -> Result<bool, QueryError> {
        let tx = self
            .begin_read_tx(consistency)
            .await
            .map_err(QueryError::from)?;
        tx.exists(project_id, scope_id, query).await
    }

    pub async fn exists_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        query: Query,
        consistency: ConsistencyMode,
    ) -> Result<bool, QueryError> {
        let tx = self
            .begin_read_tx_as(caller, consistency)
            .await
            .map_err(QueryError::from)?;
        tx.exists(project_id, scope_id, query).await
    }

    pub async fn explain_query(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryDiagnostics, QueryError> {
        self.explain_query_as(None, project_id, scope_id, query, options)
            .await
    }

    pub async fn explain_query_as(
        &self,
        caller: Option<&CallerContext>,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryDiagnostics, QueryError> {
        if self.require_authenticated_calls && caller.is_none() {
            return Err(QueryError::PermissionDenied {
                permission: "authenticated caller required in secure mode".into(),
                scope: "anonymous".into(),
            });
        }

        let mut options = options;
        self.normalize_query_cursor_options(&mut options)?;

        let lease = self
            .acquire_snapshot(options.consistency)
            .await
            .map_err(QueryError::from)?;
        explain_query_against_view(
            &lease.view,
            project_id,
            scope_id,
            query,
            &options,
            caller,
            self._config.max_scan_rows,
        )
    }

    pub async fn query_with_diagnostics(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryWithDiagnosticsResult, QueryError> {
        let diagnostics = self
            .explain_query(project_id, scope_id, query.clone(), options.clone())
            .await?;
        let result = self
            .query_no_auth(project_id, scope_id, query, options)
            .await?;
        Ok(QueryWithDiagnosticsResult {
            result,
            diagnostics,
        })
    }

    pub async fn query_with_diagnostics_as(
        &self,
        caller: &CallerContext,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryWithDiagnosticsResult, QueryError> {
        let diagnostics = self
            .explain_query_as(
                Some(caller),
                project_id,
                scope_id,
                query.clone(),
                options.clone(),
            )
            .await?;
        let result = self
            .query_with_options_as(Some(caller), project_id, scope_id, query, options)
            .await?;
        Ok(QueryWithDiagnosticsResult {
            result,
            diagnostics,
        })
    }

    pub async fn list_with_total_with(
        &self,
        request: ListWithTotalRequest,
    ) -> Result<ListPageResult, QueryError> {
        let tx = self
            .begin_read_tx(request.consistency)
            .await
            .map_err(QueryError::from)?;
        tx.list_with_total(
            &request.project_id,
            &request.scope_id,
            request.query,
            request.cursor,
            request.offset,
            request.page_size,
        )
        .await
    }

    pub async fn list_with_total_as_with(
        &self,
        caller: CallerContext,
        request: ListWithTotalRequest,
    ) -> Result<ListPageResult, QueryError> {
        let tx = self
            .begin_read_tx_as(caller, request.consistency)
            .await
            .map_err(QueryError::from)?;
        tx.list_with_total(
            &request.project_id,
            &request.scope_id,
            request.query,
            request.cursor,
            request.offset,
            request.page_size,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn list_with_total(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        cursor: Option<String>,
        offset: Option<usize>,
        page_size: usize,
        consistency: ConsistencyMode,
    ) -> Result<ListPageResult, QueryError> {
        self.list_with_total_with(ListWithTotalRequest {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            query,
            cursor,
            offset,
            page_size,
            consistency,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn list_with_total_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        query: Query,
        cursor: Option<String>,
        offset: Option<usize>,
        page_size: usize,
        consistency: ConsistencyMode,
    ) -> Result<ListPageResult, QueryError> {
        self.list_with_total_as_with(
            caller,
            ListWithTotalRequest {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                query,
                cursor,
                offset,
                page_size,
                consistency,
            },
        )
        .await
    }

    pub async fn lookup_then_hydrate_with(
        &self,
        request: LookupThenHydrateRequest,
    ) -> Result<(QueryResult, QueryResult), QueryError> {
        let tx = self
            .begin_read_tx(request.consistency)
            .await
            .map_err(QueryError::from)?;
        tx.lookup_then_hydrate(
            &request.project_id,
            &request.scope_id,
            request.source_query,
            request.source_key_index,
            request.hydrate_query,
            &request.hydrate_key_column,
        )
        .await
    }

    pub async fn lookup_then_hydrate_as_with(
        &self,
        caller: CallerContext,
        request: LookupThenHydrateRequest,
    ) -> Result<(QueryResult, QueryResult), QueryError> {
        let tx = self
            .begin_read_tx_as(caller, request.consistency)
            .await
            .map_err(QueryError::from)?;
        tx.lookup_then_hydrate(
            &request.project_id,
            &request.scope_id,
            request.source_query,
            request.source_key_index,
            request.hydrate_query,
            &request.hydrate_key_column,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn lookup_then_hydrate(
        &self,
        project_id: &str,
        scope_id: &str,
        source_query: Query,
        source_key_index: usize,
        hydrate_query: Query,
        hydrate_key_column: &str,
        consistency: ConsistencyMode,
    ) -> Result<(QueryResult, QueryResult), QueryError> {
        self.lookup_then_hydrate_with(LookupThenHydrateRequest {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            source_query,
            source_key_index,
            hydrate_query,
            hydrate_key_column: hydrate_key_column.to_string(),
            consistency,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn lookup_then_hydrate_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        source_query: Query,
        source_key_index: usize,
        hydrate_query: Query,
        hydrate_key_column: &str,
        consistency: ConsistencyMode,
    ) -> Result<(QueryResult, QueryResult), QueryError> {
        self.lookup_then_hydrate_as_with(
            caller,
            LookupThenHydrateRequest {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                source_query,
                source_key_index,
                hydrate_query,
                hydrate_key_column: hydrate_key_column.to_string(),
                consistency,
            },
        )
        .await
    }

    pub async fn query_sql_read_only(
        &self,
        adapter: &dyn ReadOnlySqlAdapter,
        project_id: &str,
        scope_id: &str,
        sql: &str,
        consistency: ConsistencyMode,
    ) -> Result<QueryResult, QueryError> {
        let (query, mut options) = adapter.execute_read_only(project_id, scope_id, sql)?;
        options.consistency = consistency;
        self.query_no_auth(project_id, scope_id, query, options)
            .await
    }

    pub async fn query_sql_read_only_as(
        &self,
        adapter: &dyn ReadOnlySqlAdapter,
        caller: &CallerContext,
        project_id: &str,
        scope_id: &str,
        sql: &str,
        consistency: ConsistencyMode,
    ) -> Result<QueryResult, QueryError> {
        let (query, mut options) = adapter.execute_read_only(project_id, scope_id, sql)?;
        options.consistency = consistency;
        self.query_with_options_as(Some(caller), project_id, scope_id, query, options)
            .await
    }

    pub async fn plan_sql_transaction(
        &self,
        consistency: ConsistencyMode,
        mutations: Vec<Mutation>,
    ) -> Result<SqlTransactionPlan, AedbError> {
        if mutations.is_empty() {
            return Err(AedbError::Validation(
                "sql transaction plan requires at least one mutation".into(),
            ));
        }
        Ok(SqlTransactionPlan {
            base_seq: self.snapshot_probe(consistency).await?,
            caller: None,
            mutations,
        })
    }

    pub async fn plan_sql_transaction_as(
        &self,
        caller: CallerContext,
        consistency: ConsistencyMode,
        mutations: Vec<Mutation>,
    ) -> Result<SqlTransactionPlan, AedbError> {
        if mutations.is_empty() {
            return Err(AedbError::Validation(
                "sql transaction plan requires at least one mutation".into(),
            ));
        }
        ensure_external_caller_allowed(&caller)?;
        Ok(SqlTransactionPlan {
            base_seq: self.snapshot_probe(consistency).await?,
            caller: Some(caller),
            mutations,
        })
    }

    pub async fn commit_sql_transaction_plan(
        &self,
        plan: SqlTransactionPlan,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: plan.caller,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: plan.mutations,
            },
            base_seq: plan.base_seq,
        })
        .await
    }

    pub async fn commit_sql_transaction(
        &self,
        consistency: ConsistencyMode,
        mutations: Vec<Mutation>,
    ) -> Result<CommitResult, AedbError> {
        let plan = self.plan_sql_transaction(consistency, mutations).await?;
        self.commit_sql_transaction_plan(plan).await
    }

    pub async fn commit_sql_transaction_as(
        &self,
        caller: CallerContext,
        consistency: ConsistencyMode,
        mutations: Vec<Mutation>,
    ) -> Result<CommitResult, AedbError> {
        let plan = self
            .plan_sql_transaction_as(caller, consistency, mutations)
            .await?;
        self.commit_sql_transaction_plan(plan).await
    }

    pub async fn commit_many_atomic(
        &self,
        mutations: Vec<Mutation>,
    ) -> Result<CommitResult, AedbError> {
        if mutations.is_empty() {
            return Err(AedbError::Validation(
                "transaction envelope has no mutations".into(),
            ));
        }
        self.commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent { mutations },
            // No read set/assertions in this helper path.
            // Keep hot-path parity with submit/submit_as and avoid snapshot acquisition.
            base_seq: 0,
        })
        .await
    }

    pub async fn commit_many_atomic_as(
        &self,
        caller: CallerContext,
        mutations: Vec<Mutation>,
    ) -> Result<CommitResult, AedbError> {
        if mutations.is_empty() {
            return Err(AedbError::Validation(
                "transaction envelope has no mutations".into(),
            ));
        }
        self.commit_envelope(TransactionEnvelope {
            caller: Some(caller),
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet::default(),
            write_intent: WriteIntent { mutations },
            // No read set/assertions in this helper path.
            // Keep hot-path parity with submit/submit_as and avoid snapshot acquisition.
            base_seq: 0,
        })
        .await
    }

    pub async fn delete_where(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        limit: Option<usize>,
    ) -> Result<Option<CommitResult>, AedbError> {
        let result = self
            .commit(Mutation::DeleteWhere {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                predicate,
                limit,
            })
            .await?;
        Ok(Some(result))
    }

    pub async fn delete_where_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        limit: Option<usize>,
    ) -> Result<Option<CommitResult>, AedbError> {
        let result = self
            .commit_as(
                caller,
                Mutation::DeleteWhere {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    table_name: table_name.to_string(),
                    predicate,
                    limit,
                },
            )
            .await?;
        Ok(Some(result))
    }

    pub async fn update_where(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, Value)>,
        limit: Option<usize>,
    ) -> Result<Option<CommitResult>, AedbError> {
        let result = self
            .commit(Mutation::UpdateWhere {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                predicate,
                updates,
                limit,
            })
            .await?;
        Ok(Some(result))
    }

    pub async fn update_where_as_with(
        &self,
        request: UpdateWhereRequest,
    ) -> Result<Option<CommitResult>, AedbError> {
        let result = self
            .commit_as(
                request.caller,
                Mutation::UpdateWhere {
                    project_id: request.project_id,
                    scope_id: request.scope_id,
                    table_name: request.table_name,
                    predicate: request.predicate,
                    updates: request.updates,
                    limit: request.limit,
                },
            )
            .await?;
        Ok(Some(result))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_where_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, Value)>,
        limit: Option<usize>,
    ) -> Result<Option<CommitResult>, AedbError> {
        self.update_where_as_with(UpdateWhereRequest {
            caller,
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            predicate,
            updates,
            limit,
        })
        .await
    }

    pub async fn update_where_expr(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, TableUpdateExpr)>,
        limit: Option<usize>,
    ) -> Result<Option<CommitResult>, AedbError> {
        let result = self
            .commit(Mutation::UpdateWhereExpr {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                predicate,
                updates,
                limit,
            })
            .await?;
        Ok(Some(result))
    }

    pub async fn update_where_expr_as_with(
        &self,
        request: UpdateWhereExprRequest,
    ) -> Result<Option<CommitResult>, AedbError> {
        let result = self
            .commit_as(
                request.caller,
                Mutation::UpdateWhereExpr {
                    project_id: request.project_id,
                    scope_id: request.scope_id,
                    table_name: request.table_name,
                    predicate: request.predicate,
                    updates: request.updates,
                    limit: request.limit,
                },
            )
            .await?;
        Ok(Some(result))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_where_expr_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, TableUpdateExpr)>,
        limit: Option<usize>,
    ) -> Result<Option<CommitResult>, AedbError> {
        self.update_where_expr_as_with(UpdateWhereExprRequest {
            caller,
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            predicate,
            updates,
            limit,
        })
        .await
    }

    pub async fn mutate_where_returning(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, TableUpdateExpr)>,
    ) -> Result<Option<MutateWhereReturningResult>, AedbError> {
        self.mutate_where_returning_inner(
            None, project_id, scope_id, table_name, predicate, updates,
        )
        .await
    }

    pub async fn mutate_where_returning_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, TableUpdateExpr)>,
    ) -> Result<Option<MutateWhereReturningResult>, AedbError> {
        self.mutate_where_returning_inner(
            Some(caller),
            project_id,
            scope_id,
            table_name,
            predicate,
            updates,
        )
        .await
    }

    pub async fn claim_one(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, TableUpdateExpr)>,
    ) -> Result<Option<MutateWhereReturningResult>, AedbError> {
        self.mutate_where_returning(project_id, scope_id, table_name, predicate, updates)
            .await
    }

    pub async fn claim_one_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: Expr,
        updates: Vec<(String, TableUpdateExpr)>,
    ) -> Result<Option<MutateWhereReturningResult>, AedbError> {
        self.mutate_where_returning_as(caller, project_id, scope_id, table_name, predicate, updates)
            .await
    }
}
