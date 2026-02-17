use super::*;

pub(crate) fn ensure_stable_order_from_catalog(
    project_id: &str,
    scope_id: &str,
    catalog: &crate::catalog::Catalog,
    mut query: Query,
) -> Query {
    if query.joins.is_empty() && query.order_by.is_empty() {
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
    query
}

pub(crate) fn execute_query_against_view(
    view: &SnapshotReadView,
    project_id: &str,
    scope_id: &str,
    mut query: Query,
    options: &QueryOptions,
    caller: Option<&CallerContext>,
    max_scan_rows: usize,
) -> Result<QueryResult, QueryError> {
    let snapshot = &view.keyspace;
    let catalog = &view.catalog;
    let seq = view.seq;

    query = authorize_and_bind_query_for_caller(
        project_id,
        scope_id,
        query,
        options,
        caller,
        catalog.as_ref(),
    )?;

    execute_query_with_options(
        snapshot.as_ref(),
        catalog.as_ref(),
        project_id,
        scope_id,
        query,
        options,
        seq,
        max_scan_rows,
    )
}

pub(crate) fn explain_query_against_view(
    view: &SnapshotReadView,
    project_id: &str,
    scope_id: &str,
    query: Query,
    options: &QueryOptions,
    caller: Option<&CallerContext>,
    max_scan_rows: usize,
) -> Result<QueryDiagnostics, QueryError> {
    let snapshot = &view.keyspace;
    let catalog = &view.catalog;
    let snapshot_seq = view.seq;
    let query = authorize_and_bind_query_for_caller(
        project_id,
        scope_id,
        query,
        options,
        caller,
        catalog.as_ref(),
    )?;
    let index_used = options
        .async_index
        .clone()
        .or_else(|| query.use_index.clone());
    let bounded_by_limit_or_cursor = query.limit.is_some() || options.cursor.is_some();
    if !query.joins.is_empty() {
        let mut estimated = snapshot
            .table(project_id, scope_id, &query.table)
            .map(|t| t.rows.len() as u64)
            .unwrap_or(0);
        for join in &query.joins {
            let (jp, js, jt) = resolve_query_table_ref(project_id, scope_id, &join.table);
            let join_rows = snapshot
                .table(&jp, &js, &jt)
                .map(|t| t.rows.len() as u64)
                .unwrap_or(0);
            estimated = estimated.saturating_mul(join_rows.max(1));
        }
        let mut stages = vec![ExecutionStage::Scan];
        if query.predicate.is_some() {
            stages.push(ExecutionStage::Filter);
        }
        if !query.order_by.is_empty() {
            stages.push(ExecutionStage::Sort);
        }
        if !query.select.is_empty() && query.select[0] != "*" {
            stages.push(ExecutionStage::Project);
        }
        if query.limit.is_some() {
            stages.push(ExecutionStage::Limit);
        }
        return Ok(QueryDiagnostics {
            snapshot_seq,
            estimated_scan_rows: estimated,
            max_scan_rows: max_scan_rows as u64,
            index_used,
            stages,
            bounded_by_limit_or_cursor,
            has_joins: true,
        });
    }

    let (q_project, q_scope, q_table) = resolve_query_table_ref(project_id, scope_id, &query.table);
    let schema = catalog
        .tables
        .get(&(namespace_key(&q_project, &q_scope), q_table.clone()))
        .ok_or_else(|| QueryError::TableNotFound {
            project_id: q_project.clone(),
            table: q_table.clone(),
        })?;
    let estimated_scan_rows = snapshot
        .table(&q_project, &q_scope, &q_table)
        .map(|t| t.rows.len() as u64)
        .unwrap_or(0);
    let planned = build_physical_plan(
        schema,
        &query,
        index_used.clone(),
        estimated_scan_rows,
        query.predicate.is_some(),
    )?;
    Ok(QueryDiagnostics {
        snapshot_seq,
        estimated_scan_rows,
        max_scan_rows: max_scan_rows as u64,
        index_used,
        stages: planned.stages,
        bounded_by_limit_or_cursor,
        has_joins: false,
    })
}

pub(crate) fn authorize_and_bind_query_for_caller(
    project_id: &str,
    scope_id: &str,
    mut query: Query,
    options: &QueryOptions,
    caller: Option<&CallerContext>,
    catalog: &crate::catalog::Catalog,
) -> Result<Query, QueryError> {
    let Some(caller) = caller else {
        return Ok(query);
    };

    ensure_query_caller_allowed(caller)?;
    let (base_project_id, base_scope_id, base_table_name) =
        resolve_query_table_ref(project_id, scope_id, &query.table);
    let required = if let Some(index_name) = &options.async_index {
        Permission::IndexRead {
            project_id: base_project_id.clone(),
            scope_id: base_scope_id.clone(),
            table_name: base_table_name.clone(),
            index_name: index_name.clone(),
        }
    } else {
        Permission::TableRead {
            project_id: base_project_id.clone(),
            scope_id: base_scope_id.clone(),
            table_name: base_table_name.clone(),
        }
    };
    if !catalog.has_permission(&caller.caller_id, &required) {
        return Err(QueryError::PermissionDenied {
            permission: format!("{required:?}"),
            scope: caller.caller_id.clone(),
        });
    }
    for join in &query.joins {
        let (join_project_id, join_scope_id, join_table_name) =
            resolve_query_table_ref(project_id, scope_id, &join.table);
        let join_required = Permission::TableRead {
            project_id: join_project_id,
            scope_id: join_scope_id,
            table_name: join_table_name,
        };
        if !catalog.has_permission(&caller.caller_id, &join_required) {
            return Err(QueryError::PermissionDenied {
                permission: format!("{join_required:?}"),
                scope: caller.caller_id.clone(),
            });
        }
    }
    let mut policies = Vec::new();
    if let Some(policy) =
        catalog.read_policy_for_table(&base_project_id, &base_scope_id, &base_table_name)
    {
        let bound_policy = bind_policy_expr(&policy, &caller.caller_id);
        if query.joins.is_empty() {
            policies.push(bound_policy);
        } else {
            let base_alias = query
                .table_alias
                .clone()
                .unwrap_or_else(|| base_table_name.clone());
            policies.push(qualify_policy_columns(&bound_policy, &base_alias));
        }
    }
    for join in &query.joins {
        let (join_project_id, join_scope_id, join_table_name) =
            resolve_query_table_ref(project_id, scope_id, &join.table);
        if let Some(policy) =
            catalog.read_policy_for_table(&join_project_id, &join_scope_id, &join_table_name)
        {
            let bound_policy = bind_policy_expr(&policy, &caller.caller_id);
            let join_alias = join.alias.clone().unwrap_or(join_table_name);
            policies.push(qualify_policy_columns(&bound_policy, &join_alias));
        }
    }
    for policy in policies {
        query.predicate = Some(match query.predicate.take() {
            Some(existing) => Expr::And(Box::new(existing), Box::new(policy)),
            None => policy,
        });
    }

    Ok(query)
}

pub(crate) fn preflight_reason_to_error(reason: String) -> AedbError {
    if reason == AedbError::Underflow.to_string() {
        return AedbError::Underflow;
    }
    if let Some(detail) = reason.strip_prefix("conflict error: ") {
        return AedbError::Conflict(detail.to_string());
    }
    if let Some(detail) = reason.strip_prefix("validation error: ") {
        return AedbError::Validation(detail.to_string());
    }
    AedbError::Validation(reason)
}

pub(crate) async fn commit_from_preflight_plan(
    db: &AedbInstance,
    caller: Option<CallerContext>,
    plan: crate::commit::tx::PreflightPlan,
) -> Result<CommitResult, AedbError> {
    if !plan.valid {
        let reason = plan
            .errors
            .first()
            .cloned()
            .unwrap_or_else(|| "preflight failed".to_string());
        return Err(preflight_reason_to_error(reason));
    }
    db.commit_envelope(TransactionEnvelope {
        caller,
        idempotency_key: None,
        write_class: crate::commit::tx::WriteClass::Standard,
        assertions: Vec::new(),
        read_set: plan.read_set,
        write_intent: plan.write_intent,
        base_seq: plan.base_seq,
    })
    .await
}

pub(crate) fn seed_system_global_admin(catalog: &mut crate::catalog::Catalog) {
    let mut perms = catalog
        .permissions
        .get(SYSTEM_CALLER_ID)
        .cloned()
        .unwrap_or_else(BTreeSet::new);
    perms.insert(Permission::GlobalAdmin);
    catalog.permissions.insert(SYSTEM_CALLER_ID.into(), perms);
}

pub(crate) fn validate_config(config: &AedbConfig) -> Result<(), AedbError> {
    if config.max_segment_bytes == 0 {
        return Err(AedbError::InvalidConfig {
            message: "max_segment_bytes must be > 0".into(),
        });
    }
    if config.max_segment_age_secs == 0 {
        return Err(AedbError::InvalidConfig {
            message: "max_segment_age_secs must be > 0".into(),
        });
    }
    if config.max_inflight_commits == 0 {
        return Err(AedbError::InvalidConfig {
            message: "max_inflight_commits must be > 0".into(),
        });
    }
    if config.max_commit_queue_bytes == 0 {
        return Err(AedbError::InvalidConfig {
            message: "max_commit_queue_bytes must be > 0".into(),
        });
    }
    if config.max_transaction_bytes == 0
        || config.max_transaction_bytes > config.max_commit_queue_bytes
    {
        return Err(AedbError::InvalidConfig {
            message: "max_transaction_bytes must be > 0 and <= max_commit_queue_bytes".into(),
        });
    }
    if config.max_transaction_bytes > crate::wal::frame::MAX_FRAME_BODY_BYTES {
        return Err(AedbError::InvalidConfig {
            message: format!(
                "max_transaction_bytes must be <= {} to fit WAL frame bounds",
                crate::wal::frame::MAX_FRAME_BODY_BYTES
            ),
        });
    }
    if config.commit_timeout_ms == 0 {
        return Err(AedbError::InvalidConfig {
            message: "commit_timeout_ms must be > 0".into(),
        });
    }
    if config.max_scan_rows == 0 {
        return Err(AedbError::InvalidConfig {
            message: "max_scan_rows must be > 0".into(),
        });
    }
    if config.max_kv_key_bytes == 0 || config.max_kv_value_bytes == 0 {
        return Err(AedbError::InvalidConfig {
            message: "max_kv_key_bytes/max_kv_value_bytes must be > 0".into(),
        });
    }
    if config.prestage_shards == 0 {
        return Err(AedbError::InvalidConfig {
            message: "prestage_shards must be > 0".into(),
        });
    }
    if config.epoch_max_wait_us == 0
        || config.epoch_min_commits == 0
        || config.epoch_max_commits == 0
    {
        return Err(AedbError::InvalidConfig {
            message: "epoch_max_wait_us, epoch_min_commits, and epoch_max_commits must be > 0"
                .into(),
        });
    }
    if config.epoch_min_commits > config.epoch_max_commits {
        return Err(AedbError::InvalidConfig {
            message: "epoch_min_commits must be <= epoch_max_commits".into(),
        });
    }
    if config.adaptive_epoch_enabled {
        if config.adaptive_epoch_min_commits_floor == 0
            || config.adaptive_epoch_min_commits_ceiling == 0
            || config.adaptive_epoch_wait_us_floor == 0
            || config.adaptive_epoch_wait_us_ceiling == 0
            || config.adaptive_epoch_target_latency_us == 0
        {
            return Err(AedbError::InvalidConfig {
                message: "adaptive epoch tuning values must be > 0 when enabled".into(),
            });
        }
        if config.adaptive_epoch_min_commits_floor > config.adaptive_epoch_min_commits_ceiling {
            return Err(AedbError::InvalidConfig {
                message:
                    "adaptive_epoch_min_commits_floor must be <= adaptive_epoch_min_commits_ceiling"
                        .into(),
            });
        }
        if config.adaptive_epoch_wait_us_floor > config.adaptive_epoch_wait_us_ceiling {
            return Err(AedbError::InvalidConfig {
                message: "adaptive_epoch_wait_us_floor must be <= adaptive_epoch_wait_us_ceiling"
                    .into(),
            });
        }
    }
    if config.parallel_apply_enabled && config.parallel_worker_threads == 0 {
        return Err(AedbError::InvalidConfig {
            message: "parallel_worker_threads must be > 0 when parallel_apply_enabled".into(),
        });
    }
    if config.partition_lock_timeout_ms == 0 || config.epoch_apply_timeout_ms == 0 {
        return Err(AedbError::InvalidConfig {
            message: "partition_lock_timeout_ms and epoch_apply_timeout_ms must be > 0".into(),
        });
    }
    if config.max_versions == 0 || config.version_gc_interval_ms == 0 {
        return Err(AedbError::InvalidConfig {
            message: "version store limits must be > 0".into(),
        });
    }
    if config.max_snapshot_age_ms == 0 || config.max_concurrent_snapshots == 0 {
        return Err(AedbError::InvalidConfig {
            message: "snapshot limits must be > 0".into(),
        });
    }
    if matches!(config.durability_mode, DurabilityMode::Batch)
        && (config.batch_interval_ms == 0 || config.batch_max_bytes == 0)
    {
        return Err(AedbError::InvalidConfig {
            message: "batch mode requires batch_interval_ms and batch_max_bytes > 0".into(),
        });
    }
    if matches!(config.durability_mode, DurabilityMode::OsBuffered)
        && matches!(config.recovery_mode, RecoveryMode::Strict)
    {
        return Err(AedbError::InvalidConfig {
            message: "OsBuffered mode is not allowed with strict recovery".into(),
        });
    }
    if !config.hash_chain_required && matches!(config.recovery_mode, RecoveryMode::Strict) {
        return Err(AedbError::InvalidConfig {
            message: "strict recovery requires hash_chain_required=true".into(),
        });
    }
    if !config.coordinator_locking_enabled && matches!(config.recovery_mode, RecoveryMode::Strict) {
        return Err(AedbError::InvalidConfig {
            message: "strict recovery requires coordinator_locking_enabled=true".into(),
        });
    }
    if config.checkpoint_encryption_key.is_none() && config.checkpoint_key_id.is_some() {
        return Err(AedbError::InvalidConfig {
            message: "checkpoint_key_id requires checkpoint_encryption_key".into(),
        });
    }
    if let Some(key) = &config.manifest_hmac_key
        && key.is_empty()
    {
        return Err(AedbError::InvalidConfig {
            message: "manifest_hmac_key must not be empty".into(),
        });
    }
    Ok(())
}

pub(crate) fn validate_secure_config(config: &AedbConfig) -> Result<(), AedbError> {
    validate_config(config)?;
    if config.manifest_hmac_key.is_none() {
        return Err(AedbError::InvalidConfig {
            message: "secure mode requires manifest_hmac_key".into(),
        });
    }
    if !matches!(config.recovery_mode, RecoveryMode::Strict) {
        return Err(AedbError::InvalidConfig {
            message: "secure mode requires strict recovery".into(),
        });
    }
    if matches!(config.durability_mode, DurabilityMode::OsBuffered) {
        return Err(AedbError::InvalidConfig {
            message: "secure mode forbids OsBuffered durability mode".into(),
        });
    }
    if !config.hash_chain_required {
        return Err(AedbError::InvalidConfig {
            message: "secure mode requires hash_chain_required=true".into(),
        });
    }
    Ok(())
}

pub fn validate_arcana_config(config: &AedbConfig) -> Result<(), AedbError> {
    validate_config(config)?;
    if config.manifest_hmac_key.is_none() {
        return Err(AedbError::InvalidConfig {
            message: "manifest_hmac_key is required for Arcana production profile".into(),
        });
    }
    if !matches!(config.recovery_mode, RecoveryMode::Strict) {
        return Err(AedbError::InvalidConfig {
            message: "Arcana production profile requires strict recovery".into(),
        });
    }
    if matches!(config.durability_mode, DurabilityMode::OsBuffered) {
        return Err(AedbError::InvalidConfig {
            message: "Arcana production profile forbids OsBuffered durability mode".into(),
        });
    }
    if !config.hash_chain_required {
        return Err(AedbError::InvalidConfig {
            message: "Arcana production profile requires hash_chain_required=true".into(),
        });
    }
    Ok(())
}

pub(crate) fn parse_cursor_seq(cursor: &str) -> Result<u64, AedbError> {
    if cursor.len() < 2 {
        return Err(AedbError::Decode("invalid cursor".into()));
    }
    let mut bytes = Vec::with_capacity(cursor.len() / 2);
    let chars: Vec<char> = cursor.chars().collect();
    if !chars.len().is_multiple_of(2) {
        return Err(AedbError::Decode("invalid cursor".into()));
    }
    for i in (0..chars.len()).step_by(2) {
        let pair = [chars[i], chars[i + 1]].iter().collect::<String>();
        let b = u8::from_str_radix(&pair, 16)
            .map_err(|_| AedbError::Decode("invalid cursor".into()))?;
        bytes.push(b);
    }
    #[derive(serde::Deserialize)]
    struct CursorSeq {
        snapshot_seq: u64,
    }
    if let Ok(token) = rmp_serde::from_slice::<CursorSeq>(&bytes) {
        return Ok(token.snapshot_seq);
    }

    #[derive(serde::Deserialize)]
    struct CursorToken {
        snapshot_seq: u64,
        last_sort_key: Vec<crate::catalog::types::Value>,
        last_pk: Vec<crate::catalog::types::Value>,
        page_size: usize,
        remaining_limit: Option<usize>,
    }
    let token: CursorToken =
        rmp_serde::from_slice(&bytes).map_err(|_| AedbError::Decode("invalid cursor".into()))?;
    let _ = (
        token.last_sort_key,
        token.last_pk,
        token.page_size,
        token.remaining_limit,
    );
    Ok(token.snapshot_seq)
}

pub(crate) fn should_fallback_to_recovery(err: &AedbError) -> bool {
    match err {
        AedbError::Validation(msg) => {
            msg.contains("garbage collected") || msg.contains("not found in version store")
        }
        _ => false,
    }
}

pub(crate) fn bind_policy_expr(expr: &Expr, caller_id: &str) -> Expr {
    fn bind_value(
        value: &crate::catalog::types::Value,
        caller_id: &str,
    ) -> crate::catalog::types::Value {
        match value {
            crate::catalog::types::Value::Text(text) if text.as_str() == "$caller_id" => {
                crate::catalog::types::Value::Text(caller_id.into())
            }
            _ => value.clone(),
        }
    }
    match expr {
        Expr::Eq(col, v) => Expr::Eq(col.clone(), bind_value(v, caller_id)),
        Expr::Ne(col, v) => Expr::Ne(col.clone(), bind_value(v, caller_id)),
        Expr::Lt(col, v) => Expr::Lt(col.clone(), bind_value(v, caller_id)),
        Expr::Lte(col, v) => Expr::Lte(col.clone(), bind_value(v, caller_id)),
        Expr::Gt(col, v) => Expr::Gt(col.clone(), bind_value(v, caller_id)),
        Expr::Gte(col, v) => Expr::Gte(col.clone(), bind_value(v, caller_id)),
        Expr::In(col, vals) => Expr::In(
            col.clone(),
            vals.iter().map(|v| bind_value(v, caller_id)).collect(),
        ),
        Expr::Between(col, lo, hi) => Expr::Between(
            col.clone(),
            bind_value(lo, caller_id),
            bind_value(hi, caller_id),
        ),
        Expr::IsNull(col) => Expr::IsNull(col.clone()),
        Expr::IsNotNull(col) => Expr::IsNotNull(col.clone()),
        Expr::Like(col, pattern) => Expr::Like(col.clone(), pattern.clone()),
        Expr::And(lhs, rhs) => Expr::And(
            Box::new(bind_policy_expr(lhs, caller_id)),
            Box::new(bind_policy_expr(rhs, caller_id)),
        ),
        Expr::Or(lhs, rhs) => Expr::Or(
            Box::new(bind_policy_expr(lhs, caller_id)),
            Box::new(bind_policy_expr(rhs, caller_id)),
        ),
        Expr::Not(inner) => Expr::Not(Box::new(bind_policy_expr(inner, caller_id))),
    }
}

pub(crate) fn qualify_policy_columns(expr: &Expr, alias: &str) -> Expr {
    fn qualify_column(column: &str, alias: &str) -> String {
        if column.contains('.') {
            column.to_string()
        } else {
            format!("{alias}.{column}")
        }
    }

    match expr {
        Expr::Eq(col, v) => Expr::Eq(qualify_column(col, alias), v.clone()),
        Expr::Ne(col, v) => Expr::Ne(qualify_column(col, alias), v.clone()),
        Expr::Lt(col, v) => Expr::Lt(qualify_column(col, alias), v.clone()),
        Expr::Lte(col, v) => Expr::Lte(qualify_column(col, alias), v.clone()),
        Expr::Gt(col, v) => Expr::Gt(qualify_column(col, alias), v.clone()),
        Expr::Gte(col, v) => Expr::Gte(qualify_column(col, alias), v.clone()),
        Expr::In(col, vals) => Expr::In(qualify_column(col, alias), vals.clone()),
        Expr::Between(col, lo, hi) => {
            Expr::Between(qualify_column(col, alias), lo.clone(), hi.clone())
        }
        Expr::IsNull(col) => Expr::IsNull(qualify_column(col, alias)),
        Expr::IsNotNull(col) => Expr::IsNotNull(qualify_column(col, alias)),
        Expr::Like(col, pattern) => Expr::Like(qualify_column(col, alias), pattern.clone()),
        Expr::And(lhs, rhs) => Expr::And(
            Box::new(qualify_policy_columns(lhs, alias)),
            Box::new(qualify_policy_columns(rhs, alias)),
        ),
        Expr::Or(lhs, rhs) => Expr::Or(
            Box::new(qualify_policy_columns(lhs, alias)),
            Box::new(qualify_policy_columns(rhs, alias)),
        ),
        Expr::Not(inner) => Expr::Not(Box::new(qualify_policy_columns(inner, alias))),
    }
}

pub(crate) fn ensure_external_caller_allowed(caller: &CallerContext) -> Result<(), AedbError> {
    if caller.caller_id == SYSTEM_CALLER_ID && !caller.is_internal_system() {
        return Err(AedbError::PermissionDenied(
            "caller_id 'system' is reserved for internal use".into(),
        ));
    }
    Ok(())
}

pub(crate) fn ensure_query_caller_allowed(caller: &CallerContext) -> Result<(), QueryError> {
    if caller.caller_id == SYSTEM_CALLER_ID && !caller.is_internal_system() {
        return Err(QueryError::PermissionDenied {
            permission: "caller_id 'system' is reserved for internal use".into(),
            scope: caller.caller_id.clone(),
        });
    }
    Ok(())
}

pub(crate) fn extract_primary_key_values(
    schema: &TableSchema,
    row: &Row,
) -> Result<Vec<Value>, AedbError> {
    let mut primary_key = Vec::with_capacity(schema.primary_key.len());
    for pk_name in &schema.primary_key {
        let idx = schema
            .columns
            .iter()
            .position(|c| c.name == *pk_name)
            .ok_or_else(|| {
                AedbError::Validation(format!("primary key column missing: {pk_name}"))
            })?;
        let value = row.values.get(idx).ok_or_else(|| {
            AedbError::Validation(format!(
                "row missing primary key value for column: {pk_name}"
            ))
        })?;
        primary_key.push(value.clone());
    }
    Ok(primary_key)
}

pub(crate) fn apply_table_update_exprs(
    schema: &TableSchema,
    row: &Row,
    updates: &[(String, TableUpdateExpr)],
) -> Result<Row, AedbError> {
    let mut next = row.clone();
    for (target_column, expr) in updates {
        let target_idx = schema
            .columns
            .iter()
            .position(|c| c.name == *target_column)
            .ok_or_else(|| AedbError::UnknownColumn {
                table: schema.table_name.clone(),
                column: target_column.clone(),
            })?;
        let current = next.values.get(target_idx).cloned().ok_or_else(|| {
            AedbError::Validation(format!(
                "target column index out of bounds: {target_column}"
            ))
        })?;
        let value = match expr {
            TableUpdateExpr::Value(value) => value.clone(),
            TableUpdateExpr::CopyColumn(source_column) => {
                let source_idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name == *source_column)
                    .ok_or_else(|| AedbError::UnknownColumn {
                        table: schema.table_name.clone(),
                        column: source_column.clone(),
                    })?;
                next.values.get(source_idx).cloned().ok_or_else(|| {
                    AedbError::Validation(format!(
                        "source column index out of bounds: {source_column}"
                    ))
                })?
            }
            TableUpdateExpr::AddI64(delta) => match current {
                Value::Integer(current) => current
                    .checked_add(*delta)
                    .map(Value::Integer)
                    .ok_or(AedbError::Overflow)?,
                _ => {
                    return Err(AedbError::Validation(format!(
                        "AddI64 requires Integer current value for column {target_column}"
                    )));
                }
            },
            TableUpdateExpr::Coalesce(fallback) => {
                if matches!(current, Value::Null) {
                    fallback.clone()
                } else {
                    current
                }
            }
        };
        next.values[target_idx] = value;
    }
    Ok(next)
}

pub(crate) fn query_error_to_aedb(error: QueryError) -> AedbError {
    match error {
        QueryError::PermissionDenied { permission, scope } => {
            AedbError::PermissionDenied(format!("{permission} (scope={scope})"))
        }
        other => AedbError::Validation(other.to_string()),
    }
}

pub(crate) fn ddl_resource_key(op: &DdlOperation) -> Option<String> {
    match op {
        DdlOperation::CreateProject { project_id, .. } => Some(format!("project:{project_id}")),
        DdlOperation::DropProject { project_id, .. } => Some(format!("project:{project_id}")),
        DdlOperation::CreateScope {
            project_id,
            scope_id,
            ..
        } => Some(format!("scope:{project_id}:{scope_id}")),
        DdlOperation::DropScope {
            project_id,
            scope_id,
            ..
        } => Some(format!("scope:{project_id}:{scope_id}")),
        DdlOperation::CreateTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => Some(format!("table:{project_id}:{scope_id}:{table_name}")),
        DdlOperation::AlterTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => Some(format!("table:{project_id}:{scope_id}:{table_name}")),
        DdlOperation::DropTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => Some(format!("table:{project_id}:{scope_id}:{table_name}")),
        DdlOperation::CreateIndex {
            project_id,
            scope_id,
            table_name,
            index_name,
            ..
        } => Some(format!(
            "index:{project_id}:{scope_id}:{table_name}:{index_name}"
        )),
        DdlOperation::DropIndex {
            project_id,
            scope_id,
            table_name,
            index_name,
            ..
        } => Some(format!(
            "index:{project_id}:{scope_id}:{table_name}:{index_name}"
        )),
        DdlOperation::CreateAsyncIndex {
            project_id,
            scope_id,
            table_name,
            index_name,
            ..
        } => Some(format!(
            "aindex:{project_id}:{scope_id}:{table_name}:{index_name}"
        )),
        DdlOperation::DropAsyncIndex {
            project_id,
            scope_id,
            table_name,
            index_name,
            ..
        } => Some(format!(
            "aindex:{project_id}:{scope_id}:{table_name}:{index_name}"
        )),
        _ => None,
    }
}

pub(crate) fn ddl_dependencies(op: &DdlOperation) -> Vec<String> {
    match op {
        DdlOperation::CreateScope { project_id, .. } => vec![format!("project:{project_id}")],
        DdlOperation::CreateTable {
            project_id,
            scope_id,
            ..
        } => vec![
            format!("project:{project_id}"),
            format!("scope:{project_id}:{scope_id}"),
        ],
        DdlOperation::AlterTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => vec![format!("table:{project_id}:{scope_id}:{table_name}")],
        DdlOperation::CreateIndex {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | DdlOperation::CreateAsyncIndex {
            project_id,
            scope_id,
            table_name,
            ..
        } => vec![format!("table:{project_id}:{scope_id}:{table_name}")],
        DdlOperation::DropProject { project_id, .. } => vec![
            format!("scope_drop_gate:{project_id}"),
            format!("table_drop_gate:{project_id}"),
            format!("index_drop_gate:{project_id}"),
        ],
        DdlOperation::DropScope {
            project_id,
            scope_id,
            ..
        } => vec![
            format!("table_drop_gate:{project_id}:{scope_id}"),
            format!("index_drop_gate:{project_id}:{scope_id}"),
        ],
        DdlOperation::DropTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => vec![format!(
            "index_drop_gate:{project_id}:{scope_id}:{table_name}"
        )],
        _ => Vec::new(),
    }
}

pub(crate) fn ddl_gates_produced(op: &DdlOperation) -> Vec<String> {
    match op {
        DdlOperation::DropScope { project_id, .. } => vec![format!("scope_drop_gate:{project_id}")],
        DdlOperation::DropTable {
            project_id,
            scope_id,
            ..
        } => vec![
            format!("table_drop_gate:{project_id}"),
            format!("table_drop_gate:{project_id}:{scope_id}"),
        ],
        DdlOperation::DropIndex {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | DdlOperation::DropAsyncIndex {
            project_id,
            scope_id,
            table_name,
            ..
        } => vec![
            format!("index_drop_gate:{project_id}"),
            format!("index_drop_gate:{project_id}:{scope_id}"),
            format!("index_drop_gate:{project_id}:{scope_id}:{table_name}"),
        ],
        _ => Vec::new(),
    }
}

pub(crate) fn order_ddl_ops_for_batch(
    ops: Vec<DdlOperation>,
) -> Result<Vec<DdlOperation>, AedbError> {
    if ops.len() <= 1 {
        return Ok(ops);
    }
    let mut providers: HashMap<String, Vec<usize>> = HashMap::new();
    for (idx, op) in ops.iter().enumerate() {
        if let Some(key) = ddl_resource_key(op) {
            providers.entry(key).or_default().push(idx);
        }
        for gate in ddl_gates_produced(op) {
            providers.entry(gate).or_default().push(idx);
        }
    }

    let mut outgoing: Vec<Vec<usize>> = vec![Vec::new(); ops.len()];
    let mut indegree = vec![0usize; ops.len()];
    for (idx, op) in ops.iter().enumerate() {
        for dep in ddl_dependencies(op) {
            if let Some(dep_indices) = providers.get(&dep) {
                for &dep_idx in dep_indices {
                    if dep_idx == idx {
                        continue;
                    }
                    outgoing[dep_idx].push(idx);
                    indegree[idx] += 1;
                }
            }
        }
    }

    let mut ready = std::collections::BTreeSet::new();
    for (idx, degree) in indegree.iter().enumerate() {
        if *degree == 0 {
            ready.insert(idx);
        }
    }

    let mut order = Vec::with_capacity(ops.len());
    while let Some(idx) = ready.pop_first() {
        order.push(idx);
        for &next in &outgoing[idx] {
            indegree[next] -= 1;
            if indegree[next] == 0 {
                ready.insert(next);
            }
        }
    }

    if order.len() != ops.len() {
        return Err(AedbError::InvalidConfig {
            message: "ddl batch has cyclic dependencies".into(),
        });
    }

    Ok(order.into_iter().map(|idx| ops[idx].clone()).collect())
}

pub(crate) fn ddl_would_apply(catalog: &crate::catalog::Catalog, op: &DdlOperation) -> bool {
    match op {
        DdlOperation::CreateProject { project_id, .. } => {
            !catalog.projects.contains_key(project_id)
        }
        DdlOperation::CreateScope {
            project_id,
            scope_id,
            ..
        } => !catalog
            .scopes
            .contains_key(&(project_id.clone(), scope_id.clone())),
        DdlOperation::CreateTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => !catalog
            .tables
            .contains_key(&(namespace_key(project_id, scope_id), table_name.clone())),
        DdlOperation::CreateIndex {
            project_id,
            scope_id,
            table_name,
            index_name,
            ..
        } => !catalog.indexes.contains_key(&(
            namespace_key(project_id, scope_id),
            table_name.clone(),
            index_name.clone(),
        )),
        DdlOperation::CreateAsyncIndex {
            project_id,
            scope_id,
            table_name,
            index_name,
            ..
        } => !catalog.async_indexes.contains_key(&(
            namespace_key(project_id, scope_id),
            table_name.clone(),
            index_name.clone(),
        )),
        DdlOperation::DropProject { project_id, .. } => catalog.projects.contains_key(project_id),
        DdlOperation::DropScope {
            project_id,
            scope_id,
            ..
        } => catalog
            .scopes
            .contains_key(&(project_id.clone(), scope_id.clone())),
        DdlOperation::DropTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => catalog
            .tables
            .contains_key(&(namespace_key(project_id, scope_id), table_name.clone())),
        DdlOperation::DropIndex {
            project_id,
            scope_id,
            table_name,
            index_name,
            ..
        } => catalog.indexes.contains_key(&(
            namespace_key(project_id, scope_id),
            table_name.clone(),
            index_name.clone(),
        )),
        DdlOperation::DropAsyncIndex {
            project_id,
            scope_id,
            table_name,
            index_name,
            ..
        } => catalog.async_indexes.contains_key(&(
            namespace_key(project_id, scope_id),
            table_name.clone(),
            index_name.clone(),
        )),
        _ => true,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LifecycleEventTemplate {
    ProjectCreated {
        project_id: String,
    },
    ProjectDropped {
        project_id: String,
    },
    ScopeCreated {
        project_id: String,
        scope_id: String,
    },
    ScopeDropped {
        project_id: String,
        scope_id: String,
    },
    TableCreated {
        project_id: String,
        scope_id: String,
        table_name: String,
    },
    TableDropped {
        project_id: String,
        scope_id: String,
        table_name: String,
    },
    TableAltered {
        project_id: String,
        scope_id: String,
        table_name: String,
    },
}

impl LifecycleEventTemplate {
    pub(crate) fn with_seq(self, seq: u64) -> LifecycleEvent {
        match self {
            LifecycleEventTemplate::ProjectCreated { project_id } => {
                LifecycleEvent::ProjectCreated { project_id, seq }
            }
            LifecycleEventTemplate::ProjectDropped { project_id } => {
                LifecycleEvent::ProjectDropped { project_id, seq }
            }
            LifecycleEventTemplate::ScopeCreated {
                project_id,
                scope_id,
            } => LifecycleEvent::ScopeCreated {
                project_id,
                scope_id,
                seq,
            },
            LifecycleEventTemplate::ScopeDropped {
                project_id,
                scope_id,
            } => LifecycleEvent::ScopeDropped {
                project_id,
                scope_id,
                seq,
            },
            LifecycleEventTemplate::TableCreated {
                project_id,
                scope_id,
                table_name,
            } => LifecycleEvent::TableCreated {
                project_id,
                scope_id,
                table_name,
                seq,
            },
            LifecycleEventTemplate::TableDropped {
                project_id,
                scope_id,
                table_name,
            } => LifecycleEvent::TableDropped {
                project_id,
                scope_id,
                table_name,
                seq,
            },
            LifecycleEventTemplate::TableAltered {
                project_id,
                scope_id,
                table_name,
            } => LifecycleEvent::TableAltered {
                project_id,
                scope_id,
                table_name,
                seq,
            },
        }
    }
}

pub(crate) fn lifecycle_template_for_ddl(op: &DdlOperation) -> Option<LifecycleEventTemplate> {
    match op {
        DdlOperation::CreateProject { project_id, .. } => {
            Some(LifecycleEventTemplate::ProjectCreated {
                project_id: project_id.clone(),
            })
        }
        DdlOperation::DropProject { project_id, .. } => {
            Some(LifecycleEventTemplate::ProjectDropped {
                project_id: project_id.clone(),
            })
        }
        DdlOperation::CreateScope {
            project_id,
            scope_id,
            ..
        } => Some(LifecycleEventTemplate::ScopeCreated {
            project_id: project_id.clone(),
            scope_id: scope_id.clone(),
        }),
        DdlOperation::DropScope {
            project_id,
            scope_id,
            ..
        } => Some(LifecycleEventTemplate::ScopeDropped {
            project_id: project_id.clone(),
            scope_id: scope_id.clone(),
        }),
        DdlOperation::CreateTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => Some(LifecycleEventTemplate::TableCreated {
            project_id: project_id.clone(),
            scope_id: scope_id.clone(),
            table_name: table_name.clone(),
        }),
        DdlOperation::DropTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => Some(LifecycleEventTemplate::TableDropped {
            project_id: project_id.clone(),
            scope_id: scope_id.clone(),
            table_name: table_name.clone(),
        }),
        DdlOperation::AlterTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => Some(LifecycleEventTemplate::TableAltered {
            project_id: project_id.clone(),
            scope_id: scope_id.clone(),
            table_name: table_name.clone(),
        }),
        _ => None,
    }
}

pub(crate) fn next_prefix_bytes(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bytes = prefix.to_vec();
    for i in (0..bytes.len()).rev() {
        if bytes[i] != u8::MAX {
            bytes[i] += 1;
            bytes.truncate(i + 1);
            return Some(bytes);
        }
    }
    None
}

pub(crate) fn resolve_query_table_ref(
    project_id: &str,
    scope_id: &str,
    table_ref: &str,
) -> (String, String, String) {
    if let Some(name) = table_ref.strip_prefix("_global.") {
        return (
            "_global".to_string(),
            crate::catalog::DEFAULT_SCOPE_ID.to_string(),
            name.to_string(),
        );
    }
    (
        project_id.to_string(),
        scope_id.to_string(),
        table_ref.to_string(),
    )
}

pub(crate) fn read_segments(dir: &Path) -> Result<Vec<SegmentMeta>, AedbError> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("segment_") && name.ends_with(".aedbwal") {
            let seq = name
                .trim_start_matches("segment_")
                .trim_end_matches(".aedbwal")
                .parse::<u64>()
                .map_err(|e| AedbError::Decode(e.to_string()))?;
            let path = entry.path();
            let size_bytes = fs::metadata(&path)?.len();
            out.push(SegmentMeta {
                filename: name,
                segment_seq: seq,
                sha256_hex: sha256_file_hex(&path)?,
                size_bytes,
            });
        }
    }
    out.sort_by_key(|s| s.segment_seq);
    Ok(out)
}

pub(crate) fn segment_seq_from_name(name: &str) -> Option<u64> {
    if !name.starts_with("segment_") || !name.ends_with(".aedbwal") {
        return None;
    }
    let middle = name
        .trim_start_matches("segment_")
        .trim_end_matches(".aedbwal");
    middle.parse::<u64>().ok()
}

pub(crate) fn scan_segment_seq_range(path: &Path) -> Result<Option<(u64, u64)>, AedbError> {
    let file = File::open(path)?;
    if file.metadata()?.len() <= SEGMENT_HEADER_SIZE as u64 {
        return Ok(None);
    }
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut header = [0u8; SEGMENT_HEADER_SIZE];
    reader.read_exact(&mut header)?;
    let mut frame_reader = FrameReader::new(reader);
    let mut min_seq = u64::MAX;
    let mut max_seq = 0u64;
    loop {
        match frame_reader.next_frame() {
            Ok(Some(frame)) => {
                min_seq = min_seq.min(frame.commit_seq);
                max_seq = max_seq.max(frame.commit_seq);
            }
            Ok(None) | Err(FrameError::Truncation) => break,
            Err(FrameError::Corruption) => {
                return Err(AedbError::Validation(
                    "wal frame corruption detected while scanning backup segment".into(),
                ));
            }
            Err(FrameError::Io(e)) => return Err(AedbError::Io(std::io::Error::other(e))),
        }
    }
    if min_seq == u64::MAX {
        return Ok(None);
    }
    Ok(Some((min_seq, max_seq)))
}

pub(crate) fn validate_backup_chain(chain: &[(PathBuf, BackupManifest)]) -> Result<(), AedbError> {
    let Some((_, full)) = chain.first() else {
        return Err(AedbError::Validation("backup chain cannot be empty".into()));
    };
    if full.backup_type != "full" {
        return Err(AedbError::Validation(
            "backup chain must start with a full backup".into(),
        ));
    }
    for idx in 1..chain.len() {
        let prev = &chain[idx - 1].1;
        let cur = &chain[idx].1;
        if cur.backup_type != "incremental" {
            return Err(AedbError::Validation(format!(
                "chain entry {idx} is not incremental"
            )));
        }
        if cur.parent_backup_id.as_deref() != Some(prev.backup_id.as_str()) {
            return Err(AedbError::Validation(format!(
                "chain entry {idx} parent mismatch"
            )));
        }
        let expected_from = prev.wal_head_seq.saturating_add(1);
        if cur.from_seq != Some(expected_from) {
            return Err(AedbError::Validation(format!(
                "chain entry {idx} from_seq mismatch"
            )));
        }
        if cur.wal_head_seq < expected_from.saturating_sub(1) {
            return Err(AedbError::Validation(format!(
                "chain entry {idx} wal_head_seq invalid"
            )));
        }
    }
    Ok(())
}

pub(crate) fn load_verified_backup_chain(
    backup_dirs: &[PathBuf],
    config: &AedbConfig,
) -> Result<Vec<(PathBuf, BackupManifest)>, AedbError> {
    if backup_dirs.is_empty() {
        return Err(AedbError::Validation("backup chain cannot be empty".into()));
    }
    let mut chain = Vec::with_capacity(backup_dirs.len());
    for dir in backup_dirs {
        let manifest = load_backup_manifest(dir, config.hmac_key())?;
        verify_backup_files(dir, &manifest)?;
        chain.push((dir.clone(), manifest));
    }
    validate_backup_chain(&chain)?;
    Ok(chain)
}

pub(crate) fn resolve_target_seq_for_time(
    chain: &[(PathBuf, BackupManifest)],
    target_time_micros: u64,
) -> Result<u64, AedbError> {
    let Some((_, full)) = chain.first() else {
        return Err(AedbError::Validation("backup chain cannot be empty".into()));
    };
    let mut best_seq = full.checkpoint_seq;
    for (dir, manifest) in chain {
        let mut wal_paths = Vec::new();
        for seg in &manifest.wal_segments {
            wal_paths.push(resolve_backup_path(dir, &format!("wal_tail/{seg}"))?);
        }
        wal_paths.sort_by_key(|p| {
            p.file_name()
                .and_then(|n| segment_seq_from_name(&n.to_string_lossy()))
                .unwrap_or(0)
        });
        for wal in wal_paths {
            let file = File::open(&wal)?;
            if file.metadata()?.len() <= SEGMENT_HEADER_SIZE as u64 {
                continue;
            }
            let mut reader = BufReader::with_capacity(64 * 1024, file);
            let mut header = [0u8; SEGMENT_HEADER_SIZE];
            reader.read_exact(&mut header)?;
            let mut frame_reader = FrameReader::new(reader);
            loop {
                match frame_reader.next_frame() {
                    Ok(Some(frame)) => {
                        if frame.commit_seq <= full.checkpoint_seq
                            || frame.commit_seq > manifest.wal_head_seq
                        {
                            continue;
                        }
                        if frame.timestamp_micros <= target_time_micros
                            && frame.commit_seq > best_seq
                        {
                            best_seq = frame.commit_seq;
                        }
                    }
                    Ok(None) | Err(FrameError::Truncation) => break,
                    Err(FrameError::Corruption) => {
                        return Err(AedbError::Validation(
                            "wal frame corruption detected while resolving target time".into(),
                        ));
                    }
                    Err(FrameError::Io(e)) => {
                        return Err(AedbError::Io(std::io::Error::other(e)));
                    }
                }
            }
        }
    }
    Ok(best_seq)
}

pub(crate) fn verify_hash_chain_batch(
    wal_paths: &[PathBuf],
    mut expected_prev_hash: Option<[u8; 32]>,
) -> Result<Option<(u64, [u8; 32])>, AedbError> {
    let mut last: Option<(u64, [u8; 32])> = None;
    for wal in wal_paths {
        let mut file = File::open(wal)?;
        if file.metadata()?.len() < SEGMENT_HEADER_SIZE as u64 {
            return Err(AedbError::Decode("segment too small".into()));
        }
        let mut header = [0u8; SEGMENT_HEADER_SIZE];
        file.read_exact(&mut header)?;
        let parsed = SegmentHeader::from_bytes(&header)
            .map_err(|e| AedbError::Validation(format!("bad segment header: {e}")))?;
        if let Some(expected_prev) = expected_prev_hash
            && parsed.prev_segment_hash != expected_prev
        {
            return Err(AedbError::Validation("segment hash chain mismatch".into()));
        }

        let mut hasher = blake3::Hasher::new();
        hasher.update(&header);
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }
        let hash = *hasher.finalize().as_bytes();
        expected_prev_hash = Some(hash);
        last = Some((parsed.segment_seq, hash));
    }
    Ok(last)
}
