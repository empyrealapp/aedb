use crate::catalog::types::{Row, Value};
use crate::commit::executor::CommitResult;
use crate::commit::tx::{
    ReadAssertion, ReadKey, ReadSet, ReadSetEntry, TransactionEnvelope, WriteClass, WriteIntent,
};
use crate::commit::validation::{Mutation, validate_mutation_with_config};
use crate::error::AedbError;
use crate::order_book::{
    ExecInstruction, ExecutionReport, FillRecord, FillSpec, InstrumentConfig, OrderBookDepth,
    OrderBookTableMode, OrderRecord, OrderRequest, OrderSide, OrderStatus, OrderType, Spread,
    TimeInForce, key_client_id, key_order, read_last_execution_report, read_open_orders,
    read_order_status, read_recent_trades, read_spread, read_top_n, scoped_instrument,
    u256_from_be,
};
use crate::permission::{CallerContext, Permission};
use crate::preflight::PreflightResult;
use crate::query::error::QueryError;
use crate::query::plan::ConsistencyMode;
use crate::query_authorization::ensure_query_caller_allowed;
use crate::{AedbInstance, CommitFinality, CompareAndSwapRequest};
use std::sync::atomic::Ordering;

impl AedbInstance {
    pub async fn order_book_new(
        &self,
        project_id: &str,
        scope_id: &str,
        request: OrderRequest,
    ) -> Result<CommitResult, AedbError> {
        self.preflight_order_book_new_if_high_reject_risk(None, project_id, scope_id, &request)
            .await?;
        let mutation = Mutation::OrderBookNew {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            request,
        };
        let (_, catalog, _) = self.executor.snapshot_state().await;
        validate_mutation_with_config(&catalog, &mutation, &self._config)?;
        self.commit_prevalidated_internal("order_book_new", mutation)
            .await
    }

    pub async fn order_book_new_with_finality(
        &self,
        project_id: &str,
        scope_id: &str,
        request: OrderRequest,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let mut result = self.order_book_new(project_id, scope_id, request).await?;
        self.enforce_finality(&mut result, finality).await?;
        Ok(result)
    }

    pub async fn order_book_define_table(
        &self,
        project_id: &str,
        scope_id: &str,
        table_id: &str,
        mode: OrderBookTableMode,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_define_table",
            Mutation::OrderBookDefineTable {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_id: table_id.to_string(),
                mode,
            },
        )
        .await
    }

    pub async fn order_book_define_table_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_id: &str,
        mode: OrderBookTableMode,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookDefineTable {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_id: table_id.to_string(),
                mode,
            },
        )
        .await
    }

    pub async fn order_book_drop_table(
        &self,
        project_id: &str,
        scope_id: &str,
        table_id: &str,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_drop_table",
            Mutation::OrderBookDropTable {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_id: table_id.to_string(),
            },
        )
        .await
    }

    pub async fn order_book_drop_table_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_id: &str,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookDropTable {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_id: table_id.to_string(),
            },
        )
        .await
    }

    pub async fn order_book_set_instrument_config(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        config: InstrumentConfig,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_set_instrument_config",
            Mutation::OrderBookSetInstrumentConfig {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                config,
            },
        )
        .await
    }

    pub async fn order_book_set_instrument_config_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        config: InstrumentConfig,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookSetInstrumentConfig {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                config,
            },
        )
        .await
    }

    pub async fn order_book_set_instrument_halted(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        halted: bool,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_set_instrument_halted",
            Mutation::OrderBookSetInstrumentHalted {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                halted,
            },
        )
        .await
    }

    pub async fn order_book_set_instrument_halted_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        halted: bool,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookSetInstrumentHalted {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                halted,
            },
        )
        .await
    }

    pub async fn order_book_new_in_table(
        &self,
        project_id: &str,
        scope_id: &str,
        table_id: &str,
        asset_id: &str,
        mut request: OrderRequest,
    ) -> Result<CommitResult, AedbError> {
        request.instrument = scoped_instrument(table_id, asset_id);
        self.order_book_new(project_id, scope_id, request).await
    }

    pub async fn order_book_new_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        request: OrderRequest,
    ) -> Result<CommitResult, AedbError> {
        self.preflight_order_book_new_if_high_reject_risk(
            Some(&caller),
            project_id,
            scope_id,
            &request,
        )
        .await?;
        self.commit_as(
            caller,
            Mutation::OrderBookNew {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                request,
            },
        )
        .await
    }

    pub async fn order_book_new_as_with_finality(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        request: OrderRequest,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let mut result = self
            .order_book_new_as(caller, project_id, scope_id, request)
            .await?;
        self.enforce_finality(&mut result, finality).await?;
        Ok(result)
    }

    fn should_preflight_order_book_new(request: &OrderRequest) -> bool {
        request.exec_instructions.post_only()
            || matches!(request.time_in_force, TimeInForce::Fok)
            || matches!(request.order_type, OrderType::Market)
    }

    async fn preflight_order_book_new_if_high_reject_risk(
        &self,
        caller: Option<&CallerContext>,
        project_id: &str,
        scope_id: &str,
        request: &OrderRequest,
    ) -> Result<(), AedbError> {
        if !Self::should_preflight_order_book_new(request) {
            return Ok(());
        }
        let mutation = Mutation::OrderBookNew {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            request: request.clone(),
        };
        let preflight_result = if let Some(caller) = caller {
            self.preflight_as(caller, mutation).await?
        } else {
            self.preflight(mutation).await
        };
        if let PreflightResult::Err { reason } = preflight_result {
            self.upstream_validation_rejections
                .fetch_add(1, Ordering::Relaxed);
            return Err(AedbError::Validation(reason));
        }
        Ok(())
    }

    pub async fn order_book_cancel(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_cancel",
            Mutation::OrderBookCancel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id,
                client_order_id: None,
                owner: owner.to_string(),
            },
        )
        .await
    }

    pub async fn order_book_cancel_with_finality(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal_with_finality(
            "order_book_cancel",
            Mutation::OrderBookCancel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id,
                client_order_id: None,
                owner: owner.to_string(),
            },
            finality,
        )
        .await
    }

    pub async fn order_book_cancel_strict(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.order_book_cancel_strict_as_internal(
            None, project_id, scope_id, instrument, order_id, owner, finality,
        )
        .await
    }

    pub async fn order_book_cancel_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookCancel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id,
                client_order_id: None,
                owner: owner.to_string(),
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_cancel_as_with_finality(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let mut result = self
            .order_book_cancel_as(caller, project_id, scope_id, instrument, order_id, owner)
            .await?;
        self.enforce_finality(&mut result, finality).await?;
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_cancel_strict_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.order_book_cancel_strict_as_internal(
            Some(caller),
            project_id,
            scope_id,
            instrument,
            order_id,
            owner,
            finality,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn order_book_cancel_strict_as_internal(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let order_key = key_order(instrument, order_id);
        let Some(entry) = lease
            .view
            .keyspace
            .try_kv_get(project_id, scope_id, &order_key)?
        else {
            return Err(AedbError::Validation(format!(
                "strict cancel target not found: order_id={order_id}"
            )));
        };
        let order: OrderRecord =
            rmp_serde::from_slice(&entry.value).map_err(|e| AedbError::Decode(e.to_string()))?;
        if order.owner != owner {
            return Err(AedbError::PermissionDenied(
                "order ownership mismatch".into(),
            ));
        }
        if !matches!(
            order.status,
            OrderStatus::Open | OrderStatus::PartiallyFilled
        ) || u256_from_be(order.remaining_qty_be).is_zero()
        {
            return Err(AedbError::Validation(format!(
                "order not cancellable in current status: {:?}",
                order.status
            )));
        }
        let envelope = TransactionEnvelope {
            caller,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet {
                points: vec![ReadSetEntry {
                    key: ReadKey::KvKey {
                        project_id: project_id.to_string(),
                        scope_id: scope_id.to_string(),
                        key: order_key,
                    },
                    version_at_read: entry.version,
                }],
                ranges: Vec::new(),
            },
            write_intent: WriteIntent {
                mutations: vec![Mutation::OrderBookCancel {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    instrument: instrument.to_string(),
                    order_id,
                    client_order_id: None,
                    owner: owner.to_string(),
                }],
            },
            base_seq: lease.view.seq,
        };
        self.commit_envelope_with_finality(envelope, finality).await
    }

    pub async fn order_book_cancel_by_client_id(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        client_order_id: &str,
        owner: &str,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_cancel_by_client_id",
            Mutation::OrderBookCancel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id: 0,
                client_order_id: Some(client_order_id.to_string()),
                owner: owner.to_string(),
            },
        )
        .await
    }

    pub async fn order_book_cancel_by_client_id_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        client_order_id: &str,
        owner: &str,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookCancel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id: 0,
                client_order_id: Some(client_order_id.to_string()),
                owner: owner.to_string(),
            },
        )
        .await
    }

    pub async fn order_book_cancel_by_client_id_strict(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        client_order_id: &str,
        owner: &str,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.order_book_cancel_by_client_id_strict_as_internal(
            None,
            project_id,
            scope_id,
            instrument,
            client_order_id,
            owner,
            finality,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_cancel_by_client_id_strict_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        client_order_id: &str,
        owner: &str,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.order_book_cancel_by_client_id_strict_as_internal(
            Some(caller),
            project_id,
            scope_id,
            instrument,
            client_order_id,
            owner,
            finality,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn order_book_cancel_by_client_id_strict_as_internal(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        client_order_id: &str,
        owner: &str,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let cid_key = key_client_id(instrument, owner, client_order_id);
        let Some(cid_entry) = lease
            .view
            .keyspace
            .try_kv_get(project_id, scope_id, &cid_key)?
        else {
            return Err(AedbError::Validation(format!(
                "strict cancel target not found: client_order_id={client_order_id}"
            )));
        };
        if cid_entry.value.len() != 8 {
            return Err(AedbError::Validation(
                "invalid client-order mapping encoding".into(),
            ));
        }
        let mut id_bytes = [0u8; 8];
        id_bytes.copy_from_slice(&cid_entry.value);
        let order_id = u64::from_be_bytes(id_bytes);
        let order_key = key_order(instrument, order_id);
        let Some(order_entry) = lease
            .view
            .keyspace
            .try_kv_get(project_id, scope_id, &order_key)?
        else {
            return Err(AedbError::Validation(format!(
                "strict cancel target not found: order_id={order_id}"
            )));
        };
        let order: OrderRecord = rmp_serde::from_slice(&order_entry.value)
            .map_err(|e| AedbError::Decode(e.to_string()))?;
        if order.owner != owner {
            return Err(AedbError::PermissionDenied(
                "order ownership mismatch".into(),
            ));
        }
        if !matches!(
            order.status,
            OrderStatus::Open | OrderStatus::PartiallyFilled
        ) || u256_from_be(order.remaining_qty_be).is_zero()
        {
            return Err(AedbError::Validation(format!(
                "order not cancellable in current status: {:?}",
                order.status
            )));
        }
        let envelope = TransactionEnvelope {
            caller,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet {
                points: vec![
                    ReadSetEntry {
                        key: ReadKey::KvKey {
                            project_id: project_id.to_string(),
                            scope_id: scope_id.to_string(),
                            key: cid_key,
                        },
                        version_at_read: cid_entry.version,
                    },
                    ReadSetEntry {
                        key: ReadKey::KvKey {
                            project_id: project_id.to_string(),
                            scope_id: scope_id.to_string(),
                            key: order_key,
                        },
                        version_at_read: order_entry.version,
                    },
                ],
                ranges: Vec::new(),
            },
            write_intent: WriteIntent {
                mutations: vec![Mutation::OrderBookCancel {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    instrument: instrument.to_string(),
                    order_id: 0,
                    client_order_id: Some(client_order_id.to_string()),
                    owner: owner.to_string(),
                }],
            },
            base_seq: lease.view.seq,
        };
        self.commit_envelope_with_finality(envelope, finality).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_cancel_replace_strict(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        new_price_ticks: Option<i64>,
        new_qty_be: Option<[u8; 32]>,
        new_time_in_force: Option<TimeInForce>,
        new_exec_instructions: Option<ExecInstruction>,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.order_book_cancel_replace_strict_as_internal(
            None,
            project_id,
            scope_id,
            instrument,
            order_id,
            owner,
            new_price_ticks,
            new_qty_be,
            new_time_in_force,
            new_exec_instructions,
            finality,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_cancel_replace_strict_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        new_price_ticks: Option<i64>,
        new_qty_be: Option<[u8; 32]>,
        new_time_in_force: Option<TimeInForce>,
        new_exec_instructions: Option<ExecInstruction>,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.order_book_cancel_replace_strict_as_internal(
            Some(caller),
            project_id,
            scope_id,
            instrument,
            order_id,
            owner,
            new_price_ticks,
            new_qty_be,
            new_time_in_force,
            new_exec_instructions,
            finality,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn order_book_cancel_replace_strict_as_internal(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        new_price_ticks: Option<i64>,
        new_qty_be: Option<[u8; 32]>,
        new_time_in_force: Option<TimeInForce>,
        new_exec_instructions: Option<ExecInstruction>,
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let (order_key, version, base_seq) = self
            .order_book_strict_cancellable_version(
                project_id, scope_id, instrument, order_id, owner,
            )
            .await?;
        let envelope = TransactionEnvelope {
            caller,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet {
                points: vec![ReadSetEntry {
                    key: ReadKey::KvKey {
                        project_id: project_id.to_string(),
                        scope_id: scope_id.to_string(),
                        key: order_key,
                    },
                    version_at_read: version,
                }],
                ranges: Vec::new(),
            },
            write_intent: WriteIntent {
                mutations: vec![Mutation::OrderBookCancelReplace {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    instrument: instrument.to_string(),
                    order_id,
                    owner: owner.to_string(),
                    new_price_ticks,
                    new_qty_be,
                    new_time_in_force,
                    new_exec_instructions,
                }],
            },
            base_seq,
        };
        self.commit_envelope_with_finality(envelope, finality).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_reduce_strict(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        reduce_by_be: [u8; 32],
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.order_book_reduce_strict_as_internal(
            None,
            project_id,
            scope_id,
            instrument,
            order_id,
            owner,
            reduce_by_be,
            finality,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_reduce_strict_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        reduce_by_be: [u8; 32],
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        self.order_book_reduce_strict_as_internal(
            Some(caller),
            project_id,
            scope_id,
            instrument,
            order_id,
            owner,
            reduce_by_be,
            finality,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn order_book_reduce_strict_as_internal(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        reduce_by_be: [u8; 32],
        finality: CommitFinality,
    ) -> Result<CommitResult, AedbError> {
        let reduce_by = u256_from_be(reduce_by_be);
        if reduce_by.is_zero() {
            return Err(AedbError::Validation(
                "strict reduce requires reduce_by > 0".into(),
            ));
        }
        let (order_key, version, base_seq) = self
            .order_book_strict_cancellable_version(
                project_id, scope_id, instrument, order_id, owner,
            )
            .await?;
        let envelope = TransactionEnvelope {
            caller,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: ReadSet {
                points: vec![ReadSetEntry {
                    key: ReadKey::KvKey {
                        project_id: project_id.to_string(),
                        scope_id: scope_id.to_string(),
                        key: order_key,
                    },
                    version_at_read: version,
                }],
                ranges: Vec::new(),
            },
            write_intent: WriteIntent {
                mutations: vec![Mutation::OrderBookReduce {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    instrument: instrument.to_string(),
                    order_id,
                    owner: owner.to_string(),
                    reduce_by_be,
                }],
            },
            base_seq,
        };
        self.commit_envelope_with_finality(envelope, finality).await
    }

    async fn order_book_strict_cancellable_version(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
    ) -> Result<(Vec<u8>, u64, u64), AedbError> {
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let order_key = key_order(instrument, order_id);
        let Some(entry) = lease
            .view
            .keyspace
            .try_kv_get(project_id, scope_id, &order_key)?
        else {
            return Err(AedbError::Validation(format!(
                "strict target not found: order_id={order_id}"
            )));
        };
        let order: OrderRecord =
            rmp_serde::from_slice(&entry.value).map_err(|e| AedbError::Decode(e.to_string()))?;
        if order.owner != owner {
            return Err(AedbError::PermissionDenied(
                "order ownership mismatch".into(),
            ));
        }
        if !matches!(
            order.status,
            OrderStatus::Open | OrderStatus::PartiallyFilled
        ) || u256_from_be(order.remaining_qty_be).is_zero()
        {
            return Err(AedbError::Validation(format!(
                "order not mutable in current status: {:?}",
                order.status
            )));
        }
        Ok((order_key, entry.version, lease.view.seq))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_cancel_replace(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        new_price_ticks: Option<i64>,
        new_qty_be: Option<[u8; 32]>,
        new_time_in_force: Option<TimeInForce>,
        new_exec_instructions: Option<ExecInstruction>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_cancel_replace",
            Mutation::OrderBookCancelReplace {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id,
                owner: owner.to_string(),
                new_price_ticks,
                new_qty_be,
                new_time_in_force,
                new_exec_instructions,
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_cancel_replace_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        new_price_ticks: Option<i64>,
        new_qty_be: Option<[u8; 32]>,
        new_time_in_force: Option<TimeInForce>,
        new_exec_instructions: Option<ExecInstruction>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookCancelReplace {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id,
                owner: owner.to_string(),
                new_price_ticks,
                new_qty_be,
                new_time_in_force,
                new_exec_instructions,
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_mass_cancel(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        owner: &str,
        side: Option<OrderSide>,
        owner_filter: Option<String>,
        price_range_ticks: Option<(i64, i64)>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_mass_cancel",
            Mutation::OrderBookMassCancel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                owner: owner.to_string(),
                side,
                owner_filter,
                price_range_ticks,
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_mass_cancel_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        owner: &str,
        side: Option<OrderSide>,
        owner_filter: Option<String>,
        price_range_ticks: Option<(i64, i64)>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookMassCancel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                owner: owner.to_string(),
                side,
                owner_filter,
                price_range_ticks,
            },
        )
        .await
    }

    pub async fn order_book_reduce(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        reduce_by_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_reduce",
            Mutation::OrderBookReduce {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id,
                owner: owner.to_string(),
                reduce_by_be,
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn order_book_reduce_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        owner: &str,
        reduce_by_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookReduce {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                order_id,
                owner: owner.to_string(),
                reduce_by_be,
            },
        )
        .await
    }

    pub async fn order_book_match_internal(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        fills: Vec<FillSpec>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_prevalidated_internal(
            "order_book_match_internal",
            Mutation::OrderBookMatch {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                fills,
            },
        )
        .await
    }

    pub async fn order_book_match_internal_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        fills: Vec<FillSpec>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::OrderBookMatch {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                instrument: instrument.to_string(),
                fills,
            },
        )
        .await
    }

    pub async fn order_book_top_n(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        depth: u32,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<OrderBookDepth, QueryError> {
        ensure_query_caller_allowed(caller)?;
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let prefix = format!("ob:{instrument}:");
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            prefix.as_bytes(),
        ) {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.{scope_id})"),
                scope: caller.caller_id.clone(),
            });
        }
        read_top_n(
            &lease.view.keyspace,
            project_id,
            scope_id,
            instrument,
            depth as usize,
            lease.view.seq,
        )
        .map_err(QueryError::from)
    }

    pub async fn order_status(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        order_id: u64,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<Option<OrderRecord>, QueryError> {
        ensure_query_caller_allowed(caller)?;
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let prefix = format!("ob:{instrument}:");
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            prefix.as_bytes(),
        ) {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.{scope_id})"),
                scope: caller.caller_id.clone(),
            });
        }
        let order = read_order_status(
            &lease.view.keyspace,
            project_id,
            scope_id,
            instrument,
            order_id,
        )
        .map_err(QueryError::from)?;
        if let Some(order) = &order {
            let admin = lease
                .view
                .catalog
                .has_permission(&caller.caller_id, &Permission::GlobalAdmin);
            if !admin && order.owner != caller.caller_id {
                return Err(QueryError::PermissionDenied {
                    permission: "order_status(owner match)".into(),
                    scope: caller.caller_id.clone(),
                });
            }
        }
        Ok(order)
    }

    pub async fn open_orders(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        owner: &str,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<Vec<OrderRecord>, QueryError> {
        ensure_query_caller_allowed(caller)?;
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let prefix = format!("ob:{instrument}:");
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            prefix.as_bytes(),
        ) {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.{scope_id})"),
                scope: caller.caller_id.clone(),
            });
        }
        let admin = lease
            .view
            .catalog
            .has_permission(&caller.caller_id, &Permission::GlobalAdmin);
        if !admin && owner != caller.caller_id {
            return Err(QueryError::PermissionDenied {
                permission: "open_orders(owner match)".into(),
                scope: caller.caller_id.clone(),
            });
        }
        read_open_orders(
            &lease.view.keyspace,
            project_id,
            scope_id,
            instrument,
            owner,
        )
        .map_err(QueryError::from)
    }

    pub async fn recent_trades(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        limit: u32,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<Vec<FillRecord>, QueryError> {
        ensure_query_caller_allowed(caller)?;
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let prefix = format!("ob:{instrument}:");
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            prefix.as_bytes(),
        ) {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.{scope_id})"),
                scope: caller.caller_id.clone(),
            });
        }
        read_recent_trades(
            &lease.view.keyspace,
            project_id,
            scope_id,
            instrument,
            limit as usize,
        )
        .map_err(QueryError::from)
    }

    pub async fn spread(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<Spread, QueryError> {
        ensure_query_caller_allowed(caller)?;
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let prefix = format!("ob:{instrument}:");
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            prefix.as_bytes(),
        ) {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.{scope_id})"),
                scope: caller.caller_id.clone(),
            });
        }
        read_spread(
            &lease.view.keyspace,
            project_id,
            scope_id,
            instrument,
            lease.view.seq,
        )
        .map_err(QueryError::from)
    }

    pub async fn order_book_last_execution_report(
        &self,
        project_id: &str,
        scope_id: &str,
        instrument: &str,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<Option<ExecutionReport>, QueryError> {
        ensure_query_caller_allowed(caller)?;
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let prefix = format!("ob:{instrument}:");
        if !lease.view.catalog.has_kv_read_permission(
            &caller.caller_id,
            project_id,
            scope_id,
            prefix.as_bytes(),
        ) {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.{scope_id})"),
                scope: caller.caller_id.clone(),
            });
        }
        read_last_execution_report(&lease.view.keyspace, project_id, scope_id, instrument)
            .map_err(QueryError::from)
    }

    pub async fn compare_and_swap(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        row: Row,
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::RowVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                primary_key: primary_key.clone(),
                expected_seq,
            }],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::Upsert {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    table_name: table_name.to_string(),
                    primary_key,
                    row,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    pub async fn compare_and_swap_as_with(
        &self,
        request: CompareAndSwapRequest,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: Some(request.caller),
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::RowVersion {
                project_id: request.project_id.clone(),
                scope_id: request.scope_id.clone(),
                table_name: request.table_name.clone(),
                primary_key: request.primary_key.clone(),
                expected_seq: request.expected_seq,
            }],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::Upsert {
                    project_id: request.project_id,
                    scope_id: request.scope_id,
                    table_name: request.table_name,
                    primary_key: request.primary_key,
                    row: request.row,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn compare_and_swap_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        row: Row,
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.compare_and_swap_as_with(CompareAndSwapRequest {
            caller,
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            primary_key,
            row,
            expected_seq,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn compare_and_inc_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::RowVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                primary_key: primary_key.clone(),
                expected_seq,
            }],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::TableIncU256 {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    table_name: table_name.to_string(),
                    primary_key,
                    column: column.to_string(),
                    amount_be,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn compare_and_inc_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: Some(caller),
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::RowVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                primary_key: primary_key.clone(),
                expected_seq,
            }],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::TableIncU256 {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    table_name: table_name.to_string(),
                    primary_key,
                    column: column.to_string(),
                    amount_be,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn compare_and_dec_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::RowVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                primary_key: primary_key.clone(),
                expected_seq,
            }],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::TableDecU256 {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    table_name: table_name.to_string(),
                    primary_key,
                    column: column.to_string(),
                    amount_be,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn compare_and_dec_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: Some(caller),
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::RowVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: table_name.to_string(),
                primary_key: primary_key.clone(),
                expected_seq,
            }],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::TableDecU256 {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    table_name: table_name.to_string(),
                    primary_key,
                    column: column.to_string(),
                    amount_be,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }
}
