use crate::*;

impl AedbInstance {
    pub async fn emit_event(
        &self,
        project_id: &str,
        scope_id: &str,
        topic: &str,
        event_key: String,
        payload_json: String,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::EmitEvent {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            topic: topic.to_string(),
            event_key,
            payload_json,
        })
        .await
    }

    pub async fn emit_event_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        topic: &str,
        event_key: String,
        payload_json: String,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::EmitEvent {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                topic: topic.to_string(),
                event_key,
                payload_json,
            },
        )
        .await
    }

    pub async fn read_event_stream(
        &self,
        topic_filter: Option<&str>,
        from_commit_seq_exclusive: u64,
        limit: usize,
        consistency: ConsistencyMode,
    ) -> Result<EventStreamPage, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        self.read_event_stream_internal(
            topic_filter,
            from_commit_seq_exclusive,
            limit,
            consistency,
            None,
        )
        .await
    }

    pub async fn read_event_stream_as(
        &self,
        caller: &CallerContext,
        topic_filter: Option<&str>,
        from_commit_seq_exclusive: u64,
        limit: usize,
        consistency: ConsistencyMode,
    ) -> Result<EventStreamPage, AedbError> {
        ensure_external_caller_allowed(caller)?;
        self.read_event_stream_internal(
            topic_filter,
            from_commit_seq_exclusive,
            limit,
            consistency,
            Some(caller),
        )
        .await
    }

    async fn read_event_stream_internal(
        &self,
        topic_filter: Option<&str>,
        from_commit_seq_exclusive: u64,
        limit: usize,
        consistency: ConsistencyMode,
        caller: Option<&CallerContext>,
    ) -> Result<EventStreamPage, AedbError> {
        let lease = self.acquire_snapshot(consistency).await?;
        let snapshot_seq = lease.view.seq;
        if limit == 0 {
            return Ok(EventStreamPage {
                events: Vec::new(),
                next_commit_seq: None,
                snapshot_seq,
            });
        }
        if let Some(caller) = caller {
            let required = Permission::TableRead {
                project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
                scope_id: SYSTEM_SCOPE_ID.to_string(),
                table_name: EVENT_OUTBOX_TABLE.to_string(),
            };
            if !lease
                .view
                .catalog
                .has_permission(&caller.caller_id, &required)
            {
                return Err(AedbError::PermissionDenied(format!(
                    "caller={} missing table read permission for system event stream",
                    caller.caller_id
                )));
            }
        }
        let Some(table) = lease.view.keyspace.table(
            crate::catalog::SYSTEM_PROJECT_ID,
            SYSTEM_SCOPE_ID,
            EVENT_OUTBOX_TABLE,
        ) else {
            return Ok(EventStreamPage {
                events: Vec::new(),
                next_commit_seq: None,
                snapshot_seq,
            });
        };
        let mut events = Vec::new();
        let start_seq = from_commit_seq_exclusive.saturating_add(1);
        let Ok(start_seq_i64) = i64::try_from(start_seq) else {
            return Ok(EventStreamPage {
                events: Vec::new(),
                next_commit_seq: None,
                snapshot_seq,
            });
        };
        let start_key = EncodedKey::from_values(&[
            Value::Integer(start_seq_i64),
            Value::Text("".into()),
            Value::Text("".into()),
        ]);
        for row in table
            .rows
            .range((Bound::Included(start_key), Bound::Unbounded))
            .map(|(_, row)| row)
        {
            if events.len() >= limit {
                break;
            }
            let (
                Some(Value::Integer(commit_seq_i64)),
                Some(Value::Timestamp(ts_i64)),
                Some(Value::Text(project_id)),
                Some(Value::Text(scope_id)),
                Some(Value::Text(topic)),
                Some(Value::Text(event_key)),
                Some(Value::Json(payload)),
            ) = (
                row.values.first(),
                row.values.get(1),
                row.values.get(2),
                row.values.get(3),
                row.values.get(4),
                row.values.get(5),
                row.values.get(6),
            )
            else {
                continue;
            };
            let Ok(commit_seq) = u64::try_from(*commit_seq_i64) else {
                continue;
            };
            if let Some(filter_topic) = topic_filter
                && topic.as_str() != filter_topic
            {
                continue;
            }
            let Ok(ts_micros) = u64::try_from(*ts_i64) else {
                continue;
            };
            events.push(EventOutboxRecord {
                commit_seq,
                ts_micros,
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                topic: topic.to_string(),
                event_key: event_key.to_string(),
                payload_json: payload.to_string(),
            });
        }
        let next_commit_seq = events.last().map(|e| e.commit_seq);
        Ok(EventStreamPage {
            events,
            next_commit_seq,
            snapshot_seq,
        })
    }

    pub async fn ack_reactive_processor_checkpoint(
        &self,
        processor_name: &str,
        checkpoint_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        let envelope = self
            .build_reactive_processor_checkpoint_envelope(processor_name, checkpoint_seq)
            .await?;
        self.commit_envelope_prevalidated_internal("ack_reactive_processor_checkpoint", envelope)
            .await
    }

    pub async fn ack_reactive_processor_checkpoint_as(
        &self,
        caller: CallerContext,
        processor_name: &str,
        checkpoint_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        ensure_external_caller_allowed(&caller)?;
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        self.ensure_reactive_processor_checkpoint_write_allowed(&caller)
            .await?;
        let mut envelope = self
            .build_reactive_processor_checkpoint_envelope(processor_name, checkpoint_seq)
            .await?;
        envelope.caller = Some(caller);
        self.commit_envelope_prevalidated_system_internal(
            "ack_reactive_processor_checkpoint_as",
            envelope,
        )
        .await
    }

    pub async fn ack_reactive_processor_checkpoint_batched(
        &self,
        processor_name: &str,
        checkpoint_seq: u64,
        watermark_commits: u64,
    ) -> Result<Option<CommitResult>, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        if watermark_commits == 0 {
            return Err(AedbError::Validation(
                "watermark_commits must be > 0".into(),
            ));
        }
        let cache_key = ReactiveCheckpointAckCacheKey {
            processor_name: processor_name.to_string(),
            caller_id: None,
        };
        let should_persist = {
            let cache = self.reactive_processor_ack_watermarks.lock();
            let last_persisted = cache
                .get(&cache_key)
                .map(|s| s.last_persisted_seq)
                .unwrap_or(0);
            checkpoint_seq > last_persisted
                && (last_persisted == 0
                    || checkpoint_seq.saturating_sub(last_persisted) >= watermark_commits)
        };
        if !should_persist {
            return Ok(None);
        }
        let envelope = self
            .build_reactive_processor_checkpoint_envelope(processor_name, checkpoint_seq)
            .await?;
        let committed = self
            .commit_envelope_prevalidated_internal(
                "ack_reactive_processor_checkpoint_batched",
                envelope,
            )
            .await?;
        {
            let mut cache = self.reactive_processor_ack_watermarks.lock();
            let state = cache.entry(cache_key).or_default();
            state.last_persisted_seq = state.last_persisted_seq.max(checkpoint_seq);
            state.last_touch_micros = system_now_micros();
            prune_reactive_ack_cache(&mut cache);
        }
        Ok(Some(committed))
    }

    pub async fn ack_reactive_processor_checkpoint_batched_as(
        &self,
        caller: CallerContext,
        processor_name: &str,
        checkpoint_seq: u64,
        watermark_commits: u64,
    ) -> Result<Option<CommitResult>, AedbError> {
        ensure_external_caller_allowed(&caller)?;
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        if watermark_commits == 0 {
            return Err(AedbError::Validation(
                "watermark_commits must be > 0".into(),
            ));
        }
        let cache_key = ReactiveCheckpointAckCacheKey {
            processor_name: processor_name.to_string(),
            caller_id: Some(caller.caller_id.clone()),
        };
        let should_persist = {
            let cache = self.reactive_processor_ack_watermarks.lock();
            let last_persisted = cache
                .get(&cache_key)
                .map(|s| s.last_persisted_seq)
                .unwrap_or(0);
            checkpoint_seq > last_persisted
                && (last_persisted == 0
                    || checkpoint_seq.saturating_sub(last_persisted) >= watermark_commits)
        };
        if !should_persist {
            return Ok(None);
        }
        self.ensure_reactive_processor_checkpoint_write_allowed(&caller)
            .await?;
        let mut envelope = self
            .build_reactive_processor_checkpoint_envelope(processor_name, checkpoint_seq)
            .await?;
        envelope.caller = Some(caller);
        let committed = self
            .commit_envelope_prevalidated_system_internal(
                "ack_reactive_processor_checkpoint_batched_as",
                envelope,
            )
            .await?;
        {
            let mut cache = self.reactive_processor_ack_watermarks.lock();
            let state = cache.entry(cache_key).or_default();
            state.last_persisted_seq = state.last_persisted_seq.max(checkpoint_seq);
            state.last_touch_micros = system_now_micros();
            prune_reactive_ack_cache(&mut cache);
        }
        Ok(Some(committed))
    }

    async fn ensure_reactive_processor_checkpoint_write_allowed(
        &self,
        caller: &CallerContext,
    ) -> Result<(), AedbError> {
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let required = Permission::TableWrite {
            project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
            scope_id: SYSTEM_SCOPE_ID.to_string(),
            table_name: REACTIVE_PROCESSOR_CHECKPOINTS_TABLE.to_string(),
        };
        if !lease
            .view
            .catalog
            .has_permission(&caller.caller_id, &required)
        {
            return Err(AedbError::PermissionDenied("permission denied".into()));
        }
        Ok(())
    }

    pub(crate) async fn build_reactive_processor_checkpoint_envelope(
        &self,
        processor_name: &str,
        checkpoint_seq: u64,
    ) -> Result<TransactionEnvelope, AedbError> {
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let checkpoint_key = Value::Text(processor_name.to_string().into());
        let checkpoint_pk = EncodedKey::from_values(std::slice::from_ref(&checkpoint_key));
        let checkpoint_table = lease.view.keyspace.table(
            crate::catalog::SYSTEM_PROJECT_ID,
            SYSTEM_SCOPE_ID,
            REACTIVE_PROCESSOR_CHECKPOINTS_TABLE,
        );
        let current_checkpoint = checkpoint_table
            .and_then(|table| table.rows.get(&checkpoint_pk))
            .and_then(|row| row.values.get(1))
            .and_then(|v| match v {
                Value::Integer(i) => u64::try_from(*i).ok(),
                _ => None,
            })
            .unwrap_or(0);
        if checkpoint_seq < current_checkpoint {
            return Err(AedbError::Validation(format!(
                "checkpoint_seq {checkpoint_seq} regresses current checkpoint {current_checkpoint}"
            )));
        }
        let checkpoint_version = checkpoint_table
            .and_then(|table| table.row_versions.get(&checkpoint_pk))
            .copied()
            .unwrap_or(0);
        let primary_key = vec![checkpoint_key.clone()];
        let read_set = ReadSet {
            points: vec![ReadSetEntry {
                key: ReadKey::TableRow {
                    project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
                    scope_id: SYSTEM_SCOPE_ID.to_string(),
                    table_name: REACTIVE_PROCESSOR_CHECKPOINTS_TABLE.to_string(),
                    primary_key,
                },
                version_at_read: checkpoint_version,
            }],
            ranges: Vec::new(),
        };

        Ok(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set,
            write_intent: WriteIntent {
                mutations: vec![reactive_processor_checkpoint_mutation(
                    processor_name,
                    checkpoint_seq,
                )],
            },
            base_seq: lease.view.seq,
        })
    }

    pub async fn reactive_processor_lag(
        &self,
        processor_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<ReactiveProcessorLag, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        self.reactive_processor_lag_internal(processor_name, consistency, None)
            .await
    }

    async fn reactive_processor_lag_internal(
        &self,
        processor_name: &str,
        consistency: ConsistencyMode,
        caller: Option<&CallerContext>,
    ) -> Result<ReactiveProcessorLag, AedbError> {
        let lease = self.acquire_snapshot(consistency).await?;
        if let Some(caller) = caller {
            let required = Permission::TableRead {
                project_id: crate::catalog::SYSTEM_PROJECT_ID.to_string(),
                scope_id: SYSTEM_SCOPE_ID.to_string(),
                table_name: REACTIVE_PROCESSOR_CHECKPOINTS_TABLE.to_string(),
            };
            if !lease
                .view
                .catalog
                .has_permission(&caller.caller_id, &required)
            {
                return Err(AedbError::PermissionDenied(format!(
                    "caller={} missing table read permission for reactive processor checkpoints",
                    caller.caller_id
                )));
            }
        }
        let mut checkpoint_seq = 0u64;
        if let Some(table) = lease.view.keyspace.table(
            crate::catalog::SYSTEM_PROJECT_ID,
            SYSTEM_SCOPE_ID,
            REACTIVE_PROCESSOR_CHECKPOINTS_TABLE,
        ) {
            let pk = EncodedKey::from_values(&[Value::Text(processor_name.to_string().into())]);
            if let Some(row) = table.rows.get(&pk)
                && let Some(Value::Integer(v)) = row.values.get(1)
            {
                checkpoint_seq = u64::try_from(*v).unwrap_or(0);
            }
        }
        let head_seq = lease.view.seq;
        Ok(ReactiveProcessorLag {
            processor_name: processor_name.to_string(),
            checkpoint_seq,
            head_seq,
            lag_commits: head_seq.saturating_sub(checkpoint_seq),
        })
    }

    pub async fn reactive_processor_lag_as(
        &self,
        caller: &CallerContext,
        processor_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<ReactiveProcessorLag, AedbError> {
        ensure_external_caller_allowed(caller)?;
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        self.reactive_processor_lag_internal(processor_name, consistency, Some(caller))
            .await
    }

    pub async fn reactive_processor_runtime_status(
        &self,
        processor_name: &str,
    ) -> Option<ReactiveProcessorRuntimeStatus> {
        let (running, stats) = {
            let runtimes = self.reactive_processor_runtimes.lock().await;
            let runtime = runtimes.get(processor_name)?;
            (true, Arc::clone(&runtime.stats))
        };
        let mut status = stats.lock().await.clone();
        status.running = running;
        Some(status)
    }

    pub async fn stop_reactive_processor(&self, processor_name: &str) -> Result<(), AedbError> {
        self.pause_reactive_processor(processor_name).await
    }

    pub async fn pause_reactive_processor(&self, processor_name: &str) -> Result<(), AedbError> {
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        if let Some((options, _)) = self
            .load_reactive_processor_registration(processor_name)
            .await?
        {
            self.persist_reactive_processor_registration(processor_name, &options, false)
                .await?;
        }
        let runtime = {
            let mut runtimes = self.reactive_processor_runtimes.lock().await;
            runtimes.remove(processor_name)
        };
        if let Some(runtime) = runtime {
            runtime.stop.store(true, Ordering::Release);
            runtime.join.abort();
            let _ = runtime.join.await;
        }
        Ok(())
    }

    pub async fn resume_reactive_processor(
        self: &Arc<Self>,
        processor_name: &str,
    ) -> Result<(), AedbError> {
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        {
            let runtimes = self.reactive_processor_runtimes.lock().await;
            if runtimes.contains_key(processor_name) {
                return Ok(());
            }
        }
        let registration = self
            .load_reactive_processor_registration(processor_name)
            .await?
            .ok_or_else(|| AedbError::NotFound {
                resource_type: ErrorResourceType::Table,
                resource_id: format!(
                    "{}.{}.{}:{}",
                    crate::catalog::SYSTEM_PROJECT_ID,
                    SYSTEM_SCOPE_ID,
                    REACTIVE_PROCESSOR_REGISTRY_TABLE,
                    processor_name
                ),
            })?;
        let handler = {
            let handlers = self.reactive_processor_handlers.lock();
            handlers.get(processor_name).cloned().ok_or_else(|| {
                AedbError::Validation(format!(
                    "reactive processor handler not registered: {processor_name}"
                ))
            })?
        };
        let (options, _) = registration;
        self.start_reactive_processor_with_handler(processor_name, options, handler, true)
            .await
    }

    pub async fn list_reactive_processors(
        &self,
        consistency: ConsistencyMode,
    ) -> Result<Vec<ReactiveProcessorInfo>, AedbError> {
        let mut registrations = self
            .load_reactive_processor_registrations(consistency)
            .await?;
        registrations.sort_by(|a, b| a.processor_name.cmp(&b.processor_name));
        let running_names: HashSet<String> = {
            let runtimes = self.reactive_processor_runtimes.lock().await;
            runtimes.keys().cloned().collect()
        };
        Ok(registrations
            .into_iter()
            .map(|r| ReactiveProcessorInfo {
                running: running_names.contains(&r.processor_name),
                processor_name: r.processor_name,
                options: r.options,
                enabled: r.enabled,
                updated_at_micros: r.updated_at_micros,
            })
            .collect())
    }

    pub async fn reactive_processor_health(
        &self,
        processor_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<ReactiveProcessorHealth, AedbError> {
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        let registration = self
            .load_reactive_processor_registration_record(processor_name, consistency)
            .await?;
        let runtime_status = self.reactive_processor_runtime_status(processor_name).await;
        let lag = self
            .reactive_processor_lag_internal(processor_name, consistency, None)
            .await?;
        let running = runtime_status.as_ref().map(|s| s.running).unwrap_or(false);
        let enabled = registration.as_ref().map(|r| r.enabled).unwrap_or(running);
        let status = runtime_status.unwrap_or_else(|| ReactiveProcessorRuntimeStatus {
            processor_name: processor_name.to_string(),
            running,
            ..ReactiveProcessorRuntimeStatus::default()
        });
        Ok(ReactiveProcessorHealth {
            processor_name: processor_name.to_string(),
            enabled,
            running,
            checkpoint_seq: lag.checkpoint_seq,
            head_seq: lag.head_seq,
            lag_commits: lag.lag_commits,
            runs_total: status.runs_total,
            processed_events_total: status.processed_events_total,
            failures_total: status.failures_total,
            retries_total: status.retries_total,
            dead_lettered_total: status.dead_lettered_total,
            last_processed_seq: status.last_processed_seq,
            last_error: status.last_error,
            last_run_started_micros: status.last_run_started_micros,
            last_run_completed_micros: status.last_run_completed_micros,
            last_success_micros: status.last_success_micros,
            last_failure_micros: status.last_failure_micros,
            last_retry_micros: status.last_retry_micros,
            last_sleep_ms: status.last_sleep_ms,
            last_batch_events: status.last_batch_events,
        })
    }

    pub async fn reactive_processor_slo_status(
        &self,
        processor_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<ReactiveProcessorSloStatus, AedbError> {
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        let registration = self
            .load_reactive_processor_registration_record(processor_name, consistency)
            .await?
            .ok_or_else(|| AedbError::NotFound {
                resource_type: ErrorResourceType::Table,
                resource_id: format!(
                    "{}.{}.{}:{}",
                    crate::catalog::SYSTEM_PROJECT_ID,
                    SYSTEM_SCOPE_ID,
                    REACTIVE_PROCESSOR_REGISTRY_TABLE,
                    processor_name
                ),
            })?;
        let health = self
            .reactive_processor_health(processor_name, consistency)
            .await?;
        Ok(Self::build_reactive_processor_slo_status(
            &registration.options,
            health,
        ))
    }

    pub async fn list_reactive_processor_slo_statuses(
        &self,
        consistency: ConsistencyMode,
    ) -> Result<Vec<ReactiveProcessorSloStatus>, AedbError> {
        let infos = self.list_reactive_processors(consistency).await?;
        let mut out = Vec::with_capacity(infos.len());
        for info in infos {
            let health = self
                .reactive_processor_health(&info.processor_name, consistency)
                .await?;
            out.push(Self::build_reactive_processor_slo_status(
                &info.options,
                health,
            ));
        }
        out.sort_by(|a, b| a.processor_name.cmp(&b.processor_name));
        Ok(out)
    }

    pub async fn enforce_reactive_processor_slos(
        &self,
        consistency: ConsistencyMode,
    ) -> Result<(), AedbError> {
        let statuses = self
            .list_reactive_processor_slo_statuses(consistency)
            .await?;
        let breaches: Vec<ReactiveProcessorSloStatus> =
            statuses.into_iter().filter(|s| s.breached).collect();
        if breaches.is_empty() {
            return Ok(());
        }
        let details = breaches
            .iter()
            .map(|s| {
                format!(
                    "{}: {}",
                    s.processor_name,
                    if s.reasons.is_empty() {
                        "unknown breach".to_string()
                    } else {
                        s.reasons.join("; ")
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(" | ");
        Err(AedbError::Unavailable {
            message: format!("reactive processor SLO breach: {details}"),
        })
    }

    pub async fn start_reactive_processor<F, Fut>(
        self: &Arc<Self>,
        processor_name: &str,
        options: ReactiveProcessorOptions,
        handler: F,
    ) -> Result<(), AedbError>
    where
        F: Fn(Arc<AedbInstance>, Vec<EventOutboxRecord>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), AedbError>> + Send + 'static,
    {
        let handler: ReactiveProcessorHandler =
            Arc::new(move |db, events| Box::pin(handler(db, events)));
        self.start_reactive_processor_with_handler(processor_name, options, handler, true)
            .await
    }

    pub async fn register_reactive_processor_handler<F, Fut>(
        self: &Arc<Self>,
        processor_name: &str,
        handler: F,
    ) -> Result<bool, AedbError>
    where
        F: Fn(Arc<AedbInstance>, Vec<EventOutboxRecord>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), AedbError>> + Send + 'static,
    {
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        let handler: ReactiveProcessorHandler =
            Arc::new(move |db, events| Box::pin(handler(db, events)));
        self.reactive_processor_handlers
            .lock()
            .insert(processor_name.to_string(), Arc::clone(&handler));
        let Some((options, enabled)) = self
            .load_reactive_processor_registration(processor_name)
            .await?
        else {
            return Ok(false);
        };
        if !enabled {
            return Ok(false);
        }
        {
            let runtimes = self.reactive_processor_runtimes.lock().await;
            if runtimes.contains_key(processor_name) {
                return Ok(false);
            }
        }
        self.start_reactive_processor_with_handler(processor_name, options, handler, false)
            .await?;
        Ok(true)
    }

    async fn start_reactive_processor_with_handler(
        self: &Arc<Self>,
        processor_name: &str,
        options: ReactiveProcessorOptions,
        handler: ReactiveProcessorHandler,
        persist_registry: bool,
    ) -> Result<(), AedbError> {
        if processor_name.trim().is_empty() {
            return Err(AedbError::Validation(
                "processor_name cannot be empty".into(),
            ));
        }
        if options.max_events_per_run == 0
            || options.max_bytes_per_run == 0
            || options.max_run_duration_ms == 0
            || options.run_interval_ms == 0
            || options.idle_backoff_ms == 0
            || options.checkpoint_watermark_commits == 0
            || (options.max_retries > 0 && options.retry_backoff_ms == 0)
        {
            return Err(AedbError::Validation(
                "reactive processor options must be > 0".into(),
            ));
        }
        let caller = if let Some(raw) = options.caller_id.as_ref() {
            let caller = CallerContext::new(raw.clone());
            ensure_external_caller_allowed(&caller)?;
            Some(caller)
        } else if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "reactive processor caller_id required in secure mode".into(),
            ));
        } else {
            None
        };
        if persist_registry {
            self.persist_reactive_processor_registration(processor_name, &options, true)
                .await?;
        }
        // Seed checkpoint state so caller-scoped ACKs never race a missing internal table.
        let _ = self
            .commit_prevalidated_system_internal(
                "init_reactive_processor_checkpoint",
                reactive_processor_checkpoint_mutation(processor_name, 0),
            )
            .await?;
        let stats = Arc::new(AsyncMutex::new(ReactiveProcessorRuntimeStatus {
            processor_name: processor_name.to_string(),
            running: true,
            ..ReactiveProcessorRuntimeStatus::default()
        }));
        {
            let runtimes = self.reactive_processor_runtimes.lock().await;
            if runtimes.contains_key(processor_name) {
                return Err(AedbError::Validation(format!(
                    "reactive processor already running: {processor_name}"
                )));
            }
        }
        let weak = Arc::downgrade(self);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_loop = Arc::clone(&stop);
        let processor_name_owned = processor_name.to_string();
        let options_owned = options.clone();
        let caller_owned = caller.clone();
        let stats_loop = Arc::clone(&stats);
        let handler_loop = Arc::clone(&handler);
        let join = tokio::spawn(async move {
            let mut retry: Option<ReactiveProcessorRetryState> = None;
            loop {
                if stop_loop.load(Ordering::Acquire) {
                    break;
                }
                let loop_started_micros = system_now_micros();
                let Some(db) = weak.upgrade() else {
                    break;
                };
                let mut sleep_ms = options_owned.idle_backoff_ms;
                let mut add_processed = 0u64;
                let mut add_failures = 0u64;
                let mut add_retries = 0u64;
                let mut add_dead_lettered = 0u64;
                let mut last_processed_seq = 0u64;
                let mut last_error: Option<String> = None;

                if let Some(pending) = retry.as_mut() {
                    if Instant::now() < pending.next_retry_at {
                        let wait_ms = (pending.next_retry_at - Instant::now()).as_millis() as u64;
                        sleep_ms = wait_ms.max(1).min(options_owned.idle_backoff_ms.max(1));
                    } else {
                        match AedbInstance::process_reactive_processor_batch(
                            Arc::clone(&db),
                            &processor_name_owned,
                            caller_owned.as_ref(),
                            &options_owned,
                            &handler_loop,
                            pending.events.clone(),
                            pending.last_seq,
                        )
                        .await
                        {
                            Ok((processed, last_seq)) => {
                                add_processed = processed;
                                last_processed_seq = last_seq;
                                last_error = None;
                                retry = None;
                                sleep_ms = options_owned.run_interval_ms;
                            }
                            Err(err) => {
                                add_failures = 1;
                                last_error = Some(err.to_string());
                                if pending.attempts >= options_owned.max_retries {
                                    let exhausted = pending.clone();
                                    let _ = db
                                        .dead_letter_reactive_processor_batch(
                                            &processor_name_owned,
                                            caller_owned.as_ref(),
                                            &exhausted.events,
                                            &err.to_string(),
                                            exhausted.attempts.saturating_add(1),
                                        )
                                        .await;
                                    let _ = if let Some(caller) = caller_owned.as_ref() {
                                        db.ack_reactive_processor_checkpoint_batched_as(
                                            caller.clone(),
                                            &processor_name_owned,
                                            exhausted.last_seq,
                                            options_owned.checkpoint_watermark_commits,
                                        )
                                        .await
                                    } else {
                                        db.ack_reactive_processor_checkpoint_batched(
                                            &processor_name_owned,
                                            exhausted.last_seq,
                                            options_owned.checkpoint_watermark_commits,
                                        )
                                        .await
                                    };
                                    add_dead_lettered = exhausted.events.len() as u64;
                                    last_processed_seq = exhausted.last_seq;
                                    retry = None;
                                    sleep_ms = options_owned.run_interval_ms;
                                } else {
                                    pending.attempts = pending.attempts.saturating_add(1);
                                    let exp = pending.attempts.saturating_sub(1).min(8);
                                    let backoff =
                                        options_owned.retry_backoff_ms.saturating_mul(1u64 << exp);
                                    pending.last_error = err.to_string();
                                    pending.next_retry_at =
                                        Instant::now() + Duration::from_millis(backoff.max(1));
                                    add_retries = 1;
                                    sleep_ms = backoff.max(1);
                                }
                            }
                        }
                    }
                } else {
                    match AedbInstance::fetch_reactive_processor_batch(
                        Arc::clone(&db),
                        &processor_name_owned,
                        caller_owned.as_ref(),
                        &options_owned,
                    )
                    .await
                    {
                        Ok(events) if events.is_empty() => {
                            if options_owned.run_on_interval {
                                match AedbInstance::process_reactive_processor_batch(
                                    Arc::clone(&db),
                                    &processor_name_owned,
                                    caller_owned.as_ref(),
                                    &options_owned,
                                    &handler_loop,
                                    Vec::new(),
                                    0,
                                )
                                .await
                                {
                                    Ok((_, _)) => {
                                        last_error = None;
                                        sleep_ms = options_owned.run_interval_ms;
                                    }
                                    Err(err) => {
                                        add_failures = 1;
                                        last_error = Some(err.to_string());
                                        if options_owned.max_retries > 0 {
                                            add_retries = 1;
                                            let backoff = options_owned.retry_backoff_ms.max(1);
                                            retry = Some(ReactiveProcessorRetryState {
                                                events: Vec::new(),
                                                last_seq: 0,
                                                attempts: 1,
                                                next_retry_at: Instant::now()
                                                    + Duration::from_millis(backoff),
                                                last_error: err.to_string(),
                                            });
                                            sleep_ms = backoff;
                                        }
                                    }
                                }
                            }
                        }
                        Ok(events) => {
                            let last_seq = events.last().map(|e| e.commit_seq).unwrap_or(0);
                            match AedbInstance::process_reactive_processor_batch(
                                Arc::clone(&db),
                                &processor_name_owned,
                                caller_owned.as_ref(),
                                &options_owned,
                                &handler_loop,
                                events.clone(),
                                last_seq,
                            )
                            .await
                            {
                                Ok((processed, seq)) => {
                                    add_processed = processed;
                                    last_processed_seq = seq;
                                    sleep_ms = options_owned.run_interval_ms;
                                }
                                Err(err) => {
                                    add_failures = 1;
                                    last_error = Some(err.to_string());
                                    if options_owned.max_retries == 0 {
                                        let _ = db
                                            .dead_letter_reactive_processor_batch(
                                                &processor_name_owned,
                                                caller_owned.as_ref(),
                                                &events,
                                                &err.to_string(),
                                                1,
                                            )
                                            .await;
                                        let _ = if let Some(caller) = caller_owned.as_ref() {
                                            db.ack_reactive_processor_checkpoint_batched_as(
                                                caller.clone(),
                                                &processor_name_owned,
                                                last_seq,
                                                options_owned.checkpoint_watermark_commits,
                                            )
                                            .await
                                        } else {
                                            db.ack_reactive_processor_checkpoint_batched(
                                                &processor_name_owned,
                                                last_seq,
                                                options_owned.checkpoint_watermark_commits,
                                            )
                                            .await
                                        };
                                        add_dead_lettered = events.len() as u64;
                                        last_processed_seq = last_seq;
                                        sleep_ms = options_owned.run_interval_ms;
                                    } else {
                                        add_retries = 1;
                                        let backoff = options_owned.retry_backoff_ms.max(1);
                                        retry = Some(ReactiveProcessorRetryState {
                                            events,
                                            last_seq,
                                            attempts: 1,
                                            next_retry_at: Instant::now()
                                                + Duration::from_millis(backoff),
                                            last_error: err.to_string(),
                                        });
                                        sleep_ms = backoff;
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            add_failures = 1;
                            last_error = Some(err.to_string());
                        }
                    }
                }

                {
                    let mut s = stats_loop.lock().await;
                    s.last_run_started_micros = Some(loop_started_micros);
                    s.runs_total = s.runs_total.saturating_add(1);
                    s.processed_events_total =
                        s.processed_events_total.saturating_add(add_processed);
                    s.failures_total = s.failures_total.saturating_add(add_failures);
                    s.retries_total = s.retries_total.saturating_add(add_retries);
                    s.dead_lettered_total = s.dead_lettered_total.saturating_add(add_dead_lettered);
                    s.last_sleep_ms = sleep_ms;
                    s.last_batch_events = add_processed.saturating_add(add_dead_lettered);
                    if last_processed_seq > 0 {
                        s.last_processed_seq = s.last_processed_seq.max(last_processed_seq);
                    }
                    if let Some(err) = last_error {
                        s.last_error = Some(err);
                    } else if add_processed > 0 || add_dead_lettered > 0 {
                        s.last_error = None;
                    }
                    let now = system_now_micros();
                    s.last_run_completed_micros = Some(now);
                    if add_processed > 0 || add_dead_lettered > 0 {
                        s.last_success_micros = Some(now);
                    }
                    if add_failures > 0 {
                        s.last_failure_micros = Some(now);
                    }
                    if add_retries > 0 {
                        s.last_retry_micros = Some(now);
                    }
                }
                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            }
            let Some(db) = weak.upgrade() else {
                return;
            };
            if let Some(runtime) =
                db.reactive_processor_runtimes
                    .try_lock()
                    .ok()
                    .and_then(|runtimes| {
                        runtimes
                            .get(&processor_name_owned)
                            .map(|r| Arc::clone(&r.stats))
                    })
            {
                let mut s = runtime.lock().await;
                s.running = false;
            }
        });

        let mut runtimes = self.reactive_processor_runtimes.lock().await;
        if runtimes.contains_key(processor_name) {
            stop.store(true, Ordering::Release);
            join.abort();
            return Err(AedbError::Validation(format!(
                "reactive processor already running: {processor_name}"
            )));
        }
        self.reactive_processor_handlers
            .lock()
            .insert(processor_name.to_string(), handler);
        runtimes.insert(
            processor_name.to_string(),
            ReactiveProcessorRuntime { stop, join, stats },
        );
        Ok(())
    }

    async fn persist_reactive_processor_registration(
        &self,
        processor_name: &str,
        options: &ReactiveProcessorOptions,
        enabled: bool,
    ) -> Result<CommitResult, AedbError> {
        let mutation = reactive_processor_registry_mutation(processor_name, options, enabled)?;
        self.commit_prevalidated_system_internal(
            "persist_reactive_processor_registration",
            mutation,
        )
        .await
    }

    async fn load_reactive_processor_registration(
        &self,
        processor_name: &str,
    ) -> Result<Option<(ReactiveProcessorOptions, bool)>, AedbError> {
        self.load_reactive_processor_registration_record(processor_name, ConsistencyMode::AtLatest)
            .await
            .map(|opt| opt.map(|r| (r.options, r.enabled)))
    }

    async fn load_reactive_processor_registration_record(
        &self,
        processor_name: &str,
        consistency: ConsistencyMode,
    ) -> Result<Option<ReactiveProcessorRegistration>, AedbError> {
        let lease = self.acquire_snapshot(consistency).await?;
        let Some(table) = lease.view.keyspace.table(
            crate::catalog::SYSTEM_PROJECT_ID,
            SYSTEM_SCOPE_ID,
            REACTIVE_PROCESSOR_REGISTRY_TABLE,
        ) else {
            return Ok(None);
        };
        let pk = EncodedKey::from_values(&[Value::Text(processor_name.to_string().into())]);
        let Some(row) = table.rows.get(&pk) else {
            return Ok(None);
        };
        Self::decode_reactive_processor_registration_row(row)
    }

    async fn load_reactive_processor_registrations(
        &self,
        consistency: ConsistencyMode,
    ) -> Result<Vec<ReactiveProcessorRegistration>, AedbError> {
        let lease = self.acquire_snapshot(consistency).await?;
        let Some(table) = lease.view.keyspace.table(
            crate::catalog::SYSTEM_PROJECT_ID,
            SYSTEM_SCOPE_ID,
            REACTIVE_PROCESSOR_REGISTRY_TABLE,
        ) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::with_capacity(table.rows.len());
        for row in table.rows.values() {
            out.push(
                Self::decode_reactive_processor_registration_row(row)?.ok_or_else(|| {
                    AedbError::Validation(
                        "reactive processor registry row missing processor_name".into(),
                    )
                })?,
            );
        }
        Ok(out)
    }

    fn decode_reactive_processor_registration_row(
        row: &Row,
    ) -> Result<Option<ReactiveProcessorRegistration>, AedbError> {
        let Some(Value::Text(processor_name)) = row.values.first() else {
            return Ok(None);
        };
        let Some(Value::Json(options_json)) = row.values.get(1) else {
            return Err(AedbError::Validation(
                "reactive processor registry options missing".into(),
            ));
        };
        let Some(Value::Boolean(enabled)) = row.values.get(2) else {
            return Err(AedbError::Validation(
                "reactive processor registry enabled flag missing".into(),
            ));
        };
        let updated_at_micros = match row.values.get(3) {
            Some(Value::Timestamp(v)) => u64::try_from(*v).unwrap_or(0),
            _ => 0,
        };
        let options: ReactiveProcessorOptions =
            serde_json::from_str(options_json).map_err(|e| AedbError::Validation(e.to_string()))?;
        Ok(Some(ReactiveProcessorRegistration {
            processor_name: processor_name.to_string(),
            options,
            enabled: *enabled,
            updated_at_micros,
        }))
    }

    fn build_reactive_processor_slo_status(
        options: &ReactiveProcessorOptions,
        health: ReactiveProcessorHealth,
    ) -> ReactiveProcessorSloStatus {
        let now = system_now_micros();
        let reference_ts = health
            .last_success_micros
            .or(health.last_run_completed_micros);
        let stall_ms = reference_ts.map(|ts| now.saturating_sub(ts) / 1_000);
        let mut reasons = Vec::new();
        let mut breached = false;
        if health.enabled {
            if let Some(max_lag) = options.max_allowed_lag_commits
                && health.lag_commits > max_lag
            {
                breached = true;
                reasons.push(format!(
                    "lag_commits={} exceeds max_allowed_lag_commits={}",
                    health.lag_commits, max_lag
                ));
            }
            if let Some(max_stall) = options.max_allowed_stall_ms {
                match stall_ms {
                    Some(stall) if stall > max_stall => {
                        breached = true;
                        reasons.push(format!(
                            "stall_ms={} exceeds max_allowed_stall_ms={}",
                            stall, max_stall
                        ));
                    }
                    None => {
                        breached = true;
                        reasons.push(
                            "stall_ms unavailable; processor has not produced a completed run"
                                .to_string(),
                        );
                    }
                    _ => {}
                }
            }
        }
        ReactiveProcessorSloStatus {
            processor_name: health.processor_name.clone(),
            breached,
            enabled: health.enabled,
            running: health.running,
            lag_commits: health.lag_commits,
            max_allowed_lag_commits: options.max_allowed_lag_commits,
            stall_ms,
            max_allowed_stall_ms: options.max_allowed_stall_ms,
            reasons,
        }
    }

    async fn fetch_reactive_processor_batch(
        db: Arc<Self>,
        processor_name: &str,
        caller: Option<&CallerContext>,
        options: &ReactiveProcessorOptions,
    ) -> Result<Vec<EventOutboxRecord>, AedbError> {
        let lag = if let Some(caller) = caller {
            db.reactive_processor_lag_as(caller, processor_name, ConsistencyMode::AtLatest)
                .await?
        } else {
            db.reactive_processor_lag(processor_name, ConsistencyMode::AtLatest)
                .await?
        };
        let mut from_seq = lag.checkpoint_seq;
        let deadline = Instant::now() + Duration::from_millis(options.max_run_duration_ms);
        let mut events = Vec::new();
        let mut bytes = 0usize;
        'read: while events.len() < options.max_events_per_run
            && bytes < options.max_bytes_per_run
            && Instant::now() < deadline
        {
            let limit = (options.max_events_per_run - events.len()).clamp(1, 128);
            let page = if let Some(caller) = caller {
                db.read_event_stream_as(
                    caller,
                    options.topic_filter.as_deref(),
                    from_seq,
                    limit,
                    ConsistencyMode::AtLatest,
                )
                .await?
            } else {
                db.read_event_stream(
                    options.topic_filter.as_deref(),
                    from_seq,
                    limit,
                    ConsistencyMode::AtLatest,
                )
                .await?
            };
            if page.events.is_empty() {
                break;
            }
            for event in page.events {
                let approx_bytes =
                    event.payload_json.len() + event.topic.len() + event.event_key.len() + 64;
                if !events.is_empty()
                    && bytes.saturating_add(approx_bytes) > options.max_bytes_per_run
                {
                    break 'read;
                }
                bytes = bytes.saturating_add(approx_bytes);
                from_seq = event.commit_seq;
                events.push(event);
                if events.len() >= options.max_events_per_run
                    || bytes >= options.max_bytes_per_run
                    || Instant::now() >= deadline
                {
                    break 'read;
                }
            }
            if page.next_commit_seq.is_none() {
                break;
            }
        }
        Ok(events)
    }

    async fn process_reactive_processor_batch(
        db: Arc<Self>,
        processor_name: &str,
        caller: Option<&CallerContext>,
        options: &ReactiveProcessorOptions,
        handler: &ReactiveProcessorHandler,
        events: Vec<EventOutboxRecord>,
        last_seq: u64,
    ) -> Result<(u64, u64), AedbError> {
        if events.is_empty() {
            handler(Arc::clone(&db), events).await?;
            return Ok((0, last_seq));
        }
        let processed = events.len() as u64;
        handler(Arc::clone(&db), events).await?;
        let _ = if let Some(caller) = caller {
            db.ack_reactive_processor_checkpoint_batched_as(
                caller.clone(),
                processor_name,
                last_seq,
                options.checkpoint_watermark_commits,
            )
            .await?
        } else {
            db.ack_reactive_processor_checkpoint_batched(
                processor_name,
                last_seq,
                options.checkpoint_watermark_commits,
            )
            .await?
        };
        Ok((processed, last_seq))
    }

    async fn dead_letter_reactive_processor_batch(
        &self,
        processor_name: &str,
        caller: Option<&CallerContext>,
        events: &[EventOutboxRecord],
        error: &str,
        attempts: u32,
    ) -> Result<CommitResult, AedbError> {
        let mutation = reactive_processor_dlq_mutation(processor_name, events, error, attempts);
        if let Some(caller) = caller {
            self.commit_as(caller.clone(), mutation).await
        } else {
            self.commit_prevalidated_system_internal("reactive_processor_dead_letter", mutation)
                .await
        }
    }
}
