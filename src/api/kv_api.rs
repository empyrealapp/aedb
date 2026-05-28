use crate::catalog::DEFAULT_SCOPE_ID;
use crate::catalog::types::Value;
use crate::commit::tx::{ReadAssertion, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use crate::commit::validation::{
    KvU64MissingPolicy, KvU64MutatorOp, KvU64OverflowPolicy, KvU64UnderflowPolicy, KvU256MutatorOp,
    MAX_COUNTER_SHARDS, Mutation,
};
use crate::error::AedbError;
use crate::lib_helpers::next_prefix_bytes;
use crate::permission::{CallerContext, Permission};
use crate::query::error::QueryError;
use crate::query::plan::ConsistencyMode;
use crate::query::{KvCursor, KvScanResult, ScopedKvEntry};
use crate::query_authorization::ensure_external_caller_allowed;
use crate::storage::keyspace::KvEntry;
use crate::{
    AedbInstance, CommitResult, ENVELOPE_OVERHEAD_UPPER_BOUND, MUTATION_OVERHEAD_UPPER_BOUND,
    TableU256MutationRequest,
};
use std::ops::Bound;
use std::time::Instant;

impl AedbInstance {
    pub async fn kv_get(
        &self,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<Option<KvEntry>, QueryError> {
        ensure_external_caller_allowed(caller)?;
        let snapshot_started = Instant::now();
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot_micros = snapshot_started.elapsed().as_micros() as u64;
        let snapshot = &lease.view.keyspace;
        let catalog = &lease.view.catalog;
        if !catalog.has_kv_read_permission(&caller.caller_id, project_id, scope_id, key) {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.{scope_id})"),
                scope: caller.caller_id.clone(),
            });
        }
        let execute_started = Instant::now();
        let result = snapshot
            .try_kv_get(project_id, scope_id, key)
            .map_err(QueryError::from)?;
        let execute_micros = execute_started.elapsed().as_micros() as u64;
        self.maybe_log_read_phase(
            "kv_get",
            consistency,
            snapshot_micros,
            execute_micros,
            usize::from(result.is_some()),
            true,
        );
        Ok(result)
    }

    pub async fn kv_get_default_scope(
        &self,
        project_id: &str,
        key: &[u8],
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<Option<KvEntry>, QueryError> {
        self.kv_get(project_id, DEFAULT_SCOPE_ID, key, consistency, caller)
            .await
    }

    pub(crate) async fn kv_get_unchecked(
        &self,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        consistency: ConsistencyMode,
    ) -> Result<Option<KvEntry>, QueryError> {
        let snapshot_started = Instant::now();
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot_micros = snapshot_started.elapsed().as_micros() as u64;
        let execute_started = Instant::now();
        let result = lease
            .view
            .keyspace
            .try_kv_get(project_id, scope_id, key)
            .map_err(QueryError::from)?;
        let execute_micros = execute_started.elapsed().as_micros() as u64;
        self.maybe_log_read_phase(
            "kv_get",
            consistency,
            snapshot_micros,
            execute_micros,
            usize::from(result.is_some()),
            true,
        );
        Ok(result)
    }

    pub async fn kv_get_no_auth(
        &self,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        consistency: ConsistencyMode,
    ) -> Result<Option<KvEntry>, QueryError> {
        if self.require_authenticated_calls {
            return Err(QueryError::PermissionDenied {
                permission: "kv_get_no_auth is unavailable in secure mode".into(),
                scope: "anonymous".into(),
            });
        }
        self.kv_get_unchecked(project_id, scope_id, key, consistency)
            .await
    }

    pub async fn kv_get_many_no_auth(
        &self,
        project_id: &str,
        scope_id: &str,
        keys: &[Vec<u8>],
        consistency: ConsistencyMode,
    ) -> Result<Vec<Option<KvEntry>>, QueryError> {
        if self.require_authenticated_calls {
            return Err(QueryError::PermissionDenied {
                permission: "kv_get_many_no_auth is unavailable in secure mode".into(),
                scope: "anonymous".into(),
            });
        }

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let snapshot_started = Instant::now();
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot_micros = snapshot_started.elapsed().as_micros() as u64;
        let snapshot = &lease.view.keyspace;
        let execute_started = Instant::now();
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            out.push(
                snapshot
                    .try_kv_get(project_id, scope_id, key)
                    .map_err(QueryError::from)?,
            );
        }
        let execute_micros = execute_started.elapsed().as_micros() as u64;
        self.maybe_log_read_phase(
            "kv_get_many",
            consistency,
            snapshot_micros,
            execute_micros,
            keys.len(),
            true,
        );
        Ok(out)
    }

    pub(crate) async fn kv_scan_prefix_unchecked(
        &self,
        project_id: &str,
        scope_id: &str,
        prefix: &[u8],
        limit: u64,
        consistency: ConsistencyMode,
    ) -> Result<Vec<(Vec<u8>, KvEntry)>, QueryError> {
        let snapshot_started = Instant::now();
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot_micros = snapshot_started.elapsed().as_micros() as u64;
        let page_size = limit.min(self._config.max_scan_rows as u64) as usize;
        let start_bound = Bound::Included(prefix.to_vec());
        let end_bound = next_prefix_bytes(prefix).map_or(Bound::Unbounded, Bound::Excluded);
        let execute_started = Instant::now();
        let entries = lease
            .view
            .keyspace
            .try_kv_scan_range(project_id, scope_id, start_bound, end_bound, page_size)
            .map_err(QueryError::from)?;
        let execute_micros = execute_started.elapsed().as_micros() as u64;
        self.maybe_log_read_phase(
            "kv_scan_prefix",
            consistency,
            snapshot_micros,
            execute_micros,
            entries.len(),
            true,
        );
        Ok(entries)
    }

    pub async fn kv_scan_prefix_no_auth(
        &self,
        project_id: &str,
        scope_id: &str,
        prefix: &[u8],
        limit: u64,
        consistency: ConsistencyMode,
    ) -> Result<Vec<(Vec<u8>, KvEntry)>, QueryError> {
        if self.require_authenticated_calls {
            return Err(QueryError::PermissionDenied {
                permission: "kv_scan_prefix_no_auth is unavailable in secure mode".into(),
                scope: "anonymous".into(),
            });
        }
        self.kv_scan_prefix_unchecked(project_id, scope_id, prefix, limit, consistency)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn kv_scan_prefix(
        &self,
        project_id: &str,
        scope_id: &str,
        prefix: &[u8],
        limit: u64,
        cursor: Option<KvCursor>,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<KvScanResult, QueryError> {
        ensure_external_caller_allowed(caller)?;
        let effective_consistency = if let Some(c) = &cursor {
            ConsistencyMode::AtSeq(c.snapshot_seq)
        } else {
            consistency
        };
        let snapshot_started = Instant::now();
        let lease = self
            .acquire_snapshot(effective_consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot_micros = snapshot_started.elapsed().as_micros() as u64;
        let snapshot = &lease.view.keyspace;
        let catalog = &lease.view.catalog;
        let snapshot_seq = lease.view.seq;
        let allowed_prefixes =
            match catalog.kv_read_prefixes_for_caller(&caller.caller_id, project_id, scope_id) {
                Some(prefixes) => prefixes,
                None => {
                    return Err(QueryError::PermissionDenied {
                        permission: format!("KvRead({project_id}.{scope_id})"),
                        scope: caller.caller_id.clone(),
                    });
                }
            };
        if let Some(c) = &cursor
            && c.snapshot_seq != snapshot_seq
        {
            return Err(QueryError::InvalidQuery {
                reason: "cursor snapshot_seq mismatch".into(),
            });
        }
        let page_size = limit.min(self._config.max_scan_rows as u64) as usize;
        let start_bound = cursor
            .as_ref()
            .map_or(Bound::Included(prefix.to_vec()), |c| {
                Bound::Excluded(c.last_key.clone())
            });
        let end_bound = next_prefix_bytes(prefix).map_or(Bound::Unbounded, Bound::Excluded);

        let execute_started = Instant::now();
        let scan_limit = if allowed_prefixes.is_empty() {
            page_size + 1
        } else {
            self._config.max_scan_rows
        };
        let mut entries = snapshot
            .try_kv_scan_range(project_id, scope_id, start_bound, end_bound, scan_limit)
            .map_err(QueryError::from)?;
        if !allowed_prefixes.is_empty() {
            entries.retain(|(k, _)| {
                allowed_prefixes
                    .iter()
                    .any(|allowed| k.starts_with(allowed))
            });
        }

        let truncated = entries.len() > page_size;
        if truncated {
            entries.truncate(page_size);
        }
        let execute_micros = execute_started.elapsed().as_micros() as u64;
        let next_cursor = if truncated {
            entries.last().map(|(k, _)| KvCursor {
                snapshot_seq,
                last_key: k.clone(),
                page_size: page_size as u64,
            })
        } else {
            None
        };
        Ok(KvScanResult {
            entries,
            cursor: next_cursor,
            snapshot_seq,
            truncated,
        })
        .inspect(|res| {
            self.maybe_log_read_phase(
                "kv_scan_prefix",
                effective_consistency,
                snapshot_micros,
                execute_micros,
                res.entries.len(),
                true,
            );
        })
    }

    pub async fn kv_scan_prefix_default_scope(
        &self,
        project_id: &str,
        prefix: &[u8],
        limit: u64,
        cursor: Option<KvCursor>,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<KvScanResult, QueryError> {
        self.kv_scan_prefix(
            project_id,
            DEFAULT_SCOPE_ID,
            prefix,
            limit,
            cursor,
            consistency,
            caller,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn kv_scan_range(
        &self,
        project_id: &str,
        scope_id: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        limit: u64,
        cursor: Option<KvCursor>,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<KvScanResult, QueryError> {
        ensure_external_caller_allowed(caller)?;
        let effective_consistency = if let Some(c) = &cursor {
            ConsistencyMode::AtSeq(c.snapshot_seq)
        } else {
            consistency
        };
        let snapshot_started = Instant::now();
        let lease = self
            .acquire_snapshot(effective_consistency)
            .await
            .map_err(QueryError::from)?;
        let snapshot_micros = snapshot_started.elapsed().as_micros() as u64;
        let snapshot = &lease.view.keyspace;
        let catalog = &lease.view.catalog;
        let snapshot_seq = lease.view.seq;
        let allowed_prefixes =
            match catalog.kv_read_prefixes_for_caller(&caller.caller_id, project_id, scope_id) {
                Some(prefixes) => prefixes,
                None => {
                    return Err(QueryError::PermissionDenied {
                        permission: format!("KvRead({project_id}.{scope_id})"),
                        scope: caller.caller_id.clone(),
                    });
                }
            };
        if let Some(c) = &cursor
            && c.snapshot_seq != snapshot_seq
        {
            return Err(QueryError::InvalidQuery {
                reason: "cursor snapshot_seq mismatch".into(),
            });
        }
        let page_size = limit.min(self._config.max_scan_rows as u64) as usize;
        let adjusted_start = match (&cursor, start) {
            (Some(c), _) => Bound::Excluded(c.last_key.clone()),
            (None, b) => b,
        };

        let execute_started = Instant::now();
        let scan_limit = if allowed_prefixes.is_empty() {
            page_size + 1
        } else {
            self._config.max_scan_rows
        };
        let mut entries = snapshot
            .try_kv_scan_range(project_id, scope_id, adjusted_start, end, scan_limit)
            .map_err(QueryError::from)?;
        if !allowed_prefixes.is_empty() {
            entries.retain(|(k, _)| {
                allowed_prefixes
                    .iter()
                    .any(|allowed| k.starts_with(allowed))
            });
        }

        let truncated = entries.len() > page_size;
        if truncated {
            entries.truncate(page_size);
        }
        let execute_micros = execute_started.elapsed().as_micros() as u64;
        let next_cursor = if truncated {
            entries.last().map(|(k, _)| KvCursor {
                snapshot_seq,
                last_key: k.clone(),
                page_size: page_size as u64,
            })
        } else {
            None
        };
        Ok(KvScanResult {
            entries,
            cursor: next_cursor,
            snapshot_seq,
            truncated,
        })
        .inspect(|res| {
            self.maybe_log_read_phase(
                "kv_scan_range",
                effective_consistency,
                snapshot_micros,
                execute_micros,
                res.entries.len(),
                true,
            );
        })
    }

    pub async fn kv_scan_all_scopes(
        &self,
        project_id: &str,
        prefix: &[u8],
        limit: u64,
        consistency: ConsistencyMode,
        caller: &CallerContext,
    ) -> Result<Vec<ScopedKvEntry>, QueryError> {
        ensure_external_caller_allowed(caller)?;
        let lease = self
            .acquire_snapshot(consistency)
            .await
            .map_err(QueryError::from)?;
        let project_read = lease.view.catalog.has_permission(
            &caller.caller_id,
            &Permission::KvRead {
                project_id: project_id.to_string(),
                scope_id: None,
                prefix: None,
            },
        ) || lease.view.catalog.has_permission(
            &caller.caller_id,
            &Permission::ProjectAdmin {
                project_id: project_id.to_string(),
            },
        );
        if !project_read {
            return Err(QueryError::PermissionDenied {
                permission: format!("KvRead({project_id}.*)"),
                scope: caller.caller_id.clone(),
            });
        }
        let snapshot = &lease.view.keyspace;
        let mut out = Vec::new();
        for (ns_id, _) in snapshot.namespaces.iter() {
            let Some(ns_key) = ns_id.as_project_scope_key() else {
                continue;
            };
            let Some((p, scope)) = ns_key.split_once("::") else {
                continue;
            };
            if p != project_id {
                continue;
            }
            let remaining = (limit as usize).saturating_sub(out.len());
            if remaining == 0 {
                return Ok(out);
            }
            for (k, v) in snapshot
                .try_kv_scan_prefix(project_id, scope, prefix, remaining)
                .map_err(QueryError::from)?
            {
                out.push(ScopedKvEntry {
                    scope_id: scope.to_string(),
                    key: k,
                    value: v.value,
                    version: v.version,
                });
                if out.len() >= limit as usize {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }

    pub async fn kv_set(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvSet {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            value,
        })
        .await
    }

    pub async fn kv_set_default_scope(
        &self,
        project_id: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<CommitResult, AedbError> {
        self.kv_set(project_id, DEFAULT_SCOPE_ID, key, value).await
    }

    pub async fn kv_set_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvSet {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                value,
            },
        )
        .await
    }

    pub async fn kv_set_many_atomic(
        &self,
        project_id: &str,
        scope_id: &str,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<CommitResult, AedbError> {
        if entries.is_empty() {
            return Err(AedbError::Validation(
                "kv_set_many_atomic requires at least one entry".into(),
            ));
        }
        let project_id_owned = project_id.to_string();
        let scope_id_owned = scope_id.to_string();
        let mut encoded_size_hint = Some(ENVELOPE_OVERHEAD_UPPER_BOUND);
        let mut mutations = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            encoded_size_hint = encoded_size_hint.and_then(|size| {
                size.checked_add(
                    MUTATION_OVERHEAD_UPPER_BOUND
                        .saturating_add(project_id.len())
                        .saturating_add(scope_id.len())
                        .saturating_add(key.len())
                        .saturating_add(value.len()),
                )
            });
            mutations.push(Mutation::KvSet {
                project_id: project_id_owned.clone(),
                scope_id: scope_id_owned.clone(),
                key,
                value,
            });
        }
        self.commit_envelope_with_size_hint(
            TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: Vec::new(),
                read_set: ReadSet::default(),
                write_intent: WriteIntent { mutations },
                base_seq: 0,
            },
            encoded_size_hint,
        )
        .await
    }

    pub async fn kv_set_many_atomic_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<CommitResult, AedbError> {
        if entries.is_empty() {
            return Err(AedbError::Validation(
                "kv_set_many_atomic_as requires at least one entry".into(),
            ));
        }
        let project_id_owned = project_id.to_string();
        let scope_id_owned = scope_id.to_string();
        let mut encoded_size_hint = Some(ENVELOPE_OVERHEAD_UPPER_BOUND);
        let mut mutations = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            encoded_size_hint = encoded_size_hint.and_then(|size| {
                size.checked_add(
                    MUTATION_OVERHEAD_UPPER_BOUND
                        .saturating_add(project_id.len())
                        .saturating_add(scope_id.len())
                        .saturating_add(key.len())
                        .saturating_add(value.len()),
                )
            });
            mutations.push(Mutation::KvSet {
                project_id: project_id_owned.clone(),
                scope_id: scope_id_owned.clone(),
                key,
                value,
            });
        }
        self.commit_envelope_with_size_hint(
            TransactionEnvelope {
                caller: Some(caller),
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: Vec::new(),
                read_set: ReadSet::default(),
                write_intent: WriteIntent { mutations },
                base_seq: 0,
            },
            encoded_size_hint,
        )
        .await
    }

    pub async fn kv_del(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvDel {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
        })
        .await
    }

    pub async fn kv_del_default_scope(
        &self,
        project_id: &str,
        key: Vec<u8>,
    ) -> Result<CommitResult, AedbError> {
        self.kv_del(project_id, DEFAULT_SCOPE_ID, key).await
    }

    pub async fn kv_del_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvDel {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
            },
        )
        .await
    }

    pub async fn kv_inc_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvIncU256 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            amount_be,
        })
        .await
    }

    pub async fn kv_inc_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvIncU256 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                amount_be,
            },
        )
        .await
    }

    pub async fn kv_dec_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvDecU256 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            amount_be,
        })
        .await
    }

    pub async fn kv_dec_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvDecU256 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                amount_be,
            },
        )
        .await
    }

    pub async fn kv_add_u64_ex(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        on_missing: KvU64MissingPolicy,
        on_overflow: KvU64OverflowPolicy,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvAddU64Ex {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            amount_be,
            on_missing,
            on_overflow,
        })
        .await
    }

    pub async fn kv_sub_u64_ex(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        on_missing: KvU64MissingPolicy,
        on_underflow: KvU64UnderflowPolicy,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvSubU64Ex {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            amount_be,
            on_missing,
            on_underflow,
        })
        .await
    }

    pub async fn kv_max_u64(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        candidate_be: [u8; 8],
        on_missing: KvU64MissingPolicy,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMaxU64 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            candidate_be,
            on_missing,
        })
        .await
    }

    pub async fn kv_min_u64(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        candidate_be: [u8; 8],
        on_missing: KvU64MissingPolicy,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMinU64 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            candidate_be,
            on_missing,
        })
        .await
    }

    pub async fn kv_mutate_u64(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        op: KvU64MutatorOp,
        operand_be: [u8; 8],
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMutateU64 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            op,
            operand_be,
            expected_seq: None,
        })
        .await
    }

    pub async fn kv_mutate_u64_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        op: KvU64MutatorOp,
        operand_be: [u8; 8],
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvMutateU64 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                op,
                operand_be,
                expected_seq: None,
            },
        )
        .await
    }

    pub async fn counter_add_sharded(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        shard_count: u16,
        shard_hint: u32,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::CounterAdd {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            amount_be,
            shard_count,
            shard_hint,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn counter_add_sharded_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        shard_count: u16,
        shard_hint: u32,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::CounterAdd {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                amount_be,
                shard_count,
                shard_hint,
            },
        )
        .await
    }

    pub async fn counter_read_sharded(
        &self,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        shard_count: u16,
        consistency: ConsistencyMode,
    ) -> Result<u64, AedbError> {
        if shard_count == 0 {
            return Err(AedbError::Validation(
                "counter shard_count must be > 0".into(),
            ));
        }
        if shard_count > MAX_COUNTER_SHARDS {
            return Err(AedbError::Validation(format!(
                "counter shard_count exceeds maximum {}",
                MAX_COUNTER_SHARDS
            )));
        }
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        let lease = self.acquire_snapshot(consistency).await?;
        lease
            .view
            .keyspace
            .counter_read_sharded(project_id, scope_id, key, shard_count)
    }

    pub async fn counter_read_sharded_as(
        &self,
        caller: &CallerContext,
        project_id: &str,
        scope_id: &str,
        key: &[u8],
        shard_count: u16,
        consistency: ConsistencyMode,
    ) -> Result<u64, AedbError> {
        if shard_count == 0 {
            return Err(AedbError::Validation(
                "counter shard_count must be > 0".into(),
            ));
        }
        if shard_count > MAX_COUNTER_SHARDS {
            return Err(AedbError::Validation(format!(
                "counter shard_count exceeds maximum {}",
                MAX_COUNTER_SHARDS
            )));
        }
        ensure_external_caller_allowed(caller)?;
        let lease = self.acquire_snapshot(consistency).await?;
        if !lease
            .view
            .catalog
            .has_kv_read_permission(&caller.caller_id, project_id, scope_id, key)
        {
            return Err(AedbError::PermissionDenied(format!(
                "caller={} missing kv read permission for counter",
                caller.caller_id
            )));
        }
        lease
            .view
            .keyspace
            .counter_read_sharded(project_id, scope_id, key, shard_count)
    }

    pub async fn kv_compare_and_swap(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value: Vec<u8>,
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::KeyVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key: key.clone(),
                expected_seq,
            }],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    key,
                    value,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    pub async fn kv_compare_and_swap_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value: Vec<u8>,
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_envelope(TransactionEnvelope {
            caller: Some(caller),
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::KeyVersion {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key: key.clone(),
                expected_seq,
            }],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    key,
                    value,
                }],
            },
            base_seq: self.snapshot_probe(ConsistencyMode::AtLatest).await?,
        })
        .await
    }

    pub async fn kv_compare_and_inc_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMutateU256 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            op: KvU256MutatorOp::Add,
            operand_be: amount_be,
            expected_seq: Some(expected_seq),
        })
        .await
    }

    pub async fn kv_compare_and_add_u64(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMutateU64 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            op: KvU64MutatorOp::Add,
            operand_be: amount_be,
            expected_seq: Some(expected_seq),
        })
        .await
    }

    pub async fn kv_compare_and_add_u64_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvMutateU64 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                op: KvU64MutatorOp::Add,
                operand_be: amount_be,
                expected_seq: Some(expected_seq),
            },
        )
        .await
    }

    pub async fn kv_compare_and_set_u64(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value_be: [u8; 8],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMutateU64 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            op: KvU64MutatorOp::Set,
            operand_be: value_be,
            expected_seq: Some(expected_seq),
        })
        .await
    }

    pub async fn kv_compare_and_set_u64_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value_be: [u8; 8],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvMutateU64 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                op: KvU64MutatorOp::Set,
                operand_be: value_be,
                expected_seq: Some(expected_seq),
            },
        )
        .await
    }

    pub async fn kv_compare_and_inc_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvMutateU256 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                op: KvU256MutatorOp::Add,
                operand_be: amount_be,
                expected_seq: Some(expected_seq),
            },
        )
        .await
    }

    pub async fn kv_compare_and_set_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMutateU256 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            op: KvU256MutatorOp::Set,
            operand_be: value_be,
            expected_seq: Some(expected_seq),
        })
        .await
    }

    pub async fn kv_compare_and_dec_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMutateU256 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            op: KvU256MutatorOp::Sub,
            operand_be: amount_be,
            expected_seq: Some(expected_seq),
        })
        .await
    }

    pub async fn kv_compare_and_sub_u64(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMutateU64 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            op: KvU64MutatorOp::Sub,
            operand_be: amount_be,
            expected_seq: Some(expected_seq),
        })
        .await
    }

    pub async fn kv_compare_and_sub_u64_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 8],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvMutateU64 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                op: KvU64MutatorOp::Sub,
                operand_be: amount_be,
                expected_seq: Some(expected_seq),
            },
        )
        .await
    }

    pub async fn kv_compare_and_set_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvMutateU256 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                op: KvU256MutatorOp::Set,
                operand_be: value_be,
                expected_seq: Some(expected_seq),
            },
        )
        .await
    }

    pub async fn kv_mutate_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        op: KvU256MutatorOp,
        operand_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::KvMutateU256 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key,
            op,
            operand_be,
            expected_seq: None,
        })
        .await
    }

    pub async fn kv_mutate_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        op: KvU256MutatorOp,
        operand_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvMutateU256 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                op,
                operand_be,
                expected_seq: None,
            },
        )
        .await
    }

    pub async fn kv_compare_and_dec_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            caller,
            Mutation::KvMutateU256 {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key,
                op: KvU256MutatorOp::Sub,
                operand_be: amount_be,
                expected_seq: Some(expected_seq),
            },
        )
        .await
    }

    pub async fn table_inc_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::TableIncU256 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            primary_key,
            column: column.to_string(),
            amount_be,
        })
        .await
    }

    pub async fn table_inc_u256_as_with(
        &self,
        request: TableU256MutationRequest,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            request.caller,
            Mutation::TableIncU256 {
                project_id: request.project_id,
                scope_id: request.scope_id,
                table_name: request.table_name,
                primary_key: request.primary_key,
                column: request.column,
                amount_be: request.amount_be,
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn table_inc_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.table_inc_u256_as_with(TableU256MutationRequest {
            caller,
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            primary_key,
            column: column.to_string(),
            amount_be,
        })
        .await
    }

    pub async fn table_dec_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.commit(Mutation::TableDecU256 {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            primary_key,
            column: column.to_string(),
            amount_be,
        })
        .await
    }

    pub async fn table_dec_u256_as_with(
        &self,
        request: TableU256MutationRequest,
    ) -> Result<CommitResult, AedbError> {
        self.commit_as(
            request.caller,
            Mutation::TableDecU256 {
                project_id: request.project_id,
                scope_id: request.scope_id,
                table_name: request.table_name,
                primary_key: request.primary_key,
                column: request.column,
                amount_be: request.amount_be,
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn table_dec_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
    ) -> Result<CommitResult, AedbError> {
        self.table_dec_u256_as_with(TableU256MutationRequest {
            caller,
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: table_name.to_string(),
            primary_key,
            column: column.to_string(),
            amount_be,
        })
        .await
    }
}
