use crate::AedbInstance;
use crate::CompareAndSwapRequest;
use crate::DdlBatchResult;
use crate::DdlResult;
use crate::MutateWhereReturningResult;
use crate::catalog::DdlOperation;
use crate::catalog::types::{Row, Value};
use crate::commit::executor::CommitResult;
use crate::commit::tx::{PreflightPlan, TransactionEnvelope};
use crate::commit::validation::{Mutation, TableUpdateExpr};
use crate::error::AedbError;
use crate::permission::CallerContext;
use crate::preflight::PreflightResult;
use crate::query::error::QueryError;
use crate::query::executor::QueryResult;
use crate::query::plan::{Query, QueryOptions};
use std::future::Future;
use std::sync::Arc;
use tokio::runtime::Handle;

pub fn block_on_aedb<F, T>(rt: &Handle, f: F) -> T
where
    F: Future<Output = T>,
{
    match Handle::try_current() {
        Ok(_) => tokio::task::block_in_place(|| rt.block_on(f)),
        Err(_) => rt.block_on(f),
    }
}

pub struct AedbSync {
    inner: Arc<AedbInstance>,
    rt: Handle,
}

impl AedbSync {
    pub fn new(inner: Arc<AedbInstance>, rt: Handle) -> Self {
        Self { inner, rt }
    }

    pub fn commit(&self, mutation: Mutation) -> Result<CommitResult, AedbError> {
        block_on_aedb(&self.rt, self.inner.commit(mutation))
    }

    pub fn commit_as(
        &self,
        caller: CallerContext,
        mutation: Mutation,
    ) -> Result<CommitResult, AedbError> {
        block_on_aedb(&self.rt, self.inner.commit_as(caller, mutation))
    }

    pub fn commit_with_preflight(&self, mutation: Mutation) -> Result<CommitResult, AedbError> {
        block_on_aedb(&self.rt, self.inner.commit_with_preflight(mutation))
    }

    pub fn commit_as_with_preflight(
        &self,
        caller: CallerContext,
        mutation: Mutation,
    ) -> Result<CommitResult, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner.commit_as_with_preflight(caller, mutation),
        )
    }

    pub fn commit_envelope(
        &self,
        envelope: TransactionEnvelope,
    ) -> Result<CommitResult, AedbError> {
        block_on_aedb(&self.rt, self.inner.commit_envelope(envelope))
    }

    pub fn commit_ddl(&self, op: DdlOperation) -> Result<DdlResult, AedbError> {
        block_on_aedb(&self.rt, self.inner.commit_ddl(op))
    }

    pub fn commit_ddl_batch(&self, ops: Vec<DdlOperation>) -> Result<DdlBatchResult, AedbError> {
        block_on_aedb(&self.rt, self.inner.commit_ddl_batch(ops))
    }

    pub fn project_exists(&self, project_id: &str) -> Result<bool, AedbError> {
        block_on_aedb(&self.rt, self.inner.project_exists(project_id))
    }

    pub fn scope_exists(&self, project_id: &str, scope_id: &str) -> Result<bool, AedbError> {
        block_on_aedb(&self.rt, self.inner.scope_exists(project_id, scope_id))
    }

    pub fn table_exists(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
    ) -> Result<bool, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner.table_exists(project_id, scope_id, table_name),
        )
    }

    pub fn query(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
    ) -> Result<QueryResult, QueryError> {
        block_on_aedb(&self.rt, self.inner.query(project_id, scope_id, query))
    }

    pub fn query_no_auth(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryResult, QueryError> {
        block_on_aedb(
            &self.rt,
            self.inner
                .query_no_auth(project_id, scope_id, query, options),
        )
    }

    pub fn query_with_options(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryResult, QueryError> {
        block_on_aedb(
            &self.rt,
            self.inner
                .query_with_options(project_id, scope_id, query, options),
        )
    }

    pub fn preflight(&self, mutation: Mutation) -> PreflightResult {
        block_on_aedb(&self.rt, self.inner.preflight(mutation))
    }

    pub fn preflight_plan(&self, mutation: Mutation) -> PreflightPlan {
        block_on_aedb(&self.rt, self.inner.preflight_plan(mutation))
    }

    pub fn preflight_as(
        &self,
        caller: &CallerContext,
        mutation: Mutation,
    ) -> Result<PreflightResult, AedbError> {
        block_on_aedb(&self.rt, self.inner.preflight_as(caller, mutation))
    }

    pub fn preflight_plan_as(
        &self,
        caller: &CallerContext,
        mutation: Mutation,
    ) -> Result<PreflightPlan, AedbError> {
        block_on_aedb(&self.rt, self.inner.preflight_plan_as(caller, mutation))
    }

    pub fn query_page_stable(
        &self,
        project_id: &str,
        scope_id: &str,
        query: Query,
        cursor: Option<String>,
        page_size: usize,
        consistency: crate::query::plan::ConsistencyMode,
    ) -> Result<QueryResult, QueryError> {
        block_on_aedb(
            &self.rt,
            self.inner.query_page_stable(
                project_id,
                scope_id,
                query,
                cursor,
                page_size,
                consistency,
            ),
        )
    }

    pub fn commit_many_atomic(&self, mutations: Vec<Mutation>) -> Result<CommitResult, AedbError> {
        block_on_aedb(&self.rt, self.inner.commit_many_atomic(mutations))
    }

    pub fn commit_many_atomic_as(
        &self,
        caller: CallerContext,
        mutations: Vec<Mutation>,
    ) -> Result<CommitResult, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner.commit_many_atomic_as(caller, mutations),
        )
    }

    pub fn delete_where(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: crate::query::plan::Expr,
        limit: Option<usize>,
    ) -> Result<Option<CommitResult>, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner
                .delete_where(project_id, scope_id, table_name, predicate, limit),
        )
    }

    pub fn update_where(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: crate::query::plan::Expr,
        updates: Vec<(String, Value)>,
        limit: Option<usize>,
    ) -> Result<Option<CommitResult>, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner
                .update_where(project_id, scope_id, table_name, predicate, updates, limit),
        )
    }

    pub fn update_where_expr(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: crate::query::plan::Expr,
        updates: Vec<(String, TableUpdateExpr)>,
        limit: Option<usize>,
    ) -> Result<Option<CommitResult>, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner
                .update_where_expr(project_id, scope_id, table_name, predicate, updates, limit),
        )
    }

    pub fn mutate_where_returning(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: crate::query::plan::Expr,
        updates: Vec<(String, TableUpdateExpr)>,
    ) -> Result<Option<MutateWhereReturningResult>, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner
                .mutate_where_returning(project_id, scope_id, table_name, predicate, updates),
        )
    }

    pub fn claim_one(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        predicate: crate::query::plan::Expr,
        updates: Vec<(String, TableUpdateExpr)>,
    ) -> Result<Option<MutateWhereReturningResult>, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner
                .claim_one(project_id, scope_id, table_name, predicate, updates),
        )
    }

    pub fn compare_and_swap(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        row: Row,
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner.compare_and_swap(
                project_id,
                scope_id,
                table_name,
                primary_key,
                row,
                expected_seq,
            ),
        )
    }

    pub fn compare_and_swap_as_with(
        &self,
        request: CompareAndSwapRequest,
    ) -> Result<CommitResult, AedbError> {
        block_on_aedb(&self.rt, self.inner.compare_and_swap_as_with(request))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn compare_and_swap_as(
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
    }

    #[allow(clippy::too_many_arguments)]
    pub fn compare_and_inc_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner.compare_and_inc_u256(
                project_id,
                scope_id,
                table_name,
                primary_key,
                column,
                amount_be,
                expected_seq,
            ),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn compare_and_inc_u256_as(
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
        block_on_aedb(
            &self.rt,
            self.inner.compare_and_inc_u256_as(
                caller,
                project_id,
                scope_id,
                table_name,
                primary_key,
                column,
                amount_be,
                expected_seq,
            ),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn compare_and_dec_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        table_name: &str,
        primary_key: Vec<Value>,
        column: &str,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner.compare_and_dec_u256(
                project_id,
                scope_id,
                table_name,
                primary_key,
                column,
                amount_be,
                expected_seq,
            ),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn compare_and_dec_u256_as(
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
        block_on_aedb(
            &self.rt,
            self.inner.compare_and_dec_u256_as(
                caller,
                project_id,
                scope_id,
                table_name,
                primary_key,
                column,
                amount_be,
                expected_seq,
            ),
        )
    }

    pub fn kv_compare_and_swap(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value: Vec<u8>,
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner
                .kv_compare_and_swap(project_id, scope_id, key, value, expected_seq),
        )
    }

    pub fn kv_compare_and_swap_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        value: Vec<u8>,
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner.kv_compare_and_swap_as(
                caller,
                project_id,
                scope_id,
                key,
                value,
                expected_seq,
            ),
        )
    }

    pub fn kv_compare_and_inc_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner
                .kv_compare_and_inc_u256(project_id, scope_id, key, amount_be, expected_seq),
        )
    }

    pub fn kv_compare_and_inc_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner.kv_compare_and_inc_u256_as(
                caller,
                project_id,
                scope_id,
                key,
                amount_be,
                expected_seq,
            ),
        )
    }

    pub fn kv_compare_and_dec_u256(
        &self,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner
                .kv_compare_and_dec_u256(project_id, scope_id, key, amount_be, expected_seq),
        )
    }

    pub fn kv_compare_and_dec_u256_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        key: Vec<u8>,
        amount_be: [u8; 32],
        expected_seq: u64,
    ) -> Result<CommitResult, AedbError> {
        block_on_aedb(
            &self.rt,
            self.inner.kv_compare_and_dec_u256_as(
                caller,
                project_id,
                scope_id,
                key,
                amount_be,
                expected_seq,
            ),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DdlOperation;
    use crate::catalog::schema::ColumnDef;
    use crate::catalog::types::ColumnType;
    use crate::catalog::types::{Row, Value};
    use crate::commit::tx::{ReadAssertion, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
    use crate::config::AedbConfig;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::runtime::Runtime;

    #[test]
    fn sync_bridge_supports_envelope_assertions() {
        let rt = Runtime::new().expect("runtime");
        let dir = tempdir().expect("tempdir");
        let db = {
            let _guard = rt.enter();
            Arc::new(AedbInstance::open(AedbConfig::default(), dir.path()).expect("open"))
        };
        let sync = AedbSync::new(Arc::clone(&db), rt.handle().clone());

        sync.commit_ddl(DdlOperation::CreateProject {
            owner_id: None,
            project_id: "p".into(),
            if_not_exists: true,
        })
        .expect("create project");
        sync.commit_ddl(DdlOperation::CreateScope {
            owner_id: None,
            project_id: "p".into(),
            scope_id: "app".into(),
            if_not_exists: true,
        })
        .expect("create scope");
        sync.commit_ddl(DdlOperation::CreateTable {
            owner_id: None,
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
            if_not_exists: false,
        })
        .expect("create table");

        let seed = sync
            .commit(Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Text("u1".into())],
                row: Row {
                    values: vec![Value::Text("u1".into()), Value::Text("alice".into())],
                },
            })
            .expect("seed");

        let base_seq = seed.commit_seq;
        let ok = sync
            .commit_envelope(TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: vec![ReadAssertion::RowVersion {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    table_name: "users".into(),
                    primary_key: vec![Value::Text("u1".into())],
                    expected_seq: base_seq,
                }],
                read_set: ReadSet::default(),
                write_intent: WriteIntent {
                    mutations: vec![Mutation::Upsert {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        table_name: "users".into(),
                        primary_key: vec![Value::Text("u1".into())],
                        row: Row {
                            values: vec![Value::Text("u1".into()), Value::Text("alice2".into())],
                        },
                    }],
                },
                base_seq,
            })
            .expect("envelope commit");
        assert!(ok.commit_seq > base_seq);

        let stale = sync.commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::RowVersion {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Text("u1".into())],
                expected_seq: base_seq,
            }],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::Upsert {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    table_name: "users".into(),
                    primary_key: vec![Value::Text("u1".into())],
                    row: Row {
                        values: vec![Value::Text("u1".into()), Value::Text("alice3".into())],
                    },
                }],
            },
            base_seq,
        });
        assert!(matches!(stale, Err(AedbError::AssertionFailed { .. })));
    }
}
