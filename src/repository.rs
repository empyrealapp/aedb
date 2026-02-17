use crate::catalog::types::{Row, Value};
use crate::commit::executor::CommitResult;
use crate::commit::validation::Mutation;
use crate::error::AedbError;
use crate::permission::CallerContext;
use crate::query::error::QueryError;
use crate::query::executor::QueryResult;
use crate::query::plan::{ConsistencyMode, Query, QueryOptions};
use crate::{
    AedbInstance, ListPageResult, QueryBatchItem, QueryDiagnostics, ReadTx, SqlTransactionPlan,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRequest {
    pub limit: usize,
    pub cursor: Option<String>,
}

impl PageRequest {
    pub fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            cursor: None,
        }
    }

    pub fn with_cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
    pub snapshot_seq: u64,
    pub rows_examined: usize,
    pub materialized_seq: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PageWithTotal<T> {
    pub items: Vec<T>,
    pub total_count: usize,
    pub next_cursor: Option<String>,
    pub snapshot_seq: u64,
    pub rows_examined: usize,
}

pub trait TryFromRow: Sized {
    fn try_from_row(row: Row) -> Result<Self, RowDecodeError>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RowDecodeError {
    #[error("missing column '{column}' at index {index}")]
    MissingColumn { column: String, index: usize },
    #[error("column '{column}' type mismatch: expected {expected}, got {actual}")]
    TypeMismatch {
        column: String,
        expected: &'static str,
        actual: &'static str,
    },
    #[error("{message}")]
    Custom { message: String },
}

#[derive(Debug, thiserror::Error)]
pub enum RepositoryError {
    #[error(transparent)]
    Aedb(#[from] AedbError),
    #[error(transparent)]
    Query(#[from] QueryError),
    #[error(transparent)]
    Decode(#[from] RowDecodeError),
}

pub struct RepositoryContext<'a> {
    db: &'a AedbInstance,
    project_id: String,
    scope_id: String,
    caller: Option<CallerContext>,
    default_consistency: ConsistencyMode,
    allow_full_scan: bool,
}

impl<'a> RepositoryContext<'a> {
    pub fn new(
        db: &'a AedbInstance,
        project_id: impl Into<String>,
        scope_id: impl Into<String>,
    ) -> Self {
        Self {
            db,
            project_id: project_id.into(),
            scope_id: scope_id.into(),
            caller: None,
            default_consistency: ConsistencyMode::AtLatest,
            allow_full_scan: false,
        }
    }

    pub fn with_caller(mut self, caller: CallerContext) -> Self {
        self.caller = Some(caller);
        self
    }

    pub fn with_default_consistency(mut self, consistency: ConsistencyMode) -> Self {
        self.default_consistency = consistency;
        self
    }

    pub fn with_allow_full_scan(mut self, allow_full_scan: bool) -> Self {
        self.allow_full_scan = allow_full_scan;
        self
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn scope_id(&self) -> &str {
        &self.scope_id
    }

    pub fn caller(&self) -> Option<&CallerContext> {
        self.caller.as_ref()
    }

    pub fn base_query_options(&self) -> QueryOptions {
        QueryOptions {
            consistency: self.default_consistency,
            cursor: None,
            async_index: None,
            allow_full_scan: self.allow_full_scan,
        }
    }

    pub async fn commit_checked(&self, mutation: Mutation) -> Result<CommitResult, AedbError> {
        match &self.caller {
            Some(caller) => {
                self.db
                    .commit_as_with_preflight(caller.clone(), mutation)
                    .await
            }
            None => self.db.commit_with_preflight(mutation).await,
        }
    }

    pub async fn query(&self, query: Query) -> Result<QueryResult, QueryError> {
        self.query_with_options(query, self.base_query_options())
            .await
    }

    pub async fn query_with_options(
        &self,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryResult, QueryError> {
        match &self.caller {
            Some(caller) => {
                self.db
                    .query_with_options_as(
                        Some(caller),
                        &self.project_id,
                        &self.scope_id,
                        query,
                        options,
                    )
                    .await
            }
            None => {
                self.db
                    .query_with_options(&self.project_id, &self.scope_id, query, options)
                    .await
            }
        }
    }

    pub async fn query_page_rows(
        &self,
        mut query: Query,
        page: PageRequest,
    ) -> Result<Page<Row>, QueryError> {
        query.limit = Some(page.limit.max(1));
        let mut options = self.base_query_options();
        options.cursor = page.cursor;

        let result = self.query_with_options(query, options).await?;
        Ok(Page {
            items: result.rows,
            next_cursor: result.cursor,
            snapshot_seq: result.snapshot_seq,
            rows_examined: result.rows_examined,
            materialized_seq: result.materialized_seq,
        })
    }

    pub async fn query_page<T: TryFromRow>(
        &self,
        query: Query,
        page: PageRequest,
    ) -> Result<Page<T>, RepositoryError> {
        let rows = self.query_page_rows(query, page).await?;
        Ok(Page {
            items: decode_rows(rows.items)?,
            next_cursor: rows.next_cursor,
            snapshot_seq: rows.snapshot_seq,
            rows_examined: rows.rows_examined,
            materialized_seq: rows.materialized_seq,
        })
    }

    pub async fn query_page_with_total_rows(
        &self,
        query: Query,
        page: PageRequest,
        offset: Option<usize>,
    ) -> Result<ListPageResult, QueryError> {
        match &self.caller {
            Some(caller) => {
                self.db
                    .list_with_total_as(
                        caller.clone(),
                        &self.project_id,
                        &self.scope_id,
                        query,
                        page.cursor,
                        offset,
                        page.limit,
                        self.default_consistency,
                    )
                    .await
            }
            None => {
                self.db
                    .list_with_total(
                        &self.project_id,
                        &self.scope_id,
                        query,
                        page.cursor,
                        offset,
                        page.limit,
                        self.default_consistency,
                    )
                    .await
            }
        }
    }

    pub async fn query_page_with_total<T: TryFromRow>(
        &self,
        query: Query,
        page: PageRequest,
        offset: Option<usize>,
    ) -> Result<PageWithTotal<T>, RepositoryError> {
        let rows = self.query_page_with_total_rows(query, page, offset).await?;
        Ok(PageWithTotal {
            items: decode_rows(rows.rows)?,
            total_count: rows.total_count,
            next_cursor: rows.next_cursor,
            snapshot_seq: rows.snapshot_seq,
            rows_examined: rows.rows_examined,
        })
    }

    pub async fn query_batch(
        &self,
        items: Vec<QueryBatchItem>,
    ) -> Result<Vec<QueryResult>, QueryError> {
        match &self.caller {
            Some(caller) => {
                self.db
                    .query_batch_as(
                        caller.clone(),
                        &self.project_id,
                        &self.scope_id,
                        items,
                        self.default_consistency,
                    )
                    .await
            }
            None => {
                self.db
                    .query_batch(
                        &self.project_id,
                        &self.scope_id,
                        items,
                        self.default_consistency,
                    )
                    .await
            }
        }
    }

    pub async fn begin_read_tx(&self) -> Result<ReadTx<'a>, AedbError> {
        match &self.caller {
            Some(caller) => {
                self.db
                    .begin_read_tx_as(caller.clone(), self.default_consistency)
                    .await
            }
            None => self.db.begin_read_tx(self.default_consistency).await,
        }
    }

    pub async fn exists(&self, query: Query) -> Result<bool, QueryError> {
        match &self.caller {
            Some(caller) => {
                self.db
                    .exists_as(
                        caller.clone(),
                        &self.project_id,
                        &self.scope_id,
                        query,
                        self.default_consistency,
                    )
                    .await
            }
            None => {
                self.db
                    .exists(
                        &self.project_id,
                        &self.scope_id,
                        query,
                        self.default_consistency,
                    )
                    .await
            }
        }
    }

    pub async fn explain(
        &self,
        query: Query,
        options: QueryOptions,
    ) -> Result<QueryDiagnostics, QueryError> {
        let mut options = options;
        options.consistency = self.default_consistency;
        self.db
            .explain_query_as(
                self.caller.as_ref(),
                &self.project_id,
                &self.scope_id,
                query,
                options,
            )
            .await
    }

    pub async fn plan_sql_transaction(
        &self,
        mutations: Vec<Mutation>,
    ) -> Result<SqlTransactionPlan, AedbError> {
        match &self.caller {
            Some(caller) => {
                self.db
                    .plan_sql_transaction_as(caller.clone(), self.default_consistency, mutations)
                    .await
            }
            None => {
                self.db
                    .plan_sql_transaction(self.default_consistency, mutations)
                    .await
            }
        }
    }

    pub async fn commit_sql_transaction(
        &self,
        mutations: Vec<Mutation>,
    ) -> Result<CommitResult, AedbError> {
        match &self.caller {
            Some(caller) => {
                self.db
                    .commit_sql_transaction_as(caller.clone(), self.default_consistency, mutations)
                    .await
            }
            None => {
                self.db
                    .commit_sql_transaction(self.default_consistency, mutations)
                    .await
            }
        }
    }
}

pub fn decode_rows<T: TryFromRow>(rows: Vec<Row>) -> Result<Vec<T>, RowDecodeError> {
    rows.into_iter().map(T::try_from_row).collect()
}

pub fn text_at<'a>(row: &'a Row, index: usize, column: &str) -> Result<&'a str, RowDecodeError> {
    match row.values.get(index) {
        Some(Value::Text(v)) => Ok(v.as_str()),
        Some(other) => Err(RowDecodeError::TypeMismatch {
            column: column.to_string(),
            expected: "Text",
            actual: value_kind(other),
        }),
        None => Err(RowDecodeError::MissingColumn {
            column: column.to_string(),
            index,
        }),
    }
}

pub fn i64_at(row: &Row, index: usize, column: &str) -> Result<i64, RowDecodeError> {
    match row.values.get(index) {
        Some(Value::Integer(v)) => Ok(*v),
        Some(other) => Err(RowDecodeError::TypeMismatch {
            column: column.to_string(),
            expected: "Integer",
            actual: value_kind(other),
        }),
        None => Err(RowDecodeError::MissingColumn {
            column: column.to_string(),
            index,
        }),
    }
}

pub fn bool_at(row: &Row, index: usize, column: &str) -> Result<bool, RowDecodeError> {
    match row.values.get(index) {
        Some(Value::Boolean(v)) => Ok(*v),
        Some(other) => Err(RowDecodeError::TypeMismatch {
            column: column.to_string(),
            expected: "Boolean",
            actual: value_kind(other),
        }),
        None => Err(RowDecodeError::MissingColumn {
            column: column.to_string(),
            index,
        }),
    }
}

pub fn timestamp_at(row: &Row, index: usize, column: &str) -> Result<i64, RowDecodeError> {
    match row.values.get(index) {
        Some(Value::Timestamp(v)) => Ok(*v),
        Some(other) => Err(RowDecodeError::TypeMismatch {
            column: column.to_string(),
            expected: "Timestamp",
            actual: value_kind(other),
        }),
        None => Err(RowDecodeError::MissingColumn {
            column: column.to_string(),
            index,
        }),
    }
}

pub fn u256_at(row: &Row, index: usize, column: &str) -> Result<[u8; 32], RowDecodeError> {
    match row.values.get(index) {
        Some(Value::U256(v)) => Ok(*v),
        Some(other) => Err(RowDecodeError::TypeMismatch {
            column: column.to_string(),
            expected: "U256",
            actual: value_kind(other),
        }),
        None => Err(RowDecodeError::MissingColumn {
            column: column.to_string(),
            index,
        }),
    }
}

fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::Text(_) => "Text",
        Value::Integer(_) => "Integer",
        Value::Float(_) => "Float",
        Value::Boolean(_) => "Boolean",
        Value::U256(_) => "U256",
        Value::I256(_) => "I256",
        Value::Blob(_) => "Blob",
        Value::Timestamp(_) => "Timestamp",
        Value::Json(_) => "Json",
        Value::Null => "Null",
    }
}
