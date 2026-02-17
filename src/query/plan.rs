use crate::catalog::types::Value;
use crate::error::AedbError;
use serde::{Deserialize, Serialize};

/// Maximum nesting depth for expressions to prevent stack overflow
const MAX_EXPR_DEPTH: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expr {
    Eq(String, Value),
    Ne(String, Value),
    Lt(String, Value),
    Lte(String, Value),
    Gt(String, Value),
    Gte(String, Value),
    In(String, Vec<Value>),
    Between(String, Value, Value),
    IsNull(String),
    IsNotNull(String),
    Like(String, String),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}

impl Expr {
    pub fn and(self, rhs: Expr) -> Expr {
        Expr::And(Box::new(self), Box::new(rhs))
    }

    pub fn or(self, rhs: Expr) -> Expr {
        Expr::Or(Box::new(self), Box::new(rhs))
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Expr {
        Expr::Not(Box::new(self))
    }

    /// Calculates the maximum nesting depth of this expression tree.
    /// Used to prevent stack overflow from deeply nested expressions.
    pub fn depth(&self) -> usize {
        match self {
            // Leaf expressions have depth 1
            Expr::Eq(_, _)
            | Expr::Ne(_, _)
            | Expr::Lt(_, _)
            | Expr::Lte(_, _)
            | Expr::Gt(_, _)
            | Expr::Gte(_, _)
            | Expr::In(_, _)
            | Expr::Between(_, _, _)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::Like(_, _) => 1,
            // Unary operator adds 1 to child depth
            Expr::Not(inner) => 1 + inner.depth(),
            // Binary operators add 1 to max of children depths
            Expr::And(left, right) | Expr::Or(left, right) => 1 + left.depth().max(right.depth()),
        }
    }

    /// Validates that the expression depth does not exceed MAX_EXPR_DEPTH.
    /// Returns an error if the expression is too deeply nested.
    pub fn validate_depth(&self) -> Result<(), AedbError> {
        let depth = self.depth();
        if depth > MAX_EXPR_DEPTH {
            return Err(AedbError::Validation(format!(
                "expression depth {} exceeds maximum allowed depth of {}",
                depth, MAX_EXPR_DEPTH
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub select: Vec<String>,
    pub table: String,
    pub table_alias: Option<String>,
    pub joins: Vec<JoinSpec>,
    pub predicate: Option<Expr>,
    pub order_by: Vec<(String, Order)>,
    pub limit: Option<usize>,
    pub group_by: Vec<String>,
    pub aggregates: Vec<Aggregate>,
    pub having: Option<Expr>,
    pub use_index: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinSpec {
    pub table: String,
    pub alias: Option<String>,
    pub join_type: JoinType,
    pub left_column: Option<String>,
    pub right_column: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
    pub output_columns: Vec<String>,
    pub index_used: Option<String>,
    pub estimated_scan_rows: u64,
    pub has_residual_filter: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Aggregate {
    Count,
    Sum(String),
    Min(String),
    Max(String),
    Avg(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsistencyMode {
    AtLatest,
    AtSeq(u64),
    AtCheckpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOptions {
    pub consistency: ConsistencyMode,
    pub cursor: Option<String>,
    pub async_index: Option<String>,
    pub allow_full_scan: bool,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            consistency: ConsistencyMode::AtLatest,
            cursor: None,
            async_index: None,
            allow_full_scan: false,
        }
    }
}

impl Query {
    pub fn select(cols: &[&str]) -> Self {
        Self {
            select: cols.iter().map(|s| s.to_string()).collect(),
            table: String::new(),
            table_alias: None,
            joins: Vec::new(),
            predicate: None,
            order_by: Vec::new(),
            limit: None,
            group_by: vec![],
            aggregates: Vec::new(),
            having: None,
            use_index: None,
        }
    }

    pub fn from(mut self, table: &str) -> Self {
        self.table = table.to_string();
        self
    }

    pub fn alias(mut self, alias: &str) -> Self {
        self.table_alias = Some(alias.to_string());
        self
    }

    pub fn inner_join(mut self, table: &str, left_column: &str, right_column: &str) -> Self {
        self.joins.push(JoinSpec {
            table: table.to_string(),
            alias: None,
            join_type: JoinType::Inner,
            left_column: Some(left_column.to_string()),
            right_column: Some(right_column.to_string()),
        });
        self
    }

    pub fn left_join(mut self, table: &str, left_column: &str, right_column: &str) -> Self {
        self.joins.push(JoinSpec {
            table: table.to_string(),
            alias: None,
            join_type: JoinType::Left,
            left_column: Some(left_column.to_string()),
            right_column: Some(right_column.to_string()),
        });
        self
    }

    pub fn right_join(mut self, table: &str, left_column: &str, right_column: &str) -> Self {
        self.joins.push(JoinSpec {
            table: table.to_string(),
            alias: None,
            join_type: JoinType::Right,
            left_column: Some(left_column.to_string()),
            right_column: Some(right_column.to_string()),
        });
        self
    }

    pub fn cross_join(mut self, table: &str) -> Self {
        self.joins.push(JoinSpec {
            table: table.to_string(),
            alias: None,
            join_type: JoinType::Cross,
            left_column: None,
            right_column: None,
        });
        self
    }

    pub fn with_last_join_alias(mut self, alias: &str) -> Self {
        if let Some(last) = self.joins.last_mut() {
            last.alias = Some(alias.to_string());
        }
        self
    }

    pub fn where_(mut self, expr: Expr) -> Self {
        self.predicate = Some(expr);
        self
    }

    pub fn order_by(mut self, col: &str, order: Order) -> Self {
        self.order_by.push((col.to_string(), order));
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn group_by(mut self, cols: &[&str]) -> Self {
        self.group_by = cols.iter().map(|c| c.to_string()).collect();
        self
    }

    pub fn aggregate(mut self, aggregate: Aggregate) -> Self {
        self.aggregates.push(aggregate);
        self
    }

    pub fn having(mut self, expr: Expr) -> Self {
        self.having = Some(expr);
        self
    }

    pub fn use_index(mut self, index_name: &str) -> Self {
        self.use_index = Some(index_name.to_string());
        self
    }
}

pub struct ColumnRef(String);

pub fn col(name: &str) -> ColumnRef {
    ColumnRef(name.to_string())
}

pub trait IntoQueryValue {
    fn into_query_value(self) -> Value;
}

impl IntoQueryValue for Value {
    fn into_query_value(self) -> Value {
        self
    }
}

impl IntoQueryValue for bool {
    fn into_query_value(self) -> Value {
        Value::Boolean(self)
    }
}

impl IntoQueryValue for i64 {
    fn into_query_value(self) -> Value {
        Value::Integer(self)
    }
}

impl IntoQueryValue for i32 {
    fn into_query_value(self) -> Value {
        Value::Integer(self as i64)
    }
}

impl IntoQueryValue for u64 {
    fn into_query_value(self) -> Value {
        Value::Integer(self as i64)
    }
}

impl IntoQueryValue for f64 {
    fn into_query_value(self) -> Value {
        Value::Float(self)
    }
}

impl IntoQueryValue for String {
    fn into_query_value(self) -> Value {
        Value::Text(self.into())
    }
}

impl IntoQueryValue for &str {
    fn into_query_value(self) -> Value {
        Value::Text(self.to_string().into())
    }
}

pub fn lit<T: IntoQueryValue>(value: T) -> Value {
    value.into_query_value()
}

impl ColumnRef {
    pub fn eq(self, value: Value) -> Expr {
        Expr::Eq(self.0, value)
    }

    pub fn neq(self, value: Value) -> Expr {
        Expr::Ne(self.0, value)
    }

    pub fn gt(self, value: Value) -> Expr {
        Expr::Gt(self.0, value)
    }

    pub fn gte(self, value: Value) -> Expr {
        Expr::Gte(self.0, value)
    }

    pub fn lt(self, value: Value) -> Expr {
        Expr::Lt(self.0, value)
    }

    pub fn lte(self, value: Value) -> Expr {
        Expr::Lte(self.0, value)
    }

    pub fn between(self, low: Value, high: Value) -> Expr {
        Expr::Between(self.0, low, high)
    }

    pub fn in_(self, values: Vec<Value>) -> Expr {
        Expr::In(self.0, values)
    }

    pub fn like(self, pattern: Value) -> Expr {
        match pattern {
            Value::Text(s) => Expr::Like(self.0, s.to_string()),
            _ => Expr::Like(self.0, String::new()),
        }
    }

    pub fn is_null(self) -> Expr {
        Expr::IsNull(self.0)
    }

    pub fn is_not_null(self) -> Expr {
        Expr::IsNotNull(self.0)
    }
}
