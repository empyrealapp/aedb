use crate::catalog::types::{Row, Value};
use crate::query::error::QueryError;
use crate::query::plan::{Aggregate, Expr, Order};
use lru::LruCache;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;

const EXPR_CACHE_SHARDS: usize = 16;
const EXPR_CACHE_TOTAL_CAPACITY: usize = 256;
const EXPR_CACHE_PER_SHARD: usize = EXPR_CACHE_TOTAL_CAPACITY / EXPR_CACHE_SHARDS;

/// Global cache for compiled expressions to avoid recompiling identical predicates.
/// Key is (expr_debug_string, column_names, table) to ensure correct schema context.
/// This provides 50-100μs savings for repeated queries with the same predicates.
type ExprCacheKey = (String, Vec<String>, String);
type ExprCompileCacheShard = parking_lot::Mutex<LruCache<ExprCacheKey, CompiledExpr>>;
type ExprCompileCache = [ExprCompileCacheShard; EXPR_CACHE_SHARDS];

static EXPR_COMPILE_CACHE: once_cell::sync::Lazy<ExprCompileCache> =
    once_cell::sync::Lazy::new(|| {
        std::array::from_fn(|_| {
            let cap = NonZeroUsize::new(EXPR_CACHE_PER_SHARD).unwrap_or(NonZeroUsize::MIN);
            parking_lot::Mutex::new(LruCache::new(cap))
        })
    });

fn expr_cache_shard_idx(cache_key: &ExprCacheKey) -> usize {
    let mut hasher = DefaultHasher::new();
    cache_key.hash(&mut hasher);
    (hasher.finish() as usize) % EXPR_CACHE_SHARDS
}

pub trait Operator {
    fn next(&mut self) -> Option<Row>;
    fn rows_examined(&self) -> usize {
        0
    }
}

pub struct ScanOperator {
    rows: Box<dyn Iterator<Item = Row> + Send>,
    examined: usize,
}

impl ScanOperator {
    pub fn new<I>(rows: I) -> Self
    where
        I: IntoIterator<Item = Row>,
        I::IntoIter: Iterator<Item = Row> + Send + 'static,
    {
        Self {
            rows: Box::new(rows.into_iter()),
            examined: 0,
        }
    }
}

impl Operator for ScanOperator {
    fn next(&mut self) -> Option<Row> {
        let row = self.rows.next()?;
        self.examined += 1;
        Some(row)
    }

    fn rows_examined(&self) -> usize {
        self.examined
    }
}

pub struct FilterOperator {
    child: Box<dyn Operator + Send>,
    predicate: CompiledExpr,
}

impl FilterOperator {
    pub fn new(child: Box<dyn Operator + Send>, predicate: CompiledExpr) -> Self {
        Self { child, predicate }
    }
}

impl Operator for FilterOperator {
    fn next(&mut self) -> Option<Row> {
        loop {
            let row = self.child.next()?;
            if eval_compiled_expr(&self.predicate, &row) {
                return Some(row);
            }
        }
    }

    fn rows_examined(&self) -> usize {
        self.child.rows_examined()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompiledExpr {
    Eq(usize, Value),
    Ne(usize, Value),
    Lt(usize, Value),
    Lte(usize, Value),
    Gt(usize, Value),
    Gte(usize, Value),
    In(usize, Vec<Value>),
    Between(usize, Value, Value),
    IsNull(usize),
    IsNotNull(usize),
    Like(usize, String),
    And(Box<CompiledExpr>, Box<CompiledExpr>),
    Or(Box<CompiledExpr>, Box<CompiledExpr>),
    Not(Box<CompiledExpr>),
}

pub fn compile_expr(
    expr: &Expr,
    columns: &[String],
    table: &str,
) -> Result<CompiledExpr, QueryError> {
    // Try cache first - use debug representation as key (includes expr structure)
    let cache_key = (format!("{:?}", expr), columns.to_vec(), table.to_string());
    let shard_idx = expr_cache_shard_idx(&cache_key);
    let cache_shard = &EXPR_COMPILE_CACHE[shard_idx];

    if let Some(compiled) = cache_shard.lock().get(&cache_key) {
        // Cache hit - return cloned compiled expression
        return Ok(compiled.clone());
    }

    // Cache miss - compile the expression
    let compiled = compile_expr_uncached(expr, columns, table)?;

    // Store in cache
    cache_shard.lock().put(cache_key, compiled.clone());

    Ok(compiled)
}

fn compile_expr_uncached(
    expr: &Expr,
    columns: &[String],
    table: &str,
) -> Result<CompiledExpr, QueryError> {
    match expr {
        Expr::Eq(c, v) => Ok(CompiledExpr::Eq(
            find_col_idx(columns, c, table)?,
            v.clone(),
        )),
        Expr::Ne(c, v) => Ok(CompiledExpr::Ne(
            find_col_idx(columns, c, table)?,
            v.clone(),
        )),
        Expr::Lt(c, v) => Ok(CompiledExpr::Lt(
            find_col_idx(columns, c, table)?,
            v.clone(),
        )),
        Expr::Lte(c, v) => Ok(CompiledExpr::Lte(
            find_col_idx(columns, c, table)?,
            v.clone(),
        )),
        Expr::Gt(c, v) => Ok(CompiledExpr::Gt(
            find_col_idx(columns, c, table)?,
            v.clone(),
        )),
        Expr::Gte(c, v) => Ok(CompiledExpr::Gte(
            find_col_idx(columns, c, table)?,
            v.clone(),
        )),
        Expr::In(c, values) => Ok(CompiledExpr::In(
            find_col_idx(columns, c, table)?,
            values.clone(),
        )),
        Expr::Between(c, lo, hi) => Ok(CompiledExpr::Between(
            find_col_idx(columns, c, table)?,
            lo.clone(),
            hi.clone(),
        )),
        Expr::IsNull(c) => Ok(CompiledExpr::IsNull(find_col_idx(columns, c, table)?)),
        Expr::IsNotNull(c) => Ok(CompiledExpr::IsNotNull(find_col_idx(columns, c, table)?)),
        Expr::Like(c, pattern) => Ok(CompiledExpr::Like(
            find_col_idx(columns, c, table)?,
            pattern.clone(),
        )),
        Expr::And(a, b) => Ok(CompiledExpr::And(
            Box::new(compile_expr_uncached(a, columns, table)?),
            Box::new(compile_expr_uncached(b, columns, table)?),
        )),
        Expr::Or(a, b) => Ok(CompiledExpr::Or(
            Box::new(compile_expr_uncached(a, columns, table)?),
            Box::new(compile_expr_uncached(b, columns, table)?),
        )),
        Expr::Not(inner) => Ok(CompiledExpr::Not(Box::new(compile_expr_uncached(
            inner, columns, table,
        )?))),
    }
}

pub struct ProjectOperator {
    child: Box<dyn Operator + Send>,
    selected: Vec<usize>,
}

impl ProjectOperator {
    pub fn new(child: Box<dyn Operator + Send>, selected: Vec<usize>) -> Self {
        Self { child, selected }
    }
}

impl Operator for ProjectOperator {
    fn next(&mut self) -> Option<Row> {
        let row = self.child.next()?;
        let values = self
            .selected
            .iter()
            .map(|idx| row.values[*idx].clone())
            .collect();
        Some(Row { values })
    }

    fn rows_examined(&self) -> usize {
        self.child.rows_examined()
    }
}

pub struct SortOperator {
    rows: Vec<Row>,
    idx: usize,
    examined: usize,
}

impl SortOperator {
    pub fn new(child: Box<dyn Operator + Send>, order_by: Vec<(usize, Order)>) -> Self {
        Self::new_with_limit(child, order_by, None)
    }

    pub fn new_with_limit(
        mut child: Box<dyn Operator + Send>,
        order_by: Vec<(usize, Order)>,
        limit: Option<usize>,
    ) -> Self {
        let compare_rows = |a: &Row, b: &Row| {
            for (column_idx, order) in &order_by {
                let cmp = a.values[*column_idx].cmp(&b.values[*column_idx]);
                let ord = match order {
                    Order::Asc => cmp,
                    Order::Desc => cmp.reverse(),
                };
                if !ord.is_eq() {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        };

        if let Some(limit) = limit {
            if limit == 0 {
                return Self {
                    rows: Vec::new(),
                    idx: 0,
                    examined: 0,
                };
            }
            #[derive(Clone)]
            struct TopKRow {
                row: Row,
                sort_key: Vec<Value>,
                orders: Arc<Vec<Order>>,
            }

            impl Ord for TopKRow {
                fn cmp(&self, other: &Self) -> Ordering {
                    for ((lhs, rhs), order) in self
                        .sort_key
                        .iter()
                        .zip(other.sort_key.iter())
                        .zip(self.orders.iter())
                    {
                        let cmp = lhs.cmp(rhs);
                        let ord = match order {
                            Order::Asc => cmp,
                            Order::Desc => cmp.reverse(),
                        };
                        if !ord.is_eq() {
                            return ord;
                        }
                    }
                    Ordering::Equal
                }
            }

            impl PartialOrd for TopKRow {
                fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                    Some(self.cmp(other))
                }
            }

            impl PartialEq for TopKRow {
                fn eq(&self, other: &Self) -> bool {
                    self.cmp(other).is_eq()
                }
            }

            impl Eq for TopKRow {}

            let sort_key_columns: Vec<usize> = order_by.iter().map(|(idx, _)| *idx).collect();
            let sort_orders =
                Arc::new(order_by.iter().map(|(_, order)| *order).collect::<Vec<_>>());
            let mut heap: BinaryHeap<TopKRow> = BinaryHeap::with_capacity(limit);
            while let Some(row) = child.next() {
                let candidate = TopKRow {
                    sort_key: sort_key_columns
                        .iter()
                        .map(|idx| row.values[*idx].clone())
                        .collect(),
                    row,
                    orders: Arc::clone(&sort_orders),
                };
                if heap.len() < limit {
                    heap.push(candidate);
                    continue;
                }
                // Keep only the best N rows under the requested ORDER BY.
                if heap
                    .peek()
                    .is_some_and(|worst_of_best| candidate < *worst_of_best)
                {
                    let _ = heap.pop();
                    heap.push(candidate);
                }
            }
            let examined = child.rows_examined();
            let mut rows = heap.into_iter().map(|entry| entry.row).collect::<Vec<_>>();
            rows.sort_by(compare_rows);
            return Self {
                rows,
                idx: 0,
                examined,
            };
        }

        let mut rows = Vec::new();
        while let Some(row) = child.next() {
            rows.push(row);
        }
        let examined = child.rows_examined();
        rows.sort_by(compare_rows);
        Self {
            rows,
            idx: 0,
            examined,
        }
    }
}

impl Operator for SortOperator {
    fn next(&mut self) -> Option<Row> {
        if self.idx >= self.rows.len() {
            return None;
        }
        let row = self.rows[self.idx].clone();
        self.idx += 1;
        Some(row)
    }

    fn rows_examined(&self) -> usize {
        self.examined
    }
}

pub struct LimitOperator {
    child: Box<dyn Operator + Send>,
    remaining: usize,
}

impl LimitOperator {
    pub fn new(child: Box<dyn Operator + Send>, limit: usize) -> Self {
        Self {
            child,
            remaining: limit,
        }
    }
}

impl Operator for LimitOperator {
    fn next(&mut self) -> Option<Row> {
        if self.remaining == 0 {
            return None;
        }
        let row = self.child.next()?;
        self.remaining -= 1;
        Some(row)
    }

    fn rows_examined(&self) -> usize {
        self.child.rows_examined()
    }
}

pub struct AggregateOperator {
    rows: Vec<Row>,
    idx: usize,
    examined: usize,
}

#[derive(Debug, Clone)]
enum AggregateState {
    Count(i64),
    Sum(i64),
    Min(Option<Value>),
    Max(Option<Value>),
    Avg { total: i64, count: i64 },
}

impl AggregateState {
    fn from_aggregate(aggregate: &Aggregate) -> Self {
        match aggregate {
            Aggregate::Count => AggregateState::Count(0),
            Aggregate::Sum(_) => AggregateState::Sum(0),
            Aggregate::Min(_) => AggregateState::Min(None),
            Aggregate::Max(_) => AggregateState::Max(None),
            Aggregate::Avg(_) => AggregateState::Avg { total: 0, count: 0 },
        }
    }

    fn update(&mut self, aggregate: &Aggregate, row: &Row, col_idx: Option<usize>) {
        match (self, aggregate) {
            (AggregateState::Count(v), Aggregate::Count) => {
                *v = v.saturating_add(1);
            }
            (AggregateState::Sum(sum), Aggregate::Sum(_)) => {
                if let Some(idx) = col_idx
                    && let Value::Integer(v) = &row.values[idx]
                {
                    *sum = sum.saturating_add(*v);
                }
            }
            (AggregateState::Min(state), Aggregate::Min(_)) => {
                if let Some(idx) = col_idx {
                    let value = row.values[idx].clone();
                    if state.as_ref().is_none_or(|current| value < *current) {
                        *state = Some(value);
                    }
                }
            }
            (AggregateState::Max(state), Aggregate::Max(_)) => {
                if let Some(idx) = col_idx {
                    let value = row.values[idx].clone();
                    if state.as_ref().is_none_or(|current| value > *current) {
                        *state = Some(value);
                    }
                }
            }
            (AggregateState::Avg { total, count }, Aggregate::Avg(_)) => {
                if let Some(idx) = col_idx
                    && let Value::Integer(v) = &row.values[idx]
                {
                    *total = total.saturating_add(*v);
                    *count = count.saturating_add(1);
                }
            }
            _ => {}
        }
    }

    fn finalize(self) -> Value {
        match self {
            AggregateState::Count(v) => Value::Integer(v),
            AggregateState::Sum(v) => Value::Integer(v),
            AggregateState::Min(v) => v.unwrap_or(Value::Null),
            AggregateState::Max(v) => v.unwrap_or(Value::Null),
            AggregateState::Avg { total, count } => {
                if count == 0 {
                    Value::Null
                } else {
                    Value::Float(total as f64 / count as f64)
                }
            }
        }
    }
}

impl AggregateOperator {
    pub fn new(
        mut child: Box<dyn Operator + Send>,
        aggregates: Vec<Aggregate>,
        group_by_idx: Vec<usize>,
        aggregate_col_idx: Vec<Option<usize>>,
    ) -> Self {
        let aggregates = Arc::new(aggregates);
        let aggregate_col_idx = Arc::new(aggregate_col_idx);
        let mut buckets: BTreeMap<Vec<Value>, Vec<AggregateState>> = BTreeMap::new();
        while let Some(row) = child.next() {
            let key: Vec<Value> = if group_by_idx.is_empty() {
                Vec::new()
            } else {
                group_by_idx
                    .iter()
                    .map(|i| row.values[*i].clone())
                    .collect()
            };
            let states = buckets.entry(key).or_insert_with(|| {
                aggregates
                    .iter()
                    .map(AggregateState::from_aggregate)
                    .collect::<Vec<_>>()
            });
            for (idx, state) in states.iter_mut().enumerate() {
                state.update(&aggregates[idx], &row, aggregate_col_idx[idx]);
            }
        }
        let examined = child.rows_examined();

        let mut rows = Vec::new();
        for (group_key, group_states) in buckets {
            let mut values = group_key;
            for state in group_states {
                values.push(state.finalize());
            }
            rows.push(Row { values });
        }

        Self {
            rows,
            idx: 0,
            examined,
        }
    }
}

impl Operator for AggregateOperator {
    fn next(&mut self) -> Option<Row> {
        if self.idx >= self.rows.len() {
            return None;
        }
        let row = self.rows[self.idx].clone();
        self.idx += 1;
        Some(row)
    }

    fn rows_examined(&self) -> usize {
        self.examined
    }
}

fn eval_compiled_expr(expr: &CompiledExpr, row: &Row) -> bool {
    match expr {
        CompiledExpr::Eq(idx, v) => {
            get_col(row, *idx).is_some_and(|rv| compare_values(rv, v).is_some_and(|o| o.is_eq()))
        }
        CompiledExpr::Ne(idx, v) => {
            get_col(row, *idx).is_some_and(|rv| compare_values(rv, v).is_some_and(|o| !o.is_eq()))
        }
        CompiledExpr::Lt(idx, v) => {
            get_col(row, *idx).is_some_and(|rv| compare_values(rv, v).is_some_and(|o| o.is_lt()))
        }
        CompiledExpr::Lte(idx, v) => {
            get_col(row, *idx).is_some_and(|rv| compare_values(rv, v).is_some_and(|o| o.is_le()))
        }
        CompiledExpr::Gt(idx, v) => {
            get_col(row, *idx).is_some_and(|rv| compare_values(rv, v).is_some_and(|o| o.is_gt()))
        }
        CompiledExpr::Gte(idx, v) => {
            get_col(row, *idx).is_some_and(|rv| compare_values(rv, v).is_some_and(|o| o.is_ge()))
        }
        CompiledExpr::In(idx, values) => get_col(row, *idx).is_some_and(|rv| {
            values
                .iter()
                .any(|v| compare_values(rv, v).is_some_and(|o| o.is_eq()))
        }),
        CompiledExpr::Between(idx, lo, hi) => get_col(row, *idx).is_some_and(|rv| {
            compare_values(rv, lo).is_some_and(|o| o.is_ge())
                && compare_values(rv, hi).is_some_and(|o| o.is_le())
        }),
        CompiledExpr::IsNull(idx) => get_col(row, *idx).is_some_and(|rv| matches!(rv, Value::Null)),
        CompiledExpr::IsNotNull(idx) => {
            get_col(row, *idx).is_some_and(|rv| !matches!(rv, Value::Null))
        }
        CompiledExpr::Like(idx, pattern) => get_col(row, *idx).is_some_and(|rv| match rv {
            Value::Text(s) => like_match(s, pattern),
            _ => false,
        }),
        CompiledExpr::And(a, b) => eval_compiled_expr(a, row) && eval_compiled_expr(b, row),
        CompiledExpr::Or(a, b) => eval_compiled_expr(a, row) || eval_compiled_expr(b, row),
        CompiledExpr::Not(inner) => !eval_compiled_expr(inner, row),
    }
}

pub fn eval_compiled_expr_public(expr: &CompiledExpr, row: &Row) -> bool {
    eval_compiled_expr(expr, row)
}

fn find_col_idx(columns: &[String], col: &str, table: &str) -> Result<usize, QueryError> {
    columns
        .iter()
        .position(|c| c == col)
        .ok_or_else(|| QueryError::ColumnNotFound {
            table: table.to_string(),
            column: col.to_string(),
        })
}

fn get_col(row: &Row, idx: usize) -> Option<&Value> {
    row.values.get(idx)
}

fn like_match(value: &str, pattern: &str) -> bool {
    let text = value.as_bytes();
    let pat = pattern.as_bytes();
    let mut ti = 0usize;
    let mut pi = 0usize;
    let mut star_pi: Option<usize> = None;
    let mut star_ti = 0usize;

    while ti < text.len() {
        if pi < pat.len() && (pat[pi] == b'_' || pat[pi] == text[ti]) {
            ti += 1;
            pi += 1;
            continue;
        }
        if pi < pat.len() && pat[pi] == b'%' {
            star_pi = Some(pi);
            pi += 1;
            star_ti = ti;
            continue;
        }
        if let Some(saved_pi) = star_pi {
            pi = saved_pi + 1;
            star_ti += 1;
            ti = star_ti;
            continue;
        }
        return false;
    }

    while pi < pat.len() && pat[pi] == b'%' {
        pi += 1;
    }

    pi == pat.len()
}

fn compare_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
        (Value::Timestamp(a), Value::Integer(b)) => a.partial_cmp(b),
        (Value::Integer(a), Value::Timestamp(b)) => a.partial_cmp(b),
        (Value::Timestamp(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Timestamp(b)) => a.partial_cmp(&(*b as f64)),
        _ => Some(left.cmp(right)),
    }
}
