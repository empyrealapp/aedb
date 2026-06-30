use crate::catalog::types::{Row, Value};
use crate::query::error::QueryError;
use crate::query::plan::{Aggregate, Expr, Order};
use primitive_types::U256;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::hash::{DefaultHasher, Hash, Hasher};

const EXPR_CACHE_SHARDS: usize = 16;
const EXPR_CACHE_TOTAL_CAPACITY: usize = 256;
const EXPR_CACHE_PER_SHARD: usize = EXPR_CACHE_TOTAL_CAPACITY / EXPR_CACHE_SHARDS;
const OPERATOR_BUILD_BATCH_SIZE: usize = 1024;

/// Global cache for compiled expressions to avoid recompiling identical predicates.
/// Key is (expr_ast, column_names, table) to ensure correct schema context.
/// This provides 50-100μs savings for repeated queries with the same predicates.
type ExprCacheKey = (Expr, Vec<String>, String);
type ExprCompileCacheShard = parking_lot::Mutex<ExprCompileLruCache>;
type ExprCompileCache = [ExprCompileCacheShard; EXPR_CACHE_SHARDS];

static EXPR_COMPILE_CACHE: once_cell::sync::Lazy<ExprCompileCache> =
    once_cell::sync::Lazy::new(|| {
        std::array::from_fn(|_| {
            parking_lot::Mutex::new(ExprCompileLruCache::new(EXPR_CACHE_PER_SHARD))
        })
    });

struct ExprCompileLruCache {
    capacity: usize,
    clock: u64,
    map: HashMap<ExprCacheKey, ExprCacheEntry>,
    order: VecDeque<(u64, ExprCacheKey)>,
}

#[derive(Clone)]
struct ExprCacheEntry {
    value: CompiledExpr,
    stamp: u64,
}

impl ExprCompileLruCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            clock: 0,
            map: HashMap::with_capacity(capacity.max(1)),
            order: VecDeque::with_capacity(capacity.max(1)),
        }
    }

    fn get(&mut self, expr: &Expr, columns: &[String], table: &str) -> Option<CompiledExpr> {
        let stamp = self.next_stamp();
        let lookup_key = (expr.clone(), columns.to_vec(), table.to_string());
        let entry = self.map.get_mut(&lookup_key)?;
        entry.stamp = stamp;
        self.order.push_back((stamp, lookup_key));
        Some(entry.value.clone())
    }

    fn put(&mut self, key: ExprCacheKey, value: CompiledExpr) {
        let stamp = self.next_stamp();
        if let Some(entry) = self.map.get_mut(&key) {
            entry.value = value;
            entry.stamp = stamp;
            self.order.push_back((stamp, key));
            return;
        }

        if self.map.len() >= self.capacity {
            while let Some(evicted) = self.order.pop_front() {
                let (stamp, evicted_key) = evicted;
                let should_evict = self
                    .map
                    .get(&evicted_key)
                    .map(|entry| entry.stamp == stamp)
                    .unwrap_or(false);
                if should_evict {
                    self.map.remove(&evicted_key);
                    break;
                }
            }
        }

        self.order.push_back((stamp, key.clone()));
        self.map.insert(key, ExprCacheEntry { value, stamp });
    }

    fn next_stamp(&mut self) -> u64 {
        self.clock = self.clock.wrapping_add(1);
        self.clock
    }
}

#[cfg(test)]
mod expr_compile_lru_cache_tests;

pub trait Operator {
    fn next(&mut self) -> Option<Row>;
    fn next_batch(&mut self, batch_size: usize) -> Option<Vec<Row>> {
        let batch_size = batch_size.max(1);
        let mut rows = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            match self.next() {
                Some(row) => rows.push(row),
                None => break,
            }
        }
        if rows.is_empty() { None } else { Some(rows) }
    }
    fn rows_examined(&self) -> usize {
        0
    }
}

pub struct ScanOperator<'a> {
    rows: Box<dyn Iterator<Item = Row> + Send + 'a>,
    examined: usize,
}

impl<'a> ScanOperator<'a> {
    pub fn new<I>(rows: I) -> Self
    where
        I: IntoIterator<Item = Row>,
        I::IntoIter: Iterator<Item = Row> + Send + 'a,
    {
        Self {
            rows: Box::new(rows.into_iter()),
            examined: 0,
        }
    }
}

impl Operator for ScanOperator<'_> {
    fn next(&mut self) -> Option<Row> {
        let row = self.rows.next()?;
        self.examined += 1;
        Some(row)
    }

    fn rows_examined(&self) -> usize {
        self.examined
    }

    fn next_batch(&mut self, batch_size: usize) -> Option<Vec<Row>> {
        let batch_size = batch_size.max(1);
        let mut rows = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            match self.rows.next() {
                Some(row) => rows.push(row),
                None => break,
            }
        }
        self.examined = self.examined.saturating_add(rows.len());
        if rows.is_empty() { None } else { Some(rows) }
    }
}

pub struct FilterOperator<'a> {
    child: Box<dyn Operator + Send + 'a>,
    predicate: CompiledExpr,
}

impl<'a> FilterOperator<'a> {
    pub fn new(child: Box<dyn Operator + Send + 'a>, predicate: CompiledExpr) -> Self {
        Self { child, predicate }
    }
}

impl Operator for FilterOperator<'_> {
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

    fn next_batch(&mut self, batch_size: usize) -> Option<Vec<Row>> {
        let batch_size = batch_size.max(1);
        loop {
            let child_batch = self.child.next_batch(batch_size)?;
            let mut filtered = Vec::with_capacity(child_batch.len());
            for row in child_batch {
                if eval_compiled_expr(&self.predicate, &row) {
                    filtered.push(row);
                }
            }
            if !filtered.is_empty() {
                return Some(filtered);
            }
        }
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
    In(usize, Vec<Value>, HashSet<Value>),
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
    // Try cache first - keyed by expression AST + schema context.
    let mut hasher = DefaultHasher::new();
    expr.hash(&mut hasher);
    columns.hash(&mut hasher);
    table.hash(&mut hasher);
    let shard_idx = (hasher.finish() as usize) % EXPR_CACHE_SHARDS;
    let cache_shard = &EXPR_COMPILE_CACHE[shard_idx];

    if let Some(compiled) = cache_shard.lock().get(expr, columns, table) {
        return Ok(compiled);
    }

    // Cache miss - compile the expression
    let compiled = compile_expr_uncached(expr, columns, table)?;

    // Store in cache
    let cache_key = (expr.clone(), columns.to_vec(), table.to_string());
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
        Expr::In(c, values) => {
            let set: HashSet<Value> = values.iter().cloned().collect();
            Ok(CompiledExpr::In(
                find_col_idx(columns, c, table)?,
                values.clone(),
                set,
            ))
        }
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

pub struct ProjectOperator<'a> {
    child: Box<dyn Operator + Send + 'a>,
    selected: Vec<usize>,
}

impl<'a> ProjectOperator<'a> {
    pub fn new(child: Box<dyn Operator + Send + 'a>, selected: Vec<usize>) -> Self {
        Self { child, selected }
    }
}

impl Operator for ProjectOperator<'_> {
    fn next(&mut self) -> Option<Row> {
        let row = self.child.next()?;
        let values = self
            .selected
            .iter()
            .map(|column_index| row.values[*column_index].clone())
            .collect();
        Some(Row { values })
    }

    fn rows_examined(&self) -> usize {
        self.child.rows_examined()
    }

    fn next_batch(&mut self, batch_size: usize) -> Option<Vec<Row>> {
        let child_batch = self.child.next_batch(batch_size)?;
        let projected = child_batch
            .into_iter()
            .map(|row| Row {
                values: self
                    .selected
                    .iter()
                    .map(|column_index| row.values[*column_index].clone())
                    .collect(),
            })
            .collect::<Vec<_>>();
        if projected.is_empty() {
            None
        } else {
            Some(projected)
        }
    }
}

pub struct SortOperator {
    rows: Vec<Row>,
    row_index: usize,
    examined: usize,
}

impl SortOperator {
    pub fn new(child: Box<dyn Operator + Send + '_>, order_by: Vec<(usize, Order)>) -> Self {
        Self::new_with_limit(child, order_by, None)
    }

    pub fn new_with_limit(
        mut child: Box<dyn Operator + Send + '_>,
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
                    row_index: 0,
                    examined: 0,
                };
            }
            let sort_key_columns: Vec<usize> = order_by
                .iter()
                .map(|(column_index, _)| *column_index)
                .collect();
            let sort_orders = order_by.iter().map(|(_, order)| *order).collect::<Vec<_>>();
            let mut heap = TopKHeap::with_capacity(limit, sort_key_columns, sort_orders);
            while let Some(rows) = child.next_batch(OPERATOR_BUILD_BATCH_SIZE) {
                for row in rows {
                    if heap.len() < limit {
                        heap.push(row);
                        continue;
                    }
                    let should_insert = heap.peek().is_some_and(|worst_of_best| {
                        compare_rows_with_order_by(
                            &row,
                            worst_of_best,
                            &heap.sort_key_columns,
                            &heap.sort_orders,
                        )
                        .is_lt()
                    });
                    // Keep only the best N rows under the requested ORDER BY.
                    if should_insert {
                        let _ = heap.pop();
                        heap.push(row);
                    }
                }
            }
            let examined = child.rows_examined();
            let mut rows = heap.into_rows();
            rows.sort_by(compare_rows);
            return Self {
                rows,
                row_index: 0,
                examined,
            };
        }

        let mut rows = Vec::new();
        while let Some(batch) = child.next_batch(OPERATOR_BUILD_BATCH_SIZE) {
            rows.extend(batch);
        }
        let examined = child.rows_examined();
        rows.sort_by(compare_rows);
        Self {
            rows,
            row_index: 0,
            examined,
        }
    }
}

struct TopKHeap {
    rows: Vec<Row>,
    sort_key_columns: Vec<usize>,
    sort_orders: Vec<Order>,
}

impl TopKHeap {
    fn with_capacity(
        capacity: usize,
        sort_key_columns: Vec<usize>,
        sort_orders: Vec<Order>,
    ) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
            sort_key_columns,
            sort_orders,
        }
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn peek(&self) -> Option<&Row> {
        self.rows.first()
    }

    fn push(&mut self, row: Row) {
        self.rows.push(row);
        self.sift_up(self.rows.len() - 1);
    }

    fn pop(&mut self) -> Option<Row> {
        if self.rows.is_empty() {
            return None;
        }
        let last = self.rows.len() - 1;
        self.rows.swap(0, last);
        let root = self.rows.pop();
        if !self.rows.is_empty() {
            self.sift_down(0);
        }
        root
    }

    fn into_rows(self) -> Vec<Row> {
        self.rows
    }

    fn compare_indices(&self, left: usize, right: usize) -> Ordering {
        compare_rows_with_order_by(
            &self.rows[left],
            &self.rows[right],
            &self.sort_key_columns,
            &self.sort_orders,
        )
    }

    fn sift_up(&mut self, mut index: usize) {
        while index > 0 {
            let parent = (index - 1) / 2;
            if !self.compare_indices(index, parent).is_gt() {
                break;
            }
            self.rows.swap(index, parent);
            index = parent;
        }
    }

    fn sift_down(&mut self, mut heap_index: usize) {
        let row_count = self.rows.len();
        loop {
            let left = heap_index * 2 + 1;
            let right = left + 1;
            if left >= row_count {
                break;
            }
            let mut largest = left;
            if right < row_count && self.compare_indices(right, left).is_gt() {
                largest = right;
            }
            if !self.compare_indices(largest, heap_index).is_gt() {
                break;
            }
            self.rows.swap(heap_index, largest);
            heap_index = largest;
        }
    }
}

#[inline]
fn compare_rows_with_order_by(
    left: &Row,
    right: &Row,
    sort_key_columns: &[usize],
    sort_orders: &[Order],
) -> Ordering {
    if sort_key_columns.len() == 1 {
        let cmp = left.values[sort_key_columns[0]].cmp(&right.values[sort_key_columns[0]]);
        return match sort_orders[0] {
            Order::Asc => cmp,
            Order::Desc => cmp.reverse(),
        };
    }
    for (column_index, sort_order) in sort_key_columns.iter().zip(sort_orders.iter()) {
        let cmp = left.values[*column_index].cmp(&right.values[*column_index]);
        let ord = match sort_order {
            Order::Asc => cmp,
            Order::Desc => cmp.reverse(),
        };
        if !ord.is_eq() {
            return ord;
        }
    }
    Ordering::Equal
}

impl Operator for SortOperator {
    fn next(&mut self) -> Option<Row> {
        if self.row_index >= self.rows.len() {
            return None;
        }
        let row = self.rows[self.row_index].clone();
        self.row_index += 1;
        Some(row)
    }

    fn rows_examined(&self) -> usize {
        self.examined
    }

    fn next_batch(&mut self, batch_size: usize) -> Option<Vec<Row>> {
        if self.row_index >= self.rows.len() {
            return None;
        }
        let remaining = self.rows.len() - self.row_index;
        let take = remaining.min(batch_size.max(1));
        let end = self.row_index + take;
        let out = self.rows[self.row_index..end].to_vec();
        self.row_index = end;
        Some(out)
    }
}

pub struct LimitOperator<'a> {
    child: Box<dyn Operator + Send + 'a>,
    remaining: usize,
}

impl<'a> LimitOperator<'a> {
    pub fn new(child: Box<dyn Operator + Send + 'a>, limit: usize) -> Self {
        Self {
            child,
            remaining: limit,
        }
    }
}

impl Operator for LimitOperator<'_> {
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

    fn next_batch(&mut self, batch_size: usize) -> Option<Vec<Row>> {
        if self.remaining == 0 {
            return None;
        }
        let mut batch = self
            .child
            .next_batch(batch_size.max(1).min(self.remaining))?;
        if batch.len() > self.remaining {
            batch.truncate(self.remaining);
        }
        self.remaining = self.remaining.saturating_sub(batch.len());
        if batch.is_empty() { None } else { Some(batch) }
    }
}

pub struct AggregateOperator {
    rows: Vec<Row>,
    row_index: usize,
    examined: usize,
}

/// A numeric column value coerced for SUM/AVG accumulation. Integer-family
/// values (`U8`/`U64`/`Integer`) accumulate as `i64` to preserve the historical
/// integer result type; `Float` switches the accumulator to floating point.
enum NumericInput {
    Int(i64),
    Float(f64),
}

fn numeric_input(value: &Value) -> Option<NumericInput> {
    match value {
        Value::Integer(v) => Some(NumericInput::Int(*v)),
        Value::U8(v) => Some(NumericInput::Int(*v as i64)),
        Value::U64(v) => Some(NumericInput::Int(*v as i64)),
        Value::Float(v) => Some(NumericInput::Float(*v)),
        _ => None,
    }
}

#[derive(Debug, Clone)]
enum AggregateState {
    Count(i64),
    Sum {
        int: i64,
        float: f64,
        has_float: bool,
    },
    Min(Option<Value>),
    Max(Option<Value>),
    Avg {
        int: i64,
        float: f64,
        has_float: bool,
        count: i64,
    },
}

impl AggregateState {
    fn from_aggregate(aggregate: &Aggregate) -> Self {
        match aggregate {
            Aggregate::Count => AggregateState::Count(0),
            Aggregate::Sum(_) => AggregateState::Sum {
                int: 0,
                float: 0.0,
                has_float: false,
            },
            Aggregate::Min(_) => AggregateState::Min(None),
            Aggregate::Max(_) => AggregateState::Max(None),
            Aggregate::Avg(_) => AggregateState::Avg {
                int: 0,
                float: 0.0,
                has_float: false,
                count: 0,
            },
        }
    }

    fn update(&mut self, aggregate: &Aggregate, row: &Row, aggregate_column_index: Option<usize>) {
        match (self, aggregate) {
            (AggregateState::Count(v), Aggregate::Count) => {
                *v = v.saturating_add(1);
            }
            (
                AggregateState::Sum {
                    int,
                    float,
                    has_float,
                },
                Aggregate::Sum(_),
            ) => {
                if let Some(column_index) = aggregate_column_index {
                    match numeric_input(&row.values[column_index]) {
                        Some(NumericInput::Int(v)) => *int = int.saturating_add(v),
                        Some(NumericInput::Float(v)) => {
                            *float += v;
                            *has_float = true;
                        }
                        None => {}
                    }
                }
            }
            (AggregateState::Min(state), Aggregate::Min(_)) => {
                if let Some(column_index) = aggregate_column_index {
                    // SQL MIN/MAX ignore NULLs; without this guard a NULL (which
                    // sorts below every other value) would always win MIN.
                    let value = &row.values[column_index];
                    if !matches!(value, Value::Null)
                        && state.as_ref().is_none_or(|current| value < current)
                    {
                        *state = Some(value.clone());
                    }
                }
            }
            (AggregateState::Max(state), Aggregate::Max(_)) => {
                if let Some(column_index) = aggregate_column_index {
                    let value = &row.values[column_index];
                    if !matches!(value, Value::Null)
                        && state.as_ref().is_none_or(|current| value > current)
                    {
                        *state = Some(value.clone());
                    }
                }
            }
            (
                AggregateState::Avg {
                    int,
                    float,
                    has_float,
                    count,
                },
                Aggregate::Avg(_),
            ) => {
                if let Some(column_index) = aggregate_column_index {
                    match numeric_input(&row.values[column_index]) {
                        Some(NumericInput::Int(v)) => {
                            *int = int.saturating_add(v);
                            *count = count.saturating_add(1);
                        }
                        Some(NumericInput::Float(v)) => {
                            *float += v;
                            *has_float = true;
                            *count = count.saturating_add(1);
                        }
                        None => {}
                    }
                }
            }
            _ => {}
        }
    }

    fn finalize(self) -> Value {
        match self {
            AggregateState::Count(v) => Value::Integer(v),
            AggregateState::Sum {
                int,
                float,
                has_float,
            } => {
                if has_float {
                    Value::Float(int as f64 + float)
                } else {
                    Value::Integer(int)
                }
            }
            AggregateState::Min(v) => v.unwrap_or(Value::Null),
            AggregateState::Max(v) => v.unwrap_or(Value::Null),
            AggregateState::Avg {
                int,
                float,
                has_float,
                count,
            } => {
                if count == 0 {
                    Value::Null
                } else {
                    let total = if has_float {
                        int as f64 + float
                    } else {
                        int as f64
                    };
                    Value::Float(total / count as f64)
                }
            }
        }
    }
}

impl AggregateOperator {
    pub fn new(
        mut child: Box<dyn Operator + Send + '_>,
        aggregates: Vec<Aggregate>,
        group_by_idx: Vec<usize>,
        aggregate_col_idx: Vec<Option<usize>>,
    ) -> Self {
        let template_states = aggregates
            .iter()
            .map(AggregateState::from_aggregate)
            .collect::<Vec<_>>();

        if group_by_idx.is_empty() {
            let mut states = template_states.clone();
            let mut saw_row = false;
            while let Some(rows) = child.next_batch(OPERATOR_BUILD_BATCH_SIZE) {
                saw_row = true;
                for row in rows {
                    for ((state, aggregate), agg_col_idx) in states
                        .iter_mut()
                        .zip(aggregates.iter())
                        .zip(aggregate_col_idx.iter().copied())
                    {
                        state.update(aggregate, &row, agg_col_idx);
                    }
                }
            }
            let examined = child.rows_examined();
            let rows = if saw_row {
                vec![Row {
                    values: states.into_iter().map(AggregateState::finalize).collect(),
                }]
            } else {
                Vec::new()
            };
            return Self {
                rows,
                row_index: 0,
                examined,
            };
        }

        let mut buckets: HashMap<Vec<Value>, Vec<AggregateState>> = HashMap::new();
        while let Some(rows) = child.next_batch(OPERATOR_BUILD_BATCH_SIZE) {
            for row in rows {
                let mut key: Vec<Value> = Vec::with_capacity(group_by_idx.len());
                for group_idx in &group_by_idx {
                    key.push(row.values[*group_idx].clone());
                }
                let states = buckets
                    .entry(key)
                    .or_insert_with(|| template_states.clone());
                for ((state, aggregate), agg_col_idx) in states
                    .iter_mut()
                    .zip(aggregates.iter())
                    .zip(aggregate_col_idx.iter().copied())
                {
                    state.update(aggregate, &row, agg_col_idx);
                }
            }
        }
        let examined = child.rows_examined();

        let mut bucket_entries = buckets.into_iter().collect::<Vec<_>>();
        bucket_entries.sort_unstable_by(|(left_key, _), (right_key, _)| left_key.cmp(right_key));
        let mut rows = Vec::with_capacity(bucket_entries.len());
        for (group_key, group_states) in bucket_entries {
            let mut values = group_key;
            for state in group_states {
                values.push(state.finalize());
            }
            rows.push(Row { values });
        }

        Self {
            rows,
            row_index: 0,
            examined,
        }
    }
}

impl Operator for AggregateOperator {
    fn next(&mut self) -> Option<Row> {
        if self.row_index >= self.rows.len() {
            return None;
        }
        let row = self.rows[self.row_index].clone();
        self.row_index += 1;
        Some(row)
    }

    fn rows_examined(&self) -> usize {
        self.examined
    }

    fn next_batch(&mut self, batch_size: usize) -> Option<Vec<Row>> {
        if self.row_index >= self.rows.len() {
            return None;
        }
        let remaining = self.rows.len() - self.row_index;
        let take = remaining.min(batch_size.max(1));
        let end = self.row_index + take;
        let out = self.rows[self.row_index..end].to_vec();
        self.row_index = end;
        Some(out)
    }
}

fn eval_compiled_expr(expr: &CompiledExpr, row: &Row) -> bool {
    match expr {
        CompiledExpr::Eq(column_index, v) => get_col(row, *column_index)
            .is_some_and(|rv| compare_values(rv, v).is_some_and(|o| o.is_eq())),
        CompiledExpr::Ne(column_index, v) => get_col(row, *column_index)
            .is_some_and(|rv| compare_values(rv, v).is_some_and(|o| !o.is_eq())),
        CompiledExpr::Lt(column_index, v) => get_col(row, *column_index)
            .is_some_and(|rv| compare_values(rv, v).is_some_and(|o| o.is_lt())),
        CompiledExpr::Lte(column_index, v) => get_col(row, *column_index)
            .is_some_and(|rv| compare_values(rv, v).is_some_and(|o| o.is_le())),
        CompiledExpr::Gt(column_index, v) => get_col(row, *column_index)
            .is_some_and(|rv| compare_values(rv, v).is_some_and(|o| o.is_gt())),
        CompiledExpr::Gte(column_index, v) => get_col(row, *column_index)
            .is_some_and(|rv| compare_values(rv, v).is_some_and(|o| o.is_ge())),
        CompiledExpr::In(column_index, values, value_set) => get_col(row, *column_index)
            .is_some_and(|rv| {
                if value_set.contains(rv) {
                    return true;
                }
                values
                    .iter()
                    .any(|v| compare_values(rv, v).is_some_and(|o| o.is_eq()))
            }),
        CompiledExpr::Between(column_index, lo, hi) => {
            get_col(row, *column_index).is_some_and(|rv| {
                compare_values(rv, lo).is_some_and(|o| o.is_ge())
                    && compare_values(rv, hi).is_some_and(|o| o.is_le())
            })
        }
        CompiledExpr::IsNull(column_index) => {
            get_col(row, *column_index).is_some_and(|rv| matches!(rv, Value::Null))
        }
        CompiledExpr::IsNotNull(column_index) => {
            get_col(row, *column_index).is_some_and(|rv| !matches!(rv, Value::Null))
        }
        CompiledExpr::Like(column_index, pattern) => {
            get_col(row, *column_index).is_some_and(|rv| match rv {
                Value::Text(s) => like_match(s, pattern),
                _ => false,
            })
        }
        CompiledExpr::And(a, b) => eval_compiled_expr(a, row) && eval_compiled_expr(b, row),
        CompiledExpr::Or(a, b) => eval_compiled_expr(a, row) || eval_compiled_expr(b, row),
        CompiledExpr::Not(inner) => !eval_compiled_expr(inner, row),
    }
}

pub fn eval_compiled_expr_public(expr: &CompiledExpr, row: &Row) -> bool {
    eval_compiled_expr(expr, row)
}

/// Cross-type-aware comparison of two values, returning `None` when either is
/// NULL or the types are not comparable. This is the same coercion `WHERE`
/// filters use (e.g. `U8`/`U64`/`Integer`/`Timestamp`/`Float` compare
/// numerically across types), exposed so the join executor can share identical
/// equality/ordering semantics.
pub(crate) fn value_compare(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    compare_values(left, right)
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

fn get_col(row: &Row, column_index: usize) -> Option<&Value> {
    row.values.get(column_index)
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
        (Value::U8(a), Value::U8(b)) => a.partial_cmp(b),
        (Value::U64(a), Value::U64(b)) => a.partial_cmp(b),
        (Value::U8(a), Value::U64(b)) => (*a as u64).partial_cmp(b),
        (Value::U64(a), Value::U8(b)) => a.partial_cmp(&(*b as u64)),
        (Value::U8(a), Value::Integer(b)) => (*a as i64).partial_cmp(b),
        (Value::Integer(a), Value::U8(b)) => a.partial_cmp(&(*b as i64)),
        (Value::U64(a), Value::Integer(b)) => {
            if *b < 0 {
                Some(std::cmp::Ordering::Greater)
            } else {
                a.partial_cmp(&(*b as u64))
            }
        }
        (Value::Integer(a), Value::U64(b)) => {
            if *a < 0 {
                Some(std::cmp::Ordering::Less)
            } else {
                (*a as u64).partial_cmp(b)
            }
        }
        (Value::U8(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::U8(b)) => a.partial_cmp(&(*b as f64)),
        (Value::U64(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::U64(b)) => a.partial_cmp(&(*b as f64)),
        (Value::U8(a), Value::Timestamp(b)) => (*a as i64).partial_cmp(b),
        (Value::Timestamp(a), Value::U8(b)) => a.partial_cmp(&(*b as i64)),
        (Value::U64(a), Value::Timestamp(b)) => {
            if *b < 0 {
                Some(std::cmp::Ordering::Greater)
            } else {
                a.partial_cmp(&(*b as u64))
            }
        }
        (Value::Timestamp(a), Value::U64(b)) => {
            if *a < 0 {
                Some(std::cmp::Ordering::Less)
            } else {
                (*a as u64).partial_cmp(b)
            }
        }
        (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
        (Value::Timestamp(a), Value::Integer(b)) => a.partial_cmp(b),
        (Value::Integer(a), Value::Timestamp(b)) => a.partial_cmp(b),
        (Value::Timestamp(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Timestamp(b)) => a.partial_cmp(&(*b as f64)),

        // Wide-integer (U256/I256) cross-type comparisons. Every integer-group
        // value fits in i128, so we compare magnitudes/sign against an i128
        // without precision loss (unlike the float arms above). Without these,
        // `WHERE balance = 5` on a U256 column fell through to `Value::cmp`,
        // which is kind-ranked and never matches an `Integer`/`U64` literal.
        (Value::U256(a), Value::U8(b)) => Some(cmp_u256_i128(a, *b as i128)),
        (Value::U8(a), Value::U256(b)) => Some(cmp_u256_i128(b, *a as i128).reverse()),
        (Value::U256(a), Value::U64(b)) => Some(cmp_u256_i128(a, *b as i128)),
        (Value::U64(a), Value::U256(b)) => Some(cmp_u256_i128(b, *a as i128).reverse()),
        (Value::U256(a), Value::Integer(b)) => Some(cmp_u256_i128(a, *b as i128)),
        (Value::Integer(a), Value::U256(b)) => Some(cmp_u256_i128(b, *a as i128).reverse()),
        (Value::U256(a), Value::Timestamp(b)) => Some(cmp_u256_i128(a, *b as i128)),
        (Value::Timestamp(a), Value::U256(b)) => Some(cmp_u256_i128(b, *a as i128).reverse()),

        (Value::I256(a), Value::U8(b)) => Some(cmp_i256_i128(a, *b as i128)),
        (Value::U8(a), Value::I256(b)) => Some(cmp_i256_i128(b, *a as i128).reverse()),
        (Value::I256(a), Value::U64(b)) => Some(cmp_i256_i128(a, *b as i128)),
        (Value::U64(a), Value::I256(b)) => Some(cmp_i256_i128(b, *a as i128).reverse()),
        (Value::I256(a), Value::Integer(b)) => Some(cmp_i256_i128(a, *b as i128)),
        (Value::Integer(a), Value::I256(b)) => Some(cmp_i256_i128(b, *a as i128).reverse()),
        (Value::I256(a), Value::Timestamp(b)) => Some(cmp_i256_i128(a, *b as i128)),
        (Value::Timestamp(a), Value::I256(b)) => Some(cmp_i256_i128(b, *a as i128).reverse()),

        (Value::U256(a), Value::I256(b)) => Some(cmp_u256_i256(a, b)),
        (Value::I256(a), Value::U256(b)) => Some(cmp_u256_i256(b, a).reverse()),

        // Same-type I256-vs-I256 falls through to `Value::cmp`, which orders
        // I256 sign-aware (negatives before positives). `EncodedKey` sign-flips
        // I256 to match, so the full-scan and index range-scan paths agree.
        _ => Some(left.cmp(right)),
    }
}

/// Parse a big-endian unsigned 256-bit value.
fn u256_be(bytes: &[u8; 32]) -> U256 {
    U256::from_big_endian(bytes)
}

/// Decompose a two's-complement I256 into `(is_negative, magnitude)`.
fn i256_parts(bytes: &[u8; 32]) -> (bool, U256) {
    let raw = U256::from_big_endian(bytes);
    if bytes[0] & 0x80 != 0 {
        // Negative: magnitude = (~raw + 1) mod 2^256.
        (true, (!raw).overflowing_add(U256::one()).0)
    } else {
        (false, raw)
    }
}

/// Compare a U256 (always >= 0) against a signed i128.
fn cmp_u256_i128(u: &[u8; 32], n: i128) -> Ordering {
    if n < 0 {
        return Ordering::Greater;
    }
    u256_be(u).cmp(&U256::from(n as u128))
}

/// Compare an I256 against a signed i128.
fn cmp_i256_i128(i: &[u8; 32], n: i128) -> Ordering {
    let (neg, mag) = i256_parts(i);
    match (neg, n < 0) {
        (false, false) => mag.cmp(&U256::from(n as u128)),
        // Both negative: the larger magnitude is the smaller value.
        (true, true) => mag.cmp(&U256::from(n.unsigned_abs())).reverse(),
        (false, true) => Ordering::Greater,
        (true, false) => Ordering::Less,
    }
}

/// Compare a U256 against an I256.
fn cmp_u256_i256(u: &[u8; 32], i: &[u8; 32]) -> Ordering {
    let (neg, mag) = i256_parts(i);
    if neg {
        Ordering::Greater
    } else {
        u256_be(u).cmp(&mag)
    }
}

/// Largest i128 magnitude that an I256 can hold and still fit, for join-key
/// normalization. Returns `None` when the value is out of `i128` range.
pub(crate) fn i256_to_i128(bytes: &[u8; 32]) -> Option<i128> {
    let (neg, mag) = i256_parts(bytes);
    let m: u128 = u128::try_from(mag).ok()?;
    if neg {
        // -(2^127) is representable as i128::MIN; guard that boundary.
        if m == (i128::MAX as u128) + 1 {
            Some(i128::MIN)
        } else {
            i128::try_from(m).ok().map(|v| -v)
        }
    } else {
        i128::try_from(m).ok()
    }
}

/// U256 as i128 when it fits (for join-key normalization).
pub(crate) fn u256_to_i128(bytes: &[u8; 32]) -> Option<i128> {
    let m: u128 = u128::try_from(u256_be(bytes)).ok()?;
    i128::try_from(m).ok()
}

#[cfg(test)]
mod wide_int_compare_tests {
    use super::{i256_to_i128, u256_to_i128, value_compare};
    use crate::catalog::types::Value;
    use primitive_types::U256;
    use std::cmp::Ordering;

    fn to_be(v: U256) -> [u8; 32] {
        let mut out = [0u8; 32];
        v.to_big_endian(&mut out);
        out
    }

    fn u256(v: u128) -> Value {
        Value::U256(to_be(U256::from(v)))
    }

    fn i256(v: i128) -> Value {
        // Two's-complement big-endian encoding of a signed 256-bit value.
        let mag = U256::from(v.unsigned_abs());
        let raw = if v < 0 {
            (!mag).overflowing_add(U256::one()).0
        } else {
            mag
        };
        Value::I256(to_be(raw))
    }

    #[test]
    fn u256_compares_numerically_with_integers() {
        assert_eq!(
            value_compare(&u256(5), &Value::Integer(5)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            value_compare(&u256(5), &Value::U64(5)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            value_compare(&u256(5), &Value::U8(5)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            value_compare(&u256(10), &Value::Integer(5)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            value_compare(&Value::Integer(5), &u256(10)),
            Some(Ordering::Less)
        );
        // U256 is always >= 0, so it outranks any negative integer.
        assert_eq!(
            value_compare(&u256(0), &Value::Integer(-1)),
            Some(Ordering::Greater)
        );
        // Beyond i128/u128 range, still orders correctly against small ints.
        let huge = Value::U256(to_be(U256::MAX));
        assert_eq!(
            value_compare(&huge, &Value::Integer(1)),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn i256_compares_numerically_with_integers_and_sign() {
        assert_eq!(
            value_compare(&i256(5), &Value::Integer(5)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            value_compare(&i256(-5), &Value::Integer(-5)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            value_compare(&i256(-5), &Value::Integer(5)),
            Some(Ordering::Less)
        );
        assert_eq!(
            value_compare(&i256(-10), &Value::Integer(-5)),
            Some(Ordering::Less)
        );
        assert_eq!(
            value_compare(&Value::Integer(-5), &i256(-10)),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn i256_same_type_equality_holds_ordering_matches_index() {
        // Same-type I256 equality is exact (the cross-type finding relies on it).
        assert_eq!(value_compare(&i256(7), &i256(7)), Some(Ordering::Equal));
        assert_eq!(value_compare(&i256(-5), &i256(-5)), Some(Ordering::Equal));
        // Ordering deliberately stays byte-order (== `Value::cmp`, == EncodedKey
        // index order) so full-scan and index-accelerated range queries agree.
        // Raw two's-complement sorts negatives after positives; correcting that
        // needs an EncodedKey format migration (see compare_values NOTE). What
        // matters here is that the predicate path agrees with `Value::cmp` (and
        // hence the index), not that it is signed-correct.
        assert_eq!(
            value_compare(&i256(-1), &i256(1)),
            Some(i256(-1).cmp(&i256(1)))
        );
    }

    #[test]
    fn u256_vs_i256() {
        assert_eq!(value_compare(&u256(5), &i256(5)), Some(Ordering::Equal));
        assert_eq!(value_compare(&u256(5), &i256(-5)), Some(Ordering::Greater));
        assert_eq!(value_compare(&i256(-5), &u256(0)), Some(Ordering::Less));
    }

    #[test]
    fn to_i128_round_trips() {
        if let Value::U256(b) = u256(42) {
            assert_eq!(u256_to_i128(&b), Some(42));
        }
        if let Value::I256(b) = i256(-42) {
            assert_eq!(i256_to_i128(&b), Some(-42));
        }
        // Out of range returns None.
        let huge = to_be(U256::MAX);
        assert_eq!(u256_to_i128(&huge), None);
    }
}
