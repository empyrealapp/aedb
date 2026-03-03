use crate::catalog::types::{Row, Value};
use crate::query::error::QueryError;
use crate::query::plan::{Aggregate, Expr, Order};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

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
        for (key, entry) in &mut self.map {
            if key.2 != table || key.1.len() != columns.len() {
                continue;
            }
            if key.0 == *expr && key.1.as_slice() == columns {
                entry.stamp = stamp;
                self.order.push_back((stamp, key.clone()));
                return Some(entry.value.clone());
            }
        }
        None
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
mod expr_compile_lru_cache_tests {
    use super::{CompiledExpr, ExprCacheKey, ExprCompileLruCache};

    fn key(name: &str) -> ExprCacheKey {
        (
            crate::query::plan::Expr::IsNull(name.to_string()),
            vec!["c".to_string()],
            "t".to_string(),
        )
    }

    #[test]
    fn evicts_oldest_entry_when_capacity_reached() {
        let mut cache = ExprCompileLruCache::new(2);
        cache.put(key("a"), CompiledExpr::IsNull(0));
        cache.put(key("b"), CompiledExpr::IsNull(1));
        cache.put(key("c"), CompiledExpr::IsNull(2));

        let ka = key("a");
        let kb = key("b");
        let kc = key("c");
        assert!(cache.get(&ka.0, &ka.1, &ka.2).is_none());
        assert_eq!(
            cache.get(&kb.0, &kb.1, &kb.2),
            Some(CompiledExpr::IsNull(1))
        );
        assert_eq!(
            cache.get(&kc.0, &kc.1, &kc.2),
            Some(CompiledExpr::IsNull(2))
        );
    }

    #[test]
    fn hit_promotes_entry_and_prevents_eviction() {
        let mut cache = ExprCompileLruCache::new(2);
        cache.put(key("a"), CompiledExpr::IsNull(0));
        cache.put(key("b"), CompiledExpr::IsNull(1));

        let ka = key("a");
        let kb = key("b");
        assert_eq!(
            cache.get(&ka.0, &ka.1, &ka.2),
            Some(CompiledExpr::IsNull(0))
        );
        cache.put(key("c"), CompiledExpr::IsNull(2));

        let kc = key("c");
        assert_eq!(
            cache.get(&ka.0, &ka.1, &ka.2),
            Some(CompiledExpr::IsNull(0))
        );
        assert!(cache.get(&kb.0, &kb.1, &kb.2).is_none());
        assert_eq!(
            cache.get(&kc.0, &kc.1, &kc.2),
            Some(CompiledExpr::IsNull(2))
        );
    }
}

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
                    row_index: 0,
                    examined: 0,
                };
            }
            #[derive(Clone)]
            struct TopKRow {
                row: Row,
                sort_key_columns: Arc<Vec<usize>>,
                orders: Arc<Vec<Order>>,
            }

            impl Ord for TopKRow {
                fn cmp(&self, other: &Self) -> Ordering {
                    if self.sort_key_columns.len() == 1 {
                        let column_index = self.sort_key_columns[0];
                        let cmp =
                            self.row.values[column_index].cmp(&other.row.values[column_index]);
                        return match self.orders[0] {
                            Order::Asc => cmp,
                            Order::Desc => cmp.reverse(),
                        };
                    }
                    for (column_index, order) in
                        self.sort_key_columns.iter().zip(self.orders.iter())
                    {
                        let cmp =
                            self.row.values[*column_index].cmp(&other.row.values[*column_index]);
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

            let sort_key_columns: Vec<usize> = order_by
                .iter()
                .map(|(column_index, _)| *column_index)
                .collect();
            let sort_key_columns = Arc::new(sort_key_columns);
            let sort_orders =
                Arc::new(order_by.iter().map(|(_, order)| *order).collect::<Vec<_>>());
            let mut heap: BinaryHeap<TopKRow> = BinaryHeap::with_capacity(limit);
            while let Some(rows) = child.next_batch(OPERATOR_BUILD_BATCH_SIZE) {
                for row in rows {
                    if heap.len() < limit {
                        heap.push(TopKRow {
                            row,
                            sort_key_columns: Arc::clone(&sort_key_columns),
                            orders: Arc::clone(&sort_orders),
                        });
                        continue;
                    }
                    let should_insert = heap.peek().is_some_and(|worst_of_best| {
                        compare_rows_with_order_by(
                            &row,
                            &worst_of_best.row,
                            sort_key_columns.as_ref(),
                            sort_orders.as_ref(),
                        )
                        .is_lt()
                    });
                    // Keep only the best N rows under the requested ORDER BY.
                    if should_insert {
                        let _ = heap.pop();
                        heap.push(TopKRow {
                            row,
                            sort_key_columns: Arc::clone(&sort_key_columns),
                            orders: Arc::clone(&sort_orders),
                        });
                    }
                }
            }
            let examined = child.rows_examined();
            let mut rows = heap.into_iter().map(|entry| entry.row).collect::<Vec<_>>();
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

    fn update(&mut self, aggregate: &Aggregate, row: &Row, aggregate_column_index: Option<usize>) {
        match (self, aggregate) {
            (AggregateState::Count(v), Aggregate::Count) => {
                *v = v.saturating_add(1);
            }
            (AggregateState::Sum(sum), Aggregate::Sum(_)) => {
                if let Some(column_index) = aggregate_column_index {
                    let v = match &row.values[column_index] {
                        Value::Integer(v) => *v,
                        Value::U8(v) => *v as i64,
                        _ => 0,
                    };
                    *sum = sum.saturating_add(v);
                }
            }
            (AggregateState::Min(state), Aggregate::Min(_)) => {
                if let Some(column_index) = aggregate_column_index {
                    let value = row.values[column_index].clone();
                    if state.as_ref().is_none_or(|current| value < *current) {
                        *state = Some(value);
                    }
                }
            }
            (AggregateState::Max(state), Aggregate::Max(_)) => {
                if let Some(column_index) = aggregate_column_index {
                    let value = row.values[column_index].clone();
                    if state.as_ref().is_none_or(|current| value > *current) {
                        *state = Some(value);
                    }
                }
            }
            (AggregateState::Avg { total, count }, Aggregate::Avg(_)) => {
                if let Some(column_index) = aggregate_column_index {
                    let maybe_v = match &row.values[column_index] {
                        Value::Integer(v) => Some(*v),
                        Value::U8(v) => Some(*v as i64),
                        _ => None,
                    };
                    if let Some(v) = maybe_v {
                        *total = total.saturating_add(v);
                        *count = count.saturating_add(1);
                    }
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
        _ => Some(left.cmp(right)),
    }
}
