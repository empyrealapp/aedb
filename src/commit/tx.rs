use crate::catalog::types::Value;
use crate::commit::validation::{CompareOp, Mutation};
use crate::error::AedbError;
use crate::permission::CallerContext;
use crate::query::plan::Expr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(pub [u8; 16]);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReadKey {
    TableRow {
        project_id: String,
        scope_id: String,
        table_name: String,
        primary_key: Vec<crate::catalog::types::Value>,
    },
    KvKey {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadSetEntry {
    pub key: ReadKey,
    pub version_at_read: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReadBound<T> {
    Unbounded,
    Included(T),
    Excluded(T),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReadRange {
    TableRange {
        project_id: String,
        scope_id: String,
        table_name: String,
        start: ReadBound<Vec<crate::catalog::types::Value>>,
        end: ReadBound<Vec<crate::catalog::types::Value>>,
    },
    /// A read scoped to every row whose primary key begins with `prefix` (a leading
    /// prefix of the PK). Recorded by bounded primary-key-prefix scans so a reactive
    /// subscription over one key band (e.g. one instance's rows in a shared table) is
    /// only invalidated by writes within that band — not by every write to the table.
    TablePrefix {
        project_id: String,
        scope_id: String,
        table_name: String,
        prefix: Vec<crate::catalog::types::Value>,
    },
    KvRange {
        project_id: String,
        scope_id: String,
        start: ReadBound<Vec<u8>>,
        end: ReadBound<Vec<u8>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadRangeEntry {
    pub range: ReadRange,
    pub max_version_at_read: u64,
    #[serde(default)]
    pub structural_version_at_read: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ReadSet {
    #[serde(default)]
    pub points: Vec<ReadSetEntry>,
    #[serde(default)]
    pub ranges: Vec<ReadRangeEntry>,
}

/// A concrete write effect from a single mutation, expressed in the
/// same shape as a [`ReadKey`] / [`ReadRange`] so it can be intersected
/// with a [`ReadSet`] cheaply.
///
/// Mutations whose effect is broad (predicate updates, DDL, internal
/// event-outbox bookkeeping, order-book state) are reported
/// conservatively as a `TableRange` / `KvRange` over the full table or
/// scope. Callers must therefore treat `WriteKey` as a *superset* of the
/// actually written keys — intersection may be false-positive but never
/// false-negative.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WriteKey {
    TableRow {
        project_id: String,
        scope_id: String,
        table_name: String,
        primary_key: Vec<Value>,
    },
    TableRange {
        project_id: String,
        scope_id: String,
        table_name: String,
        start: ReadBound<Vec<Value>>,
        end: ReadBound<Vec<Value>>,
    },
    KvKey {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
    },
    KvRange {
        project_id: String,
        scope_id: String,
        start: ReadBound<Vec<u8>>,
        end: ReadBound<Vec<u8>>,
    },
    /// Conservative catch-all: any read in this (project, scope) intersects.
    /// Emitted for DDL, event outbox, order-book state.
    ScopeAll {
        project_id: String,
        scope_id: String,
    },
}

impl WriteKey {
    /// Returns the (project, scope) pair this write applies to. `None` if
    /// the write affects multiple scopes (currently no variant does).
    pub fn project_scope(&self) -> (&str, &str) {
        match self {
            WriteKey::TableRow {
                project_id,
                scope_id,
                ..
            }
            | WriteKey::TableRange {
                project_id,
                scope_id,
                ..
            }
            | WriteKey::KvKey {
                project_id,
                scope_id,
                ..
            }
            | WriteKey::KvRange {
                project_id,
                scope_id,
                ..
            }
            | WriteKey::ScopeAll {
                project_id,
                scope_id,
            } => (project_id.as_str(), scope_id.as_str()),
        }
    }
}

impl ReadSet {
    /// Returns `true` when any read key or range in this set overlaps any
    /// of the supplied [`WriteKey`]s.
    ///
    /// Overlap considers project_id, scope_id, table_name, and key/PK
    /// extent. Keys of mismatching kind (e.g. table vs kv) never overlap.
    /// `WriteKey::ScopeAll` overlaps every read with matching
    /// `(project_id, scope_id)`.
    pub fn intersects(&self, write_keys: &[WriteKey]) -> bool {
        if write_keys.is_empty() {
            return false;
        }
        for entry in &self.points {
            for wk in write_keys {
                if read_key_intersects_write_key(&entry.key, wk) {
                    return true;
                }
            }
        }
        for entry in &self.ranges {
            for wk in write_keys {
                if read_range_intersects_write_key(&entry.range, wk) {
                    return true;
                }
            }
        }
        false
    }
}

fn read_key_intersects_write_key(read: &ReadKey, write: &WriteKey) -> bool {
    match (read, write) {
        (
            ReadKey::TableRow {
                project_id: rp,
                scope_id: rs,
                table_name: rt,
                primary_key: rpk,
            },
            WriteKey::TableRow {
                project_id: wp,
                scope_id: ws,
                table_name: wt,
                primary_key: wpk,
            },
        ) => rp == wp && rs == ws && rt == wt && rpk == wpk,
        (
            ReadKey::TableRow {
                project_id: rp,
                scope_id: rs,
                table_name: rt,
                primary_key,
            },
            WriteKey::TableRange {
                project_id: wp,
                scope_id: ws,
                table_name: wt,
                start,
                end,
            },
        ) => rp == wp && rs == ws && rt == wt && pk_in_range(primary_key, start, end),
        (
            ReadKey::KvKey {
                project_id: rp,
                scope_id: rs,
                key: rk,
            },
            WriteKey::KvKey {
                project_id: wp,
                scope_id: ws,
                key: wk,
            },
        ) => rp == wp && rs == ws && rk == wk,
        (
            ReadKey::KvKey {
                project_id: rp,
                scope_id: rs,
                key,
            },
            WriteKey::KvRange {
                project_id: wp,
                scope_id: ws,
                start,
                end,
            },
        ) => rp == wp && rs == ws && bytes_in_range(key, start, end),
        (
            ReadKey::TableRow {
                project_id: rp,
                scope_id: rs,
                ..
            }
            | ReadKey::KvKey {
                project_id: rp,
                scope_id: rs,
                ..
            },
            WriteKey::ScopeAll {
                project_id: wp,
                scope_id: ws,
            },
        ) => rp == wp && rs == ws,
        // mismatching kinds (table vs kv) do not overlap
        _ => false,
    }
}

fn read_range_intersects_write_key(read: &ReadRange, write: &WriteKey) -> bool {
    match (read, write) {
        (
            ReadRange::TableRange {
                project_id: rp,
                scope_id: rs,
                table_name: rt,
                start: rstart,
                end: rend,
            },
            WriteKey::TableRow {
                project_id: wp,
                scope_id: ws,
                table_name: wt,
                primary_key,
            },
        ) => rp == wp && rs == ws && rt == wt && pk_in_range(primary_key, rstart, rend),
        (
            ReadRange::TableRange {
                project_id: rp,
                scope_id: rs,
                table_name: rt,
                start: rstart,
                end: rend,
            },
            WriteKey::TableRange {
                project_id: wp,
                scope_id: ws,
                table_name: wt,
                start: wstart,
                end: wend,
            },
        ) => rp == wp && rs == ws && rt == wt && ranges_overlap_pk(rstart, rend, wstart, wend),
        (
            ReadRange::TablePrefix {
                project_id: rp,
                scope_id: rs,
                table_name: rt,
                prefix,
            },
            WriteKey::TableRow {
                project_id: wp,
                scope_id: ws,
                table_name: wt,
                primary_key,
            },
        ) => rp == wp && rs == ws && rt == wt && pk_has_prefix(primary_key, prefix),
        (
            // A predicate write (whole-table WriteKey::TableRange) could touch any row,
            // so conservatively invalidate a prefix read on the same table.
            ReadRange::TablePrefix {
                project_id: rp,
                scope_id: rs,
                table_name: rt,
                ..
            },
            WriteKey::TableRange {
                project_id: wp,
                scope_id: ws,
                table_name: wt,
                ..
            },
        ) => rp == wp && rs == ws && rt == wt,
        (
            ReadRange::KvRange {
                project_id: rp,
                scope_id: rs,
                start: rstart,
                end: rend,
            },
            WriteKey::KvKey {
                project_id: wp,
                scope_id: ws,
                key,
            },
        ) => rp == wp && rs == ws && bytes_in_range(key, rstart, rend),
        (
            ReadRange::KvRange {
                project_id: rp,
                scope_id: rs,
                start: rstart,
                end: rend,
            },
            WriteKey::KvRange {
                project_id: wp,
                scope_id: ws,
                start: wstart,
                end: wend,
            },
        ) => rp == wp && rs == ws && ranges_overlap_bytes(rstart, rend, wstart, wend),
        (
            ReadRange::TableRange {
                project_id: rp,
                scope_id: rs,
                ..
            }
            | ReadRange::TablePrefix {
                project_id: rp,
                scope_id: rs,
                ..
            }
            | ReadRange::KvRange {
                project_id: rp,
                scope_id: rs,
                ..
            },
            WriteKey::ScopeAll {
                project_id: wp,
                scope_id: ws,
            },
        ) => rp == wp && rs == ws,
        _ => false,
    }
}

/// True if the full primary key `point` begins with `prefix` (a leading PK prefix).
fn pk_has_prefix(point: &[Value], prefix: &[Value]) -> bool {
    point.len() >= prefix.len()
        && cmp_pk(prefix, &point[..prefix.len()]) == std::cmp::Ordering::Equal
}

fn pk_in_range(
    point: &[Value],
    start: &ReadBound<Vec<Value>>,
    end: &ReadBound<Vec<Value>>,
) -> bool {
    let lo_ok = match start {
        ReadBound::Unbounded => true,
        ReadBound::Included(v) => !matches!(cmp_pk(v, point), std::cmp::Ordering::Greater),
        ReadBound::Excluded(v) => matches!(cmp_pk(v, point), std::cmp::Ordering::Less),
    };
    if !lo_ok {
        return false;
    }
    match end {
        ReadBound::Unbounded => true,
        ReadBound::Included(v) => !matches!(cmp_pk(point, v), std::cmp::Ordering::Greater),
        ReadBound::Excluded(v) => matches!(cmp_pk(point, v), std::cmp::Ordering::Less),
    }
}

fn bytes_in_range(point: &[u8], start: &ReadBound<Vec<u8>>, end: &ReadBound<Vec<u8>>) -> bool {
    let lo_ok = match start {
        ReadBound::Unbounded => true,
        ReadBound::Included(v) => v.as_slice() <= point,
        ReadBound::Excluded(v) => v.as_slice() < point,
    };
    if !lo_ok {
        return false;
    }
    match end {
        ReadBound::Unbounded => true,
        ReadBound::Included(v) => point <= v.as_slice(),
        ReadBound::Excluded(v) => point < v.as_slice(),
    }
}

fn cmp_pk(a: &[Value], b: &[Value]) -> std::cmp::Ordering {
    let n = a.len().min(b.len());
    for i in 0..n {
        match a[i].cmp(&b[i]) {
            std::cmp::Ordering::Equal => continue,
            other => return other,
        }
    }
    a.len().cmp(&b.len())
}

fn ranges_overlap_pk(
    a_start: &ReadBound<Vec<Value>>,
    a_end: &ReadBound<Vec<Value>>,
    b_start: &ReadBound<Vec<Value>>,
    b_end: &ReadBound<Vec<Value>>,
) -> bool {
    fn lo_le_hi(lo: &ReadBound<Vec<Value>>, hi: &ReadBound<Vec<Value>>) -> bool {
        match (lo, hi) {
            (ReadBound::Unbounded, _) | (_, ReadBound::Unbounded) => true,
            (ReadBound::Included(l), ReadBound::Included(h)) => {
                !matches!(cmp_pk(l, h), std::cmp::Ordering::Greater)
            }
            (ReadBound::Included(l), ReadBound::Excluded(h))
            | (ReadBound::Excluded(l), ReadBound::Included(h))
            | (ReadBound::Excluded(l), ReadBound::Excluded(h)) => {
                matches!(cmp_pk(l, h), std::cmp::Ordering::Less)
            }
        }
    }
    lo_le_hi(a_start, b_end) && lo_le_hi(b_start, a_end)
}

fn ranges_overlap_bytes(
    a_start: &ReadBound<Vec<u8>>,
    a_end: &ReadBound<Vec<u8>>,
    b_start: &ReadBound<Vec<u8>>,
    b_end: &ReadBound<Vec<u8>>,
) -> bool {
    fn lo_le_hi(lo: &ReadBound<Vec<u8>>, hi: &ReadBound<Vec<u8>>) -> bool {
        match (lo, hi) {
            (ReadBound::Unbounded, _) | (_, ReadBound::Unbounded) => true,
            (ReadBound::Included(l), ReadBound::Included(h)) => l.as_slice() <= h.as_slice(),
            (ReadBound::Included(l), ReadBound::Excluded(h))
            | (ReadBound::Excluded(l), ReadBound::Included(h))
            | (ReadBound::Excluded(l), ReadBound::Excluded(h)) => l.as_slice() < h.as_slice(),
        }
    }
    lo_le_hi(a_start, b_end) && lo_le_hi(b_start, a_end)
}

/// Commit-time predicates evaluated against the working state before an
/// envelope's mutations are applied.
///
/// Preflight can use the same shapes to produce helpful early feedback, but
/// these assertions are the authoritative concurrency boundary. Pair them with
/// atomic numeric mutations when a business invariant must survive contention,
/// for example "this balance is still above the debit amount".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReadAssertion {
    KeyEquals {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    },
    KeyCompare {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
        op: CompareOp,
        threshold: Vec<u8>,
    },
    KeyExists {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
        expected: bool,
    },
    KeyVersion {
        project_id: String,
        scope_id: String,
        key: Vec<u8>,
        expected_seq: u64,
    },
    RowVersion {
        project_id: String,
        scope_id: String,
        table_name: String,
        primary_key: Vec<Value>,
        expected_seq: u64,
    },
    RowExists {
        project_id: String,
        scope_id: String,
        table_name: String,
        primary_key: Vec<Value>,
        expected: bool,
    },
    RowColumnCompare {
        project_id: String,
        scope_id: String,
        table_name: String,
        primary_key: Vec<Value>,
        column: String,
        op: CompareOp,
        threshold: Value,
    },
    CountCompare {
        project_id: String,
        scope_id: String,
        table_name: String,
        filter: Option<Expr>,
        op: CompareOp,
        threshold: u64,
    },
    SumCompare {
        project_id: String,
        scope_id: String,
        table_name: String,
        column: String,
        filter: Option<Expr>,
        op: CompareOp,
        threshold: Value,
    },
    All(Vec<ReadAssertion>),
    Any(Vec<ReadAssertion>),
    Not(Box<ReadAssertion>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AssertionActual {
    Missing,
    Bool(bool),
    Version(u64),
    Bytes(Vec<u8>),
    Value(Value),
    Count(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteIntent {
    /// Ordered mutations for one atomic envelope. `Mutation::PostflightCheck`
    /// can appear after atomic updates to validate the transaction-local
    /// post-update state before the envelope is published.
    pub mutations: Vec<Mutation>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum WriteClass {
    Economic,
    #[default]
    Standard,
    /// Applied to the in-memory keyspace (so it is immediately visible to readers)
    /// but **never written to the WAL**, and it does not advance the durable-seq
    /// watermark.
    ///
    /// Persistence: a checkpoint is anchored to the durable horizon, so volatile
    /// writes are captured by a checkpoint only once a *durable* commit advances
    /// the durable seq past them. Volatile writes made after the most recent
    /// durable commit are held purely in memory and are lost on any crash/restart.
    /// In effect a program "anchors" accumulated volatile state by emitting a
    /// durable commit (e.g. a lifecycle/heartbeat write) before the next checkpoint.
    ///
    /// Intended for high-frequency, regenerable state (e.g. an ECS simulation's
    /// per-tick entity writes) where paying WAL fsync + replay cost per tick is
    /// wasteful and losing the tail window since the last durable anchor is
    /// acceptable. Requesting [`CommitFinality::Durable`] on a volatile commit is a
    /// no-op — there is no WAL durability to wait for.
    Volatile,
}

impl WriteClass {
    /// Whether commits of this class skip the WAL and persist only via checkpoints.
    #[inline]
    pub fn is_volatile(self) -> bool {
        matches!(self, WriteClass::Volatile)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalCommitPayload {
    pub mutations: Vec<Mutation>,
    pub assertions: Vec<ReadAssertion>,
    pub idempotency_key: Option<IdempotencyKey>,
    #[serde(default)]
    pub request_fingerprint: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IdempotencyRecord {
    pub commit_seq: u64,
    pub recorded_at_micros: u64,
    #[serde(default)]
    pub request_fingerprint: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionEnvelope {
    pub caller: Option<CallerContext>,
    pub idempotency_key: Option<IdempotencyKey>,
    #[serde(default)]
    pub write_class: WriteClass,
    /// Authoritative checks evaluated immediately before `write_intent`.
    /// Use these with atomic updates for invariants that cannot rely on the
    /// advisory preflight snapshot alone.
    pub assertions: Vec<ReadAssertion>,
    /// Versions/ranges observed during preflight or planning. This guards
    /// against TOCTOU by rejecting envelopes whose planned reads went stale
    /// before commit.
    pub read_set: ReadSet,
    pub write_intent: WriteIntent,
    pub base_seq: u64,
}

#[derive(Serialize)]
struct IdempotencyFingerprintPayload<'a> {
    caller: Option<&'a CallerContext>,
    write_class: WriteClass,
    assertions: &'a [ReadAssertion],
    read_set: &'a ReadSet,
    write_intent: &'a WriteIntent,
    base_seq: u64,
}

impl TransactionEnvelope {
    pub fn request_fingerprint(&self) -> Result<[u8; 32], AedbError> {
        let payload = IdempotencyFingerprintPayload {
            caller: self.caller.as_ref(),
            write_class: self.write_class,
            assertions: &self.assertions,
            read_set: &self.read_set,
            write_intent: &self.write_intent,
            base_seq: self.base_seq,
        };
        let encoded = rmp_serde::to_vec(&payload).map_err(|e| AedbError::Encode(e.to_string()))?;
        Ok(*blake3::hash(&encoded).as_bytes())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightPlan {
    /// Advisory result from the snapshot inspected during preflight.
    /// A valid plan can still be rejected at commit if its read set or
    /// assertions no longer match current state.
    pub valid: bool,
    pub read_set: ReadSet,
    pub write_intent: WriteIntent,
    pub base_seq: u64,
    pub estimated_affected_rows: usize,
    pub errors: Vec<String>,
}

#[cfg(test)]
mod table_prefix_tests {
    use super::*;
    use crate::catalog::types::Value;

    fn prefix_read(prefix: Vec<Value>) -> ReadSet {
        ReadSet {
            points: vec![],
            ranges: vec![ReadRangeEntry {
                range: ReadRange::TablePrefix {
                    project_id: "arcana".into(),
                    scope_id: "app".into(),
                    table_name: "entities".into(),
                    prefix,
                },
                max_version_at_read: 0,
                structural_version_at_read: 0,
            }],
        }
    }

    fn row_write(table: &str, pk: Vec<Value>) -> WriteKey {
        WriteKey::TableRow {
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: table.into(),
            primary_key: pk,
        }
    }

    #[test]
    fn table_prefix_read_invalidated_only_within_band() {
        // A reactive read over one instance's rows in the shared entities table.
        let read = prefix_read(vec![Value::Text("a".into())]);

        // A write to a row in instance "a"'s band wakes it (this also covers a future
        // insert — the range matches any PK beginning with the prefix, not just rows
        // that existed at read time).
        assert!(read.intersects(&[row_write("entities", vec![
            Value::Text("a".into()),
            Value::Text("e1".into()),
        ])]));

        // A write to a *different* instance's band must NOT wake it — this is the whole
        // point: 5000 games writing the shared table don't cross-invalidate.
        assert!(!read.intersects(&[row_write("entities", vec![
            Value::Text("b".into()),
            Value::Text("e1".into()),
        ])]));

        // A whole-table predicate write (WriteKey::TableRange) conservatively wakes it.
        assert!(read.intersects(&[WriteKey::TableRange {
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "entities".into(),
            start: ReadBound::Unbounded,
            end: ReadBound::Unbounded,
        }]));

        // A ScopeAll write (e.g. DDL) wakes it.
        assert!(read.intersects(&[WriteKey::ScopeAll {
            project_id: "arcana".into(),
            scope_id: "app".into(),
        }]));

        // A write to a different table does not.
        assert!(!read.intersects(&[row_write("other", vec![Value::Text("a".into())])]));
    }
}
