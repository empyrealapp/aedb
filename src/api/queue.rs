//! Durable task/timer queue.
//!
//! A single durable, time-ordered, priority-aware queue is the substrate under
//! every asynchronous concept in the platform — task queues, schedulers,
//! timers, delayed/retried work, dead-lettering, and workflow steps. The engine
//! never knows what a "keeper" or "workflow" is; it knows queues, due times,
//! leases, and transactions. Arcana gives those meaning.
//!
//! Everything composes from one record shape:
//! * **delay / timer / scheduled execution** = `not_before = now + delay`.
//! * **retries / backoff** = on failure, re-arm with a later `not_before` and a
//!   bumped attempt count; exhausting `max_attempts` moves the task to the
//!   dead-letter state.
//! * **transactional enqueue (outbox)** = [`AedbInstance::queue_enqueue`] applies
//!   the caller's own `extra_mutations` in the *same commit* as the enqueue, so a
//!   task is created if and only if the surrounding state change commits.
//! * **exactly-once completion / scheduler re-arm / workflow next step** =
//!   [`AedbInstance::queue_complete`] acks the task, applies the caller's
//!   mutations, and enqueues any `follow_ups` in one atomic, lease-fenced commit.
//!
//! Ownership uses the same fencing-token discipline as [`crate::api::lease`]: a
//! claim bumps a per-task fencing token and sets a lease deadline; a crashed
//! owner's lease expires and the task is reclaimed, and a zombie owner's
//! `complete`/`fail`/`heartbeat` is fenced out.
//!
//! All state lives in KV under [`QUEUE_KEY_PREFIX`] within the caller's
//! project/scope (durable across restart/checkpoint/restore). Application KV
//! keys must not begin with this prefix. The per-queue sequence counter
//! serializes concurrent enqueues to the *same* queue (a CAS retry point);
//! distinct queues never contend.

use std::collections::HashMap;
use std::ops::Bound;

use serde::{Deserialize, Serialize};

use crate::commit::tx::{ReadAssertion, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use crate::commit::validation::Mutation;
use crate::error::AedbError;
use crate::permission::CallerContext;
use crate::query::plan::ConsistencyMode;
use crate::storage::keyspace::KvEntry;
use crate::{AedbInstance, FencedCommit};

/// Reserved KV key prefix for durable queues. Application keys must not start
/// with these bytes.
pub const QUEUE_KEY_PREFIX: &[u8] = b"\x00aedb:queue:";

/// Lifecycle state of a durable-queue task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// Waiting to be claimed; runnable once `not_before_micros` has passed.
    Ready,
    /// Claimed by an owner under a lease.
    InFlight,
    /// Attempt budget exhausted; parked in the dead-letter state for inspection.
    Dead,
}

/// Options for enqueuing a durable-queue task.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnqueueOptions {
    /// Opaque task payload; must be empty or valid JSON.
    pub payload_json: String,
    /// Defer execution by this many micros (a timer / scheduled task).
    pub delay_micros: u64,
    /// Higher priority is served first among due tasks.
    pub priority: u64,
    /// Max attempts before dead-lettering; `0` uses the engine default.
    pub max_attempts: u32,
    /// First-retry backoff; `0` uses the engine default.
    pub backoff_base_micros: u64,
    /// Backoff ceiling; `0` uses the engine default.
    pub backoff_max_micros: u64,
    /// If set, makes the enqueue idempotent within the queue.
    pub idempotency_key: Option<String>,
}

/// A target queue plus its [`EnqueueOptions`] — used for `queue_complete`
/// follow-ups (scheduler re-arm, workflow next step, event fan-out).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueSpec {
    pub queue: String,
    pub options: EnqueueOptions,
}

/// Result of a `queue_enqueue`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueOutcome {
    /// The id assigned to the task (or the pre-existing id on a dedupe hit).
    pub task_id: String,
    /// The per-queue sequence number assigned (0 on a dedupe hit).
    pub seq: u64,
    /// True if an idempotency key matched an existing task; nothing was enqueued.
    pub deduplicated: bool,
}

/// A task handed to a worker by `queue_claim`, carrying the fencing token the
/// worker must present to `queue_complete`/`queue_fail`/`queue_heartbeat`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedTask {
    pub task_id: String,
    pub queue: String,
    pub payload_json: String,
    pub attempts: u32,
    pub fencing_token: u64,
    pub lease_deadline_micros: u64,
    pub priority: u64,
    pub enqueued_micros: u64,
}

/// Full inspection view of a durable-queue task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRecord {
    pub task_id: String,
    pub queue: String,
    pub state: TaskState,
    pub not_before_micros: u64,
    pub priority: u64,
    pub attempts: u32,
    pub max_attempts: u32,
    pub owner_id: Option<String>,
    pub fencing_token: u64,
    pub lease_deadline_micros: u64,
    pub payload_json: String,
    pub progress_json: String,
    pub result_json: String,
    pub last_error: Option<String>,
    pub created_micros: u64,
    pub updated_micros: u64,
}

const MAX_CAS_RETRIES: usize = 16;

/// Default retry budget applied when `max_attempts == 0`.
const DEFAULT_MAX_ATTEMPTS: u32 = 5;
/// Default first-retry backoff (1s) when `backoff_base_micros == 0`.
const DEFAULT_BACKOFF_BASE_MICROS: u64 = 1_000_000;
/// Default backoff ceiling (60s) when `backoff_max_micros == 0`.
const DEFAULT_BACKOFF_MAX_MICROS: u64 = 60_000_000;

// Record-kind discriminators within a queue's keyspace.
const KIND_PRIMARY: u8 = b'p'; // primary task record, keyed by id
const KIND_SCHEDULED: u8 = b's'; // waiting-for-due-time index (ordered by not_before)
const KIND_READY: u8 = b'r'; // runnable index (ordered by priority, then FIFO)
const KIND_INFLIGHT: u8 = b'f'; // in-flight index (ordered by lease deadline)
const KIND_IDEM: u8 = b'k'; // idempotency-key index
const KIND_COUNTER: u8 = b'n'; // per-queue monotonic sequence counter

const STATE_READY: u8 = 0;
const STATE_INFLIGHT: u8 = 1;
const STATE_DEAD: u8 = 2;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StoredTask {
    id: String,
    queue: String,
    state: u8,
    not_before_micros: u64,
    priority: u64,
    seq: u64,
    /// For a `Ready`-state task: `true` if it sits in the priority-ordered ready
    /// index, `false` if it is still in the due-time-ordered scheduled index.
    #[serde(default)]
    in_ready: bool,
    attempts: u32,
    max_attempts: u32,
    backoff_base_micros: u64,
    backoff_max_micros: u64,
    #[serde(default)]
    owner_id: Option<String>,
    #[serde(default)]
    fencing_token: u64,
    #[serde(default)]
    lease_deadline_micros: u64,
    #[serde(default)]
    payload_json: String,
    #[serde(default)]
    progress_json: String,
    #[serde(default)]
    result_json: String,
    #[serde(default)]
    last_error: Option<String>,
    created_micros: u64,
    updated_micros: u64,
}

// ---- key construction -------------------------------------------------------

fn be(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

/// `PREFIX || u16(qlen) || qname` — the per-queue keyspace base.
fn q_base(queue: &str) -> Vec<u8> {
    let qbytes = queue.as_bytes();
    let mut k = Vec::with_capacity(QUEUE_KEY_PREFIX.len() + 2 + qbytes.len() + 24);
    k.extend_from_slice(QUEUE_KEY_PREFIX);
    k.extend_from_slice(&(qbytes.len() as u16).to_be_bytes());
    k.extend_from_slice(qbytes);
    k
}

fn primary_key(queue: &str, id: &str) -> Vec<u8> {
    let mut k = q_base(queue);
    k.push(KIND_PRIMARY);
    k.extend_from_slice(id.as_bytes());
    k
}

fn primary_prefix(queue: &str) -> Vec<u8> {
    let mut k = q_base(queue);
    k.push(KIND_PRIMARY);
    k
}

fn counter_key(queue: &str) -> Vec<u8> {
    let mut k = q_base(queue);
    k.push(KIND_COUNTER);
    k
}

fn idem_key(queue: &str, key: &str) -> Vec<u8> {
    let mut k = q_base(queue);
    k.push(KIND_IDEM);
    k.extend_from_slice(key.as_bytes());
    k
}

/// Scheduled index key: tasks waiting for their due time, ordered by
/// (not_before asc, seq asc). A matured entry is promoted into the ready index.
fn scheduled_key(queue: &str, not_before: u64, seq: u64, id: &str) -> Vec<u8> {
    let mut k = q_base(queue);
    k.push(KIND_SCHEDULED);
    k.extend_from_slice(&be(not_before));
    k.extend_from_slice(&be(seq));
    k.extend_from_slice(id.as_bytes());
    k
}

/// Ready index key: runnable tasks ordered by (priority desc, seq asc) so a
/// claim serves the highest-priority due task, FIFO within a priority. The id
/// suffix guarantees uniqueness even on a full tuple collision.
fn ready_key(queue: &str, priority: u64, seq: u64, id: &str) -> Vec<u8> {
    let mut k = q_base(queue);
    k.push(KIND_READY);
    k.extend_from_slice(&be(u64::MAX - priority)); // higher priority sorts first
    k.extend_from_slice(&be(seq));
    k.extend_from_slice(id.as_bytes());
    k
}

fn ready_prefix(queue: &str) -> Vec<u8> {
    let mut k = q_base(queue);
    k.push(KIND_READY);
    k
}

/// The index key a `Ready`-state task currently occupies.
fn ready_state_index_key(queue: &str, task: &StoredTask) -> Vec<u8> {
    if task.in_ready {
        ready_key(queue, task.priority, task.seq, &task.id)
    } else {
        scheduled_key(queue, task.not_before_micros, task.seq, &task.id)
    }
}

/// In-flight index key: ordered by lease deadline asc, so the earliest-expiring
/// lease is reclaimed first.
fn inflight_key(queue: &str, deadline: u64, seq: u64, id: &str) -> Vec<u8> {
    let mut k = q_base(queue);
    k.push(KIND_INFLIGHT);
    k.extend_from_slice(&be(deadline));
    k.extend_from_slice(&be(seq));
    k.extend_from_slice(id.as_bytes());
    k
}

/// Inclusive lower / exclusive upper bounds selecting index keys of `kind` whose
/// leading u64 time field is `<= now`.
fn due_bounds(queue: &str, kind: u8, now: u64) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let mut lo = q_base(queue);
    lo.push(kind);
    let mut hi = lo.clone();
    hi.extend_from_slice(&be(now.saturating_add(1)));
    (Bound::Included(lo), Bound::Excluded(hi))
}

fn is_retryable_conflict(err: &AedbError) -> bool {
    matches!(
        err,
        AedbError::Conflict(_) | AedbError::AssertionFailed { .. }
    )
}

fn key_assertion(
    project_id: &str,
    scope_id: &str,
    key: &[u8],
    entry: Option<&KvEntry>,
) -> ReadAssertion {
    match entry {
        Some(entry) => ReadAssertion::KeyVersion {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key: key.to_vec(),
            expected_seq: entry.version,
        },
        None => ReadAssertion::KeyExists {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            key: key.to_vec(),
            expected: false,
        },
    }
}

fn set_mut(project_id: &str, scope_id: &str, key: Vec<u8>, value: Vec<u8>) -> Mutation {
    Mutation::KvSet {
        project_id: project_id.to_string(),
        scope_id: scope_id.to_string(),
        key,
        value,
    }
}

fn del_mut(project_id: &str, scope_id: &str, key: Vec<u8>) -> Mutation {
    Mutation::KvDel {
        project_id: project_id.to_string(),
        scope_id: scope_id.to_string(),
        key,
    }
}

fn encode_task(task: &StoredTask) -> Result<Vec<u8>, AedbError> {
    serde_json::to_vec(task).map_err(|e| AedbError::Encode(format!("task encode failed: {e}")))
}

fn decode_task(entry: &KvEntry) -> Result<StoredTask, AedbError> {
    serde_json::from_slice(&entry.value)
        .map_err(|_| AedbError::Decode("task record is corrupt".into()))
}

fn next_backoff(task: &StoredTask, now: u64) -> u64 {
    let base = if task.backoff_base_micros == 0 {
        DEFAULT_BACKOFF_BASE_MICROS
    } else {
        task.backoff_base_micros
    };
    let ceil = if task.backoff_max_micros == 0 {
        DEFAULT_BACKOFF_MAX_MICROS
    } else {
        task.backoff_max_micros
    };
    // base * 2^(attempts-1), saturating, clamped to the ceiling.
    let shift = task.attempts.saturating_sub(1).min(32);
    let delay = base.saturating_mul(1u64 << shift).min(ceil);
    now.saturating_add(delay)
}

fn ensure_valid_json(s: &str, what: &str) -> Result<(), AedbError> {
    if s.is_empty() {
        return Ok(());
    }
    serde_json::from_str::<serde_json::Value>(s)
        .map(|_| ())
        .map_err(|_| AedbError::Validation(format!("{what} must be empty or valid JSON")))
}

/// Per-queue counter state accumulated while planning one (possibly multi-queue,
/// multi-task) enqueue commit, so several follow-ups to the same queue get
/// consecutive sequence numbers under a single counter CAS.
struct CounterCursor {
    entry_version: Option<u64>,
    next_seq: u64,
}

/// Accumulates the mutations/assertions for a set of enqueues sharing one commit.
struct EnqueuePlan {
    mutations: Vec<Mutation>,
    assertions: Vec<ReadAssertion>,
    counters: HashMap<String, CounterCursor>,
}

impl EnqueuePlan {
    fn new() -> Self {
        Self {
            mutations: Vec::new(),
            assertions: Vec::new(),
            counters: HashMap::new(),
        }
    }
}

impl AedbInstance {
    /// Enqueue a task. The task is created atomically with the caller's
    /// `extra_mutations` (the transactional-outbox guarantee: the task exists iff
    /// the surrounding state change commits). `options.delay_micros` defers
    /// execution (a timer); `options.priority` orders ties (higher first);
    /// `options.idempotency_key`, if set, makes the enqueue idempotent.
    pub async fn queue_enqueue(
        &self,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        options: EnqueueOptions,
        extra_mutations: Vec<Mutation>,
    ) -> Result<EnqueueOutcome, AedbError> {
        self.queue_enqueue_inner(None, project_id, scope_id, queue, options, extra_mutations)
            .await
    }

    /// [`AedbInstance::queue_enqueue`] on behalf of `caller` (must hold the
    /// permissions for `extra_mutations` plus `KvWrite` on the project/scope).
    pub async fn queue_enqueue_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        options: EnqueueOptions,
        extra_mutations: Vec<Mutation>,
    ) -> Result<EnqueueOutcome, AedbError> {
        self.queue_enqueue_inner(
            Some(caller),
            project_id,
            scope_id,
            queue,
            options,
            extra_mutations,
        )
        .await
    }

    async fn queue_enqueue_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        options: EnqueueOptions,
        extra_mutations: Vec<Mutation>,
    ) -> Result<EnqueueOutcome, AedbError> {
        if queue.is_empty() {
            return Err(AedbError::Validation("queue name cannot be empty".into()));
        }
        ensure_valid_json(&options.payload_json, "payload_json")?;
        let spec = EnqueueSpec {
            queue: queue.to_string(),
            options,
        };

        for _ in 0..MAX_CAS_RETRIES {
            let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
            let base_seq = lease.view.seq;
            let view = &lease.view.keyspace;
            let now = crate::system_now_micros();

            // Idempotency short-circuit against committed state.
            if let Some(idem) = &spec.options.idempotency_key
                && let Some(entry) = view.kv_get(project_id, scope_id, &idem_key(queue, idem))
            {
                let task_id = String::from_utf8_lossy(&entry.value).into_owned();
                return Ok(EnqueueOutcome {
                    task_id,
                    seq: 0,
                    deduplicated: true,
                });
            }

            let mut plan = EnqueuePlan::new();
            let outcome = self.plan_enqueue(project_id, scope_id, view, &spec, now, &mut plan)?;
            plan.mutations.extend(extra_mutations.clone());
            self.finalize_counters(project_id, scope_id, view, &mut plan);

            match self.commit_plan(caller.clone(), plan, base_seq).await {
                Ok(_) => return Ok(outcome),
                Err(err) if is_retryable_conflict(&err) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(AedbError::Conflict(
            "queue_enqueue exhausted retries".into(),
        ))
    }

    /// Claim up to `max` ready tasks for `owner_id`, leasing each for
    /// `lease_micros`. First reclaims tasks whose previous owner's lease expired
    /// (retrying or dead-lettering as their attempt budget dictates), then
    /// returns due tasks in (due-time, priority, FIFO) order. Each returned task
    /// carries a fencing token for `queue_complete`/`queue_fail`/`queue_heartbeat`.
    pub async fn queue_claim(
        &self,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        owner_id: &str,
        lease_micros: u64,
        max: usize,
    ) -> Result<Vec<ClaimedTask>, AedbError> {
        self.queue_claim_inner(
            None,
            project_id,
            scope_id,
            queue,
            owner_id,
            lease_micros,
            max,
        )
        .await
    }

    /// [`AedbInstance::queue_claim`] on behalf of `caller`.
    #[allow(clippy::too_many_arguments)]
    pub async fn queue_claim_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        owner_id: &str,
        lease_micros: u64,
        max: usize,
    ) -> Result<Vec<ClaimedTask>, AedbError> {
        self.queue_claim_inner(
            Some(caller),
            project_id,
            scope_id,
            queue,
            owner_id,
            lease_micros,
            max,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn queue_claim_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        owner_id: &str,
        lease_micros: u64,
        max: usize,
    ) -> Result<Vec<ClaimedTask>, AedbError> {
        if queue.is_empty() || owner_id.is_empty() {
            return Err(AedbError::Validation(
                "queue and owner_id cannot be empty".into(),
            ));
        }
        if max == 0 {
            return Ok(Vec::new());
        }
        // Move matured scheduled tasks into the priority run-queue, then reclaim
        // any leases that expired, before serving fresh work.
        self.promote_due(caller.clone(), project_id, scope_id, queue, max.max(8))
            .await?;
        self.reclaim_expired(caller.clone(), project_id, scope_id, queue, max.max(8))
            .await?;

        let mut claimed = Vec::with_capacity(max);
        while claimed.len() < max {
            match self
                .try_claim_one(
                    caller.clone(),
                    project_id,
                    scope_id,
                    queue,
                    owner_id,
                    lease_micros,
                )
                .await?
            {
                Some(task) => claimed.push(task),
                None => break,
            }
        }
        Ok(claimed)
    }

    async fn try_claim_one(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        owner_id: &str,
        lease_micros: u64,
    ) -> Result<Option<ClaimedTask>, AedbError> {
        for _ in 0..MAX_CAS_RETRIES {
            let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
            let base_seq = lease.view.seq;
            let view = &lease.view.keyspace;
            let now = crate::system_now_micros();

            let ready = view.try_kv_scan_prefix(project_id, scope_id, &ready_prefix(queue), 1)?;
            let Some((ready_k, ready_entry)) = ready.into_iter().next() else {
                return Ok(None);
            };
            let id = String::from_utf8_lossy(&ready_entry.value).into_owned();
            let pk = primary_key(queue, &id);
            let Some(primary) = view.kv_get(project_id, scope_id, &pk) else {
                // Dangling index entry; clean it up and retry.
                let env = self.envelope(
                    caller.clone(),
                    vec![del_mut(project_id, scope_id, ready_k)],
                    Vec::new(),
                    base_seq,
                );
                let _ = self.commit_envelope(env).await;
                continue;
            };
            let mut task = decode_task(&primary)?;

            task.attempts = task.attempts.saturating_add(1);
            task.state = STATE_INFLIGHT;
            task.owner_id = Some(owner_id.to_string());
            task.fencing_token = task.fencing_token.saturating_add(1);
            task.lease_deadline_micros = now.saturating_add(lease_micros);
            task.updated_micros = now;

            let inflight_k = inflight_key(queue, task.lease_deadline_micros, task.seq, &task.id);
            let mutations = vec![
                set_mut(project_id, scope_id, pk.clone(), encode_task(&task)?),
                del_mut(project_id, scope_id, ready_k),
                set_mut(project_id, scope_id, inflight_k, id.as_bytes().to_vec()),
            ];
            let assertions = vec![key_assertion(project_id, scope_id, &pk, Some(&primary))];
            let env = self.envelope(caller.clone(), mutations, assertions, base_seq);
            match self.commit_envelope(env).await {
                Ok(_) => {
                    return Ok(Some(ClaimedTask {
                        task_id: task.id,
                        queue: task.queue,
                        payload_json: task.payload_json,
                        attempts: task.attempts,
                        fencing_token: task.fencing_token,
                        lease_deadline_micros: task.lease_deadline_micros,
                        priority: task.priority,
                        enqueued_micros: task.created_micros,
                    }));
                }
                Err(err) if is_retryable_conflict(&err) => continue,
                Err(err) => return Err(err),
            }
        }
        // Heavy contention on the head of the queue; let the caller retry.
        Ok(None)
    }

    /// Promote scheduled tasks whose `not_before` has arrived into the
    /// priority-ordered ready index, so claims serve them by priority rather than
    /// by maturation timestamp.
    async fn promote_due(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        cap: usize,
    ) -> Result<(), AedbError> {
        for _ in 0..cap {
            let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
            let base_seq = lease.view.seq;
            let view = &lease.view.keyspace;
            let now = crate::system_now_micros();

            let (lo, hi) = due_bounds(queue, KIND_SCHEDULED, now);
            let due = view.try_kv_scan_range(project_id, scope_id, lo, hi, 1)?;
            let Some((sched_k, sched_entry)) = due.into_iter().next() else {
                return Ok(());
            };
            let id = String::from_utf8_lossy(&sched_entry.value).into_owned();
            let pk = primary_key(queue, &id);
            let Some(primary) = view.kv_get(project_id, scope_id, &pk) else {
                let env = self.envelope(
                    caller.clone(),
                    vec![del_mut(project_id, scope_id, sched_k)],
                    Vec::new(),
                    base_seq,
                );
                let _ = self.commit_envelope(env).await;
                continue;
            };
            let mut task = decode_task(&primary)?;
            if task.state != STATE_READY || task.in_ready {
                // Already promoted or no longer schedulable; drop the stale index.
                let _ = self
                    .commit_envelope(self.envelope(
                        caller.clone(),
                        vec![del_mut(project_id, scope_id, sched_k)],
                        Vec::new(),
                        base_seq,
                    ))
                    .await;
                continue;
            }
            task.in_ready = true;
            let mutations = vec![
                del_mut(project_id, scope_id, sched_k),
                set_mut(
                    project_id,
                    scope_id,
                    ready_key(queue, task.priority, task.seq, &task.id),
                    id.as_bytes().to_vec(),
                ),
                set_mut(project_id, scope_id, pk.clone(), encode_task(&task)?),
            ];
            let assertions = vec![key_assertion(project_id, scope_id, &pk, Some(&primary))];
            let env = self.envelope(caller.clone(), mutations, assertions, base_seq);
            // Conflicts mean another worker promoted it; that's fine.
            let _ = self.commit_envelope(env).await;
        }
        Ok(())
    }

    async fn reclaim_expired(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        cap: usize,
    ) -> Result<(), AedbError> {
        for _ in 0..cap {
            let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
            let base_seq = lease.view.seq;
            let view = &lease.view.keyspace;
            let now = crate::system_now_micros();

            let (lo, hi) = due_bounds(queue, KIND_INFLIGHT, now);
            let expired = view.try_kv_scan_range(project_id, scope_id, lo, hi, 1)?;
            let Some((inflight_k, inflight_entry)) = expired.into_iter().next() else {
                return Ok(());
            };
            let id = String::from_utf8_lossy(&inflight_entry.value).into_owned();
            let pk = primary_key(queue, &id);
            let Some(primary) = view.kv_get(project_id, scope_id, &pk) else {
                let env = self.envelope(
                    caller.clone(),
                    vec![del_mut(project_id, scope_id, inflight_k)],
                    Vec::new(),
                    base_seq,
                );
                let _ = self.commit_envelope(env).await;
                continue;
            };
            let mut task = decode_task(&primary)?;
            // Another worker may have completed/renewed; only act on a still-stale lease.
            if task.state != STATE_INFLIGHT || task.lease_deadline_micros > now {
                let _ = self
                    .commit_envelope(self.envelope(
                        caller.clone(),
                        vec![del_mut(project_id, scope_id, inflight_k.clone())],
                        vec![ReadAssertion::KeyExists {
                            project_id: project_id.to_string(),
                            scope_id: scope_id.to_string(),
                            key: inflight_k,
                            expected: true,
                        }],
                        base_seq,
                    ))
                    .await;
                continue;
            }

            let max_attempts = effective_max_attempts(&task);
            let mut mutations = vec![del_mut(project_id, scope_id, inflight_k)];
            task.owner_id = None;
            task.updated_micros = now;
            if task.attempts >= max_attempts {
                task.state = STATE_DEAD;
                task.last_error = Some("lease expired; attempts exhausted".into());
            } else {
                // A lost lease means the worker vanished, not that the work
                // failed — retry promptly (no failure backoff) by putting it
                // straight back into the priority run-queue.
                task.state = STATE_READY;
                task.in_ready = true;
                task.not_before_micros = now;
                task.last_error = Some("lease expired; requeued".into());
                mutations.push(set_mut(
                    project_id,
                    scope_id,
                    ready_key(queue, task.priority, task.seq, &task.id),
                    task.id.as_bytes().to_vec(),
                ));
            }
            mutations.push(set_mut(
                project_id,
                scope_id,
                pk.clone(),
                encode_task(&task)?,
            ));
            let assertions = vec![key_assertion(project_id, scope_id, &pk, Some(&primary))];
            let env = self.envelope(caller.clone(), mutations, assertions, base_seq);
            // Conflicts mean another worker reclaimed it; that's fine.
            let _ = self.commit_envelope(env).await;
        }
        Ok(())
    }

    /// Acknowledge a successfully processed task. Deletes it, applies the
    /// caller's `extra_mutations`, and enqueues any `follow_ups` — all in one
    /// lease-fenced commit. A stale `fencing_token` yields
    /// [`FencedCommit::LeaseLost`] and applies nothing.
    #[allow(clippy::too_many_arguments)]
    pub async fn queue_complete(
        &self,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        task_id: &str,
        fencing_token: u64,
        result_json: String,
        follow_ups: Vec<EnqueueSpec>,
        extra_mutations: Vec<Mutation>,
    ) -> Result<FencedCommit, AedbError> {
        self.queue_complete_inner(
            None,
            project_id,
            scope_id,
            queue,
            task_id,
            fencing_token,
            result_json,
            follow_ups,
            extra_mutations,
        )
        .await
    }

    /// [`AedbInstance::queue_complete`] on behalf of `caller`.
    #[allow(clippy::too_many_arguments)]
    pub async fn queue_complete_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        task_id: &str,
        fencing_token: u64,
        result_json: String,
        follow_ups: Vec<EnqueueSpec>,
        extra_mutations: Vec<Mutation>,
    ) -> Result<FencedCommit, AedbError> {
        self.queue_complete_inner(
            Some(caller),
            project_id,
            scope_id,
            queue,
            task_id,
            fencing_token,
            result_json,
            follow_ups,
            extra_mutations,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn queue_complete_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        task_id: &str,
        fencing_token: u64,
        result_json: String,
        follow_ups: Vec<EnqueueSpec>,
        extra_mutations: Vec<Mutation>,
    ) -> Result<FencedCommit, AedbError> {
        ensure_valid_json(&result_json, "result_json")?;
        for spec in &follow_ups {
            ensure_valid_json(&spec.options.payload_json, "follow-up payload_json")?;
        }
        let pk = primary_key(queue, task_id);
        for _ in 0..MAX_CAS_RETRIES {
            let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
            let base_seq = lease.view.seq;
            let view = &lease.view.keyspace;
            let Some(primary) = view.kv_get(project_id, scope_id, &pk) else {
                return Ok(FencedCommit::LeaseLost);
            };
            let task = decode_task(&primary)?;
            if task.state != STATE_INFLIGHT || task.fencing_token != fencing_token {
                return Ok(FencedCommit::LeaseLost);
            }
            let now = crate::system_now_micros();

            let mut plan = EnqueuePlan::new();
            plan.mutations
                .push(del_mut(project_id, scope_id, pk.clone()));
            plan.mutations.push(del_mut(
                project_id,
                scope_id,
                inflight_key(queue, task.lease_deadline_micros, task.seq, &task.id),
            ));
            plan.assertions
                .push(key_assertion(project_id, scope_id, &pk, Some(&primary)));
            plan.mutations.extend(extra_mutations.clone());
            for spec in &follow_ups {
                self.plan_enqueue(project_id, scope_id, view, spec, now, &mut plan)?;
            }
            self.finalize_counters(project_id, scope_id, view, &mut plan);

            match self.commit_plan(caller.clone(), plan, base_seq).await {
                Ok(commit) => return Ok(FencedCommit::Applied(commit)),
                Err(err) if is_retryable_conflict(&err) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(AedbError::Conflict(
            "queue_complete exhausted retries".into(),
        ))
    }

    /// Report a failed attempt. If the attempt budget remains, the task is
    /// re-armed with exponential backoff; otherwise it moves to the dead-letter
    /// state. Lease-fenced like [`AedbInstance::queue_complete`].
    #[allow(clippy::too_many_arguments)]
    pub async fn queue_fail(
        &self,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        task_id: &str,
        fencing_token: u64,
        error: String,
    ) -> Result<FencedCommit, AedbError> {
        self.queue_fail_inner(
            None,
            project_id,
            scope_id,
            queue,
            task_id,
            fencing_token,
            error,
        )
        .await
    }

    /// [`AedbInstance::queue_fail`] on behalf of `caller`.
    #[allow(clippy::too_many_arguments)]
    pub async fn queue_fail_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        task_id: &str,
        fencing_token: u64,
        error: String,
    ) -> Result<FencedCommit, AedbError> {
        self.queue_fail_inner(
            Some(caller),
            project_id,
            scope_id,
            queue,
            task_id,
            fencing_token,
            error,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn queue_fail_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        task_id: &str,
        fencing_token: u64,
        error: String,
    ) -> Result<FencedCommit, AedbError> {
        let pk = primary_key(queue, task_id);
        for _ in 0..MAX_CAS_RETRIES {
            let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
            let base_seq = lease.view.seq;
            let view = &lease.view.keyspace;
            let Some(primary) = view.kv_get(project_id, scope_id, &pk) else {
                return Ok(FencedCommit::LeaseLost);
            };
            let mut task = decode_task(&primary)?;
            if task.state != STATE_INFLIGHT || task.fencing_token != fencing_token {
                return Ok(FencedCommit::LeaseLost);
            }
            let now = crate::system_now_micros();
            let max_attempts = effective_max_attempts(&task);
            let mut mutations = vec![del_mut(
                project_id,
                scope_id,
                inflight_key(queue, task.lease_deadline_micros, task.seq, &task.id),
            )];
            task.owner_id = None;
            task.last_error = Some(error.clone());
            task.updated_micros = now;
            if task.attempts >= max_attempts {
                task.state = STATE_DEAD;
            } else {
                task.state = STATE_READY;
                task.in_ready = false;
                task.not_before_micros = next_backoff(&task, now);
                mutations.push(set_mut(
                    project_id,
                    scope_id,
                    scheduled_key(queue, task.not_before_micros, task.seq, &task.id),
                    task.id.as_bytes().to_vec(),
                ));
            }
            mutations.push(set_mut(
                project_id,
                scope_id,
                pk.clone(),
                encode_task(&task)?,
            ));
            let assertions = vec![key_assertion(project_id, scope_id, &pk, Some(&primary))];
            let env = self.envelope(caller.clone(), mutations, assertions, base_seq);
            match self.commit_envelope(env).await {
                Ok(commit) => return Ok(FencedCommit::Applied(commit)),
                Err(err) if is_retryable_conflict(&err) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(AedbError::Conflict("queue_fail exhausted retries".into()))
    }

    /// Extend the lease on an in-flight task and optionally record
    /// `progress_json`. Returns [`FencedCommit::LeaseLost`] if the caller has been
    /// fenced. This is the workflow-checkpoint primitive: progress is durable, so
    /// a resumed worker can continue from the last recorded step.
    #[allow(clippy::too_many_arguments)]
    pub async fn queue_heartbeat(
        &self,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        task_id: &str,
        fencing_token: u64,
        extend_micros: u64,
        progress_json: Option<String>,
    ) -> Result<FencedCommit, AedbError> {
        self.queue_heartbeat_inner(
            None,
            project_id,
            scope_id,
            queue,
            task_id,
            fencing_token,
            extend_micros,
            progress_json,
        )
        .await
    }

    /// [`AedbInstance::queue_heartbeat`] on behalf of `caller`.
    #[allow(clippy::too_many_arguments)]
    pub async fn queue_heartbeat_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        task_id: &str,
        fencing_token: u64,
        extend_micros: u64,
        progress_json: Option<String>,
    ) -> Result<FencedCommit, AedbError> {
        self.queue_heartbeat_inner(
            Some(caller),
            project_id,
            scope_id,
            queue,
            task_id,
            fencing_token,
            extend_micros,
            progress_json,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn queue_heartbeat_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        task_id: &str,
        fencing_token: u64,
        extend_micros: u64,
        progress_json: Option<String>,
    ) -> Result<FencedCommit, AedbError> {
        if let Some(p) = &progress_json {
            ensure_valid_json(p, "progress_json")?;
        }
        let pk = primary_key(queue, task_id);
        for _ in 0..MAX_CAS_RETRIES {
            let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
            let base_seq = lease.view.seq;
            let view = &lease.view.keyspace;
            let Some(primary) = view.kv_get(project_id, scope_id, &pk) else {
                return Ok(FencedCommit::LeaseLost);
            };
            let mut task = decode_task(&primary)?;
            if task.state != STATE_INFLIGHT || task.fencing_token != fencing_token {
                return Ok(FencedCommit::LeaseLost);
            }
            let now = crate::system_now_micros();
            let old_inflight = inflight_key(queue, task.lease_deadline_micros, task.seq, &task.id);
            task.lease_deadline_micros = now.saturating_add(extend_micros);
            if let Some(p) = &progress_json {
                task.progress_json = p.clone();
            }
            task.updated_micros = now;
            let new_inflight = inflight_key(queue, task.lease_deadline_micros, task.seq, &task.id);
            let mut mutations = Vec::with_capacity(3);
            if new_inflight != old_inflight {
                mutations.push(del_mut(project_id, scope_id, old_inflight));
                mutations.push(set_mut(
                    project_id,
                    scope_id,
                    new_inflight,
                    task.id.as_bytes().to_vec(),
                ));
            }
            mutations.push(set_mut(
                project_id,
                scope_id,
                pk.clone(),
                encode_task(&task)?,
            ));
            let assertions = vec![key_assertion(project_id, scope_id, &pk, Some(&primary))];
            let env = self.envelope(caller.clone(), mutations, assertions, base_seq);
            match self.commit_envelope(env).await {
                Ok(commit) => return Ok(FencedCommit::Applied(commit)),
                Err(err) if is_retryable_conflict(&err) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(AedbError::Conflict(
            "queue_heartbeat exhausted retries".into(),
        ))
    }

    /// Cancel a task by id, removing it and its index entry regardless of state.
    /// Returns `false` if the task no longer exists.
    pub async fn queue_cancel(
        &self,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        task_id: &str,
    ) -> Result<bool, AedbError> {
        self.queue_cancel_inner(None, project_id, scope_id, queue, task_id)
            .await
    }

    /// [`AedbInstance::queue_cancel`] on behalf of `caller`.
    pub async fn queue_cancel_as(
        &self,
        caller: CallerContext,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        task_id: &str,
    ) -> Result<bool, AedbError> {
        self.queue_cancel_inner(Some(caller), project_id, scope_id, queue, task_id)
            .await
    }

    async fn queue_cancel_inner(
        &self,
        caller: Option<CallerContext>,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        task_id: &str,
    ) -> Result<bool, AedbError> {
        let pk = primary_key(queue, task_id);
        for _ in 0..MAX_CAS_RETRIES {
            let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
            let base_seq = lease.view.seq;
            let view = &lease.view.keyspace;
            let Some(primary) = view.kv_get(project_id, scope_id, &pk) else {
                return Ok(false);
            };
            let task = decode_task(&primary)?;
            let index_k = match task.state {
                STATE_READY => Some(ready_state_index_key(queue, &task)),
                STATE_INFLIGHT => Some(inflight_key(
                    queue,
                    task.lease_deadline_micros,
                    task.seq,
                    &task.id,
                )),
                _ => None,
            };
            let mut mutations = vec![del_mut(project_id, scope_id, pk.clone())];
            if let Some(k) = index_k {
                mutations.push(del_mut(project_id, scope_id, k));
            }
            let assertions = vec![key_assertion(project_id, scope_id, &pk, Some(&primary))];
            let env = self.envelope(caller.clone(), mutations, assertions, base_seq);
            match self.commit_envelope(env).await {
                Ok(_) => return Ok(true),
                Err(err) if is_retryable_conflict(&err) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(AedbError::Conflict("queue_cancel exhausted retries".into()))
    }

    /// Inspect a single task by id. `None` if it does not exist.
    pub async fn queue_get(
        &self,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        task_id: &str,
        consistency: ConsistencyMode,
    ) -> Result<Option<TaskRecord>, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        let lease = self.acquire_snapshot(consistency).await?;
        let Some(entry) =
            lease
                .view
                .keyspace
                .kv_get(project_id, scope_id, &primary_key(queue, task_id))
        else {
            return Ok(None);
        };
        Ok(Some(task_to_record(decode_task(&entry)?)))
    }

    /// List tasks in a queue, optionally filtered by `state`, for operational
    /// inspection (including the dead-letter queue via `TaskState::Dead`).
    pub async fn queue_list(
        &self,
        project_id: &str,
        scope_id: &str,
        queue: &str,
        state: Option<TaskState>,
        limit: usize,
        consistency: ConsistencyMode,
    ) -> Result<Vec<TaskRecord>, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        let lease = self.acquire_snapshot(consistency).await?;
        let view = &lease.view.keyspace;
        let prefix = primary_prefix(queue);
        let scan_limit = if state.is_some() {
            view.try_kv_scan_prefix(project_id, scope_id, &prefix, self._config.max_scan_rows)?
        } else {
            view.try_kv_scan_prefix(project_id, scope_id, &prefix, limit)?
        };
        let want = state.map(state_to_u8);
        let mut out = Vec::new();
        for (_, entry) in scan_limit {
            let task = decode_task(&entry)?;
            if want.is_none_or(|w| w == task.state) {
                out.push(task_to_record(task));
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    // ---- internal planning helpers -----------------------------------------

    /// Plan one enqueue into `plan`: allocate a sequence number from the target
    /// queue's counter cursor and append the primary, ready-index, and (optional)
    /// idempotency-index mutations.
    fn plan_enqueue(
        &self,
        project_id: &str,
        scope_id: &str,
        view: &crate::storage::keyspace::KeyspaceSnapshot,
        spec: &EnqueueSpec,
        now: u64,
        plan: &mut EnqueuePlan,
    ) -> Result<EnqueueOutcome, AedbError> {
        let queue = &spec.queue;
        // Load (once per commit) the counter cursor for this queue.
        if !plan.counters.contains_key(queue) {
            let entry = view.kv_get(project_id, scope_id, &counter_key(queue));
            let next_seq = entry
                .as_ref()
                .and_then(|e| e.value.get(..8))
                .map(|b| u64::from_be_bytes(b.try_into().unwrap()))
                .unwrap_or(0);
            plan.counters.insert(
                queue.clone(),
                CounterCursor {
                    entry_version: entry.as_ref().map(|e| e.version),
                    next_seq,
                },
            );
        }
        let cursor = plan.counters.get_mut(queue).expect("counter present");
        let seq = cursor.next_seq;
        cursor.next_seq = cursor.next_seq.saturating_add(1);

        let id = format!("{seq:020}");
        let not_before = now.saturating_add(spec.options.delay_micros);
        let in_ready = not_before <= now; // no delay -> immediately runnable
        let task = StoredTask {
            id: id.clone(),
            queue: queue.clone(),
            state: STATE_READY,
            not_before_micros: not_before,
            priority: spec.options.priority,
            seq,
            in_ready,
            attempts: 0,
            max_attempts: spec.options.max_attempts,
            backoff_base_micros: spec.options.backoff_base_micros,
            backoff_max_micros: spec.options.backoff_max_micros,
            owner_id: None,
            fencing_token: 0,
            lease_deadline_micros: 0,
            payload_json: spec.options.payload_json.clone(),
            progress_json: String::new(),
            result_json: String::new(),
            last_error: None,
            created_micros: now,
            updated_micros: now,
        };
        plan.mutations.push(set_mut(
            project_id,
            scope_id,
            primary_key(queue, &id),
            encode_task(&task)?,
        ));
        plan.mutations.push(set_mut(
            project_id,
            scope_id,
            ready_state_index_key(queue, &task),
            id.as_bytes().to_vec(),
        ));
        if let Some(idem) = &spec.options.idempotency_key {
            let ik = idem_key(queue, idem);
            plan.assertions.push(ReadAssertion::KeyExists {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                key: ik.clone(),
                expected: false,
            });
            plan.mutations
                .push(set_mut(project_id, scope_id, ik, id.as_bytes().to_vec()));
        }
        Ok(EnqueueOutcome {
            task_id: id,
            seq,
            deduplicated: false,
        })
    }

    /// Emit the counter writes (and their CAS assertions) for every queue the
    /// plan allocated sequence numbers from.
    fn finalize_counters(
        &self,
        project_id: &str,
        scope_id: &str,
        _view: &crate::storage::keyspace::KeyspaceSnapshot,
        plan: &mut EnqueuePlan,
    ) {
        for (queue, cursor) in &plan.counters {
            let ck = counter_key(queue);
            plan.assertions.push(match cursor.entry_version {
                Some(version) => ReadAssertion::KeyVersion {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    key: ck.clone(),
                    expected_seq: version,
                },
                None => ReadAssertion::KeyExists {
                    project_id: project_id.to_string(),
                    scope_id: scope_id.to_string(),
                    key: ck.clone(),
                    expected: false,
                },
            });
            plan.mutations.push(set_mut(
                project_id,
                scope_id,
                ck,
                cursor.next_seq.to_be_bytes().to_vec(),
            ));
        }
    }

    fn envelope(
        &self,
        caller: Option<CallerContext>,
        mutations: Vec<Mutation>,
        assertions: Vec<ReadAssertion>,
        base_seq: u64,
    ) -> TransactionEnvelope {
        TransactionEnvelope {
            caller,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions,
            read_set: ReadSet::default(),
            write_intent: WriteIntent { mutations },
            base_seq,
        }
    }

    async fn commit_plan(
        &self,
        caller: Option<CallerContext>,
        plan: EnqueuePlan,
        base_seq: u64,
    ) -> Result<crate::CommitResult, AedbError> {
        let env = self.envelope(caller, plan.mutations, plan.assertions, base_seq);
        self.commit_envelope(env).await
    }
}

fn effective_max_attempts(task: &StoredTask) -> u32 {
    if task.max_attempts == 0 {
        DEFAULT_MAX_ATTEMPTS
    } else {
        task.max_attempts
    }
}

fn state_to_u8(state: TaskState) -> u8 {
    match state {
        TaskState::Ready => STATE_READY,
        TaskState::InFlight => STATE_INFLIGHT,
        TaskState::Dead => STATE_DEAD,
    }
}

fn u8_to_state(state: u8) -> TaskState {
    match state {
        STATE_INFLIGHT => TaskState::InFlight,
        STATE_DEAD => TaskState::Dead,
        _ => TaskState::Ready,
    }
}

fn task_to_record(task: StoredTask) -> TaskRecord {
    TaskRecord {
        task_id: task.id,
        queue: task.queue,
        state: u8_to_state(task.state),
        not_before_micros: task.not_before_micros,
        priority: task.priority,
        attempts: task.attempts,
        max_attempts: effective_max_attempts_val(task.max_attempts),
        owner_id: task.owner_id,
        fencing_token: task.fencing_token,
        lease_deadline_micros: task.lease_deadline_micros,
        payload_json: task.payload_json,
        progress_json: task.progress_json,
        result_json: task.result_json,
        last_error: task.last_error,
        created_micros: task.created_micros,
        updated_micros: task.updated_micros,
    }
}

fn effective_max_attempts_val(max_attempts: u32) -> u32 {
    if max_attempts == 0 {
        DEFAULT_MAX_ATTEMPTS
    } else {
        max_attempts
    }
}

#[cfg(test)]
mod tests;
