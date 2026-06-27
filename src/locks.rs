//! Transaction-scoped, in-memory exclusive locks for logical objects.
//!
//! These primitives let a higher-level runtime (e.g. Arcana) serialize
//! *modifications* to a single logical object — a program instance, a
//! document, a workflow — while leaving AEDB's MVCC snapshot reads and all
//! unrelated writes fully concurrent.
//!
//! ## What this is (and is not)
//!
//! AEDB does not schedule program execution, maintain mailboxes, run queues,
//! or decide which transaction proceeds next. It provides only the locking
//! *mechanism*. The runtime composes it with the normal commit/query APIs to
//! build whatever execution model it needs.
//!
//! ## Guarantees
//!
//! * **Exclusive while held.** At most one [`TxLockSet`] holds a given
//!   [`LockKey`] at a time. A second acquirer blocks (with a deadline) or, for
//!   [`TxLockSet::try_lock`], reports the contention without blocking.
//! * **Transaction-scoped.** A [`TxLockSet`] represents the lock scope of one
//!   logical transaction. Every key it holds is released when it is committed,
//!   rolled back, or simply dropped — there is no way to leak a lock past the
//!   scope's lifetime.
//! * **Snapshot reads are never blocked.** Locks live in a side table that the
//!   read and commit paths do not consult. MVCC readers always observe a
//!   consistent committed snapshot regardless of who holds which lock.
//! * **Deadlock-safe.** Acquire several keys at once with
//!   [`TxLockSet::lock_all`], which sorts them into a deterministic global
//!   order so two transactions locking overlapping sets can never form a wait
//!   cycle. Single blocking acquisitions additionally honour a finite deadline,
//!   so a mis-ordered pair degrades to a timeout error rather than a hang.
//! * **Nothing survives restart.** Locks are pure in-memory state. They are
//!   never written to the WAL, never checkpointed, and are discarded the moment
//!   the process exits. After recovery the lock table starts empty and the
//!   runtime is responsible for re-acquiring whatever it needs as execution
//!   resumes from the recovered committed state.

use crate::error::AedbError;
use parking_lot::{Condvar, Mutex};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default deadline applied by [`TxLockSet::lock`] / [`TxLockSet::lock_all`]
/// when the caller does not specify one. A finite default keeps a mis-ordered
/// acquisition from blocking forever; the runtime can always pass an explicit
/// deadline via the `*_with_timeout` variants.
pub const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(30);

/// Identity of a logical object that can be locked.
///
/// The `(project_id, scope_id)` pair mirrors AEDB's namespacing so program,
/// document, or workflow locks line up with the data they guard, but the
/// `object` bytes are entirely opaque — any logical identity works. Use
/// [`LockKey::global`] (or the `From<&str>` / `From<String>` impls) when you do
/// not need scope qualification.
///
/// The total order derived here (`(project_id, scope_id, object)`) is the
/// canonical ordering used to make multi-key acquisition deadlock-free.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LockKey {
    pub project_id: String,
    pub scope_id: String,
    pub object: Vec<u8>,
}

impl LockKey {
    /// A scope-qualified lock key.
    pub fn new(
        project_id: impl Into<String>,
        scope_id: impl Into<String>,
        object: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            scope_id: scope_id.into(),
            object: object.into(),
        }
    }

    /// A lock key with no project/scope qualification — useful for purely
    /// logical objects ("program:42") that are not tied to an AEDB scope.
    pub fn global(object: impl Into<Vec<u8>>) -> Self {
        Self {
            project_id: String::new(),
            scope_id: String::new(),
            object: object.into(),
        }
    }
}

impl From<&str> for LockKey {
    fn from(s: &str) -> Self {
        LockKey::global(s.as_bytes().to_vec())
    }
}

impl From<String> for LockKey {
    fn from(s: String) -> Self {
        LockKey::global(s.into_bytes())
    }
}

#[derive(Debug, Clone, Copy)]
struct HeldLock {
    owner: u64,
    fencing_token: u64,
}

#[derive(Default)]
struct Inner {
    held: HashMap<LockKey, HeldLock>,
    next_owner: u64,
    next_fencing_token: u64,
}

/// In-memory registry of currently-held object locks.
///
/// One [`LockManager`] is owned by each `AedbInstance`. It holds no durable
/// state: dropping it (or restarting the process) releases everything.
pub struct LockManager {
    inner: Mutex<Inner>,
    /// Signalled whenever a lock is released, waking blocked acquirers.
    released: Condvar,
    default_timeout: Duration,
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new(DEFAULT_LOCK_TIMEOUT)
    }
}

impl LockManager {
    pub fn new(default_timeout: Duration) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            released: Condvar::new(),
            default_timeout,
        }
    }

    /// Begin a new transaction-scoped lock set. Each call yields a distinct
    /// owner identity, so two scopes never alias even for the same caller.
    pub fn begin(self: &Arc<Self>) -> TxLockSet {
        let owner = {
            let mut inner = self.inner.lock();
            inner.next_owner += 1;
            inner.next_owner
        };
        TxLockSet {
            manager: Arc::clone(self),
            owner,
            held: BTreeSet::new(),
            released: false,
        }
    }

    /// Number of locks currently held across all scopes. Primarily for tests
    /// and operational inspection.
    pub fn held_count(&self) -> usize {
        self.inner.lock().held.len()
    }

    /// Whether `key` is currently locked by any scope.
    pub fn is_locked(&self, key: &LockKey) -> bool {
        self.inner.lock().held.contains_key(key)
    }

    /// Acquire `key` for `owner`. Returns the fencing token granted for this
    /// hold. Re-acquiring a key the same owner already holds is idempotent and
    /// returns the original fencing token.
    ///
    /// `deadline` of `None` means non-blocking (`try`) semantics: an error is
    /// returned immediately if another owner holds the key. Otherwise the call
    /// blocks until the key is free or the deadline elapses.
    fn acquire(
        &self,
        owner: u64,
        key: &LockKey,
        deadline: Option<Instant>,
    ) -> Result<u64, LockError> {
        let mut inner = self.inner.lock();
        loop {
            match inner.held.get(key) {
                Some(existing) if existing.owner == owner => {
                    return Ok(existing.fencing_token);
                }
                Some(_) => match deadline {
                    None => return Err(LockError::WouldBlock),
                    Some(deadline) => {
                        let now = Instant::now();
                        if now >= deadline {
                            return Err(LockError::Timeout);
                        }
                        // parking_lot wakes spuriously; the loop re-checks state.
                        if self
                            .released
                            .wait_for(&mut inner, deadline - now)
                            .timed_out()
                            && Instant::now() >= deadline
                        {
                            return Err(LockError::Timeout);
                        }
                    }
                },
                None => {
                    inner.next_fencing_token += 1;
                    let fencing_token = inner.next_fencing_token;
                    inner.held.insert(
                        key.clone(),
                        HeldLock {
                            owner,
                            fencing_token,
                        },
                    );
                    return Ok(fencing_token);
                }
            }
        }
    }

    /// Release a single key if (and only if) `owner` holds it.
    fn release_one(&self, owner: u64, key: &LockKey) {
        let mut inner = self.inner.lock();
        if let Some(existing) = inner.held.get(key)
            && existing.owner == owner
        {
            inner.held.remove(key);
            drop(inner);
            self.released.notify_all();
        }
    }

    /// Release every key in `keys` owned by `owner`, in one batch.
    fn release_many(&self, owner: u64, keys: &BTreeSet<LockKey>) {
        if keys.is_empty() {
            return;
        }
        {
            let mut inner = self.inner.lock();
            for key in keys {
                if let Some(existing) = inner.held.get(key)
                    && existing.owner == owner
                {
                    inner.held.remove(key);
                }
            }
        }
        self.released.notify_all();
    }
}

/// Why a lock acquisition did not succeed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockError {
    /// Non-blocking acquisition found the key held by another owner.
    WouldBlock,
    /// Blocking acquisition gave up after its deadline elapsed.
    Timeout,
}

/// The lock scope of a single logical transaction.
///
/// Created via [`LockManager::begin`] (exposed on the instance as
/// `AedbInstance::lock_scope`). Acquire one or more [`LockKey`]s, perform the
/// guarded work using the normal commit/query APIs, then call
/// [`commit`](Self::commit) or [`rollback`](Self::rollback) — both release all
/// held locks. Dropping the set without calling either also releases them, so a
/// panic or early `?` return can never strand a lock.
pub struct TxLockSet {
    manager: Arc<LockManager>,
    owner: u64,
    held: BTreeSet<LockKey>,
    released: bool,
}

impl TxLockSet {
    /// Acquire `key`, blocking up to the manager's default timeout. Returns the
    /// fencing token for the hold (monotonically increasing across the
    /// instance; useful for fencing out stale external actions).
    ///
    /// Re-locking a key already held by this scope is a no-op that returns the
    /// existing token.
    pub fn lock(&mut self, key: impl Into<LockKey>) -> Result<u64, AedbError> {
        let timeout = self.manager.default_timeout;
        self.lock_with_timeout(key, timeout)
    }

    /// Acquire `key`, blocking up to `timeout`.
    pub fn lock_with_timeout(
        &mut self,
        key: impl Into<LockKey>,
        timeout: Duration,
    ) -> Result<u64, AedbError> {
        let key = key.into();
        let deadline = Instant::now().checked_add(timeout);
        let token = self
            .manager
            .acquire(self.owner, &key, deadline.or(Some(Instant::now())))
            .map_err(|e| e.into_aedb(&key))?;
        self.held.insert(key);
        Ok(token)
    }

    /// Try to acquire `key` without blocking. Returns `Ok(Some(token))` when
    /// acquired, `Ok(None)` when another scope currently holds it.
    pub fn try_lock(&mut self, key: impl Into<LockKey>) -> Result<Option<u64>, AedbError> {
        let key = key.into();
        match self.manager.acquire(self.owner, &key, None) {
            Ok(token) => {
                self.held.insert(key);
                Ok(Some(token))
            }
            Err(LockError::WouldBlock) => Ok(None),
            Err(e) => Err(e.into_aedb(&key)),
        }
    }

    /// Atomically acquire every key in `keys`, blocking up to the default
    /// timeout. Keys are acquired in their canonical sorted order, which makes
    /// concurrent multi-key acquisition deadlock-free.
    ///
    /// All-or-nothing with respect to keys not already held: if any acquisition
    /// fails, every key newly taken by *this call* is released before returning
    /// the error. Keys this scope already held are left untouched.
    pub fn lock_all<K, I>(&mut self, keys: I) -> Result<(), AedbError>
    where
        K: Into<LockKey>,
        I: IntoIterator<Item = K>,
    {
        let timeout = self.manager.default_timeout;
        self.lock_all_with_timeout(keys, timeout)
    }

    /// [`lock_all`](Self::lock_all) with an explicit deadline.
    pub fn lock_all_with_timeout<K, I>(
        &mut self,
        keys: I,
        timeout: Duration,
    ) -> Result<(), AedbError>
    where
        K: Into<LockKey>,
        I: IntoIterator<Item = K>,
    {
        // Deterministic global order eliminates lock-ordering deadlocks.
        let ordered: BTreeSet<LockKey> = keys.into_iter().map(Into::into).collect();
        let deadline = Instant::now().checked_add(timeout).or(Some(Instant::now()));
        let mut newly_acquired: BTreeSet<LockKey> = BTreeSet::new();
        for key in &ordered {
            if self.held.contains(key) {
                continue;
            }
            match self.manager.acquire(self.owner, key, deadline) {
                Ok(_) => {
                    newly_acquired.insert(key.clone());
                }
                Err(e) => {
                    // Roll back this call's partial progress so acquisition is
                    // atomic; pre-existing holds are preserved.
                    self.manager.release_many(self.owner, &newly_acquired);
                    return Err(e.into_aedb(key));
                }
            }
        }
        self.held.extend(newly_acquired);
        Ok(())
    }

    /// Whether this scope currently holds `key`.
    pub fn holds(&self, key: &LockKey) -> bool {
        self.held.contains(key)
    }

    /// The keys currently held by this scope, in canonical order.
    pub fn held_keys(&self) -> impl Iterator<Item = &LockKey> {
        self.held.iter()
    }

    /// Unique owner identity of this scope.
    pub fn owner_id(&self) -> u64 {
        self.owner
    }

    /// Release a single key early, before the scope ends. Other held keys stay
    /// locked. No-op if this scope does not hold `key`.
    pub fn unlock(&mut self, key: &LockKey) {
        if self.held.remove(key) {
            self.manager.release_one(self.owner, key);
        }
    }

    /// Commit the transaction scope: release all held locks. The data writes
    /// themselves go through the normal commit APIs; this only relinquishes the
    /// serialization locks once those writes are durable.
    pub fn commit(mut self) {
        self.release_all();
    }

    /// Roll back the transaction scope: release all held locks. Identical lock
    /// behaviour to [`commit`](Self::commit) — no partial lock state can
    /// remain either way.
    pub fn rollback(mut self) {
        self.release_all();
    }

    fn release_all(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let keys = std::mem::take(&mut self.held);
        self.manager.release_many(self.owner, &keys);
    }
}

impl Drop for TxLockSet {
    fn drop(&mut self) {
        // Guarantees no lock outlives its scope, even on panic / early return.
        self.release_all();
    }
}

impl LockError {
    fn into_aedb(self, key: &LockKey) -> AedbError {
        match self {
            LockError::Timeout => AedbError::Timeout,
            LockError::WouldBlock => AedbError::Conflict(format!(
                "object lock held by another transaction: {}",
                describe_key(key)
            )),
        }
    }
}

fn describe_key(key: &LockKey) -> String {
    let object = String::from_utf8(key.object.clone())
        .unwrap_or_else(|_| format!("0x{}", hex::encode(&key.object)));
    if key.project_id.is_empty() && key.scope_id.is_empty() {
        object
    } else {
        format!("{}/{}/{}", key.project_id, key.scope_id, object)
    }
}

#[cfg(test)]
#[path = "locks_tests.rs"]
mod locks_tests;
