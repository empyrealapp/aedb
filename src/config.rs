use std::sync::Arc;
use zeroize::Zeroizing;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    Full,
    Batch,
    OsBuffered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMode {
    Strict,
    Permissive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum PrimaryIndexBackend {
    #[default]
    OrdMap,
    ArtExperimental,
}

/// Runtime configuration for an AEDB instance.
#[derive(Debug, Clone)]
pub struct AedbConfig {
    pub max_segment_bytes: u64,
    pub max_segment_age_secs: u64,
    pub durability_mode: DurabilityMode,
    pub batch_interval_ms: u64,
    pub batch_max_bytes: usize,
    pub idempotency_window_seconds: u64,
    pub max_inflight_commits: usize,
    pub max_commit_queue_bytes: usize,
    pub max_transaction_bytes: usize,
    pub commit_timeout_ms: u64,
    pub max_snapshot_age_ms: u64,
    pub max_concurrent_snapshots: usize,
    pub max_scan_rows: usize,
    pub max_kv_key_bytes: usize,
    pub max_kv_value_bytes: usize,
    pub max_memory_estimate_bytes: usize,
    pub prestage_shards: usize,
    pub epoch_max_wait_us: u64,
    pub epoch_min_commits: usize,
    pub epoch_max_commits: usize,
    pub adaptive_epoch_enabled: bool,
    pub adaptive_epoch_min_commits_floor: usize,
    pub adaptive_epoch_min_commits_ceiling: usize,
    pub adaptive_epoch_wait_us_floor: u64,
    pub adaptive_epoch_wait_us_ceiling: u64,
    pub adaptive_epoch_target_latency_us: u64,
    pub parallel_apply_enabled: bool,
    pub parallel_worker_threads: usize,
    pub coordinator_locking_enabled: bool,
    pub global_unique_index_enabled: bool,
    pub partition_lock_timeout_ms: u64,
    pub epoch_apply_timeout_ms: u64,
    pub max_versions: usize,
    pub min_version_age_ms: u64,
    pub version_gc_interval_ms: u64,
    /// Encryption key for checkpoint files. Wrapped in Arc<Zeroizing<>> to ensure
    /// the key is securely zeroed from memory when the last reference is dropped,
    /// preventing key disclosure through core dumps or memory scanning.
    pub checkpoint_encryption_key: Option<Arc<Zeroizing<[u8; 32]>>>,
    pub checkpoint_key_id: Option<String>,
    /// HMAC key for manifest integrity. Wrapped in Arc<Zeroizing<>> to ensure
    /// the key is securely zeroed from memory when the last reference is dropped.
    pub manifest_hmac_key: Option<Arc<Zeroizing<Vec<u8>>>>,
    pub recovery_mode: RecoveryMode,
    pub hash_chain_required: bool,
    pub primary_index_backend: PrimaryIndexBackend,
}

impl Default for AedbConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: 64 * 1024 * 1024,
            max_segment_age_secs: 60,
            durability_mode: DurabilityMode::Full,
            batch_interval_ms: 10,
            batch_max_bytes: 1024 * 1024,
            idempotency_window_seconds: 300,
            max_inflight_commits: 64,
            max_commit_queue_bytes: 64 * 1024 * 1024,
            max_transaction_bytes: 16 * 1024 * 1024,
            commit_timeout_ms: 5000,
            max_snapshot_age_ms: 30_000,
            max_concurrent_snapshots: 128,
            max_scan_rows: 10_000,
            max_kv_key_bytes: 1024,
            max_kv_value_bytes: 1024 * 1024,
            max_memory_estimate_bytes: 2 * 1024 * 1024 * 1024,
            prestage_shards: 8,
            epoch_max_wait_us: 100,
            epoch_min_commits: 1,
            epoch_max_commits: 64,
            adaptive_epoch_enabled: true,
            adaptive_epoch_min_commits_floor: 1,
            adaptive_epoch_min_commits_ceiling: 128,
            adaptive_epoch_wait_us_floor: 25,
            adaptive_epoch_wait_us_ceiling: 1_000,
            adaptive_epoch_target_latency_us: 1_500,
            parallel_apply_enabled: true,
            parallel_worker_threads: std::thread::available_parallelism()
                .map(|n| n.get().max(2))
                .unwrap_or(4),
            coordinator_locking_enabled: true,
            global_unique_index_enabled: true,
            partition_lock_timeout_ms: 5_000,
            epoch_apply_timeout_ms: 10_000,
            max_versions: 1024,
            min_version_age_ms: 5_000,
            version_gc_interval_ms: 1_000,
            checkpoint_encryption_key: None,
            checkpoint_key_id: None,
            manifest_hmac_key: None,
            recovery_mode: RecoveryMode::Strict,
            hash_chain_required: true,
            primary_index_backend: PrimaryIndexBackend::OrdMap,
        }
    }
}

impl AedbConfig {
    pub fn production(hmac_key: [u8; 32]) -> Self {
        Self {
            manifest_hmac_key: Some(Arc::new(Zeroizing::new(hmac_key.to_vec()))),
            recovery_mode: RecoveryMode::Strict,
            durability_mode: DurabilityMode::Full,
            hash_chain_required: true,
            ..Self::default()
        }
    }

    pub fn development() -> Self {
        Self {
            manifest_hmac_key: None,
            recovery_mode: RecoveryMode::Permissive,
            durability_mode: DurabilityMode::OsBuffered,
            hash_chain_required: false,
            ..Self::default()
        }
    }

    /// Low-latency profile tuned for user-facing commit acknowledgements.
    ///
    /// This profile intentionally uses batched durability so commits can
    /// finalize at visible-head latency while WAL fsync is coalesced in short
    /// intervals. It keeps strict recovery + hash-chain + manifest HMAC.
    ///
    /// Use `AedbInstance::commit_with_finality(..., CommitFinality::Durable)`
    /// for flows that must wait for durability.
    pub fn low_latency(hmac_key: [u8; 32]) -> Self {
        Self {
            manifest_hmac_key: Some(Arc::new(Zeroizing::new(hmac_key.to_vec()))),
            recovery_mode: RecoveryMode::Strict,
            durability_mode: DurabilityMode::Batch,
            hash_chain_required: true,
            batch_interval_ms: 2,
            batch_max_bytes: 512 * 1024,
            epoch_max_wait_us: 50,
            adaptive_epoch_target_latency_us: 1_000,
            ..Self::default()
        }
    }

    pub fn strict_recovery(&self) -> bool {
        matches!(self.recovery_mode, RecoveryMode::Strict)
    }

    /// Returns a reference to the checkpoint encryption key, dereferencing through
    /// the Arc<Zeroizing<>> wrapper for use with checkpoint functions.
    pub fn checkpoint_key(&self) -> Option<&[u8; 32]> {
        self.checkpoint_encryption_key.as_ref().map(|arc| &***arc)
    }

    /// Returns a reference to the manifest HMAC key, dereferencing through
    /// the Arc<Zeroizing<>> wrapper for use with manifest functions.
    pub fn hmac_key(&self) -> Option<&[u8]> {
        self.manifest_hmac_key.as_ref().map(|arc| &***arc as &[u8])
    }

    /// Sets the checkpoint encryption key, wrapping it in Arc<Zeroizing<>> for
    /// secure memory handling.
    pub fn with_checkpoint_key(mut self, key: [u8; 32]) -> Self {
        self.checkpoint_encryption_key = Some(Arc::new(Zeroizing::new(key)));
        self
    }

    /// Sets the manifest HMAC key, wrapping it in Arc<Zeroizing<>> for
    /// secure memory handling.
    pub fn with_hmac_key(mut self, key: Vec<u8>) -> Self {
        self.manifest_hmac_key = Some(Arc::new(Zeroizing::new(key)));
        self
    }
}
