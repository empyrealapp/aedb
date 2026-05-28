use crate::config::{AedbConfig, DurabilityMode, RecoveryMode, StorageMode};
use crate::error::AedbError;

pub(crate) fn validate_config(config: &AedbConfig) -> Result<(), AedbError> {
    if config.max_segment_bytes == 0 {
        return Err(AedbError::InvalidConfig {
            message: "max_segment_bytes must be > 0".into(),
        });
    }
    if config.max_segment_age_secs == 0 {
        return Err(AedbError::InvalidConfig {
            message: "max_segment_age_secs must be > 0".into(),
        });
    }
    if config.max_inflight_commits == 0 {
        return Err(AedbError::InvalidConfig {
            message: "max_inflight_commits must be > 0".into(),
        });
    }
    if config.max_commit_queue_bytes == 0 {
        return Err(AedbError::InvalidConfig {
            message: "max_commit_queue_bytes must be > 0".into(),
        });
    }
    if config.max_transaction_bytes == 0
        || config.max_transaction_bytes > config.max_commit_queue_bytes
    {
        return Err(AedbError::InvalidConfig {
            message: "max_transaction_bytes must be > 0 and <= max_commit_queue_bytes".into(),
        });
    }
    if config.max_transaction_bytes > crate::wal::frame::MAX_FRAME_BODY_BYTES {
        return Err(AedbError::InvalidConfig {
            message: format!(
                "max_transaction_bytes must be <= {} to fit WAL frame bounds",
                crate::wal::frame::MAX_FRAME_BODY_BYTES
            ),
        });
    }
    if config.commit_timeout_ms == 0 {
        return Err(AedbError::InvalidConfig {
            message: "commit_timeout_ms must be > 0".into(),
        });
    }
    if config.max_scan_rows == 0 {
        return Err(AedbError::InvalidConfig {
            message: "max_scan_rows must be > 0".into(),
        });
    }
    if config.max_batch_rows == 0 {
        return Err(AedbError::InvalidConfig {
            message: "max_batch_rows must be > 0".into(),
        });
    }
    if config.max_event_payload_bytes == 0 {
        return Err(AedbError::InvalidConfig {
            message: "max_event_payload_bytes must be > 0".into(),
        });
    }
    if config.max_kv_key_bytes == 0
        || config.max_kv_value_bytes == 0
        || config.max_table_value_bytes == 0
    {
        return Err(AedbError::InvalidConfig {
            message: "max_kv_key_bytes/max_kv_value_bytes/max_table_value_bytes must be > 0".into(),
        });
    }
    if config.prestage_shards == 0 {
        return Err(AedbError::InvalidConfig {
            message: "prestage_shards must be > 0".into(),
        });
    }
    if config.epoch_max_wait_us == 0
        || config.epoch_min_commits == 0
        || config.epoch_max_commits == 0
    {
        return Err(AedbError::InvalidConfig {
            message: "epoch_max_wait_us, epoch_min_commits, and epoch_max_commits must be > 0"
                .into(),
        });
    }
    if config.epoch_min_commits > config.epoch_max_commits {
        return Err(AedbError::InvalidConfig {
            message: "epoch_min_commits must be <= epoch_max_commits".into(),
        });
    }
    if config.adaptive_epoch_enabled {
        if config.adaptive_epoch_min_commits_floor == 0
            || config.adaptive_epoch_min_commits_ceiling == 0
            || config.adaptive_epoch_wait_us_floor == 0
            || config.adaptive_epoch_wait_us_ceiling == 0
            || config.adaptive_epoch_target_latency_us == 0
        {
            return Err(AedbError::InvalidConfig {
                message: "adaptive epoch tuning values must be > 0 when enabled".into(),
            });
        }
        if config.adaptive_epoch_min_commits_floor > config.adaptive_epoch_min_commits_ceiling {
            return Err(AedbError::InvalidConfig {
                message:
                    "adaptive_epoch_min_commits_floor must be <= adaptive_epoch_min_commits_ceiling"
                        .into(),
            });
        }
        if config.adaptive_epoch_wait_us_floor > config.adaptive_epoch_wait_us_ceiling {
            return Err(AedbError::InvalidConfig {
                message: "adaptive_epoch_wait_us_floor must be <= adaptive_epoch_wait_us_ceiling"
                    .into(),
            });
        }
    }
    if config.parallel_apply_enabled && config.parallel_worker_threads == 0 {
        return Err(AedbError::InvalidConfig {
            message: "parallel_worker_threads must be > 0 when parallel_apply_enabled".into(),
        });
    }
    if config.partition_lock_timeout_ms == 0 || config.epoch_apply_timeout_ms == 0 {
        return Err(AedbError::InvalidConfig {
            message: "partition_lock_timeout_ms and epoch_apply_timeout_ms must be > 0".into(),
        });
    }
    if config.max_versions == 0
        || config.version_store_full_snapshot_interval_deltas == 0
        || config.version_gc_interval_ms == 0
    {
        return Err(AedbError::InvalidConfig {
            message: "version store limits must be > 0".into(),
        });
    }
    if config.max_snapshot_age_ms == 0 || config.max_concurrent_snapshots == 0 {
        return Err(AedbError::InvalidConfig {
            message: "snapshot limits must be > 0".into(),
        });
    }
    if config.commit_broadcast_capacity == 0 {
        return Err(AedbError::InvalidConfig {
            message: "commit_broadcast_capacity must be > 0".into(),
        });
    }
    if matches!(config.durability_mode, DurabilityMode::Batch)
        && (config.batch_interval_ms == 0 || config.batch_max_bytes == 0)
    {
        return Err(AedbError::InvalidConfig {
            message: "batch mode requires batch_interval_ms and batch_max_bytes > 0".into(),
        });
    }
    if matches!(config.durability_mode, DurabilityMode::OsBuffered)
        && matches!(config.recovery_mode, RecoveryMode::Strict)
    {
        return Err(AedbError::InvalidConfig {
            message: "OsBuffered mode is not allowed with strict recovery".into(),
        });
    }
    if !config.hash_chain_required && matches!(config.recovery_mode, RecoveryMode::Strict) {
        return Err(AedbError::InvalidConfig {
            message: "strict recovery requires hash_chain_required=true".into(),
        });
    }
    if !config.coordinator_locking_enabled && matches!(config.recovery_mode, RecoveryMode::Strict) {
        return Err(AedbError::InvalidConfig {
            message: "strict recovery requires coordinator_locking_enabled=true".into(),
        });
    }
    if config.checkpoint_encryption_key.is_none() && config.checkpoint_key_id.is_some() {
        return Err(AedbError::InvalidConfig {
            message: "checkpoint_key_id requires checkpoint_encryption_key".into(),
        });
    }
    if !(-7..=22).contains(&config.checkpoint_compression_level) {
        return Err(AedbError::InvalidConfig {
            message: "checkpoint_compression_level must be between -7 and 22".into(),
        });
    }
    if let Some(key) = &config.manifest_hmac_key
        && key.is_empty()
    {
        return Err(AedbError::InvalidConfig {
            message: "manifest_hmac_key must not be empty".into(),
        });
    }
    Ok(())
}

pub(crate) fn validate_secure_config(config: &AedbConfig) -> Result<(), AedbError> {
    validate_config(config)?;
    if config.cursor_signing_key().is_none() {
        tracing::warn!(
            target: "aedb.config",
            "secure config has cursor_signing_key disabled; pagination cursors will not be HMAC-signed at the executor layer"
        );
    }
    let Some(hmac_key) = &config.manifest_hmac_key else {
        return Err(AedbError::InvalidConfig {
            message: "secure mode requires manifest_hmac_key".into(),
        });
    };
    validate_production_config_requirements(
        config,
        hmac_key,
        "secure mode",
        "secure mode requires manifest_hmac_key length >= 32 bytes",
    )
}

pub(crate) fn validate_arcana_config(config: &AedbConfig) -> Result<(), AedbError> {
    validate_config(config)?;
    if config.cursor_signing_key().is_none() {
        tracing::warn!(
            target: "aedb.config",
            "Arcana production profile has cursor_signing_key disabled; pagination cursors will not be HMAC-signed at the executor layer"
        );
    }
    let Some(hmac_key) = &config.manifest_hmac_key else {
        return Err(AedbError::InvalidConfig {
            message: "manifest_hmac_key is required for Arcana production profile".into(),
        });
    };
    validate_production_config_requirements(
        config,
        hmac_key,
        "Arcana production profile",
        "Arcana production profile requires manifest_hmac_key length >= 32 bytes",
    )
}

fn validate_production_config_requirements(
    config: &AedbConfig,
    hmac_key: &[u8],
    profile_name: &str,
    hmac_error: &str,
) -> Result<(), AedbError> {
    if hmac_key.len() < 32 {
        return Err(AedbError::InvalidConfig {
            message: hmac_error.into(),
        });
    }
    if !matches!(config.recovery_mode, RecoveryMode::Strict) {
        return Err(AedbError::InvalidConfig {
            message: format!("{profile_name} requires strict recovery"),
        });
    }
    if !matches!(config.durability_mode, DurabilityMode::Full) {
        return Err(AedbError::InvalidConfig {
            message: format!(
                "{profile_name} requires DurabilityMode::Full for crash-safe durability"
            ),
        });
    }
    if !config.hash_chain_required {
        return Err(AedbError::InvalidConfig {
            message: format!("{profile_name} requires hash_chain_required=true"),
        });
    }
    if !matches!(config.storage_mode, StorageMode::DiskBacked) {
        return Err(AedbError::InvalidConfig {
            message: format!("{profile_name} requires StorageMode::DiskBacked"),
        });
    }
    if config.persistent_value_inline_threshold_bytes != 0 {
        return Err(AedbError::InvalidConfig {
            message: format!(
                "{profile_name} requires persistent_value_inline_threshold_bytes=0 so KV payloads are disk-backed"
            ),
        });
    }
    Ok(())
}
