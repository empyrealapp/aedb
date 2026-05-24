use crate::backup_chain::read_segments;
use crate::config::{AedbConfig, StorageMode};
use crate::error::AedbError;
use crate::snapshot::gc::SnapshotManager;
use crate::storage::keyspace::Keyspace;
use crate::storage::kv_segment::KvSegmentStore;
use crate::storage::value_store::PersistentValueStore;
use parking_lot::Mutex;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

/// Creates a directory with restrictive permissions (0o700 on Unix) to prevent
/// unauthorized access to database files on multi-user systems.
pub(crate) fn create_private_dir_all(path: &Path) -> Result<(), AedbError> {
    #[cfg(unix)]
    {
        use std::fs::DirBuilder;
        use std::os::unix::fs::DirBuilderExt;
        use std::os::unix::fs::PermissionsExt;

        DirBuilder::new()
            .recursive(true)
            .mode(0o700) // Owner read/write/execute only
            .create(path)?;
        let metadata = fs::metadata(path)?;
        if !metadata.is_dir() {
            return Err(AedbError::Validation(format!(
                "path is not a directory: {}",
                path.display()
            )));
        }
        let mut perms = metadata.permissions();
        perms.set_mode(0o700);
        fs::set_permissions(path, perms)?;
    }
    #[cfg(not(unix))]
    {
        fs::create_dir_all(path)?;
    }
    Ok(())
}

pub(crate) fn derive_cursor_signing_key(config: &AedbConfig, dir: &Path) -> [u8; 32] {
    let mut material = Vec::new();
    if let Some(key) = config.hmac_key() {
        material.extend_from_slice(key);
        material.extend_from_slice(dir.to_string_lossy().as_bytes());
    } else {
        material.extend_from_slice(Uuid::new_v4().as_bytes());
        material.extend_from_slice(dir.to_string_lossy().as_bytes());
    }
    blake3::derive_key("aedb-query-cursor-v1", &material)
}

pub(crate) fn attach_configured_value_store(
    keyspace: &mut Keyspace,
    config: &AedbConfig,
    dir: &Path,
) -> Result<(), AedbError> {
    if matches!(config.storage_mode, StorageMode::DiskBacked) {
        let store = Arc::new(PersistentValueStore::open_with_hot_cache_bytes(
            dir,
            config.persistent_value_hot_cache_bytes,
        )?);
        let segment_store = Arc::new(KvSegmentStore::open_with_block_cache_bytes(
            dir,
            config.kv_segment_block_cache_bytes,
        )?);
        keyspace
            .attach_persistent_value_store(store, config.persistent_value_inline_threshold_bytes)?;
        keyspace.attach_kv_segment_store(segment_store);
        keyspace.spill_kv_values_to_memory_target(config.max_memory_estimate_bytes)?;
        keyspace.flush_kv_to_segments_to_memory_target(config.max_memory_estimate_bytes)?;
    } else {
        keyspace.detach_persistent_value_store();
    }
    Ok(())
}

pub(crate) fn reclaim_eligible_wal_segments(
    dir: &Path,
    snapshot_manager: &Arc<Mutex<SnapshotManager>>,
    manifest_hmac_key: Option<&[u8]>,
) -> Result<usize, AedbError> {
    let manifest = match crate::manifest::atomic::load_manifest_signed(dir, manifest_hmac_key) {
        Ok(manifest) => manifest,
        Err(_) => return Ok(0),
    };
    let Some(checkpointed_through_seq) = manifest.checkpoints.last().map(|cp| cp.seq) else {
        return Ok(0);
    };
    let segments = read_segments(dir)?;
    let segment_seqs: Vec<u64> = segments.iter().map(|segment| segment.segment_seq).collect();
    let eligible = {
        let mgr = snapshot_manager.lock();
        mgr.eligible_segment_reclaims(
            &segment_seqs,
            checkpointed_through_seq,
            manifest.active_segment_seq,
        )
    };
    if eligible.is_empty() {
        return Ok(0);
    }

    let mut reclaimed = 0usize;
    for seq in eligible {
        let path = dir.join(format!("segment_{seq:016}.aedbwal"));
        if path.exists() {
            fs::remove_file(&path)?;
            reclaimed = reclaimed.saturating_add(1);
        }
    }
    Ok(reclaimed)
}
