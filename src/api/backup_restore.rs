use crate::backup::{
    BackupManifest, extract_backup_archive, load_backup_manifest, resolve_backup_path,
    verify_backup_files, write_backup_archive, write_backup_manifest,
};
use crate::backup_chain::{
    copy_file_prefix_sha256_hex, load_verified_backup_chain, read_segments,
    read_segments_for_checkpoint, resolve_target_seq_for_time, scan_segment_seq_range,
    segment_seq_from_name, validate_backup_chain_compatibility, verify_hash_chain_batch,
};
use crate::catalog::namespace_key;
use crate::checkpoint::loader::load_checkpoint_with_key;
use crate::checkpoint::writer::write_checkpoint_with_key;
use crate::config::AedbConfig;
use crate::error::AedbError;
use crate::manifest::atomic::write_manifest_atomic_signed;
use crate::manifest::schema::Manifest;
use crate::open_support::create_private_dir_all;
use crate::query::plan::ConsistencyMode;
use crate::recovery::recover_with_config;
use crate::recovery::replay::{ReplaySegmentsRequest, replay_segments};
use crate::storage::keyspace::{Keyspace, NamespaceId};
use crate::{AedbInstance, RemoteBackupAdapter};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

impl AedbInstance {
    pub async fn backup_full_to_remote(
        &self,
        adapter: &dyn RemoteBackupAdapter,
        uri: &str,
    ) -> Result<BackupManifest, AedbError> {
        let temp = tempfile::tempdir()?;
        let manifest = self.backup_full(temp.path()).await?;
        adapter.store_backup_dir(uri, temp.path())?;
        Ok(manifest)
    }

    pub fn restore_from_remote(
        adapter: &dyn RemoteBackupAdapter,
        uri: &str,
        data_dir: &Path,
        config: &AedbConfig,
    ) -> Result<u64, AedbError> {
        let temp = tempfile::tempdir()?;
        let chain = adapter.materialize_backup_chain(uri, temp.path())?;
        Self::restore_from_backup_chain(&chain, data_dir, config, None)
    }

    pub async fn backup_full(&self, backup_dir: &Path) -> Result<BackupManifest, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        create_private_dir_all(backup_dir)?;
        create_private_dir_all(&backup_dir.join("wal_tail"))?;

        // Hot backup: pin a consistent read view without blocking the write path.
        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let snapshot_seq = lease.view.seq;
        let keyspace = Arc::clone(&lease.view.keyspace);
        let catalog = Arc::clone(&lease.view.catalog);
        let idempotency = self.executor.idempotency_snapshot().await;

        let backup_dir = backup_dir.to_path_buf();
        let source_dir = self.dir.clone();
        let checkpoint_key = self._config.checkpoint_key().copied();
        let checkpoint_key_id = self._config.checkpoint_key_id.clone();
        let compression_level = self._config.checkpoint_compression_level;
        let manifest_hmac_key = self._config.hmac_key().map(|key| key.to_vec());

        tokio::task::spawn_blocking(move || -> Result<BackupManifest, AedbError> {
            let checkpoint = write_checkpoint_with_key(
                keyspace.as_ref(),
                catalog.as_ref(),
                snapshot_seq,
                &backup_dir,
                checkpoint_key.as_ref(),
                checkpoint_key_id,
                idempotency,
                compression_level,
            )?;

            let mut wal_segments = Vec::new();
            let mut file_sha256 = BTreeMap::new();
            file_sha256.insert(checkpoint.filename.clone(), checkpoint.sha256_hex.clone());

            let mut segments = read_segments_for_checkpoint(&source_dir, snapshot_seq)?;
            for segment in segments.clone() {
                let src = source_dir.join(&segment.filename);
                let rel = format!("wal_tail/{}", segment.filename);
                let dst = backup_dir.join(&rel);
                let hash = match copy_file_prefix_sha256_hex(&src, &dst, segment.size_bytes) {
                    Ok(hash) => hash,
                    Err(AedbError::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
                        segments = read_segments_for_checkpoint(&source_dir, snapshot_seq)?;
                        continue;
                    }
                    Err(err) => return Err(err),
                };
                wal_segments.push(segment.filename);
                file_sha256.insert(rel, hash);
            }
            if wal_segments.is_empty() {
                for segment in segments {
                    let src = source_dir.join(&segment.filename);
                    let rel = format!("wal_tail/{}", segment.filename);
                    let dst = backup_dir.join(&rel);
                    let hash = copy_file_prefix_sha256_hex(&src, &dst, segment.size_bytes)?;
                    wal_segments.push(segment.filename);
                    file_sha256.insert(rel, hash);
                }
            }

            wal_segments.sort_by_key(|name| segment_seq_from_name(name).unwrap_or(0));
            let created_at_micros = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64;
            let manifest = BackupManifest {
                backup_id: format!("bk_{}_{}", created_at_micros, snapshot_seq),
                backup_type: "full".into(),
                parent_backup_id: None,
                from_seq: None,
                created_at_micros,
                aedb_version: env!("CARGO_PKG_VERSION").to_string(),
                checkpoint_seq: snapshot_seq,
                wal_head_seq: snapshot_seq,
                checkpoint_file: checkpoint.filename,
                wal_segments,
                file_sha256,
            };
            write_backup_manifest(&backup_dir, &manifest, manifest_hmac_key.as_deref())?;
            Ok(manifest)
        })
        .await
        .map_err(|e| AedbError::Io(std::io::Error::other(e.to_string())))?
    }

    /// Creates a full backup and packs it into a single archive file.
    ///
    /// The archive is encrypted when `checkpoint_encryption_key` is configured.
    pub async fn backup_full_to_file(
        &self,
        backup_file: &Path,
    ) -> Result<BackupManifest, AedbError> {
        let temp = tempfile::tempdir()?;
        let manifest = self.backup_full(temp.path()).await?;
        let temp_path = temp.path().to_path_buf();
        let backup_file = backup_file.to_path_buf();
        let checkpoint_key = self._config.checkpoint_key().copied();
        tokio::task::spawn_blocking(move || {
            write_backup_archive(&temp_path, &backup_file, checkpoint_key.as_ref())
        })
        .await
        .map_err(|e| AedbError::Io(std::io::Error::other(e.to_string())))??;
        Ok(manifest)
    }

    pub async fn backup_incremental(
        &self,
        backup_dir: &Path,
        parent_backup_dir: &Path,
    ) -> Result<BackupManifest, AedbError> {
        if self.require_authenticated_calls {
            return Err(AedbError::PermissionDenied(
                "authenticated caller required in secure mode".into(),
            ));
        }
        create_private_dir_all(backup_dir)?;
        create_private_dir_all(&backup_dir.join("wal_tail"))?;
        let parent = load_backup_manifest(parent_backup_dir, self._config.hmac_key())?;
        verify_backup_files(parent_backup_dir, &parent)?;
        let from_seq = parent.wal_head_seq.saturating_add(1);

        let lease = self.acquire_snapshot(ConsistencyMode::AtLatest).await?;
        let to_seq = lease.view.seq;
        let created_at_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let mut wal_segments = Vec::new();
        let mut file_sha256 = BTreeMap::new();

        for segment in read_segments(&self.dir)? {
            let src = self.dir.join(&segment.filename);
            let Some((min_seq, max_seq)) = scan_segment_seq_range(&src)? else {
                continue;
            };
            if max_seq < from_seq || min_seq > to_seq {
                continue;
            }
            let rel = format!("wal_tail/{}", segment.filename);
            let dst = backup_dir.join(&rel);
            let hash = copy_file_prefix_sha256_hex(&src, &dst, segment.size_bytes)?;
            wal_segments.push(segment.filename);
            file_sha256.insert(rel, hash);
        }

        wal_segments.sort_by_key(|name| segment_seq_from_name(name).unwrap_or(0));
        let manifest = BackupManifest {
            backup_id: format!("bk_{}_{}", created_at_micros, to_seq),
            backup_type: "incremental".into(),
            parent_backup_id: Some(parent.backup_id),
            from_seq: Some(from_seq),
            created_at_micros,
            aedb_version: env!("CARGO_PKG_VERSION").to_string(),
            checkpoint_seq: parent.checkpoint_seq,
            wal_head_seq: to_seq,
            checkpoint_file: parent.checkpoint_file,
            wal_segments,
            file_sha256,
        };
        write_backup_manifest(backup_dir, &manifest, self._config.hmac_key())?;
        Ok(manifest)
    }

    pub fn restore_from_backup(
        backup_dir: &Path,
        data_dir: &Path,
        config: &AedbConfig,
    ) -> Result<u64, AedbError> {
        Self::restore_from_backup_chain(&[backup_dir.to_path_buf()], data_dir, config, None)
    }

    /// Restores from a single-file full backup archive created by `backup_full_to_file`.
    pub fn restore_from_backup_file(
        backup_file: &Path,
        data_dir: &Path,
        config: &AedbConfig,
    ) -> Result<u64, AedbError> {
        let temp = tempfile::tempdir()?;
        extract_backup_archive(backup_file, temp.path(), config.checkpoint_key())?;
        Self::restore_from_backup(temp.path(), data_dir, config)
    }

    pub fn restore_from_backup_chain(
        backup_dirs: &[PathBuf],
        data_dir: &Path,
        config: &AedbConfig,
        target_seq: Option<u64>,
    ) -> Result<u64, AedbError> {
        let chain = load_verified_backup_chain(backup_dirs, config)?;
        validate_backup_chain_compatibility(&chain, config.strict_recovery())?;
        if data_dir.exists() && fs::read_dir(data_dir)?.next().is_some() {
            return Err(AedbError::Validation(
                "restore target directory must be empty".into(),
            ));
        }
        create_private_dir_all(data_dir)?;
        let effective_target = target_seq.unwrap_or(last_backup_manifest(&chain)?.wal_head_seq);
        let full = &chain[0].1;
        if effective_target < full.checkpoint_seq {
            return Err(AedbError::Validation(
                "target_seq is older than full checkpoint_seq".into(),
            ));
        }

        let checkpoint_path = resolve_backup_path(&chain[0].0, &full.checkpoint_file)?;
        let (mut keyspace, mut catalog, checkpoint_seq, mut idempotency) =
            load_checkpoint_with_key(&checkpoint_path, config.checkpoint_key())?;
        let mut current_seq = checkpoint_seq;
        let mut last_verified_chain: Option<(u64, [u8; 32])> = None;

        for (dir, manifest) in &chain {
            let replay_to = effective_target.min(manifest.wal_head_seq);
            if replay_to <= current_seq {
                continue;
            }
            let mut wal_paths = Vec::new();
            for seg in &manifest.wal_segments {
                wal_paths.push(resolve_backup_path(dir, &format!("wal_tail/{seg}"))?);
            }
            wal_paths.sort_by_key(|p| {
                p.file_name()
                    .and_then(|n| segment_seq_from_name(&n.to_string_lossy()))
                    .unwrap_or(0)
            });
            if config.hash_chain_required && !wal_paths.is_empty() {
                let first_seq = wal_paths
                    .first()
                    .and_then(|p| p.file_name())
                    .and_then(|n| segment_seq_from_name(&n.to_string_lossy()));
                let chain_anchor = match (last_verified_chain, first_seq) {
                    (Some((last_seq, last_hash)), Some(first_seq)) if first_seq > last_seq => {
                        Some(last_hash)
                    }
                    _ => None,
                };
                if let Some(last) = verify_hash_chain_batch(&wal_paths, chain_anchor)? {
                    last_verified_chain = Some(last);
                }
            }
            current_seq = replay_segments(ReplaySegmentsRequest {
                segments: &wal_paths,
                from_seq_exclusive: current_seq,
                to_seq_inclusive: Some(replay_to),
                segment_replay_byte_limits: None,
                hash_chain_required: false,
                strict_recovery: config.strict_recovery(),
                keyspace: &mut keyspace,
                catalog: &mut catalog,
                idempotency: &mut idempotency,
            })?;
            if current_seq >= effective_target {
                break;
            }
        }
        if current_seq != effective_target {
            return Err(AedbError::Validation(format!(
                "backup chain replay incomplete: restored seq {current_seq}, expected {effective_target}"
            )));
        }
        let restored_seq = current_seq;
        let cp = write_checkpoint_with_key(
            &keyspace.snapshot(),
            &catalog,
            restored_seq,
            data_dir,
            config.checkpoint_key(),
            config.checkpoint_key_id.clone(),
            idempotency,
            config.checkpoint_compression_level,
        )?;
        let restored_manifest = Manifest {
            durable_seq: restored_seq,
            visible_seq: restored_seq,
            active_segment_seq: restored_seq + 1,
            checkpoints: vec![cp],
            segments: Vec::new(),
            ..Default::default()
        };
        write_manifest_atomic_signed(&restored_manifest, data_dir, config.hmac_key())?;
        Ok(restored_seq)
    }

    pub fn restore_from_backup_chain_at_time(
        backup_dirs: &[PathBuf],
        data_dir: &Path,
        config: &AedbConfig,
        target_time_micros: u64,
    ) -> Result<u64, AedbError> {
        let chain = load_verified_backup_chain(backup_dirs, config)?;
        validate_backup_chain_compatibility(&chain, config.strict_recovery())?;
        let target_seq = resolve_target_seq_for_time(
            &chain,
            target_time_micros,
            config.hash_chain_required,
            config.strict_recovery(),
        )?;
        Self::restore_from_backup_chain(backup_dirs, data_dir, config, Some(target_seq))
    }

    pub fn restore_namespace_from_backup_chain(
        backup_dirs: &[PathBuf],
        data_dir: &Path,
        config: &AedbConfig,
        project_id: &str,
        scope_id: &str,
        target_seq: Option<u64>,
    ) -> Result<u64, AedbError> {
        let chain = load_verified_backup_chain(backup_dirs, config)?;
        let effective_target = target_seq.unwrap_or(last_backup_manifest(&chain)?.wal_head_seq);
        let live = recover_with_config(data_dir, config)?;

        let temp = tempfile::tempdir()?;
        let _ = Self::restore_from_backup_chain(
            backup_dirs,
            temp.path(),
            config,
            Some(effective_target),
        )?;
        let restored = recover_with_config(temp.path(), config)?;

        let ns_key = namespace_key(project_id, scope_id);
        let ns_id = NamespaceId::project_scope(project_id, scope_id);

        let mut merged_namespaces = live
            .keyspace
            .namespaces
            .iter()
            .filter(|(ns, _)| **ns != ns_id)
            .map(|(ns, namespace)| (ns.clone(), namespace.clone()))
            .collect::<HashMap<_, _>>();
        if let Some(namespace) = restored.keyspace.namespaces.get(&ns_id) {
            merged_namespaces.insert(ns_id.clone(), namespace.clone());
        }
        let mut merged_async_indexes = live
            .keyspace
            .async_indexes
            .iter()
            .filter(|(key, _)| key.0 != ns_id)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<HashMap<_, _>>();
        for (key, value) in restored.keyspace.async_indexes.iter() {
            if key.0 == ns_id {
                merged_async_indexes.insert(key.clone(), value.clone());
            }
        }
        let mut merged_keyspace = Keyspace {
            primary_index_backend: live.keyspace.primary_index_backend,
            value_store: live.keyspace.value_store.clone(),
            kv_segment_store: live.keyspace.kv_segment_store.clone(),
            persistent_value_inline_threshold_bytes: live
                .keyspace
                .persistent_value_inline_threshold_bytes,
            namespaces: Arc::new(merged_namespaces.into()),
            async_indexes: Arc::new(merged_async_indexes.into()),
            mem_bytes: 0,
        };
        merged_keyspace.refresh_mem_bytes();

        let mut merged_catalog = live.catalog.clone();
        if let Some(project) = restored.catalog.projects.get(project_id) {
            merged_catalog
                .projects
                .insert(project_id.to_string(), project.clone());
        }
        let scope_key = (project_id.to_string(), scope_id.to_string());
        match restored.catalog.scopes.get(&scope_key) {
            Some(scope) => {
                merged_catalog
                    .scopes
                    .insert(scope_key.clone(), scope.clone());
            }
            None => {
                merged_catalog.scopes.remove(&scope_key);
            }
        }
        let table_keys: Vec<(String, String)> = merged_catalog
            .tables
            .keys()
            .filter(|(ns, _)| ns == &ns_key)
            .cloned()
            .collect();
        for key in table_keys {
            merged_catalog.tables.remove(&key);
        }
        for (key, table) in restored.catalog.tables.iter() {
            if key.0 == ns_key {
                merged_catalog.tables.insert(key.clone(), table.clone());
            }
        }

        let index_keys: Vec<(String, String, String)> = merged_catalog
            .indexes
            .keys()
            .filter(|(ns, _, _)| ns == &ns_key)
            .cloned()
            .collect();
        for key in index_keys {
            merged_catalog.indexes.remove(&key);
        }
        for (key, index) in restored.catalog.indexes.iter() {
            if key.0 == ns_key {
                merged_catalog.indexes.insert(key.clone(), index.clone());
            }
        }

        let async_index_keys: Vec<(String, String, String)> = merged_catalog
            .async_indexes
            .keys()
            .filter(|(ns, _, _)| ns == &ns_key)
            .cloned()
            .collect();
        for key in async_index_keys {
            merged_catalog.async_indexes.remove(&key);
        }
        for (key, index) in restored.catalog.async_indexes.iter() {
            if key.0 == ns_key {
                merged_catalog
                    .async_indexes
                    .insert(key.clone(), index.clone());
            }
        }

        let merged_seq = live.current_seq.max(effective_target);
        let cp = write_checkpoint_with_key(
            &merged_keyspace.snapshot(),
            &merged_catalog,
            merged_seq,
            data_dir,
            config.checkpoint_key(),
            config.checkpoint_key_id.clone(),
            live.idempotency,
            config.checkpoint_compression_level,
        )?;
        let manifest = Manifest {
            durable_seq: merged_seq,
            visible_seq: merged_seq,
            active_segment_seq: merged_seq + 1,
            checkpoints: vec![cp],
            segments: Vec::new(),
            ..Default::default()
        };
        write_manifest_atomic_signed(&manifest, data_dir, config.hmac_key())?;
        Ok(merged_seq)
    }
}

fn last_backup_manifest(chain: &[(PathBuf, BackupManifest)]) -> Result<&BackupManifest, AedbError> {
    chain
        .last()
        .map(|(_, manifest)| manifest)
        .ok_or_else(|| AedbError::Validation("backup chain cannot be empty".into()))
}
