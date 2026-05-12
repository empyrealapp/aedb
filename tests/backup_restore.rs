use aedb::AedbInstance;
use aedb::backup::{
    load_backup_manifest, sha256_file_hex, verify_backup_files, write_backup_manifest,
};
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::tx::{TransactionEnvelope, WriteClass, WriteIntent};
use aedb::commit::validation::{KvU64MissingPolicy, KvU64OverflowPolicy, Mutation};
use aedb::config::{AedbConfig, DurabilityMode, RecoveryMode, StorageMode};
use aedb::query::plan::{ConsistencyMode, Query};
use aedb::recovery::recover_with_config;
use aedb::wal::frame::{FrameError, FrameReader};
use aedb::wal::segment::SEGMENT_HEADER_SIZE;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tempfile::tempdir;
use tokio::task::JoinSet;
use tokio::time::{Duration, timeout};

type CommitLog = Arc<tokio::sync::Mutex<Vec<(u64, Vec<u8>)>>>;

fn backup_test_config() -> AedbConfig {
    AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        ..AedbConfig::default()
    }
    .with_hmac_key(vec![5u8; 32])
}

fn strict_backup_chain_config() -> AedbConfig {
    let mut cfg = AedbConfig::production([11u8; 32]);
    cfg.max_segment_bytes = 4 * 1024;
    cfg.max_segment_age_secs = 3600;
    cfg.durability_mode = DurabilityMode::Batch;
    cfg.batch_interval_ms = 60_000;
    cfg.batch_max_bytes = usize::MAX;
    cfg
}

fn production_e2e_config() -> AedbConfig {
    let mut cfg = AedbConfig::production([73u8; 32]).with_checkpoint_key([91u8; 32]);
    cfg.max_segment_bytes = 32 * 1024;
    cfg.persistent_value_hot_cache_bytes = 64 * 1024;
    cfg.max_kv_value_bytes = 128 * 1024;
    cfg.max_transaction_bytes = 512 * 1024;
    cfg.max_inflight_commits = 128;
    cfg.epoch_max_commits = 32;
    cfg.parallel_worker_threads = 4;
    cfg
}

fn e2e_blob(worker_id: usize, op_id: usize) -> Vec<u8> {
    let mut blob = vec![u8::try_from((worker_id * 31 + op_id) % 251).unwrap(); 4096];
    blob[..8].copy_from_slice(&(worker_id as u64).to_be_bytes());
    blob[8..16].copy_from_slice(&(op_id as u64).to_be_bytes());
    blob
}

async fn assert_parallel_e2e_state(db: &AedbInstance, expected_rows: usize) {
    let counter = db
        .kv_get_no_auth("p", "app", b"total", ConsistencyMode::AtLatest)
        .await
        .expect("get total")
        .expect("total present");
    let counter_value = u64::from_be_bytes(counter.value.try_into().expect("u64 counter encoding"));
    assert_eq!(counter_value, expected_rows as u64);

    let rows = db
        .query(
            "p",
            "app",
            Query::select(&["id"])
                .from("events")
                .limit(expected_rows + 1),
        )
        .await
        .expect("query events");
    assert_eq!(rows.rows.len(), expected_rows);

    for (worker_id, op_id) in [(0usize, 0usize), (1, 7), (3, 11)] {
        let key = format!("blob:{worker_id}:{op_id}");
        let value = db
            .kv_get_no_auth("p", "app", key.as_bytes(), ConsistencyMode::AtLatest)
            .await
            .expect("get blob")
            .expect("blob present");
        assert_eq!(value.value, e2e_blob(worker_id, op_id));
    }
}

fn read_wal_frame_seq_times(backup_dir: &Path) -> Vec<(u64, u64)> {
    let wal_dir = backup_dir.join("wal_tail");
    let mut segments: Vec<(u64, String)> = fs::read_dir(&wal_dir)
        .expect("read wal dir")
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            if !name.starts_with("segment_") || !name.ends_with(".aedbwal") {
                return None;
            }
            let seq = name
                .trim_start_matches("segment_")
                .trim_end_matches(".aedbwal")
                .parse::<u64>()
                .ok()?;
            Some((seq, name))
        })
        .collect();
    segments.sort_by_key(|(seq, _)| *seq);

    let mut out = Vec::new();
    for (_, name) in segments {
        let file = File::open(wal_dir.join(name)).expect("open segment");
        if file.metadata().expect("meta").len() <= SEGMENT_HEADER_SIZE as u64 {
            continue;
        }
        let mut reader = BufReader::with_capacity(64 * 1024, file);
        let mut header = [0u8; SEGMENT_HEADER_SIZE];
        reader.read_exact(&mut header).expect("header");
        let mut frame_reader = FrameReader::new(reader);
        loop {
            match frame_reader.next_frame() {
                Ok(Some(frame)) => out.push((frame.commit_seq, frame.timestamp_micros)),
                Ok(None) | Err(FrameError::Truncation) => break,
                Err(e) => panic!("frame decode error: {e:?}"),
            }
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_full_is_hot_and_restore_is_consistent_at_backup_head() {
    let live_dir = tempdir().expect("live dir");
    let backup_dir = tempdir().expect("backup dir");
    let restore_dir = tempdir().expect("restore dir");
    let config = backup_test_config();

    let db = Arc::new(AedbInstance::open(config.clone(), live_dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    for i in 0..2_000u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("seed:{i}").into_bytes(),
            value: vec![u8::try_from(i % 251).unwrap_or(0); 1024],
        })
        .await
        .expect("seed write");
    }

    let stop = Arc::new(AtomicBool::new(false));
    let write_count = Arc::new(AtomicU64::new(0));
    let read_count = Arc::new(AtomicU64::new(0));
    let commit_log: CommitLog = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let writer_db = Arc::clone(&db);
    let writer_stop = Arc::clone(&stop);
    let writer_count = Arc::clone(&write_count);
    let writer_log = Arc::clone(&commit_log);
    let writer = tokio::spawn(async move {
        let mut i = 0u64;
        while !writer_stop.load(Ordering::Relaxed) {
            let key = format!("hot:{i}").into_bytes();
            let res = writer_db
                .commit(Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: key.clone(),
                    value: i.to_string().into_bytes(),
                })
                .await;
            if let Ok(committed) = res {
                writer_count.fetch_add(1, Ordering::Relaxed);
                writer_log.lock().await.push((committed.commit_seq, key));
            }
            i = i.saturating_add(1);
            tokio::task::yield_now().await;
        }
    });

    let reader_db = Arc::clone(&db);
    let reader_stop = Arc::clone(&stop);
    let reader_count_ref = Arc::clone(&read_count);
    let reader = tokio::spawn(async move {
        while !reader_stop.load(Ordering::Relaxed) {
            let _ = reader_db.snapshot_probe(ConsistencyMode::AtLatest).await;
            reader_count_ref.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let writes_before = write_count.load(Ordering::Relaxed);
    let reads_before = read_count.load(Ordering::Relaxed);

    let backup = timeout(Duration::from_secs(10), db.backup_full(backup_dir.path()))
        .await
        .expect("backup full timed out")
        .expect("backup full");
    let writes_after = write_count.load(Ordering::Relaxed);
    let reads_after = read_count.load(Ordering::Relaxed);
    assert!(writes_after > writes_before, "writes stalled during backup");
    assert!(reads_after > reads_before, "reads stalled during backup");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    stop.store(true, Ordering::Relaxed);
    timeout(Duration::from_secs(5), writer)
        .await
        .expect("writer join timed out")
        .expect("writer join");
    timeout(Duration::from_secs(5), reader)
        .await
        .expect("reader join timed out")
        .expect("reader join");

    timeout(Duration::from_secs(5), db.shutdown())
        .await
        .expect("shutdown timed out")
        .expect("graceful shutdown");
    drop(db);

    let restored_seq =
        AedbInstance::restore_from_backup(backup_dir.path(), restore_dir.path(), &config)
            .expect("restore from backup");
    assert_eq!(restored_seq, backup.wal_head_seq);

    let recovered = recover_with_config(restore_dir.path(), &config).expect("recover restored");
    assert_eq!(recovered.current_seq, backup.wal_head_seq);

    let log = commit_log.lock().await.clone();
    assert!(
        log.iter().any(|(seq, _)| *seq > backup.wal_head_seq),
        "test must produce writes after backup head"
    );
    for (_, key) in log
        .iter()
        .filter(|(seq, _)| *seq <= backup.wal_head_seq)
        .take(64)
    {
        assert!(
            recovered.keyspace.kv_get("p", "app", key).is_some(),
            "key <= backup head must exist in restored state"
        );
    }
    for (_, key) in log
        .iter()
        .filter(|(seq, _)| *seq > backup.wal_head_seq)
        .take(64)
    {
        assert!(
            recovered.keyspace.kv_get("p", "app", key).is_none(),
            "key > backup head must not exist in restored state"
        );
    }
}

#[tokio::test]
async fn full_backup_of_disk_backed_values_is_self_contained() {
    let live_dir = tempdir().expect("live dir");
    let backup_dir = tempdir().expect("backup dir");
    let disk_restore_dir = tempdir().expect("disk restore dir");
    let memory_restore_dir = tempdir().expect("memory restore dir");
    let config = AedbConfig {
        storage_mode: StorageMode::DiskBacked,
        persistent_value_inline_threshold_bytes: 32,
        max_kv_value_bytes: 256 * 1024,
        max_transaction_bytes: 512 * 1024,
        ..backup_test_config()
    };
    let value: Vec<u8> = (0..96 * 1024).map(|i| (i % 251) as u8).collect();

    let db = AedbInstance::open(config.clone(), live_dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.kv_set("p", "app", b"blob".to_vec(), value.clone())
        .await
        .expect("set large value");
    let metrics = db.operational_metrics().await;
    assert!(
        metrics.persistent_value_store_bytes > value.len() as u64,
        "test must spill value to persistent sidecar"
    );

    let backup = db.backup_full(backup_dir.path()).await.expect("backup");
    assert_eq!(backup.wal_head_seq, backup.checkpoint_seq);
    verify_backup_files(backup_dir.path(), &backup).expect("backup manifest verifies");
    assert!(
        !backup_dir.path().join("values.aedbdat").exists(),
        "backup must be self-contained in checkpoint/WAL artifacts, not sidecar-dependent"
    );

    AedbInstance::restore_from_backup(backup_dir.path(), disk_restore_dir.path(), &config)
        .expect("restore disk-backed");
    let disk_restored =
        AedbInstance::open(config.clone(), disk_restore_dir.path()).expect("open disk restored");
    let disk_value = disk_restored
        .kv_get_no_auth("p", "app", b"blob", ConsistencyMode::AtLatest)
        .await
        .expect("disk get")
        .expect("disk value");
    assert_eq!(disk_value.value, value);

    let memory_config = AedbConfig {
        storage_mode: StorageMode::InMemory,
        ..config.clone()
    };
    AedbInstance::restore_from_backup(backup_dir.path(), memory_restore_dir.path(), &memory_config)
        .expect("restore in-memory");
    let memory_restored =
        AedbInstance::open(memory_config, memory_restore_dir.path()).expect("open memory restored");
    let memory_value = memory_restored
        .kv_get_no_auth("p", "app", b"blob", ConsistencyMode::AtLatest)
        .await
        .expect("memory get")
        .expect("memory value");
    assert_eq!(memory_value.value, value);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incremental_chain_restore_to_target_seq_is_exact() {
    let live_dir = tempdir().expect("live dir");
    let full_dir = tempdir().expect("full dir");
    let inc1_dir = tempdir().expect("inc1 dir");
    let inc2_dir = tempdir().expect("inc2 dir");
    let restore_dir = tempdir().expect("restore dir");
    let config = backup_test_config();

    let db = AedbInstance::open(config.clone(), live_dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let mut seq_to_key = Vec::new();
    for i in 0..60u64 {
        let key = format!("chain:{i}").into_bytes();
        let committed = db
            .commit(Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: key.clone(),
                value: i.to_string().into_bytes(),
            })
            .await
            .expect("write");
        seq_to_key.push((committed.commit_seq, key));
    }
    let full_manifest = db.backup_full(full_dir.path()).await.expect("full");

    for i in 60..120u64 {
        let key = format!("chain:{i}").into_bytes();
        let committed = db
            .commit(Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: key.clone(),
                value: i.to_string().into_bytes(),
            })
            .await
            .expect("write");
        seq_to_key.push((committed.commit_seq, key));
    }
    let inc1_manifest = db
        .backup_incremental(inc1_dir.path(), full_dir.path())
        .await
        .expect("inc1");
    assert_eq!(inc1_manifest.from_seq, Some(full_manifest.wal_head_seq + 1));

    for i in 120..180u64 {
        let key = format!("chain:{i}").into_bytes();
        let committed = db
            .commit(Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: key.clone(),
                value: i.to_string().into_bytes(),
            })
            .await
            .expect("write");
        seq_to_key.push((committed.commit_seq, key));
    }
    let inc2_manifest = db
        .backup_incremental(inc2_dir.path(), inc1_dir.path())
        .await
        .expect("inc2");
    assert_eq!(inc2_manifest.from_seq, Some(inc1_manifest.wal_head_seq + 1));

    let target_seq = inc1_manifest.wal_head_seq.saturating_sub(5);
    let restored_seq = AedbInstance::restore_from_backup_chain(
        &[
            full_dir.path().to_path_buf(),
            inc1_dir.path().to_path_buf(),
            inc2_dir.path().to_path_buf(),
        ],
        restore_dir.path(),
        &config,
        Some(target_seq),
    )
    .expect("restore chain");
    assert_eq!(restored_seq, target_seq);

    let recovered = recover_with_config(restore_dir.path(), &config).expect("recover restored");
    assert_eq!(recovered.current_seq, target_seq);
    for (seq, key) in seq_to_key.iter().take(120) {
        if *seq <= target_seq {
            assert!(
                recovered.keyspace.kv_get("p", "app", key).is_some(),
                "key at/before target should exist"
            );
        } else {
            assert!(
                recovered.keyspace.kv_get("p", "app", key).is_none(),
                "key after target should not exist"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_incremental_is_hot_and_completes_without_stalling() {
    let live_dir = tempdir().expect("live dir");
    let full_dir = tempdir().expect("full dir");
    let inc_dir = tempdir().expect("inc dir");
    let config = backup_test_config();

    let db = Arc::new(AedbInstance::open(config.clone(), live_dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    for i in 0..512u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("seed:{i}").into_bytes(),
            value: vec![u8::try_from(i % 251).unwrap_or(0); 256],
        })
        .await
        .expect("seed write");
    }

    db.backup_full(full_dir.path()).await.expect("full backup");

    let stop = Arc::new(AtomicBool::new(false));
    let write_count = Arc::new(AtomicU64::new(0));
    let read_count = Arc::new(AtomicU64::new(0));

    let writer_db = Arc::clone(&db);
    let writer_stop = Arc::clone(&stop);
    let writer_count_ref = Arc::clone(&write_count);
    let writer = tokio::spawn(async move {
        let mut i = 0u64;
        while !writer_stop.load(Ordering::Relaxed) {
            let _ = writer_db
                .commit(Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: format!("inc-hot:{i}").into_bytes(),
                    value: i.to_string().into_bytes(),
                })
                .await;
            writer_count_ref.fetch_add(1, Ordering::Relaxed);
            i = i.saturating_add(1);
            tokio::task::yield_now().await;
        }
    });

    let reader_db = Arc::clone(&db);
    let reader_stop = Arc::clone(&stop);
    let reader_count_ref = Arc::clone(&read_count);
    let reader = tokio::spawn(async move {
        while !reader_stop.load(Ordering::Relaxed) {
            let _ = reader_db.snapshot_probe(ConsistencyMode::AtLatest).await;
            reader_count_ref.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    let writes_before = write_count.load(Ordering::Relaxed);
    let reads_before = read_count.load(Ordering::Relaxed);

    timeout(
        Duration::from_secs(10),
        db.backup_incremental(inc_dir.path(), full_dir.path()),
    )
    .await
    .expect("incremental backup timed out")
    .expect("incremental backup");

    let writes_after = write_count.load(Ordering::Relaxed);
    let reads_after = read_count.load(Ordering::Relaxed);
    assert!(
        writes_after > writes_before,
        "writes stalled during incremental backup"
    );
    assert!(
        reads_after > reads_before,
        "reads stalled during incremental backup"
    );

    stop.store(true, Ordering::Relaxed);
    timeout(Duration::from_secs(5), writer)
        .await
        .expect("writer join timed out")
        .expect("writer join");
    timeout(Duration::from_secs(5), reader)
        .await
        .expect("reader join timed out")
        .expect("reader join");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incremental_chain_restore_fails_when_chain_cannot_reach_target() {
    let live_dir = tempdir().expect("live dir");
    let full_dir = tempdir().expect("full dir");
    let inc1_dir = tempdir().expect("inc1 dir");
    let restore_dir = tempdir().expect("restore dir");
    let config = backup_test_config();

    let db = AedbInstance::open(config.clone(), live_dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    for i in 0..20u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("gap:{i}").into_bytes(),
            value: i.to_string().into_bytes(),
        })
        .await
        .expect("seed full");
    }
    db.backup_full(full_dir.path()).await.expect("full");

    for i in 20..40u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("gap:{i}").into_bytes(),
            value: i.to_string().into_bytes(),
        })
        .await
        .expect("seed inc");
    }
    let inc1 = db
        .backup_incremental(inc1_dir.path(), full_dir.path())
        .await
        .expect("inc1");

    let mut manifest = load_backup_manifest(inc1_dir.path(), config.hmac_key()).expect("manifest");
    manifest.wal_segments.clear();
    manifest.file_sha256.clear();
    write_backup_manifest(inc1_dir.path(), &manifest, config.hmac_key()).expect("rewrite manifest");

    let err = AedbInstance::restore_from_backup_chain(
        &[full_dir.path().to_path_buf(), inc1_dir.path().to_path_buf()],
        restore_dir.path(),
        &config,
        Some(inc1.wal_head_seq),
    )
    .expect_err("restore should fail when chain cannot reach requested target");
    assert!(
        err.to_string().contains("replay incomplete"),
        "unexpected restore error: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incremental_chain_restore_to_target_time_is_exact() {
    let live_dir = tempdir().expect("live dir");
    let full_dir = tempdir().expect("full dir");
    let inc1_dir = tempdir().expect("inc1 dir");
    let inc2_dir = tempdir().expect("inc2 dir");
    let restore_dir = tempdir().expect("restore dir");
    let config = backup_test_config();

    let db = AedbInstance::open(config.clone(), live_dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    for i in 0..40u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("time:{i}").into_bytes(),
            value: i.to_string().into_bytes(),
        })
        .await
        .expect("write");
    }
    db.backup_full(full_dir.path()).await.expect("full");

    for i in 40..80u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("time:{i}").into_bytes(),
            value: i.to_string().into_bytes(),
        })
        .await
        .expect("write");
    }
    db.backup_incremental(inc1_dir.path(), full_dir.path())
        .await
        .expect("inc1");

    for i in 80..120u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("time:{i}").into_bytes(),
            value: i.to_string().into_bytes(),
        })
        .await
        .expect("write");
    }
    db.backup_incremental(inc2_dir.path(), inc1_dir.path())
        .await
        .expect("inc2");

    let mut seq_times = read_wal_frame_seq_times(full_dir.path());
    seq_times.extend(read_wal_frame_seq_times(inc1_dir.path()));
    seq_times.extend(read_wal_frame_seq_times(inc2_dir.path()));
    seq_times.sort_by_key(|(seq, _)| *seq);
    let target_idx = seq_times.len() / 2;
    let target_time_micros = seq_times[target_idx].1;
    let expected_seq = seq_times
        .iter()
        .filter(|(_, ts)| *ts <= target_time_micros)
        .map(|(seq, _)| *seq)
        .max()
        .expect("expected seq");

    let restored_seq = AedbInstance::restore_from_backup_chain_at_time(
        &[
            full_dir.path().to_path_buf(),
            inc1_dir.path().to_path_buf(),
            inc2_dir.path().to_path_buf(),
        ],
        restore_dir.path(),
        &config,
        target_time_micros,
    )
    .expect("restore by time");
    assert_eq!(restored_seq, expected_seq);

    let recovered = recover_with_config(restore_dir.path(), &config).expect("recover restored");
    assert_eq!(recovered.current_seq, expected_seq);
}

#[tokio::test]
async fn namespace_restore_replaces_only_target_namespace() {
    let live_dir = tempdir().expect("live dir");
    let full_dir = tempdir().expect("full dir");
    let config = backup_test_config();

    let db = AedbInstance::open(config.clone(), live_dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "other").await.expect("scope other");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"k".to_vec(),
        value: b"old".to_vec(),
    })
    .await
    .expect("seed app old");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "other".into(),
        key: b"k".to_vec(),
        value: b"other-new".to_vec(),
    })
    .await
    .expect("seed other");
    let base = db.backup_full(full_dir.path()).await.expect("full");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"k".to_vec(),
        value: b"new".to_vec(),
    })
    .await
    .expect("app new");
    db.shutdown().await.expect("shutdown");

    let live_before_restore =
        recover_with_config(live_dir.path(), &config).expect("recover live before restore");
    let other_before_restore = live_before_restore
        .keyspace
        .kv_get("p", "other", b"k")
        .expect("other value exists before restore");
    assert_eq!(other_before_restore.value, b"other-new".to_vec());

    let merged_seq = AedbInstance::restore_namespace_from_backup_chain(
        &[full_dir.path().to_path_buf()],
        live_dir.path(),
        &config,
        "p",
        "app",
        Some(base.wal_head_seq),
    )
    .expect("namespace restore");
    assert!(merged_seq >= base.wal_head_seq);

    let recovered = recover_with_config(live_dir.path(), &config).expect("recover merged");
    let app = recovered
        .keyspace
        .kv_get("p", "app", b"k")
        .expect("app value exists");
    assert_eq!(app.value, b"old".to_vec());
    let other = recovered
        .keyspace
        .kv_get("p", "other", b"k")
        .expect("other value exists");
    assert_eq!(other.value, b"other-new".to_vec());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_backup_chain_restore_succeeds_with_hash_chain_enforcement() {
    let live_dir = tempdir().expect("live dir");
    let full_dir = tempdir().expect("full dir");
    let inc1_dir = tempdir().expect("inc1 dir");
    let inc2_dir = tempdir().expect("inc2 dir");
    let restore_dir = tempdir().expect("restore dir");
    let config = strict_backup_chain_config();

    let db = AedbInstance::open(config.clone(), live_dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    for i in 0..120u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("strict:{i}").into_bytes(),
            value: vec![u8::try_from(i % 251).expect("byte"); 512],
        })
        .await
        .expect("seed before full");
    }
    db.backup_full(full_dir.path()).await.expect("full");

    for i in 120..220u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("strict:{i}").into_bytes(),
            value: vec![u8::try_from(i % 251).expect("byte"); 512],
        })
        .await
        .expect("seed before inc1");
    }
    let inc1 = db
        .backup_incremental(inc1_dir.path(), full_dir.path())
        .await
        .expect("inc1");

    for i in 220..300u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("strict:{i}").into_bytes(),
            value: vec![u8::try_from(i % 251).expect("byte"); 512],
        })
        .await
        .expect("seed before inc2");
    }
    let inc2 = db
        .backup_incremental(inc2_dir.path(), inc1_dir.path())
        .await
        .expect("inc2");
    db.shutdown().await.expect("shutdown");

    let restored_seq = AedbInstance::restore_from_backup_chain(
        &[
            full_dir.path().to_path_buf(),
            inc1_dir.path().to_path_buf(),
            inc2_dir.path().to_path_buf(),
        ],
        restore_dir.path(),
        &config,
        None,
    )
    .expect("strict restore");
    assert_eq!(restored_seq, inc2.wal_head_seq);
    assert!(inc2.wal_head_seq >= inc1.wal_head_seq);

    let recovered = recover_with_config(restore_dir.path(), &config).expect("recover");
    assert_eq!(recovered.current_seq, inc2.wal_head_seq);
    assert!(
        recovered
            .keyspace
            .kv_get("p", "app", b"strict:299")
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_backup_chain_restore_rejects_tampered_incremental_segment() {
    let live_dir = tempdir().expect("live dir");
    let full_dir = tempdir().expect("full dir");
    let inc1_dir = tempdir().expect("inc1 dir");
    let inc2_dir = tempdir().expect("inc2 dir");
    let restore_dir = tempdir().expect("restore dir");
    let config = strict_backup_chain_config();
    let signing_key = config
        .manifest_hmac_key
        .clone()
        .expect("production config must include manifest_hmac_key");

    let db = AedbInstance::open(config.clone(), live_dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    for i in 0..140u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("tamper:{i}").into_bytes(),
            value: vec![u8::try_from(i % 251).expect("byte"); 512],
        })
        .await
        .expect("seed before full");
    }
    db.backup_full(full_dir.path()).await.expect("full");

    for i in 140..260u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("tamper:{i}").into_bytes(),
            value: vec![u8::try_from(i % 251).expect("byte"); 512],
        })
        .await
        .expect("seed before inc1");
    }
    db.backup_incremental(inc1_dir.path(), full_dir.path())
        .await
        .expect("inc1");

    for i in 260..420u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("tamper:{i}").into_bytes(),
            value: vec![u8::try_from(i % 251).expect("byte"); 512],
        })
        .await
        .expect("seed before inc2");
    }
    db.backup_incremental(inc2_dir.path(), inc1_dir.path())
        .await
        .expect("inc2");
    db.shutdown().await.expect("shutdown");

    let mut inc2_manifest = load_backup_manifest(inc2_dir.path(), config.hmac_key()).expect("load");
    let mut wal_segments = inc2_manifest.wal_segments.clone();
    wal_segments.sort_by_key(|name| {
        name.trim_start_matches("segment_")
            .trim_end_matches(".aedbwal")
            .parse::<u64>()
            .unwrap_or(0)
    });
    assert!(
        wal_segments.len() >= 2,
        "test requires >= 2 wal segments in inc2 backup"
    );
    let tampered_name = wal_segments[1].clone();
    let tampered_rel = format!("wal_tail/{tampered_name}");
    let tampered_path = inc2_dir.path().join(&tampered_rel);
    let mut bytes = fs::read(&tampered_path).expect("read tamper target");
    assert!(bytes.len() > 48, "segment must contain full header");
    bytes[40] ^= 0x5A;
    fs::write(&tampered_path, bytes).expect("write tampered segment");

    let updated_hash = sha256_file_hex(&tampered_path).expect("rehash tampered segment");
    inc2_manifest
        .file_sha256
        .insert(tampered_rel.clone(), updated_hash);
    write_backup_manifest(
        inc2_dir.path(),
        &inc2_manifest,
        Some(signing_key.as_slice()),
    )
    .expect("re-sign tampered backup manifest");

    let err = AedbInstance::restore_from_backup_chain(
        &[
            full_dir.path().to_path_buf(),
            inc1_dir.path().to_path_buf(),
            inc2_dir.path().to_path_buf(),
        ],
        restore_dir.path(),
        &config,
        None,
    )
    .expect_err("strict restore must fail on tampered hash chain");
    assert!(
        format!("{err}").contains("segment hash chain mismatch"),
        "unexpected error: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_file_encrypted_backup_roundtrip_and_wrong_key_fails() {
    let live_dir = tempdir().expect("live dir");
    let restore_dir = tempdir().expect("restore dir");
    let backup_file_dir = tempdir().expect("backup file dir");
    let backup_file = backup_file_dir.path().join("full_backup.aedbarc");
    let key = [42u8; 32];
    let config = backup_test_config().with_checkpoint_key(key);

    let db = AedbInstance::open(config.clone(), live_dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    for i in 0..20u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("single:{i}").into_bytes(),
            value: i.to_string().into_bytes(),
        })
        .await
        .expect("write");
    }

    let backup = db
        .backup_full_to_file(&backup_file)
        .await
        .expect("backup file");
    db.shutdown().await.expect("shutdown");

    let restored_seq =
        AedbInstance::restore_from_backup_file(&backup_file, restore_dir.path(), &config)
            .expect("restore file");
    assert_eq!(restored_seq, backup.wal_head_seq);

    let recovered = recover_with_config(restore_dir.path(), &config).expect("recover restored");
    assert_eq!(recovered.current_seq, backup.wal_head_seq);
    assert!(
        recovered
            .keyspace
            .kv_get("p", "app", b"single:19")
            .is_some()
    );

    let wrong_config = backup_test_config().with_checkpoint_key([1u8; 32]);
    let err = AedbInstance::restore_from_backup_file(
        &backup_file,
        tempdir().expect("bad restore").path(),
        &wrong_config,
    )
    .expect_err("wrong key must fail");
    assert!(format!("{err}").contains("decryption failed"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_disk_backed_updates_checkpoint_and_encrypted_file_restore_are_consistent() {
    const WORKERS: usize = 4;
    const OPS_PER_WORKER: usize = 16;
    const EXPECTED_ROWS: usize = WORKERS * OPS_PER_WORKER;

    let live_dir = tempdir().expect("live dir");
    let restore_dir = tempdir().expect("restore dir");
    let backup_file_dir = tempdir().expect("backup file dir");
    let backup_file = backup_file_dir.path().join("parallel_disk_backed.aedbarc");
    let config = production_e2e_config();

    let db = Arc::new(AedbInstance::open(config.clone(), live_dir.path()).expect("open"));
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "events".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "worker".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "op".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "status".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create events table");

    let mut commits = JoinSet::new();
    for worker_id in 0..WORKERS {
        for op_id in 0..OPS_PER_WORKER {
            let db = Arc::clone(&db);
            commits.spawn(async move {
                let row_id = (worker_id * 10_000 + op_id) as i64;
                let base_seq = db
                    .snapshot_probe(ConsistencyMode::AtLatest)
                    .await
                    .expect("snapshot probe");
                db.commit_envelope(TransactionEnvelope {
                    caller: None,
                    idempotency_key: None,
                    write_class: WriteClass::Standard,
                    assertions: Vec::new(),
                    read_set: Default::default(),
                    write_intent: WriteIntent {
                        mutations: vec![
                            Mutation::KvAddU64Ex {
                                project_id: "p".into(),
                                scope_id: "app".into(),
                                key: b"total".to_vec(),
                                amount_be: 1u64.to_be_bytes(),
                                on_missing: KvU64MissingPolicy::TreatAsZero,
                                on_overflow: KvU64OverflowPolicy::Reject,
                            },
                            Mutation::KvSet {
                                project_id: "p".into(),
                                scope_id: "app".into(),
                                key: format!("blob:{worker_id}:{op_id}").into_bytes(),
                                value: e2e_blob(worker_id, op_id),
                            },
                            Mutation::Upsert {
                                project_id: "p".into(),
                                scope_id: "app".into(),
                                table_name: "events".into(),
                                primary_key: vec![Value::Integer(row_id)],
                                row: Row::from_values(vec![
                                    Value::Integer(row_id),
                                    Value::Integer(worker_id as i64),
                                    Value::Integer(op_id as i64),
                                    Value::Text("committed".into()),
                                ]),
                            },
                        ],
                    },
                    base_seq,
                })
                .await
            });
        }
    }

    let checkpoint_db = Arc::clone(&db);
    let checkpoint = tokio::spawn(async move { checkpoint_db.checkpoint_now().await });

    let mut commit_seqs = Vec::with_capacity(EXPECTED_ROWS);
    while let Some(result) = commits.join_next().await {
        let committed = result.expect("commit task panicked").expect("commit");
        commit_seqs.push(committed.commit_seq);
    }
    assert_eq!(commit_seqs.len(), EXPECTED_ROWS);
    commit_seqs.sort_unstable();
    commit_seqs.dedup();
    assert_eq!(
        commit_seqs.len(),
        EXPECTED_ROWS,
        "parallel commits must get unique monotonic sequence numbers"
    );
    checkpoint
        .await
        .expect("checkpoint task panicked")
        .expect("concurrent checkpoint");

    assert_parallel_e2e_state(&db, EXPECTED_ROWS).await;
    let metrics = db.operational_metrics().await;
    assert!(
        metrics.persistent_value_store_bytes >= (EXPECTED_ROWS * 4096) as u64,
        "disk-backed value store should carry the committed payloads"
    );
    assert!(
        db.estimated_memory_bytes().await < EXPECTED_ROWS * 4096,
        "spilled payload bytes should not be counted as inline keyspace memory"
    );
    assert!(
        fs::metadata(live_dir.path().join("values.aedbdat"))
            .expect("value store metadata")
            .len()
            >= (EXPECTED_ROWS * 4096) as u64,
        "value store file should contain the committed payloads"
    );

    db.checkpoint_now().await.expect("final checkpoint");
    db.shutdown().await.expect("shutdown");
    drop(db);

    let reopened = AedbInstance::open(config.clone(), live_dir.path()).expect("reopen");
    assert_parallel_e2e_state(&reopened, EXPECTED_ROWS).await;
    let backup = reopened
        .backup_full_to_file(&backup_file)
        .await
        .expect("backup file");
    reopened.shutdown().await.expect("shutdown reopened");

    let restored_seq =
        AedbInstance::restore_from_backup_file(&backup_file, restore_dir.path(), &config)
            .expect("restore file");
    assert_eq!(restored_seq, backup.wal_head_seq);

    let restored = AedbInstance::open(config, restore_dir.path()).expect("open restored");
    assert_parallel_e2e_state(&restored, EXPECTED_ROWS).await;
    restored.shutdown().await.expect("shutdown restored");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_backup_excludes_stale_wal_segments() {
    let live_dir = tempdir().expect("live dir");
    let backup_dir = tempdir().expect("backup dir");
    let config = strict_backup_chain_config();

    let db = AedbInstance::open(config, live_dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    for i in 0..160u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("seed:{i}").into_bytes(),
            value: vec![u8::try_from(i % 251).unwrap_or(0); 512],
        })
        .await
        .expect("seed write");
    }

    db.checkpoint_now().await.expect("checkpoint");

    for i in 0..4u64 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("tail:{i}").into_bytes(),
            value: vec![u8::try_from(i % 251).unwrap_or(0); 128],
        })
        .await
        .expect("tail write");
    }

    let live_segments = fs::read_dir(live_dir.path())
        .expect("read live dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".aedbwal"))
        .count();
    assert!(
        live_segments > 1,
        "test requires stale wal segments in the live dir"
    );

    let backup = db.backup_full(backup_dir.path()).await.expect("backup");
    assert_eq!(
        backup.wal_segments.len(),
        1,
        "full backup should keep only the active wal anchor for the checkpointed snapshot"
    );
    assert!(
        live_segments > backup.wal_segments.len(),
        "backup should exclude stale wal segments from the live dir"
    );
}
