use aedb::AedbInstance;
use aedb::backup::{load_backup_manifest, sha256_file_hex, write_backup_manifest};
use aedb::commit::validation::Mutation;
use aedb::config::{AedbConfig, DurabilityMode, RecoveryMode};
use aedb::query::plan::ConsistencyMode;
use aedb::recovery::recover_with_config;
use aedb::wal::frame::{FrameError, FrameReader};
use aedb::wal::segment::SEGMENT_HEADER_SIZE;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tempfile::tempdir;

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
        }
    });

    let reader_db = Arc::clone(&db);
    let reader_stop = Arc::clone(&stop);
    let reader_count_ref = Arc::clone(&read_count);
    let reader = tokio::spawn(async move {
        while !reader_stop.load(Ordering::Relaxed) {
            let _ = reader_db.snapshot_probe(ConsistencyMode::AtLatest).await;
            reader_count_ref.fetch_add(1, Ordering::Relaxed);
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let writes_before = write_count.load(Ordering::Relaxed);
    let reads_before = read_count.load(Ordering::Relaxed);

    let backup = db
        .backup_full(backup_dir.path())
        .await
        .expect("backup full");
    let writes_after = write_count.load(Ordering::Relaxed);
    let reads_after = read_count.load(Ordering::Relaxed);
    assert!(writes_after > writes_before, "writes stalled during backup");
    assert!(reads_after > reads_before, "reads stalled during backup");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    stop.store(true, Ordering::Relaxed);
    writer.await.expect("writer join");
    reader.await.expect("reader join");

    db.shutdown().await.expect("graceful shutdown");
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
