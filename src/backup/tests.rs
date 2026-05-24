use crate::backup::{
    BACKUP_ARCHIVE_CHUNK_BYTES, BACKUP_ARCHIVE_CHUNKED_FILE_THRESHOLD_BYTES,
    BACKUP_ARCHIVE_ENTRY_CHUNKED_FILE, BACKUP_ARCHIVE_ENTRY_FILE, BACKUP_ARCHIVE_MAGIC,
    BackupManifest, MAX_BACKUP_ARCHIVE_PATH_BYTES, MAX_BACKUP_ARCHIVE_PAYLOAD_BYTES,
    extract_backup_archive, resolve_backup_output_path, sha256_file_hex, validate_backup_manifest,
    verify_backup_files, write_backup_archive,
};
use crate::error::AedbError;
use std::collections::BTreeMap;

fn sample_manifest() -> BackupManifest {
    BackupManifest {
        backup_id: "bk_1".into(),
        backup_type: "full".into(),
        parent_backup_id: None,
        from_seq: None,
        created_at_micros: 1,
        aedb_version: "0.1.0".into(),
        checkpoint_seq: 1,
        wal_head_seq: 2,
        checkpoint_file: "checkpoint_1.aedbcp".into(),
        wal_segments: vec!["segment_2.aedbwal".into()],
        file_sha256: BTreeMap::from([
            ("checkpoint_1.aedbcp".into(), "a".repeat(64)),
            ("wal_tail/segment_2.aedbwal".into(), "b".repeat(64)),
        ]),
    }
}

#[test]
fn backup_manifest_rejects_unsafe_paths() {
    let mut manifest = sample_manifest();
    manifest.wal_segments = vec!["../segment_2.aedbwal".into()];
    let err = validate_backup_manifest(&manifest).expect_err("must reject parent path");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[test]
fn backup_manifest_requires_wal_checksums() {
    let mut manifest = sample_manifest();
    manifest.file_sha256.remove("wal_tail/segment_2.aedbwal");
    let err = validate_backup_manifest(&manifest).expect_err("must require checksum");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[test]
fn backup_manifest_rejects_invalid_checksum_hex() {
    let mut manifest = sample_manifest();
    manifest
        .file_sha256
        .insert("checkpoint_1.aedbcp".into(), "not_hex".into());
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("checkpoint_1.aedbcp"), b"x").expect("write");
    std::fs::create_dir_all(dir.path().join("wal_tail")).expect("wal dir");
    std::fs::write(dir.path().join("wal_tail/segment_2.aedbwal"), b"x").expect("write wal");
    let err = verify_backup_files(dir.path(), &manifest).expect_err("must reject checksum");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[test]
fn backup_archive_roundtrip_plain_and_encrypted() {
    let src = tempfile::tempdir().expect("src");
    let dst_plain = tempfile::tempdir().expect("dst plain");
    let dst_enc = tempfile::tempdir().expect("dst enc");
    let archive_plain = src.path().join("backup_plain.aedbarc");
    let archive_enc = src.path().join("backup_enc.aedbarc");
    std::fs::create_dir_all(src.path().join("wal_tail")).expect("wal dir");
    std::fs::create_dir_all(src.path().join("pages")).expect("pages dir");
    std::fs::write(src.path().join("backup_manifest.json"), b"{\"x\":1}").expect("manifest");
    std::fs::write(src.path().join("wal_tail/segment_1.aedbwal"), b"segment").expect("wal");
    std::fs::write(src.path().join("pages/rows.aedbpg"), b"page-data").expect("page store file");

    write_backup_archive(src.path(), &archive_plain, None).expect("write plain");
    extract_backup_archive(&archive_plain, dst_plain.path(), None).expect("extract plain");
    assert_eq!(
        std::fs::read(dst_plain.path().join("backup_manifest.json")).expect("read manifest"),
        b"{\"x\":1}".to_vec()
    );

    let key = [9u8; 32];
    write_backup_archive(src.path(), &archive_enc, Some(&key)).expect("write enc");
    extract_backup_archive(&archive_enc, dst_enc.path(), Some(&key)).expect("extract enc");
    assert_eq!(
        std::fs::read(dst_enc.path().join("wal_tail/segment_1.aedbwal")).expect("read wal"),
        b"segment".to_vec()
    );
    assert_eq!(
        std::fs::read(dst_enc.path().join("pages/rows.aedbpg")).expect("read page store file"),
        b"page-data".to_vec()
    );

    let wrong = [7u8; 32];
    let err = extract_backup_archive(
        &archive_enc,
        tempfile::tempdir().expect("tmp").path(),
        Some(&wrong),
    )
    .expect_err("wrong key must fail");
    assert!(format!("{err}").contains("decryption failed"));
}

#[test]
fn backup_archive_streams_large_page_file_in_encrypted_chunks() {
    let src = tempfile::tempdir().expect("src");
    let dst = tempfile::tempdir().expect("dst");
    let archive = src.path().join("backup_large_page.aedbarc");
    let page_rel = "pages/rows.aedbpg";
    let page_path = src.path().join(page_rel);
    std::fs::create_dir_all(page_path.parent().expect("page parent")).expect("pages dir");
    std::fs::write(src.path().join("backup_manifest.json"), b"{\"x\":1}").expect("manifest");

    let large_len =
        BACKUP_ARCHIVE_CHUNKED_FILE_THRESHOLD_BYTES as usize + BACKUP_ARCHIVE_CHUNK_BYTES + 17;
    let mut large_page = Vec::with_capacity(large_len);
    for i in 0..large_len {
        large_page.push(((i.wrapping_mul(31) ^ (i >> 7)) & 0xff) as u8);
    }
    std::fs::write(&page_path, &large_page).expect("large page file");

    let key = [11u8; 32];
    write_backup_archive(src.path(), &archive, Some(&key)).expect("write archive");
    let archive_bytes = std::fs::read(&archive).expect("read archive");
    assert!(
        archive_bytes.contains(&BACKUP_ARCHIVE_ENTRY_CHUNKED_FILE),
        "large page file should use chunked archive entry"
    );

    extract_backup_archive(&archive, dst.path(), Some(&key)).expect("extract archive");
    assert_eq!(
        sha256_file_hex(&page_path).expect("source hash"),
        sha256_file_hex(&dst.path().join(page_rel)).expect("restored hash")
    );
    assert_eq!(
        std::fs::read(dst.path().join("backup_manifest.json")).expect("manifest"),
        b"{\"x\":1}".to_vec()
    );

    let wrong = [12u8; 32];
    let err = extract_backup_archive(
        &archive,
        tempfile::tempdir().expect("bad dst").path(),
        Some(&wrong),
    )
    .expect_err("wrong key must fail");
    assert!(format!("{err}").contains("decryption failed"));
}

#[test]
fn backup_archive_rejects_oversized_path_and_payload_lengths() {
    let archive_path = tempfile::NamedTempFile::new().expect("archive");
    let out_dir = tempfile::tempdir().expect("out");

    let mut bytes = Vec::new();
    bytes.extend_from_slice(BACKUP_ARCHIVE_MAGIC);
    bytes.push(0);
    bytes.extend_from_slice(&[0u8; 16]);
    bytes.push(BACKUP_ARCHIVE_ENTRY_FILE);
    bytes.extend_from_slice(&(MAX_BACKUP_ARCHIVE_PATH_BYTES + 1).to_le_bytes());
    std::fs::write(archive_path.path(), &bytes).expect("write malformed archive");
    let err = extract_backup_archive(archive_path.path(), out_dir.path(), None)
        .expect_err("oversized path must be rejected");
    assert!(format!("{err}").contains("path exceeds max length"));

    let archive_path = tempfile::NamedTempFile::new().expect("archive");
    let out_dir = tempfile::tempdir().expect("out");
    let mut bytes = Vec::new();
    bytes.extend_from_slice(BACKUP_ARCHIVE_MAGIC);
    bytes.push(0);
    bytes.extend_from_slice(&[0u8; 16]);
    bytes.push(BACKUP_ARCHIVE_ENTRY_FILE);
    bytes.extend_from_slice(&(8u32).to_le_bytes());
    bytes.extend_from_slice(b"file.bin");
    bytes.extend_from_slice(&(MAX_BACKUP_ARCHIVE_PAYLOAD_BYTES + 1).to_le_bytes());
    std::fs::write(archive_path.path(), &bytes).expect("write malformed archive");
    let err = extract_backup_archive(archive_path.path(), out_dir.path(), None)
        .expect_err("oversized payload must be rejected");
    assert!(format!("{err}").contains("payload exceeds max size"));
}

#[cfg(unix)]
#[test]
fn backup_manifest_rejects_symlink_escape() {
    let mut manifest = sample_manifest();
    let dir = tempfile::tempdir().expect("tempdir");
    let outside = tempfile::tempdir().expect("outside");
    let outside_file = outside.path().join("outside.bin");
    std::fs::write(&outside_file, b"outside").expect("write outside");
    std::os::unix::fs::symlink(&outside_file, dir.path().join("checkpoint_1.aedbcp"))
        .expect("symlink checkpoint");
    std::fs::create_dir_all(dir.path().join("wal_tail")).expect("wal dir");
    std::fs::write(dir.path().join("wal_tail/segment_2.aedbwal"), b"x").expect("write wal");
    manifest.file_sha256.insert(
        "checkpoint_1.aedbcp".into(),
        sha256_file_hex(&outside_file).expect("hash"),
    );
    manifest.file_sha256.insert(
        "wal_tail/segment_2.aedbwal".into(),
        sha256_file_hex(&dir.path().join("wal_tail/segment_2.aedbwal")).expect("hash wal"),
    );
    let err = verify_backup_files(dir.path(), &manifest).expect_err("must reject escape");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[cfg(unix)]
#[test]
fn backup_output_path_rejects_symlinked_parent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let outside = tempfile::tempdir().expect("outside");
    std::os::unix::fs::symlink(outside.path(), dir.path().join("wal_tail"))
        .expect("symlink parent");
    let err = resolve_backup_output_path(dir.path(), "wal_tail/segment_1.aedbwal")
        .expect_err("must reject symlinked output parent");
    assert!(matches!(err, AedbError::Validation(_)));
}
