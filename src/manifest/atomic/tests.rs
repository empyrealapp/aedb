use crate::manifest::atomic::{
    hmac_hex, load_manifest, load_manifest_signed, load_manifest_signed_mode,
    write_manifest_atomic, write_manifest_atomic_signed,
};
use crate::manifest::schema::Manifest;
use tempfile::tempdir;

#[test]
fn manifest_roundtrip_and_prev_fallback() {
    let dir = tempdir().expect("temp");
    let m1 = Manifest {
        durable_seq: 1,
        visible_seq: 1,
        active_segment_seq: 1,
        checkpoints: vec![],
        segments: vec![],
        ..Default::default()
    };
    write_manifest_atomic(&m1, dir.path()).expect("write 1");

    let m2 = Manifest {
        durable_seq: 2,
        visible_seq: 2,
        active_segment_seq: 2,
        checkpoints: vec![],
        segments: vec![],
        ..Default::default()
    };
    write_manifest_atomic(&m2, dir.path()).expect("write 2");

    // The write path stamps the current format header; compare against the
    // stamped expectation.
    let mut expected_m2 = m2.clone();
    expected_m2.stamp_current_header();
    let mut expected_m1 = m1.clone();
    expected_m1.stamp_current_header();

    let loaded = load_manifest(dir.path()).expect("load primary");
    assert_eq!(loaded, expected_m2);
    assert_eq!(loaded.magic, crate::manifest::schema::MANIFEST_MAGIC);
    assert_eq!(
        loaded.format_version,
        crate::manifest::schema::MANIFEST_FORMAT_VERSION
    );

    std::fs::write(dir.path().join("manifest.json"), b"{broken").expect("corrupt primary");
    let fallback = load_manifest(dir.path()).expect("fallback");
    assert_eq!(fallback, expected_m1);
}

#[test]
fn legacy_manifest_without_header_still_loads() {
    let dir = tempdir().expect("temp");
    // A manifest written before format headers existed: no magic/version fields.
    let legacy = br#"{
        "durable_seq": 5,
        "visible_seq": 5,
        "active_segment_seq": 6,
        "checkpoints": [],
        "segments": []
    }"#;
    std::fs::write(dir.path().join("manifest.json"), legacy).expect("write legacy");

    let loaded = load_manifest(dir.path()).expect("legacy manifest loads");
    assert_eq!(loaded.durable_seq, 5);
    assert_eq!(loaded.magic, 0, "legacy magic defaults to 0");
    assert_eq!(loaded.format_version, 0, "legacy version defaults to 0");
}

#[test]
fn manifest_with_future_format_version_is_rejected() {
    let dir = tempdir().expect("temp");
    let future = format!(
        r#"{{
        "magic": {},
        "format_version": 65535,
        "feature_flags": 0,
        "durable_seq": 1,
        "visible_seq": 1,
        "active_segment_seq": 2,
        "checkpoints": [],
        "segments": []
    }}"#,
        crate::manifest::schema::MANIFEST_MAGIC
    );
    std::fs::write(dir.path().join("manifest.json"), future).expect("write future");
    let err = load_manifest(dir.path()).expect_err("future version rejected");
    assert!(
        matches!(err, crate::error::AedbError::Unavailable { .. }),
        "unexpected error: {err:?}"
    );
}

#[test]
fn manifest_with_wrong_magic_is_rejected() {
    let dir = tempdir().expect("temp");
    let bad = br#"{
        "magic": 305419896,
        "format_version": 1,
        "feature_flags": 0,
        "durable_seq": 1,
        "visible_seq": 1,
        "active_segment_seq": 2,
        "checkpoints": [],
        "segments": []
    }"#;
    std::fs::write(dir.path().join("manifest.json"), bad).expect("write bad");
    let err = load_manifest(dir.path()).expect_err("bad magic rejected");
    assert!(
        matches!(err, crate::error::AedbError::IntegrityError { .. }),
        "unexpected error: {err:?}"
    );
}

#[test]
fn manifest_hmac_sign_and_verify() {
    let dir = tempdir().expect("temp");
    let m = Manifest {
        durable_seq: 3,
        visible_seq: 3,
        active_segment_seq: 3,
        checkpoints: vec![],
        segments: vec![],
        ..Default::default()
    };
    let key = b"super-secret-key";
    write_manifest_atomic_signed(&m, dir.path(), Some(key)).expect("write signed");
    let loaded = load_manifest_signed(dir.path(), Some(key)).expect("verify");
    let mut expected = m.clone();
    expected.stamp_current_header();
    assert_eq!(loaded, expected);
    std::fs::write(dir.path().join("manifest.hmac"), "bad").expect("corrupt sig");
    std::fs::write(dir.path().join("manifest.hmac.prev"), "bad").expect("corrupt sig prev");
    assert!(load_manifest_signed(dir.path(), Some(key)).is_err());
}

#[test]
fn signed_manifest_loads_previous_copy_when_primary_hmac_is_newer_than_manifest() {
    let dir = tempdir().expect("temp");
    let key = b"super-secret-key";
    let m2 = Manifest {
        durable_seq: 2,
        visible_seq: 2,
        active_segment_seq: 2,
        checkpoints: vec![],
        segments: vec![],
        ..Default::default()
    };
    let m3 = Manifest {
        durable_seq: 3,
        visible_seq: 3,
        active_segment_seq: 3,
        checkpoints: vec![],
        segments: vec![],
        ..Default::default()
    };
    write_manifest_atomic_signed(&m2, dir.path(), Some(key)).expect("write signed");
    let m2_bytes = serde_json::to_vec_pretty(&m2).expect("serialize m2");
    let m3_bytes = serde_json::to_vec_pretty(&m3).expect("serialize m3");

    std::fs::write(dir.path().join("manifest.json.prev"), &m2_bytes).expect("write prev");
    std::fs::write(
        dir.path().join("manifest.hmac.prev"),
        hmac_hex(key, &m2_bytes).expect("sign prev"),
    )
    .expect("write prev sig");
    std::fs::write(
        dir.path().join("manifest.hmac"),
        hmac_hex(key, &m3_bytes).expect("sign newer primary"),
    )
    .expect("write mismatched primary sig");

    let loaded = load_manifest_signed(dir.path(), Some(key)).expect("fallback to prev");
    assert_eq!(loaded, m2);
}

#[test]
fn strict_recovery_rejects_reconstruction_without_manifest() {
    let dir = tempdir().expect("temp");
    std::fs::write(
        dir.path().join("segment_0000000000000001.aedbwal"),
        b"placeholder",
    )
    .expect("segment");
    let err = load_manifest_signed_mode(dir.path(), None, true).expect_err("strict fail");
    assert!(format!("{err}").contains("reconstruction disabled"));
}

#[test]
fn reconstruction_collects_all_checkpoints() {
    let dir = tempdir().expect("temp");
    std::fs::write(
        dir.path().join("checkpoint_0000000000000002.aedb.zst"),
        b"x",
    )
    .expect("cp2");
    std::fs::write(
        dir.path().join("checkpoint_0000000000000005.aedb.zst"),
        b"y",
    )
    .expect("cp5");
    let m = load_manifest(dir.path()).expect("reconstruct");
    assert_eq!(m.checkpoints.len(), 2);
    assert_eq!(m.checkpoints[0].seq, 2);
    assert_eq!(m.checkpoints[1].seq, 5);
    assert_eq!(m.visible_seq, 5);
    assert_eq!(m.durable_seq, 5);
}
