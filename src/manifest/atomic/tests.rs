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
    };
    write_manifest_atomic(&m1, dir.path()).expect("write 1");

    let m2 = Manifest {
        durable_seq: 2,
        visible_seq: 2,
        active_segment_seq: 2,
        checkpoints: vec![],
        segments: vec![],
    };
    write_manifest_atomic(&m2, dir.path()).expect("write 2");

    let loaded = load_manifest(dir.path()).expect("load primary");
    assert_eq!(loaded, m2);

    std::fs::write(dir.path().join("manifest.json"), b"{broken").expect("corrupt primary");
    let fallback = load_manifest(dir.path()).expect("fallback");
    assert_eq!(fallback, m1);
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
    };
    let key = b"super-secret-key";
    write_manifest_atomic_signed(&m, dir.path(), Some(key)).expect("write signed");
    let loaded = load_manifest_signed(dir.path(), Some(key)).expect("verify");
    assert_eq!(loaded, m);
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
    };
    let m3 = Manifest {
        durable_seq: 3,
        visible_seq: 3,
        active_segment_seq: 3,
        checkpoints: vec![],
        segments: vec![],
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
