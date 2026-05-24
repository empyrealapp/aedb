use super::{
    validated_hash_chain_prefix_len, validated_hash_chain_prefix_len_from_checkpoint,
    verify_hash_chain,
};
use crate::wal::segment::SegmentHeader;
use std::io::Write;
use std::path::Path;

fn write_segment(path: &Path, header: SegmentHeader, payload: &[u8]) {
    let mut file = std::fs::File::create(path).expect("create segment");
    file.write_all(&header.to_bytes()).expect("write header");
    file.write_all(payload).expect("write payload");
    file.sync_all().expect("sync");
}

fn segment_hash(path: &Path) -> [u8; 32] {
    let bytes = std::fs::read(path).expect("read segment");
    *blake3::hash(&bytes).as_bytes()
}

#[test]
fn permissive_mode_trims_invalid_chain_tail() {
    let dir = tempfile::tempdir().expect("tempdir");
    let seg1 = dir.path().join("segment_0000000000000001.aedbwal");
    let seg2 = dir.path().join("segment_0000000000000002.aedbwal");

    let h1 = SegmentHeader::new(1, 1, [0u8; 32]);
    write_segment(&seg1, h1, b"frame-data-1");
    let hash1 = segment_hash(&seg1);

    // Valid successor header for seq=2 would use hash1; intentionally break it.
    let h2_bad = SegmentHeader::new(1, 2, [9u8; 32]);
    write_segment(&seg2, h2_bad, b"frame-data-2");

    let paths = vec![seg1.clone(), seg2.clone()];
    assert_eq!(
        validated_hash_chain_prefix_len(&paths, true, false).expect("permissive"),
        1
    );

    let strict_err = validated_hash_chain_prefix_len(&paths, true, true).expect_err("strict");
    assert!(
        strict_err
            .to_string()
            .contains("segment hash chain mismatch")
    );

    let h2_good = SegmentHeader::new(1, 2, hash1);
    write_segment(&seg2, h2_good, b"frame-data-2");
    assert_eq!(
        validated_hash_chain_prefix_len(&paths, true, true).expect("strict valid"),
        2
    );
}

#[test]
fn checkpoint_tail_anchor_allows_first_retained_segment_to_reference_reclaimed_segment() {
    let dir = tempfile::tempdir().expect("tempdir");
    let seg2 = dir.path().join("segment_0000000000000002.aedbwal");
    let seg3 = dir.path().join("segment_0000000000000003.aedbwal");

    let reclaimed_hash = [7u8; 32];
    let h2 = SegmentHeader::new(1, 2, reclaimed_hash);
    write_segment(&seg2, h2, b"frame-data-2");
    let hash2 = segment_hash(&seg2);

    let h3 = SegmentHeader::new(1, 3, hash2);
    write_segment(&seg3, h3, b"frame-data-3");

    let paths = vec![seg2.clone(), seg3.clone()];
    let err = validated_hash_chain_prefix_len(&paths, true, true).expect_err("strict");
    assert!(err.to_string().contains("segment hash chain mismatch"));
    assert_eq!(
        validated_hash_chain_prefix_len_from_checkpoint(&paths, true, true, true)
            .expect("checkpoint anchored tail"),
        2
    );

    let h3_bad = SegmentHeader::new(1, 3, [9u8; 32]);
    write_segment(&seg3, h3_bad, b"frame-data-3");
    let err = validated_hash_chain_prefix_len_from_checkpoint(&paths, true, true, true)
        .expect_err("broken retained tail must still fail");
    assert!(err.to_string().contains("segment hash chain mismatch"));
}

#[test]
fn verify_hash_chain_accepts_valid_segment_sequence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let seg1 = dir.path().join("segment_0000000000000001.aedbwal");
    let seg2 = dir.path().join("segment_0000000000000002.aedbwal");

    let h1 = SegmentHeader::new(7, 1, [0u8; 32]);
    write_segment(&seg1, h1, b"frame-a");
    let hash1 = segment_hash(&seg1);

    let h2 = SegmentHeader::new(7, 2, hash1);
    write_segment(&seg2, h2, b"frame-b");

    verify_hash_chain(&[seg1, seg2]).expect("valid hash chain");
}
