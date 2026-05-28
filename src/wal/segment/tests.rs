use super::{
    SEGMENT_HEADER_SIZE, SEGMENT_MAGIC, SegmentConfig, SegmentError, SegmentHeader, SegmentManager,
};
use crate::wal::frame::FrameReader;
use crate::wal::rotation::RotationReason;
use std::fs;
use std::io::Cursor;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[test]
fn segment_header_roundtrip_and_magic_validation() {
    let header = SegmentHeader::new(42, 7, [9u8; 32]);
    let bytes = header.to_bytes();
    assert_eq!(bytes.len(), SEGMENT_HEADER_SIZE);
    let decoded = SegmentHeader::from_bytes(&bytes).expect("decode");
    assert_eq!(decoded.magic, SEGMENT_MAGIC);
    assert_eq!(decoded.instance_id, 42);
    assert_eq!(decoded.segment_seq, 7);
    assert_eq!(decoded.prev_segment_hash, [9u8; 32]);

    let mut bad = bytes;
    bad[0] = 0;
    assert!(matches!(
        SegmentHeader::from_bytes(&bad),
        Err(SegmentError::InvalidMagic)
    ));
}

#[test]
fn size_rotation_trigger_and_no_split_invariant() {
    let dir = tempdir().expect("temp dir");
    let mut manager = SegmentManager::new(
        dir.path(),
        SegmentConfig {
            max_segment_bytes: 160,
            max_segment_age: Duration::from_secs(3600),
        },
        1,
    );
    manager.open_active(1).expect("open");

    manager
        .append_frame(1, 1, 0x01, &[0u8; 128])
        .expect("append");

    assert_eq!(manager.should_rotate(), Some(RotationReason::Size));
    let closed = manager.rotate().expect("rotate");
    assert_eq!(closed.segment_seq, 1);
    assert!(closed.size_bytes > 160);
}

#[test]
fn time_rotation_trigger() {
    let dir = tempdir().expect("temp dir");
    let mut manager = SegmentManager::new(
        dir.path(),
        SegmentConfig {
            max_segment_bytes: 10_000,
            max_segment_age: Duration::from_millis(10),
        },
        2,
    );
    manager.open_active(1).expect("open");
    manager.append_frame(1, 1, 0x01, &[1]).expect("append");

    let now = Instant::now() + Duration::from_millis(20);
    assert_eq!(manager.should_rotate_at(now), Some(RotationReason::Time));
}

#[test]
fn hash_chain_propagates_across_rotations() {
    let dir = tempdir().expect("temp dir");
    let mut manager = SegmentManager::new(
        dir.path(),
        SegmentConfig {
            max_segment_bytes: 200,
            max_segment_age: Duration::from_secs(3600),
        },
        3,
    );
    manager.open_active(1).expect("open");

    let mut closed = Vec::new();
    for i in 1..=5 {
        manager
            .append_frame(i, i, 0x01, &[i as u8; 128])
            .expect("append");
        if manager.should_rotate().is_some() {
            closed.push(manager.rotate().expect("rotate"));
        }
    }
    closed.push(manager.close_active().expect("close"));
    assert!(closed.len() >= 2);

    for s in &closed {
        let data = fs::read(&s.path).expect("read segment");
        let header: [u8; SEGMENT_HEADER_SIZE] =
            data[..SEGMENT_HEADER_SIZE].try_into().expect("header");
        let parsed = SegmentHeader::from_bytes(&header).expect("header parse");
        if parsed.segment_seq == 1 {
            assert_eq!(parsed.prev_segment_hash, [0u8; 32]);
        } else {
            let prev = closed
                .iter()
                .find(|x| x.segment_seq + 1 == parsed.segment_seq)
                .expect("previous segment");
            assert_eq!(parsed.prev_segment_hash, prev.hash);
        }
    }
}

#[test]
fn close_active_hash_matches_segment_bytes() {
    let dir = tempdir().expect("temp dir");
    let mut manager = SegmentManager::new(
        dir.path(),
        SegmentConfig {
            max_segment_bytes: 10_000,
            max_segment_age: Duration::from_secs(3600),
        },
        11,
    );
    manager.open_active(1).expect("open");
    manager
        .append_frame(1, 1, 0x01, b"segment-payload")
        .expect("append");

    let closed = manager.close_active().expect("close");
    let bytes = fs::read(&closed.path).expect("read segment");

    assert_eq!(closed.hash, *blake3::hash(&bytes).as_bytes());
}

#[test]
fn append_frames_batch_writes_multiple_frames_in_order() {
    let dir = tempdir().expect("temp dir");
    let mut manager = SegmentManager::new(
        dir.path(),
        SegmentConfig {
            max_segment_bytes: 10_000,
            max_segment_age: Duration::from_secs(3600),
        },
        9,
    );
    manager.open_active(1).expect("open");

    let batch = [
        super::PendingFrame {
            seq: 11,
            timestamp_micros: 111,
            payload_type: 0x01,
            payload: b"first",
        },
        super::PendingFrame {
            seq: 12,
            timestamp_micros: 222,
            payload_type: 0x02,
            payload: b"second",
        },
    ];
    manager
        .append_frames_with_sync(&batch, false)
        .expect("append batch");
    let closed = manager.close_active().expect("close");

    let data = fs::read(&closed.path).expect("read segment");
    let mut reader = FrameReader::new(Cursor::new(&data[SEGMENT_HEADER_SIZE..]));
    let first = reader
        .next_frame()
        .expect("first frame parse")
        .expect("first frame");
    assert_eq!(first.commit_seq, 11);
    assert_eq!(first.timestamp_micros, 111);
    assert_eq!(first.payload_type, 0x01);
    assert_eq!(first.payload, b"first");

    let second = reader
        .next_frame()
        .expect("second frame parse")
        .expect("second frame");
    assert_eq!(second.commit_seq, 12);
    assert_eq!(second.timestamp_micros, 222);
    assert_eq!(second.payload_type, 0x02);
    assert_eq!(second.payload, b"second");
    assert!(reader.next_frame().expect("end parse").is_none());
}
