use super::{FrameError, FrameReader, FrameWriter, PAYLOAD_TYPE_SIZE_BYTES, U64_SIZE_BYTES};
use std::io::Cursor;

const FRAME_LENGTH_SIZE_BYTES: usize = 4;

#[test]
fn frame_happy_path_reads_what_was_written() {
    let mut writer = FrameWriter::new(Vec::<u8>::new());
    for i in 1..=1000 {
        writer
            .append(i, 1000 + i, 0x01, format!("payload-{i}").as_bytes())
            .expect("append");
    }
    let bytes = writer.into_inner();

    let mut reader = FrameReader::new(Cursor::new(bytes));
    for i in 1..=1000 {
        let frame = reader.next_frame().expect("next").expect("frame");
        assert_eq!(frame.commit_seq, i);
        assert_eq!(frame.timestamp_micros, 1000 + i);
        assert_eq!(frame.payload_type, 0x01);
        assert_eq!(frame.payload, format!("payload-{i}").as_bytes());
    }
    assert!(reader.next_frame().expect("final next").is_none());
}

#[test]
fn frame_corruption_detected() {
    let mut writer = FrameWriter::new(Vec::<u8>::new());
    for i in 1..=10 {
        writer
            .append(i, i, 0x01, format!("payload-{i}").as_bytes())
            .expect("append");
    }
    let mut bytes = writer.into_inner();
    let mut frame_offset_bytes = 0usize;
    let frame_count = 10usize;
    for frame_index in 1..=frame_count {
        assert!(frame_index <= frame_count);
        let frame_body_size_bytes = u32::from_be_bytes(
            bytes[frame_offset_bytes..frame_offset_bytes + FRAME_LENGTH_SIZE_BYTES]
                .try_into()
                .expect("frame size bytes"),
        ) as usize;
        let frame_size_bytes = FRAME_LENGTH_SIZE_BYTES + frame_body_size_bytes;
        assert!(frame_offset_bytes + frame_size_bytes <= bytes.len());
        if frame_index == 5 {
            let payload_offset_bytes = frame_offset_bytes
                + FRAME_LENGTH_SIZE_BYTES
                + U64_SIZE_BYTES
                + U64_SIZE_BYTES
                + PAYLOAD_TYPE_SIZE_BYTES;
            bytes[payload_offset_bytes] ^= 0xFF;
            break;
        }
        frame_offset_bytes += frame_size_bytes;
    }

    let mut reader = FrameReader::new(Cursor::new(bytes));
    for _ in 0..4 {
        let _ = reader
            .next_frame()
            .expect("valid frame")
            .expect("some frame");
    }
    assert_eq!(
        reader.next_frame().expect_err("must be corruption"),
        FrameError::Corruption
    );
}

#[test]
fn frame_truncation_detected() {
    let mut writer = FrameWriter::new(Vec::<u8>::new());
    for i in 1..=10 {
        writer.append(i, i, 0x01, &[1, 2, 3, 4, 5]).expect("append");
    }
    let bytes = writer.into_inner();

    for cut in 1..20 {
        let truncated = &bytes[..bytes.len() - cut];
        let mut reader = FrameReader::new(Cursor::new(truncated));
        loop {
            match reader.next_frame() {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(FrameError::Truncation) => break,
                Err(e) => panic!("unexpected error: {e:?}"),
            }
        }
    }
}

#[test]
fn empty_file_returns_none() {
    let mut reader = FrameReader::new(Cursor::new(Vec::<u8>::new()));
    assert!(reader.next_frame().expect("next").is_none());
}

#[test]
fn partial_length_returns_truncation() {
    let mut reader = FrameReader::new(Cursor::new(vec![0x00, 0x00]));
    assert_eq!(
        reader.next_frame().expect_err("truncation"),
        FrameError::Truncation
    );
}

#[test]
fn oversized_frame_length_is_rejected_without_allocation() {
    let mut bytes = Vec::new();
    let oversized = (super::MAX_FRAME_BODY_BYTES as u32).saturating_add(1);
    bytes.extend_from_slice(&oversized.to_be_bytes());
    let mut reader = FrameReader::new(Cursor::new(bytes));
    assert_eq!(
        reader.next_frame().expect_err("oversized frame"),
        FrameError::Corruption
    );
}
