use crate::catalog::Catalog;
use crate::commit::apply::apply_mutation;
use crate::commit::tx::{IdempotencyKey, IdempotencyRecord, WalCommitPayload};
use crate::error::AedbError;
use crate::recovery::scanner::validated_hash_chain_prefix_len;
use crate::storage::keyspace::Keyspace;
use crate::wal::frame::{FrameError, FrameReader};
use crate::wal::segment::SEGMENT_HEADER_SIZE;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;

#[allow(clippy::too_many_arguments)]
pub fn replay_segments(
    segments: &[PathBuf],
    from_seq_exclusive: u64,
    to_seq_inclusive: Option<u64>,
    segment_replay_byte_limits: Option<&HashMap<PathBuf, u64>>,
    hash_chain_required: bool,
    strict_recovery: bool,
    keyspace: &mut Keyspace,
    catalog: &mut Catalog,
    idempotency: &mut HashMap<IdempotencyKey, IdempotencyRecord>,
) -> Result<u64, AedbError> {
    let valid_prefix_len =
        validated_hash_chain_prefix_len(segments, hash_chain_required, strict_recovery)?;
    let replay_segments = &segments[..valid_prefix_len];

    let mut max_seq = from_seq_exclusive;
    let mut last_applied_seq = from_seq_exclusive;
    for segment in replay_segments {
        let file = File::open(segment)?;
        let file_size = file.metadata()?.len();
        let replay_bytes = segment_replay_byte_limits
            .and_then(|limits| limits.get(segment).copied())
            .unwrap_or(file_size)
            .min(file_size);
        if replay_bytes <= SEGMENT_HEADER_SIZE as u64 {
            continue;
        }
        let mut reader = BufReader::with_capacity(64 * 1024, file);
        let mut header = [0u8; SEGMENT_HEADER_SIZE];
        reader.read_exact(&mut header)?;
        let payload_bytes = replay_bytes.saturating_sub(SEGMENT_HEADER_SIZE as u64);
        let mut frame_reader = FrameReader::new(reader.take(payload_bytes));
        loop {
            match frame_reader.next_frame() {
                Ok(Some(frame)) => {
                    if frame.commit_seq <= from_seq_exclusive {
                        continue;
                    }
                    if let Some(to_seq) = to_seq_inclusive
                        && frame.commit_seq > to_seq
                    {
                        continue;
                    }
                    if frame.commit_seq <= last_applied_seq {
                        return Err(AedbError::Validation(
                            "non-monotonic wal commit_seq during replay".into(),
                        ));
                    }
                    apply_payload(
                        frame.payload_type,
                        &frame.payload,
                        frame.commit_seq,
                        frame.timestamp_micros,
                        keyspace,
                        catalog,
                        idempotency,
                    )?;
                    last_applied_seq = frame.commit_seq;
                    max_seq = max_seq.max(frame.commit_seq);
                }
                Ok(None) => break,
                Err(FrameError::Truncation) => break,
                Err(FrameError::Corruption) => {
                    return Err(AedbError::Validation(
                        "wal frame corruption detected during replay".into(),
                    ));
                }
                Err(FrameError::Io(e)) => return Err(AedbError::Io(std::io::Error::other(e))),
            }
        }
    }
    Ok(max_seq)
}

fn apply_payload(
    payload_type: u8,
    payload: &[u8],
    commit_seq: u64,
    timestamp_micros: u64,
    keyspace: &mut Keyspace,
    catalog: &mut Catalog,
    idempotency: &mut HashMap<IdempotencyKey, IdempotencyRecord>,
) -> Result<(), AedbError> {
    match payload_type {
        0x01 | 0x02 | 0x04 => {
            let wal_payload = decode_wal_payload(payload)?;
            for mutation in wal_payload.mutations {
                apply_mutation(catalog, keyspace, mutation, commit_seq)?;
            }
            if let Some(key) = wal_payload.idempotency_key {
                idempotency.insert(
                    key,
                    IdempotencyRecord {
                        commit_seq,
                        recorded_at_micros: timestamp_micros,
                    },
                );
            }
        }
        _ => {
            return Err(AedbError::Decode(format!(
                "unknown payload type: {payload_type}"
            )));
        }
    }
    Ok(())
}

fn decode_wal_payload(payload: &[u8]) -> Result<WalCommitPayload, AedbError> {
    rmp_serde::from_slice(payload).map_err(|e| AedbError::Decode(e.to_string()))
}
