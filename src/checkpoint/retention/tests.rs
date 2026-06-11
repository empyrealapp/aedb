use crate::checkpoint::retention::merge_retained_checkpoints;
use crate::checkpoint::writer::CheckpointMeta;

fn meta(seq: u64) -> CheckpointMeta {
    CheckpointMeta {
        filename: format!("checkpoint_{seq:016}.aedb.zst"),
        seq,
        sha256_hex: String::new(),
        created_at_micros: 0,
        key_id: None,
    }
}

#[test]
fn merge_keeps_newest_within_retention_and_sorts_ascending() {
    let prior = vec![meta(1), meta(2)];
    let merged = merge_retained_checkpoints(prior, meta(3), 2);
    let seqs: Vec<u64> = merged.iter().map(|cp| cp.seq).collect();
    assert_eq!(seqs, vec![2, 3]);
}

#[test]
fn merge_replaces_entry_with_duplicate_seq() {
    let prior = vec![meta(1), meta(2)];
    let merged = merge_retained_checkpoints(prior, meta(2), 5);
    let seqs: Vec<u64> = merged.iter().map(|cp| cp.seq).collect();
    assert_eq!(seqs, vec![1, 2]);
}

#[test]
fn merge_clamps_retention_to_at_least_one() {
    let merged = merge_retained_checkpoints(vec![meta(1), meta(2)], meta(3), 0);
    let seqs: Vec<u64> = merged.iter().map(|cp| cp.seq).collect();
    assert_eq!(seqs, vec![3]);
}
