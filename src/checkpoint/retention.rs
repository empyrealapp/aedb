use crate::checkpoint::writer::CheckpointMeta;
use crate::manifest::atomic::parse_checkpoint_seq;
use std::path::Path;

/// Merges a freshly written checkpoint into the prior checkpoint list, keeping
/// only the `retention` most recent entries (sorted by seq ascending, so the
/// newest is last). Any prior entry sharing the new seq is replaced.
pub(crate) fn merge_retained_checkpoints(
    prior: Vec<CheckpointMeta>,
    new_checkpoint: CheckpointMeta,
    retention: usize,
) -> Vec<CheckpointMeta> {
    let retention = retention.max(1);
    let mut checkpoints: Vec<CheckpointMeta> = prior
        .into_iter()
        .filter(|cp| cp.seq != new_checkpoint.seq)
        .collect();
    checkpoints.push(new_checkpoint);
    checkpoints.sort_by_key(|cp| cp.seq);
    if checkpoints.len() > retention {
        let drop = checkpoints.len() - retention;
        checkpoints.drain(0..drop);
    }
    checkpoints
}

/// Deletes orphaned `checkpoint_*.aedb.zst` files whose seq is strictly below
/// `oldest_retained_seq`. Best-effort: a failure to remove a stale file is
/// ignored so pruning never fails an already-durable checkpoint.
pub(crate) fn prune_superseded_checkpoint_files(dir: &Path, oldest_retained_seq: u64) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if let Some(seq) = parse_checkpoint_seq(name)
            && seq < oldest_retained_seq
        {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

#[cfg(test)]
mod tests;
