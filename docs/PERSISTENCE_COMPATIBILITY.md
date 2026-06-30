# Persistence Format Compatibility Policy

AEDB persists data across WAL segments, checkpoints, manifests, value-store files, page-store files, and KV segment files. Changes to those boundaries must be treated as compatibility changes, even when the Rust API is unchanged.

## Policy

- Compatible readers may accept older files only when tests prove the old format still opens, replays, restores, or is migrated.
- Writers must not silently emit a new incompatible format under the same magic/header/manifest semantics.
- New required fields in persisted metadata need defaults, version gates, or migration logic.
- Removing, renaming, or changing the meaning of persisted fields requires migration tests.
- Strict recovery must fail closed when it sees corruption or unsupported required format data.

## Compatibility Boundaries

| Boundary | Current files/data | What is compatibility-sensitive |
| --- | --- | --- |
| WAL frame | `segment_*.aedbwal`, `src/wal/frame.rs` | Frame length layout, `commit_seq`, timestamp, payload type, payload encoding, CRC32C coverage, segment header, hash-chain behavior. |
| WAL payload | `WalCommitPayload` encoding in `src/commit/executor/internals.rs` and replay in `src/recovery/replay.rs` | Mutation serialization, idempotency metadata, request fingerprints, payload type dispatch. |
| Checkpoint | `checkpoint_*.aedb.zst`, `src/checkpoint/` | Serialized catalog/keyspace/version state, compression/encryption envelope, checkpoint metadata, materialization of spilled values and KV segments. **Row version representation:** `StoredRow` carries its MVCC version inline via the `Versioned` variant; the legacy `Resident`/`Spilled` variants remain at enum positions 0/1 so older checkpoints deserialize unchanged, with their version read from the (now `#[serde(default)]`) `TableData::row_versions` parallel map. New checkpoints write `Versioned` rows and leave `row_versions` empty; old checkpoints load and resolve versions via the map until rows are rewritten. Adding enum variants is backward-compatible (old data uses the original variant indices). |
| Manifest | `manifest.json`, `manifest.hmac`, `.prev` copies, `src/manifest/schema.rs` | Versioned header (`magic` = `0x41454442`, `format_version`, `feature_flags`), `durable_seq`, `visible_seq`, active segment, checkpoint list, segment list, HMAC rules, reconstruction rules in strict/permissive recovery. Header fields are `#[serde(default)]`. The minimum supported `format_version` is **2** (`MIN_SUPPORTED_FORMAT_VERSION`); the v2 break sign-flips I256 keys in `EncodedKey` (`src/storage/encoded_key.rs`) so signed 256-bit keys sort correctly. Legacy manifests (no header, `magic == 0`) and any `format_version < 2` are refused on load and must be recreated (their keys use the old ordering); a wrong magic or a `format_version` newer than the build is likewise refused. None are reconstructed — this is an early-beta clean break with no in-place migration. Writers stamp the current header on every persisted manifest. **Recovery semantics:** the WAL is the source of truth for the durable boundary. `durable_seq` (which only advances at checkpoint/shutdown) is the checkpoint anchor, NOT a hard replay cap — recovery loads the manifest checkpoint, then replays every CRC + hash-chain-valid WAL frame forward, including the active segment to its physical end and any post-checkpoint segments not yet listed in the manifest. This guarantees a `DurabilityMode::Full` commit fsynced after the last checkpoint survives an abrupt restart. |
| Value store | `values.aedbdat`, `src/storage/value_store.rs` | Magic `AEDBVAL1`, append-only offsets, lengths, BLAKE3 refs, sync behavior for externally referenced values. |
| Page store | configured page-store files, `src/storage/page_store.rs` | Magic `AEDBPG1\0`, page size, frame header layout, page ID, length, hash, padding/stride. |
| KV segment store | `kv_segments/*.aedbkv`, `src/storage/kv_segment.rs` | Magic `AEDBKV1\0`, block layout, sorted key encoding, block metadata, bloom bits, SHA-256 metadata, publish/reclaim rules. |
| Backup manifest/archive | `backup_manifest.json`, `backup_manifest.hmac`, backup archive files, `src/backup/` | File list, hashes, chain metadata, target sequence/time semantics, archive encryption requirements. |

## Changes That Require Migration Tests

Add migration or compatibility tests when a change:

- changes any magic bytes, frame header, page header, compression/encryption envelope, or checksum/hash calculation;
- changes WAL payload serialization or replay semantics;
- changes mutation encoding in a way that old WAL data cannot replay directly;
- adds a non-defaulted field to manifest, checkpoint, backup manifest, KV segment metadata, or persisted refs;
- changes how `durable_seq`, `visible_seq`, active segment sequence, checkpoint sequence, or segment filenames are interpreted;
- changes value-store offsets/length/hash semantics;
- changes page-store page size/stride/header interpretation;
- changes KV segment sort order, block encoding, bloom filtering, tombstone handling, or segment publish/reclaim behavior;
- changes strict versus permissive recovery behavior;
- changes backup chain selection, verification, or restore target semantics.

## Required Test Coverage

Choose the smallest targeted test first, then broaden:

```bash
cargo test --test wal_frame_robustness
cargo test --test crash_matrix -- --test-threads=1
cargo test --test backup_restore -- --test-threads=1
```

For WAL/checkpoint/recovery changes, include:

```bash
cargo test --test crash_matrix crash_matrix_corrupt_wal_frame_fails_closed -- --test-threads=1
cargo test --test crash_matrix crash_matrix_after_checkpoint_before_manifest_respects_manifest_lower_bound -- --test-threads=1
cargo test --test crash_matrix crash_matrix_segment_deletion_breaks_hash_chain -- --test-threads=1
```

For backup/restore changes, include:

```bash
cargo test --test backup_restore strict_backup_chain_restore_succeeds_with_hash_chain_enforcement -- --test-threads=1
cargo test --test backup_restore strict_backup_chain_restore_rejects_tampered_incremental_segment -- --test-threads=1
```

For value/page/KV segment changes, add or update tests that create data with the old representation, reopen it with the new code, and verify point reads, scans, checkpoint, restore, and recovery as applicable.

## Release Checklist Item

Every release must answer this question:

> Did this release change any on-disk behavior or persisted metadata?

If yes, the release notes must identify the boundary, compatibility mode, migration path, and the exact tests or restore drill used to prove it.
