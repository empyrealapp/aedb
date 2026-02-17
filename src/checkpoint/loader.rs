use crate::catalog::Catalog;
use crate::checkpoint::writer::CheckpointData;
use crate::commit::tx::{IdempotencyKey, IdempotencyRecord};
use crate::error::AedbError;
use crate::storage::keyspace::Keyspace;
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

pub fn load_checkpoint(
    path: &Path,
) -> Result<
    (
        Keyspace,
        Catalog,
        u64,
        HashMap<IdempotencyKey, IdempotencyRecord>,
    ),
    AedbError,
> {
    load_checkpoint_with_key(path, None)
}

pub fn load_checkpoint_with_key(
    path: &Path,
    encryption_key: Option<&[u8; 32]>,
) -> Result<
    (
        Keyspace,
        Catalog,
        u64,
        HashMap<IdempotencyKey, IdempotencyRecord>,
    ),
    AedbError,
> {
    let bytes = fs::read(path)?;
    let compressed = if bytes.starts_with(b"AEDBENC1") {
        let key = encryption_key
            .ok_or_else(|| AedbError::Validation("checkpoint requires key".into()))?;
        decrypt_checkpoint_payload(&bytes, key)?
    } else {
        if bytes.len() < 32 {
            return Err(AedbError::Decode("checkpoint too small".into()));
        }
        let (compressed, trailer_hash) = bytes.split_at(bytes.len() - 32);
        let actual = Sha256::digest(compressed);
        if actual.as_slice() != trailer_hash {
            return Err(AedbError::Validation("checkpoint hash mismatch".into()));
        }
        compressed.to_vec()
    };
    let decompressed = zstd::stream::decode_all(compressed.as_slice())
        .map_err(|e| AedbError::Io(std::io::Error::other(e.to_string())))?;
    let data: CheckpointData =
        rmp_serde::from_slice(&decompressed).map_err(|e| AedbError::Decode(e.to_string()))?;
    Ok((
        Keyspace {
            primary_index_backend: data.keyspace.primary_index_backend,
            namespaces: data.keyspace.namespaces,
            async_indexes: data.keyspace.async_indexes,
        },
        data.catalog,
        data.seq,
        data.idempotency,
    ))
}

fn decrypt_checkpoint_payload(bytes: &[u8], key: &[u8; 32]) -> Result<Vec<u8>, AedbError> {
    if bytes.len() < 8 + 12 {
        return Err(AedbError::Decode("encrypted checkpoint too small".into()));
    }
    let nonce = Nonce::from_slice(&bytes[8..20]);
    let ciphertext = &bytes[20..];
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| AedbError::Validation(format!("invalid encryption key: {e}")))?;
    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| AedbError::Validation(format!("checkpoint decryption failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::{load_checkpoint, load_checkpoint_with_key};
    use crate::catalog::Catalog;
    use crate::catalog::schema::ColumnDef;
    use crate::catalog::types::{ColumnType, Row, Value};
    use crate::checkpoint::writer::{write_checkpoint, write_checkpoint_with_key};
    use crate::storage::keyspace::Keyspace;
    use tempfile::tempdir;

    #[test]
    fn checkpoint_roundtrip_preserves_state() {
        let dir = tempdir().expect("temp");
        let mut keyspace = Keyspace::default();
        let mut catalog = Catalog::default();

        for p in ["p1", "p2", "p3"] {
            catalog.create_project(p).expect("project");
            for t in ["t1", "t2"] {
                catalog
                    .create_table(
                        p,
                        "app",
                        t,
                        vec![
                            ColumnDef {
                                name: "id".into(),
                                col_type: ColumnType::Integer,
                                nullable: false,
                            },
                            ColumnDef {
                                name: "name".into(),
                                col_type: ColumnType::Text,
                                nullable: false,
                            },
                        ],
                        vec!["id".into()],
                    )
                    .expect("table");
                for i in 0..1000 {
                    keyspace.upsert_row(
                        p,
                        "app",
                        t,
                        vec![Value::Integer(i)],
                        Row {
                            values: vec![
                                Value::Integer(i),
                                Value::Text(format!("{p}-{t}-{i}").into()),
                            ],
                        },
                        i as u64,
                    );
                }
            }
        }

        let snapshot = keyspace.snapshot();
        let meta = write_checkpoint(&snapshot, &catalog, 42, dir.path()).expect("checkpoint");
        let (loaded_ks, loaded_cat, seq, _) =
            load_checkpoint(&dir.path().join(meta.filename)).expect("load");
        assert_eq!(seq, 42);
        assert_eq!(loaded_ks, keyspace);
        assert_eq!(loaded_cat, catalog);
    }

    #[test]
    fn checkpoint_loader_rejects_hash_mismatch() {
        let dir = tempdir().expect("temp");
        let keyspace = Keyspace::default();
        let catalog = Catalog::default();
        let meta = write_checkpoint(&keyspace.snapshot(), &catalog, 1, dir.path()).expect("write");
        let path = dir.path().join(meta.filename);
        let mut bytes = std::fs::read(&path).expect("read");
        bytes[0] ^= 0xAA;
        std::fs::write(&path, bytes).expect("write");
        assert!(load_checkpoint(&path).is_err());
    }

    #[test]
    fn checkpoint_encryption_roundtrip() {
        let dir = tempdir().expect("temp");
        let mut keyspace = Keyspace::default();
        let mut catalog = Catalog::default();
        catalog.create_project("p").expect("project");
        keyspace.upsert_row(
            "p",
            "app",
            "t",
            vec![Value::Integer(1)],
            Row {
                values: vec![Value::Integer(1)],
            },
            1,
        );
        let key = [9u8; 32];
        let meta = write_checkpoint_with_key(
            &keyspace.snapshot(),
            &catalog,
            7,
            dir.path(),
            Some(&key),
            Some("k1".into()),
            std::collections::HashMap::new(),
        )
        .expect("write");
        assert_eq!(meta.key_id.as_deref(), Some("k1"));
        let path = dir.path().join(meta.filename);
        assert!(load_checkpoint(&path).is_err());
        let (ks, _, seq, _) = load_checkpoint_with_key(&path, Some(&key)).expect("load");
        assert_eq!(seq, 7);
        assert_eq!(ks, keyspace);
    }
}
