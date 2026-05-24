use crate::catalog::Catalog;
use crate::catalog::schema::ColumnDef;
use crate::catalog::types::{ColumnType, Row, Value};
use crate::checkpoint::loader::{load_checkpoint, load_checkpoint_with_key};
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
        3,
    )
    .expect("write");
    assert_eq!(meta.key_id.as_deref(), Some("k1"));
    let path = dir.path().join(meta.filename);
    assert!(load_checkpoint(&path).is_err());
    let (ks, _, seq, _) = load_checkpoint_with_key(&path, Some(&key)).expect("load");
    assert_eq!(seq, 7);
    assert_eq!(ks, keyspace);
}

#[test]
fn encrypted_checkpoints_use_distinct_random_nonces() {
    let dir = tempdir().expect("temp");
    let keyspace = Keyspace::default();
    let catalog = Catalog::default();
    let key = [7u8; 32];

    let first = write_checkpoint_with_key(
        &keyspace.snapshot(),
        &catalog,
        42,
        dir.path(),
        Some(&key),
        Some("k1".into()),
        std::collections::HashMap::new(),
        3,
    )
    .expect("write first");
    let first_bytes = std::fs::read(dir.path().join(&first.filename)).expect("read first");
    let second = write_checkpoint_with_key(
        &keyspace.snapshot(),
        &catalog,
        42,
        dir.path(),
        Some(&key),
        Some("k1".into()),
        std::collections::HashMap::new(),
        3,
    )
    .expect("write second");
    let second_bytes = std::fs::read(dir.path().join(second.filename)).expect("read second");

    assert!(first_bytes.starts_with(b"AEDBENC1"));
    assert!(second_bytes.starts_with(b"AEDBENC1"));
    assert_ne!(
        &first_bytes[8..20],
        &second_bytes[8..20],
        "encrypted checkpoints must not reuse nonces"
    );
}
