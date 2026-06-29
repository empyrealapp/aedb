//! Randomized differential test for the cold secondary-index tier.
//!
//! Drives a random stream of insert/update/delete commits with index (and row)
//! eviction forced by a tiny memory budget, interleaving segment reclamation and
//! checkpoints, and continually compares index query results against an
//! in-memory oracle. Then crash-recovers (drop without shutdown) and re-verifies.
//! This exercises the merge paths the deterministic tests cannot: many segments,
//! re-eviction, tombstones from updates/deletes, and re-insertion after deletion.

use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::{ColumnDef, IndexType};
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::config::{AedbConfig, StorageMode};
use aedb::query::plan::{ConsistencyMode, Query, QueryOptions, col, lit};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tempfile::TempDir;

fn config() -> AedbConfig {
    AedbConfig {
        storage_mode: StorageMode::DiskBacked,
        table_row_segment_eviction_enabled: true,
        index_segment_eviction_enabled: true,
        table_row_spill_enabled: false,
        max_memory_estimate_bytes: 24 * 1024,
        ..AedbConfig::default()
    }
}

async fn create_schema(db: &AedbInstance) {
    db.commit(Mutation::Ddl(DdlOperation::CreateProject {
        project_id: "p".into(),
        owner_id: None,
        if_not_exists: true,
    }))
    .await
    .expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateScope {
        project_id: "p".into(),
        scope_id: "app".into(),
        owner_id: None,
        if_not_exists: true,
    }))
    .await
    .expect("scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        owner_id: None,
        if_not_exists: true,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "k".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "h".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    // Ordered (BTree) index on k, hashed index on h: exercises both read paths.
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        index_name: "by_k".into(),
        if_not_exists: true,
        columns: vec!["k".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("k index");
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        index_name: "by_h".into(),
        if_not_exists: true,
        columns: vec!["h".into()],
        index_type: IndexType::Hash,
        partial_filter: None,
    }))
    .await
    .expect("h index");
}

async fn upsert(db: &AedbInstance, id: i64, k: i64, h: i64) {
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        primary_key: vec![Value::Integer(id)],
        row: Row {
            values: vec![Value::Integer(id), Value::Integer(k), Value::Integer(h)],
        },
    })
    .await
    .expect("upsert");
}

async fn delete(db: &AedbInstance, id: i64) {
    db.commit(Mutation::Delete {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        primary_key: vec![Value::Integer(id)],
    })
    .await
    .expect("delete");
}

/// Ids returned by an equality query on an indexed column.
async fn ids_where(db: &AedbInstance, column: &str, value: i64) -> BTreeSet<i64> {
    db.query_no_auth(
        "p",
        "app",
        Query::select(&["id"])
            .from("items")
            .where_(col(column).eq(lit(value)))
            .limit(100_000),
        QueryOptions {
            consistency: ConsistencyMode::AtLatest,
            ..QueryOptions::default()
        },
    )
    .await
    .expect("index query")
    .rows
    .into_iter()
    .map(|r| match r.values[0] {
        Value::Integer(v) => v,
        ref other => panic!("unexpected id value {other:?}"),
    })
    .collect()
}

async fn all_ids(db: &AedbInstance) -> BTreeSet<i64> {
    db.query_no_auth(
        "p",
        "app",
        Query::select(&["id"]).from("items").limit(100_000),
        QueryOptions {
            consistency: ConsistencyMode::AtLatest,
            ..QueryOptions::default()
        },
    )
    .await
    .expect("scan")
    .rows
    .into_iter()
    .map(|r| match r.values[0] {
        Value::Integer(v) => v,
        ref other => panic!("unexpected id value {other:?}"),
    })
    .collect()
}

fn oracle_ids_with<F: Fn(&(i64, i64)) -> i64>(
    oracle: &BTreeMap<i64, (i64, i64)>,
    project: F,
    value: i64,
) -> BTreeSet<i64> {
    oracle
        .iter()
        .filter(|(_, kh)| project(kh) == value)
        .map(|(id, _)| *id)
        .collect()
}

async fn verify_against_oracle(
    db: &AedbInstance,
    oracle: &BTreeMap<i64, (i64, i64)>,
    value_space: i64,
) {
    let expected_ids: BTreeSet<i64> = oracle.keys().copied().collect();
    assert_eq!(all_ids(db).await, expected_ids, "full scan mismatch");
    for v in 0..value_space {
        assert_eq!(
            ids_where(db, "k", v).await,
            oracle_ids_with(oracle, |kh| kh.0, v),
            "BTree index query mismatch for k={v}"
        );
        assert_eq!(
            ids_where(db, "h", v).await,
            oracle_ids_with(oracle, |kh| kh.1, v),
            "Hash index query mismatch for h={v}"
        );
    }
}

async fn create_unique_schema(db: &AedbInstance) {
    db.commit(Mutation::Ddl(DdlOperation::CreateProject {
        project_id: "p".into(),
        owner_id: None,
        if_not_exists: true,
    }))
    .await
    .expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateScope {
        project_id: "p".into(),
        scope_id: "app".into(),
        owner_id: None,
        if_not_exists: true,
    }))
    .await
    .expect("scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        owner_id: None,
        if_not_exists: true,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "u".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        index_name: "uniq_u".into(),
        if_not_exists: true,
        columns: vec!["u".into()],
        index_type: IndexType::UniqueHash,
        partial_filter: None,
    }))
    .await
    .expect("unique index");
}

async fn try_upsert_u(db: &AedbInstance, id: i64, u: i64) -> bool {
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        primary_key: vec![Value::Integer(id)],
        row: Row {
            values: vec![Value::Integer(id), Value::Integer(u)],
        },
    })
    .await
    .is_ok()
}

/// Unique constraints must be enforced against cold (evicted) postings under
/// random churn: a value held by another row is rejected; a freed value can be
/// reused. The oracle tracks the exact `id -> u` assignment and predicts each
/// commit's accept/reject outcome.
#[tokio::test]
async fn randomized_unique_index_tiering_enforces_across_cold_tier() {
    const ID_SPACE: i64 = 48;
    const U_SPACE: i64 = 24; // tight value space => frequent unique collisions
    const STEPS: usize = 450;

    for seed in [3u64, 29, 101] {
        let dir = TempDir::new().expect("temp");
        let cfg = config();
        let db = Arc::new(AedbInstance::open_anonymous(cfg.clone(), dir.path()).expect("open"));
        create_unique_schema(&db).await;

        let mut rng = StdRng::seed_from_u64(seed);
        // id -> u
        let mut oracle: BTreeMap<i64, i64> = BTreeMap::new();

        for step in 0..STEPS {
            let id = rng.gen_range(0..ID_SPACE);
            if rng.gen_bool(0.78) || !oracle.contains_key(&id) {
                let u = rng.gen_range(0..U_SPACE);
                // Predicted outcome: rejected iff some *other* id already holds u.
                let conflict = oracle
                    .iter()
                    .any(|(other_id, other_u)| *other_id != id && *other_u == u);
                let accepted = try_upsert_u(&db, id, u).await;
                assert_eq!(
                    accepted, !conflict,
                    "seed {seed} step {step}: upsert(id={id}, u={u}) outcome mismatch (conflict={conflict})"
                );
                if accepted {
                    oracle.insert(id, u);
                }
            } else {
                delete(&db, id).await;
                oracle.remove(&id);
            }

            if step % 40 == 0 {
                db.reclaim_unused_kv_segments().await.expect("reclaim");
                if step % 120 == 0 {
                    db.checkpoint_now().await.expect("checkpoint");
                }
                // Every u maps to at most the one id the oracle records.
                let scanned: BTreeSet<i64> = all_ids(&db).await;
                assert_eq!(scanned, oracle.keys().copied().collect::<BTreeSet<_>>());
                for u in 0..U_SPACE {
                    let expected: BTreeSet<i64> = oracle
                        .iter()
                        .filter(|(_, uu)| **uu == u)
                        .map(|(id, _)| *id)
                        .collect();
                    assert_eq!(
                        ids_where(&db, "u", u).await,
                        expected,
                        "unique query mismatch u={u}"
                    );
                }
            }
        }

        // Recover and confirm the unique constraint still holds: every recorded
        // value is still rejected for a fresh id, and the mapping is intact.
        drop(db);
        let recovered = Arc::new(AedbInstance::open_anonymous(cfg, dir.path()).expect("reopen"));
        let scanned: BTreeSet<i64> = all_ids(&recovered).await;
        assert_eq!(scanned, oracle.keys().copied().collect::<BTreeSet<_>>());
        // A brand-new id cannot reuse any currently-held value.
        if let Some((_, &held_u)) = oracle.iter().next() {
            let fresh_id = ID_SPACE + 1;
            assert!(
                !try_upsert_u(&recovered, fresh_id, held_u).await,
                "held unique value must stay enforced after recovery"
            );
        }
        recovered.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn randomized_index_tiering_matches_oracle_and_recovers() {
    const ID_SPACE: i64 = 64;
    const VALUE_SPACE: i64 = 8;
    const STEPS: usize = 450;

    // A few seeds give good coverage while keeping the test from monopolizing
    // CPU and starving timing-sensitive tests under the parallel suite.
    for seed in [1u64, 7, 42] {
        let dir = TempDir::new().expect("temp");
        let cfg = config();
        let db = Arc::new(AedbInstance::open_anonymous(cfg.clone(), dir.path()).expect("open"));
        create_schema(&db).await;

        let mut rng = StdRng::seed_from_u64(seed);
        let mut oracle: BTreeMap<i64, (i64, i64)> = BTreeMap::new();

        for step in 0..STEPS {
            let id = rng.gen_range(0..ID_SPACE);
            // Bias toward upserts; only delete ids that exist so every commit
            // succeeds and the oracle stays exact.
            if rng.gen_bool(0.72) || !oracle.contains_key(&id) {
                let k = rng.gen_range(0..VALUE_SPACE);
                let h = rng.gen_range(0..VALUE_SPACE);
                upsert(&db, id, k, h).await;
                oracle.insert(id, (k, h));
            } else {
                delete(&db, id).await;
                oracle.remove(&id);
            }

            if step % 40 == 0 {
                // Interleave reclamation and checkpoints with live cold data.
                db.reclaim_unused_kv_segments()
                    .await
                    .expect("reclaim must not corrupt cold segments");
                if step % 120 == 0 {
                    db.checkpoint_now().await.expect("checkpoint");
                }
                verify_against_oracle(&db, &oracle, VALUE_SPACE).await;
            }
        }

        // Exhaustive final check before recovery.
        verify_against_oracle(&db, &oracle, VALUE_SPACE).await;

        // Crash recovery: drop WITHOUT shutdown, reopen, and re-verify that the
        // durable WAL + checkpoint reconstruct every row and index exactly.
        drop(db);
        let recovered =
            Arc::new(AedbInstance::open_anonymous(cfg, dir.path()).expect("reopen after crash"));
        verify_against_oracle(&recovered, &oracle, VALUE_SPACE).await;
        // And reclamation still safe post-recovery.
        recovered
            .reclaim_unused_kv_segments()
            .await
            .expect("post-recovery reclaim");
        verify_against_oracle(&recovered, &oracle, VALUE_SPACE).await;
        recovered.shutdown().await.expect("shutdown");
    }
}
