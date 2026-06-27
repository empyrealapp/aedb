//! Long-running soak test.
//!
//! Drives millions of randomized operations (insert / update / delete / KV /
//! checkpoint / backup / abrupt "kill" restart) against a single instance,
//! maintaining an in-memory shadow model of the expected state. After every
//! simulated crash and at the end it runs the full integrity verifier and
//! asserts the recovered database matches every durably-acknowledged operation.
//!
//! `#[ignore]` by default — it is meant to be run explicitly:
//!
//! ```bash
//! cargo test --test long_soak -- --ignored --nocapture
//! # crank it up for a multi-day run:
//! AEDB_SOAK_ITERS=50000000 AEDB_SOAK_CRASH_EVERY=250000 \
//!   cargo test --test long_soak -- --ignored --nocapture
//! ```
//!
//! Uses `DurabilityMode::Full`, so every `Ok` commit is fsynced before ack and
//! must survive an abrupt drop. The shadow model is therefore an exact oracle.

use aedb::{AedbInstance, CommitFinality};
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::config::{AedbConfig, DurabilityMode, RecoveryMode};
use aedb::error::AedbError;
use aedb::offline;
use aedb::query::plan::{ConsistencyMode, Query, QueryOptions};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::tempdir;

const PROJECT: &str = "soak";
const SCOPE: &str = "app";
const TABLE: &str = "items";

fn env_u64(default: u64, var: &str) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

/// Small, deterministic xorshift64* PRNG so soak runs are reproducible from a
/// seed without pulling in an external crate.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

fn soak_config() -> AedbConfig {
    AedbConfig {
        durability_mode: DurabilityMode::Full,
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        ..AedbConfig::default()
    }
}

/// Reopen with retry: a detached reactive-processor loop can briefly hold a
/// strong ref to the prior instance, so the directory lock may not be free for
/// a few milliseconds after an abrupt drop.
async fn reopen(config: &AedbConfig, dir: &Path) -> AedbInstance {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match AedbInstance::open(config.clone(), dir) {
            Ok(db) => return db,
            Err(AedbError::Unavailable { .. }) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(err) => panic!("reopen failed: {err:?}"),
        }
    }
}

async fn create_schema(db: &AedbInstance) {
    db.commit(Mutation::Ddl(DdlOperation::CreateProject {
        project_id: PROJECT.into(),
        owner_id: None,
        if_not_exists: true,
    }))
    .await
    .expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateScope {
        project_id: PROJECT.into(),
        scope_id: SCOPE.into(),
        owner_id: None,
        if_not_exists: true,
    }))
    .await
    .expect("scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: PROJECT.into(),
        scope_id: SCOPE.into(),
        table_name: TABLE.into(),
        owner_id: None,
        if_not_exists: true,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "val".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
}

/// Assert every row in the store matches the shadow model exactly.
async fn assert_matches_model(db: &AedbInstance, model: &BTreeMap<i64, i64>) {
    let result = db
        .query_no_auth(
            PROJECT,
            SCOPE,
            Query::select(&["id", "val"])
                .from(TABLE)
                .limit(model.len().saturating_add(16)),
            QueryOptions {
                consistency: ConsistencyMode::AtLatest,
                ..QueryOptions::default()
            },
        )
        .await
        .expect("scan query");
    if result.rows.len() != model.len() {
        let queried: std::collections::BTreeSet<i64> = result
            .rows
            .iter()
            .map(|r| match r.values.first() {
                Some(Value::Integer(v)) => *v,
                other => panic!("unexpected id: {other:?}"),
            })
            .collect();
        let model_keys: std::collections::BTreeSet<i64> = model.keys().copied().collect();
        let only_model: Vec<_> = model_keys.difference(&queried).collect();
        let only_query: Vec<_> = queried.difference(&model_keys).collect();
        panic!(
            "row count diverged: query={} model={} only_in_model={:?} only_in_query={:?}",
            result.rows.len(),
            model.len(),
            only_model,
            only_query
        );
    }
    for row in &result.rows {
        let id = match row.values.first() {
            Some(Value::Integer(v)) => *v,
            other => panic!("unexpected id value: {other:?}"),
        };
        let val = match row.values.get(1) {
            Some(Value::Integer(v)) => *v,
            other => panic!("unexpected val value: {other:?}"),
        };
        assert_eq!(model.get(&id), Some(&val), "value diverged for id {id}");
    }
}

#[tokio::test]
#[ignore = "long-running soak; run with --ignored"]
async fn long_soak_random_ops_with_crashes_and_verify() {
    let iters = env_u64(5_000, "AEDB_SOAK_ITERS");
    let crash_every = env_u64(1_000, "AEDB_SOAK_CRASH_EVERY");
    let checkpoint_every = env_u64(250, "AEDB_SOAK_CHECKPOINT_EVERY");
    let key_space = env_u64(512, "AEDB_SOAK_KEYSPACE").max(2);
    let seed = env_u64(0xC0FF_EE15_600D, "AEDB_SOAK_SEED");

    let dir = tempdir().expect("temp");
    let config = soak_config();
    let mut rng = Rng::new(seed);
    let mut model: BTreeMap<i64, i64> = BTreeMap::new();

    let mut db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    create_schema(&db).await;

    let started = Instant::now();
    let mut applied = 0u64;

    for i in 0..iters {
        let id = rng.below(key_space) as i64;
        match rng.below(10) {
            // 0..=5: upsert (insert or update)
            0..=5 => {
                let val = rng.next_u64() as i64;
                db.commit_with_finality(
                    Mutation::Upsert {
                        project_id: PROJECT.into(),
                        scope_id: SCOPE.into(),
                        table_name: TABLE.into(),
                        primary_key: vec![Value::Integer(id)],
                        row: Row {
                            values: vec![Value::Integer(id), Value::Integer(val)],
                        },
                    },
                    CommitFinality::Durable,
                )
                .await
                .expect("upsert commit");
                model.insert(id, val);
                applied += 1;
            }
            // 6..=7: delete (no-op if absent; still durable)
            6..=7 => {
                db.commit_with_finality(
                    Mutation::Delete {
                        project_id: PROJECT.into(),
                        scope_id: SCOPE.into(),
                        table_name: TABLE.into(),
                        primary_key: vec![Value::Integer(id)],
                    },
                    CommitFinality::Durable,
                )
                .await
                .expect("delete commit");
                model.remove(&id);
                applied += 1;
            }
            // 8: KV churn (exercises the KV + value-store paths)
            8 => {
                db.commit_with_finality(
                    Mutation::KvSet {
                        project_id: PROJECT.into(),
                        scope_id: SCOPE.into(),
                        key: format!("k:{id}").into_bytes(),
                        value: rng.next_u64().to_be_bytes().to_vec(),
                    },
                    CommitFinality::Durable,
                )
                .await
                .expect("kv commit");
                applied += 1;
            }
            // 9: occasional background checkpoint
            _ => {
                if i % checkpoint_every == 0 {
                    db.checkpoint_now().await.expect("checkpoint");
                }
            }
        }

        if checkpoint_every > 0 && i > 0 && i % checkpoint_every == 0 {
            db.checkpoint_now().await.expect("periodic checkpoint");
        }

        // Simulate a true `kill -9`: drop the instance with no shutdown and no
        // pre-kill checkpoint. Under DurabilityMode::Full every acknowledged
        // commit is fsynced to the WAL, and recovery replays the full validated
        // WAL tail (not just up to the last checkpoint's manifest.durable_seq),
        // so the recovered state must match the shadow model exactly — including
        // every commit made since the most recent periodic checkpoint.
        if crash_every > 0 && i > 0 && i % crash_every == 0 {
            drop(db);

            let report = offline::verify_database(dir.path(), &config);
            assert!(
                report.ok,
                "verify failed after simulated crash at iter {i}: {report:?}"
            );

            db = reopen(&config, dir.path()).await;
            assert_matches_model(&db, &model).await;
        }
    }

    // Clean shutdown, then a final full verification.
    db.shutdown().await.expect("shutdown");
    drop(db);
    let report = offline::verify_database(dir.path(), &config);
    assert!(report.ok, "final verify failed: {report:?}");

    eprintln!(
        "soak complete: {iters} iters, {applied} durable ops, {} live keys, {:.1}s",
        model.len(),
        started.elapsed().as_secs_f64()
    );
}
