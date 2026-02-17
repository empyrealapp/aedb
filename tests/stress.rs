use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::{ColumnDef, IndexType};
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::tx::{
    ReadKey, ReadSet, ReadSetEntry, TransactionEnvelope, WriteClass, WriteIntent,
};
use aedb::commit::validation::Mutation;
use aedb::config::{AedbConfig, DurabilityMode, RecoveryMode};
use aedb::error::AedbError;
use aedb::permission::{CallerContext, Permission};
use aedb::query::plan::{ConsistencyMode, Expr, Query, QueryOptions, col, lit};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tempfile::tempdir;

fn stress_scale() -> usize {
    std::env::var("AEDB_STRESS_SCALE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1)
}

fn scaled(base: usize) -> usize {
    base.saturating_mul(stress_scale())
}

fn stress_config() -> AedbConfig {
    AedbConfig {
        durability_mode: DurabilityMode::OsBuffered,
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        ..AedbConfig::default()
    }
}

fn env_or(default: usize, var: &str) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn scoped_tps_projects() -> usize {
    env_or(scaled(4), "AEDB_SCOPED_TPS_PROJECTS")
}

fn scoped_tps_workers_per_scope() -> usize {
    env_or(2, "AEDB_SCOPED_TPS_WORKERS_PER_SCOPE")
}

fn scoped_tps_transfers_per_worker() -> usize {
    env_or(scaled(500), "AEDB_SCOPED_TPS_TRANSFERS_PER_WORKER")
}

fn scoped_tps_accounts_per_scope() -> usize {
    env_or(32, "AEDB_SCOPED_TPS_ACCOUNTS_PER_SCOPE")
}

fn scoped_tps_max_retries() -> usize {
    env_or(32, "AEDB_SCOPED_TPS_MAX_RETRIES")
}

fn mixed_projects() -> usize {
    env_or(scaled(4), "AEDB_MIXED_PROJECTS")
}

fn mixed_writer_workers_per_scope() -> usize {
    std::env::var("AEDB_MIXED_WRITERS_PER_SCOPE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(2)
}

fn mixed_reader_workers_per_scope() -> usize {
    std::env::var("AEDB_MIXED_READERS_PER_SCOPE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(2)
}

fn mixed_seed_rows_per_scope() -> usize {
    env_or(scaled(2_000), "AEDB_MIXED_SEED_ROWS_PER_SCOPE")
}

fn mixed_run_seconds() -> usize {
    env_or(10, "AEDB_MIXED_RUN_SECONDS")
}

fn realistic_projects() -> usize {
    env_or(1, "AEDB_REALISTIC_PROJECTS")
}

fn realistic_scopes_per_project() -> usize {
    env_or(8, "AEDB_REALISTIC_SCOPES_PER_PROJECT")
}

fn realistic_writer_workers_per_scope() -> usize {
    env_or(1, "AEDB_REALISTIC_WRITERS_PER_SCOPE")
}

fn realistic_reader_workers_per_scope() -> usize {
    env_or(1, "AEDB_REALISTIC_READERS_PER_SCOPE")
}

fn realistic_seed_rows_per_scope() -> usize {
    env_or(scaled(1_000), "AEDB_REALISTIC_SEED_ROWS_PER_SCOPE")
}

fn realistic_seed_kv_keys_per_scope() -> usize {
    env_or(scaled(1_000), "AEDB_REALISTIC_SEED_KV_KEYS_PER_SCOPE")
}

fn realistic_run_seconds() -> usize {
    env_or(15, "AEDB_REALISTIC_RUN_SECONDS")
}

fn to_i64(value: usize) -> i64 {
    i64::try_from(value).expect("value must fit in i64")
}

fn read_i64(row: &Row, idx: usize) -> i64 {
    match &row.values[idx] {
        Value::Integer(v) => *v,
        v => panic!("expected integer at column {idx}, got {v:?}"),
    }
}

async fn read_account_balance_at_seq(
    db: &AedbInstance,
    project_id: &str,
    account_id: i64,
    seq: u64,
) -> i64 {
    let result = db
        .query_with_options(
            project_id,
            "app",
            Query::select(&["id", "balance"])
                .from("accounts")
                .where_(col("id").eq(lit(account_id)))
                .limit(1),
            QueryOptions {
                consistency: ConsistencyMode::AtSeq(seq),
                ..QueryOptions::default()
            },
        )
        .await
        .expect("account query");
    assert_eq!(result.rows.len(), 1, "account row must exist");
    read_i64(&result.rows[0], 1)
}

async fn count_rows_bounded(
    db: &AedbInstance,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    expected_max_rows: usize,
) -> usize {
    db.query(
        project_id,
        scope_id,
        Query::select(&["*"])
            .from(table_name)
            .limit(expected_max_rows.saturating_add(1)),
    )
    .await
    .expect("count query")
    .rows
    .len()
}

#[tokio::test]
async fn arcana_l1_balance_conservation_under_load() {
    const INITIAL_BALANCE: i64 = 2_000;
    const ACCOUNTS: usize = 12;
    const WORKERS: usize = 4;
    const TRANSFERS_PER_WORKER: usize = 120;
    const MAX_RETRIES: usize = 12;

    let dir = tempdir().expect("temp");
    let db = Arc::new(AedbInstance::open(stress_config(), dir.path()).expect("open"));
    db.create_project("l1").await.expect("project");

    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "l1".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "balance".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("accounts table");

    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "l1".into(),
        scope_id: "app".into(),
        table_name: "transfers".into(),
        columns: vec![
            ColumnDef {
                name: "tx_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "from_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "to_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "amount".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["tx_id".into()],
    }))
    .await
    .expect("transfers table");

    for i in 0..ACCOUNTS {
        db.commit(Mutation::Upsert {
            project_id: "l1".into(),
            scope_id: "app".into(),
            table_name: "accounts".into(),
            primary_key: vec![Value::Integer(to_i64(i))],
            row: Row {
                values: vec![Value::Integer(to_i64(i)), Value::Integer(INITIAL_BALANCE)],
            },
        })
        .await
        .expect("seed account");
    }

    let committed = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::new();
    for worker_idx in 0..WORKERS {
        let db = Arc::clone(&db);
        let committed = Arc::clone(&committed);
        workers.push(tokio::spawn(async move {
            for transfer_idx in 0..TRANSFERS_PER_WORKER {
                for retry in 0..MAX_RETRIES {
                    let seq = db
                        .snapshot_probe(ConsistencyMode::AtLatest)
                        .await
                        .expect("snapshot seq");
                    let from = (worker_idx + transfer_idx + retry) % ACCOUNTS;
                    let to = (from + 1 + worker_idx) % ACCOUNTS;
                    let amount = 1 + ((transfer_idx + worker_idx) % 7);

                    let from_id = to_i64(from);
                    let to_id = to_i64(to);
                    let amount_i64 = to_i64(amount);
                    let from_balance = read_account_balance_at_seq(&db, "l1", from_id, seq).await;
                    if from_balance < amount_i64 {
                        break;
                    }
                    let to_balance = read_account_balance_at_seq(&db, "l1", to_id, seq).await;

                    let tx_id_u64 = ((worker_idx as u64) << 32)
                        | (transfer_idx as u64 * (MAX_RETRIES as u64 + 1) + retry as u64);
                    let tx_id = i64::try_from(tx_id_u64).expect("tx id fits i64");
                    let envelope = TransactionEnvelope {
                        caller: None,
                        idempotency_key: None,
                        write_class: WriteClass::Standard,
                        assertions: Vec::new(),
                        read_set: ReadSet {
                            points: vec![
                                ReadSetEntry {
                                    key: ReadKey::TableRow {
                                        project_id: "l1".into(),
                                        scope_id: "app".into(),
                                        table_name: "accounts".into(),
                                        primary_key: vec![Value::Integer(from_id)],
                                    },
                                    version_at_read: seq,
                                },
                                ReadSetEntry {
                                    key: ReadKey::TableRow {
                                        project_id: "l1".into(),
                                        scope_id: "app".into(),
                                        table_name: "accounts".into(),
                                        primary_key: vec![Value::Integer(to_id)],
                                    },
                                    version_at_read: seq,
                                },
                            ],
                            ranges: Vec::new(),
                        },
                        write_intent: WriteIntent {
                            mutations: vec![
                                Mutation::Upsert {
                                    project_id: "l1".into(),
                                    scope_id: "app".into(),
                                    table_name: "accounts".into(),
                                    primary_key: vec![Value::Integer(from_id)],
                                    row: Row {
                                        values: vec![
                                            Value::Integer(from_id),
                                            Value::Integer(from_balance - amount_i64),
                                        ],
                                    },
                                },
                                Mutation::Upsert {
                                    project_id: "l1".into(),
                                    scope_id: "app".into(),
                                    table_name: "accounts".into(),
                                    primary_key: vec![Value::Integer(to_id)],
                                    row: Row {
                                        values: vec![
                                            Value::Integer(to_id),
                                            Value::Integer(to_balance + amount_i64),
                                        ],
                                    },
                                },
                                Mutation::Upsert {
                                    project_id: "l1".into(),
                                    scope_id: "app".into(),
                                    table_name: "transfers".into(),
                                    primary_key: vec![Value::Integer(tx_id)],
                                    row: Row {
                                        values: vec![
                                            Value::Integer(tx_id),
                                            Value::Integer(from_id),
                                            Value::Integer(to_id),
                                            Value::Integer(amount_i64),
                                        ],
                                    },
                                },
                            ],
                        },
                        base_seq: seq,
                    };
                    match db.commit_envelope(envelope).await {
                        Ok(_) => {
                            committed.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Err(AedbError::Conflict(_)) => {}
                        Err(AedbError::Underflow) => break,
                        Err(e) => panic!("unexpected transfer error: {e}"),
                    }
                }
            }
        }));
    }
    for worker in workers {
        worker.await.expect("worker join");
    }

    let accounts = db
        .query(
            "l1",
            "app",
            Query::select(&["id", "balance"])
                .from("accounts")
                .limit(ACCOUNTS),
        )
        .await
        .expect("accounts query");
    assert_eq!(accounts.rows.len(), ACCOUNTS, "all accounts should remain");
    let expected_total = INITIAL_BALANCE * to_i64(ACCOUNTS);
    let mut total = 0i64;
    for row in &accounts.rows {
        let balance = read_i64(row, 1);
        assert!(balance >= 0, "negative balance");
        total += balance;
    }
    assert_eq!(total, expected_total, "balance conservation invariant");

    let transfer_rows = count_rows_bounded(
        &db,
        "l1",
        "app",
        "transfers",
        WORKERS.saturating_mul(TRANSFERS_PER_WORKER),
    )
    .await as u64;
    assert_eq!(
        transfer_rows,
        committed.load(Ordering::Relaxed),
        "transfer ledger must match committed writes"
    );
}

#[tokio::test]
#[ignore = "long-running stress test"]
async fn stress_write_throughput() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(stress_config(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "events".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "value".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    for i in 0..scaled(10_000) {
        let id = i as i64;
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "events".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row {
                values: vec![Value::Integer(id), Value::Integer(id % 100)],
            },
        })
        .await
        .expect("commit");
    }
}

#[tokio::test]
#[ignore = "long-running stress test"]
async fn stress_read_under_write() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(stress_config(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "age".into(),
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
        table_name: "users".into(),
        index_name: "by_age".into(),
        if_not_exists: false,
        columns: vec!["age".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("index");

    let writer = {
        let db = db;
        tokio::spawn(async move {
            for i in 0..scaled(5_000) {
                let id = i as i64;
                db.commit(Mutation::Upsert {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    table_name: "users".into(),
                    primary_key: vec![Value::Integer(id)],
                    row: Row {
                        values: vec![Value::Integer(id), Value::Integer(id % 80)],
                    },
                })
                .await
                .expect("write");
            }
            db
        })
    };

    let db = writer.await.expect("join");
    for _ in 0..100 {
        let _ = db
            .query(
                "p",
                "app",
                Query::select(&["*"]).from("users").where_(Expr::Between(
                    "age".into(),
                    Value::Integer(30),
                    Value::Integer(40),
                )),
            )
            .await
            .expect("query");
    }
}

#[tokio::test]
#[ignore = "long-running stress test"]
async fn stress_multi_project_isolation() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(stress_config(), dir.path()).expect("open");

    for p in 0..scaled(5) {
        let project = format!("p{p}");
        db.create_project(&project).await.expect("project");
        for t in 0..3 {
            let table = format!("t{t}");
            db.commit(Mutation::Ddl(DdlOperation::CreateTable {
                owner_id: None,
                if_not_exists: false,
                project_id: project.clone(),
                scope_id: "app".into(),
                table_name: table,
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "value".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                ],
                primary_key: vec!["id".into()],
            }))
            .await
            .expect("table");
        }
    }

    for p in 0..scaled(5) {
        let project = format!("p{p}");
        for i in 0..scaled(500) {
            let id = i as i64;
            db.commit(Mutation::Upsert {
                project_id: project.clone(),
                scope_id: "app".into(),
                table_name: "t0".into(),
                primary_key: vec![Value::Integer(id)],
                row: Row {
                    values: vec![Value::Integer(id), Value::Integer(p as i64)],
                },
            })
            .await
            .expect("insert");
        }
    }

    for p in 0..scaled(5) {
        let project = format!("p{p}");
        let result = db
            .query(
                &project,
                "app",
                Query::select(&["*"])
                    .from("t0")
                    .where_(Expr::Eq("value".into(), Value::Integer(p as i64)))
                    .limit(scaled(500)),
            )
            .await
            .expect("query");
        assert_eq!(result.rows.len(), scaled(500));
    }
}

#[tokio::test]
#[ignore = "long-running stress test"]
async fn stress_large_state_checkpoint_recovery() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(stress_config(), dir.path()).expect("open");
    db.create_project("bulk").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "bulk".into(),
        scope_id: "app".into(),
        table_name: "payloads".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "blob".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");

    let payload = "x".repeat(1024);
    for i in 0..scaled(20_000) {
        let id = i as i64;
        db.commit(Mutation::Upsert {
            project_id: "bulk".into(),
            scope_id: "app".into(),
            table_name: "payloads".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row {
                values: vec![Value::Integer(id), Value::Text(payload.clone().into())],
            },
        })
        .await
        .expect("insert");
    }
    db.shutdown().await.expect("shutdown");

    let reopened = AedbInstance::open(stress_config(), dir.path()).expect("reopen");
    let rows = reopened
        .query(
            "bulk",
            "app",
            Query::select(&["*"]).from("payloads").limit(10),
        )
        .await
        .expect("query");
    assert_eq!(rows.rows.len(), 10);
}

#[tokio::test]
#[ignore = "long-running scoped TPS + invariants benchmark"]
async fn stress_effective_tps_independent_scopes() {
    const INITIAL_BALANCE: i64 = 10_000;
    const PROJECT_PREFIX: &str = "scopebench";
    let project_count = scoped_tps_projects();
    let workers_per_scope = scoped_tps_workers_per_scope();
    let transfers_per_worker = scoped_tps_transfers_per_worker();
    let accounts_per_scope = scoped_tps_accounts_per_scope();
    let max_retries = scoped_tps_max_retries();
    assert!(
        accounts_per_scope >= 2,
        "need at least 2 accounts per scope"
    );

    let dir = tempdir().expect("temp");
    let db = Arc::new(AedbInstance::open(stress_config(), dir.path()).expect("open"));

    for project_idx in 0..project_count {
        let project_id = format!("{PROJECT_PREFIX}-{project_idx}");
        db.create_project(&project_id).await.expect("project");
        db.commit(Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: project_id.clone(),
            scope_id: "app".into(),
            table_name: "accounts".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "balance".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
        }))
        .await
        .expect("accounts table");
        db.commit(Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: project_id.clone(),
            scope_id: "app".into(),
            table_name: "transfers".into(),
            columns: vec![
                ColumnDef {
                    name: "tx_id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "from_id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "to_id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "amount".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
            ],
            primary_key: vec!["tx_id".into()],
        }))
        .await
        .expect("transfers table");

        for account_idx in 0..accounts_per_scope {
            db.commit(Mutation::Upsert {
                project_id: project_id.clone(),
                scope_id: "app".into(),
                table_name: "accounts".into(),
                primary_key: vec![Value::Integer(to_i64(account_idx))],
                row: Row {
                    values: vec![
                        Value::Integer(to_i64(account_idx)),
                        Value::Integer(INITIAL_BALANCE),
                    ],
                },
            })
            .await
            .expect("seed account");
        }
    }

    let committed_total = Arc::new(AtomicU64::new(0));
    let conflict_total = Arc::new(AtomicU64::new(0));
    let insufficient_total = Arc::new(AtomicU64::new(0));
    let committed_by_scope = Arc::new(
        (0..project_count)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );

    let started = Instant::now();
    let mut tasks = Vec::new();
    for project_idx in 0..project_count {
        for worker_idx in 0..workers_per_scope {
            let db = Arc::clone(&db);
            let committed_total = Arc::clone(&committed_total);
            let conflict_total = Arc::clone(&conflict_total);
            let insufficient_total = Arc::clone(&insufficient_total);
            let committed_by_scope = Arc::clone(&committed_by_scope);
            tasks.push(tokio::spawn(async move {
                let project_id = format!("{PROJECT_PREFIX}-{project_idx}");
                for transfer_idx in 0..transfers_per_worker {
                    for retry in 0..max_retries {
                        let seq = db
                            .snapshot_probe(ConsistencyMode::AtLatest)
                            .await
                            .expect("snapshot seq");
                        let from = (worker_idx + transfer_idx + retry) % accounts_per_scope;
                        let to = (from + 1 + worker_idx) % accounts_per_scope;
                        let amount = 1 + ((transfer_idx + worker_idx) % 5);

                        let from_id = to_i64(from);
                        let to_id = to_i64(to);
                        let amount_i64 = to_i64(amount);
                        let from_balance =
                            read_account_balance_at_seq(&db, &project_id, from_id, seq).await;
                        if from_balance < amount_i64 {
                            insufficient_total.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        let to_balance =
                            read_account_balance_at_seq(&db, &project_id, to_id, seq).await;

                        let tx_id_u64 = ((project_idx as u64) << 42)
                            | ((worker_idx as u64) << 32)
                            | (transfer_idx as u64 * (max_retries as u64 + 1) + retry as u64);
                        let tx_id = i64::try_from(tx_id_u64).expect("tx id fits i64");
                        let envelope = TransactionEnvelope {
                            caller: None,
                            idempotency_key: None,
                            write_class: WriteClass::Standard,
                            assertions: Vec::new(),
                            read_set: ReadSet {
                                points: vec![
                                    ReadSetEntry {
                                        key: ReadKey::TableRow {
                                            project_id: project_id.clone(),
                                            scope_id: "app".into(),
                                            table_name: "accounts".into(),
                                            primary_key: vec![Value::Integer(from_id)],
                                        },
                                        version_at_read: seq,
                                    },
                                    ReadSetEntry {
                                        key: ReadKey::TableRow {
                                            project_id: project_id.clone(),
                                            scope_id: "app".into(),
                                            table_name: "accounts".into(),
                                            primary_key: vec![Value::Integer(to_id)],
                                        },
                                        version_at_read: seq,
                                    },
                                ],
                                ranges: Vec::new(),
                            },
                            write_intent: WriteIntent {
                                mutations: vec![
                                    Mutation::Upsert {
                                        project_id: project_id.clone(),
                                        scope_id: "app".into(),
                                        table_name: "accounts".into(),
                                        primary_key: vec![Value::Integer(from_id)],
                                        row: Row {
                                            values: vec![
                                                Value::Integer(from_id),
                                                Value::Integer(from_balance - amount_i64),
                                            ],
                                        },
                                    },
                                    Mutation::Upsert {
                                        project_id: project_id.clone(),
                                        scope_id: "app".into(),
                                        table_name: "accounts".into(),
                                        primary_key: vec![Value::Integer(to_id)],
                                        row: Row {
                                            values: vec![
                                                Value::Integer(to_id),
                                                Value::Integer(to_balance + amount_i64),
                                            ],
                                        },
                                    },
                                    Mutation::Upsert {
                                        project_id: project_id.clone(),
                                        scope_id: "app".into(),
                                        table_name: "transfers".into(),
                                        primary_key: vec![Value::Integer(tx_id)],
                                        row: Row {
                                            values: vec![
                                                Value::Integer(tx_id),
                                                Value::Integer(from_id),
                                                Value::Integer(to_id),
                                                Value::Integer(amount_i64),
                                            ],
                                        },
                                    },
                                ],
                            },
                            base_seq: seq,
                        };
                        match db.commit_envelope(envelope).await {
                            Ok(_) => {
                                committed_total.fetch_add(1, Ordering::Relaxed);
                                committed_by_scope[project_idx].fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            Err(AedbError::Conflict(_)) => {
                                conflict_total.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(AedbError::Underflow) => {
                                insufficient_total.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            Err(e) => panic!("unexpected transfer error: {e}"),
                        }
                    }
                }
            }));
        }
    }
    for task in tasks {
        task.await.expect("worker join");
    }
    let elapsed = started.elapsed();

    let committed = committed_total.load(Ordering::Relaxed);
    let conflicts = conflict_total.load(Ordering::Relaxed);
    let insufficient = insufficient_total.load(Ordering::Relaxed);
    let effective_tps = committed as f64 / elapsed.as_secs_f64().max(0.001);

    let expected_total_per_scope = INITIAL_BALANCE * to_i64(accounts_per_scope);
    for project_idx in 0..project_count {
        let project_id = format!("{PROJECT_PREFIX}-{project_idx}");
        let accounts = db
            .query(
                &project_id,
                "app",
                Query::select(&["id", "balance"])
                    .from("accounts")
                    .limit(accounts_per_scope),
            )
            .await
            .expect("accounts verify query");
        assert_eq!(accounts.rows.len(), accounts_per_scope, "account row count");
        let mut total = 0i64;
        for row in &accounts.rows {
            let balance = read_i64(row, 1);
            assert!(balance >= 0, "negative balance in {project_id}");
            total += balance;
        }
        assert_eq!(
            total, expected_total_per_scope,
            "balance conservation failed in {project_id}"
        );

        let tx_rows = count_rows_bounded(
            &db,
            &project_id,
            "app",
            "transfers",
            workers_per_scope.saturating_mul(transfers_per_worker),
        )
        .await as u64;
        let expected = committed_by_scope[project_idx].load(Ordering::Relaxed);
        assert_eq!(
            tx_rows, expected,
            "missing or duplicate transfer rows in {project_id}"
        );
    }

    db.shutdown().await.expect("shutdown");
    drop(db);

    let reopened = AedbInstance::open(stress_config(), dir.path()).expect("reopen");
    for project_idx in 0..project_count {
        let project_id = format!("{PROJECT_PREFIX}-{project_idx}");
        let accounts = reopened
            .query(
                &project_id,
                "app",
                Query::select(&["id", "balance"])
                    .from("accounts")
                    .limit(accounts_per_scope),
            )
            .await
            .expect("reopen accounts verify");
        let mut total = 0i64;
        for row in &accounts.rows {
            total += read_i64(row, 1);
        }
        assert_eq!(total, expected_total_per_scope, "reopen balance check");
        let tx_rows = count_rows_bounded(
            &reopened,
            &project_id,
            "app",
            "transfers",
            workers_per_scope.saturating_mul(transfers_per_worker),
        )
        .await;
        let expected = committed_by_scope[project_idx].load(Ordering::Relaxed) as usize;
        assert_eq!(tx_rows, expected, "reopen tx count");
    }

    eprintln!(
        "scoped_tps: projects={} workers_per_scope={} transfers_per_worker={} committed={} conflicts={} insufficient={} elapsed_ms={} effective_tps={:.2}",
        project_count,
        workers_per_scope,
        transfers_per_worker,
        committed,
        conflicts,
        insufficient,
        elapsed.as_millis(),
        effective_tps
    );
    assert!(committed > 0, "expected at least one committed transfer");
}

#[tokio::test]
#[ignore = "long-running mixed insert/update/read/query throughput benchmark"]
async fn stress_mixed_ops_independent_scopes() {
    const PROJECT_PREFIX: &str = "mixbench";
    let project_count = mixed_projects();
    let writers_per_scope = mixed_writer_workers_per_scope();
    let readers_per_scope = mixed_reader_workers_per_scope();
    let seed_rows = mixed_seed_rows_per_scope();
    let run_secs = mixed_run_seconds();
    assert!(run_secs > 0, "run seconds must be > 0");

    let mut config = stress_config();
    config.max_scan_rows = 1_000_000;

    let dir = tempdir().expect("temp");
    let db = Arc::new(AedbInstance::open(config.clone(), dir.path()).expect("open"));

    for project_idx in 0..project_count {
        let project_id = format!("{PROJECT_PREFIX}-{project_idx}");
        db.create_project(&project_id).await.expect("project");
        db.commit(Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: project_id.clone(),
            scope_id: "app".into(),
            table_name: "items".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "value".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "tag".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
        }))
        .await
        .expect("items table");
        db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
            project_id: project_id.clone(),
            scope_id: "app".into(),
            table_name: "items".into(),
            index_name: "by_tag".into(),
            if_not_exists: false,
            columns: vec!["tag".into()],
            index_type: IndexType::BTree,
            partial_filter: None,
        }))
        .await
        .expect("items index");

        for i in 0..seed_rows {
            let id = to_i64(i);
            db.commit(Mutation::Upsert {
                project_id: project_id.clone(),
                scope_id: "app".into(),
                table_name: "items".into(),
                primary_key: vec![Value::Integer(id)],
                row: Row {
                    values: vec![
                        Value::Integer(id),
                        Value::Integer(id % 1_000),
                        Value::Integer(id % 100),
                    ],
                },
            })
            .await
            .expect("seed row");
        }
    }

    let inserts = Arc::new(AtomicU64::new(0));
    let updates = Arc::new(AtomicU64::new(0));
    let point_reads = Arc::new(AtomicU64::new(0));
    let index_queries = Arc::new(AtomicU64::new(0));
    let next_insert_by_scope = Arc::new(
        (0..project_count)
            .map(|_| AtomicU64::new(seed_rows as u64))
            .collect::<Vec<_>>(),
    );
    let inserts_by_scope = Arc::new(
        (0..project_count)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let stop_at = Instant::now() + Duration::from_secs(run_secs as u64);

    let mut tasks = Vec::new();
    for project_idx in 0..project_count {
        for worker_idx in 0..writers_per_scope {
            let db = Arc::clone(&db);
            let inserts = Arc::clone(&inserts);
            let updates = Arc::clone(&updates);
            let stop = Arc::clone(&stop);
            let next_insert_by_scope = Arc::clone(&next_insert_by_scope);
            let inserts_by_scope = Arc::clone(&inserts_by_scope);
            tasks.push(tokio::spawn(async move {
                let mut state = ((project_idx as u64 + 1) << 32) ^ worker_idx as u64 ^ 0x9E37;
                let project_id = format!("{PROJECT_PREFIX}-{project_idx}");
                while !stop.load(Ordering::Relaxed) {
                    state = state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    let do_insert = (state & 0b11) == 0;
                    if do_insert {
                        let id_u64 =
                            next_insert_by_scope[project_idx].fetch_add(1, Ordering::Relaxed);
                        let id = i64::try_from(id_u64).expect("id fits i64");
                        db.commit(Mutation::Upsert {
                            project_id: project_id.clone(),
                            scope_id: "app".into(),
                            table_name: "items".into(),
                            primary_key: vec![Value::Integer(id)],
                            row: Row {
                                values: vec![
                                    Value::Integer(id),
                                    Value::Integer((id % 10_000).abs()),
                                    Value::Integer((id % 100).abs()),
                                ],
                            },
                        })
                        .await
                        .expect("insert upsert");
                        inserts.fetch_add(1, Ordering::Relaxed);
                        inserts_by_scope[project_idx].fetch_add(1, Ordering::Relaxed);
                    } else {
                        let max_id = next_insert_by_scope[project_idx]
                            .load(Ordering::Relaxed)
                            .max(1);
                        let id = i64::try_from(state % max_id).expect("id fits");
                        db.commit(Mutation::Upsert {
                            project_id: project_id.clone(),
                            scope_id: "app".into(),
                            table_name: "items".into(),
                            primary_key: vec![Value::Integer(id)],
                            row: Row {
                                values: vec![
                                    Value::Integer(id),
                                    Value::Integer((state % 10_000) as i64),
                                    Value::Integer((state % 100) as i64),
                                ],
                            },
                        })
                        .await
                        .expect("update upsert");
                        updates.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for worker_idx in 0..readers_per_scope {
            let db = Arc::clone(&db);
            let point_reads = Arc::clone(&point_reads);
            let index_queries = Arc::clone(&index_queries);
            let stop = Arc::clone(&stop);
            let next_insert_by_scope = Arc::clone(&next_insert_by_scope);
            tasks.push(tokio::spawn(async move {
                let mut state =
                    ((project_idx as u64 + 3) << 28) ^ worker_idx as u64 ^ 0x517c_c1b7_2722_0a95;
                let project_id = format!("{PROJECT_PREFIX}-{project_idx}");
                while !stop.load(Ordering::Relaxed) {
                    state = state
                        .wrapping_mul(2862933555777941757)
                        .wrapping_add(3037000493);

                    let max_id = next_insert_by_scope[project_idx]
                        .load(Ordering::Relaxed)
                        .max(1);
                    let id = i64::try_from(state % max_id).expect("id fits");
                    let point = db
                        .query(
                            &project_id,
                            "app",
                            Query::select(&["id", "value"])
                                .from("items")
                                .where_(col("id").eq(lit(id)))
                                .limit(1),
                        )
                        .await
                        .expect("point query");
                    assert!(point.rows.len() <= 1, "point query returned >1 row");
                    point_reads.fetch_add(1, Ordering::Relaxed);

                    let tag = i64::try_from((state >> 8) % 100).expect("tag fits");
                    let _ = db
                        .query(
                            &project_id,
                            "app",
                            Query::select(&["id", "tag"])
                                .from("items")
                                .where_(col("tag").eq(lit(tag)))
                                .limit(20),
                        )
                        .await
                        .expect("index query");
                    index_queries.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
    }

    while Instant::now() < stop_at {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    stop.store(true, Ordering::Relaxed);
    for task in tasks {
        task.await.expect("mixed worker join");
    }

    let elapsed = Duration::from_secs(run_secs as u64);
    let inserts_n = inserts.load(Ordering::Relaxed);
    let updates_n = updates.load(Ordering::Relaxed);
    let point_reads_n = point_reads.load(Ordering::Relaxed);
    let index_queries_n = index_queries.load(Ordering::Relaxed);
    let write_ops = inserts_n + updates_n;
    let read_ops = point_reads_n + index_queries_n;
    let total_ops = write_ops + read_ops;
    let secs = elapsed.as_secs_f64().max(0.001);
    let write_tps = write_ops as f64 / secs;
    let read_tps = read_ops as f64 / secs;
    let total_ops_tps = total_ops as f64 / secs;

    for project_idx in 0..project_count {
        let project_id = format!("{PROJECT_PREFIX}-{project_idx}");
        let expected_rows =
            seed_rows + inserts_by_scope[project_idx].load(Ordering::Relaxed) as usize;
        let rows = db
            .query(
                &project_id,
                "app",
                Query::select(&["id"])
                    .from("items")
                    .limit(expected_rows.saturating_add(1)),
            )
            .await
            .expect("mixed verify query")
            .rows;
        assert_eq!(
            rows.len(),
            expected_rows,
            "row count mismatch in {project_id}"
        );
    }

    db.shutdown().await.expect("shutdown mixed");

    eprintln!(
        "mixed_tps: projects={} writers_per_scope={} readers_per_scope={} run_s={} inserts={} updates={} point_reads={} index_queries={} write_ops={} read_ops={} total_ops={} write_tps={:.2} read_tps={:.2} total_ops_tps={:.2}",
        project_count,
        writers_per_scope,
        readers_per_scope,
        run_secs,
        inserts_n,
        updates_n,
        point_reads_n,
        index_queries_n,
        write_ops,
        read_ops,
        total_ops,
        write_tps,
        read_tps,
        total_ops_tps
    );
    assert!(
        total_ops > 0,
        "expected mixed workload to execute operations"
    );
}

#[tokio::test]
#[ignore = "long-running realistic mixed table/index/kv throughput benchmark"]
async fn stress_realistic_user_load_multi_scope_tps() {
    const PROJECT_PREFIX: &str = "realbench";
    const SCOPE_PREFIX: &str = "tenant";

    let project_count = realistic_projects();
    let scopes_per_project = realistic_scopes_per_project();
    let writers_per_scope = realistic_writer_workers_per_scope();
    let readers_per_scope = realistic_reader_workers_per_scope();
    let seed_rows = realistic_seed_rows_per_scope();
    let seed_kv_keys = realistic_seed_kv_keys_per_scope();
    let run_secs = realistic_run_seconds();
    assert!(run_secs > 0, "run seconds must be > 0");

    let mut config = stress_config();
    config.max_scan_rows = 1_000_000;

    let dir = tempdir().expect("temp");
    let db = Arc::new(AedbInstance::open(config, dir.path()).expect("open"));
    let reader_caller = CallerContext::new("realbench_reader");

    let mut scopes = Vec::new();
    for project_idx in 0..project_count {
        let project_id = format!("{PROJECT_PREFIX}-{project_idx}");
        db.create_project(&project_id).await.expect("project");

        for scope_idx in 0..scopes_per_project {
            let scope_id = if scope_idx == 0 {
                "app".to_string()
            } else {
                format!("{SCOPE_PREFIX}-{scope_idx}")
            };
            if scope_id != "app" {
                db.create_scope(&project_id, &scope_id)
                    .await
                    .expect("scope");
            }

            db.commit(Mutation::Ddl(DdlOperation::CreateTable {
                owner_id: None,
                if_not_exists: false,
                project_id: project_id.clone(),
                scope_id: scope_id.clone(),
                table_name: "items".into(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "value".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                    ColumnDef {
                        name: "tag".into(),
                        col_type: ColumnType::Integer,
                        nullable: false,
                    },
                ],
                primary_key: vec!["id".into()],
            }))
            .await
            .expect("items table");

            db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
                project_id: project_id.clone(),
                scope_id: scope_id.clone(),
                table_name: "items".into(),
                index_name: "by_tag".into(),
                if_not_exists: false,
                columns: vec!["tag".into()],
                index_type: IndexType::BTree,
                partial_filter: None,
            }))
            .await
            .expect("items index");

            db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
                actor_id: None,
                delegable: false,
                caller_id: reader_caller.caller_id.clone(),
                permission: Permission::KvRead {
                    project_id: project_id.clone(),
                    scope_id: Some(scope_id.clone()),
                    prefix: None,
                },
            }))
            .await
            .expect("grant kv read");

            for i in 0..seed_rows {
                let id = to_i64(i);
                db.commit(Mutation::Upsert {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    table_name: "items".into(),
                    primary_key: vec![Value::Integer(id)],
                    row: Row {
                        values: vec![
                            Value::Integer(id),
                            Value::Integer(id % 10_000),
                            Value::Integer(id % 100),
                        ],
                    },
                })
                .await
                .expect("seed row");
            }

            for i in 0..seed_kv_keys {
                let key = format!("session:{i}").into_bytes();
                let value = format!("v{i}").into_bytes();
                db.commit(Mutation::KvSet {
                    project_id: project_id.clone(),
                    scope_id: scope_id.clone(),
                    key,
                    value,
                })
                .await
                .expect("seed kv");
            }

            scopes.push((project_id.clone(), scope_id));
        }
    }

    let scope_count = scopes.len();
    let next_row_by_scope = Arc::new(
        (0..scope_count)
            .map(|_| AtomicU64::new(seed_rows as u64))
            .collect::<Vec<_>>(),
    );
    let row_inserts_by_scope = Arc::new(
        (0..scope_count)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );
    let next_kv_by_scope = Arc::new(
        (0..scope_count)
            .map(|_| AtomicU64::new(seed_kv_keys as u64))
            .collect::<Vec<_>>(),
    );

    let table_inserts = Arc::new(AtomicU64::new(0));
    let table_updates = Arc::new(AtomicU64::new(0));
    let table_point_reads = Arc::new(AtomicU64::new(0));
    let table_index_queries = Arc::new(AtomicU64::new(0));
    let kv_sets = Arc::new(AtomicU64::new(0));
    let kv_gets = Arc::new(AtomicU64::new(0));
    let kv_scans = Arc::new(AtomicU64::new(0));

    let stop = Arc::new(AtomicBool::new(false));
    let stop_at = Instant::now() + Duration::from_secs(run_secs as u64);
    let started = Instant::now();
    let mut tasks = Vec::new();

    for (scope_slot, (project_id, scope_id)) in scopes.iter().enumerate() {
        for worker_idx in 0..writers_per_scope {
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            let next_row_by_scope = Arc::clone(&next_row_by_scope);
            let row_inserts_by_scope = Arc::clone(&row_inserts_by_scope);
            let next_kv_by_scope = Arc::clone(&next_kv_by_scope);
            let table_inserts = Arc::clone(&table_inserts);
            let table_updates = Arc::clone(&table_updates);
            let kv_sets = Arc::clone(&kv_sets);
            let project_id = project_id.clone();
            let scope_id = scope_id.clone();
            tasks.push(tokio::spawn(async move {
                let mut state =
                    ((scope_slot as u64 + 1) << 24) ^ worker_idx as u64 ^ 0xa076_1d64_78bd_642f;
                while !stop.load(Ordering::Relaxed) {
                    state = state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    let op = state % 100;
                    if op < 30 {
                        let id_u64 = next_row_by_scope[scope_slot].fetch_add(1, Ordering::Relaxed);
                        let id = i64::try_from(id_u64).expect("id fits i64");
                        db.commit(Mutation::Upsert {
                            project_id: project_id.clone(),
                            scope_id: scope_id.clone(),
                            table_name: "items".into(),
                            primary_key: vec![Value::Integer(id)],
                            row: Row {
                                values: vec![
                                    Value::Integer(id),
                                    Value::Integer((id % 20_000).abs()),
                                    Value::Integer((id % 100).abs()),
                                ],
                            },
                        })
                        .await
                        .expect("table insert");
                        table_inserts.fetch_add(1, Ordering::Relaxed);
                        row_inserts_by_scope[scope_slot].fetch_add(1, Ordering::Relaxed);
                    } else if op < 60 {
                        let max_id = next_row_by_scope[scope_slot].load(Ordering::Relaxed).max(1);
                        let id = i64::try_from(state % max_id).expect("id fits");
                        db.commit(Mutation::Upsert {
                            project_id: project_id.clone(),
                            scope_id: scope_id.clone(),
                            table_name: "items".into(),
                            primary_key: vec![Value::Integer(id)],
                            row: Row {
                                values: vec![
                                    Value::Integer(id),
                                    Value::Integer((state % 20_000) as i64),
                                    Value::Integer((state % 100) as i64),
                                ],
                            },
                        })
                        .await
                        .expect("table update");
                        table_updates.fetch_add(1, Ordering::Relaxed);
                    } else {
                        let do_new_kv = op >= 85;
                        let kv_id_u64 = if do_new_kv {
                            next_kv_by_scope[scope_slot].fetch_add(1, Ordering::Relaxed)
                        } else {
                            let max_id =
                                next_kv_by_scope[scope_slot].load(Ordering::Relaxed).max(1);
                            state % max_id
                        };
                        let key = format!("session:{kv_id_u64}").into_bytes();
                        let value = format!("w{}:{state}", worker_idx).into_bytes();
                        db.commit(Mutation::KvSet {
                            project_id: project_id.clone(),
                            scope_id: scope_id.clone(),
                            key,
                            value,
                        })
                        .await
                        .expect("kv set");
                        kv_sets.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for worker_idx in 0..readers_per_scope {
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            let next_row_by_scope = Arc::clone(&next_row_by_scope);
            let next_kv_by_scope = Arc::clone(&next_kv_by_scope);
            let table_point_reads = Arc::clone(&table_point_reads);
            let table_index_queries = Arc::clone(&table_index_queries);
            let kv_gets = Arc::clone(&kv_gets);
            let kv_scans = Arc::clone(&kv_scans);
            let project_id = project_id.clone();
            let scope_id = scope_id.clone();
            let reader_caller = reader_caller.clone();
            tasks.push(tokio::spawn(async move {
                let mut state =
                    ((scope_slot as u64 + 3) << 28) ^ worker_idx as u64 ^ 0x517c_c1b7_2722_0a95;
                while !stop.load(Ordering::Relaxed) {
                    state = state
                        .wrapping_mul(2862933555777941757)
                        .wrapping_add(3037000493);
                    let op = state % 100;
                    if op < 40 {
                        let max_id = next_row_by_scope[scope_slot].load(Ordering::Relaxed).max(1);
                        let id = i64::try_from(state % max_id).expect("id fits");
                        let point = db
                            .query(
                                &project_id,
                                &scope_id,
                                Query::select(&["id", "value"])
                                    .from("items")
                                    .where_(col("id").eq(lit(id)))
                                    .limit(1),
                            )
                            .await
                            .expect("table point read");
                        assert!(point.rows.len() <= 1, "point query returned >1 row");
                        table_point_reads.fetch_add(1, Ordering::Relaxed);
                    } else if op < 75 {
                        let tag = i64::try_from((state >> 8) % 100).expect("tag fits");
                        let _ = db
                            .query(
                                &project_id,
                                &scope_id,
                                Query::select(&["id", "tag"])
                                    .from("items")
                                    .where_(col("tag").eq(lit(tag)))
                                    .limit(20),
                            )
                            .await
                            .expect("table index query");
                        table_index_queries.fetch_add(1, Ordering::Relaxed);
                    } else if op < 95 {
                        let max_id = next_kv_by_scope[scope_slot].load(Ordering::Relaxed).max(1);
                        let kv_id = state % max_id;
                        let key = format!("session:{kv_id}");
                        let _ = db
                            .kv_get(
                                &project_id,
                                &scope_id,
                                key.as_bytes(),
                                ConsistencyMode::AtLatest,
                                &reader_caller,
                            )
                            .await
                            .expect("kv get");
                        kv_gets.fetch_add(1, Ordering::Relaxed);
                    } else {
                        let _ = db
                            .kv_scan_prefix(
                                &project_id,
                                &scope_id,
                                b"session:",
                                20,
                                None,
                                ConsistencyMode::AtLatest,
                                &reader_caller,
                            )
                            .await
                            .expect("kv scan");
                        kv_scans.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
    }

    while Instant::now() < stop_at {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    stop.store(true, Ordering::Relaxed);
    for task in tasks {
        task.await.expect("realistic worker join");
    }

    let elapsed = started.elapsed();

    for (scope_slot, (project_id, scope_id)) in scopes.iter().enumerate() {
        let expected_rows =
            seed_rows + row_inserts_by_scope[scope_slot].load(Ordering::Relaxed) as usize;
        let rows = db
            .query(
                project_id,
                scope_id,
                Query::select(&["id"])
                    .from("items")
                    .limit(expected_rows.saturating_add(1)),
            )
            .await
            .expect("row count verify")
            .rows;
        assert_eq!(
            rows.len(),
            expected_rows,
            "row count mismatch in {project_id}.{scope_id}"
        );
    }

    let table_write_ops =
        table_inserts.load(Ordering::Relaxed) + table_updates.load(Ordering::Relaxed);
    let table_read_ops =
        table_point_reads.load(Ordering::Relaxed) + table_index_queries.load(Ordering::Relaxed);
    let kv_ops = kv_sets.load(Ordering::Relaxed)
        + kv_gets.load(Ordering::Relaxed)
        + kv_scans.load(Ordering::Relaxed);
    let total_ops = table_write_ops + table_read_ops + kv_ops;

    let secs = elapsed.as_secs_f64().max(0.001);
    eprintln!(
        "realistic_tps: projects={} scopes_per_project={} writers_per_scope={} readers_per_scope={} run_s={} table_inserts={} table_updates={} table_point_reads={} table_index_queries={} kv_sets={} kv_gets={} kv_scans={} table_write_tps={:.2} table_read_tps={:.2} kv_tps={:.2} total_ops_tps={:.2}",
        project_count,
        scopes_per_project,
        writers_per_scope,
        readers_per_scope,
        run_secs,
        table_inserts.load(Ordering::Relaxed),
        table_updates.load(Ordering::Relaxed),
        table_point_reads.load(Ordering::Relaxed),
        table_index_queries.load(Ordering::Relaxed),
        kv_sets.load(Ordering::Relaxed),
        kv_gets.load(Ordering::Relaxed),
        kv_scans.load(Ordering::Relaxed),
        table_write_ops as f64 / secs,
        table_read_ops as f64 / secs,
        kv_ops as f64 / secs,
        total_ops as f64 / secs
    );

    db.shutdown().await.expect("shutdown realistic workload");
    assert!(total_ops > 0, "expected workload to execute operations");
}
