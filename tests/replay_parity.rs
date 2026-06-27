#![allow(deprecated)]
mod common;

use aedb::AedbInstance;
use aedb::catalog::schema::ColumnDef;
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::catalog::{DdlOperation, KV_INDEX_TABLE};
use aedb::commit::tx::{IdempotencyKey, ReadSet, TransactionEnvelope, WriteClass, WriteIntent};
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use aedb::query::executor::QueryResult;
use aedb::query::plan::{Aggregate, ConsistencyMode, Order, Query, QueryOptions};

use common::{
    PROJECT, SCOPE, TestDb, create_async_index, create_project_scope, create_table, open_db,
    seed_kv,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

fn u256_be(value: u64) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&value.to_be_bytes());
    bytes
}

fn u256_from_be(bytes: &[u8; 32]) -> u64 {
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[24..]);
    u64::from_be_bytes(out)
}

/// Read every `accounts` balance ordered by id, as plain u64s, for parity comparison.
async fn read_account_balances(db: &AedbInstance, count: i64) -> Vec<u64> {
    let result = db
        .query(
            PROJECT,
            SCOPE,
            Query::select(&["id", "balance"])
                .from("accounts")
                .order_by("id", Order::Asc)
                .limit(count as usize + 1),
        )
        .await
        .expect("query account balances");
    result
        .rows
        .iter()
        .map(|row| match row.values[1] {
            Value::U256(be) => u256_from_be(&be),
            ref other => panic!("expected U256 balance, got {other:?}"),
        })
        .collect()
}

fn rows_signature(result: &QueryResult) -> Vec<Vec<Value>> {
    let mut rows: Vec<Vec<Value>> = result.rows.iter().map(|row| row.values.clone()).collect();
    rows.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
    rows
}

async fn projected_kv_signature(db: &AedbInstance) -> Vec<Vec<Value>> {
    for _ in 0..25 {
        let result = db
            .query(
                PROJECT,
                SCOPE,
                Query::select(&["project_id", "scope_id", "key", "value"])
                    .from(KV_INDEX_TABLE)
                    .order_by("key", Order::Asc)
                    .limit(10),
            )
            .await
            .expect("query kv projection");
        if result.rows.len() == 3 {
            return rows_signature(&result);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("kv projection did not materialize expected rows");
}

async fn build_feature_rich_db(config: AedbConfig) -> TestDb {
    let fixture = open_db(config);
    let db = &fixture.db;
    create_project_scope(db, PROJECT, SCOPE).await;
    create_table(
        db,
        PROJECT,
        SCOPE,
        "users",
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
            ColumnDef {
                name: "points".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
    )
    .await;
    create_async_index(
        db,
        PROJECT,
        SCOPE,
        "users",
        "users_projection",
        vec!["id", "name", "points"],
    )
    .await;
    db.enable_kv_projection(PROJECT, SCOPE)
        .await
        .expect("enable kv projection");
    seed_kv(
        db,
        PROJECT,
        SCOPE,
        &[(b"feature:1", b"alpha"), (b"feature:2", b"beta")],
    )
    .await;
    for (id, name, points) in [(1_i64, "alice", 10_i64), (2, "bob", 20), (3, "carol", 30)] {
        db.commit(Mutation::Upsert {
            project_id: PROJECT.into(),
            scope_id: SCOPE.into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![
                Value::Integer(id),
                Value::Text(name.into()),
                Value::Integer(points),
            ]),
        })
        .await
        .expect("seed user");
    }
    db.emit_event(
        PROJECT,
        SCOPE,
        "user_points",
        "evt-1".into(),
        r#"{"user_id":1,"points":10}"#.into(),
    )
    .await
    .expect("emit event");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: Some("system".into()),
        if_not_exists: true,
        project_id: aedb::catalog::SYSTEM_PROJECT_ID.into(),
        scope_id: SCOPE.into(),
        table_name: "reactive_processor_checkpoints".into(),
        columns: vec![
            ColumnDef {
                name: "processor_name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "checkpoint_seq".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "updated_at_micros".into(),
                col_type: ColumnType::Timestamp,
                nullable: false,
            },
        ],
        primary_key: vec!["processor_name".into()],
    }))
    .await
    .expect("create processor checkpoint table");
    db.ack_reactive_processor_checkpoint("points_processor", 1)
        .await
        .expect("ack reactive checkpoint");
    fixture
}

#[tokio::test]
async fn replay_parity_covers_async_indexes_kv_projection_outbox_reactive_and_idempotency() {
    let config = AedbConfig::production([42u8; 32]);
    let fixture = build_feature_rich_db(config.clone()).await;
    let TestDb { db, dir } = fixture;

    let idem_key = IdempotencyKey([9u8; 16]);
    let envelope = TransactionEnvelope {
        caller: None,
        idempotency_key: Some(idem_key.clone()),
        write_class: WriteClass::Economic,
        assertions: Vec::new(),
        read_set: ReadSet::default(),
        write_intent: WriteIntent {
            mutations: vec![Mutation::KvSet {
                project_id: PROJECT.into(),
                scope_id: SCOPE.into(),
                key: b"idempotent:feature".to_vec(),
                value: b"once".to_vec(),
            }],
        },
        base_seq: 0,
    };
    let first_idem = db
        .commit_envelope(envelope.clone())
        .await
        .expect("first idempotent commit");

    let live_users = db
        .query(
            PROJECT,
            SCOPE,
            Query::select(&["id", "name", "points"])
                .from("users")
                .order_by("id", Order::Asc)
                .limit(10),
        )
        .await
        .expect("live users");
    let live_async = db
        .query_with_options(
            PROJECT,
            SCOPE,
            Query::select(&["id", "name", "points"])
                .from("users")
                .order_by("id", Order::Asc)
                .limit(10),
            QueryOptions {
                async_index: Some("users_projection".into()),
                ..QueryOptions::default()
            },
        )
        .await
        .expect("live async projection");
    let live_kv_projection = projected_kv_signature(&db).await;
    let live_event_stream = db
        .read_event_stream(Some("user_points"), 0, 10, ConsistencyMode::AtLatest)
        .await
        .expect("live event stream");
    let live_lifecycle_count = db
        .query(
            "_system",
            SCOPE,
            Query::select(&[])
                .from("lifecycle_outbox")
                .aggregate(Aggregate::Count),
        )
        .await
        .expect("live lifecycle count");
    let live_lag = db
        .reactive_processor_lag("points_processor", ConsistencyMode::AtLatest)
        .await
        .expect("live processor lag");

    let data_dir = dir.path().to_path_buf();
    db.shutdown().await.expect("shutdown");
    drop(db);

    let recovered = AedbInstance::open_anonymous(config, &data_dir).expect("reopen");
    let recovered_users = recovered
        .query(
            PROJECT,
            SCOPE,
            Query::select(&["id", "name", "points"])
                .from("users")
                .order_by("id", Order::Asc)
                .limit(10),
        )
        .await
        .expect("recovered users");
    let recovered_async = recovered
        .query_with_options(
            PROJECT,
            SCOPE,
            Query::select(&["id", "name", "points"])
                .from("users")
                .order_by("id", Order::Asc)
                .limit(10),
            QueryOptions {
                async_index: Some("users_projection".into()),
                ..QueryOptions::default()
            },
        )
        .await
        .expect("recovered async projection");
    let recovered_kv_projection = projected_kv_signature(&recovered).await;
    let recovered_event_stream = recovered
        .read_event_stream(Some("user_points"), 0, 10, ConsistencyMode::AtLatest)
        .await
        .expect("recovered event stream");
    let recovered_lifecycle_count = recovered
        .query(
            "_system",
            SCOPE,
            Query::select(&[])
                .from("lifecycle_outbox")
                .aggregate(Aggregate::Count),
        )
        .await
        .expect("recovered lifecycle count");
    let recovered_lag = recovered
        .reactive_processor_lag("points_processor", ConsistencyMode::AtLatest)
        .await
        .expect("recovered processor lag");
    let idem_retry = recovered
        .commit_envelope(envelope)
        .await
        .expect("idempotent retry after recovery");

    assert_eq!(
        rows_signature(&recovered_users),
        rows_signature(&live_users)
    );
    assert_eq!(
        rows_signature(&recovered_async),
        rows_signature(&live_async)
    );
    assert_eq!(recovered_kv_projection, live_kv_projection);
    assert_eq!(
        recovered_event_stream
            .events
            .iter()
            .map(|event| (&event.topic, &event.event_key, &event.payload_json))
            .collect::<Vec<_>>(),
        live_event_stream
            .events
            .iter()
            .map(|event| (&event.topic, &event.event_key, &event.payload_json))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        rows_signature(&recovered_lifecycle_count),
        rows_signature(&live_lifecycle_count)
    );
    assert_eq!(recovered_lag.checkpoint_seq, live_lag.checkpoint_seq);
    assert_eq!(idem_retry.commit_seq, first_idem.commit_seq);
}

/// Global-replay parity under CONCURRENT multi-writer load on a shared key.
///
/// The existing parity test above drives a single write stream. The interesting distributed-
/// systems claim is stronger: once many independent writers concurrently touch a *shared* key,
/// the order in which their writes land is decided by runtime scheduling (lock-acquisition /
/// dispatch timing), which is non-deterministic across runs. Faithful whole-system replay
/// therefore can't rely on per-writer logs alone — it relies on the WAL being a single, totally
/// ordered log keyed by a global monotonic `commit_seq`. This test exercises exactly that path:
/// a hot shared row (id 0) that every one of N concurrent writers increments, plus per-actor
/// rows, then recovers purely from the WAL and asserts byte-for-byte parity with the live run.
///
/// If recovery ever replayed shared-key writes in a different order (or dropped the global
/// ordering), the recovered balances would diverge from the live snapshot and this fails.
/// `replay_segments` additionally rejects any non-monotonic `commit_seq` during recovery, so a
/// broken total order would surface as a hard recovery error rather than silent divergence.
#[tokio::test]
async fn replay_parity_under_concurrent_shared_key_writers() {
    use aedb::catalog::schema::ColumnDef;

    let config = AedbConfig::production([7u8; 32]);
    let TestDb { db, dir } = open_db(config.clone());
    create_project_scope(&db, PROJECT, SCOPE).await;
    create_table(
        &db,
        PROJECT,
        SCOPE,
        "accounts",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "balance".into(),
                col_type: ColumnType::U256,
                nullable: false,
            },
        ],
    )
    .await;

    // id 0 = shared hot row touched by every writer; ids 1..ACTORS = per-actor rows.
    const ACTORS: i64 = 8;
    const ROWS: i64 = ACTORS + 1;
    const WRITERS: i64 = 240;
    for id in 0..ROWS {
        db.commit(Mutation::Upsert {
            project_id: PROJECT.into(),
            scope_id: SCOPE.into(),
            table_name: "accounts".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![Value::Integer(id), Value::U256(u256_be(0))]),
        })
        .await
        .expect("seed account");
    }

    // Fan out WRITERS concurrent tasks. Each one funnels a commutative +1 through the shared
    // row (id 0) and a +1 through its own actor row. Commutative deltas never conflict, so all
    // of them commit — but the *order* they hit the shared partition is scheduler-decided.
    let db = Arc::new(db);
    let mut tasks = JoinSet::new();
    for i in 0..WRITERS {
        let db = Arc::clone(&db);
        tasks.spawn(async move {
            // Stagger starts so writers genuinely interleave on the hot row.
            tokio::time::sleep(Duration::from_micros(((i % 12) * 30) as u64)).await;
            db.table_inc_u256(
                PROJECT,
                SCOPE,
                "accounts",
                vec![Value::Integer(0)],
                "balance",
                u256_be(1),
            )
            .await?;
            db.table_inc_u256(
                PROJECT,
                SCOPE,
                "accounts",
                vec![Value::Integer(1 + (i % ACTORS))],
                "balance",
                u256_be(1),
            )
            .await
        });
    }
    while let Some(result) = tasks.join_next().await {
        result
            .expect("writer task panicked")
            .expect("commutative inc must commit");
    }

    let live = read_account_balances(&db, ROWS).await;
    // Every writer landed on the shared row; the per-actor rows split WRITERS evenly.
    assert_eq!(
        live[0], WRITERS as u64,
        "all {WRITERS} concurrent writers must land on the shared hot row"
    );
    let actor_total: u64 = live[1..].iter().copied().sum();
    assert_eq!(actor_total, WRITERS as u64, "per-actor writes conserved");

    let data_dir = dir.path().to_path_buf();
    db.shutdown().await.expect("shutdown");
    drop(db);

    // Recover from the WAL alone and require exact parity with the concurrent live run.
    let recovered =
        AedbInstance::open_anonymous(config, &data_dir).expect("reopen after concurrent load");
    let recovered_balances = read_account_balances(&recovered, ROWS).await;
    assert_eq!(
        recovered_balances, live,
        "WAL replay must reproduce the concurrent shared-key interleaving exactly"
    );
}
