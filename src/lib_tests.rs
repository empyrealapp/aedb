use super::{
    AedbInstance, CommitFinality, CommitTelemetryEvent, LifecycleEvent, LifecycleHook,
    QueryBatchItem, QueryCommitTelemetryHook, QueryTelemetryEvent, ReadOnlySqlAdapter,
    RecoveryCache, RemoteBackupAdapter, SYSTEM_CALLER_ID,
};
use crate::PredicateEvaluationPath;
use crate::catalog::schema::{ColumnDef, IndexType};
use crate::catalog::types::{ColumnType, Row, Value};
use crate::catalog::{DdlOperation, ResourceType};
use crate::commit::tx::{IdempotencyKey, TransactionEnvelope, WriteClass, WriteIntent};
use crate::commit::validation::Mutation;
use crate::config::{AedbConfig, DurabilityMode, RecoveryMode};
use crate::error::{AedbError, AedbErrorCode, ResourceType as ErrorResourceType};
use crate::permission::{CallerContext, Permission};
use crate::query::error::QueryError;
use crate::query::plan::{ConsistencyMode, Expr, Query, QueryOptions};
use crate::query::planner::ExecutionStage;
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

fn mk_recovery_view(seq: u64) -> crate::snapshot::reader::SnapshotReadView {
    crate::snapshot::reader::SnapshotReadView {
        keyspace: Arc::new(crate::storage::keyspace::Keyspace::default().snapshot()),
        catalog: Arc::new(crate::catalog::Catalog::default()),
        seq,
    }
}

struct RecordingLifecycleHook {
    events: Arc<std::sync::Mutex<Vec<LifecycleEvent>>>,
}

impl LifecycleHook for RecordingLifecycleHook {
    fn on_event(&self, event: &LifecycleEvent) {
        self.events
            .lock()
            .expect("recording hook mutex poisoned")
            .push(event.clone());
    }
}

#[test]
fn recovery_cache_refresh_keeps_recent_entry() {
    let mut cache = RecoveryCache::default();
    for seq in 1..=16 {
        cache.put(seq, mk_recovery_view(seq));
    }
    assert!(
        cache.get(1).is_some(),
        "first entry should exist before refresh"
    );

    cache.put(17, mk_recovery_view(17));

    assert!(
        cache.get(1).is_some(),
        "refreshing an entry must protect it from immediate eviction"
    );
    assert!(
        cache.get(2).is_none(),
        "oldest non-refreshed entry should be evicted first"
    );
}

#[test]
fn recovery_cache_prunes_expired_entries() {
    let mut cache = RecoveryCache::default();
    cache.put(7, mk_recovery_view(7));
    {
        let entry = cache.entries.get_mut(&7).expect("entry");
        entry.created = Instant::now() - super::RECOVERY_CACHE_TTL - Duration::from_secs(1);
    }
    cache.prune_expired();
    assert!(cache.get(7).is_none(), "expired entry must be removed");
}

#[tokio::test]
async fn api_open_commit_query_shutdown() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

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
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("reader".into())],
        },
    })
    .await
    .expect("insert");

    let result = db
        .query(
            "p",
            "app",
            Query::select(&["*"])
                .from("users")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
        )
        .await
        .expect("query");
    assert_eq!(result.rows.len(), 1);

    db.create_scope("p", "private").await.expect("create scope");
    let scopes = db.list_scopes("p").await.expect("list scopes");
    assert!(scopes.contains(&"app".to_string()));
    assert!(scopes.contains(&"private".to_string()));
    db.drop_scope("p", "private").await.expect("drop scope");
    db.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn permissions_enforced_at_preflight_commit_and_query() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
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
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::TableWrite {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
    }))
    .await
    .expect("grant write");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::TableRead {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
    }))
    .await
    .expect("grant read");

    let caller = CallerContext::new("alice");
    let insert = Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("alice".into())],
        },
    };
    db.preflight_as(&caller, insert.clone())
        .await
        .expect("preflight allowed");
    db.commit_as(caller.clone(), insert)
        .await
        .expect("commit allowed");

    db.commit(Mutation::Ddl(DdlOperation::RevokePermission {
        actor_id: None,
        caller_id: "alice".into(),
        permission: Permission::TableWrite {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
    }))
    .await
    .expect("revoke");

    let denied = db
        .preflight_as(
            &caller,
            Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Integer(2)],
                row: Row {
                    values: vec![Value::Integer(2), Value::Text("x".into())],
                },
            },
        )
        .await
        .expect_err("preflight denied");
    assert!(matches!(
        denied,
        crate::error::AedbError::PermissionDenied(_)
    ));

    let q = db
        .query_with_options_as(
            Some(&caller),
            "p",
            "app",
            Query::select(&["*"]).from("users").limit(10),
            crate::query::plan::QueryOptions::default(),
        )
        .await
        .expect("query with read perm");
    assert_eq!(q.rows.len(), 1);
}

#[tokio::test]
async fn insert_rejects_duplicate_primary_key() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
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
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");

    db.insert(
        "p",
        "app",
        "users",
        vec![Value::Integer(1)],
        Row {
            values: vec![Value::Integer(1), Value::Text("alice".into())],
        },
    )
    .await
    .expect("first insert");

    let duplicate = db
        .insert(
            "p",
            "app",
            "users",
            vec![Value::Integer(1)],
            Row {
                values: vec![Value::Integer(1), Value::Text("alice-2".into())],
            },
        )
        .await
        .expect_err("duplicate insert must fail");
    let duplicate_text = duplicate.to_string();
    assert!(
        duplicate_text.contains("duplicate primary key"),
        "unexpected duplicate insert error: {duplicate:?}"
    );
}

#[tokio::test]
async fn insert_batch_rejects_duplicate_primary_key() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
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
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");

    db.insert_batch(
        "p",
        "app",
        "users",
        vec![
            Row {
                values: vec![Value::Integer(1), Value::Text("alice".into())],
            },
            Row {
                values: vec![Value::Integer(2), Value::Text("bob".into())],
            },
        ],
    )
    .await
    .expect("seed batch");

    let duplicate = db
        .insert_batch(
            "p",
            "app",
            "users",
            vec![
                Row {
                    values: vec![Value::Integer(3), Value::Text("charlie".into())],
                },
                Row {
                    values: vec![Value::Integer(1), Value::Text("alice-2".into())],
                },
            ],
        )
        .await
        .expect_err("duplicate insert_batch must fail");
    let duplicate_text = duplicate.to_string();
    assert!(
        duplicate_text.contains("duplicate primary key"),
        "unexpected duplicate insert_batch error: {duplicate:?}"
    );
}

#[tokio::test]
async fn join_queries_require_permissions_for_all_referenced_tables() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project p");
    db.create_project("_global").await.expect("project global");

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
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("users table");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "_global".into(),
        scope_id: "app".into(),
        table_name: "profiles".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "email".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("profiles table");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::TableRead {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
    }))
    .await
    .expect("grant base read");
    let caller = CallerContext::new("alice");
    let query = Query::select(&["u.id"])
        .from("users")
        .alias("u")
        .left_join("_global.profiles", "u.id", "id")
        .with_last_join_alias("g")
        .limit(10);
    let denied = db
        .query_with_options_as(
            Some(&caller),
            "p",
            "app",
            query.clone(),
            crate::query::plan::QueryOptions::default(),
        )
        .await
        .expect_err("join should require global table read");
    assert!(matches!(
        denied,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::TableRead {
            project_id: "_global".into(),
            scope_id: "app".into(),
            table_name: "profiles".into(),
        },
    }))
    .await
    .expect("grant global read");
    let _ = db
        .query_with_options_as(
            Some(&caller),
            "p",
            "app",
            query,
            crate::query::plan::QueryOptions::default(),
        )
        .await
        .expect("join allowed");
}

#[tokio::test]
async fn permission_revocation_between_preflight_and_commit_is_caught() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
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
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::TableWrite {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
    }))
    .await
    .expect("grant");
    let caller = CallerContext::new("alice");
    let mutation = Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("alice".into())],
        },
    };
    db.preflight_as(&caller, mutation.clone())
        .await
        .expect("preflight ok");
    db.commit(Mutation::Ddl(DdlOperation::RevokePermission {
        actor_id: None,
        caller_id: "alice".into(),
        permission: Permission::TableWrite {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
    }))
    .await
    .expect("revoke");
    let err = db
        .commit_as(caller, mutation)
        .await
        .expect_err("denied at commit");
    assert!(matches!(err, crate::error::AedbError::PermissionDenied(_)));
}

#[tokio::test]
async fn async_projection_index_reports_materialized_seq() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
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
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");

    let r = db
        .commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row {
                values: vec![Value::Integer(1), Value::Text("alice".into())],
            },
        })
        .await
        .expect("insert");
    db.commit(Mutation::Ddl(DdlOperation::CreateAsyncIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        index_name: "users_view".into(),
        if_not_exists: false,
        projected_columns: vec!["id".into(), "name".into()],
    }))
    .await
    .expect("create async index");

    let opts = QueryOptions {
        async_index: Some("users_view".into()),
        allow_full_scan: true,
        ..QueryOptions::default()
    };

    let mut observed = None;
    for _ in 0..20 {
        let q = db
            .query_with_options(
                "p",
                "app",
                Query::select(&["*"]).from("users"),
                opts.clone(),
            )
            .await
            .expect("query async");
        observed = q.materialized_seq;
        if let Some(m) = q.materialized_seq
            && m >= r.commit_seq
        {
            assert_eq!(q.rows.len(), 1);
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("async index did not catch up, last materialized_seq={observed:?}");
}

#[tokio::test]
async fn kv_query_apis_enforce_permissions_and_paginate() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let caller = CallerContext::new("alice");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant kv read");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::KvWrite {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant kv write");

    for (k, v) in [("user:1", "alice"), ("user:2", "bob"), ("user:3", "carol")] {
        db.commit_as(
            caller.clone(),
            Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: k.as_bytes().to_vec(),
                value: v.as_bytes().to_vec(),
            },
        )
        .await
        .expect("kv set");
    }

    let one = db
        .kv_get("p", "app", b"user:1", ConsistencyMode::AtLatest, &caller)
        .await
        .expect("kv_get")
        .expect("exists");
    assert_eq!(one.value, b"alice".to_vec());

    let first = db
        .kv_scan_prefix(
            "p",
            "app",
            b"user:",
            2,
            None,
            ConsistencyMode::AtLatest,
            &caller,
        )
        .await
        .expect("scan 1");
    assert_eq!(first.entries.len(), 2);
    assert!(first.truncated);

    let second = db
        .kv_scan_prefix(
            "p",
            "app",
            b"user:",
            2,
            first.cursor.clone(),
            ConsistencyMode::AtLatest,
            &caller,
        )
        .await
        .expect("scan 2");
    assert_eq!(second.entries.len(), 1);
    assert!(!second.truncated);

    let ranged = db
        .kv_scan_range(
            "p",
            "app",
            Bound::Included(b"user:1".to_vec()),
            Bound::Included(b"user:2".to_vec()),
            10,
            None,
            ConsistencyMode::AtLatest,
            &caller,
        )
        .await
        .expect("scan range");
    assert_eq!(ranged.entries.len(), 2);
}

#[tokio::test]
async fn kv_permissions_scope_reads_and_writes_with_prefix_filters() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let caller = CallerContext::new("alice");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: Some(b"bank:user:42:".to_vec()),
        },
    }))
    .await
    .expect("grant kv prefix read");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::KvWrite {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: Some(b"bank:user:42:".to_vec()),
        },
    }))
    .await
    .expect("grant kv prefix write");

    db.commit_as(
        caller.clone(),
        Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"bank:user:42:balance".to_vec(),
            value: b"100".to_vec(),
        },
    )
    .await
    .expect("allowed prefix write");

    let err = db
        .commit_as(
            caller.clone(),
            Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"bank:user:99:balance".to_vec(),
                value: b"100".to_vec(),
            },
        )
        .await
        .expect_err("write outside prefix must fail");
    assert!(matches!(err, AedbError::PermissionDenied(_)));

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"bank:user:99:balance".to_vec(),
        value: b"77".to_vec(),
    })
    .await
    .expect("seed other key");

    let scan = db
        .kv_scan_prefix(
            "p",
            "app",
            b"bank:user:",
            10,
            None,
            ConsistencyMode::AtLatest,
            &caller,
        )
        .await
        .expect("scan with broad prefix");
    assert_eq!(scan.entries.len(), 1);
    assert_eq!(scan.entries[0].0, b"bank:user:42:balance".to_vec());
}

#[tokio::test]
async fn kv_write_helpers_respect_scope_boundaries() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "private").await.expect("scope");

    db.kv_set("p", "app", b"counter".to_vec(), b"100".to_vec())
        .await
        .expect("write default scope");
    db.kv_set("p", "private", b"counter".to_vec(), b"7".to_vec())
        .await
        .expect("write private scope");

    let app_reader = CallerContext::new("app_reader");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: app_reader.caller_id.clone(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant app read");

    let private_reader = CallerContext::new("private_reader");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: private_reader.caller_id.clone(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("private".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant private read");

    let app_value = db
        .kv_get(
            "p",
            "app",
            b"counter",
            ConsistencyMode::AtLatest,
            &app_reader,
        )
        .await
        .expect("app read")
        .expect("app key");
    assert_eq!(app_value.value, b"100".to_vec());

    let private_denied = db
        .kv_get(
            "p",
            "private",
            b"counter",
            ConsistencyMode::AtLatest,
            &app_reader,
        )
        .await
        .expect_err("app reader cannot read private scope");
    assert!(matches!(
        private_denied,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));

    let private_value = db
        .kv_get(
            "p",
            "private",
            b"counter",
            ConsistencyMode::AtLatest,
            &private_reader,
        )
        .await
        .expect("private read")
        .expect("private key");
    assert_eq!(private_value.value, b"7".to_vec());
}

#[tokio::test]
async fn kv_projection_table_materializes_kv_state() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.enable_kv_projection("p", "app")
        .await
        .expect("enable kv projection");

    db.kv_set("p", "app", b"user:1".to_vec(), b"alice".to_vec())
        .await
        .expect("kv set 1");
    db.kv_set("p", "app", b"user:2".to_vec(), b"bob".to_vec())
        .await
        .expect("kv set 2");

    let mut projected = None;
    for _ in 0..25 {
        let result = db
            .query(
                "p",
                "app",
                Query::select(&["*"])
                    .from(crate::catalog::KV_INDEX_TABLE)
                    .limit(10),
            )
            .await
            .expect("query projection table");
        if result.rows.len() == 2 {
            projected = Some(result.rows);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let rows = projected.expect("projection rows");
    let mut saw_user1 = false;
    let mut saw_user2 = false;
    for row in rows {
        let project = row.values.first().expect("project_id");
        let scope = row.values.get(1).expect("scope_id");
        let key = row.values.get(2).expect("key");
        let value = row.values.get(3).expect("value");
        let commit_seq = row.values.get(4).expect("commit_seq");
        assert_eq!(project, &Value::Text("p".into()));
        assert_eq!(scope, &Value::Text("app".into()));
        assert!(matches!(commit_seq, Value::Integer(v) if *v > 0));
        match (key, value) {
            (Value::Blob(k), Value::Blob(v)) if k == b"user:1" && v == b"alice" => {
                saw_user1 = true;
            }
            (Value::Blob(k), Value::Blob(v)) if k == b"user:2" && v == b"bob" => {
                saw_user2 = true;
            }
            other => panic!("unexpected kv projection row: {other:?}"),
        }
    }
    assert!(saw_user1, "missing user:1 projection row");
    assert!(saw_user2, "missing user:2 projection row");
}

#[tokio::test]
async fn kv_projection_table_is_managed_and_read_only() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.enable_kv_projection("p", "app")
        .await
        .expect("enable projection");

    let err = db
        .commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: crate::catalog::KV_INDEX_TABLE.into(),
            primary_key: vec![Value::Blob(b"k".to_vec())],
            row: Row {
                values: vec![
                    Value::Text("p".into()),
                    Value::Text("app".into()),
                    Value::Blob(b"k".to_vec()),
                    Value::Blob(b"v".to_vec()),
                    Value::Integer(1),
                    Value::Timestamp(1),
                ],
            },
        })
        .await
        .expect_err("managed table writes must fail");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn kv_query_apis_are_scope_aware() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "tenant1").await.expect("scope");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "tenant1".into(),
        key: b"user:1".to_vec(),
        value: b"alice".to_vec(),
    })
    .await
    .expect("seed kv");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("tenant1".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant kv read");

    let caller = CallerContext::new("alice");
    let hit = db
        .kv_get(
            "p",
            "tenant1",
            b"user:1",
            ConsistencyMode::AtLatest,
            &caller,
        )
        .await
        .expect("read in granted scope")
        .expect("value exists");
    assert_eq!(hit.value, b"alice".to_vec());

    let denied = db
        .kv_get("p", "app", b"user:1", ConsistencyMode::AtLatest, &caller)
        .await
        .expect_err("read should be denied in ungranted scope");
    assert!(matches!(
        denied,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));
}

#[tokio::test]
async fn scoped_admin_permissions_bound_grant_revoke() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "s1").await.expect("scope s1");
    db.create_scope("p", "s2").await.expect("scope s2");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "owner".into(),
        permission: Permission::ScopeAdmin {
            project_id: "p".into(),
            scope_id: "s1".into(),
        },
    }))
    .await
    .expect("grant scope admin");

    let owner = CallerContext::new("owner");
    db.commit_as(
        owner.clone(),
        Mutation::Ddl(DdlOperation::GrantPermission {
            actor_id: None,
            delegable: false,
            caller_id: "alice".into(),
            permission: Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("s1".into()),
                prefix: None,
            },
        }),
    )
    .await
    .expect("scope admin can grant in own scope");

    let denied = db
        .commit_as(
            owner.clone(),
            Mutation::Ddl(DdlOperation::GrantPermission {
                actor_id: None,
                delegable: false,
                caller_id: "alice".into(),
                permission: Permission::KvRead {
                    project_id: "p".into(),
                    scope_id: Some("s2".into()),
                    prefix: None,
                },
            }),
        )
        .await
        .expect_err("scope admin cannot grant in other scope");
    assert!(matches!(denied, AedbError::PermissionDenied(_)));

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "proj".into(),
        permission: Permission::ProjectAdmin {
            project_id: "p".into(),
        },
    }))
    .await
    .expect("grant project admin");
    let proj = CallerContext::new("proj");
    db.commit_as(
        proj,
        Mutation::Ddl(DdlOperation::GrantPermission {
            actor_id: None,
            delegable: false,
            caller_id: "alice".into(),
            permission: Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("s2".into()),
                prefix: None,
            },
        }),
    )
    .await
    .expect("project admin can grant across project scopes");
}

#[tokio::test]
async fn project_owner_and_delegable_grants_control_authz_delegation() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.commit(Mutation::Ddl(DdlOperation::CreateProject {
        project_id: "p".into(),
        owner_id: Some("owner".into()),
        if_not_exists: true,
    }))
    .await
    .expect("owner creates project");
    let owner = CallerContext::new("owner");

    let attacker = CallerContext::new("mallory");
    let denied = db
        .commit_as(
            attacker,
            Mutation::Ddl(DdlOperation::GrantPermission {
                caller_id: "alice".into(),
                permission: Permission::KvRead {
                    project_id: "p".into(),
                    scope_id: Some("app".into()),
                    prefix: None,
                },
                actor_id: None,
                delegable: false,
            }),
        )
        .await
        .expect_err("non-owner cannot grant");
    assert!(matches!(denied, AedbError::PermissionDenied(_)));

    db.commit_as(
        owner,
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "alice".into(),
            permission: Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("app".into()),
                prefix: None,
            },
            actor_id: None,
            delegable: true,
        }),
    )
    .await
    .expect("owner grants delegable read");
    let (_, catalog, _) = db.executor.snapshot_state().await;
    assert!(catalog.has_delegable_grant(
        "alice",
        &Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        }
    ));

    let alice = CallerContext::new("alice");
    db.commit_as(
        alice.clone(),
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "bob".into(),
            permission: Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("app".into()),
                prefix: None,
            },
            actor_id: None,
            delegable: false,
        }),
    )
    .await
    .expect("delegable grant allows onward delegation");

    let denied = db
        .commit_as(
            alice,
            Mutation::Ddl(DdlOperation::GrantPermission {
                caller_id: "bob".into(),
                permission: Permission::KvWrite {
                    project_id: "p".into(),
                    scope_id: Some("app".into()),
                    prefix: None,
                },
                actor_id: None,
                delegable: false,
            }),
        )
        .await
        .expect_err("delegable grant is permission-specific");
    assert!(matches!(denied, AedbError::PermissionDenied(_)));
}

#[tokio::test]
async fn kv_permission_overlap_revoke_precedence_is_explicit() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "private")
        .await
        .expect("private scope");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"user:1".to_vec(),
        value: b"alice".to_vec(),
    })
    .await
    .expect("seed app user");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"order:1".to_vec(),
        value: b"100".to_vec(),
    })
    .await
    .expect("seed app order");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "private".into(),
        key: b"user:1".to_vec(),
        value: b"secret".to_vec(),
    })
    .await
    .expect("seed private user");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: None,
            prefix: None,
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant project wide read");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: Some(b"user:".to_vec()),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant narrow read");
    db.commit(Mutation::Ddl(DdlOperation::RevokePermission {
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: None,
            prefix: None,
        },
        actor_id: None,
    }))
    .await
    .expect("revoke broad read");

    let alice = CallerContext::new("alice");
    let allowed = db
        .kv_get("p", "app", b"user:1", ConsistencyMode::AtLatest, &alice)
        .await
        .expect("narrow grant still applies");
    assert!(allowed.is_some());

    let denied_key = db
        .kv_get("p", "app", b"order:1", ConsistencyMode::AtLatest, &alice)
        .await
        .expect_err("revoke broad grant should deny non-matching key");
    assert!(matches!(
        denied_key,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));

    let denied_scope = db
        .kv_get("p", "private", b"user:1", ConsistencyMode::AtLatest, &alice)
        .await
        .expect_err("narrow app grant should not spill into private scope");
    assert!(matches!(
        denied_scope,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));
}

#[tokio::test]
async fn grant_metadata_tracks_actor_and_system_source() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.commit(Mutation::Ddl(DdlOperation::CreateProject {
        project_id: "p".into(),
        owner_id: Some("owner".into()),
        if_not_exists: true,
    }))
    .await
    .expect("project");

    let owner = CallerContext::new("owner");
    db.commit_as(
        owner.clone(),
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "bob".into(),
            permission: Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("app".into()),
                prefix: None,
            },
            actor_id: Some("mallory".into()),
            delegable: true,
        }),
    )
    .await
    .expect("owner grant with explicit actor");

    db.commit_as(
        owner,
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "carol".into(),
            permission: Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("app".into()),
                prefix: None,
            },
            actor_id: None,
            delegable: false,
        }),
    )
    .await
    .expect("owner grant with implicit actor");

    let (_, catalog, _) = db.executor.snapshot_state().await;
    let bob_meta = catalog
        .permission_grants
        .get(&(
            "bob".to_string(),
            Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("app".into()),
                prefix: None,
            },
        ))
        .expect("bob metadata");
    assert_eq!(bob_meta.granted_by, "owner");
    assert!(bob_meta.delegable);

    let carol_meta = catalog
        .permission_grants
        .get(&(
            "carol".to_string(),
            Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("app".into()),
                prefix: None,
            },
        ))
        .expect("carol metadata");
    assert_eq!(carol_meta.granted_by, "owner");
    assert!(!carol_meta.delegable);
}

#[tokio::test]
async fn grant_and_revoke_authorization_matrix_is_enforced() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.commit(Mutation::Ddl(DdlOperation::CreateProject {
        project_id: "p".into(),
        owner_id: Some("owner".into()),
        if_not_exists: true,
    }))
    .await
    .expect("project");
    db.create_scope("p", "s1").await.expect("scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "s1".into(),
        table_name: "events".into(),
        owner_id: Some("table_owner".into()),
        if_not_exists: false,
        columns: vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "s1".into(),
        table_name: "events".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1)],
        },
    })
    .await
    .expect("seed row");

    let denied = db
        .commit_as(
            CallerContext::new("mallory"),
            Mutation::Ddl(DdlOperation::GrantPermission {
                caller_id: "pa".into(),
                permission: Permission::ProjectAdmin {
                    project_id: "p".into(),
                },
                actor_id: None,
                delegable: false,
            }),
        )
        .await
        .expect_err("non-owner cannot grant project admin");
    assert!(matches!(denied, AedbError::PermissionDenied(_)));

    db.commit_as(
        CallerContext::new("owner"),
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "pa".into(),
            permission: Permission::ProjectAdmin {
                project_id: "p".into(),
            },
            actor_id: None,
            delegable: false,
        }),
    )
    .await
    .expect("owner grants project admin");

    db.commit_as(
        CallerContext::new("pa"),
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "sa".into(),
            permission: Permission::ScopeAdmin {
                project_id: "p".into(),
                scope_id: "s1".into(),
            },
            actor_id: None,
            delegable: false,
        }),
    )
    .await
    .expect("project admin grants scope admin");

    let denied = db
        .commit_as(
            CallerContext::new("sa"),
            Mutation::Ddl(DdlOperation::GrantPermission {
                caller_id: "other".into(),
                permission: Permission::ProjectAdmin {
                    project_id: "p".into(),
                },
                actor_id: None,
                delegable: false,
            }),
        )
        .await
        .expect_err("scope admin cannot grant project admin");
    assert!(matches!(denied, AedbError::PermissionDenied(_)));

    db.commit_as(
        CallerContext::new("table_owner"),
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "reader".into(),
            permission: Permission::TableRead {
                project_id: "p".into(),
                scope_id: "s1".into(),
                table_name: "events".into(),
            },
            actor_id: None,
            delegable: false,
        }),
    )
    .await
    .expect("table owner grants table read");

    let reader = CallerContext::new("reader");
    let query = db
        .query_with_options_as(
            Some(&reader),
            "p",
            "s1",
            Query::select(&["id"]).from("events").limit(10),
            QueryOptions::default(),
        )
        .await
        .expect("reader query");
    assert_eq!(query.rows.len(), 1);

    let denied = db
        .commit_as(
            CallerContext::new("mallory"),
            Mutation::Ddl(DdlOperation::RevokePermission {
                caller_id: "reader".into(),
                permission: Permission::TableRead {
                    project_id: "p".into(),
                    scope_id: "s1".into(),
                    table_name: "events".into(),
                },
                actor_id: None,
            }),
        )
        .await
        .expect_err("non-admin/non-owner cannot revoke");
    assert!(matches!(denied, AedbError::PermissionDenied(_)));

    db.commit_as(
        CallerContext::new("pa"),
        Mutation::Ddl(DdlOperation::RevokePermission {
            caller_id: "reader".into(),
            permission: Permission::TableRead {
                project_id: "p".into(),
                scope_id: "s1".into(),
                table_name: "events".into(),
            },
            actor_id: None,
        }),
    )
    .await
    .expect("project admin revokes table read");

    let denied_query = db
        .query_with_options_as(
            Some(&reader),
            "p",
            "s1",
            Query::select(&["id"]).from("events").limit(10),
            QueryOptions::default(),
        )
        .await
        .expect_err("reader should lose access after revoke");
    assert!(matches!(
        denied_query,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));
}

#[tokio::test]
async fn kv_scan_all_scopes_requires_project_wide_read() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "s1").await.expect("scope s1");
    db.create_scope("p", "s2").await.expect("scope s2");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "s1".into(),
        key: b"user:1".to_vec(),
        value: b"alice".to_vec(),
    })
    .await
    .expect("seed s1");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "s2".into(),
        key: b"user:2".to_vec(),
        value: b"bob".to_vec(),
    })
    .await
    .expect("seed s2");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: None,
            prefix: None,
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant project-wide read");

    let alice = CallerContext::new("alice");
    let entries = db
        .kv_scan_all_scopes("p", b"user:", 10, ConsistencyMode::AtLatest, &alice)
        .await
        .expect("scan all scopes");
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().any(|e| e.scope_id == "s1"));
    assert!(entries.iter().any(|e| e.scope_id == "s2"));

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "bob".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("s1".into()),
            prefix: None,
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant scope-only read");
    let bob = CallerContext::new("bob");
    let denied = db
        .kv_scan_all_scopes("p", b"user:", 10, ConsistencyMode::AtLatest, &bob)
        .await
        .expect_err("scope-only grant cannot scan all scopes");
    assert!(matches!(
        denied,
        crate::query::error::QueryError::PermissionDenied { .. }
    ));
}

#[tokio::test]
async fn ownership_transfer_requires_owner_or_global_admin() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.commit(Mutation::Ddl(DdlOperation::CreateProject {
        project_id: "p".into(),
        owner_id: Some("owner".into()),
        if_not_exists: true,
    }))
    .await
    .expect("create project with owner");

    let mallory = CallerContext::new("mallory");
    let denied = db
        .commit_as(
            mallory,
            Mutation::Ddl(DdlOperation::TransferOwnership {
                resource_type: ResourceType::Project,
                project_id: "p".into(),
                scope_id: None,
                table_name: None,
                new_owner_id: "next".into(),
                actor_id: None,
            }),
        )
        .await
        .expect_err("non-owner transfer must fail");
    assert!(matches!(denied, AedbError::PermissionDenied(_)));

    let owner = CallerContext::new("owner");
    db.commit_as(
        owner,
        Mutation::Ddl(DdlOperation::TransferOwnership {
            resource_type: ResourceType::Project,
            project_id: "p".into(),
            scope_id: None,
            table_name: None,
            new_owner_id: "next".into(),
            actor_id: None,
        }),
    )
    .await
    .expect("owner transfer");

    let (_, catalog, _) = db.executor.snapshot_state().await;
    assert_eq!(
        catalog
            .projects
            .get("p")
            .and_then(|p| p.owner_id.as_deref()),
        Some("next")
    );
}

#[tokio::test]
async fn authz_grant_revoke_events_are_persisted_in_system_audit_table() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.commit(Mutation::Ddl(DdlOperation::CreateProject {
        project_id: "p".into(),
        owner_id: Some("owner".into()),
        if_not_exists: true,
    }))
    .await
    .expect("project");
    let owner = CallerContext::new("owner");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "owner".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("users table");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant");
    db.commit(Mutation::Ddl(DdlOperation::RevokePermission {
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
        actor_id: None,
    }))
    .await
    .expect("revoke");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "owner".into(),
        permission: Permission::TableDdl {
            project_id: "p".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant owner ddl");
    db.commit_as(
        owner.clone(),
        Mutation::Ddl(DdlOperation::SetReadPolicy {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            predicate: Expr::Eq("owner".into(), Value::Text("$caller_id".into())),
            actor_id: None,
        }),
    )
    .await
    .expect("set policy");
    db.commit_as(
        owner.clone(),
        Mutation::Ddl(DdlOperation::ClearReadPolicy {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            actor_id: None,
        }),
    )
    .await
    .expect("clear policy");
    db.commit_as(
        owner,
        Mutation::Ddl(DdlOperation::TransferOwnership {
            resource_type: ResourceType::Project,
            project_id: "p".into(),
            scope_id: None,
            table_name: None,
            new_owner_id: "next".into(),
            actor_id: None,
        }),
    )
    .await
    .expect("transfer policy");

    let audit = db
        .query(
            "_system",
            "app",
            Query::select(&["*"]).from("authz_audit").limit(20),
        )
        .await
        .expect("audit query");
    assert!(audit.rows.len() >= 2, "expected at least grant+revoke rows");
    let actions: Vec<String> = audit
        .rows
        .iter()
        .filter_map(|r| match r.values.get(2) {
            Some(Value::Text(v)) => Some(v.to_string()),
            _ => None,
        })
        .collect();
    assert!(actions.iter().any(|a| a == "grant"));
    assert!(actions.iter().any(|a| a == "revoke"));
    assert!(actions.iter().any(|a| a == "set_read_policy"));
    assert!(actions.iter().any(|a| a == "clear_read_policy"));
    assert!(actions.iter().any(|a| a == "transfer_ownership"));

    let owner_actor_actions: Vec<String> = audit
        .rows
        .iter()
        .filter_map(|r| match (r.values.get(2), r.values.get(3)) {
            (Some(Value::Text(action)), Some(Value::Text(actor))) if actor.as_str() == "owner" => {
                Some(action.to_string())
            }
            _ => None,
        })
        .collect();
    assert!(owner_actor_actions.iter().any(|a| a == "set_read_policy"));
    assert!(owner_actor_actions.iter().any(|a| a == "clear_read_policy"));
    assert!(
        owner_actor_actions
            .iter()
            .any(|a| a == "transfer_ownership")
    );
}

#[tokio::test]
async fn read_policies_filter_rows_for_caller_and_can_be_cleared() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "owner".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("reader".into())],
        },
    })
    .await
    .expect("row 1");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(2)],
        row: Row {
            values: vec![Value::Integer(2), Value::Text("bob".into())],
        },
    })
    .await
    .expect("row 2");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "reader".into(),
        permission: Permission::TableRead {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant read");
    db.set_read_policy(
        "p",
        "app",
        "users",
        Expr::Eq("owner".into(), Value::Text("$caller_id".into())),
    )
    .await
    .expect("set read policy");

    let reader = CallerContext::new("reader");
    let filtered = db
        .query_with_options_as(
            Some(&reader),
            "p",
            "app",
            Query::select(&["*"]).from("users").limit(10),
            QueryOptions::default(),
        )
        .await
        .expect("filtered query");
    assert_eq!(filtered.rows.len(), 1);

    db.clear_read_policy("p", "app", "users")
        .await
        .expect("clear read policy");
    let unfiltered = db
        .query_with_options_as(
            Some(&reader),
            "p",
            "app",
            Query::select(&["*"]).from("users").limit(10),
            QueryOptions::default(),
        )
        .await
        .expect("unfiltered query");
    assert_eq!(unfiltered.rows.len(), 2);
}

#[tokio::test]
async fn read_policy_applies_to_joined_tables() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("users");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "profiles".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "user_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "owner".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".into()],
    }))
    .await
    .expect("profiles");

    for id in [1, 2] {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row {
                values: vec![Value::Integer(id)],
            },
        })
        .await
        .expect("seed user");
    }
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "profiles".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("reader".into())],
        },
    })
    .await
    .expect("profile 1");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "profiles".into(),
        primary_key: vec![Value::Integer(2)],
        row: Row {
            values: vec![Value::Integer(2), Value::Text("bob".into())],
        },
    })
    .await
    .expect("profile 2");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "reader".into(),
        permission: Permission::TableRead {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant users read");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "reader".into(),
        permission: Permission::TableRead {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "profiles".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant profiles read");
    db.set_read_policy(
        "p",
        "app",
        "profiles",
        Expr::Eq("owner".into(), Value::Text("$caller_id".into())),
    )
    .await
    .expect("set join policy");

    let reader = CallerContext::new("reader");
    let result = db
        .query_with_options_as(
            Some(&reader),
            "p",
            "app",
            Query::select(&["*"])
                .from("users")
                .alias("u")
                .inner_join("profiles", "u.id", "user_id")
                .with_last_join_alias("p"),
            QueryOptions::default(),
        )
        .await
        .expect("join query");
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn table_policy_bypass_permission_skips_row_policy_filtering() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "owner".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("users");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("reader".into())],
        },
    })
    .await
    .expect("seed user 1");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(2)],
        row: Row {
            values: vec![Value::Integer(2), Value::Text("bob".into())],
        },
    })
    .await
    .expect("seed user 2");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "reader".into(),
        permission: Permission::TableRead {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant table read");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "reader".into(),
        permission: Permission::PolicyBypass {
            project_id: "p".into(),
            table_name: Some("users".into()),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant policy bypass");
    db.set_read_policy(
        "p",
        "app",
        "users",
        Expr::Eq("owner".into(), Value::Text("$caller_id".into())),
    )
    .await
    .expect("set policy");

    let reader = CallerContext::new("reader");
    let result = db
        .query_with_options_as(
            Some(&reader),
            "p",
            "app",
            Query::select(&["*"]).from("users").limit(10),
            QueryOptions::default(),
        )
        .await
        .expect("query with policy bypass");
    assert_eq!(result.rows.len(), 2);
}

#[tokio::test]
async fn project_policy_bypass_permission_skips_joined_table_policies() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "owner".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("users");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "profiles".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "user_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "owner".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".into()],
    }))
    .await
    .expect("profiles");

    for (id, owner) in [(1, "reader"), (2, "bob")] {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row {
                values: vec![Value::Integer(id), Value::Text(owner.into())],
            },
        })
        .await
        .expect("seed user");
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "profiles".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row {
                values: vec![Value::Integer(id), Value::Text(owner.into())],
            },
        })
        .await
        .expect("seed profile");
    }

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "reader".into(),
        permission: Permission::TableRead {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant users read");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "reader".into(),
        permission: Permission::TableRead {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "profiles".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant profiles read");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "reader".into(),
        permission: Permission::PolicyBypass {
            project_id: "p".into(),
            table_name: None,
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant project policy bypass");
    db.set_read_policy(
        "p",
        "app",
        "users",
        Expr::Eq("owner".into(), Value::Text("$caller_id".into())),
    )
    .await
    .expect("set users policy");
    db.set_read_policy(
        "p",
        "app",
        "profiles",
        Expr::Eq("owner".into(), Value::Text("$caller_id".into())),
    )
    .await
    .expect("set profiles policy");

    let reader = CallerContext::new("reader");
    let result = db
        .query_with_options_as(
            Some(&reader),
            "p",
            "app",
            Query::select(&["*"])
                .from("users")
                .alias("u")
                .inner_join("profiles", "u.id", "user_id")
                .with_last_join_alias("pr")
                .limit(10),
            QueryOptions::default(),
        )
        .await
        .expect("query with project policy bypass");
    assert_eq!(result.rows.len(), 2);
}

#[tokio::test]
async fn join_query_rejects_duplicate_aliases_to_prevent_policy_binding_ambiguity() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "owner".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("users");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "profiles".into(),
        owner_id: None,
        if_not_exists: false,
        columns: vec![
            ColumnDef {
                name: "user_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "owner".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".into()],
    }))
    .await
    .expect("profiles");

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("reader".into())],
        },
    })
    .await
    .expect("seed user");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "profiles".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("bob".into())],
        },
    })
    .await
    .expect("seed profile");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "reader".into(),
        permission: Permission::TableRead {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant users read");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "reader".into(),
        permission: Permission::TableRead {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "profiles".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant profiles read");
    db.set_read_policy(
        "p",
        "app",
        "profiles",
        Expr::Eq("owner".into(), Value::Text("$caller_id".into())),
    )
    .await
    .expect("set policy");

    let reader = CallerContext::new("reader");
    let err = db
        .query_with_options_as(
            Some(&reader),
            "p",
            "app",
            Query::select(&["*"])
                .from("users")
                .alias("u")
                .inner_join("profiles", "u.id", "user_id")
                .with_last_join_alias("u"),
            QueryOptions::default(),
        )
        .await
        .expect_err("duplicate aliases should be rejected");
    assert!(matches!(err, QueryError::InvalidQuery { .. }));
}

#[tokio::test]
async fn production_profile_requires_hmac() {
    let dir = tempdir().expect("temp");
    let invalid = AedbConfig {
        recovery_mode: RecoveryMode::Strict,
        durability_mode: DurabilityMode::Full,
        hash_chain_required: true,
        manifest_hmac_key: None,
        ..AedbConfig::default()
    };
    let err = AedbInstance::open_production(invalid, dir.path())
        .err()
        .expect("must reject missing hmac");
    assert!(matches!(err, AedbError::InvalidConfig { .. }));

    let valid = AedbConfig::production([7u8; 32]);
    AedbInstance::open_production(valid, dir.path()).expect("open production");
}

#[tokio::test]
async fn secure_profile_requires_hardened_storage_settings() {
    let dir = tempdir().expect("temp");
    let weak = AedbConfig::default();
    let err = AedbInstance::open_secure(weak, dir.path())
        .err()
        .expect("must reject weak secure profile");
    assert!(matches!(err, AedbError::InvalidConfig { .. }));

    let hardened = AedbConfig::production([9u8; 32]);
    AedbInstance::open_secure(hardened, dir.path()).expect("open secure");
}

#[tokio::test]
async fn secure_profile_rejects_batch_durability() {
    let dir = tempdir().expect("temp");
    let mut weak = AedbConfig::production([9u8; 32]);
    weak.durability_mode = DurabilityMode::Batch;
    let err = AedbInstance::open_secure(weak, dir.path())
        .err()
        .expect("secure profile must reject batch durability");
    assert!(matches!(err, AedbError::InvalidConfig { .. }));
}

#[tokio::test]
async fn production_profile_rejects_batch_durability() {
    let dir = tempdir().expect("temp");
    let mut weak = AedbConfig::production([7u8; 32]);
    weak.durability_mode = DurabilityMode::Batch;
    let err = AedbInstance::open_production(weak, dir.path())
        .err()
        .expect("production profile must reject batch durability");
    assert!(matches!(err, AedbError::InvalidConfig { .. }));
}

#[tokio::test]
async fn secure_profile_rejects_short_hmac_key() {
    let dir = tempdir().expect("temp");
    let weak = AedbConfig::default()
        .with_hmac_key(vec![1, 2, 3, 4, 5, 6, 7, 8])
        .with_checkpoint_key([3u8; 32]);
    let err = AedbInstance::open_secure(weak, dir.path())
        .err()
        .expect("short hmac key must be rejected");
    assert!(matches!(err, AedbError::InvalidConfig { .. }));
}

#[test]
fn arcana_profile_rejects_short_hmac_key() {
    let weak = AedbConfig::default().with_hmac_key(vec![9u8; 16]);
    let err = crate::lib_helpers::validate_arcana_config(&weak)
        .err()
        .expect("short hmac key must be rejected");
    assert!(matches!(err, AedbError::InvalidConfig { .. }));
}

#[test]
fn low_latency_profile_uses_batch_durability_with_strict_recovery() {
    let cfg = AedbConfig::low_latency([5u8; 32]);
    assert_eq!(cfg.durability_mode, DurabilityMode::Batch);
    assert_eq!(cfg.recovery_mode, RecoveryMode::Strict);
    assert!(cfg.hash_chain_required);
    assert!(cfg.batch_interval_ms > 0);
    assert!(cfg.batch_max_bytes > 0);
    assert!(cfg.manifest_hmac_key.is_some());
}

#[test]
fn checkpoint_compression_level_is_validated() {
    let mut cfg = AedbConfig::default();
    cfg.checkpoint_compression_level = 23;
    let err = crate::lib_helpers::validate_config(&cfg)
        .err()
        .expect("out-of-range compression level must be rejected");
    assert!(matches!(err, AedbError::InvalidConfig { .. }));

    cfg.checkpoint_compression_level = 1;
    crate::lib_helpers::validate_config(&cfg).expect("valid compression level");
}

#[tokio::test]
async fn secure_mode_requires_authenticated_apis() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_production(AedbConfig::production([7u8; 32]), dir.path())
        .expect("open secure");

    let err = db
        .commit(Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }))
        .await
        .expect_err("anonymous commit must fail");
    assert!(matches!(err, AedbError::PermissionDenied(_)));

    db.commit_as(
        CallerContext::system_internal(),
        Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }),
    )
    .await
    .expect("system commit");

    let spoof_err = db
        .commit_as(
            CallerContext::new(SYSTEM_CALLER_ID),
            Mutation::Ddl(DdlOperation::CreateProject {
                owner_id: None,
                if_not_exists: true,
                project_id: "q".into(),
            }),
        )
        .await
        .expect_err("spoofed system caller must fail");
    assert!(matches!(spoof_err, AedbError::PermissionDenied(_)));

    let qerr = db
        .query("p", "app", Query::select(&["*"]).from("users").limit(1))
        .await
        .expect_err("anonymous query must fail");
    assert!(matches!(qerr, QueryError::PermissionDenied { .. }));

    let qerr_direct = db
        .query_with_options_as(
            None,
            "p",
            "app",
            Query::select(&["*"]).from("users").limit(1),
            QueryOptions::default(),
        )
        .await
        .expect_err("anonymous query_with_options_as must fail");
    assert!(matches!(qerr_direct, QueryError::PermissionDenied { .. }));

    let explain_err = db
        .explain_query_as(
            None,
            "p",
            "app",
            Query::select(&["*"]).from("users").limit(1),
            QueryOptions::default(),
        )
        .await
        .expect_err("anonymous explain_query_as must fail");
    assert!(matches!(explain_err, QueryError::PermissionDenied { .. }));
}

#[tokio::test]
async fn commit_as_rejects_empty_caller_id() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    let err = db
        .commit_as(
            CallerContext::new("   "),
            Mutation::Ddl(DdlOperation::CreateProject {
                owner_id: None,
                if_not_exists: true,
                project_id: "p".into(),
            }),
        )
        .await
        .expect_err("empty caller id should be rejected");
    assert!(matches!(err, AedbError::PermissionDenied(_)));
}

#[tokio::test]
async fn query_as_rejects_empty_caller_id() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let caller = CallerContext::new("");
    let err = db
        .query_with_options_as(
            Some(&caller),
            "p",
            "app",
            Query::select(&["*"]).from("authz_audit").limit(1),
            QueryOptions::default(),
        )
        .await
        .expect_err("empty caller id should be rejected");
    assert!(matches!(err, QueryError::PermissionDenied { .. }));
}

#[tokio::test]
async fn query_with_options_as_rejects_reserved_system_caller() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_production(AedbConfig::production([8u8; 32]), dir.path())
        .expect("open secure");
    db.commit_as(
        CallerContext::system_internal(),
        Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }),
    )
    .await
    .expect("create project");

    let caller = CallerContext::new("system");
    let err = db
        .query_with_options_as(
            Some(&caller),
            "p",
            "app",
            Query::select(&["*"]).from("__system_authz"),
            QueryOptions::default(),
        )
        .await
        .expect_err("reserved system caller should be rejected");
    assert!(matches!(err, QueryError::PermissionDenied { .. }));
}

#[tokio::test]
async fn query_no_auth_in_secure_mode_returns_structured_error() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_production(AedbConfig::production([6u8; 32]), dir.path())
        .expect("open secure");
    let err = db
        .query_no_auth(
            "p",
            "app",
            Query::select(&["*"]).from("__system_authz"),
            QueryOptions::default(),
        )
        .await
        .expect_err("secure mode should reject query_no_auth");
    assert!(matches!(err, QueryError::PermissionDenied { .. }));
}

#[tokio::test]
async fn kv_no_auth_apis_in_secure_mode_return_structured_error() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_production(AedbConfig::production([4u8; 32]), dir.path())
        .expect("open secure");

    let get_err = db
        .kv_get_no_auth("p", "app", b"k", ConsistencyMode::AtLatest)
        .await
        .expect_err("secure mode should reject kv_get_no_auth");
    assert!(matches!(get_err, QueryError::PermissionDenied { .. }));

    let scan_err = db
        .kv_scan_prefix_no_auth("p", "app", b"k", 10, ConsistencyMode::AtLatest)
        .await
        .expect_err("secure mode should reject kv_scan_prefix_no_auth");
    assert!(matches!(scan_err, QueryError::PermissionDenied { .. }));
}

#[tokio::test]
async fn existence_and_introspection_apis_report_catalog_state() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.create_project("p").await.expect("project");
    db.create_scope("p", "s1").await.expect("scope");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "s1".into(),
        table_name: "users".into(),
        columns: vec![
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
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: "s1".into(),
        table_name: "users".into(),
        index_name: "idx_users_name".into(),
        if_not_exists: false,
        columns: vec!["name".into()],
        index_type: crate::catalog::schema::IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("index");

    assert!(db.project_exists("p").await.expect("project_exists"));
    assert!(db.scope_exists("p", "s1").await.expect("scope_exists"));
    assert!(
        db.table_exists("p", "s1", "users")
            .await
            .expect("table_exists")
    );
    assert!(
        db.index_exists("p", "s1", "users", "idx_users_name")
            .await
            .expect("index_exists")
    );

    let projects = db.list_projects().await.expect("list projects");
    assert!(projects.iter().any(|p| p.project_id == "p"));

    let scopes = db.list_scopes_info("p").await.expect("list scopes");
    let scope = scopes
        .iter()
        .find(|s| s.scope_id == "s1")
        .expect("scope info");
    assert_eq!(scope.table_count, 1);

    let tables = db.list_tables_info("p", "s1").await.expect("list tables");
    let table = tables
        .iter()
        .find(|t| t.table_name == "users")
        .expect("table info");
    assert_eq!(table.column_count, 2);
    assert_eq!(table.index_count, 1);
}

#[tokio::test]
async fn no_auth_read_apis_work_in_non_secure_mode() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "cfg".into(),
        columns: vec![
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
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create cfg table");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "cfg".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Text("bootstrap".into())],
        },
    })
    .await
    .expect("upsert cfg row");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"config:bootstrap".to_vec(),
        value: b"v1".to_vec(),
    })
    .await
    .expect("set kv");

    let got = db
        .kv_get_no_auth("p", "app", b"config:bootstrap", ConsistencyMode::AtLatest)
        .await
        .expect("kv_get_no_auth")
        .expect("entry");
    assert_eq!(got.value, b"v1".to_vec());

    let scanned = db
        .kv_scan_prefix_no_auth("p", "app", b"config:", 10, ConsistencyMode::AtLatest)
        .await
        .expect("scan no auth");
    assert_eq!(scanned.len(), 1);

    let query_res = db
        .query_no_auth(
            "p",
            "app",
            Query::select(&["id", "name"]).from("cfg").limit(10),
            QueryOptions::default(),
        )
        .await
        .expect("query_no_auth");
    assert_eq!(query_res.rows.len(), 1);
}

#[tokio::test]
async fn ddl_if_not_exists_is_idempotent_and_reports_applied() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    let first = db
        .commit_ddl(DdlOperation::CreateProject {
            owner_id: None,
            project_id: "arcana".into(),
            if_not_exists: true,
        })
        .await
        .expect("create project first");
    assert!(first.applied);

    let second = db
        .commit_ddl(DdlOperation::CreateProject {
            owner_id: None,
            project_id: "arcana".into(),
            if_not_exists: true,
        })
        .await
        .expect("create project second");
    assert!(!second.applied);

    let scope_first = db
        .commit_ddl(DdlOperation::CreateScope {
            owner_id: None,
            project_id: "arcana".into(),
            scope_id: "app".into(),
            if_not_exists: true,
        })
        .await
        .expect("create scope first");
    assert!(!scope_first.applied, "default app scope already exists");

    let table_first = db
        .commit_ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: true,
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
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
            primary_key: vec!["id".into()],
        })
        .await
        .expect("create table first");
    assert!(table_first.applied);

    let table_second = db
        .commit_ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: true,
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
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
            primary_key: vec!["id".into()],
        })
        .await
        .expect("create table second");
    assert!(!table_second.applied);

    let err = db
        .commit(Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
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
            primary_key: vec!["id".into()],
        }))
        .await
        .expect_err("duplicate create should error without if_not_exists");
    assert!(matches!(
        err,
        AedbError::AlreadyExists {
            resource_type: ErrorResourceType::Table,
            ..
        }
    ));

    let drop_noop = db
        .commit_ddl(DdlOperation::DropTable {
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "does_not_exist".into(),
            if_exists: true,
        })
        .await
        .expect("drop noop with if_exists");
    assert!(!drop_noop.applied);

    let drop_err = db
        .commit(Mutation::Ddl(DdlOperation::DropTable {
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "does_not_exist".into(),
            if_exists: false,
        }))
        .await
        .expect_err("drop missing should error when if_exists=false");
    assert!(matches!(
        drop_err,
        AedbError::NotFound {
            resource_type: ErrorResourceType::Table,
            ..
        }
    ));
}

#[tokio::test]
async fn commit_ddl_batch_is_atomic_and_reports_per_op_results() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");

    let failed = db
        .commit_ddl_batch(vec![
            DdlOperation::CreateTable {
                owner_id: None,
                if_not_exists: false,
                project_id: "arcana".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                columns: vec![
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
                primary_key: vec!["id".into()],
            },
            DdlOperation::CreateTable {
                owner_id: None,
                if_not_exists: false,
                project_id: "arcana".into(),
                scope_id: "missing".into(),
                table_name: "invalid".into(),
                columns: vec![ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                }],
                primary_key: vec!["id".into()],
            },
        ])
        .await
        .expect_err("invalid second ddl should fail entire batch");
    assert!(matches!(
        failed,
        AedbError::NotFound {
            resource_type: ErrorResourceType::Scope,
            ..
        }
    ));
    assert!(
        !db.table_exists("arcana", "app", "users")
            .await
            .expect("users table exists check")
    );

    let first = db
        .commit_ddl_batch(vec![DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: true,
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
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
            primary_key: vec!["id".into()],
        }])
        .await
        .expect("first batch");
    assert_eq!(first.results.len(), 1);
    assert!(first.results[0].applied);
    assert!(
        db.table_exists("arcana", "app", "users")
            .await
            .expect("table exists")
    );

    let second = db
        .commit_ddl_batch(vec![DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: true,
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: vec![
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
            primary_key: vec!["id".into()],
        }])
        .await
        .expect("second batch");
    assert_eq!(second.results.len(), 1);
    assert!(!second.results[0].applied);
}

#[tokio::test]
async fn commit_ddl_batch_supports_dependent_ddl_ordering() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    let batch = db
        .commit_ddl_batch(vec![
            DdlOperation::CreateProject {
                owner_id: None,
                project_id: "arcana".into(),
                if_not_exists: true,
            },
            DdlOperation::CreateScope {
                owner_id: None,
                project_id: "arcana".into(),
                scope_id: "ops".into(),
                if_not_exists: true,
            },
            DdlOperation::CreateTable {
                owner_id: None,
                if_not_exists: true,
                project_id: "arcana".into(),
                scope_id: "ops".into(),
                table_name: "users".into(),
                columns: vec![
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
                primary_key: vec!["id".into()],
            },
        ])
        .await
        .expect("dependent batch");

    assert_eq!(batch.results.len(), 3);
    assert!(batch.results.iter().all(|r| r.applied));
    assert!(
        db.project_exists("arcana")
            .await
            .expect("project exists after batch")
    );
    assert!(
        db.scope_exists("arcana", "ops")
            .await
            .expect("scope exists after batch")
    );
    assert!(
        db.table_exists("arcana", "ops", "users")
            .await
            .expect("table exists after batch")
    );
}

#[tokio::test]
async fn dependency_aware_ddl_batch_reorders_create_dependencies() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");

    let batch = db
        .commit_ddl_batch_dependency_aware(vec![
            DdlOperation::CreateIndex {
                project_id: "arcana".into(),
                scope_id: "ops".into(),
                table_name: "users".into(),
                index_name: "idx_users_name".into(),
                if_not_exists: true,
                columns: vec!["name".into()],
                index_type: crate::catalog::schema::IndexType::BTree,
                partial_filter: None,
            },
            DdlOperation::CreateTable {
                owner_id: None,
                if_not_exists: true,
                project_id: "arcana".into(),
                scope_id: "ops".into(),
                table_name: "users".into(),
                columns: vec![
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
                primary_key: vec!["id".into()],
            },
            DdlOperation::CreateScope {
                owner_id: None,
                project_id: "arcana".into(),
                scope_id: "ops".into(),
                if_not_exists: true,
            },
            DdlOperation::CreateProject {
                owner_id: None,
                project_id: "arcana".into(),
                if_not_exists: true,
            },
        ])
        .await
        .expect("dependency-aware batch");

    assert_eq!(batch.results.len(), 4);
    assert!(
        db.index_exists("arcana", "ops", "users", "idx_users_name")
            .await
            .expect("index exists")
    );
}

#[tokio::test]
async fn ddl_errors_expose_stable_error_codes() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");

    let err = db
        .commit(Mutation::Ddl(DdlOperation::CreateIndex {
            project_id: "arcana".into(),
            scope_id: "app".into(),
            table_name: "missing_users".into(),
            index_name: "idx_missing".into(),
            if_not_exists: false,
            columns: vec!["name".into()],
            index_type: crate::catalog::schema::IndexType::BTree,
            partial_filter: None,
        }))
        .await
        .expect_err("create index on missing table should fail");

    assert_eq!(err.code(), AedbErrorCode::TableNotFound);
}

#[tokio::test]
async fn index_introspection_apis_return_index_definitions() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.commit_ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: true,
        project_id: "arcana".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        columns: vec![
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
        primary_key: vec!["id".into()],
    })
    .await
    .expect("create table");
    db.commit_ddl(DdlOperation::CreateIndex {
        project_id: "arcana".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        index_name: "idx_users_name".into(),
        if_not_exists: false,
        columns: vec!["name".into()],
        index_type: crate::catalog::schema::IndexType::BTree,
        partial_filter: None,
    })
    .await
    .expect("create index");

    let listed = db
        .list_indexes("arcana", "app", "users")
        .await
        .expect("list indexes");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].index_name, "idx_users_name");

    let described = db
        .describe_index("arcana", "app", "users", "idx_users_name")
        .await
        .expect("describe index");
    assert_eq!(described.columns, vec!["name".to_string()]);
}

#[tokio::test]
async fn lifecycle_hooks_receive_post_commit_events_for_applied_ddl() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hook: Arc<dyn LifecycleHook> = Arc::new(RecordingLifecycleHook {
        events: Arc::clone(&events),
    });
    db.add_lifecycle_hook(Arc::clone(&hook));

    db.commit_ddl(DdlOperation::CreateProject {
        owner_id: None,
        project_id: "arcana".into(),
        if_not_exists: true,
    })
    .await
    .expect("create project");
    db.commit_ddl(DdlOperation::CreateProject {
        owner_id: None,
        project_id: "arcana".into(),
        if_not_exists: true,
    })
    .await
    .expect("idempotent create");

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if !events.lock().expect("events lock").is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let seen = events.lock().expect("events lock");
    assert_eq!(seen.len(), 1);
    assert!(matches!(
        &seen[0],
        LifecycleEvent::ProjectCreated { project_id, .. } if project_id == "arcana"
    ));
}

#[tokio::test]
async fn lifecycle_outbox_persists_applied_events() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    let created = db
        .commit_ddl(DdlOperation::CreateProject {
            owner_id: None,
            project_id: "arcana".into(),
            if_not_exists: true,
        })
        .await
        .expect("create project");

    let outbox = db
        .query_no_auth(
            "_system",
            "app",
            Query::select(&["event_count", "events"])
                .from("lifecycle_outbox")
                .where_(Expr::Eq(
                    "commit_seq".into(),
                    Value::Integer(created.seq as i64),
                ))
                .limit(1),
            QueryOptions::default(),
        )
        .await
        .expect("query lifecycle outbox");
    assert_eq!(outbox.rows.len(), 1, "expected lifecycle outbox row");
    assert_eq!(outbox.rows[0].values[0], Value::Integer(1));
    let Value::Json(payload) = &outbox.rows[0].values[1] else {
        panic!("expected json payload");
    };
    let events: Vec<LifecycleEvent> =
        serde_json::from_str(payload.as_str()).expect("decode lifecycle payload");
    assert!(matches!(
        events.first(),
        Some(LifecycleEvent::ProjectCreated { project_id, seq })
            if project_id == "arcana" && *seq == created.seq
    ));
}

#[tokio::test]
async fn idempotency_prunes_by_commit_window() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(
        AedbConfig {
            idempotency_window_commits: 1,
            ..AedbConfig::default()
        },
        dir.path(),
    )
    .expect("open");
    db.create_project("p").await.expect("project");

    let first = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: Some(IdempotencyKey([1u8; 16])),
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: crate::commit::tx::ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                }],
            },
            base_seq: 0,
        })
        .await
        .expect("first commit");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"other".to_vec(),
        value: b"v2".to_vec(),
    })
    .await
    .expect("advance seq");

    let retried = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: Some(IdempotencyKey([1u8; 16])),
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: crate::commit::tx::ReadSet::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"k".to_vec(),
                    value: b"v3".to_vec(),
                }],
            },
            base_seq: 0,
        })
        .await
        .expect("retried commit");
    assert!(
        retried.commit_seq > first.commit_seq,
        "idempotency record should expire by sequence window"
    );
}

#[test]
fn open_rejects_invalid_config() {
    let dir = tempdir().expect("temp");
    let bad = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 0,
        ..AedbConfig::default()
    };
    let err = AedbInstance::open(bad, dir.path())
        .err()
        .expect("invalid config");
    assert!(matches!(err, crate::error::AedbError::InvalidConfig { .. }));

    let too_large_txn = AedbConfig {
        max_transaction_bytes: crate::wal::frame::MAX_FRAME_BODY_BYTES + 1,
        ..AedbConfig::default()
    };
    let err = AedbInstance::open(too_large_txn, dir.path())
        .err()
        .expect("oversized transaction bound");
    assert!(matches!(err, crate::error::AedbError::InvalidConfig { .. }));

    let deadlock_unsafe = AedbConfig {
        recovery_mode: RecoveryMode::Strict,
        coordinator_locking_enabled: false,
        ..AedbConfig::default()
    };
    let err = AedbInstance::open(deadlock_unsafe, dir.path())
        .err()
        .expect("strict mode must require coordinator locking");
    assert!(matches!(err, crate::error::AedbError::InvalidConfig { .. }));
}

#[tokio::test]
async fn snapshot_limit_enforced_on_read_path() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(
        AedbConfig {
            max_concurrent_snapshots: 1,
            ..AedbConfig::default()
        },
        dir.path(),
    )
    .expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant");
    let caller = CallerContext::new("alice");

    let handle = {
        let mut mgr = db.snapshot_manager.lock();
        mgr.acquire_bounded(
            crate::snapshot::reader::SnapshotReadView {
                keyspace: Arc::new(crate::storage::keyspace::Keyspace::default().snapshot()),
                catalog: Arc::new(crate::catalog::Catalog::default()),
                seq: 0,
            },
            1,
        )
        .expect("occupy")
    };

    let err = db
        .kv_get("p", "app", b"k", ConsistencyMode::AtLatest, &caller)
        .await
        .expect_err("snapshot cap");
    assert!(matches!(
        err,
        crate::query::error::QueryError::SnapshotLimitReached
    ));

    let mut mgr = db.snapshot_manager.lock();
    mgr.release(handle);
    let _ = mgr.gc();
}

#[tokio::test]
async fn metrics_surface_reflects_commits() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let m = db.metrics();
    assert!(m.commits_total >= 1);
    let op = db.operational_metrics().await;
    assert!(op.commits_total >= 1);
    assert!(op.read_set_conflicts <= op.conflict_rejections);
    assert!(op.queue_depth >= op.inflight_commits);
    assert!(op.snapshot_age_micros <= u64::MAX / 2);
}

#[tokio::test]
async fn checkpoint_now_enables_clean_restart() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
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
                name: "name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("table");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users".into(),
        primary_key: vec![Value::Integer(5)],
        row: Row {
            values: vec![Value::Integer(5), Value::Text("bob".into())],
        },
    })
    .await
    .expect("insert");

    let seq = db.checkpoint_now().await.expect("checkpoint");
    assert!(seq >= 3);

    let reopened = AedbInstance::open(AedbConfig::default(), dir.path()).expect("reopen");
    let rows = reopened
        .query(
            "p",
            "app",
            Query::select(&["*"])
                .from("users")
                .where_(Expr::Eq("id".into(), Value::Integer(5))),
        )
        .await
        .expect("query");
    assert_eq!(rows.rows.len(), 1);
}

#[tokio::test]
async fn checkpoint_now_in_batch_mode_flushes_wal_and_recovers_tail() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"tail".to_vec(),
        value: b"v1".to_vec(),
    })
    .await
    .expect("tail write");

    let before = db.head_state().await;
    assert!(before.visible_head_seq > before.durable_head_seq);

    let cp_seq = db.checkpoint_now().await.expect("checkpoint");
    let after = db.head_state().await;
    assert_eq!(after.visible_head_seq, after.durable_head_seq);
    assert!(after.durable_head_seq >= cp_seq);
    drop(db);

    let recovered = crate::recovery::recover_with_config(dir.path(), &config).expect("recover");
    let tail = recovered
        .keyspace
        .kv_get("p", "app", b"tail")
        .expect("tail recovered");
    assert_eq!(tail.value, b"v1".to_vec());
}

#[tokio::test]
async fn commit_with_visible_finality_can_return_before_durable_head_in_batch_mode() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let result = db
        .commit_with_finality(
            Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"fast-visible".to_vec(),
                value: b"v".to_vec(),
            },
            CommitFinality::Visible,
        )
        .await
        .expect("commit");

    assert!(
        result.durable_head_seq < result.commit_seq,
        "visible finality should not require durable head in batch mode"
    );
}

#[tokio::test]
async fn commit_with_durable_finality_waits_until_durable_head_catches_up() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let db = Arc::new(AedbInstance::open(config, dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    let fsync_db = Arc::clone(&db);
    let fsync_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        fsync_db.force_fsync().await.expect("force fsync");
    });

    let started = Instant::now();
    let result = db
        .commit_with_finality(
            Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"fast-durable".to_vec(),
                value: b"v".to_vec(),
            },
            CommitFinality::Durable,
        )
        .await
        .expect("commit");
    fsync_task.await.expect("join fsync");

    assert!(
        started.elapsed() >= Duration::from_millis(15),
        "durable finality should wait for WAL durability in batch mode"
    );
    assert!(
        result.durable_head_seq >= result.commit_seq,
        "durable finality must report durable head at or beyond commit sequence"
    );
}

#[tokio::test]
async fn order_book_new_with_durable_finality_waits_until_durable_head_catches_up() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let db = Arc::new(AedbInstance::open(config, dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    let fsync_db = Arc::clone(&db);
    let fsync_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        fsync_db.force_fsync().await.expect("force fsync");
    });

    let started = Instant::now();
    let result = db
        .order_book_new_with_finality(
            "p",
            "app",
            crate::order_book::OrderRequest {
                instrument: "BTC-USD".into(),
                client_order_id: "oid-1".into(),
                side: crate::order_book::OrderSide::Bid,
                order_type: crate::order_book::OrderType::Limit,
                time_in_force: crate::order_book::TimeInForce::Gtc,
                exec_instructions: crate::order_book::ExecInstruction(0),
                self_trade_prevention: crate::order_book::SelfTradePrevention::None,
                price_ticks: 100,
                qty_be: {
                    let mut out = [0u8; 32];
                    out[31] = 1;
                    out
                },
                owner: "alice".into(),
                account: None,
                nonce: 1,
                price_limit_ticks: None,
            },
            CommitFinality::Durable,
        )
        .await
        .expect("order");
    fsync_task.await.expect("join fsync");

    assert!(
        started.elapsed() >= Duration::from_millis(15),
        "durable finality should wait for WAL durability in batch mode"
    );
    assert!(
        result.durable_head_seq >= result.commit_seq,
        "durable finality must report durable head at or beyond commit sequence"
    );
}

#[tokio::test]
async fn order_book_new_fok_reject_is_dropped_before_wal_append() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let before = db.operational_metrics().await;

    let err = db
        .order_book_new(
            "p",
            "app",
            crate::order_book::OrderRequest {
                instrument: "BTC-USD".into(),
                client_order_id: "fok-no-liq-1".into(),
                side: crate::order_book::OrderSide::Bid,
                order_type: crate::order_book::OrderType::Limit,
                time_in_force: crate::order_book::TimeInForce::Fok,
                exec_instructions: crate::order_book::ExecInstruction(0),
                self_trade_prevention: crate::order_book::SelfTradePrevention::None,
                price_ticks: 100,
                qty_be: {
                    let mut out = [0u8; 32];
                    out[31] = 1;
                    out
                },
                owner: "alice".into(),
                account: None,
                nonce: 1,
                price_limit_ticks: None,
            },
        )
        .await
        .expect_err("unfillable FOK should reject upstream");
    let after = db.operational_metrics().await;

    match err {
        AedbError::Validation(msg) => assert!(
            msg.contains("fok cannot fill"),
            "unexpected validation message: {msg}"
        ),
        other => panic!("unexpected error variant: {other:?}"),
    }
    assert_eq!(
        after.wal_append_ops, before.wal_append_ops,
        "upstream dropped rejects should not append WAL frames"
    );
    assert_eq!(
        after.wal_append_bytes, before.wal_append_bytes,
        "upstream dropped rejects should not increase WAL append bytes"
    );
}

#[tokio::test]
#[ignore = "long-running finality latency profile"]
async fn finality_profile_visible_vs_durable_low_latency_mode() {
    async fn run_profile(
        config: AedbConfig,
        finality: CommitFinality,
        ops: usize,
    ) -> (u64, u64, u64, crate::OperationalMetrics) {
        let dir = tempdir().expect("temp");
        let db = AedbInstance::open(config, dir.path()).expect("open");
        db.create_project("p").await.expect("project");
        let started = Instant::now();
        let mut lat_sum = 0u128;
        let mut lat_max = 0u64;
        for i in 0..ops {
            let op_started = Instant::now();
            db.commit_with_finality(
                Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: format!("finality:{finality:?}:{i}").into_bytes(),
                    value: i.to_be_bytes().to_vec(),
                },
                finality,
            )
            .await
            .expect("commit with finality");
            let us = op_started.elapsed().as_micros() as u64;
            lat_sum = lat_sum.saturating_add(us as u128);
            lat_max = lat_max.max(us);
        }
        db.force_fsync().await.expect("flush");
        let elapsed = started.elapsed().as_secs_f64().max(0.001);
        let tps = (ops as f64 / elapsed) as u64;
        let avg_us = (lat_sum / ops.max(1) as u128) as u64;
        let op = db.operational_metrics().await;
        (tps, avg_us, lat_max, op)
    }

    let ops = std::env::var("AEDB_FINALITY_PROFILE_OPS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(600)
        .max(200);

    let mut low_latency_no_coalesce = AedbConfig::low_latency([1u8; 32]);
    low_latency_no_coalesce.durable_ack_coalescing_enabled = false;
    low_latency_no_coalesce.durable_ack_coalesce_window_us = 0;
    let low_latency_coalesce = AedbConfig::low_latency([1u8; 32]);

    let (visible_tps, visible_avg_us, visible_max_us, visible_op) = run_profile(
        low_latency_no_coalesce.clone(),
        CommitFinality::Visible,
        ops,
    )
    .await;
    let (durable_base_tps, durable_base_avg_us, durable_base_max_us, durable_base_op) =
        run_profile(low_latency_no_coalesce, CommitFinality::Durable, ops).await;
    let (durable_tps, durable_avg_us, durable_max_us, durable_op) =
        run_profile(low_latency_coalesce, CommitFinality::Durable, ops).await;

    eprintln!(
        "finality_profile: ops={} visible_tps={} durable_base_tps={} durable_coalesced_tps={} visible_avg_us={} durable_base_avg_us={} durable_coalesced_avg_us={} visible_max_us={} durable_base_max_us={} durable_coalesced_max_us={} visible_durable_wait_ops={} durable_base_wait_ops={} durable_coalesced_wait_ops={} visible_avg_durable_wait_us={} durable_base_avg_durable_wait_us={} durable_coalesced_avg_durable_wait_us={} visible_wal_sync_ops={} durable_base_wal_sync_ops={} durable_coalesced_wal_sync_ops={} visible_avg_wal_sync_us={} durable_base_avg_wal_sync_us={} durable_coalesced_avg_wal_sync_us={} visible_avg_wal_append_us={} durable_base_avg_wal_append_us={} durable_coalesced_avg_wal_append_us={}",
        ops,
        visible_tps,
        durable_base_tps,
        durable_tps,
        visible_avg_us,
        durable_base_avg_us,
        durable_avg_us,
        visible_max_us,
        durable_base_max_us,
        durable_max_us,
        visible_op.durable_wait_ops,
        durable_base_op.durable_wait_ops,
        durable_op.durable_wait_ops,
        visible_op.avg_durable_wait_micros,
        durable_base_op.avg_durable_wait_micros,
        durable_op.avg_durable_wait_micros,
        visible_op.wal_sync_ops,
        durable_base_op.wal_sync_ops,
        durable_op.wal_sync_ops,
        visible_op.avg_wal_sync_micros,
        durable_base_op.avg_wal_sync_micros,
        durable_op.avg_wal_sync_micros,
        visible_op.avg_wal_append_micros,
        durable_base_op.avg_wal_append_micros,
        durable_op.avg_wal_append_micros
    );

    assert_eq!(
        visible_op.queue_full_rejections, 0,
        "visible finality profile should not saturate queue"
    );
    assert_eq!(
        durable_base_op.queue_full_rejections, 0,
        "durable baseline profile should not saturate queue"
    );
    assert_eq!(
        durable_op.queue_full_rejections, 0,
        "durable coalesced profile should not saturate queue"
    );
    assert_eq!(
        visible_op.timeout_rejections, 0,
        "visible finality profile should not timeout"
    );
    assert_eq!(
        durable_base_op.timeout_rejections, 0,
        "durable baseline profile should not timeout"
    );
    assert_eq!(
        durable_op.timeout_rejections, 0,
        "durable coalesced profile should not timeout"
    );
    assert_eq!(
        visible_op.durable_wait_ops, 0,
        "visible finality profile should not accumulate durable wait operations"
    );
    assert!(
        durable_op.durable_wait_ops > 0,
        "durable finality profile should accumulate durable wait operations"
    );
    assert!(
        durable_tps >= durable_base_tps.saturating_div(2),
        "coalesced durable finality regressed severely: base={durable_base_tps} coalesced={durable_tps}"
    );
    assert!(
        durable_tps <= visible_tps.saturating_mul(2),
        "durable finality profile produced implausible TPS vs visible: visible={visible_tps} durable={durable_tps}"
    );
}

#[tokio::test]
async fn order_book_write_requires_authenticated_caller_in_secure_mode() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_secure(AedbConfig::production([9u8; 32]), dir.path())
        .expect("open secure");
    let err = db
        .order_book_new(
            "p",
            "app",
            crate::order_book::OrderRequest {
                instrument: "BTC-USD".into(),
                client_order_id: "oid-secure".into(),
                side: crate::order_book::OrderSide::Bid,
                order_type: crate::order_book::OrderType::Limit,
                time_in_force: crate::order_book::TimeInForce::Gtc,
                exec_instructions: crate::order_book::ExecInstruction(0),
                self_trade_prevention: crate::order_book::SelfTradePrevention::None,
                price_ticks: 100,
                qty_be: {
                    let mut out = [0u8; 32];
                    out[31] = 1;
                    out
                },
                owner: "alice".into(),
                account: None,
                nonce: 1,
                price_limit_ticks: None,
            },
        )
        .await
        .expect_err("secure mode should require authenticated caller");
    assert!(matches!(err, AedbError::PermissionDenied(_)));
}

#[tokio::test]
async fn secure_mode_supports_order_book_writes_via_authenticated_as_apis() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_secure(AedbConfig::production([5u8; 32]), dir.path())
        .expect("open secure");
    db.commit_as(
        CallerContext::system_internal(),
        Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }),
    )
    .await
    .expect("create project");
    db.commit_as(
        CallerContext::system_internal(),
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "alice".into(),
            permission: Permission::ProjectAdmin {
                project_id: "p".into(),
            },
            actor_id: Some("system".into()),
            delegable: false,
        }),
    )
    .await
    .expect("grant project admin");

    let alice = CallerContext::new("alice");
    db.order_book_new_as(
        alice.clone(),
        "p",
        "app",
        crate::order_book::OrderRequest {
            instrument: "BTC-USD".into(),
            client_order_id: "oid-secure-as".into(),
            side: crate::order_book::OrderSide::Bid,
            order_type: crate::order_book::OrderType::Limit,
            time_in_force: crate::order_book::TimeInForce::Gtc,
            exec_instructions: crate::order_book::ExecInstruction(0),
            self_trade_prevention: crate::order_book::SelfTradePrevention::None,
            price_ticks: 100,
            qty_be: {
                let mut out = [0u8; 32];
                out[31] = 2;
                out
            },
            owner: "alice".into(),
            account: None,
            nonce: 1,
            price_limit_ticks: None,
        },
    )
    .await
    .expect("place order");
    db.order_book_cancel_as(alice.clone(), "p", "app", "BTC-USD", 1, "alice")
        .await
        .expect("cancel order");
    let status = db
        .order_status("p", "app", "BTC-USD", 1, ConsistencyMode::AtLatest, &alice)
        .await
        .expect("status query")
        .expect("order exists");
    assert_eq!(status.status, crate::order_book::OrderStatus::Cancelled);
}

#[tokio::test]
async fn open_orders_requires_kv_read_permission() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.order_book_new(
        "p",
        "app",
        crate::order_book::OrderRequest {
            instrument: "BTC-USD".into(),
            client_order_id: "cid-open-orders-1".into(),
            side: crate::order_book::OrderSide::Bid,
            order_type: crate::order_book::OrderType::Limit,
            time_in_force: crate::order_book::TimeInForce::Gtc,
            exec_instructions: crate::order_book::ExecInstruction(0),
            self_trade_prevention: crate::order_book::SelfTradePrevention::None,
            price_ticks: 100,
            qty_be: {
                let mut out = [0u8; 32];
                out[31] = 1;
                out
            },
            owner: "alice".into(),
            account: None,
            nonce: 1,
            price_limit_ticks: None,
        },
    )
    .await
    .expect("place order");

    let alice = CallerContext::new("alice");
    let denied = db
        .open_orders(
            "p",
            "app",
            "BTC-USD",
            "alice",
            ConsistencyMode::AtLatest,
            &alice,
        )
        .await
        .expect_err("missing KvRead should be denied");
    assert!(matches!(denied, QueryError::PermissionDenied { .. }));

    db.commit_ddl(DdlOperation::GrantPermission {
        caller_id: "alice".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: Some(b"ob:BTC-USD:".to_vec()),
        },
        actor_id: None,
        delegable: false,
    })
    .await
    .expect("grant kv read");

    let open = db
        .open_orders(
            "p",
            "app",
            "BTC-USD",
            "alice",
            ConsistencyMode::AtLatest,
            &alice,
        )
        .await
        .expect("open orders");
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].owner, "alice");
}

#[tokio::test]
async fn secure_multi_agent_user_perspective_invariants_hold() {
    fn u256_be(v: u64) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[24..].copy_from_slice(&v.to_be_bytes());
        out
    }

    fn req(
        instrument: &str,
        owner: &str,
        cid: String,
        side: crate::order_book::OrderSide,
        tif: crate::order_book::TimeInForce,
        price: i64,
        qty: u64,
        nonce: u64,
    ) -> crate::order_book::OrderRequest {
        crate::order_book::OrderRequest {
            instrument: instrument.to_string(),
            client_order_id: cid,
            side,
            order_type: crate::order_book::OrderType::Limit,
            time_in_force: tif,
            exec_instructions: crate::order_book::ExecInstruction(0),
            self_trade_prevention: crate::order_book::SelfTradePrevention::None,
            price_ticks: price,
            qty_be: u256_be(qty),
            owner: owner.to_string(),
            account: None,
            nonce,
            price_limit_ticks: None,
        }
    }

    #[derive(Debug, Default)]
    struct AgentMetrics {
        primary_attempted: usize,
        primary_accepted: usize,
        primary_rejected: usize,
        lifecycle_attempted: usize,
        lifecycle_accepted: usize,
        lifecycle_rejected: usize,
        own_read_checks: usize,
    }

    let dir = tempdir().expect("temp");
    let db = Arc::new(
        AedbInstance::open_secure(AedbConfig::production([6u8; 32]), dir.path())
            .expect("open secure"),
    );
    let system = CallerContext::system_internal();
    db.commit_as(
        system.clone(),
        Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }),
    )
    .await
    .expect("create project");

    let agents: Vec<String> = (0..8).map(|i| format!("agent_{i}")).collect();
    for a in &agents {
        db.commit_as(
            system.clone(),
            Mutation::Ddl(DdlOperation::GrantPermission {
                caller_id: a.clone(),
                permission: Permission::ProjectAdmin {
                    project_id: "p".into(),
                },
                actor_id: Some("system".into()),
                delegable: false,
            }),
        )
        .await
        .expect("grant project admin");
    }

    db.order_book_set_instrument_config_as(
        system.clone(),
        "p",
        "app",
        "BTC-USD",
        crate::order_book::InstrumentConfig {
            instrument: "BTC-USD".into(),
            tick_size: 1,
            lot_size_be: u256_be(1),
            min_price_ticks: 1,
            max_price_ticks: 1_000_000,
            market_order_price_band: Some(50),
            halted: false,
            balance_config: None,
        },
    )
    .await
    .expect("instrument config");

    for i in 0..16u64 {
        db.order_book_new_as(
            system.clone(),
            "p",
            "app",
            req(
                "BTC-USD",
                &format!("seed_ask_{i}"),
                format!("seed-ask-{i}"),
                crate::order_book::OrderSide::Ask,
                crate::order_book::TimeInForce::Gtc,
                1_000 + i as i64,
                10,
                1,
            ),
        )
        .await
        .expect("seed ask");
        db.order_book_new_as(
            system.clone(),
            "p",
            "app",
            req(
                "BTC-USD",
                &format!("seed_bid_{i}"),
                format!("seed-bid-{i}"),
                crate::order_book::OrderSide::Bid,
                crate::order_book::TimeInForce::Gtc,
                999 - i as i64,
                10,
                1,
            ),
        )
        .await
        .expect("seed bid");
    }

    let mut anchors: Vec<(String, u64)> = Vec::with_capacity(agents.len());
    for (idx, agent_id) in agents.iter().enumerate() {
        let caller = CallerContext::new(agent_id.clone());
        let anchor_cid = format!("anchor-{agent_id}");
        db.order_book_new_as(
            caller.clone(),
            "p",
            "app",
            req(
                "BTC-USD",
                agent_id,
                anchor_cid.clone(),
                if idx % 2 == 0 {
                    crate::order_book::OrderSide::Bid
                } else {
                    crate::order_book::OrderSide::Ask
                },
                crate::order_book::TimeInForce::Gtc,
                if idx % 2 == 0 { 980 } else { 1_020 },
                2,
                1,
            ),
        )
        .await
        .expect("anchor order");
        let own_open = db
            .open_orders(
                "p",
                "app",
                "BTC-USD",
                agent_id,
                ConsistencyMode::AtLatest,
                &caller,
            )
            .await
            .expect("own open orders");
        let anchor = own_open
            .into_iter()
            .find(|o| o.client_order_id == anchor_cid)
            .expect("anchor order discoverable");
        anchors.push((agent_id.clone(), anchor.order_id));
    }

    let mut tasks = Vec::with_capacity(agents.len());
    for (idx, agent_id) in agents.iter().enumerate() {
        let db_clone = Arc::clone(&db);
        let caller = CallerContext::new(agent_id.clone());
        let owner = agent_id.clone();
        tasks.push(tokio::spawn(async move {
            let mut m = AgentMetrics::default();
            let mut nonce = 10u64;
            for op in 0..180usize {
                let side = if (op + idx) % 2 == 0 {
                    crate::order_book::OrderSide::Bid
                } else {
                    crate::order_book::OrderSide::Ask
                };
                let price = if matches!(side, crate::order_book::OrderSide::Bid) {
                    1_001
                } else {
                    998
                };
                m.primary_attempted += 1;
                let res = db_clone
                    .order_book_new_as(
                        caller.clone(),
                        "p",
                        "app",
                        req(
                            "BTC-USD",
                            &owner,
                            format!("{owner}-p-{op}"),
                            side,
                            crate::order_book::TimeInForce::Ioc,
                            price,
                            1 + (op % 4) as u64,
                            nonce,
                        ),
                    )
                    .await;
                nonce += 1;
                match res {
                    Ok(_) => m.primary_accepted += 1,
                    Err(AedbError::Validation(_)) | Err(AedbError::Conflict(_)) => {
                        m.primary_rejected += 1
                    }
                    Err(other) => return Err(other),
                }

                if op % 30 == 0 {
                    m.lifecycle_attempted += 1;
                    let cid = format!("{owner}-l-{op}");
                    let opened = db_clone
                        .order_book_new_as(
                            caller.clone(),
                            "p",
                            "app",
                            req(
                                "BTC-USD",
                                &owner,
                                cid.clone(),
                                crate::order_book::OrderSide::Bid,
                                crate::order_book::TimeInForce::Gtc,
                                970,
                                1,
                                nonce,
                            ),
                        )
                        .await;
                    nonce += 1;
                    match opened {
                        Ok(_) => m.lifecycle_accepted += 1,
                        Err(AedbError::Validation(_)) | Err(AedbError::Conflict(_)) => {
                            m.lifecycle_rejected += 1
                        }
                        Err(other) => return Err(other),
                    }

                    m.lifecycle_attempted += 1;
                    match db_clone
                        .order_book_cancel_by_client_id_as(
                            caller.clone(),
                            "p",
                            "app",
                            "BTC-USD",
                            &cid,
                            &owner,
                        )
                        .await
                    {
                        Ok(_) => m.lifecycle_accepted += 1,
                        Err(AedbError::Validation(_)) | Err(AedbError::Conflict(_)) => {
                            m.lifecycle_rejected += 1
                        }
                        Err(other) => return Err(other),
                    }
                }

                if op % 40 == 0 {
                    let own = db_clone
                        .open_orders(
                            "p",
                            "app",
                            "BTC-USD",
                            &owner,
                            ConsistencyMode::AtLatest,
                            &caller,
                        )
                        .await
                        .map_err(|e| {
                            AedbError::Validation(format!("own open_orders failed: {e}"))
                        })?;
                    assert!(
                        own.iter().all(|o| o.owner == owner),
                        "open_orders must only return owner rows"
                    );
                    m.own_read_checks += 1;
                }
            }
            Ok::<_, AedbError>(m)
        }));
    }

    let mut metrics = Vec::with_capacity(agents.len());
    for task in tasks {
        metrics.push(task.await.expect("join agent task").expect("agent run"));
    }

    for (idx, agent_id) in agents.iter().enumerate() {
        let caller = CallerContext::new(agent_id.clone());
        let (target_owner, target_order_id) = &anchors[(idx + 1) % anchors.len()];

        let err = db
            .order_status(
                "p",
                "app",
                "BTC-USD",
                *target_order_id,
                ConsistencyMode::AtLatest,
                &caller,
            )
            .await
            .expect_err("cross-owner order_status must be denied");
        assert!(
            matches!(err, QueryError::PermissionDenied { .. }),
            "expected permission denied, got {err:?}"
        );

        let err = db
            .open_orders(
                "p",
                "app",
                "BTC-USD",
                target_owner,
                ConsistencyMode::AtLatest,
                &caller,
            )
            .await
            .expect_err("cross-owner open_orders must be denied");
        assert!(
            matches!(err, QueryError::PermissionDenied { .. }),
            "expected permission denied, got {err:?}"
        );
    }

    for m in &metrics {
        assert_eq!(
            m.primary_accepted + m.primary_rejected,
            m.primary_attempted,
            "primary accounting mismatch"
        );
        assert_eq!(
            m.lifecycle_accepted + m.lifecycle_rejected,
            m.lifecycle_attempted,
            "lifecycle accounting mismatch"
        );
        assert!(
            m.own_read_checks > 0,
            "agent should perform own-read checks"
        );
    }
}

#[tokio::test]
async fn commit_success_is_observable_at_its_commit_seq() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let result = db
        .commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"inclusion-proof".to_vec(),
            value: b"ok".to_vec(),
        })
        .await
        .expect("commit");

    let at_seq = db
        .kv_get_no_auth(
            "p",
            "app",
            b"inclusion-proof",
            ConsistencyMode::AtSeq(result.commit_seq),
        )
        .await
        .expect("kv_get at seq")
        .expect("value present at commit seq");
    assert_eq!(at_seq.value, b"ok".to_vec());
}

#[tokio::test]
async fn failed_multi_mutation_envelope_is_atomic_and_has_no_partial_effects() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let before = db.head_state().await.visible_head_seq;
    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: Default::default(),
            write_intent: WriteIntent {
                mutations: vec![
                    Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"atomic-a".to_vec(),
                        value: b"1".to_vec(),
                    },
                    Mutation::KvDecU256 {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"missing-counter".to_vec(),
                        amount_be: {
                            let mut out = [0u8; 32];
                            out[31] = 1;
                            out
                        },
                    },
                ],
            },
            base_seq: before,
        })
        .await
        .expect_err("envelope should fail");
    assert!(
        matches!(
            err,
            AedbError::Underflow | AedbError::Validation(_) | AedbError::Conflict(_)
        ),
        "expected semantic failure, got: {err:?}"
    );

    let after = db.head_state().await.visible_head_seq;
    assert_eq!(after, before, "failed envelope must not advance head");
    let leaked = db
        .kv_get_no_auth("p", "app", b"atomic-a", ConsistencyMode::AtLatest)
        .await
        .expect("kv_get")
        .is_some();
    assert!(
        !leaked,
        "failed envelope must not partially apply mutations"
    );
}

#[tokio::test]
async fn idempotent_retry_does_not_double_apply_non_idempotent_mutation() {
    let dir = tempdir().expect("temp");
    let db = Arc::new(AedbInstance::open(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    let key = IdempotencyKey([42u8; 16]);
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let db = Arc::clone(&db);
        let key = key.clone();
        tasks.push(tokio::spawn(async move {
            db.commit_envelope(TransactionEnvelope {
                caller: None,
                idempotency_key: Some(key),
                write_class: WriteClass::Standard,
                assertions: Vec::new(),
                read_set: Default::default(),
                write_intent: WriteIntent {
                    mutations: vec![Mutation::KvIncU256 {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"idem-counter".to_vec(),
                        amount_be: {
                            let mut out = [0u8; 32];
                            out[31] = 1;
                            out
                        },
                    }],
                },
                base_seq: 0,
            })
            .await
            .expect("idempotent commit")
        }));
    }

    let mut seqs = std::collections::BTreeSet::new();
    for t in tasks {
        let res = t.await.expect("join");
        seqs.insert(res.commit_seq);
    }
    assert_eq!(seqs.len(), 1, "all retries must resolve to one commit_seq");

    let entry = db
        .kv_get_no_auth("p", "app", b"idem-counter", ConsistencyMode::AtLatest)
        .await
        .expect("kv_get")
        .expect("counter exists");
    assert_eq!(
        primitive_types::U256::from_big_endian(&entry.value),
        primitive_types::U256::one(),
        "idempotent retries must apply mutation exactly once"
    );
}

#[tokio::test]
async fn retry_idempotency_is_exactly_once_under_commit_pressure() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        commit_timeout_ms: 1,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let key = IdempotencyKey([91u8; 16]);
    let envelope = TransactionEnvelope {
        caller: None,
        idempotency_key: Some(key),
        write_class: WriteClass::Standard,
        assertions: Vec::new(),
        read_set: Default::default(),
        write_intent: WriteIntent {
            mutations: vec![
                Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"__slow_parallel_worker__".to_vec(),
                    value: b"slow".to_vec(),
                },
                Mutation::KvIncU256 {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"timeout-idem-counter".to_vec(),
                    amount_be: {
                        let mut out = [0u8; 32];
                        out[31] = 1;
                        out
                    },
                },
            ],
        },
        base_seq: 0,
    };

    let noisy_db = Arc::new(db);
    let mut noise_tasks = Vec::new();
    for worker in 0..8 {
        let db_clone = Arc::clone(&noisy_db);
        noise_tasks.push(tokio::spawn(async move {
            for i in 0..200usize {
                let _ = db_clone
                    .commit(Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: format!("noise:{worker}:{i}").into_bytes(),
                        value: b"n".to_vec(),
                    })
                    .await;
            }
        }));
    }

    let mut saw_timeout = false;
    let second = loop {
        match noisy_db.commit_envelope(envelope.clone()).await {
            Ok(result) => break result,
            Err(AedbError::Timeout) => {
                saw_timeout = true;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(other) => panic!("unexpected error during retry loop: {other:?}"),
        }
    };
    for t in noise_tasks {
        t.await.expect("join noise worker");
    }
    noisy_db
        .wait_for_durable(second.commit_seq)
        .await
        .expect("durable ack");

    let third = noisy_db
        .commit_envelope(envelope)
        .await
        .expect("repeat idempotent retry");
    assert_eq!(
        third.commit_seq, second.commit_seq,
        "all retries must resolve to one commit sequence"
    );

    let counter = noisy_db
        .kv_get_no_auth(
            "p",
            "app",
            b"timeout-idem-counter",
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("kv counter")
        .expect("counter exists");
    assert_eq!(
        primitive_types::U256::from_big_endian(&counter.value),
        primitive_types::U256::one(),
        "counter must be applied once"
    );

    let op = noisy_db.operational_metrics().await;
    if saw_timeout {
        assert!(
            op.timeout_rejections >= 1,
            "timeout path should be observable in operational metrics"
        );
    }
}

#[tokio::test]
async fn strict_cancel_rejects_missing_order() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let err = db
        .order_book_cancel_strict(
            "p",
            "app",
            "BTC-USD",
            999_999,
            "alice",
            CommitFinality::Visible,
        )
        .await
        .expect_err("strict cancel should fail when target is missing");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn strict_cancel_rejects_already_final_order() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.order_book_new(
        "p",
        "app",
        crate::order_book::OrderRequest {
            instrument: "BTC-USD".into(),
            client_order_id: "strict-final".into(),
            side: crate::order_book::OrderSide::Bid,
            order_type: crate::order_book::OrderType::Limit,
            time_in_force: crate::order_book::TimeInForce::Gtc,
            exec_instructions: crate::order_book::ExecInstruction(0),
            self_trade_prevention: crate::order_book::SelfTradePrevention::None,
            price_ticks: 100,
            qty_be: {
                let mut out = [0u8; 32];
                out[31] = 1;
                out
            },
            owner: "alice".into(),
            account: None,
            nonce: 1,
            price_limit_ticks: None,
        },
    )
    .await
    .expect("place order");

    db.order_book_cancel_strict("p", "app", "BTC-USD", 1, "alice", CommitFinality::Visible)
        .await
        .expect("first strict cancel");

    let err = db
        .order_book_cancel_strict("p", "app", "BTC-USD", 1, "alice", CommitFinality::Visible)
        .await
        .expect_err("second strict cancel should fail on already-cancelled order");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn strict_cancel_by_client_id_rejects_missing_mapping() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let err = db
        .order_book_cancel_by_client_id_strict(
            "p",
            "app",
            "BTC-USD",
            "missing-client-order-id",
            "alice",
            CommitFinality::Visible,
        )
        .await
        .expect_err("strict cancel by client id should fail when mapping is missing");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn strict_cancel_by_client_id_rejects_already_final_order() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.order_book_new(
        "p",
        "app",
        crate::order_book::OrderRequest {
            instrument: "BTC-USD".into(),
            client_order_id: "strict-client-final".into(),
            side: crate::order_book::OrderSide::Bid,
            order_type: crate::order_book::OrderType::Limit,
            time_in_force: crate::order_book::TimeInForce::Gtc,
            exec_instructions: crate::order_book::ExecInstruction(0),
            self_trade_prevention: crate::order_book::SelfTradePrevention::None,
            price_ticks: 100,
            qty_be: {
                let mut out = [0u8; 32];
                out[31] = 1;
                out
            },
            owner: "alice".into(),
            account: None,
            nonce: 1,
            price_limit_ticks: None,
        },
    )
    .await
    .expect("place order");

    db.order_book_cancel_by_client_id_strict(
        "p",
        "app",
        "BTC-USD",
        "strict-client-final",
        "alice",
        CommitFinality::Visible,
    )
    .await
    .expect("first strict cancel by client id");

    let err = db
        .order_book_cancel_by_client_id_strict(
            "p",
            "app",
            "BTC-USD",
            "strict-client-final",
            "alice",
            CommitFinality::Visible,
        )
        .await
        .expect_err("second strict cancel by client id should fail on finalized order");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn strict_cancel_by_client_id_rejects_invalid_mapping_encoding() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: crate::order_book::key_client_id("BTC-USD", "alice", "cid-corrupt"),
        value: vec![1, 2, 3, 4],
    })
    .await
    .expect("inject corrupted mapping");

    let err = db
        .order_book_cancel_by_client_id_strict(
            "p",
            "app",
            "BTC-USD",
            "cid-corrupt",
            "alice",
            CommitFinality::Visible,
        )
        .await
        .expect_err("strict cancel by client id should reject malformed mapping");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn strict_cancel_by_client_id_detects_owner_mismatch_under_tampered_mapping() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.order_book_new(
        "p",
        "app",
        crate::order_book::OrderRequest {
            instrument: "BTC-USD".into(),
            client_order_id: "cid-owner-a".into(),
            side: crate::order_book::OrderSide::Bid,
            order_type: crate::order_book::OrderType::Limit,
            time_in_force: crate::order_book::TimeInForce::Gtc,
            exec_instructions: crate::order_book::ExecInstruction(0),
            self_trade_prevention: crate::order_book::SelfTradePrevention::None,
            price_ticks: 100,
            qty_be: {
                let mut out = [0u8; 32];
                out[31] = 1;
                out
            },
            owner: "alice".into(),
            account: None,
            nonce: 1,
            price_limit_ticks: None,
        },
    )
    .await
    .expect("place order");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: crate::order_book::key_client_id("BTC-USD", "bob", "cid-owner-b"),
        value: 1u64.to_be_bytes().to_vec(),
    })
    .await
    .expect("inject tampered mapping");

    let err = db
        .order_book_cancel_by_client_id_strict(
            "p",
            "app",
            "BTC-USD",
            "cid-owner-b",
            "bob",
            CommitFinality::Visible,
        )
        .await
        .expect_err("strict cancel by client id should reject owner mismatch");
    assert!(matches!(err, AedbError::PermissionDenied(_)));
}

#[tokio::test]
async fn strict_reduce_rejects_missing_order() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let mut one = [0u8; 32];
    one[31] = 1;
    let err = db
        .order_book_reduce_strict(
            "p",
            "app",
            "BTC-USD",
            777,
            "alice",
            one,
            CommitFinality::Visible,
        )
        .await
        .expect_err("strict reduce should fail on missing order");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn strict_cancel_replace_rejects_already_final_order() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    db.order_book_new(
        "p",
        "app",
        crate::order_book::OrderRequest {
            instrument: "BTC-USD".into(),
            client_order_id: "strict-cr-final".into(),
            side: crate::order_book::OrderSide::Bid,
            order_type: crate::order_book::OrderType::Limit,
            time_in_force: crate::order_book::TimeInForce::Gtc,
            exec_instructions: crate::order_book::ExecInstruction(0),
            self_trade_prevention: crate::order_book::SelfTradePrevention::None,
            price_ticks: 100,
            qty_be: {
                let mut out = [0u8; 32];
                out[31] = 1;
                out
            },
            owner: "alice".into(),
            account: None,
            nonce: 1,
            price_limit_ticks: None,
        },
    )
    .await
    .expect("place order");
    db.order_book_cancel_strict("p", "app", "BTC-USD", 1, "alice", CommitFinality::Visible)
        .await
        .expect("cancel");

    let err = db
        .order_book_cancel_replace_strict(
            "p",
            "app",
            "BTC-USD",
            1,
            "alice",
            Some(101),
            None,
            None,
            None,
            CommitFinality::Visible,
        )
        .await
        .expect_err("strict cancel-replace should fail on finalized order");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn multi_update_transaction_envelope_updates_table_and_kv() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 60_000,
        batch_max_bytes: usize::MAX,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        owner_id: None,
        if_not_exists: false,
        project_id: "p".into(),
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

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row {
            values: vec![Value::Integer(1), Value::Integer(100)],
        },
    })
    .await
    .expect("seed account 1");
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(2)],
        row: Row {
            values: vec![Value::Integer(2), Value::Integer(80)],
        },
    })
    .await
    .expect("seed account 2");

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        actor_id: None,
        delegable: false,
        caller_id: "reader".into(),
        permission: Permission::KvRead {
            project_id: "p".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
    }))
    .await
    .expect("grant kv read");
    let caller = CallerContext::new("reader");

    let result = db
        .commit_envelope_with_finality(
            TransactionEnvelope {
                caller: None,
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: Vec::new(),
                read_set: Default::default(),
                write_intent: WriteIntent {
                    mutations: vec![
                        Mutation::Upsert {
                            project_id: "p".into(),
                            scope_id: "app".into(),
                            table_name: "accounts".into(),
                            primary_key: vec![Value::Integer(1)],
                            row: Row {
                                values: vec![Value::Integer(1), Value::Integer(90)],
                            },
                        },
                        Mutation::Upsert {
                            project_id: "p".into(),
                            scope_id: "app".into(),
                            table_name: "accounts".into(),
                            primary_key: vec![Value::Integer(2)],
                            row: Row {
                                values: vec![Value::Integer(2), Value::Integer(90)],
                            },
                        },
                        Mutation::KvSet {
                            project_id: "p".into(),
                            scope_id: "app".into(),
                            key: b"tx:last".to_vec(),
                            value: b"t1".to_vec(),
                        },
                        Mutation::KvSet {
                            project_id: "p".into(),
                            scope_id: "app".into(),
                            key: b"ledger:1->2".to_vec(),
                            value: b"10".to_vec(),
                        },
                    ],
                },
                base_seq: db.head_state().await.visible_head_seq,
            },
            CommitFinality::Visible,
        )
        .await
        .expect("multi-update tx");

    assert!(
        result.durable_head_seq < result.commit_seq,
        "visible finality should return before durable head in batch mode"
    );

    let acct1 = db
        .query(
            "p",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(1)))
                .limit(1),
        )
        .await
        .expect("query account1");
    let acct2 = db
        .query(
            "p",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(2)))
                .limit(1),
        )
        .await
        .expect("query account2");
    assert_eq!(acct1.rows.len(), 1);
    assert_eq!(acct2.rows.len(), 1);
    assert_eq!(acct1.rows[0].values[0], Value::Integer(90));
    assert_eq!(acct2.rows[0].values[0], Value::Integer(90));

    let tx_last = db
        .kv_get("p", "app", b"tx:last", ConsistencyMode::AtLatest, &caller)
        .await
        .expect("tx:last");
    let ledger = db
        .kv_get(
            "p",
            "app",
            b"ledger:1->2",
            ConsistencyMode::AtLatest,
            &caller,
        )
        .await
        .expect("ledger entry");
    assert_eq!(
        tx_last.as_ref().map(|v| v.value.clone()),
        Some(b"t1".to_vec())
    );
    assert_eq!(
        ledger.as_ref().map(|v| v.value.clone()),
        Some(b"10".to_vec())
    );
}

#[tokio::test]
async fn checkpoint_now_allows_commits_while_running() {
    let dir = tempdir().expect("temp");
    let db = Arc::new(AedbInstance::open(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    // Build enough state to make checkpoint work measurable.
    for i in 0..2_000u32 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("seed:{i:06}").into_bytes(),
            value: vec![b'x'; 1024],
        })
        .await
        .expect("seed write");
    }

    let checkpoint_db = Arc::clone(&db);
    let checkpoint_task = tokio::spawn(async move { checkpoint_db.checkpoint_now().await });

    // Wait briefly for checkpoint to begin.
    for _ in 0..10 {
        if !checkpoint_task.is_finished() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert!(
        !checkpoint_task.is_finished(),
        "checkpoint should still be running"
    );

    let commit = db
        .commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"concurrent:write".to_vec(),
            value: b"ok".to_vec(),
        })
        .await
        .expect("commit should proceed during checkpoint");
    let checkpoint_seq = checkpoint_task
        .await
        .expect("checkpoint join")
        .expect("checkpoint");
    assert!(commit.commit_seq >= checkpoint_seq);
}

#[tokio::test]
async fn checkpoint_now_serializes_checkpoint_writers() {
    let dir = tempdir().expect("temp");
    let db = Arc::new(AedbInstance::open(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    })
    .await
    .expect("seed");

    let db1 = Arc::clone(&db);
    let db2 = Arc::clone(&db);
    let t1 = tokio::spawn(async move { db1.checkpoint_now().await });
    let t2 = tokio::spawn(async move { db2.checkpoint_now().await });

    let s1 = t1.await.expect("checkpoint task 1").expect("checkpoint 1");
    let s2 = t2.await.expect("checkpoint task 2").expect("checkpoint 2");
    assert_eq!(s1, s2);
}

#[tokio::test]
async fn checkpoint_captures_transaction_all_or_none() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig::default();
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let base_seq = db
        .snapshot_probe(ConsistencyMode::AtLatest)
        .await
        .expect("base");
    let commit = db
        .commit_envelope(TransactionEnvelope {
            caller: None,
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: Default::default(),
            write_intent: WriteIntent {
                mutations: vec![
                    Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"tx:part:a".to_vec(),
                        value: b"1".to_vec(),
                    },
                    Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: b"tx:part:b".to_vec(),
                        value: b"2".to_vec(),
                    },
                ],
            },
            base_seq,
        })
        .await
        .expect("atomic multi-mutation commit");
    let checkpoint_seq = db.checkpoint_now().await.expect("checkpoint");
    assert!(checkpoint_seq >= commit.commit_seq);

    let recovered_at_cp =
        crate::recovery::recover_at_seq_with_config(dir.path(), checkpoint_seq, &config)
            .expect("recover at checkpoint");
    let snapshot_at_cp = recovered_at_cp.keyspace.snapshot();
    assert_eq!(
        snapshot_at_cp
            .kv_get("p", "app", b"tx:part:a")
            .map(|entry| entry.value.clone()),
        Some(b"1".to_vec())
    );
    assert_eq!(
        snapshot_at_cp
            .kv_get("p", "app", b"tx:part:b")
            .map(|entry| entry.value.clone()),
        Some(b"2".to_vec())
    );

    if commit.commit_seq > 0 {
        let recovered_before =
            crate::recovery::recover_at_seq_with_config(dir.path(), commit.commit_seq - 1, &config)
                .expect("recover before commit seq");
        let before_snapshot = recovered_before.keyspace.snapshot();
        assert!(before_snapshot.kv_get("p", "app", b"tx:part:a").is_none());
        assert!(before_snapshot.kv_get("p", "app", b"tx:part:b").is_none());
    }
}

#[tokio::test]
async fn checkpoint_manifest_trims_fully_covered_segments() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_segment_bytes: 4096,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config.clone(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    for i in 0..400u32 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("seg:{i:04}").into_bytes(),
            value: vec![b'x'; 256],
        })
        .await
        .expect("seed");
    }

    let all_segments = crate::lib_helpers::read_segments(dir.path()).expect("all segments");
    assert!(
        all_segments.len() > 1,
        "test must create multiple wal segments"
    );

    let checkpoint_seq = db.checkpoint_now().await.expect("checkpoint");
    let manifest = crate::manifest::atomic::load_manifest_signed(dir.path(), config.hmac_key())
        .expect("manifest");

    assert!(
        manifest.segments.len() < all_segments.len(),
        "checkpoint manifest should drop fully covered historical segments"
    );
    for segment in &manifest.segments {
        let path = dir.path().join(&segment.filename);
        let range = crate::lib_helpers::scan_segment_seq_range(&path).expect("scan segment");
        if let Some((_, max_seq)) = range {
            assert!(
                max_seq > checkpoint_seq || segment.segment_seq == manifest.active_segment_seq,
                "manifest retained a segment fully covered by checkpoint"
            );
        }
    }
}

#[tokio::test]
#[ignore = "manual perf probe: commit latency with and without concurrent checkpoint"]
async fn benchmark_commit_latency_during_checkpoint() {
    fn percentile(sorted: &[u128], p: f64) -> u128 {
        if sorted.is_empty() {
            return 0;
        }
        let percentile_index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        sorted[percentile_index.min(sorted.len() - 1)]
    }

    async fn run_phase(
        db: &Arc<AedbInstance>,
        start: usize,
        count: usize,
        with_checkpoint: bool,
    ) -> (f64, u128, u128) {
        for i in 0..10_000usize {
            db.commit(Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: format!("warmup:{i:05}").into_bytes(),
                value: vec![b'w'; 256],
            })
            .await
            .expect("warmup seed");
        }

        let checkpoint_task = if with_checkpoint {
            Some(tokio::spawn({
                let db = Arc::clone(db);
                async move { db.checkpoint_now().await }
            }))
        } else {
            None
        };

        let phase_started = Instant::now();
        let mut latencies_us = Vec::with_capacity(count);
        for i in 0..count {
            let started = Instant::now();
            db.commit(Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: format!(
                    "bench:{}:{:06}",
                    if with_checkpoint { "cp" } else { "base" },
                    start + i
                )
                .into_bytes(),
                value: vec![b'x'; 512],
            })
            .await
            .expect("bench commit");
            latencies_us.push(started.elapsed().as_micros());
        }

        if let Some(task) = checkpoint_task {
            let _ = task
                .await
                .expect("checkpoint task join")
                .expect("checkpoint");
        }

        latencies_us.sort_unstable();
        let elapsed_secs = phase_started.elapsed().as_secs_f64().max(0.000_001);
        let tps = count as f64 / elapsed_secs;
        let p50_us = percentile(&latencies_us, 0.50);
        let p99_us = percentile(&latencies_us, 0.99);
        (tps, p50_us, p99_us)
    }

    let dir = tempdir().expect("temp");
    let config = AedbConfig::default();
    let db = Arc::new(AedbInstance::open(config, dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    let (base_tps, base_p50, base_p99) = run_phase(&db, 0, 800, false).await;
    let (cp_tps, cp_p50, cp_p99) = run_phase(&db, 1_000_000, 800, true).await;

    eprintln!(
        "checkpoint_perf: base_tps={:.2} base_p50_us={} base_p99_us={} | cp_tps={:.2} cp_p50_us={} cp_p99_us={} | tps_ratio={:.3} p50_ratio={:.3} p99_ratio={:.3}",
        base_tps,
        base_p50,
        base_p99,
        cp_tps,
        cp_p50,
        cp_p99,
        cp_tps / base_tps.max(0.000_001),
        (cp_p50 as f64) / (base_p50.max(1) as f64),
        (cp_p99 as f64) / (base_p99.max(1) as f64),
    );
}

#[tokio::test]
#[ignore = "manual perf probe: parallel commit throughput with and without concurrent checkpoint"]
async fn benchmark_parallel_commit_throughput_during_checkpoint() {
    fn percentile(sorted: &[u128], p: f64) -> u128 {
        if sorted.is_empty() {
            return 0;
        }
        let percentile_index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        sorted[percentile_index.min(sorted.len() - 1)]
    }

    async fn run_parallel_phase(
        db: &Arc<AedbInstance>,
        workers: usize,
        commits_per_worker: usize,
        with_checkpoint: bool,
        offset: usize,
    ) -> (f64, u128, u128) {
        let checkpoint_task = if with_checkpoint {
            Some(tokio::spawn({
                let db = Arc::clone(db);
                async move { db.checkpoint_now().await }
            }))
        } else {
            None
        };

        let phase_started = Instant::now();
        let mut tasks = Vec::with_capacity(workers);
        for worker in 0..workers {
            let db = Arc::clone(db);
            tasks.push(tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(commits_per_worker);
                for i in 0..commits_per_worker {
                    let started = Instant::now();
                    db.commit(Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: format!(
                            "par:{}:{:02}:{:06}",
                            if with_checkpoint { "cp" } else { "base" },
                            worker,
                            offset + i
                        )
                        .into_bytes(),
                        value: vec![b'p'; 256],
                    })
                    .await
                    .expect("parallel bench commit");
                    latencies.push(started.elapsed().as_micros());
                }
                latencies
            }));
        }

        let mut all_latencies = Vec::with_capacity(workers * commits_per_worker);
        for task in tasks {
            let mut worker_latencies = task.await.expect("worker join");
            all_latencies.append(&mut worker_latencies);
        }

        if let Some(task) = checkpoint_task {
            let _ = task
                .await
                .expect("checkpoint task join")
                .expect("checkpoint");
        }

        all_latencies.sort_unstable();
        let elapsed_secs = phase_started.elapsed().as_secs_f64().max(0.000_001);
        let total = workers * commits_per_worker;
        let tps = total as f64 / elapsed_secs;
        let p50_us = percentile(&all_latencies, 0.50);
        let p99_us = percentile(&all_latencies, 0.99);
        (tps, p50_us, p99_us)
    }

    let dir = tempdir().expect("temp");
    let mut config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 10,
        batch_max_bytes: usize::MAX,
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        ..AedbConfig::default()
    };
    config.manifest_hmac_key = None;
    let db = Arc::new(AedbInstance::open(config, dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    // Seed state so the checkpoint has meaningful work.
    for i in 0..12_000usize {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("parallel-seed:{i:05}").into_bytes(),
            value: vec![b's'; 256],
        })
        .await
        .expect("seed");
    }

    let workers = 8usize;
    let commits_per_worker = 300usize;
    let (base_tps, base_p50, base_p99) =
        run_parallel_phase(&db, workers, commits_per_worker, false, 0).await;
    let (cp_tps, cp_p50, cp_p99) =
        run_parallel_phase(&db, workers, commits_per_worker, true, 1_000_000).await;

    eprintln!(
        "checkpoint_parallel_perf: workers={} commits_per_worker={} | base_tps={:.2} base_p50_us={} base_p99_us={} | cp_tps={:.2} cp_p50_us={} cp_p99_us={} | tps_ratio={:.3} p50_ratio={:.3} p99_ratio={:.3}",
        workers,
        commits_per_worker,
        base_tps,
        base_p50,
        base_p99,
        cp_tps,
        cp_p50,
        cp_p99,
        cp_tps / base_tps.max(0.000_001),
        (cp_p50 as f64) / (base_p50.max(1) as f64),
        (cp_p99 as f64) / (base_p99.max(1) as f64),
    );
}

#[tokio::test]
#[ignore = "manual perf probe: compare checkpoint compression levels under parallel load"]
async fn benchmark_parallel_checkpoint_compression_levels() {
    async fn seed(db: &Arc<AedbInstance>) {
        for i in 0..12_000usize {
            db.commit(Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: format!("parallel-seed:{i:05}").into_bytes(),
                value: vec![b's'; 256],
            })
            .await
            .expect("seed");
        }
    }

    async fn run_parallel_phase(
        db: &Arc<AedbInstance>,
        workers: usize,
        commits_per_worker: usize,
        with_checkpoint: bool,
        offset: usize,
    ) -> f64 {
        let checkpoint_task = if with_checkpoint {
            Some(tokio::spawn({
                let db = Arc::clone(db);
                async move { db.checkpoint_now().await }
            }))
        } else {
            None
        };

        let phase_started = Instant::now();
        let mut tasks = Vec::with_capacity(workers);
        for worker in 0..workers {
            let db = Arc::clone(db);
            tasks.push(tokio::spawn(async move {
                for i in 0..commits_per_worker {
                    db.commit(Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: format!(
                            "cmp:{}:{:02}:{:06}",
                            if with_checkpoint { "cp" } else { "base" },
                            worker,
                            offset + i
                        )
                        .into_bytes(),
                        value: vec![b'c'; 256],
                    })
                    .await
                    .expect("parallel bench commit");
                }
            }));
        }
        for task in tasks {
            task.await.expect("worker join");
        }
        if let Some(task) = checkpoint_task {
            let _ = task
                .await
                .expect("checkpoint task join")
                .expect("checkpoint");
        }

        let elapsed_secs = phase_started.elapsed().as_secs_f64().max(0.000_001);
        let total = workers * commits_per_worker;
        total as f64 / elapsed_secs
    }

    let workers = 8usize;
    let commits_per_worker = 300usize;
    let levels = [3, 1, 0];

    let mut baseline_tps = 0.0f64;
    for (idx, level) in levels.iter().copied().enumerate() {
        let dir = tempdir().expect("temp");
        let mut config = AedbConfig {
            durability_mode: DurabilityMode::Batch,
            batch_interval_ms: 10,
            batch_max_bytes: usize::MAX,
            recovery_mode: RecoveryMode::Permissive,
            hash_chain_required: false,
            ..AedbConfig::default()
        };
        config.manifest_hmac_key = None;
        config.checkpoint_compression_level = level;
        let db = Arc::new(AedbInstance::open(config, dir.path()).expect("open"));
        db.create_project("p").await.expect("project");
        seed(&db).await;

        let base =
            run_parallel_phase(&db, workers, commits_per_worker, false, idx * 1_000_000).await;
        let cp = run_parallel_phase(
            &db,
            workers,
            commits_per_worker,
            true,
            idx * 1_000_000 + 500_000,
        )
        .await;
        if idx == 0 {
            baseline_tps = base;
        }

        eprintln!(
            "checkpoint_compression_perf: level={} workers={} commits_per_worker={} base_tps={:.2} cp_tps={:.2} cp_to_base={:.3} cp_to_level3_base={:.3}",
            level,
            workers,
            commits_per_worker,
            base,
            cp,
            cp / base.max(0.000_001),
            cp / baseline_tps.max(0.000_001),
        );
    }
}

#[tokio::test]
#[ignore = "manual perf probe: durability knob sweep (batch/coalescing)"]
async fn benchmark_durability_knob_sweep() {
    fn percentile(sorted: &[u128], p: f64) -> u128 {
        if sorted.is_empty() {
            return 0;
        }
        let percentile_index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        sorted[percentile_index.min(sorted.len() - 1)]
    }

    #[derive(Clone)]
    struct Profile {
        name: &'static str,
        batch_interval_ms: u64,
        batch_max_bytes: usize,
        coalesce_enabled: bool,
        coalesce_window_us: u64,
    }

    async fn run_profile(profile: &Profile) -> (f64, u128, u128, u64, u64) {
        let dir = tempdir().expect("temp");
        let mut config = AedbConfig {
            durability_mode: DurabilityMode::Batch,
            batch_interval_ms: profile.batch_interval_ms,
            batch_max_bytes: profile.batch_max_bytes,
            recovery_mode: RecoveryMode::Permissive,
            hash_chain_required: false,
            durable_ack_coalescing_enabled: profile.coalesce_enabled,
            durable_ack_coalesce_window_us: profile.coalesce_window_us,
            ..AedbConfig::default()
        };
        config.manifest_hmac_key = None;
        let db = Arc::new(AedbInstance::open(config, dir.path()).expect("open"));
        db.create_project("p").await.expect("project");

        for i in 0..8_000usize {
            db.commit(Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: format!("sweep-seed:{i:05}").into_bytes(),
                value: vec![b's'; 128],
            })
            .await
            .expect("seed");
        }

        let workers = 8usize;
        let commits_per_worker = 500usize;
        let started = Instant::now();
        let mut tasks = Vec::with_capacity(workers);
        for worker in 0..workers {
            let db = Arc::clone(&db);
            tasks.push(tokio::spawn(async move {
                let mut lats = Vec::with_capacity(commits_per_worker);
                for i in 0..commits_per_worker {
                    let t0 = Instant::now();
                    db.commit(Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: format!("sweep:{worker:02}:{i:06}").into_bytes(),
                        value: vec![b'x'; 256],
                    })
                    .await
                    .expect("commit");
                    lats.push(t0.elapsed().as_micros());
                }
                lats
            }));
        }

        let mut all_lat = Vec::with_capacity(workers * commits_per_worker);
        for task in tasks {
            let mut lats = task.await.expect("worker join");
            all_lat.append(&mut lats);
        }
        all_lat.sort_unstable();

        let elapsed = started.elapsed().as_secs_f64().max(0.000_001);
        let tps = (workers * commits_per_worker) as f64 / elapsed;
        let p50 = percentile(&all_lat, 0.50);
        let p99 = percentile(&all_lat, 0.99);
        let op = db.operational_metrics().await;
        (tps, p50, p99, op.wal_sync_ops, op.avg_wal_sync_micros)
    }

    let profiles = vec![
        Profile {
            name: "baseline_10ms_1mb_no_coalesce",
            batch_interval_ms: 10,
            batch_max_bytes: 1 * 1024 * 1024,
            coalesce_enabled: false,
            coalesce_window_us: 0,
        },
        Profile {
            name: "trial_20ms_4mb_coalesce_1000us",
            batch_interval_ms: 20,
            batch_max_bytes: 4 * 1024 * 1024,
            coalesce_enabled: true,
            coalesce_window_us: 1000,
        },
        Profile {
            name: "trial_20ms_8mb_coalesce_1500us",
            batch_interval_ms: 20,
            batch_max_bytes: 8 * 1024 * 1024,
            coalesce_enabled: true,
            coalesce_window_us: 1500,
        },
        Profile {
            name: "trial_40ms_8mb_coalesce_1500us",
            batch_interval_ms: 40,
            batch_max_bytes: 8 * 1024 * 1024,
            coalesce_enabled: true,
            coalesce_window_us: 1500,
        },
    ];

    for profile in &profiles {
        let (tps, p50, p99, wal_sync_ops, avg_wal_sync_us) = run_profile(profile).await;
        eprintln!(
            "durability_sweep: profile={} tps={:.2} p50_us={} p99_us={} wal_sync_ops={} avg_wal_sync_us={}",
            profile.name, tps, p50, p99, wal_sync_ops, avg_wal_sync_us
        );
    }
}

#[tokio::test]
#[ignore = "manual profiling: end-to-end pipeline breakdown (commit/checkpoint/recovery)"]
async fn profile_end_to_end_pipeline_breakdown() {
    fn percentile(sorted: &[u128], p: f64) -> u128 {
        if sorted.is_empty() {
            return 0;
        }
        let percentile_index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        sorted[percentile_index.min(sorted.len() - 1)]
    }

    let dir = tempdir().expect("temp");
    let mut config = AedbConfig {
        durability_mode: DurabilityMode::Batch,
        batch_interval_ms: 10,
        batch_max_bytes: usize::MAX,
        recovery_mode: RecoveryMode::Permissive,
        hash_chain_required: false,
        ..AedbConfig::default()
    };
    config.manifest_hmac_key = None;
    let db = Arc::new(AedbInstance::open(config.clone(), dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    for i in 0..20_000usize {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("seed:{i:06}").into_bytes(),
            value: vec![b's'; 256],
        })
        .await
        .expect("seed");
    }

    let workers = 8usize;
    let commits_per_worker = 500usize;
    let commit_started = Instant::now();
    let mut tasks = Vec::with_capacity(workers);
    for worker in 0..workers {
        let db = Arc::clone(&db);
        tasks.push(tokio::spawn(async move {
            let mut latencies_us = Vec::with_capacity(commits_per_worker);
            for i in 0..commits_per_worker {
                let started = Instant::now();
                db.commit(Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: format!("profile:commit:{worker:02}:{i:06}").into_bytes(),
                    value: vec![b'c'; 256],
                })
                .await
                .expect("commit");
                latencies_us.push(started.elapsed().as_micros());
            }
            latencies_us
        }));
    }
    let mut all_commit_latencies = Vec::with_capacity(workers * commits_per_worker);
    for task in tasks {
        let mut worker_latencies = task.await.expect("worker join");
        all_commit_latencies.append(&mut worker_latencies);
    }
    let commit_elapsed = commit_started.elapsed();
    all_commit_latencies.sort_unstable();
    let commit_tps = (workers * commits_per_worker) as f64 / commit_elapsed.as_secs_f64();
    let commit_p50 = percentile(&all_commit_latencies, 0.50);
    let commit_p99 = percentile(&all_commit_latencies, 0.99);
    let op_after_commits = db.operational_metrics().await;

    let checkpoint_total_started = Instant::now();
    let checkpoint_lock_started = Instant::now();
    let _checkpoint_guard = db.checkpoint_lock.lock().await;
    let checkpoint_lock_wait = checkpoint_lock_started.elapsed();

    let snapshot_started = Instant::now();
    let checkpoint_seq = db.executor.durable_head_seq_now();
    let lease = db
        .acquire_snapshot(ConsistencyMode::AtSeq(checkpoint_seq))
        .await
        .expect("checkpoint snapshot");
    let snapshot_elapsed = snapshot_started.elapsed();

    let idempotency_started = Instant::now();
    let mut idempotency = db.executor.idempotency_snapshot().await;
    idempotency.retain(|_, record| record.commit_seq <= checkpoint_seq);
    let idempotency_elapsed = idempotency_started.elapsed();

    let write_checkpoint_started = Instant::now();
    let checkpoint = crate::checkpoint::writer::write_checkpoint_with_key(
        lease.view.keyspace.as_ref(),
        lease.view.catalog.as_ref(),
        checkpoint_seq,
        &db.dir,
        db._config.checkpoint_key(),
        db._config.checkpoint_key_id.clone(),
        idempotency,
        db._config.checkpoint_compression_level,
    )
    .expect("write checkpoint");
    let write_checkpoint_elapsed = write_checkpoint_started.elapsed();

    let segments_started = Instant::now();
    let segments = crate::lib_helpers::read_segments_for_checkpoint(&db.dir, checkpoint_seq)
        .expect("read segments for checkpoint");
    let active_segment_seq = segments
        .last()
        .map(|segment| segment.segment_seq)
        .unwrap_or(checkpoint_seq.saturating_add(1));
    let segments_elapsed = segments_started.elapsed();

    let manifest_started = Instant::now();
    let manifest = crate::manifest::schema::Manifest {
        durable_seq: checkpoint_seq,
        visible_seq: checkpoint_seq,
        active_segment_seq,
        checkpoints: vec![checkpoint.clone()],
        segments: segments.clone(),
    };
    crate::manifest::atomic::write_manifest_atomic_signed(
        &manifest,
        &db.dir,
        db._config.hmac_key(),
    )
    .expect("write manifest");
    let manifest_elapsed = manifest_started.elapsed();
    let checkpoint_total_elapsed = checkpoint_total_started.elapsed();
    drop(_checkpoint_guard);

    let checkpoint_bytes = std::fs::metadata(db.dir.join(&checkpoint.filename))
        .expect("checkpoint stat")
        .len();

    drop(db);
    let reopen_started = Instant::now();
    let reopened = AedbInstance::open(config, dir.path()).expect("reopen");
    let reopen_elapsed = reopen_started.elapsed();
    let reopen_metrics = reopened.operational_metrics().await;

    eprintln!(
        "pipeline_profile commit_phase: workers={} commits_per_worker={} commits={} elapsed_ms={} tps={:.2} p50_us={} p99_us={} | prestage_validate_ops={} avg_prestage_validate_us={} epoch_process_ops={} avg_epoch_process_us={} avg_wal_append_us={} avg_wal_sync_us={} avg_coordinator_apply_us={}",
        workers,
        commits_per_worker,
        workers * commits_per_worker,
        commit_elapsed.as_millis(),
        commit_tps,
        commit_p50,
        commit_p99,
        op_after_commits.prestage_validate_ops,
        op_after_commits.avg_prestage_validate_micros,
        op_after_commits.epoch_process_ops,
        op_after_commits.avg_epoch_process_micros,
        op_after_commits.avg_wal_append_micros,
        op_after_commits.avg_wal_sync_micros,
        op_after_commits.avg_coordinator_apply_micros
    );
    eprintln!(
        "pipeline_profile checkpoint_phase: seq={} total_ms={} lock_wait_ms={} snapshot_ms={} idempotency_ms={} write_checkpoint_ms={} segment_scan_ms={} manifest_ms={} checkpoint_bytes={} retained_segments={}",
        checkpoint_seq,
        checkpoint_total_elapsed.as_millis(),
        checkpoint_lock_wait.as_millis(),
        snapshot_elapsed.as_millis(),
        idempotency_elapsed.as_millis(),
        write_checkpoint_elapsed.as_millis(),
        segments_elapsed.as_millis(),
        manifest_elapsed.as_millis(),
        checkpoint_bytes,
        segments.len()
    );
    eprintln!(
        "pipeline_profile recovery_phase: reopen_ms={} startup_recovery_micros={} startup_recovered_seq={} durable_head_seq={} visible_head_seq={}",
        reopen_elapsed.as_millis(),
        reopen_metrics.startup_recovery_micros,
        reopen_metrics.startup_recovered_seq,
        reopen_metrics.durable_head_seq,
        reopen_metrics.visible_head_seq
    );
}

#[tokio::test]
async fn snapshot_probe_returns_snapshot_seq() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let seq = db
        .snapshot_probe(ConsistencyMode::AtLatest)
        .await
        .expect("probe");
    assert!(seq >= 1);
}

#[tokio::test]
async fn at_checkpoint_falls_back_when_version_evicted() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_versions: 2,
        min_version_age_ms: 0,
        version_gc_interval_ms: 1,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"checkpoint:key".to_vec(),
        value: b"v1".to_vec(),
    })
    .await
    .expect("seed checkpoint value");
    let checkpoint_seq = db.checkpoint_now().await.expect("checkpoint");
    for i in 0..8 {
        db.commit(Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: format!("tail:{i}").into_bytes(),
            value: vec![i as u8],
        })
        .await
        .expect("tail write");
    }
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cp_view_seq = db
        .snapshot_probe(ConsistencyMode::AtCheckpoint)
        .await
        .expect("checkpoint snapshot");
    assert_eq!(cp_view_seq, checkpoint_seq);
}

#[tokio::test]
async fn migrations_are_idempotent_and_checksum_guarded() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    let migration = crate::migration::Migration {
        version: 1,
        name: "create-users".into(),
        project_id: "p".into(),
        scope_id: "app".into(),
        mutations: vec![Mutation::Ddl(DdlOperation::CreateTable {
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
                    name: "name".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
        })],
        down_mutations: Some(vec![Mutation::Ddl(DdlOperation::DropTable {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            if_exists: true,
        })]),
    };
    db.apply_migration(migration.clone())
        .await
        .expect("apply 1");
    db.apply_migration(migration).await.expect("apply 2");

    let changed = crate::migration::Migration {
        version: 1,
        name: "create-users-v2".into(),
        project_id: "p".into(),
        scope_id: "app".into(),
        mutations: vec![],
        down_mutations: None,
    };
    let err = db
        .apply_migration(changed)
        .await
        .expect_err("checksum guard");
    assert!(matches!(err, AedbError::IntegrityError { .. }));
}

#[tokio::test]
async fn run_migrations_reports_applied_and_skipped_versions() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    let migration = crate::migration::Migration {
        version: 1,
        name: "create-users".into(),
        project_id: "p".into(),
        scope_id: "app".into(),
        mutations: vec![Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            if_not_exists: false,
            columns: vec![
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
            primary_key: vec!["id".into()],
        })],
        down_mutations: None,
    };

    let first = db
        .run_migrations(vec![migration.clone()])
        .await
        .expect("run first");
    assert_eq!(first.applied.len(), 1);
    assert!(first.skipped.is_empty());
    assert_eq!(first.current_version, 1);

    let second = db
        .run_migrations(vec![migration])
        .await
        .expect("run second");
    assert!(second.applied.is_empty());
    assert_eq!(second.skipped, vec![1]);
    assert_eq!(second.current_version, 1);
    assert_eq!(db.current_version("p", "app").await.expect("current"), 1);
}

#[tokio::test]
async fn concurrent_apply_migration_converges_idempotently() {
    let dir = tempdir().expect("temp");
    let db = Arc::new(AedbInstance::open(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("p").await.expect("project");

    let migration = crate::migration::Migration {
        version: 1,
        name: "create-users".into(),
        project_id: "p".into(),
        scope_id: "app".into(),
        mutations: vec![Mutation::Ddl(DdlOperation::CreateTable {
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
                    name: "name".into(),
                    col_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
        })],
        down_mutations: None,
    };

    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let db_a = Arc::clone(&db);
    let db_b = Arc::clone(&db);
    let migration_a = migration.clone();
    let migration_b = migration.clone();
    let barrier_a = Arc::clone(&barrier);
    let barrier_b = Arc::clone(&barrier);

    let task_a = tokio::spawn(async move {
        barrier_a.wait().await;
        db_a.apply_migration(migration_a).await
    });
    let task_b = tokio::spawn(async move {
        barrier_b.wait().await;
        db_b.apply_migration(migration_b).await
    });

    task_a
        .await
        .expect("task a join")
        .expect("task a apply migration");
    task_b
        .await
        .expect("task b join")
        .expect("task b apply migration");

    let applied = db
        .list_applied_migrations("p", "app")
        .await
        .expect("list applied");
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].version, 1);
    assert!(
        db.table_exists("p", "app", "users")
            .await
            .expect("users table exists")
    );
}

struct RecordingTelemetryHook {
    queries: Arc<std::sync::Mutex<Vec<QueryTelemetryEvent>>>,
    commits: Arc<std::sync::Mutex<Vec<CommitTelemetryEvent>>>,
}

impl QueryCommitTelemetryHook for RecordingTelemetryHook {
    fn on_query(&self, event: &QueryTelemetryEvent) {
        self.queries
            .lock()
            .expect("query telemetry mutex poisoned")
            .push(event.clone());
    }

    fn on_commit(&self, event: &CommitTelemetryEvent) {
        self.commits
            .lock()
            .expect("commit telemetry mutex poisoned")
            .push(event.clone());
    }
}

struct EchoSqlAdapter;

impl ReadOnlySqlAdapter for EchoSqlAdapter {
    fn execute_read_only(
        &self,
        _project_id: &str,
        _scope_id: &str,
        sql: &str,
    ) -> Result<(Query, QueryOptions), QueryError> {
        if sql.trim() == "select id from items" {
            Ok((
                Query::select(&["id"]).from("items"),
                QueryOptions::default(),
            ))
        } else {
            Err(QueryError::InvalidQuery {
                reason: "unsupported sql".into(),
            })
        }
    }
}

struct LocalRemoteAdapter;

impl RemoteBackupAdapter for LocalRemoteAdapter {
    fn store_backup_dir(&self, uri: &str, backup_dir: &Path) -> Result<(), AedbError> {
        let target = PathBuf::from(uri);
        if target.exists() {
            fs::remove_dir_all(&target)?;
        }
        copy_dir_recursive(backup_dir, &target)
    }

    fn materialize_backup_chain(
        &self,
        uri: &str,
        scratch_dir: &Path,
    ) -> Result<Vec<PathBuf>, AedbError> {
        let source = PathBuf::from(uri);
        let target = scratch_dir.join("remote_chain_0");
        copy_dir_recursive(&source, &target)?;
        Ok(vec![target])
    }
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(), AedbError> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let entry_path = entry.path();
        let out_path = dst.join(entry.file_name());
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_recursive(&entry_path, &out_path)?;
        } else if ty.is_file() {
            fs::copy(&entry_path, &out_path)?;
        }
    }
    Ok(())
}

async fn create_table(
    db: &AedbInstance,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    columns: Vec<ColumnDef>,
    primary_key: Vec<&str>,
) {
    db.commit_ddl(DdlOperation::CreateTable {
        owner_id: None,
        project_id: project_id.to_string(),
        scope_id: scope_id.to_string(),
        table_name: table_name.to_string(),
        if_not_exists: false,
        columns,
        primary_key: primary_key.into_iter().map(|v| v.to_string()).collect(),
    })
    .await
    .expect("create table");
}

#[tokio::test]
async fn read_tx_keeps_snapshot_consistency_across_queries() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "accounts",
        vec![
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
        vec!["id"],
    )
    .await;
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::Integer(10)]),
    })
    .await
    .expect("seed");

    let tx = db
        .begin_read_tx(ConsistencyMode::AtLatest)
        .await
        .expect("read tx");
    let before = tx
        .query(
            "p",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
        )
        .await
        .expect("before");
    assert_eq!(before.rows[0].values[0], Value::Integer(10));

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "accounts".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::Integer(99)]),
    })
    .await
    .expect("update");

    let still_before = tx
        .query(
            "p",
            "app",
            Query::select(&["balance"])
                .from("accounts")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
        )
        .await
        .expect("stable read");
    assert_eq!(still_before.rows[0].values[0], Value::Integer(10));
}

#[tokio::test]
async fn list_batch_and_lookup_helpers_work() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "user_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;
    create_table(
        &db,
        "p",
        "app",
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
        ],
        vec!["id"],
    )
    .await;
    for i in 1..=5 {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row::from_values(vec![Value::Integer(i), Value::Integer(i)]),
        })
        .await
        .expect("seed item");
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(i)],
            row: Row::from_values(vec![Value::Integer(i), Value::Text(format!("u{i}").into())]),
        })
        .await
        .expect("seed user");
    }

    let batched = db
        .query_batch(
            "p",
            "app",
            vec![
                QueryBatchItem {
                    query: Query::select(&["id"]).from("items").limit(2),
                    options: QueryOptions::default(),
                },
                QueryBatchItem {
                    query: Query::select(&["id"]).from("items").limit(3),
                    options: QueryOptions::default(),
                },
            ],
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("query batch");
    assert_eq!(batched.len(), 2);
    assert_eq!(batched[0].snapshot_seq, batched[1].snapshot_seq);

    let first = db
        .list_with_total(
            "p",
            "app",
            Query::select(&["id"]).from("items"),
            None,
            None,
            2,
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("list");
    assert_eq!(first.total_count, 5);
    assert_eq!(first.rows.len(), 2);
    assert!(first.next_cursor.is_some());

    let via_offset = db
        .list_with_total(
            "p",
            "app",
            Query::select(&["id"]).from("items"),
            None,
            Some(3),
            2,
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("offset");
    assert_eq!(via_offset.total_count, 5);
    assert_eq!(via_offset.rows.len(), 2);

    let (source, hydrated) = db
        .lookup_then_hydrate(
            "p",
            "app",
            Query::select(&["user_id"]).from("items").limit(3),
            0,
            Query::select(&["id", "name"]).from("users"),
            "id",
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("lookup/hydrate");
    assert_eq!(source.rows.len(), 3);
    assert!(!source.truncated);
    assert_eq!(hydrated.rows.len(), 3);
    assert!(!hydrated.truncated);
}

#[tokio::test]
async fn lookup_then_hydrate_fetches_all_pages_for_large_key_sets() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");

    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "user_id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;
    create_table(
        &db,
        "p",
        "app",
        "users",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "username".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;

    for id in 1_i64..=1_000_i64 {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![Value::Integer(id), Value::Integer(id)]),
        })
        .await
        .expect("insert item");
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![
                Value::Integer(id),
                Value::Text(format!("u{id}").into()),
            ]),
        })
        .await
        .expect("insert user");
    }

    let (source, hydrated) = db
        .lookup_then_hydrate(
            "p",
            "app",
            Query::select(&["user_id"]).from("items").limit(1_000),
            0,
            Query::select(&["id", "username"]).from("users"),
            "id",
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("lookup/hydrate");
    assert_eq!(source.rows.len(), 1_000);
    assert!(!source.truncated);
    assert_eq!(hydrated.rows.len(), 1_000);
    assert!(!hydrated.truncated);
}

#[tokio::test]
async fn telemetry_sql_and_remote_adapter_paths_work() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        vec!["id"],
    )
    .await;
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1)]),
    })
    .await
    .expect("seed");

    let hook = Arc::new(RecordingTelemetryHook {
        queries: Arc::new(std::sync::Mutex::new(Vec::new())),
        commits: Arc::new(std::sync::Mutex::new(Vec::new())),
    });
    db.add_telemetry_hook(hook.clone());

    let sql_result = db
        .query_sql_read_only(
            &EchoSqlAdapter,
            "p",
            "app",
            "select id from items",
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("sql adapter");
    assert_eq!(sql_result.rows.len(), 1);

    db.commit(Mutation::KvSet {
        project_id: "p".into(),
        scope_id: "app".into(),
        key: b"remote:test".to_vec(),
        value: b"ok".to_vec(),
    })
    .await
    .expect("kv set");

    let remote_dir = tempdir().expect("remote");
    let uri = remote_dir.path().join("snapshot");
    let adapter = LocalRemoteAdapter;
    let uri_str = uri.to_string_lossy().to_string();
    db.backup_full_to_remote(&adapter, &uri_str)
        .await
        .expect("backup remote");

    let restored_dir = tempdir().expect("restored");
    AedbInstance::restore_from_remote(
        &adapter,
        &uri_str,
        restored_dir.path(),
        &AedbConfig::default(),
    )
    .expect("restore remote");
    let restored =
        AedbInstance::open(AedbConfig::default(), restored_dir.path()).expect("open restored");
    let restored_val = restored
        .kv_get_no_auth("p", "app", b"remote:test", ConsistencyMode::AtLatest)
        .await
        .expect("kv get");
    assert_eq!(restored_val.expect("entry").value, b"ok".to_vec());

    let query_count = hook.queries.lock().expect("query telemetry").len();
    let commit_count = hook.commits.lock().expect("commit telemetry").len();
    assert!(query_count >= 1);
    assert!(commit_count >= 1);
}

#[tokio::test]
async fn exists_and_explain_diagnostics_work() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
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
        vec!["id"],
    )
    .await;
    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "items".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::Text("a".into())]),
    })
    .await
    .expect("seed");

    let tx = db
        .begin_read_tx(ConsistencyMode::AtLatest)
        .await
        .expect("read tx");
    let exists = tx
        .exists(
            "p",
            "app",
            Query::select(&["id"])
                .from("items")
                .where_(Expr::Eq("id".into(), Value::Integer(1))),
        )
        .await
        .expect("exists");
    assert!(exists);

    let explain = tx
        .explain(
            "p",
            "app",
            Query::select(&["id"]).from("items"),
            QueryOptions::default(),
        )
        .expect("explain");
    assert!(explain.estimated_scan_rows >= 1);
    assert!(explain.stages.contains(&ExecutionStage::Scan));
    assert!(
        !explain.plan_trace.is_empty(),
        "explain should include access-path trace"
    );

    let with_diag = tx
        .query_with_diagnostics(
            "p",
            "app",
            Query::select(&["id"]).from("items"),
            QueryOptions {
                allow_full_scan: true,
                ..QueryOptions::default()
            },
        )
        .await
        .expect("diag query");
    assert_eq!(with_diag.result.rows.len(), 1);
    assert_eq!(with_diag.diagnostics.snapshot_seq, tx.snapshot_seq());
}

#[tokio::test]
async fn non_pk_text_eq_regression_in_project_scope_indexed_and_non_indexed_paths() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    create_table(
        &db,
        "p",
        "app",
        "sessions",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "user_id".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "status".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;

    for (id, user_id, status) in [
        (1_i64, "8a25f1bc-ea96-48d0-8535-47b784a2df1d", "open"),
        (2, "8a25f1bc-ea96-48d0-8535-47b784a2df1d", "closed"),
        (3, "2cf2434c-ed95-4f35-b786-4853592e6f25", "open"),
    ] {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "sessions".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![
                Value::Integer(id),
                Value::Text(user_id.into()),
                Value::Text(status.into()),
            ]),
        })
        .await
        .expect("seed session");
    }

    let query = Query::select(&["id", "user_id"])
        .from("sessions")
        .where_(Expr::Eq(
            "user_id".into(),
            Value::Text("8a25f1bc-ea96-48d0-8535-47b784a2df1d".into()),
        ));
    let pre_index_result = db
        .query("p", "app", query.clone())
        .await
        .expect("eq query without index");
    assert_eq!(pre_index_result.rows.len(), 2);

    let pre_index_explain = db
        .explain_query("p", "app", query.clone(), QueryOptions::default())
        .await
        .expect("explain without index");
    assert_eq!(
        pre_index_explain.predicate_evaluation_path,
        PredicateEvaluationPath::FullScanFilter
    );
    assert!(
        pre_index_explain.selected_indexes.is_empty(),
        "non-index path should not report selected indexes"
    );

    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "sessions".into(),
        index_name: "by_user_id".into(),
        if_not_exists: false,
        columns: vec!["user_id".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("user_id index");

    let indexed_result = db
        .query("p", "app", query.clone())
        .await
        .expect("eq query with index");
    assert_eq!(indexed_result.rows.len(), 2);
    assert!(
        indexed_result.rows_examined <= pre_index_result.rows_examined,
        "indexed equality should not examine more rows"
    );

    let indexed_explain = db
        .explain_query("p", "app", query, QueryOptions::default())
        .await
        .expect("explain with index");
    assert_eq!(
        indexed_explain.predicate_evaluation_path,
        PredicateEvaluationPath::SecondaryIndexLookup
    );
    assert!(
        indexed_explain
            .selected_indexes
            .contains(&"by_user_id".to_string()),
        "explain should report selected secondary index"
    );
    assert!(
        indexed_explain
            .plan_trace
            .iter()
            .any(|line| line.contains("by_user_id")),
        "plan trace should include selected index name"
    );
}

#[tokio::test]
async fn uuid_text_equality_parity_between_primary_key_and_secondary_index_paths() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");

    create_table(
        &db,
        "p",
        "app",
        "users_pk_uuid",
        vec![
            ColumnDef {
                name: "user_uuid".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "display_name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        vec!["user_uuid"],
    )
    .await;
    create_table(
        &db,
        "p",
        "app",
        "users_secondary_uuid",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "user_uuid".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "display_name".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "users_secondary_uuid".into(),
        index_name: "by_user_uuid".into(),
        if_not_exists: false,
        columns: vec!["user_uuid".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("secondary uuid index");

    let rows = [
        (1_i64, "8e1e917f-f8f8-4f06-bf38-3f2c37dcd857", "alice"),
        (2, "6e0df6fd-5095-4e37-bf9d-1f6b5d6dfcb8", "bob"),
        (3, "1b631635-766d-4207-aa53-f5367b9bf13a", "carol"),
    ];
    for (_id, user_uuid, display_name) in rows {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users_pk_uuid".into(),
            primary_key: vec![Value::Text(user_uuid.to_string().into())],
            row: Row::from_values(vec![
                Value::Text(user_uuid.to_string().into()),
                Value::Text(display_name.into()),
            ]),
        })
        .await
        .expect("seed pk uuid");
    }

    for (id, user_uuid, display_name) in rows {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users_secondary_uuid".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![
                Value::Integer(id),
                Value::Text(user_uuid.into()),
                Value::Text(display_name.into()),
            ]),
        })
        .await
        .expect("seed secondary uuid");
    }

    for (_id, user_uuid, _display_name) in rows {
        let pk_query = Query::select(&["display_name"])
            .from("users_pk_uuid")
            .where_(Expr::Eq(
                "user_uuid".into(),
                Value::Text(user_uuid.to_string().into()),
            ));
        let secondary_query = Query::select(&["display_name"])
            .from("users_secondary_uuid")
            .where_(Expr::Eq(
                "user_uuid".into(),
                Value::Text(user_uuid.to_string().into()),
            ));

        let pk_result = db
            .query("p", "app", pk_query.clone())
            .await
            .expect("pk query");
        let secondary_result = db
            .query("p", "app", secondary_query.clone())
            .await
            .expect("secondary query");
        assert_eq!(pk_result.rows.len(), 1);
        assert_eq!(secondary_result.rows.len(), 1);
        assert_eq!(pk_result.rows[0].values, secondary_result.rows[0].values);
    }

    let pk_query = Query::select(&["display_name"])
        .from("users_pk_uuid")
        .where_(Expr::Eq(
            "user_uuid".into(),
            Value::Text("8e1e917f-f8f8-4f06-bf38-3f2c37dcd857".into()),
        ));
    let secondary_query = Query::select(&["display_name"])
        .from("users_secondary_uuid")
        .where_(Expr::Eq(
            "user_uuid".into(),
            Value::Text("8e1e917f-f8f8-4f06-bf38-3f2c37dcd857".into()),
        ));

    let pk_explain = db
        .explain_query("p", "app", pk_query, QueryOptions::default())
        .await
        .expect("pk explain");
    assert_eq!(
        pk_explain.predicate_evaluation_path,
        PredicateEvaluationPath::PrimaryKeyEqLookup
    );

    let secondary_explain = db
        .explain_query("p", "app", secondary_query, QueryOptions::default())
        .await
        .expect("secondary explain");
    assert_eq!(
        secondary_explain.predicate_evaluation_path,
        PredicateEvaluationPath::SecondaryIndexLookup
    );
    assert!(
        secondary_explain
            .selected_indexes
            .contains(&"by_user_uuid".to_string())
    );
}

#[tokio::test]
async fn u8_column_type_supports_write_read_and_indexed_equality() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");

    create_table(
        &db,
        "p",
        "app",
        "levels",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "level".into(),
                col_type: ColumnType::U8,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;

    db.commit(Mutation::Upsert {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "levels".into(),
        primary_key: vec![Value::Integer(1)],
        row: Row::from_values(vec![Value::Integer(1), Value::U8(7)]),
    })
    .await
    .expect("seed u8 row");

    let without_index = db
        .query(
            "p",
            "app",
            Query::select(&["id", "level"])
                .from("levels")
                .where_(Expr::Eq("level".into(), Value::Integer(7))),
        )
        .await
        .expect("u8 equality via integer literal");
    assert_eq!(without_index.rows.len(), 1);
    assert_eq!(without_index.rows[0].values[1], Value::U8(7));

    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: "p".into(),
        scope_id: "app".into(),
        table_name: "levels".into(),
        index_name: "by_level".into(),
        if_not_exists: false,
        columns: vec!["level".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("create u8 index");

    let with_index = db
        .query(
            "p",
            "app",
            Query::select(&["id", "level"])
                .from("levels")
                .where_(Expr::Eq("level".into(), Value::U8(7))),
        )
        .await
        .expect("u8 equality with index");
    assert_eq!(with_index.rows.len(), 1);
    assert!(with_index.rows_examined <= without_index.rows_examined);

    let err = db
        .commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "levels".into(),
            primary_key: vec![Value::Integer(2)],
            row: Row::from_values(vec![Value::Integer(2), Value::Integer(8)]),
        })
        .await
        .expect_err("integer value should not satisfy U8 column type");
    assert!(matches!(err, AedbError::TypeMismatch { .. }));
}

#[tokio::test]
async fn sql_transaction_plan_helpers_commit() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");
    create_table(
        &db,
        "p",
        "app",
        "items",
        vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        vec!["id"],
    )
    .await;

    let plan = db
        .plan_sql_transaction(
            ConsistencyMode::AtLatest,
            vec![Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "items".into(),
                primary_key: vec![Value::Integer(10)],
                row: Row::from_values(vec![Value::Integer(10)]),
            }],
        )
        .await
        .expect("plan");
    db.commit_sql_transaction_plan(plan)
        .await
        .expect("commit plan");

    let exists = db
        .exists(
            "p",
            "app",
            Query::select(&["id"])
                .from("items")
                .where_(Expr::Eq("id".into(), Value::Integer(10))),
            ConsistencyMode::AtLatest,
        )
        .await
        .expect("exists");
    assert!(exists);
}

#[tokio::test]
#[ignore = "long-running multi-agent user-perspective profile"]
async fn secure_multi_agent_profile_identifies_core_shortcomings() {
    fn u256_be(v: u64) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[24..].copy_from_slice(&v.to_be_bytes());
        out
    }
    fn decode_u256_u64(bytes: [u8; 32]) -> u64 {
        let mut out = [0u8; 8];
        out.copy_from_slice(&bytes[24..]);
        u64::from_be_bytes(out)
    }

    #[derive(Debug, Default, Clone, Copy)]
    struct WorkerStats {
        attempted: usize,
        accepted: usize,
        rejected: usize,
        unauthorized_attempted: usize,
        unauthorized_denied: usize,
        latency_sum_us: u128,
        latency_max_us: u64,
    }

    #[derive(Debug, Default, Clone, Copy)]
    struct RuntimePeaks {
        queue_depth: usize,
        inflight: usize,
        durable_lag: u64,
        conflict_rate: f64,
        durable_wait_ops: u64,
        durable_wait_avg_us: u64,
        wal_append_ops: u64,
        wal_append_bytes: u64,
        wal_append_avg_us: u64,
        wal_sync_ops: u64,
        wal_sync_avg_us: u64,
        commit_errors: u64,
        permission_rejections: u64,
        validation_rejections: u64,
        queue_full_rejections: u64,
        timeout_rejections: u64,
        read_set_conflicts: u64,
    }

    let agents = std::env::var("AEDB_MULTI_AGENT_PROFILE_AGENTS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(12)
        .max(4);
    let ops_per_agent = std::env::var("AEDB_MULTI_AGENT_PROFILE_OPS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(800)
        .max(100);

    let dir = tempdir().expect("temp");
    let db = Arc::new(
        AedbInstance::open_secure(AedbConfig::production([3u8; 32]), dir.path())
            .expect("open secure"),
    );
    let system = CallerContext::system_internal();
    db.commit_as(
        system.clone(),
        Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "p".into(),
        }),
    )
    .await
    .expect("create project");

    let agent_ids: Vec<String> = (0..agents).map(|i| format!("prof_agent_{i}")).collect();
    for a in &agent_ids {
        db.commit_as(
            system.clone(),
            Mutation::Ddl(DdlOperation::GrantPermission {
                caller_id: a.clone(),
                permission: Permission::KvRead {
                    project_id: "p".into(),
                    scope_id: Some("app".into()),
                    prefix: Some(format!("agent:{a}:").into_bytes()),
                },
                actor_id: Some("system".into()),
                delegable: false,
            }),
        )
        .await
        .expect("grant kv read");
        db.commit_as(
            system.clone(),
            Mutation::Ddl(DdlOperation::GrantPermission {
                caller_id: a.clone(),
                permission: Permission::KvWrite {
                    project_id: "p".into(),
                    scope_id: Some("app".into()),
                    prefix: Some(format!("agent:{a}:").into_bytes()),
                },
                actor_id: Some("system".into()),
                delegable: false,
            }),
        )
        .await
        .expect("grant kv write");
        db.commit_as(
            system.clone(),
            Mutation::KvSet {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: format!("agent:{a}:balance").into_bytes(),
                value: u256_be(0).to_vec(),
            },
        )
        .await
        .expect("seed balance");
    }

    let done = Arc::new(tokio::sync::Notify::new());
    let peaks = Arc::new(std::sync::Mutex::new(RuntimePeaks::default()));
    let monitor_db = Arc::clone(&db);
    let monitor_done = Arc::clone(&done);
    let monitor_peaks = Arc::clone(&peaks);
    let monitor = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = monitor_done.notified() => break,
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    let op = monitor_db.operational_metrics().await;
                    let mut p = monitor_peaks.lock().expect("peaks lock");
                    p.queue_depth = p.queue_depth.max(op.queue_depth);
                    p.inflight = p.inflight.max(op.inflight_commits);
                    p.durable_lag = p.durable_lag.max(op.durable_head_lag);
                    p.conflict_rate = p.conflict_rate.max(op.conflict_rate);
                    p.durable_wait_ops = p.durable_wait_ops.max(op.durable_wait_ops);
                    p.durable_wait_avg_us = p.durable_wait_avg_us.max(op.avg_durable_wait_micros);
                    p.wal_append_ops = p.wal_append_ops.max(op.wal_append_ops);
                    p.wal_append_bytes = p.wal_append_bytes.max(op.wal_append_bytes);
                    p.wal_append_avg_us = p.wal_append_avg_us.max(op.avg_wal_append_micros);
                    p.wal_sync_ops = p.wal_sync_ops.max(op.wal_sync_ops);
                    p.wal_sync_avg_us = p.wal_sync_avg_us.max(op.avg_wal_sync_micros);
                    p.commit_errors = p.commit_errors.max(op.commit_errors);
                    p.permission_rejections = p.permission_rejections.max(op.permission_rejections);
                    p.validation_rejections = p.validation_rejections.max(op.validation_rejections);
                    p.queue_full_rejections = p.queue_full_rejections.max(op.queue_full_rejections);
                    p.timeout_rejections = p.timeout_rejections.max(op.timeout_rejections);
                    p.read_set_conflicts = p.read_set_conflicts.max(op.read_set_conflicts);
                }
            }
        }
    });

    let started = Instant::now();
    let mut tasks = Vec::with_capacity(agent_ids.len());
    for (idx, agent) in agent_ids.iter().enumerate() {
        let db_clone = Arc::clone(&db);
        let caller = CallerContext::new(agent.clone());
        let own_key = format!("agent:{agent}:balance").into_bytes();
        let neighbor = &agent_ids[(idx + 1) % agent_ids.len()];
        let cross_key = format!("agent:{neighbor}:balance").into_bytes();
        tasks.push(tokio::spawn(async move {
            let mut stats = WorkerStats::default();
            for op in 0..ops_per_agent {
                stats.attempted += 1;
                let now = Instant::now();
                let res = db_clone
                    .commit_as(
                        caller.clone(),
                        Mutation::KvIncU256 {
                            project_id: "p".into(),
                            scope_id: "app".into(),
                            key: own_key.clone(),
                            amount_be: u256_be(1),
                        },
                    )
                    .await;
                let us = now.elapsed().as_micros() as u64;
                stats.latency_sum_us += us as u128;
                stats.latency_max_us = stats.latency_max_us.max(us);
                match res {
                    Ok(_) => stats.accepted += 1,
                    Err(AedbError::Validation(_)) | Err(AedbError::Conflict(_)) => {
                        stats.rejected += 1
                    }
                    Err(other) => return Err(other),
                }

                if op % 25 == 0 {
                    stats.unauthorized_attempted += 1;
                    let denied = db_clone
                        .commit_as(
                            caller.clone(),
                            Mutation::KvIncU256 {
                                project_id: "p".into(),
                                scope_id: "app".into(),
                                key: cross_key.clone(),
                                amount_be: u256_be(1),
                            },
                        )
                        .await;
                    if matches!(denied, Err(AedbError::PermissionDenied(_))) {
                        stats.unauthorized_denied += 1;
                    } else if let Err(other) = denied {
                        return Err(other);
                    }
                }
            }
            Ok::<_, AedbError>(stats)
        }));
    }

    let mut merged = WorkerStats::default();
    for t in tasks {
        let s = t.await.expect("join profile task").expect("profile run");
        merged.attempted += s.attempted;
        merged.accepted += s.accepted;
        merged.rejected += s.rejected;
        merged.unauthorized_attempted += s.unauthorized_attempted;
        merged.unauthorized_denied += s.unauthorized_denied;
        merged.latency_sum_us += s.latency_sum_us;
        merged.latency_max_us = merged.latency_max_us.max(s.latency_max_us);
    }

    done.notify_waiters();
    monitor.await.expect("join monitor");
    db.force_fsync().await.expect("force fsync");

    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    let attempted_tps = (merged.attempted as f64 / elapsed) as u64;
    let accepted_tps = (merged.accepted as f64 / elapsed) as u64;
    let avg_lat_us = (merged.latency_sum_us / merged.attempted.max(1) as u128) as u64;
    let peaks = *peaks.lock().expect("peaks lock");
    let heads = db.head_state().await;

    eprintln!(
        "multi_agent_profile: agents={} ops_per_agent={} attempted={} accepted={} rejected={} unauthorized_denied={} attempted_tps={} accepted_tps={} avg_lat_us={} max_lat_us={} peak_queue_depth={} peak_inflight={} peak_durable_lag={} peak_conflict_rate={:.4} peak_durable_wait_ops={} peak_durable_wait_avg_us={} peak_wal_append_ops={} peak_wal_append_bytes={} peak_wal_append_avg_us={} peak_wal_sync_ops={} peak_wal_sync_avg_us={} peak_commit_errors={} peak_permission_rejections={} peak_validation_rejections={} peak_queue_full_rejections={} peak_timeout_rejections={} peak_read_set_conflicts={} heads(v={},d={})",
        agents,
        ops_per_agent,
        merged.attempted,
        merged.accepted,
        merged.rejected,
        merged.unauthorized_denied,
        attempted_tps,
        accepted_tps,
        avg_lat_us,
        merged.latency_max_us,
        peaks.queue_depth,
        peaks.inflight,
        peaks.durable_lag,
        peaks.conflict_rate,
        peaks.durable_wait_ops,
        peaks.durable_wait_avg_us,
        peaks.wal_append_ops,
        peaks.wal_append_bytes,
        peaks.wal_append_avg_us,
        peaks.wal_sync_ops,
        peaks.wal_sync_avg_us,
        peaks.commit_errors,
        peaks.permission_rejections,
        peaks.validation_rejections,
        peaks.queue_full_rejections,
        peaks.timeout_rejections,
        peaks.read_set_conflicts,
        heads.visible_head_seq,
        heads.durable_head_seq
    );

    assert_eq!(
        merged.accepted + merged.rejected,
        merged.attempted,
        "profile run must not drop operations"
    );
    assert_eq!(
        merged.unauthorized_denied, merged.unauthorized_attempted,
        "cross-agent unauthorized writes must always be denied"
    );
    assert!(
        heads.visible_head_seq >= heads.durable_head_seq,
        "visible head should be >= durable head"
    );
    assert!(
        attempted_tps >= 200,
        "profile indicates severe throughput regression: attempted_tps={attempted_tps}"
    );
    assert!(
        peaks.commit_errors <= (merged.rejected + merged.unauthorized_denied) as u64,
        "unexpected commit errors exceed rejected + unauthorized-denied operations"
    );
    assert_eq!(
        peaks.permission_rejections, merged.unauthorized_denied as u64,
        "permission rejection accounting must match denied unauthorized attempts"
    );
    assert_eq!(
        peaks.validation_rejections, merged.rejected as u64,
        "validation rejection accounting must match application-level rejected operations"
    );
    assert_eq!(
        peaks.queue_full_rejections, 0,
        "unexpected queue-full rejections under baseline profile load"
    );
    assert_eq!(
        peaks.timeout_rejections, 0,
        "unexpected timeout rejections under baseline profile load"
    );
    assert_eq!(
        peaks.durable_wait_ops, 0,
        "baseline non-durable profile should not accumulate durable wait operations"
    );
    assert!(
        peaks.wal_sync_ops > 0,
        "full-durability secure profile should execute WAL sync operations"
    );

    for agent in &agent_ids {
        let caller = CallerContext::new(agent.clone());
        let entry = db
            .kv_get(
                "p",
                "app",
                format!("agent:{agent}:balance").as_bytes(),
                ConsistencyMode::AtLatest,
                &caller,
            )
            .await
            .expect("read own balance")
            .expect("balance exists");
        let mut bal = [0u8; 32];
        bal.copy_from_slice(&entry.value);
        assert!(
            decode_u256_u64(bal) as usize <= ops_per_agent,
            "agent balance must not exceed own attempts"
        );
    }
}

#[tokio::test]
async fn strict_open_rejects_directory_previously_opened_in_non_strict_mode() {
    let dir = tempdir().expect("temp");
    let mut permissive = AedbConfig::production([7u8; 32]);
    permissive.recovery_mode = RecoveryMode::Permissive;
    permissive.hash_chain_required = false;

    let db = AedbInstance::open(permissive, dir.path()).expect("open permissive");
    db.shutdown().await.expect("shutdown permissive");

    let strict = AedbConfig::production([7u8; 32]);
    let err = match AedbInstance::open(strict, dir.path()) {
        Ok(db) => {
            db.shutdown().await.expect("shutdown unexpected strict db");
            panic!("strict open should fail closed");
        }
        Err(err) => err,
    };
    assert!(
        matches!(err, AedbError::Validation(ref msg) if msg.contains("strict open denied")),
        "unexpected error: {err}"
    );
}
