use super::{
    AedbConfig, AedbError, AedbInstance, CallerContext, ColumnDef, ColumnType, ConsistencyMode,
    DdlOperation, DurabilityMode, Expr, IdempotencyKey, Mutation, Permission, Query, QueryError,
    QueryOptions, ReadAssertion, ResourceType, RestrictiveSqlAdapter, Row, SYSTEM_CALLER_ID,
    TransactionEnvelope, Value, WriteClass, WriteIntent, create_table,
};
use crate::commit::tx::ReadSet;
use crate::commit::validation::CompareOp;
use std::fs;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

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
    assert!(matches!(denied, AedbError::PermissionDenied(_)));

    let q = db
        .query_with_options_as(
            Some(&caller),
            "p",
            "app",
            Query::select(&["*"]).from("users").limit(10),
            QueryOptions::default(),
        )
        .await
        .expect("query with read perm");
    assert_eq!(q.rows.len(), 1);
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
            QueryOptions::default(),
        )
        .await
        .expect_err("join should require global table read");
    assert!(matches!(denied, QueryError::PermissionDenied { .. }));

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
        .query_with_options_as(Some(&caller), "p", "app", query, QueryOptions::default())
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
    assert!(matches!(err, AedbError::PermissionDenied(_)));
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
async fn permission_denied_messages_do_not_leak_table_names() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("prod").await.expect("project");
    create_table(
        &db,
        "prod",
        "app",
        "users",
        vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        }],
        vec!["id"],
    )
    .await;

    let err = db
        .commit_as(
            CallerContext::new("mallory"),
            Mutation::Upsert {
                project_id: "prod".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                primary_key: vec![Value::Integer(1)],
                row: Row::from_values(vec![Value::Integer(1)]),
            },
        )
        .await
        .expect_err("permission denied");
    let rendered = err.to_string();
    assert!(!rendered.contains("users"));
    assert!(!rendered.contains("prod"));
    assert!(!rendered.contains("app"));
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
    assert!(matches!(denied_key, QueryError::PermissionDenied { .. }));

    let denied_scope = db
        .kv_get("p", "private", b"user:1", ConsistencyMode::AtLatest, &alice)
        .await
        .expect_err("narrow app grant should not spill into private scope");
    assert!(matches!(denied_scope, QueryError::PermissionDenied { .. }));
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
    assert!(matches!(denied_query, QueryError::PermissionDenied { .. }));
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
async fn read_policy_like_patterns_substitute_caller_id() {
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
    for (id, owner) in [
        (1_i64, "reader-alpha"),
        (2, "reader-beta"),
        (3, "other-alpha"),
    ] {
        db.commit(Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![Value::Integer(id), Value::Text(owner.into())]),
        })
        .await
        .expect("seed row");
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
    .expect("grant read");
    db.set_read_policy(
        "p",
        "app",
        "users",
        Expr::Like("owner".into(), "$caller_id%".into()),
    )
    .await
    .expect("set read policy");

    let rows = db
        .query_with_options_as(
            Some(&CallerContext::new("reader")),
            "p",
            "app",
            Query::select(&["*"]).from("users").limit(10),
            QueryOptions::default(),
        )
        .await
        .expect("filtered query")
        .rows;

    assert_eq!(rows.len(), 2);
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
async fn postflight_assertions_require_read_permission_in_secure_mode() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_production(AedbConfig::production([8u8; 32]), dir.path())
        .expect("open secure");
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
    db.commit_as(
        system,
        Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"secret".to_vec(),
            value: b"not-for-mallory".to_vec(),
        },
    )
    .await
    .expect("seed secret");

    let err = db
        .commit_as(
            CallerContext::new("mallory"),
            Mutation::PostflightCheck {
                assertions: vec![ReadAssertion::KeyCompare {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"secret".to_vec(),
                    op: CompareOp::Eq,
                    threshold: b"guess".to_vec(),
                }],
            },
        )
        .await
        .expect_err("postflight assertion without read permission must fail before evaluation");
    assert!(matches!(err, AedbError::PermissionDenied(_)));
}

#[tokio::test]
async fn envelope_assertions_require_read_permission_in_secure_mode() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_production(AedbConfig::production([12u8; 32]), dir.path())
        .expect("open secure");
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
    db.commit_as(
        system,
        Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"secret".to_vec(),
            value: b"not-for-mallory".to_vec(),
        },
    )
    .await
    .expect("seed secret");

    let err = db
        .commit_envelope(TransactionEnvelope {
            caller: Some(CallerContext::new("mallory")),
            idempotency_key: None,
            write_class: WriteClass::Standard,
            assertions: vec![ReadAssertion::KeyCompare {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"secret".to_vec(),
                op: CompareOp::Eq,
                threshold: b"guess".to_vec(),
            }],
            read_set: ReadSet::default(),
            write_intent: WriteIntent {
                mutations: Vec::new(),
            },
            base_seq: 0,
        })
        .await
        .expect_err("envelope assertion without read permission must fail before evaluation");
    assert!(matches!(err, AedbError::PermissionDenied(_)));
}

#[tokio::test]
async fn async_index_query_requires_table_read_permission() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_production(AedbConfig::production([10u8; 32]), dir.path())
        .expect("open secure");
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
    db.commit_as(
        system.clone(),
        Mutation::Ddl(DdlOperation::CreateScope {
            project_id: "p".into(),
            scope_id: "app".into(),
            owner_id: None,
            if_not_exists: true,
        }),
    )
    .await
    .expect("create scope");
    db.commit_as(
        system.clone(),
        Mutation::Ddl(DdlOperation::CreateTable {
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
        }),
    )
    .await
    .expect("create table");
    db.commit_as(
        system.clone(),
        Mutation::Ddl(DdlOperation::CreateAsyncIndex {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            index_name: "users_view".into(),
            if_not_exists: false,
            projected_columns: vec!["id".into(), "name".into()],
        }),
    )
    .await
    .expect("create async index");
    db.commit_as(
        system.clone(),
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "alice".into(),
            permission: Permission::IndexRead {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "users".into(),
                index_name: "users_view".into(),
            },
            actor_id: Some("system".into()),
            delegable: false,
        }),
    )
    .await
    .expect("grant index read");

    let err = db
        .query_with_options_as(
            Some(&CallerContext::new("alice")),
            "p",
            "app",
            Query::select(&["*"]).from("users"),
            QueryOptions {
                async_index: Some("users_view".into()),
                allow_full_scan: true,
                ..QueryOptions::default()
            },
        )
        .await
        .expect_err("index read alone must not disclose async projection rows");
    assert!(matches!(err, QueryError::PermissionDenied { .. }));
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
async fn secure_multi_agent_user_perspective_invariants_hold() {
    fn u256_be(v: u64) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[24..].copy_from_slice(&v.to_be_bytes());
        out
    }

    #[allow(clippy::too_many_arguments)]
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
async fn query_sql_read_only_respects_adapter_scan_policy() {
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

    let err = db
        .query_sql_read_only(
            &RestrictiveSqlAdapter,
            "p",
            "app",
            "select * from items",
            ConsistencyMode::AtLatest,
        )
        .await
        .expect_err("adapter should not be forced into full scan mode");
    assert!(matches!(err, QueryError::InvalidQuery { .. }));
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
async fn project_and_scope_admin_grants_allow_table_writes() {
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

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "project-admin".into(),
        permission: Permission::ProjectAdmin {
            project_id: "p".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant project admin");
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "scope-admin".into(),
        permission: Permission::ScopeAdmin {
            project_id: "p".into(),
            scope_id: "app".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant scope admin");

    db.commit_as(
        CallerContext::new("project-admin"),
        Mutation::Insert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row::from_values(vec![Value::Integer(1)]),
        },
    )
    .await
    .expect("project admin insert");
    db.commit_as(
        CallerContext::new("scope-admin"),
        Mutation::Insert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            primary_key: vec![Value::Integer(2)],
            row: Row::from_values(vec![Value::Integer(2)]),
        },
    )
    .await
    .expect("scope admin insert");
}

#[tokio::test]
async fn delete_where_respects_row_level_read_policy() {
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
                name: "owner".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        vec!["id"],
    )
    .await;

    for (id, owner) in [(1_i64, "alice"), (2_i64, "bob")] {
        db.commit(Mutation::Insert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            primary_key: vec![Value::Integer(id)],
            row: Row::from_values(vec![Value::Integer(id), Value::Text(owner.into())]),
        })
        .await
        .expect("seed item");
    }

    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: "alice".into(),
        permission: Permission::TableWrite {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
        },
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant table write");
    db.set_read_policy(
        "p",
        "app",
        "items",
        Expr::Eq("owner".into(), Value::Text("$caller_id".into())),
    )
    .await
    .expect("set policy");

    db.commit_as(
        CallerContext::new("alice"),
        Mutation::DeleteWhere {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "items".into(),
            predicate: Expr::IsNotNull("id".into()),
            limit: None,
        },
    )
    .await
    .expect("delete visible rows only");

    let remaining = db
        .query_with_options(
            "p",
            "app",
            Query::select(&["id", "owner"]).from("items").limit(10),
            QueryOptions::default(),
        )
        .await
        .expect("query remaining rows");
    assert_eq!(remaining.rows.len(), 1, "one row should remain");
    assert_eq!(remaining.rows[0].values[0], Value::Integer(2));
    assert_eq!(remaining.rows[0].values[1], Value::Text("bob".into()));
}

#[tokio::test]
async fn idempotency_keys_are_scoped_to_caller() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("p").await.expect("project");

    for caller_id in ["alice", "bob"] {
        db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: caller_id.into(),
            permission: Permission::KvWrite {
                project_id: "p".into(),
                scope_id: Some("app".into()),
                prefix: None,
            },
            actor_id: None,
            delegable: false,
        }))
        .await
        .expect("grant kv write");
    }

    let key = IdempotencyKey([7u8; 16]);
    let alice = db
        .commit_envelope(TransactionEnvelope {
            caller: Some(CallerContext::new("alice")),
            idempotency_key: Some(key.clone()),
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: Default::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"alice".to_vec(),
                    value: b"1".to_vec(),
                }],
            },
            base_seq: 0,
        })
        .await
        .expect("alice commit");
    let bob = db
        .commit_envelope(TransactionEnvelope {
            caller: Some(CallerContext::new("bob")),
            idempotency_key: Some(key),
            write_class: WriteClass::Standard,
            assertions: Vec::new(),
            read_set: Default::default(),
            write_intent: WriteIntent {
                mutations: vec![Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "app".into(),
                    key: b"bob".to_vec(),
                    value: b"1".to_vec(),
                }],
            },
            base_seq: 0,
        })
        .await
        .expect("bob commit");

    assert!(matches!(
        alice.idempotency,
        crate::commit::executor::IdempotencyOutcome::Applied
    ));
    assert!(matches!(
        bob.idempotency,
        crate::commit::executor::IdempotencyOutcome::Applied
    ));
    assert_ne!(alice.commit_seq, bob.commit_seq);
}

#[derive(Clone, Copy)]
enum AuthzMatrixCase {
    TableRead,
    TableWrite,
    KvRead,
    KvWrite,
    AsyncIndexRead,
    EventStreamRead,
    ProcessorLagRead,
}

impl AuthzMatrixCase {
    fn name(self) -> &'static str {
        match self {
            Self::TableRead => "table read",
            Self::TableWrite => "table write",
            Self::KvRead => "kv read",
            Self::KvWrite => "kv write",
            Self::AsyncIndexRead => "async index read",
            Self::EventStreamRead => "event stream read",
            Self::ProcessorLagRead => "processor lag read",
        }
    }

    fn direct_permissions(self) -> Vec<Permission> {
        match self {
            Self::TableRead => vec![Permission::TableRead {
                project_id: "p".into(),
                scope_id: "s1".into(),
                table_name: "events".into(),
            }],
            Self::TableWrite => vec![Permission::TableWrite {
                project_id: "p".into(),
                scope_id: "s1".into(),
                table_name: "events".into(),
            }],
            Self::KvRead => vec![Permission::KvRead {
                project_id: "p".into(),
                scope_id: Some("s1".into()),
                prefix: Some(b"acct:".to_vec()),
            }],
            Self::KvWrite => vec![Permission::KvWrite {
                project_id: "p".into(),
                scope_id: Some("s1".into()),
                prefix: Some(b"acct:".to_vec()),
            }],
            Self::AsyncIndexRead => vec![
                Permission::TableRead {
                    project_id: "p".into(),
                    scope_id: "s1".into(),
                    table_name: "events".into(),
                },
                Permission::IndexRead {
                    project_id: "p".into(),
                    scope_id: "s1".into(),
                    table_name: "events".into(),
                    index_name: "events_view".into(),
                },
            ],
            Self::EventStreamRead => vec![Permission::TableRead {
                project_id: crate::catalog::SYSTEM_PROJECT_ID.into(),
                scope_id: crate::SYSTEM_SCOPE_ID.into(),
                table_name: crate::EVENT_OUTBOX_TABLE.into(),
            }],
            Self::ProcessorLagRead => vec![Permission::TableRead {
                project_id: crate::catalog::SYSTEM_PROJECT_ID.into(),
                scope_id: crate::SYSTEM_SCOPE_ID.into(),
                table_name: crate::REACTIVE_PROCESSOR_CHECKPOINTS_TABLE.into(),
            }],
        }
    }
}

async fn setup_authz_matrix_db(db: &AedbInstance) {
    db.commit_as(
        CallerContext::system_internal(),
        Mutation::Ddl(DdlOperation::CreateProject {
            project_id: "p".into(),
            owner_id: Some("project_owner".into()),
            if_not_exists: true,
        }),
    )
    .await
    .expect("project");
    db.commit_as(
        CallerContext::system_internal(),
        Mutation::Ddl(DdlOperation::CreateScope {
            project_id: "p".into(),
            scope_id: "s1".into(),
            owner_id: Some("scope_owner".into()),
            if_not_exists: true,
        }),
    )
    .await
    .expect("scope");
    db.commit_as(
        CallerContext::system_internal(),
        Mutation::Ddl(DdlOperation::CreateTable {
            project_id: "p".into(),
            scope_id: "s1".into(),
            table_name: "events".into(),
            owner_id: Some("table_owner".into()),
            if_not_exists: false,
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "amount".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
        }),
    )
    .await
    .expect("table");
    for mutation in [
        Mutation::Ddl(DdlOperation::CreateAsyncIndex {
            project_id: "p".into(),
            scope_id: "s1".into(),
            table_name: "events".into(),
            index_name: "events_view".into(),
            if_not_exists: false,
            projected_columns: vec!["id".into(), "amount".into()],
        }),
        Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "s1".into(),
            table_name: "events".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row::from_values(vec![Value::Integer(1), Value::Integer(10)]),
        },
        Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "s1".into(),
            key: b"acct:1".to_vec(),
            value: b"10".to_vec(),
        },
        Mutation::EmitEvent {
            project_id: "p".into(),
            scope_id: "s1".into(),
            topic: "payments".into(),
            event_key: "evt-1".into(),
            payload_json: "{\"amount\":10}".into(),
        },
    ] {
        db.commit_as(CallerContext::system_internal(), mutation)
            .await
            .expect("seed matrix mutation");
    }
}

async fn grant_matrix_permissions(
    db: &AedbInstance,
    caller_id: &str,
    permissions: Vec<Permission>,
) {
    for permission in permissions {
        db.commit_as(
            CallerContext::system_internal(),
            Mutation::Ddl(DdlOperation::GrantPermission {
                caller_id: caller_id.into(),
                permission,
                actor_id: Some(SYSTEM_CALLER_ID.into()),
                delegable: false,
            }),
        )
        .await
        .expect("grant matrix permission");
    }
}

async fn revoke_matrix_permissions(
    db: &AedbInstance,
    caller_id: &str,
    permissions: Vec<Permission>,
) {
    for permission in permissions {
        db.commit_as(
            CallerContext::system_internal(),
            Mutation::Ddl(DdlOperation::RevokePermission {
                caller_id: caller_id.into(),
                permission,
                actor_id: Some(SYSTEM_CALLER_ID.into()),
            }),
        )
        .await
        .expect("revoke matrix permission");
    }
}

async fn assert_matrix_access(db: &AedbInstance, case: AuthzMatrixCase, caller_id: &str) {
    let caller = CallerContext::new(caller_id);
    match case {
        AuthzMatrixCase::TableRead => {
            db.query_with_options_as(
                Some(&caller),
                "p",
                "s1",
                Query::select(&["id"]).from("events").limit(1),
                QueryOptions::default(),
            )
            .await
            .unwrap_or_else(|err| panic!("{} should allow {caller_id}: {err:?}", case.name()));
        }
        AuthzMatrixCase::TableWrite => {
            db.commit_as(
                caller,
                Mutation::Upsert {
                    project_id: "p".into(),
                    scope_id: "s1".into(),
                    table_name: "events".into(),
                    primary_key: vec![Value::Integer(20)],
                    row: Row::from_values(vec![Value::Integer(20), Value::Integer(20)]),
                },
            )
            .await
            .unwrap_or_else(|err| panic!("{} should allow {caller_id}: {err:?}", case.name()));
        }
        AuthzMatrixCase::KvRead => {
            db.kv_get("p", "s1", b"acct:1", ConsistencyMode::AtLatest, &caller)
                .await
                .unwrap_or_else(|err| panic!("{} should allow {caller_id}: {err:?}", case.name()));
        }
        AuthzMatrixCase::KvWrite => {
            db.commit_as(
                caller,
                Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "s1".into(),
                    key: b"acct:2".to_vec(),
                    value: b"20".to_vec(),
                },
            )
            .await
            .unwrap_or_else(|err| panic!("{} should allow {caller_id}: {err:?}", case.name()));
        }
        AuthzMatrixCase::AsyncIndexRead => {
            db.query_with_options_as(
                Some(&caller),
                "p",
                "s1",
                Query::select(&["id"]).from("events").limit(1),
                QueryOptions {
                    async_index: Some("events_view".into()),
                    allow_full_scan: true,
                    ..QueryOptions::default()
                },
            )
            .await
            .unwrap_or_else(|err| panic!("{} should allow {caller_id}: {err:?}", case.name()));
        }
        AuthzMatrixCase::EventStreamRead => {
            db.read_event_stream_as(&caller, None, 0, 10, ConsistencyMode::AtLatest)
                .await
                .unwrap_or_else(|err| panic!("{} should allow {caller_id}: {err:?}", case.name()));
        }
        AuthzMatrixCase::ProcessorLagRead => {
            db.reactive_processor_lag_as(&caller, "matrix_processor", ConsistencyMode::AtLatest)
                .await
                .unwrap_or_else(|err| panic!("{} should allow {caller_id}: {err:?}", case.name()));
        }
    }
}

async fn assert_matrix_denied(db: &AedbInstance, case: AuthzMatrixCase, caller_id: &str) {
    let caller = CallerContext::new(caller_id);
    let denied = match case {
        AuthzMatrixCase::TableRead => db
            .query_with_options_as(
                Some(&caller),
                "p",
                "s1",
                Query::select(&["id"]).from("events").limit(1),
                QueryOptions::default(),
            )
            .await
            .is_err(),
        AuthzMatrixCase::TableWrite => db
            .commit_as(
                caller,
                Mutation::Upsert {
                    project_id: "p".into(),
                    scope_id: "s1".into(),
                    table_name: "events".into(),
                    primary_key: vec![Value::Integer(99)],
                    row: Row::from_values(vec![Value::Integer(99), Value::Integer(99)]),
                },
            )
            .await
            .is_err(),
        AuthzMatrixCase::KvRead => db
            .kv_get("p", "s1", b"acct:1", ConsistencyMode::AtLatest, &caller)
            .await
            .is_err(),
        AuthzMatrixCase::KvWrite => db
            .commit_as(
                caller,
                Mutation::KvSet {
                    project_id: "p".into(),
                    scope_id: "s1".into(),
                    key: b"acct:denied".to_vec(),
                    value: b"x".to_vec(),
                },
            )
            .await
            .is_err(),
        AuthzMatrixCase::AsyncIndexRead => db
            .query_with_options_as(
                Some(&caller),
                "p",
                "s1",
                Query::select(&["id"]).from("events").limit(1),
                QueryOptions {
                    async_index: Some("events_view".into()),
                    allow_full_scan: true,
                    ..QueryOptions::default()
                },
            )
            .await
            .is_err(),
        AuthzMatrixCase::EventStreamRead => db
            .read_event_stream_as(&caller, None, 0, 10, ConsistencyMode::AtLatest)
            .await
            .is_err(),
        AuthzMatrixCase::ProcessorLagRead => db
            .reactive_processor_lag_as(&caller, "matrix_processor", ConsistencyMode::AtLatest)
            .await
            .is_err(),
    };
    assert!(denied, "{} should deny {caller_id}", case.name());
}

#[tokio::test]
async fn authorization_matrix_helper_covers_core_resource_access() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_production(AedbConfig::production([14u8; 32]), dir.path())
        .expect("open");
    setup_authz_matrix_db(&db).await;

    db.commit_as(
        CallerContext::system_internal(),
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "global_admin".into(),
            permission: Permission::GlobalAdmin,
            actor_id: Some(SYSTEM_CALLER_ID.into()),
            delegable: false,
        }),
    )
    .await
    .expect("grant global admin");
    grant_matrix_permissions(
        &db,
        "project_admin",
        vec![Permission::ProjectAdmin {
            project_id: "p".into(),
        }],
    )
    .await;
    grant_matrix_permissions(
        &db,
        "scope_admin",
        vec![Permission::ScopeAdmin {
            project_id: "p".into(),
            scope_id: "s1".into(),
        }],
    )
    .await;

    let project_cases = [
        AuthzMatrixCase::TableRead,
        AuthzMatrixCase::TableWrite,
        AuthzMatrixCase::KvRead,
        AuthzMatrixCase::KvWrite,
        AuthzMatrixCase::AsyncIndexRead,
    ];
    for case in project_cases {
        grant_matrix_permissions(&db, "direct", case.direct_permissions()).await;
        assert_matrix_access(&db, case, "direct").await;
        revoke_matrix_permissions(&db, "direct", case.direct_permissions()).await;
        assert_matrix_denied(&db, case, "direct").await;

        assert_matrix_access(&db, case, "project_admin").await;
        assert_matrix_access(&db, case, "scope_admin").await;
        assert_matrix_access(&db, case, "global_admin").await;
    }

    db.commit_as(
        CallerContext::new("table_owner"),
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "owned_table_reader".into(),
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
    .expect("table owner can grant table read");
    assert_matrix_access(&db, AuthzMatrixCase::TableRead, "owned_table_reader").await;

    for case in [
        AuthzMatrixCase::EventStreamRead,
        AuthzMatrixCase::ProcessorLagRead,
    ] {
        grant_matrix_permissions(&db, "direct_system_reader", case.direct_permissions()).await;
        assert_matrix_access(&db, case, "direct_system_reader").await;
        revoke_matrix_permissions(&db, "direct_system_reader", case.direct_permissions()).await;
        assert_matrix_denied(&db, case, "direct_system_reader").await;
        assert_matrix_access(&db, case, "global_admin").await;
    }

    assert_matrix_denied(&db, AuthzMatrixCase::TableRead, "missing").await;
    assert_matrix_denied(&db, AuthzMatrixCase::KvRead, "missing").await;
}

#[tokio::test]
async fn secure_mode_rejects_all_public_no_auth_read_escape_hatches() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_production(AedbConfig::production([16u8; 32]), dir.path())
        .expect("open secure");

    let cases = [
        db.query(
            "secret_project",
            "app",
            Query::select(&["*"]).from("secret_table").limit(1),
        )
        .await
        .err()
        .map(|err| err.to_string())
        .expect("query should deny"),
        db.query_with_options(
            "secret_project",
            "app",
            Query::select(&["*"]).from("secret_table").limit(1),
            QueryOptions::default(),
        )
        .await
        .err()
        .map(|err| err.to_string())
        .expect("query_with_options should deny"),
        db.query_with_read_set(
            "secret_project",
            "app",
            Query::select(&["*"]).from("secret_table").limit(1),
        )
        .await
        .err()
        .map(|err| err.to_string())
        .expect("query_with_read_set should deny"),
        db.query_no_auth(
            "secret_project",
            "app",
            Query::select(&["*"]).from("secret_table").limit(1),
            QueryOptions::default(),
        )
        .await
        .err()
        .map(|err| err.to_string())
        .expect("query_no_auth should deny"),
        db.kv_get_no_auth(
            "secret_project",
            "app",
            b"secret-key",
            ConsistencyMode::AtLatest,
        )
        .await
        .err()
        .map(|err| err.to_string())
        .expect("kv_get_no_auth should deny"),
        db.kv_get_many_no_auth(
            "secret_project",
            "app",
            &[b"secret-key".to_vec()],
            ConsistencyMode::AtLatest,
        )
        .await
        .err()
        .map(|err| err.to_string())
        .expect("kv_get_many_no_auth should deny"),
        db.kv_scan_prefix_no_auth(
            "secret_project",
            "app",
            b"secret-prefix",
            10,
            ConsistencyMode::AtLatest,
        )
        .await
        .err()
        .map(|err| err.to_string())
        .expect("kv_scan_prefix_no_auth should deny"),
    ];

    for rendered in cases {
        assert!(rendered.contains("permission denied"));
        assert!(!rendered.contains("secret_project"));
        assert!(!rendered.contains("secret_table"));
        assert!(!rendered.contains("secret-key"));
        assert!(!rendered.contains("secret-prefix"));
    }

    assert!(matches!(
        db.read_event_stream(None, 0, 1, ConsistencyMode::AtLatest)
            .await
            .expect_err("anonymous event stream should deny"),
        AedbError::PermissionDenied(_)
    ));
    assert!(matches!(
        db.reactive_processor_lag("secret_processor", ConsistencyMode::AtLatest)
            .await
            .expect_err("anonymous processor lag should deny"),
        AedbError::PermissionDenied(_)
    ));
    let backup_dir = tempdir().expect("backup temp");
    assert!(matches!(
        db.backup_full(backup_dir.path())
            .await
            .expect_err("anonymous backup should deny"),
        AedbError::PermissionDenied(_)
    ));
    let incremental_backup_dir = tempdir().expect("incremental backup temp");
    let parent_backup_dir = tempdir().expect("parent backup temp");
    assert!(matches!(
        db.backup_incremental(incremental_backup_dir.path(), parent_backup_dir.path())
            .await
            .expect_err("anonymous incremental backup should deny"),
        AedbError::PermissionDenied(_)
    ));
    let backup_file_dir = tempdir().expect("backup file temp");
    let backup_file = backup_file_dir.path().join("backup.aedbarc");
    assert!(matches!(
        db.backup_full_to_file(&backup_file)
            .await
            .expect_err("anonymous backup file should deny"),
        AedbError::PermissionDenied(_)
    ));
    assert!(matches!(
        db.apply_migration(crate::migration::Migration {
            version: 1,
            name: "secret migration".into(),
            project_id: "secret_project".into(),
            scope_id: "app".into(),
            mutations: Vec::new(),
            down_mutations: None,
        })
        .await
        .expect_err("anonymous migration should deny"),
        AedbError::PermissionDenied(_)
    ));
}

#[test]
fn public_no_auth_and_unchecked_surface_is_intentional() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut exposed = Vec::new();
    for rel in [
        "src/api/kv_api.rs",
        "src/api/query_api.rs",
        "src/sync_bridge.rs",
    ] {
        let source = fs::read_to_string(root.join(rel)).expect("read source");
        for line in source.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("pub ")
                && (trimmed.contains("_no_auth") || trimmed.contains("unchecked"))
            {
                exposed.push(format!("{rel}:{trimmed}"));
            }
        }
    }
    exposed.sort();
    assert_eq!(
        exposed,
        vec![
            "src/api/kv_api.rs:pub async fn kv_get_many_no_auth(".to_string(),
            "src/api/kv_api.rs:pub async fn kv_get_no_auth(".to_string(),
            "src/api/kv_api.rs:pub async fn kv_scan_prefix_no_auth(".to_string(),
            "src/api/query_api.rs:pub async fn query_no_auth(".to_string(),
            "src/sync_bridge.rs:pub fn query_no_auth(".to_string(),
        ],
        "public no_auth/unchecked API surface changed; update secure-mode tests and docs intentionally"
    );
}

#[tokio::test]
async fn permission_denied_message_snapshots_do_not_disclose_protected_data() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_production(AedbConfig::production([17u8; 32]), dir.path())
        .expect("open");
    setup_secure_assertion_denial_fixture(&db).await;

    let commit_denied = db
        .commit_as(
            CallerContext::new("mallory"),
            Mutation::Upsert {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "secret_rows".into(),
                primary_key: vec![Value::Integer(777)],
                row: Row::from_values(vec![Value::Integer(777), Value::Integer(42)]),
            },
        )
        .await
        .expect_err("table write should deny")
        .to_string();
    let query_denied = db
        .query_with_options_as(
            Some(&CallerContext::new("mallory")),
            "p",
            "app",
            Query::select(&["amount"])
                .from("secret_rows")
                .where_(Expr::Eq("amount".into(), Value::Integer(42))),
            QueryOptions::default(),
        )
        .await
        .expect_err("query should deny")
        .to_string();
    let kv_denied = db
        .kv_get(
            "p",
            "app",
            b"secret:key",
            ConsistencyMode::AtLatest,
            &CallerContext::new("mallory"),
        )
        .await
        .expect_err("kv should deny")
        .to_string();
    let event_denied = db
        .read_event_stream_as(
            &CallerContext::new("mallory"),
            None,
            0,
            1,
            ConsistencyMode::AtLatest,
        )
        .await
        .expect_err("event stream should deny")
        .to_string();
    let processor_denied = db
        .reactive_processor_lag_as(
            &CallerContext::new("mallory"),
            "secret_processor",
            ConsistencyMode::AtLatest,
        )
        .await
        .expect_err("processor lag should deny")
        .to_string();

    let snapshots = [
        (
            "commit",
            commit_denied.as_str(),
            "permission denied: permission denied",
        ),
        (
            "query",
            query_denied.as_str(),
            "permission denied: permission denied",
        ),
        (
            "kv",
            kv_denied.as_str(),
            "permission denied: permission denied",
        ),
        (
            "event",
            event_denied.as_str(),
            "permission denied: permission denied",
        ),
        (
            "processor",
            processor_denied.as_str(),
            "permission denied: permission denied",
        ),
    ];
    for (label, rendered, expected) in snapshots {
        assert_eq!(rendered, expected, "{label} denial snapshot changed");
        for forbidden in [
            "secret_rows",
            "secret:key",
            "secret_processor",
            "amount",
            "777",
            "42",
            "mallory",
        ] {
            assert!(
                !rendered.contains(forbidden),
                "{label} denial leaked {forbidden}: {rendered}"
            );
        }
    }
}

async fn setup_secure_assertion_denial_fixture(db: &AedbInstance) {
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
    db.commit_as(
        system.clone(),
        Mutation::Ddl(DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "secret_rows".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "amount".into(),
                    col_type: ColumnType::Integer,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
        }),
    )
    .await
    .expect("create secret table");
    for mutation in [
        Mutation::KvSet {
            project_id: "p".into(),
            scope_id: "app".into(),
            key: b"secret:key".to_vec(),
            value: b"secret-value".to_vec(),
        },
        Mutation::Upsert {
            project_id: "p".into(),
            scope_id: "app".into(),
            table_name: "secret_rows".into(),
            primary_key: vec![Value::Integer(1)],
            row: Row::from_values(vec![Value::Integer(1), Value::Integer(42)]),
        },
        Mutation::Ddl(DdlOperation::GrantPermission {
            caller_id: "writer".into(),
            permission: Permission::KvWrite {
                project_id: "p".into(),
                scope_id: Some("app".into()),
                prefix: Some(b"public:".to_vec()),
            },
            actor_id: Some(SYSTEM_CALLER_ID.into()),
            delegable: false,
        }),
    ] {
        db.commit_as(system.clone(), mutation)
            .await
            .expect("seed assertion denial fixture");
    }
}

fn assertion_denial_cases() -> Vec<(&'static str, ReadAssertion)> {
    vec![
        (
            "key equals",
            ReadAssertion::KeyEquals {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"secret:key".to_vec(),
                expected: b"wrong".to_vec(),
            },
        ),
        (
            "key compare",
            ReadAssertion::KeyCompare {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"secret:key".to_vec(),
                op: CompareOp::Eq,
                threshold: b"wrong".to_vec(),
            },
        ),
        (
            "key exists",
            ReadAssertion::KeyExists {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"secret:key".to_vec(),
                expected: false,
            },
        ),
        (
            "key version",
            ReadAssertion::KeyVersion {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"secret:key".to_vec(),
                expected_seq: 999,
            },
        ),
        (
            "row version",
            ReadAssertion::RowVersion {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "secret_rows".into(),
                primary_key: vec![Value::Integer(1)],
                expected_seq: 999,
            },
        ),
        (
            "row exists",
            ReadAssertion::RowExists {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "secret_rows".into(),
                primary_key: vec![Value::Integer(1)],
                expected: false,
            },
        ),
        (
            "row column compare",
            ReadAssertion::RowColumnCompare {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "secret_rows".into(),
                primary_key: vec![Value::Integer(1)],
                column: "amount".into(),
                op: CompareOp::Eq,
                threshold: Value::Integer(0),
            },
        ),
        (
            "count compare",
            ReadAssertion::CountCompare {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "secret_rows".into(),
                filter: Some(Expr::Eq("amount".into(), Value::Integer(42))),
                op: CompareOp::Eq,
                threshold: 0,
            },
        ),
        (
            "sum compare",
            ReadAssertion::SumCompare {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "secret_rows".into(),
                column: "amount".into(),
                filter: Some(Expr::Eq("id".into(), Value::Integer(1))),
                op: CompareOp::Eq,
                threshold: Value::Integer(0),
            },
        ),
        (
            "nested all",
            ReadAssertion::All(vec![ReadAssertion::KeyExists {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"secret:key".to_vec(),
                expected: false,
            }]),
        ),
        (
            "nested any",
            ReadAssertion::Any(vec![ReadAssertion::RowExists {
                project_id: "p".into(),
                scope_id: "app".into(),
                table_name: "secret_rows".into(),
                primary_key: vec![Value::Integer(1)],
                expected: false,
            }]),
        ),
        (
            "nested not",
            ReadAssertion::Not(Box::new(ReadAssertion::KeyEquals {
                project_id: "p".into(),
                scope_id: "app".into(),
                key: b"secret:key".to_vec(),
                expected: b"secret-value".to_vec(),
            })),
        ),
    ]
}

#[tokio::test]
async fn secure_mode_denies_every_read_assertion_variant_before_evaluation() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_production(AedbConfig::production([15u8; 32]), dir.path())
        .expect("open");
    setup_secure_assertion_denial_fixture(&db).await;

    for (label, assertion) in assertion_denial_cases() {
        let result = db
            .commit_envelope(TransactionEnvelope {
                caller: Some(CallerContext::new("writer")),
                idempotency_key: None,
                write_class: WriteClass::Standard,
                assertions: vec![assertion],
                read_set: ReadSet::default(),
                write_intent: WriteIntent {
                    mutations: vec![Mutation::KvSet {
                        project_id: "p".into(),
                        scope_id: "app".into(),
                        key: format!("public:{label}").into_bytes(),
                        value: b"ok".to_vec(),
                    }],
                },
                base_seq: 0,
            })
            .await;
        let err = match result {
            Ok(ok) => panic!(
                "{label} assertion unexpectedly committed at seq {}",
                ok.commit_seq
            ),
            Err(err) => err,
        };
        assert!(
            matches!(err, AedbError::PermissionDenied(_)),
            "{label} should be denied before assertion evaluation, got {err:?}"
        );
        let rendered = err.to_string();
        for forbidden in [
            "secret_rows",
            "secret:key",
            "secret-value",
            "amount",
            "42",
            "public:",
        ] {
            assert!(
                !rendered.contains(forbidden),
                "{label} permission denial leaked {forbidden}: {rendered}"
            );
        }
    }
}
