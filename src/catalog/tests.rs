use super::{Catalog, DdlOperation, namespace_key};
use crate::catalog::schema::{ColumnDef, ForeignKey, ForeignKeyAction, IndexType, TableAlteration};
use crate::catalog::types::ColumnType;
use crate::error::AedbError;
use crate::permission::Permission;

fn users_columns() -> Vec<ColumnDef> {
    vec![
        ColumnDef {
            name: "id".to_string(),
            col_type: ColumnType::Integer,
            nullable: false,
        },
        ColumnDef {
            name: "name".to_string(),
            col_type: ColumnType::Text,
            nullable: false,
        },
    ]
}

#[test]
fn project_isolation_allows_same_table_name() {
    let mut c = Catalog::default();
    c.create_project("A").expect("project A");
    c.create_project("B").expect("project B");

    c.create_table("A", "app", "users", users_columns(), vec!["id".into()])
        .expect("table A");
    c.create_table("B", "app", "users", users_columns(), vec!["id".into()])
        .expect("table B");

    assert!(
        c.tables
            .contains_key(&(namespace_key("A", "app"), "users".into()))
    );
    assert!(
        c.tables
            .contains_key(&(namespace_key("B", "app"), "users".into()))
    );
}

#[test]
fn drop_project_cascades_tables_and_indexes() {
    let mut c = Catalog::default();
    c.create_project("A").expect("create");
    for t in ["t1", "t2", "t3"] {
        c.create_table("A", "app", t, users_columns(), vec!["id".into()])
            .expect("table");
        c.create_index(
            "A",
            "app",
            t,
            "by_name",
            vec!["name".into()],
            IndexType::BTree,
            None,
        )
        .expect("index");
    }
    c.drop_project("A").expect("drop");
    assert!(!c.projects.contains_key("A"));
    assert!(c.tables.keys().all(|(p, _)| !p.starts_with("A::")));
    assert!(c.indexes.keys().all(|(p, _, _)| !p.starts_with("A::")));
}

#[test]
fn ddl_payload_roundtrip_and_apply() {
    let mut c = Catalog::default();
    let ops = vec![
        DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "A".into(),
        },
        DdlOperation::CreateTable {
            owner_id: None,
            if_not_exists: false,
            project_id: "A".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            columns: users_columns(),
            primary_key: vec!["id".into()],
        },
        DdlOperation::AlterTable {
            project_id: "A".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            alteration: TableAlteration::AddColumn(ColumnDef {
                name: "email".into(),
                col_type: ColumnType::Text,
                nullable: true,
            }),
        },
        DdlOperation::CreateIndex {
            project_id: "A".into(),
            scope_id: "app".into(),
            table_name: "users".into(),
            index_name: "by_name".into(),
            if_not_exists: false,
            columns: vec!["name".into()],
            index_type: IndexType::BTree,
            partial_filter: None,
        },
    ];

    for op in ops {
        let payload = Catalog::ddl_payload(&op).expect("encode");
        let decoded = Catalog::ddl_from_payload(&payload).expect("decode");
        c.apply_ddl(decoded).expect("apply");
    }

    assert!(
        c.tables
            .contains_key(&(namespace_key("A", "app"), "users".into()))
    );
    assert!(
        c.indexes
            .contains_key(&(namespace_key("A", "app"), "users".into(), "by_name".into()))
    );
}

#[test]
fn test_grant_and_revoke_permissions() {
    let mut c = Catalog::default();
    c.grant_permission(
        "caller",
        Permission::KvRead {
            project_id: "A".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
        None,
        false,
    )
    .expect("grant");
    assert!(c.has_permission(
        "caller",
        &Permission::KvRead {
            project_id: "A".into(),
            scope_id: Some("app".into()),
            prefix: None,
        }
    ));
    c.revoke_permission(
        "caller",
        &Permission::KvRead {
            project_id: "A".into(),
            scope_id: Some("app".into()),
            prefix: None,
        },
    )
    .expect("revoke");
    assert!(!c.has_permission(
        "caller",
        &Permission::KvRead {
            project_id: "A".into(),
            scope_id: Some("app".into()),
            prefix: None,
        }
    ));
}

#[test]
fn grant_permission_records_metadata_without_actor() {
    let mut c = Catalog::default();
    let permission = Permission::KvRead {
        project_id: "A".into(),
        scope_id: Some("app".into()),
        prefix: None,
    };
    c.grant_permission("caller", permission.clone(), None, true)
        .expect("grant");

    let meta = c
        .permission_grants
        .get(&("caller".to_string(), permission))
        .expect("metadata");
    assert!(meta.delegable);
    assert_eq!(meta.granted_by, "system");
}

#[test]
fn alter_table_rejects_foreign_key_cycles() {
    let mut c = Catalog::default();
    c.create_project("A").expect("project");
    c.create_scope("A", "app").expect("scope");
    c.create_table("A", "app", "parents", users_columns(), vec!["id".into()])
        .expect("parents");
    c.create_table("A", "app", "children", users_columns(), vec!["id".into()])
        .expect("children");
    c.alter_table(
        "A",
        "app",
        "children",
        TableAlteration::AddForeignKey(ForeignKey {
            name: "child_parent".into(),
            columns: vec!["id".into()],
            references_project_id: "A".into(),
            references_scope_id: "app".into(),
            references_table: "parents".into(),
            references_columns: vec!["id".into()],
            on_delete: ForeignKeyAction::Cascade,
            on_update: ForeignKeyAction::Cascade,
        }),
    )
    .expect("first fk");

    let err = c
        .alter_table(
            "A",
            "app",
            "parents",
            TableAlteration::AddForeignKey(ForeignKey {
                name: "parent_child".into(),
                columns: vec!["id".into()],
                references_project_id: "A".into(),
                references_scope_id: "app".into(),
                references_table: "children".into(),
                references_columns: vec!["id".into()],
                on_delete: ForeignKeyAction::Cascade,
                on_update: ForeignKeyAction::Cascade,
            }),
        )
        .expect_err("cycle should be rejected");
    assert!(matches!(err, AedbError::Validation(ref msg) if msg.contains("foreign key cycle")));
}

#[test]
fn alter_table_rejects_drop_column_with_index_dependency() {
    let mut c = Catalog::default();
    c.create_project("A").expect("project");
    c.create_table("A", "app", "users", users_columns(), vec!["id".into()])
        .expect("table");
    c.create_index(
        "A",
        "app",
        "users",
        "by_name",
        vec!["name".into()],
        IndexType::BTree,
        None,
    )
    .expect("index");

    let err = c
        .alter_table(
            "A",
            "app",
            "users",
            TableAlteration::DropColumn {
                name: "name".into(),
            },
        )
        .expect_err("drop should fail");
    assert!(err.to_string().contains("dependencies"));
}

#[test]
fn alter_table_rejects_rename_column_with_inbound_fk_dependency() {
    let mut c = Catalog::default();
    c.create_project("A").expect("project");
    c.create_table(
        "A",
        "app",
        "parents",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "code".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        vec!["id".into()],
    )
    .expect("parents");
    c.create_table(
        "A",
        "app",
        "children",
        vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Integer,
                nullable: false,
            },
            ColumnDef {
                name: "parent_code".into(),
                col_type: ColumnType::Text,
                nullable: false,
            },
        ],
        vec!["id".into()],
    )
    .expect("children");
    c.alter_table(
        "A",
        "app",
        "children",
        TableAlteration::AddForeignKey(ForeignKey {
            name: "fk_parent_code".into(),
            columns: vec!["parent_code".into()],
            references_project_id: "A".into(),
            references_scope_id: "app".into(),
            references_table: "parents".into(),
            references_columns: vec!["code".into()],
            on_delete: ForeignKeyAction::Restrict,
            on_update: ForeignKeyAction::Restrict,
        }),
    )
    .expect("fk");

    let err = c
        .alter_table(
            "A",
            "app",
            "parents",
            TableAlteration::RenameColumn {
                from: "code".into(),
                to: "external_code".into(),
            },
        )
        .expect_err("rename should fail");
    assert!(err.to_string().contains("dependencies"));
}

#[test]
fn rejects_reserved_and_ambiguous_namespace_ids() {
    let mut c = Catalog::default();
    let reserved = c
        .create_project("_system")
        .expect_err("reserved project id must fail");
    assert!(reserved.to_string().contains("reserved"));

    let bad_project = c
        .create_project("A::B")
        .expect_err("separator in project id must fail");
    assert!(bad_project.to_string().contains("project_id"));

    c.create_project("A").expect("valid project");
    let bad_scope = c
        .create_scope("A", "x::y")
        .expect_err("separator in scope id must fail");
    assert!(bad_scope.to_string().contains("scope_id"));

    c.create_project("_global")
        .expect("global project is allowed");
}

#[test]
fn rejects_invalid_table_and_index_identifiers() {
    let mut c = Catalog::default();
    c.create_project("A").expect("create project");

    let bad_table = c
        .create_table("A", "app", "users.v2", users_columns(), vec!["id".into()])
        .expect_err("table name with dot should fail");
    assert!(bad_table.to_string().contains("table_name"));

    c.create_table("A", "app", "users", users_columns(), vec!["id".into()])
        .expect("valid table");

    let bad_index = c
        .create_index(
            "A",
            "app",
            "users",
            "by name",
            vec!["name".into()],
            IndexType::BTree,
            None,
        )
        .expect_err("index name with space should fail");
    assert!(bad_index.to_string().contains("index_name"));
}

#[test]
fn rejects_overlong_namespace_identifiers() {
    let mut c = Catalog::default();
    let too_long = "a".repeat(129);
    let err = c
        .create_project(&too_long)
        .expect_err("overlong project id should fail");
    assert!(err.to_string().contains("<= 128"));
}

/// Test suite for KV prefix permission semantics
mod kv_prefix_tests {
    use crate::catalog::{Catalog, permission_allows_kv};
    use crate::permission::Permission;

    #[test]
    fn none_prefix_allows_all_keys() {
        // None prefix means unrestricted access within the scope
        let perm = Permission::KvRead {
            project_id: "proj1".into(),
            scope_id: Some("app".into()),
            prefix: None,
        };

        // Should allow any key
        assert!(permission_allows_kv(&perm, "proj1", "app", &[0x01], true));
        assert!(permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0xFF, 0xFF],
            true
        ));
        assert!(permission_allows_kv(&perm, "proj1", "app", &[], true));
        assert!(permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0x00, 0x01, 0x02, 0x03],
            true
        ));
    }

    #[test]
    fn empty_prefix_allows_all_keys() {
        // Empty prefix (Some(&[])) means every key starts with empty prefix
        let perm = Permission::KvRead {
            project_id: "proj1".into(),
            scope_id: Some("app".into()),
            prefix: Some(vec![]),
        };

        // Should allow any key (since all keys start with empty prefix)
        assert!(permission_allows_kv(&perm, "proj1", "app", &[0x01], true));
        assert!(permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0xFF, 0xFF],
            true
        ));
        assert!(permission_allows_kv(&perm, "proj1", "app", &[], true));
        assert!(permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0x00, 0x01, 0x02, 0x03],
            true
        ));
    }

    #[test]
    fn exact_prefix_match() {
        let perm = Permission::KvRead {
            project_id: "proj1".into(),
            scope_id: Some("app".into()),
            prefix: Some(vec![0x01, 0x02]),
        };

        // Key exactly equals prefix - should match
        assert!(permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0x01, 0x02],
            true
        ));

        // Key starts with prefix - should match
        assert!(permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0x01, 0x02, 0x03],
            true
        ));
        assert!(permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0x01, 0x02, 0xFF, 0xFF],
            true
        ));

        // Key does not start with prefix - should NOT match
        assert!(!permission_allows_kv(&perm, "proj1", "app", &[0x01], true));
        assert!(!permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0x01, 0x03],
            true
        ));
        assert!(!permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0x02, 0x02],
            true
        ));
        assert!(!permission_allows_kv(&perm, "proj1", "app", &[], true));
    }

    #[test]
    fn overlapping_prefixes_both_grant_access() {
        // This tests the critical edge case: what happens when user has two
        // overlapping prefixes? Both should independently grant access.

        let prefix_short = Permission::KvRead {
            project_id: "proj1".into(),
            scope_id: Some("app".into()),
            prefix: Some(vec![0x01]),
        };

        let prefix_long = Permission::KvRead {
            project_id: "proj1".into(),
            scope_id: Some("app".into()),
            prefix: Some(vec![0x01, 0x02]),
        };

        // Key [0x01] - only matches short prefix
        assert!(permission_allows_kv(
            &prefix_short,
            "proj1",
            "app",
            &[0x01],
            true
        ));
        assert!(!permission_allows_kv(
            &prefix_long,
            "proj1",
            "app",
            &[0x01],
            true
        ));

        // Key [0x01, 0x02] - matches BOTH prefixes
        assert!(permission_allows_kv(
            &prefix_short,
            "proj1",
            "app",
            &[0x01, 0x02],
            true
        ));
        assert!(permission_allows_kv(
            &prefix_long,
            "proj1",
            "app",
            &[0x01, 0x02],
            true
        ));

        // Key [0x01, 0x02, 0x03] - matches BOTH prefixes
        assert!(permission_allows_kv(
            &prefix_short,
            "proj1",
            "app",
            &[0x01, 0x02, 0x03],
            true
        ));
        assert!(permission_allows_kv(
            &prefix_long,
            "proj1",
            "app",
            &[0x01, 0x02, 0x03],
            true
        ));

        // Key [0x01, 0xFF] - only matches short prefix
        assert!(permission_allows_kv(
            &prefix_short,
            "proj1",
            "app",
            &[0x01, 0xFF],
            true
        ));
        assert!(!permission_allows_kv(
            &prefix_long,
            "proj1",
            "app",
            &[0x01, 0xFF],
            true
        ));
    }

    #[test]
    fn prefix_boundary_at_key_length() {
        // Test that prefix matching works correctly when prefix length equals key length
        let perm = Permission::KvWrite {
            project_id: "proj1".into(),
            scope_id: Some("app".into()),
            prefix: Some(vec![0xAA, 0xBB, 0xCC]),
        };

        // Exact match - prefix length == key length
        assert!(permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0xAA, 0xBB, 0xCC],
            false
        ));

        // Key shorter than prefix - should NOT match
        assert!(!permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0xAA, 0xBB],
            false
        ));
        assert!(!permission_allows_kv(&perm, "proj1", "app", &[0xAA], false));

        // Key longer than prefix with matching prefix - should match
        assert!(permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0xAA, 0xBB, 0xCC, 0xDD],
            false
        ));
    }

    #[test]
    fn scope_id_none_vs_some() {
        // scope_id: None means project-wide access across all scopes
        let project_wide = Permission::KvRead {
            project_id: "proj1".into(),
            scope_id: None,
            prefix: Some(vec![0x01]),
        };

        // Should match ANY scope within the project
        assert!(permission_allows_kv(
            &project_wide,
            "proj1",
            "app",
            &[0x01, 0x02],
            true
        ));
        assert!(permission_allows_kv(
            &project_wide,
            "proj1",
            "staging",
            &[0x01, 0x02],
            true
        ));
        assert!(permission_allows_kv(
            &project_wide,
            "proj1",
            "prod",
            &[0x01, 0x02],
            true
        ));

        // But not a different project
        assert!(!permission_allows_kv(
            &project_wide,
            "proj2",
            "app",
            &[0x01, 0x02],
            true
        ));

        // scope_id: Some means exact scope match required
        let scope_specific = Permission::KvRead {
            project_id: "proj1".into(),
            scope_id: Some("app".into()),
            prefix: Some(vec![0x01]),
        };

        // Should only match the specific scope
        assert!(permission_allows_kv(
            &scope_specific,
            "proj1",
            "app",
            &[0x01, 0x02],
            true
        ));
        assert!(!permission_allows_kv(
            &scope_specific,
            "proj1",
            "staging",
            &[0x01, 0x02],
            true
        ));
    }

    #[test]
    fn read_vs_write_permissions() {
        let read_perm = Permission::KvRead {
            project_id: "proj1".into(),
            scope_id: Some("app".into()),
            prefix: Some(vec![0x01]),
        };

        let write_perm = Permission::KvWrite {
            project_id: "proj1".into(),
            scope_id: Some("app".into()),
            prefix: Some(vec![0x01]),
        };

        let key = &[0x01, 0x02];

        // Read permission should only allow read operations (read=true)
        assert!(permission_allows_kv(&read_perm, "proj1", "app", key, true));
        assert!(!permission_allows_kv(
            &read_perm, "proj1", "app", key, false
        ));

        // Write permission should only allow write operations (read=false)
        assert!(!permission_allows_kv(
            &write_perm,
            "proj1",
            "app",
            key,
            true
        ));
        assert!(permission_allows_kv(
            &write_perm,
            "proj1",
            "app",
            key,
            false
        ));
    }

    #[test]
    fn global_admin_bypasses_all_checks() {
        let perm = Permission::GlobalAdmin;

        // GlobalAdmin should allow any operation on any project/scope/key
        assert!(permission_allows_kv(
            &perm,
            "any-project",
            "any-scope",
            &[],
            true
        ));
        assert!(permission_allows_kv(
            &perm,
            "any-project",
            "any-scope",
            &[0xFF],
            false
        ));
        assert!(permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0x01, 0x02, 0x03],
            true
        ));
    }

    #[test]
    fn project_admin_bypasses_prefix_checks() {
        let perm = Permission::ProjectAdmin {
            project_id: "proj1".into(),
        };

        // ProjectAdmin should allow any operation within the project, any scope, any key
        assert!(permission_allows_kv(&perm, "proj1", "app", &[], true));
        assert!(permission_allows_kv(&perm, "proj1", "app", &[0xFF], false));
        assert!(permission_allows_kv(
            &perm,
            "proj1",
            "staging",
            &[0x01, 0x02],
            true
        ));

        // But not a different project
        assert!(!permission_allows_kv(&perm, "proj2", "app", &[], true));
    }

    #[test]
    fn scope_admin_bypasses_prefix_checks_within_scope() {
        let perm = Permission::ScopeAdmin {
            project_id: "proj1".into(),
            scope_id: "app".into(),
        };

        // ScopeAdmin should allow any operation within the specific project+scope
        assert!(permission_allows_kv(&perm, "proj1", "app", &[], true));
        assert!(permission_allows_kv(&perm, "proj1", "app", &[0xFF], false));
        assert!(permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0x01, 0x02],
            true
        ));

        // But not a different scope or project
        assert!(!permission_allows_kv(&perm, "proj1", "staging", &[], true));
        assert!(!permission_allows_kv(&perm, "proj2", "app", &[], true));
    }

    #[test]
    fn wrong_project_denies_access() {
        let perm = Permission::KvRead {
            project_id: "proj1".into(),
            scope_id: Some("app".into()),
            prefix: Some(vec![0x01]),
        };

        // Even with matching scope and key, wrong project should deny
        assert!(!permission_allows_kv(
            &perm,
            "proj2",
            "app",
            &[0x01, 0x02],
            true
        ));
    }

    #[test]
    fn unicode_safe_binary_prefix() {
        // Prefixes are binary, not string-based, so should work with any byte values
        let perm = Permission::KvRead {
            project_id: "proj1".into(),
            scope_id: Some("app".into()),
            prefix: Some(vec![0xFF, 0xFE, 0xFD]), // Invalid UTF-8
        };

        // Should match keys starting with these bytes
        assert!(permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0xFF, 0xFE, 0xFD],
            true
        ));
        assert!(permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0xFF, 0xFE, 0xFD, 0x00],
            true
        ));
        assert!(!permission_allows_kv(
            &perm,
            "proj1",
            "app",
            &[0xFF, 0xFE],
            true
        ));
    }

    #[test]
    fn integration_has_kv_permission() {
        // Test the full has_kv_permission flow with multiple permissions
        let mut catalog = Catalog::default();
        catalog.create_project("proj1").unwrap();

        let caller_id = "user1";

        // Grant overlapping permissions
        catalog
            .grant_permission(
                caller_id,
                Permission::KvRead {
                    project_id: "proj1".into(),
                    scope_id: Some("app".into()),
                    prefix: Some(vec![0x01]),
                },
                None,
                false,
            )
            .unwrap();

        catalog
            .grant_permission(
                caller_id,
                Permission::KvRead {
                    project_id: "proj1".into(),
                    scope_id: Some("app".into()),
                    prefix: Some(vec![0x01, 0x02]),
                },
                None,
                false,
            )
            .unwrap();

        // Should be able to read keys matching either prefix
        assert!(catalog.has_kv_permission(caller_id, "proj1", "app", &[0x01], true));
        assert!(catalog.has_kv_permission(caller_id, "proj1", "app", &[0x01, 0x02], true));
        assert!(catalog.has_kv_permission(caller_id, "proj1", "app", &[0x01, 0x02, 0x03], true));
        assert!(catalog.has_kv_permission(caller_id, "proj1", "app", &[0x01, 0xFF], true));

        // Should NOT be able to read keys not matching any prefix
        assert!(!catalog.has_kv_permission(caller_id, "proj1", "app", &[0x02], true));
        assert!(!catalog.has_kv_permission(caller_id, "proj1", "app", &[], true));

        // Should NOT be able to write (only granted read)
        assert!(!catalog.has_kv_permission(caller_id, "proj1", "app", &[0x01], false));
    }
}
