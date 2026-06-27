#![allow(dead_code)]

use aedb::AedbInstance;
use aedb::catalog::DdlOperation;
use aedb::catalog::schema::{ColumnDef, IndexType};
use aedb::catalog::types::{ColumnType, Row, Value};
use aedb::commit::validation::Mutation;
use aedb::config::AedbConfig;
use aedb::permission::Permission;
use aedb::query::plan::Expr;
use tempfile::TempDir;

pub const PROJECT: &str = "p";
pub const SCOPE: &str = "app";

pub struct TestDb {
    pub db: AedbInstance,
    pub dir: TempDir,
}

pub fn open_db(config: AedbConfig) -> TestDb {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = AedbInstance::open_anonymous(config, dir.path()).expect("open db");
    TestDb { db, dir }
}

pub fn open_secure_db(config: AedbConfig) -> TestDb {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = AedbInstance::open_secure(config, dir.path()).expect("open secure db");
    TestDb { db, dir }
}

pub async fn create_project_scope(db: &AedbInstance, project_id: &str, scope_id: &str) {
    db.create_project(project_id).await.expect("create project");
    db.create_scope(project_id, scope_id)
        .await
        .expect("create scope");
}

pub fn id_owner_amount_columns() -> Vec<ColumnDef> {
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
        ColumnDef {
            name: "amount".into(),
            col_type: ColumnType::Integer,
            nullable: false,
        },
    ]
}

pub async fn create_table(
    db: &AedbInstance,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    columns: Vec<ColumnDef>,
) {
    db.commit(Mutation::Ddl(DdlOperation::CreateTable {
        project_id: project_id.into(),
        scope_id: scope_id.into(),
        table_name: table_name.into(),
        owner_id: None,
        if_not_exists: false,
        columns,
        primary_key: vec!["id".into()],
    }))
    .await
    .expect("create table");
}

pub async fn create_owner_index(
    db: &AedbInstance,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
) {
    db.commit(Mutation::Ddl(DdlOperation::CreateIndex {
        project_id: project_id.into(),
        scope_id: scope_id.into(),
        table_name: table_name.into(),
        index_name: "by_owner".into(),
        if_not_exists: false,
        columns: vec!["owner".into()],
        index_type: IndexType::BTree,
        partial_filter: None,
    }))
    .await
    .expect("create owner index");
}

pub async fn create_async_index(
    db: &AedbInstance,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    index_name: &str,
    projected_columns: Vec<&str>,
) {
    db.commit(Mutation::Ddl(DdlOperation::CreateAsyncIndex {
        project_id: project_id.into(),
        scope_id: scope_id.into(),
        table_name: table_name.into(),
        index_name: index_name.into(),
        if_not_exists: false,
        projected_columns: projected_columns.into_iter().map(str::to_string).collect(),
    }))
    .await
    .expect("create async index");
}

pub async fn grant(db: &AedbInstance, caller_id: &str, permission: Permission) {
    db.commit(Mutation::Ddl(DdlOperation::GrantPermission {
        caller_id: caller_id.into(),
        permission,
        actor_id: None,
        delegable: false,
    }))
    .await
    .expect("grant permission");
}

pub async fn grant_table_read(
    db: &AedbInstance,
    caller_id: &str,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
) {
    grant(
        db,
        caller_id,
        Permission::TableRead {
            project_id: project_id.into(),
            scope_id: scope_id.into(),
            table_name: table_name.into(),
        },
    )
    .await;
}

pub async fn grant_index_read(
    db: &AedbInstance,
    caller_id: &str,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    index_name: &str,
) {
    grant(
        db,
        caller_id,
        Permission::IndexRead {
            project_id: project_id.into(),
            scope_id: scope_id.into(),
            table_name: table_name.into(),
            index_name: index_name.into(),
        },
    )
    .await;
}

pub async fn set_owner_read_policy(
    db: &AedbInstance,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
) {
    db.set_read_policy(
        project_id,
        scope_id,
        table_name,
        Expr::Eq("owner".into(), Value::Text("$caller_id".into())),
    )
    .await
    .expect("set read policy");
}

pub async fn seed_owned_rows(
    db: &AedbInstance,
    project_id: &str,
    scope_id: &str,
    table_name: &str,
    rows: &[(i64, &str, i64)],
) {
    for (id, owner, amount) in rows {
        db.commit(Mutation::Upsert {
            project_id: project_id.into(),
            scope_id: scope_id.into(),
            table_name: table_name.into(),
            primary_key: vec![Value::Integer(*id)],
            row: Row::from_values(vec![
                Value::Integer(*id),
                Value::Text((*owner).into()),
                Value::Integer(*amount),
            ]),
        })
        .await
        .expect("seed row");
    }
}

pub async fn seed_kv(db: &AedbInstance, project_id: &str, scope_id: &str, rows: &[(&[u8], &[u8])]) {
    for (key, value) in rows {
        db.kv_set(project_id, scope_id, key.to_vec(), value.to_vec())
            .await
            .expect("seed kv");
    }
}
