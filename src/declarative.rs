use crate::catalog::schema::{ColumnDef, IndexType, TableAlteration};
use crate::catalog::{DdlOperation, ResourceType};
use crate::commit::validation::Mutation;
use crate::error::AedbError;
use crate::migration::Migration;
use crate::query::plan::Expr;
use crate::{AedbInstance, MigrationReport};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpec {
    pub index_name: String,
    pub columns: Vec<String>,
    pub index_type: IndexType,
    pub if_not_exists: bool,
    pub partial_filter: Option<Expr>,
}

impl IndexSpec {
    pub fn new(index_name: impl Into<String>, columns: &[&str], index_type: IndexType) -> Self {
        Self {
            index_name: index_name.into(),
            columns: columns.iter().map(|c| (*c).to_string()).collect(),
            index_type,
            if_not_exists: true,
            partial_filter: None,
        }
    }

    pub fn if_not_exists(mut self, if_not_exists: bool) -> Self {
        self.if_not_exists = if_not_exists;
        self
    }

    pub fn partial_filter(mut self, partial_filter: Expr) -> Self {
        self.partial_filter = Some(partial_filter);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsyncIndexSpec {
    pub index_name: String,
    pub projected_columns: Vec<String>,
    pub if_not_exists: bool,
}

impl AsyncIndexSpec {
    pub fn new(index_name: impl Into<String>, projected_columns: &[&str]) -> Self {
        Self {
            index_name: index_name.into(),
            projected_columns: projected_columns.iter().map(|c| (*c).to_string()).collect(),
            if_not_exists: true,
        }
    }

    pub fn if_not_exists(mut self, if_not_exists: bool) -> Self {
        self.if_not_exists = if_not_exists;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSpec {
    pub table_name: String,
    pub owner_id: Option<String>,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<IndexSpec>,
    pub async_indexes: Vec<AsyncIndexSpec>,
}

impl TableSpec {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            owner_id: None,
            if_not_exists: true,
            columns: Vec::new(),
            primary_key: Vec::new(),
            indexes: Vec::new(),
            async_indexes: Vec::new(),
        }
    }

    pub fn owner_id(mut self, owner_id: impl Into<String>) -> Self {
        self.owner_id = Some(owner_id.into());
        self
    }

    pub fn if_not_exists(mut self, if_not_exists: bool) -> Self {
        self.if_not_exists = if_not_exists;
        self
    }

    pub fn column(
        mut self,
        name: impl Into<String>,
        col_type: crate::catalog::types::ColumnType,
        nullable: bool,
    ) -> Self {
        self.columns.push(ColumnDef {
            name: name.into(),
            col_type,
            nullable,
        });
        self
    }

    pub fn primary_key(mut self, primary_key: &[&str]) -> Self {
        self.primary_key = primary_key.iter().map(|c| (*c).to_string()).collect();
        self
    }

    pub fn add_index(mut self, index: IndexSpec) -> Self {
        self.indexes.push(index);
        self
    }

    pub fn add_async_index(mut self, index: AsyncIndexSpec) -> Self {
        self.async_indexes.push(index);
        self
    }

    pub fn validate(&self) -> Result<(), AedbError> {
        if self.columns.is_empty() {
            return Err(AedbError::Validation(format!(
                "table '{}' must define at least one column",
                self.table_name
            )));
        }
        if self.primary_key.is_empty() {
            return Err(AedbError::Validation(format!(
                "table '{}' must define a primary key",
                self.table_name
            )));
        }

        let col_names = self
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<BTreeSet<_>>();
        for pk in &self.primary_key {
            if !col_names.contains(pk.as_str()) {
                return Err(AedbError::Validation(format!(
                    "table '{}' primary key column '{}' is not declared",
                    self.table_name, pk
                )));
            }
        }

        for index in &self.indexes {
            if index.columns.is_empty() {
                return Err(AedbError::Validation(format!(
                    "index '{}' on table '{}' must define at least one column",
                    index.index_name, self.table_name
                )));
            }
            for col in &index.columns {
                if !col_names.contains(col.as_str()) {
                    return Err(AedbError::Validation(format!(
                        "index '{}' on table '{}' references unknown column '{}'",
                        index.index_name, self.table_name, col
                    )));
                }
            }
        }

        for index in &self.async_indexes {
            if index.projected_columns.is_empty() {
                return Err(AedbError::Validation(format!(
                    "async index '{}' on table '{}' must project at least one column",
                    index.index_name, self.table_name
                )));
            }
            for col in &index.projected_columns {
                if !col_names.contains(col.as_str()) {
                    return Err(AedbError::Validation(format!(
                        "async index '{}' on table '{}' references unknown column '{}'",
                        index.index_name, self.table_name, col
                    )));
                }
            }
        }

        Ok(())
    }

    pub fn to_ddl(&self, project_id: &str, scope_id: &str) -> Result<Vec<DdlOperation>, AedbError> {
        self.validate()?;

        let mut ops = vec![DdlOperation::CreateTable {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: self.table_name.clone(),
            owner_id: self.owner_id.clone(),
            if_not_exists: self.if_not_exists,
            columns: self.columns.clone(),
            primary_key: self.primary_key.clone(),
        }];

        for index in &self.indexes {
            ops.push(DdlOperation::CreateIndex {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: self.table_name.clone(),
                index_name: index.index_name.clone(),
                if_not_exists: index.if_not_exists,
                columns: index.columns.clone(),
                index_type: index.index_type.clone(),
                partial_filter: index.partial_filter.clone(),
            });
        }

        for index in &self.async_indexes {
            ops.push(DdlOperation::CreateAsyncIndex {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: self.table_name.clone(),
                index_name: index.index_name.clone(),
                if_not_exists: index.if_not_exists,
                projected_columns: index.projected_columns.clone(),
            });
        }

        Ok(ops)
    }

    pub fn drop_ops(&self, project_id: &str, scope_id: &str, if_exists: bool) -> Vec<DdlOperation> {
        let mut ops = Vec::with_capacity(1 + self.indexes.len() + self.async_indexes.len());

        for index in &self.async_indexes {
            ops.push(DdlOperation::DropAsyncIndex {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: self.table_name.clone(),
                index_name: index.index_name.clone(),
                if_exists,
            });
        }

        for index in &self.indexes {
            ops.push(DdlOperation::DropIndex {
                project_id: project_id.to_string(),
                scope_id: scope_id.to_string(),
                table_name: self.table_name.clone(),
                index_name: index.index_name.clone(),
                if_exists,
            });
        }

        ops.push(DdlOperation::DropTable {
            project_id: project_id.to_string(),
            scope_id: scope_id.to_string(),
            table_name: self.table_name.clone(),
            if_exists,
        });

        ops
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationSpec {
    pub version: u64,
    pub name: String,
    pub up: Vec<DdlOperation>,
    pub down: Vec<DdlOperation>,
}

impl MigrationSpec {
    pub fn new(version: u64, name: impl Into<String>) -> Self {
        Self {
            version,
            name: name.into(),
            up: Vec::new(),
            down: Vec::new(),
        }
    }

    pub fn up_ddl(mut self, ddl: DdlOperation) -> Self {
        self.up.push(ddl);
        self
    }

    pub fn down_ddl(mut self, ddl: DdlOperation) -> Self {
        self.down.push(ddl);
        self
    }

    pub fn up_table(
        mut self,
        project_id: &str,
        scope_id: &str,
        table: &TableSpec,
    ) -> Result<Self, AedbError> {
        self.up.extend(table.to_ddl(project_id, scope_id)?);
        Ok(self)
    }

    pub fn down_drop_table(mut self, project_id: &str, scope_id: &str, table: &TableSpec) -> Self {
        self.down.extend(table.drop_ops(project_id, scope_id, true));
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReversibleDdl {
    up: DdlOperation,
    down: DdlOperation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMigrationBuilder {
    version: u64,
    name: String,
    project_id: String,
    scope_id: String,
    table_name: String,
    changes: Vec<ReversibleDdl>,
}

impl TableMigrationBuilder {
    pub fn new(
        version: u64,
        name: impl Into<String>,
        project_id: impl Into<String>,
        scope_id: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            version,
            name: name.into(),
            project_id: project_id.into(),
            scope_id: scope_id.into(),
            table_name: table_name.into(),
            changes: Vec::new(),
        }
    }

    pub fn add_column(mut self, column: ColumnDef) -> Self {
        let column_name = column.name.clone();
        self.changes.push(ReversibleDdl {
            up: DdlOperation::AlterTable {
                project_id: self.project_id.clone(),
                scope_id: self.scope_id.clone(),
                table_name: self.table_name.clone(),
                alteration: TableAlteration::AddColumn(column),
            },
            down: DdlOperation::AlterTable {
                project_id: self.project_id.clone(),
                scope_id: self.scope_id.clone(),
                table_name: self.table_name.clone(),
                alteration: TableAlteration::DropColumn { name: column_name },
            },
        });
        self
    }

    pub fn drop_column(mut self, name: impl Into<String>, restore_as: ColumnDef) -> Self {
        let name = name.into();
        self.changes.push(ReversibleDdl {
            up: DdlOperation::AlterTable {
                project_id: self.project_id.clone(),
                scope_id: self.scope_id.clone(),
                table_name: self.table_name.clone(),
                alteration: TableAlteration::DropColumn { name: name.clone() },
            },
            down: DdlOperation::AlterTable {
                project_id: self.project_id.clone(),
                scope_id: self.scope_id.clone(),
                table_name: self.table_name.clone(),
                alteration: TableAlteration::AddColumn(restore_as),
            },
        });
        self
    }

    pub fn rename_column(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        let from = from.into();
        let to = to.into();
        self.changes.push(ReversibleDdl {
            up: DdlOperation::AlterTable {
                project_id: self.project_id.clone(),
                scope_id: self.scope_id.clone(),
                table_name: self.table_name.clone(),
                alteration: TableAlteration::RenameColumn {
                    from: from.clone(),
                    to: to.clone(),
                },
            },
            down: DdlOperation::AlterTable {
                project_id: self.project_id.clone(),
                scope_id: self.scope_id.clone(),
                table_name: self.table_name.clone(),
                alteration: TableAlteration::RenameColumn { from: to, to: from },
            },
        });
        self
    }

    pub fn add_index(mut self, index: IndexSpec) -> Self {
        self.changes.push(ReversibleDdl {
            up: DdlOperation::CreateIndex {
                project_id: self.project_id.clone(),
                scope_id: self.scope_id.clone(),
                table_name: self.table_name.clone(),
                index_name: index.index_name.clone(),
                if_not_exists: index.if_not_exists,
                columns: index.columns.clone(),
                index_type: index.index_type.clone(),
                partial_filter: index.partial_filter.clone(),
            },
            down: DdlOperation::DropIndex {
                project_id: self.project_id.clone(),
                scope_id: self.scope_id.clone(),
                table_name: self.table_name.clone(),
                index_name: index.index_name,
                if_exists: true,
            },
        });
        self
    }

    pub fn drop_index(mut self, index_name: impl Into<String>, restore_as: IndexSpec) -> Self {
        self.changes.push(ReversibleDdl {
            up: DdlOperation::DropIndex {
                project_id: self.project_id.clone(),
                scope_id: self.scope_id.clone(),
                table_name: self.table_name.clone(),
                index_name: index_name.into(),
                if_exists: false,
            },
            down: DdlOperation::CreateIndex {
                project_id: self.project_id.clone(),
                scope_id: self.scope_id.clone(),
                table_name: self.table_name.clone(),
                index_name: restore_as.index_name,
                if_not_exists: true,
                columns: restore_as.columns,
                index_type: restore_as.index_type,
                partial_filter: restore_as.partial_filter,
            },
        });
        self
    }

    pub fn add_async_index(mut self, index: AsyncIndexSpec) -> Self {
        self.changes.push(ReversibleDdl {
            up: DdlOperation::CreateAsyncIndex {
                project_id: self.project_id.clone(),
                scope_id: self.scope_id.clone(),
                table_name: self.table_name.clone(),
                index_name: index.index_name.clone(),
                if_not_exists: index.if_not_exists,
                projected_columns: index.projected_columns.clone(),
            },
            down: DdlOperation::DropAsyncIndex {
                project_id: self.project_id.clone(),
                scope_id: self.scope_id.clone(),
                table_name: self.table_name.clone(),
                index_name: index.index_name,
                if_exists: true,
            },
        });
        self
    }

    pub fn drop_async_index(
        mut self,
        index_name: impl Into<String>,
        restore_as: AsyncIndexSpec,
    ) -> Self {
        self.changes.push(ReversibleDdl {
            up: DdlOperation::DropAsyncIndex {
                project_id: self.project_id.clone(),
                scope_id: self.scope_id.clone(),
                table_name: self.table_name.clone(),
                index_name: index_name.into(),
                if_exists: false,
            },
            down: DdlOperation::CreateAsyncIndex {
                project_id: self.project_id.clone(),
                scope_id: self.scope_id.clone(),
                table_name: self.table_name.clone(),
                index_name: restore_as.index_name,
                if_not_exists: true,
                projected_columns: restore_as.projected_columns,
            },
        });
        self
    }

    pub fn build(self) -> Result<MigrationSpec, AedbError> {
        if self.changes.is_empty() {
            return Err(AedbError::Validation(
                "table migration requires at least one change".into(),
            ));
        }

        let mut spec = MigrationSpec::new(self.version, self.name);
        for change in &self.changes {
            spec.up.push(change.up.clone());
        }
        for change in self.changes.iter().rev() {
            spec.down.push(change.down.clone());
        }
        Ok(spec)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaMigrationPlan {
    project_id: String,
    scope_id: String,
    include_namespace_bootstrap: bool,
    specs: Vec<MigrationSpec>,
}

impl SchemaMigrationPlan {
    pub fn new(project_id: impl Into<String>, scope_id: impl Into<String>) -> Self {
        Self {
            project_id: project_id.into(),
            scope_id: scope_id.into(),
            include_namespace_bootstrap: true,
            specs: Vec::new(),
        }
    }

    pub fn with_namespace_bootstrap(mut self, include_namespace_bootstrap: bool) -> Self {
        self.include_namespace_bootstrap = include_namespace_bootstrap;
        self
    }

    pub fn with_spec(mut self, spec: MigrationSpec) -> Self {
        self.specs.push(spec);
        self
    }

    #[allow(clippy::should_implement_trait)]
    pub fn add(self, spec: MigrationSpec) -> Self {
        self.with_spec(spec)
    }

    pub fn migrations(&self) -> Result<Vec<Migration>, AedbError> {
        let mut specs = self.specs.clone();
        specs.sort_by_key(|s| s.version);

        let mut seen = BTreeSet::new();
        let mut out = Vec::with_capacity(specs.len());

        for (idx, spec) in specs.into_iter().enumerate() {
            if spec.up.is_empty() {
                return Err(AedbError::Validation(format!(
                    "migration {} ({}) has no up operations",
                    spec.version, spec.name
                )));
            }
            if !seen.insert(spec.version) {
                return Err(AedbError::Validation(format!(
                    "duplicate migration version {}",
                    spec.version
                )));
            }

            let mut up = spec.up;
            if idx == 0 && self.include_namespace_bootstrap {
                up.insert(
                    0,
                    DdlOperation::CreateScope {
                        project_id: self.project_id.clone(),
                        scope_id: self.scope_id.clone(),
                        owner_id: None,
                        if_not_exists: true,
                    },
                );
                up.insert(
                    0,
                    DdlOperation::CreateProject {
                        project_id: self.project_id.clone(),
                        owner_id: None,
                        if_not_exists: true,
                    },
                );
            }

            out.push(Migration {
                version: spec.version,
                name: spec.name,
                project_id: self.project_id.clone(),
                scope_id: self.scope_id.clone(),
                mutations: up.into_iter().map(Mutation::Ddl).collect(),
                down_mutations: if spec.down.is_empty() {
                    None
                } else {
                    Some(spec.down.into_iter().map(Mutation::Ddl).collect())
                },
            });
        }

        Ok(out)
    }

    pub async fn run(&self, db: &AedbInstance) -> Result<MigrationReport, AedbError> {
        db.run_migrations(self.migrations()?).await
    }
}

pub fn transfer_table_ownership(
    project_id: impl Into<String>,
    scope_id: impl Into<String>,
    table_name: impl Into<String>,
    new_owner_id: impl Into<String>,
) -> DdlOperation {
    DdlOperation::TransferOwnership {
        resource_type: ResourceType::Table,
        project_id: project_id.into(),
        scope_id: Some(scope_id.into()),
        table_name: Some(table_name.into()),
        new_owner_id: new_owner_id.into(),
        actor_id: None,
    }
}
