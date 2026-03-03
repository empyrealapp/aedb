use crate::catalog::types::ColumnType;
use crate::catalog::types::Value;
use crate::query::plan::Expr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSchema {
    pub project_id: String,
    pub scope_id: String,
    pub table_name: String,
    #[serde(default)]
    pub owner_id: Option<String>,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<String>,
    #[serde(default)]
    pub constraints: Vec<Constraint>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Constraint {
    Unique { name: String, columns: Vec<String> },
    Check { name: String, expr: Expr },
    NotNull { column: String },
    Default { column: String, value: Value },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub references_project_id: String,
    pub references_scope_id: String,
    pub references_table: String,
    pub references_columns: Vec<String>,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ForeignKeyAction {
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
    NoAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexType {
    BTree,
    Art,
    Hash,
    UniqueHash,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexDef {
    pub project_id: String,
    pub scope_id: String,
    pub table_name: String,
    pub index_name: String,
    pub columns: Vec<String>,
    pub index_type: IndexType,
    pub columns_bitmask: u128,
    pub partial_filter: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AsyncIndexDef {
    pub project_id: String,
    pub scope_id: String,
    pub table_name: String,
    pub index_name: String,
    pub projected_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvProjectionDef {
    pub project_id: String,
    pub scope_id: String,
    pub table_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AccumulatorValueType {
    BigInt,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccumulatorDef {
    pub project_id: String,
    pub scope_id: String,
    pub accumulator_name: String,
    pub value_type: AccumulatorValueType,
    #[serde(default)]
    pub dedupe_retain_commits: Option<u64>,
    #[serde(default = "default_accumulator_snapshot_every")]
    pub snapshot_every: u64,
    #[serde(default = "default_accumulator_exposure_margin_bps")]
    pub exposure_margin_bps: u32,
    #[serde(default)]
    pub exposure_ttl_commits: Option<u64>,
}

fn default_accumulator_snapshot_every() -> u64 {
    10_000
}

fn default_accumulator_exposure_margin_bps() -> u32 {
    1_000
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TableAlteration {
    AddColumn(ColumnDef),
    DropColumn { name: String },
    RenameColumn { from: String, to: String },
    AddConstraint(Constraint),
    DropConstraint { name: String },
    AddForeignKey(ForeignKey),
    DropForeignKey { name: String },
}
