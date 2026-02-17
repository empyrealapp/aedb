use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectMeta {
    pub project_id: String,
    pub created_at_micros: u64,
    #[serde(default)]
    pub owner_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScopeMeta {
    pub project_id: String,
    pub scope_id: String,
    pub created_at_micros: u64,
    #[serde(default)]
    pub owner_id: Option<String>,
}
