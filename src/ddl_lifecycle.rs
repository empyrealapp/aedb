use crate::LifecycleEvent;
use crate::catalog::{DdlOperation, namespace_key};
use crate::commit::validation::Mutation;
use crate::error::AedbError;
use std::collections::HashMap;

pub(crate) fn ddl_resource_key(op: &DdlOperation) -> Option<String> {
    match op {
        DdlOperation::CreateProject { project_id, .. } => Some(format!("project:{project_id}")),
        DdlOperation::DropProject { project_id, .. } => Some(format!("project:{project_id}")),
        DdlOperation::CreateScope {
            project_id,
            scope_id,
            ..
        } => Some(format!("scope:{project_id}:{scope_id}")),
        DdlOperation::DropScope {
            project_id,
            scope_id,
            ..
        } => Some(format!("scope:{project_id}:{scope_id}")),
        DdlOperation::CreateTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => Some(format!("table:{project_id}:{scope_id}:{table_name}")),
        DdlOperation::AlterTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => Some(format!("table:{project_id}:{scope_id}:{table_name}")),
        DdlOperation::DropTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => Some(format!("table:{project_id}:{scope_id}:{table_name}")),
        DdlOperation::CreateIndex {
            project_id,
            scope_id,
            table_name,
            index_name,
            ..
        } => Some(format!(
            "index:{project_id}:{scope_id}:{table_name}:{index_name}"
        )),
        DdlOperation::DropIndex {
            project_id,
            scope_id,
            table_name,
            index_name,
            ..
        } => Some(format!(
            "index:{project_id}:{scope_id}:{table_name}:{index_name}"
        )),
        DdlOperation::CreateAsyncIndex {
            project_id,
            scope_id,
            table_name,
            index_name,
            ..
        } => Some(format!(
            "aindex:{project_id}:{scope_id}:{table_name}:{index_name}"
        )),
        DdlOperation::DropAsyncIndex {
            project_id,
            scope_id,
            table_name,
            index_name,
            ..
        } => Some(format!(
            "aindex:{project_id}:{scope_id}:{table_name}:{index_name}"
        )),
        _ => None,
    }
}

pub(crate) fn ddl_dependencies(op: &DdlOperation) -> Vec<String> {
    match op {
        DdlOperation::CreateScope { project_id, .. } => vec![format!("project:{project_id}")],
        DdlOperation::CreateTable {
            project_id,
            scope_id,
            ..
        } => vec![
            format!("project:{project_id}"),
            format!("scope:{project_id}:{scope_id}"),
        ],
        DdlOperation::AlterTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => vec![format!("table:{project_id}:{scope_id}:{table_name}")],
        DdlOperation::CreateIndex {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | DdlOperation::CreateAsyncIndex {
            project_id,
            scope_id,
            table_name,
            ..
        } => vec![format!("table:{project_id}:{scope_id}:{table_name}")],
        DdlOperation::DropProject { project_id, .. } => vec![
            format!("scope_drop_gate:{project_id}"),
            format!("table_drop_gate:{project_id}"),
            format!("index_drop_gate:{project_id}"),
        ],
        DdlOperation::DropScope {
            project_id,
            scope_id,
            ..
        } => vec![
            format!("table_drop_gate:{project_id}:{scope_id}"),
            format!("index_drop_gate:{project_id}:{scope_id}"),
        ],
        DdlOperation::DropTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => vec![format!(
            "index_drop_gate:{project_id}:{scope_id}:{table_name}"
        )],
        _ => Vec::new(),
    }
}

pub(crate) fn ddl_gates_produced(op: &DdlOperation) -> Vec<String> {
    match op {
        DdlOperation::DropScope { project_id, .. } => vec![format!("scope_drop_gate:{project_id}")],
        DdlOperation::DropTable {
            project_id,
            scope_id,
            ..
        } => vec![
            format!("table_drop_gate:{project_id}"),
            format!("table_drop_gate:{project_id}:{scope_id}"),
        ],
        DdlOperation::DropIndex {
            project_id,
            scope_id,
            table_name,
            ..
        }
        | DdlOperation::DropAsyncIndex {
            project_id,
            scope_id,
            table_name,
            ..
        } => vec![
            format!("index_drop_gate:{project_id}"),
            format!("index_drop_gate:{project_id}:{scope_id}"),
            format!("index_drop_gate:{project_id}:{scope_id}:{table_name}"),
        ],
        _ => Vec::new(),
    }
}

pub(crate) fn order_ddl_ops_for_batch(
    ops: Vec<DdlOperation>,
) -> Result<Vec<DdlOperation>, AedbError> {
    if ops.len() <= 1 {
        return Ok(ops);
    }
    let mut providers: HashMap<String, Vec<usize>> = HashMap::new();
    for (idx, op) in ops.iter().enumerate() {
        if let Some(key) = ddl_resource_key(op) {
            providers.entry(key).or_default().push(idx);
        }
        for gate in ddl_gates_produced(op) {
            providers.entry(gate).or_default().push(idx);
        }
    }

    let mut outgoing: Vec<Vec<usize>> = vec![Vec::new(); ops.len()];
    let mut indegree = vec![0usize; ops.len()];
    for (idx, op) in ops.iter().enumerate() {
        for dep in ddl_dependencies(op) {
            if let Some(dep_indices) = providers.get(&dep) {
                for &dep_idx in dep_indices {
                    if dep_idx == idx {
                        continue;
                    }
                    outgoing[dep_idx].push(idx);
                    indegree[idx] += 1;
                }
            }
        }
    }

    let mut ready = std::collections::BTreeSet::new();
    for (idx, degree) in indegree.iter().enumerate() {
        if *degree == 0 {
            ready.insert(idx);
        }
    }

    let mut order = Vec::with_capacity(ops.len());
    while let Some(idx) = ready.pop_first() {
        order.push(idx);
        for &next in &outgoing[idx] {
            indegree[next] -= 1;
            if indegree[next] == 0 {
                ready.insert(next);
            }
        }
    }

    if order.len() != ops.len() {
        return Err(AedbError::InvalidConfig {
            message: "ddl batch has cyclic dependencies".into(),
        });
    }

    Ok(order.into_iter().map(|idx| ops[idx].clone()).collect())
}

pub(crate) fn ddl_would_apply(catalog: &crate::catalog::Catalog, op: &DdlOperation) -> bool {
    match op {
        DdlOperation::CreateProject { project_id, .. } => {
            !catalog.projects.contains_key(project_id)
        }
        DdlOperation::CreateScope {
            project_id,
            scope_id,
            ..
        } => !catalog
            .scopes
            .contains_key(&(project_id.clone(), scope_id.clone())),
        DdlOperation::CreateTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => !catalog
            .tables
            .contains_key(&(namespace_key(project_id, scope_id), table_name.clone())),
        DdlOperation::CreateIndex {
            project_id,
            scope_id,
            table_name,
            index_name,
            ..
        } => !catalog.indexes.contains_key(&(
            namespace_key(project_id, scope_id),
            table_name.clone(),
            index_name.clone(),
        )),
        DdlOperation::CreateAsyncIndex {
            project_id,
            scope_id,
            table_name,
            index_name,
            ..
        } => !catalog.async_indexes.contains_key(&(
            namespace_key(project_id, scope_id),
            table_name.clone(),
            index_name.clone(),
        )),
        DdlOperation::DropProject { project_id, .. } => catalog.projects.contains_key(project_id),
        DdlOperation::DropScope {
            project_id,
            scope_id,
            ..
        } => catalog
            .scopes
            .contains_key(&(project_id.clone(), scope_id.clone())),
        DdlOperation::DropTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => catalog
            .tables
            .contains_key(&(namespace_key(project_id, scope_id), table_name.clone())),
        DdlOperation::DropIndex {
            project_id,
            scope_id,
            table_name,
            index_name,
            ..
        } => catalog.indexes.contains_key(&(
            namespace_key(project_id, scope_id),
            table_name.clone(),
            index_name.clone(),
        )),
        DdlOperation::DropAsyncIndex {
            project_id,
            scope_id,
            table_name,
            index_name,
            ..
        } => catalog.async_indexes.contains_key(&(
            namespace_key(project_id, scope_id),
            table_name.clone(),
            index_name.clone(),
        )),
        _ => true,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LifecycleEventTemplate {
    ProjectCreated {
        project_id: String,
    },
    ProjectDropped {
        project_id: String,
    },
    ScopeCreated {
        project_id: String,
        scope_id: String,
    },
    ScopeDropped {
        project_id: String,
        scope_id: String,
    },
    TableCreated {
        project_id: String,
        scope_id: String,
        table_name: String,
    },
    TableDropped {
        project_id: String,
        scope_id: String,
        table_name: String,
    },
    TableAltered {
        project_id: String,
        scope_id: String,
        table_name: String,
    },
    AppEventEmitted {
        project_id: String,
        scope_id: String,
        topic: String,
        event_key: String,
        payload_json: String,
    },
}

impl LifecycleEventTemplate {
    pub(crate) fn with_seq(self, seq: u64) -> LifecycleEvent {
        match self {
            LifecycleEventTemplate::ProjectCreated { project_id } => {
                LifecycleEvent::ProjectCreated { project_id, seq }
            }
            LifecycleEventTemplate::ProjectDropped { project_id } => {
                LifecycleEvent::ProjectDropped { project_id, seq }
            }
            LifecycleEventTemplate::ScopeCreated {
                project_id,
                scope_id,
            } => LifecycleEvent::ScopeCreated {
                project_id,
                scope_id,
                seq,
            },
            LifecycleEventTemplate::ScopeDropped {
                project_id,
                scope_id,
            } => LifecycleEvent::ScopeDropped {
                project_id,
                scope_id,
                seq,
            },
            LifecycleEventTemplate::TableCreated {
                project_id,
                scope_id,
                table_name,
            } => LifecycleEvent::TableCreated {
                project_id,
                scope_id,
                table_name,
                seq,
            },
            LifecycleEventTemplate::TableDropped {
                project_id,
                scope_id,
                table_name,
            } => LifecycleEvent::TableDropped {
                project_id,
                scope_id,
                table_name,
                seq,
            },
            LifecycleEventTemplate::TableAltered {
                project_id,
                scope_id,
                table_name,
            } => LifecycleEvent::TableAltered {
                project_id,
                scope_id,
                table_name,
                seq,
            },
            LifecycleEventTemplate::AppEventEmitted {
                project_id,
                scope_id,
                topic,
                event_key,
                payload_json,
            } => LifecycleEvent::AppEventEmitted {
                project_id,
                scope_id,
                topic,
                event_key,
                payload_json,
                seq,
            },
        }
    }
}

pub(crate) fn lifecycle_template_for_ddl(op: &DdlOperation) -> Option<LifecycleEventTemplate> {
    match op {
        DdlOperation::CreateProject { project_id, .. } => {
            Some(LifecycleEventTemplate::ProjectCreated {
                project_id: project_id.clone(),
            })
        }
        DdlOperation::DropProject { project_id, .. } => {
            Some(LifecycleEventTemplate::ProjectDropped {
                project_id: project_id.clone(),
            })
        }
        DdlOperation::CreateScope {
            project_id,
            scope_id,
            ..
        } => Some(LifecycleEventTemplate::ScopeCreated {
            project_id: project_id.clone(),
            scope_id: scope_id.clone(),
        }),
        DdlOperation::DropScope {
            project_id,
            scope_id,
            ..
        } => Some(LifecycleEventTemplate::ScopeDropped {
            project_id: project_id.clone(),
            scope_id: scope_id.clone(),
        }),
        DdlOperation::CreateTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => Some(LifecycleEventTemplate::TableCreated {
            project_id: project_id.clone(),
            scope_id: scope_id.clone(),
            table_name: table_name.clone(),
        }),
        DdlOperation::DropTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => Some(LifecycleEventTemplate::TableDropped {
            project_id: project_id.clone(),
            scope_id: scope_id.clone(),
            table_name: table_name.clone(),
        }),
        DdlOperation::AlterTable {
            project_id,
            scope_id,
            table_name,
            ..
        } => Some(LifecycleEventTemplate::TableAltered {
            project_id: project_id.clone(),
            scope_id: scope_id.clone(),
            table_name: table_name.clone(),
        }),
        _ => None,
    }
}

pub(crate) fn lifecycle_templates_for_mutation(mutation: &Mutation) -> Vec<LifecycleEventTemplate> {
    match mutation {
        Mutation::EmitEvent {
            project_id,
            scope_id,
            topic,
            event_key,
            payload_json,
        } => vec![LifecycleEventTemplate::AppEventEmitted {
            project_id: project_id.clone(),
            scope_id: scope_id.clone(),
            topic: topic.clone(),
            event_key: event_key.clone(),
            payload_json: payload_json.clone(),
        }],
        _ => Vec::new(),
    }
}
