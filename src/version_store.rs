use crate::catalog::Catalog;
use crate::commit::apply::apply_mutation;
use crate::commit::validation::Mutation;
use crate::error::AedbError;
use crate::snapshot::reader::SnapshotReadView;
use crate::storage::keyspace::{Keyspace, KeyspaceSnapshot};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Debug, Clone)]
struct Version {
    seq: u64,
    keyspace: Option<Arc<KeyspaceSnapshot>>,
    catalog: Option<Arc<Catalog>>,
    delta: Option<Arc<CommitDelta>>,
    created_at: Instant,
    ref_count: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
pub struct VersionStore {
    versions: VecDeque<Version>,
    max_versions: usize,
    min_version_age_ms: u64,
}

#[derive(Debug, Clone)]
pub struct CommitDelta {
    pub seq: u64,
    pub mutations: Vec<Mutation>,
}

#[derive(Debug)]
pub struct ReadViewGuard {
    view: SnapshotReadView,
    ref_count: Arc<AtomicU64>,
}

impl Drop for ReadViewGuard {
    fn drop(&mut self) {
        self.ref_count.fetch_sub(1, Ordering::AcqRel);
    }
}

impl ReadViewGuard {
    pub fn view(&self) -> &SnapshotReadView {
        &self.view
    }

    pub fn into_view(&self) -> SnapshotReadView {
        self.view.clone()
    }
}

impl VersionStore {
    pub fn new(max_versions: usize, min_version_age_ms: u64) -> Self {
        Self {
            versions: VecDeque::new(),
            max_versions,
            min_version_age_ms,
        }
    }

    pub fn bootstrap(&mut self, seq: u64, keyspace: KeyspaceSnapshot, catalog: Catalog) {
        self.versions.clear();
        self.versions.push_back(Version {
            seq,
            keyspace: Some(Arc::new(keyspace)),
            catalog: Some(Arc::new(catalog)),
            delta: None,
            created_at: Instant::now(),
            ref_count: Arc::new(AtomicU64::new(0)),
        });
    }

    pub fn publish_delta(&mut self, seq: u64, delta: CommitDelta) {
        self.versions.push_back(Version {
            seq,
            keyspace: None,
            catalog: None,
            delta: Some(Arc::new(delta)),
            created_at: Instant::now(),
            ref_count: Arc::new(AtomicU64::new(0)),
        });
        self.prune();
    }

    pub fn publish_full(&mut self, seq: u64, keyspace: KeyspaceSnapshot, catalog: Catalog) {
        if let Some(existing) = self.versions.iter_mut().find(|v| v.seq == seq) {
            existing.keyspace = Some(Arc::new(keyspace));
            existing.catalog = Some(Arc::new(catalog));
            return;
        }
        self.versions.push_back(Version {
            seq,
            keyspace: Some(Arc::new(keyspace)),
            catalog: Some(Arc::new(catalog)),
            delta: None,
            created_at: Instant::now(),
            ref_count: Arc::new(AtomicU64::new(0)),
        });
        self.prune();
    }

    pub fn gc(&mut self) {
        self.prune();
    }

    pub fn acquire_latest(&mut self) -> Result<ReadViewGuard, AedbError> {
        let Some(last_idx) = self.versions.len().checked_sub(1) else {
            return Err(AedbError::Validation("version store is empty".into()));
        };
        self.materialize_at_index(last_idx)?;
        let version = self
            .versions
            .get(last_idx)
            .ok_or_else(|| AedbError::Validation("version store is empty".into()))?;
        Ok(acquire(version))
    }

    pub fn acquire_at_seq(&mut self, seq: u64) -> Result<ReadViewGuard, AedbError> {
        let Some(target_idx) = self.versions.iter().position(|v| v.seq == seq) else {
            let oldest = self.versions.front().map(|v| v.seq).unwrap_or(0);
            let newest = self.versions.back().map(|v| v.seq).unwrap_or(0);
            if seq < oldest {
                return Err(AedbError::Validation(format!(
                    "requested seq {seq} has been garbage collected (oldest retained seq: {oldest})"
                )));
            }
            if seq > newest {
                return Err(AedbError::Validation(format!(
                    "requested seq {seq} is not yet visible (latest visible seq: {newest})"
                )));
            }
            return Err(AedbError::Validation(format!(
                "requested seq {seq} not found in version store"
            )));
        };
        self.materialize_at_index(target_idx)?;
        let version = self
            .versions
            .get(target_idx)
            .ok_or_else(|| AedbError::Validation("requested seq vanished".into()))?;
        Ok(acquire(version))
    }

    pub fn oldest_seq(&self) -> u64 {
        self.versions.front().map(|v| v.seq).unwrap_or(0)
    }

    pub fn deltas_since(
        &self,
        from_seq_exclusive: u64,
        to_seq_inclusive: u64,
    ) -> Option<Vec<Arc<CommitDelta>>> {
        if from_seq_exclusive >= to_seq_inclusive {
            return Some(Vec::new());
        }
        let oldest = self.versions.front().map(|v| v.seq).unwrap_or(0);
        if from_seq_exclusive < oldest {
            return None;
        }
        let mut out = Vec::new();
        for version in &self.versions {
            if version.seq <= from_seq_exclusive || version.seq > to_seq_inclusive {
                continue;
            }
            if let Some(delta) = &version.delta {
                out.push(Arc::clone(delta));
            }
        }
        Some(out)
    }

    fn prune(&mut self) {
        while self.versions.len() > self.max_versions {
            let has_other_full = self.versions.iter().skip(1).any(|v| v.keyspace.is_some());
            let can_remove = self.versions.front().map(|v| {
                let preserve_oldest_full = v.keyspace.is_some() && !has_other_full;
                !preserve_oldest_full
                    && v.ref_count.load(Ordering::Acquire) == 0
                    && v.created_at.elapsed().as_millis() >= u128::from(self.min_version_age_ms)
            });
            let can_remove = can_remove.unwrap_or(false);
            if !can_remove {
                break;
            }
            self.versions.pop_front();
        }
    }

    fn materialize_at_index(&mut self, target_idx: usize) -> Result<(), AedbError> {
        let already_materialized = self
            .versions
            .get(target_idx)
            .map(|v| v.keyspace.is_some() && v.catalog.is_some())
            .unwrap_or(false);
        if already_materialized {
            return Ok(());
        }
        let base_idx = (0..=target_idx)
            .rev()
            .find(|idx| {
                self.versions
                    .get(*idx)
                    .map(|v| v.keyspace.is_some() && v.catalog.is_some())
                    .unwrap_or(false)
            })
            .ok_or_else(|| AedbError::Validation("no materialized base version found".into()))?;
        let base = self
            .versions
            .get(base_idx)
            .ok_or_else(|| AedbError::Validation("base version missing".into()))?;
        let base_keyspace = base
            .keyspace
            .as_ref()
            .ok_or_else(|| AedbError::Validation("base keyspace missing".into()))?;
        let base_catalog = base
            .catalog
            .as_ref()
            .ok_or_else(|| AedbError::Validation("base catalog missing".into()))?;

        let mut keyspace = snapshot_to_keyspace(base_keyspace);
        let mut catalog = (**base_catalog).clone();
        for idx in (base_idx + 1)..=target_idx {
            let version = self
                .versions
                .get(idx)
                .ok_or_else(|| AedbError::Validation("delta version missing".into()))?;
            if let Some(delta) = &version.delta {
                for mutation in &delta.mutations {
                    apply_mutation(&mut catalog, &mut keyspace, mutation.clone(), delta.seq)?;
                }
            } else if let (Some(ks), Some(cat)) = (&version.keyspace, &version.catalog) {
                keyspace = snapshot_to_keyspace(ks);
                catalog = (**cat).clone();
            }
        }
        let target = self
            .versions
            .get_mut(target_idx)
            .ok_or_else(|| AedbError::Validation("target version missing".into()))?;
        target.keyspace = Some(Arc::new(keyspace.snapshot()));
        target.catalog = Some(Arc::new(catalog));
        Ok(())
    }
}

fn acquire(version: &Version) -> ReadViewGuard {
    let keyspace = version
        .keyspace
        .as_ref()
        .expect("materialized keyspace required");
    let catalog = version
        .catalog
        .as_ref()
        .expect("materialized catalog required");
    version.ref_count.fetch_add(1, Ordering::AcqRel);
    ReadViewGuard {
        view: SnapshotReadView {
            keyspace: Arc::clone(keyspace),
            catalog: Arc::clone(catalog),
            seq: version.seq,
        },
        ref_count: Arc::clone(&version.ref_count),
    }
}

fn snapshot_to_keyspace(snapshot: &KeyspaceSnapshot) -> Keyspace {
    Keyspace {
        primary_index_backend: snapshot.primary_index_backend,
        namespaces: snapshot.namespaces.clone(),
        async_indexes: snapshot.async_indexes.clone(),
    }
}
