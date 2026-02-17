use crate::catalog::Catalog;
use crate::commit::apply::apply_mutation;
use crate::commit::validation::Mutation;
use crate::config::PrimaryIndexBackend;
use crate::error::AedbError;
use crate::storage::keyspace::{Keyspace, Namespace, NamespaceId};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self as std_mpsc, Receiver, Sender};

#[derive(Clone)]
pub(super) struct ParallelApplyRuntime {
    workers: Arc<Vec<Sender<ParallelTask>>>,
    queued_tasks: Arc<AtomicUsize>,
}

pub(super) struct ParallelTask {
    pub(super) namespace_id: NamespaceId,
    pub(super) base_namespace: Namespace,
    pub(super) mutations: Vec<Mutation>,
    pub(super) commit_seq: u64,
    pub(super) backend: PrimaryIndexBackend,
    pub(super) catalog: Catalog,
    pub(super) cancel: Arc<AtomicBool>,
    pub(super) response_tx: Sender<Result<(NamespaceId, Namespace), AedbError>>,
}

impl ParallelApplyRuntime {
    pub(super) fn new(worker_threads: usize) -> Self {
        let worker_count = worker_threads.max(1);
        let mut workers = Vec::with_capacity(worker_count);
        let queued_tasks = Arc::new(AtomicUsize::new(0));
        for _ in 0..worker_count {
            let (tx, rx) = std_mpsc::channel::<ParallelTask>();
            let q = Arc::clone(&queued_tasks);
            std::thread::spawn(move || run_worker(rx, q));
            workers.push(tx);
        }
        Self {
            workers: Arc::new(workers),
            queued_tasks,
        }
    }

    pub(super) fn submit(&self, task: ParallelTask) -> Result<(), AedbError> {
        let shard = shard_for_namespace(&task.namespace_id, self.workers.len());
        self.queued_tasks.fetch_add(1, Ordering::Relaxed);
        if let Err(e) = self.workers[shard].send(task) {
            self.queued_tasks.fetch_sub(1, Ordering::Relaxed);
            return Err(AedbError::Validation(format!(
                "parallel runtime unavailable: {e}"
            )));
        }
        Ok(())
    }

    pub(super) fn queued_tasks(&self) -> usize {
        self.queued_tasks.load(Ordering::Relaxed)
    }
}

fn run_worker(rx: Receiver<ParallelTask>, queued_tasks: Arc<AtomicUsize>) {
    while let Ok(task) = rx.recv() {
        queued_tasks.fetch_sub(1, Ordering::Relaxed);
        let panic_response = task.response_tx.clone();
        let response =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| execute_task(task)));
        if response.is_err() {
            let _ = panic_response.send(Err(AedbError::ParallelApplyWorkerPanicked));
        }
    }
}

fn execute_task(task: ParallelTask) -> Result<(), AedbError> {
    if task.cancel.load(Ordering::Relaxed) {
        let _ = task
            .response_tx
            .send(Err(AedbError::ParallelApplyCancelled));
        return Ok(());
    }
    let mut local_catalog = task.catalog;
    let mut local_keyspace = Keyspace::with_backend(task.backend);
    local_keyspace.insert_namespace(task.namespace_id.clone(), task.base_namespace);
    for mutation in &task.mutations {
        if task.cancel.load(Ordering::Relaxed) {
            let _ = task
                .response_tx
                .send(Err(AedbError::ParallelApplyCancelled));
            return Ok(());
        }
        super::parallel_worker_test_hook_for_mutation(mutation);
        if let Err(e) = apply_mutation(
            &mut local_catalog,
            &mut local_keyspace,
            mutation.clone(),
            task.commit_seq,
        ) {
            let _ = task.response_tx.send(Err(e));
            return Ok(());
        }
    }
    let namespace = local_keyspace
        .namespaces
        .get(&task.namespace_id)
        .cloned()
        .ok_or_else(|| AedbError::Validation("parallel apply namespace missing".into()))?;
    let _ = task.response_tx.send(Ok((task.namespace_id, namespace)));
    Ok(())
}

fn shard_for_namespace(namespace_id: &NamespaceId, shard_count: usize) -> usize {
    if shard_count <= 1 {
        return 0;
    }
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    namespace_id.hash(&mut h);
    (h.finish() as usize) % shard_count
}
