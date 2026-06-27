use super::{
    AedbConfig, AedbError, AedbInstance, CallerContext, ConsistencyMode, DdlOperation, Expr,
    LifecycleEvent, LifecycleHook, Mutation, Permission, Query, QueryOptions,
    ReactiveProcessorOptions, RecordingLifecycleHook, Value,
};
use crate::catalog::SYSTEM_PROJECT_ID;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[tokio::test]
async fn event_stream_and_processor_lag_as_require_explicit_permissions() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_production(AedbConfig::production([3u8; 32]), dir.path())
        .expect("open secure");

    let caller = CallerContext::new("reader_1");
    let stream_err = db
        .read_event_stream_as(&caller, None, 0, 10, ConsistencyMode::AtLatest)
        .await
        .expect_err("event stream read without permission must fail");
    assert!(matches!(stream_err, AedbError::PermissionDenied(_)));

    let lag_err = db
        .reactive_processor_lag_as(&caller, "points_processor", ConsistencyMode::AtLatest)
        .await
        .expect_err("reactive processor lag without permission must fail");
    assert!(matches!(lag_err, AedbError::PermissionDenied(_)));
}

#[tokio::test]
async fn lifecycle_hooks_receive_post_commit_events_for_applied_ddl() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
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
async fn removing_last_lifecycle_hook_disables_dispatch() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hook: Arc<dyn LifecycleHook> = Arc::new(RecordingLifecycleHook {
        events: Arc::clone(&events),
    });
    db.add_lifecycle_hook(Arc::clone(&hook));
    db.remove_lifecycle_hook(&hook);

    db.commit_ddl(DdlOperation::CreateProject {
        owner_id: None,
        project_id: "arcana".into(),
        if_not_exists: true,
    })
    .await
    .expect("create project");
    tokio::time::sleep(Duration::from_millis(25)).await;

    assert!(events.lock().expect("events lock").is_empty());
}

#[tokio::test]
async fn lifecycle_outbox_persists_applied_events() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
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
async fn lifecycle_outbox_includes_app_emit_events() {
    let dir = tempdir().expect("temp");
    let db = AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open");
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    let committed = db
        .emit_event(
            "arcana",
            "app",
            "hand_settled",
            "hand_1".into(),
            r#"{"user_id":"u1","wager":100,"pnl":-25}"#.into(),
        )
        .await
        .expect("emit");

    let outbox = db
        .query_no_auth(
            "_system",
            "app",
            Query::select(&["events"])
                .from("lifecycle_outbox")
                .where_(Expr::Eq(
                    "commit_seq".into(),
                    Value::Integer(committed.commit_seq as i64),
                ))
                .limit(1),
            QueryOptions::default(),
        )
        .await
        .expect("query lifecycle outbox");
    assert_eq!(outbox.rows.len(), 1, "expected lifecycle outbox row");
    let Value::Json(payload) = &outbox.rows[0].values[0] else {
        panic!("expected json payload");
    };
    let events: Vec<LifecycleEvent> =
        serde_json::from_str(payload.as_str()).expect("decode lifecycle payload");
    assert!(events.iter().any(|evt| matches!(
        evt,
        LifecycleEvent::AppEventEmitted {
            project_id,
            scope_id,
            topic,
            event_key,
            payload_json,
            ..
        } if project_id == "arcana"
            && scope_id == "app"
            && topic == "hand_settled"
            && event_key == "hand_1"
            && payload_json.contains("\"user_id\":\"u1\"")
    )));
}

#[tokio::test]
async fn reactive_processor_scheduler_runs_with_period_and_batch_limits() {
    let dir = tempdir().expect("temp");
    let db =
        Arc::new(AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");

    let seen_batches = Arc::new(std::sync::Mutex::new(Vec::<usize>::new()));
    let seen_batches_handler = Arc::clone(&seen_batches);
    db.start_reactive_processor(
        "sched_processor",
        ReactiveProcessorOptions {
            caller_id: None,
            topic_filter: Some("hand_settled".into()),
            run_on_interval: false,
            max_allowed_lag_commits: None,
            max_allowed_stall_ms: None,
            max_events_per_run: 2,
            max_bytes_per_run: 1_000_000,
            max_run_duration_ms: 250,
            run_interval_ms: 10,
            idle_backoff_ms: 10,
            checkpoint_watermark_commits: 1,
            max_retries: 3,
            retry_backoff_ms: 5,
        },
        move |_db, events| {
            let seen_batches_handler = Arc::clone(&seen_batches_handler);
            async move {
                seen_batches_handler
                    .lock()
                    .expect("batch lock")
                    .push(events.len());
                Ok(())
            }
        },
    )
    .await
    .expect("start processor");

    let mut last_seq = 0u64;
    for i in 0..5 {
        let commit = db
            .emit_event(
                "arcana",
                "app",
                "hand_settled",
                format!("hand-{i}"),
                format!(r#"{{"hand":"{i}"}}"#),
            )
            .await
            .expect("emit event");
        last_seq = last_seq.max(commit.commit_seq);
    }

    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let lag = db
            .reactive_processor_lag("sched_processor", ConsistencyMode::AtLatest)
            .await
            .expect("lag");
        if lag.checkpoint_seq >= last_seq {
            break;
        }
        if Instant::now() >= deadline {
            panic!("processor did not catch up before deadline");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let status = db
        .reactive_processor_runtime_status("sched_processor")
        .await
        .expect("runtime status");
    assert!(status.processed_events_total >= 5);
    let batches = seen_batches.lock().expect("batches").clone();
    assert!(!batches.is_empty());
    assert!(batches.iter().all(|size| *size <= 2));

    db.stop_reactive_processor("sched_processor")
        .await
        .expect("stop processor");
    assert!(
        db.reactive_processor_runtime_status("sched_processor")
            .await
            .is_none()
    );
}

#[tokio::test]
async fn reactive_processor_registry_persists_and_auto_resumes_on_handler_registration() {
    let dir = tempdir().expect("temp");
    let db =
        Arc::new(AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");

    let processed = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let processed_first = Arc::clone(&processed);
    db.start_reactive_processor(
        "resume_processor",
        ReactiveProcessorOptions {
            topic_filter: Some("hand_settled".into()),
            checkpoint_watermark_commits: 1,
            ..ReactiveProcessorOptions::default()
        },
        move |_db, events| {
            let processed_first = Arc::clone(&processed_first);
            async move {
                processed_first
                    .fetch_add(events.len() as u64, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
        },
    )
    .await
    .expect("start processor");

    let first = db
        .emit_event("arcana", "app", "hand_settled", "h1".into(), "{}".into())
        .await
        .expect("emit first");
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let lag = db
            .reactive_processor_lag("resume_processor", ConsistencyMode::AtLatest)
            .await
            .expect("lag");
        if lag.checkpoint_seq >= first.commit_seq {
            break;
        }
        if Instant::now() >= deadline {
            panic!("processor failed to checkpoint first event");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    db.shutdown().await.expect("graceful shutdown");
    drop(db);

    let db2 =
        Arc::new(AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("reopen"));
    let processed_resume = Arc::clone(&processed);
    let resumed = db2
        .register_reactive_processor_handler("resume_processor", move |_db, events| {
            let processed = Arc::clone(&processed_resume);
            async move {
                processed.fetch_add(events.len() as u64, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
        })
        .await
        .expect("register handler");
    assert!(resumed, "enabled processor should auto-resume");

    let second = db2
        .emit_event("arcana", "app", "hand_settled", "h2".into(), "{}".into())
        .await
        .expect("emit second");
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let lag = db2
            .reactive_processor_lag("resume_processor", ConsistencyMode::AtLatest)
            .await
            .expect("lag after resume");
        if lag.checkpoint_seq >= second.commit_seq {
            break;
        }
        if Instant::now() >= deadline {
            panic!("processor failed to checkpoint second event after resume");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    db2.stop_reactive_processor("resume_processor")
        .await
        .expect("stop resumed");
    assert!(
        processed.load(std::sync::atomic::Ordering::Relaxed) >= 2,
        "expected both events processed"
    );
}

#[tokio::test]
async fn reactive_processor_scheduler_retries_and_then_succeeds() {
    let dir = tempdir().expect("temp");
    let db =
        Arc::new(AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");

    let attempts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let attempts_handler = Arc::clone(&attempts);
    db.start_reactive_processor(
        "retry_processor",
        ReactiveProcessorOptions {
            caller_id: None,
            topic_filter: Some("hand_settled".into()),
            run_on_interval: false,
            max_allowed_lag_commits: None,
            max_allowed_stall_ms: None,
            checkpoint_watermark_commits: 1,
            max_events_per_run: 16,
            max_bytes_per_run: 1_000_000,
            max_run_duration_ms: 250,
            run_interval_ms: 10,
            idle_backoff_ms: 10,
            max_retries: 3,
            retry_backoff_ms: 10,
        },
        move |_db, _events| {
            let attempts_handler = Arc::clone(&attempts_handler);
            async move {
                let n = attempts_handler.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if n == 0 {
                    return Err(AedbError::Validation("injected retryable failure".into()));
                }
                Ok(())
            }
        },
    )
    .await
    .expect("start retry processor");

    let evt = db
        .emit_event(
            "arcana",
            "app",
            "hand_settled",
            "retry-hand".into(),
            "{}".into(),
        )
        .await
        .expect("emit");

    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let lag = db
            .reactive_processor_lag("retry_processor", ConsistencyMode::AtLatest)
            .await
            .expect("lag");
        if lag.checkpoint_seq >= evt.commit_seq {
            break;
        }
        if Instant::now() >= deadline {
            panic!("retry processor did not checkpoint");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let status = db
        .reactive_processor_runtime_status("retry_processor")
        .await
        .expect("status");
    assert!(status.retries_total >= 1, "expected at least one retry");
    assert_eq!(status.dead_lettered_total, 0);
    db.stop_reactive_processor("retry_processor")
        .await
        .expect("stop");
}

#[tokio::test]
async fn reactive_processor_scheduler_dead_letters_after_retry_exhaustion() {
    let dir = tempdir().expect("temp");
    let db =
        Arc::new(AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");

    db.start_reactive_processor(
        "dlq_processor",
        ReactiveProcessorOptions {
            caller_id: None,
            topic_filter: Some("hand_settled".into()),
            run_on_interval: false,
            max_allowed_lag_commits: None,
            max_allowed_stall_ms: None,
            checkpoint_watermark_commits: 1,
            max_events_per_run: 16,
            max_bytes_per_run: 1_000_000,
            max_run_duration_ms: 250,
            run_interval_ms: 10,
            idle_backoff_ms: 10,
            max_retries: 1,
            retry_backoff_ms: 10,
        },
        move |_db, _events| async move { Err(AedbError::Validation("permanent fail".into())) },
    )
    .await
    .expect("start dlq processor");

    let evt = db
        .emit_event(
            "arcana",
            "app",
            "hand_settled",
            "dlq-hand".into(),
            "{}".into(),
        )
        .await
        .expect("emit");

    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        let lag = db
            .reactive_processor_lag("dlq_processor", ConsistencyMode::AtLatest)
            .await
            .expect("lag");
        if lag.checkpoint_seq >= evt.commit_seq {
            break;
        }
        if Instant::now() >= deadline {
            panic!("dlq processor did not advance checkpoint");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let dlq = db
        .query(
            SYSTEM_PROJECT_ID,
            "app",
            Query::select(&["processor_name", "event_key", "attempts"])
                .from("reactive_processor_dead_letters")
                .where_(Expr::Eq(
                    "processor_name".into(),
                    Value::Text("dlq_processor".into()),
                ))
                .limit(10),
        )
        .await
        .expect("query dlq");
    assert_eq!(dlq.rows.len(), 1);
    assert_eq!(dlq.rows[0].values[1], Value::Text("dlq-hand".into()));
    assert_eq!(dlq.rows[0].values[2], Value::Integer(2));

    let status = db
        .reactive_processor_runtime_status("dlq_processor")
        .await
        .expect("status");
    assert!(status.dead_lettered_total >= 1);
    db.stop_reactive_processor("dlq_processor")
        .await
        .expect("stop");
}

#[tokio::test]
async fn reactive_processor_scheduler_requires_caller_id_in_secure_mode() {
    let dir = tempdir().expect("temp");
    let db = Arc::new(
        AedbInstance::open_secure(AedbConfig::production([11u8; 32]), dir.path())
            .expect("open secure"),
    );

    let err = db
        .start_reactive_processor(
            "secure_proc",
            ReactiveProcessorOptions {
                caller_id: None,
                ..ReactiveProcessorOptions::default()
            },
            move |_db, _events| async move { Ok(()) },
        )
        .await
        .expect_err("secure mode should require caller_id");
    assert!(matches!(err, AedbError::PermissionDenied(_)));
}

#[tokio::test]
async fn reactive_processor_scheduler_uses_explicit_caller_permissions() {
    let dir = tempdir().expect("temp");
    let db = Arc::new(
        AedbInstance::open_secure(AedbConfig::production([12u8; 32]), dir.path())
            .expect("open secure"),
    );
    let system = CallerContext::system_internal();
    db.commit_as(
        system.clone(),
        Mutation::Ddl(DdlOperation::CreateProject {
            owner_id: None,
            if_not_exists: true,
            project_id: "arcana".into(),
        }),
    )
    .await
    .expect("create project");
    db.commit_as(
        system.clone(),
        Mutation::Ddl(DdlOperation::CreateScope {
            owner_id: None,
            if_not_exists: true,
            project_id: "arcana".into(),
            scope_id: "app".into(),
        }),
    )
    .await
    .expect("create scope");
    db.commit_as(
        system.clone(),
        Mutation::Ddl(DdlOperation::GrantPermission {
            actor_id: Some("system".into()),
            delegable: false,
            caller_id: "proc_sched".into(),
            permission: Permission::TableWrite {
                project_id: SYSTEM_PROJECT_ID.into(),
                scope_id: "app".into(),
                table_name: "reactive_processor_registry".into(),
            },
        }),
    )
    .await
    .expect("grant registry write");

    let processed = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let processed_handler = Arc::clone(&processed);
    db.start_reactive_processor(
        "secure_sched",
        ReactiveProcessorOptions {
            caller_id: Some("proc_sched".into()),
            topic_filter: Some("hand_settled".into()),
            checkpoint_watermark_commits: 1,
            run_interval_ms: 10,
            idle_backoff_ms: 10,
            max_retries: 0,
            ..ReactiveProcessorOptions::default()
        },
        move |_db, events| {
            let processed_handler = Arc::clone(&processed_handler);
            async move {
                processed_handler
                    .fetch_add(events.len() as u64, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
        },
    )
    .await
    .expect("start processor");

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let status = db
            .reactive_processor_runtime_status("secure_sched")
            .await
            .expect("status");
        if status.failures_total > 0 {
            break;
        }
        if Instant::now() >= deadline {
            panic!("expected scheduler permission failures before grants");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    for permission in [
        Permission::TableRead {
            project_id: SYSTEM_PROJECT_ID.into(),
            scope_id: "app".into(),
            table_name: "event_outbox".into(),
        },
        Permission::TableRead {
            project_id: SYSTEM_PROJECT_ID.into(),
            scope_id: "app".into(),
            table_name: "reactive_processor_checkpoints".into(),
        },
        Permission::TableWrite {
            project_id: SYSTEM_PROJECT_ID.into(),
            scope_id: "app".into(),
            table_name: "reactive_processor_checkpoints".into(),
        },
    ] {
        db.commit_as(
            system.clone(),
            Mutation::Ddl(DdlOperation::GrantPermission {
                actor_id: Some("system".into()),
                delegable: false,
                caller_id: "proc_sched".into(),
                permission,
            }),
        )
        .await
        .expect("grant processor permission");
    }

    let evt = db
        .emit_event_as(
            system,
            "arcana",
            "app",
            "hand_settled",
            "secure-hand".into(),
            "{}".into(),
        )
        .await
        .expect("emit secure event");
    let proc_caller = CallerContext::new("proc_sched");
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let lag = db
            .reactive_processor_lag_as(&proc_caller, "secure_sched", ConsistencyMode::AtLatest)
            .await
            .expect("lag");
        if lag.checkpoint_seq >= evt.commit_seq {
            break;
        }
        if Instant::now() >= deadline {
            panic!("processor failed to progress after grants");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(
        processed.load(std::sync::atomic::Ordering::Relaxed) >= 1,
        "expected at least one processed event"
    );
    db.stop_reactive_processor("secure_sched")
        .await
        .expect("stop processor");
}

#[tokio::test]
async fn reactive_processor_pause_resume_list_and_health_work() {
    let dir = tempdir().expect("temp");
    let db =
        Arc::new(AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");

    db.start_reactive_processor(
        "lifecycle_processor",
        ReactiveProcessorOptions {
            caller_id: None,
            topic_filter: Some("hand_settled".into()),
            checkpoint_watermark_commits: 1,
            run_interval_ms: 10,
            idle_backoff_ms: 10,
            ..ReactiveProcessorOptions::default()
        },
        move |_db, _events| async move { Ok(()) },
    )
    .await
    .expect("start");

    let first = db
        .emit_event(
            "arcana",
            "app",
            "hand_settled",
            "life-1".into(),
            "{}".into(),
        )
        .await
        .expect("emit first");
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let lag = db
            .reactive_processor_lag("lifecycle_processor", ConsistencyMode::AtLatest)
            .await
            .expect("lag");
        if lag.checkpoint_seq >= first.commit_seq {
            break;
        }
        if Instant::now() >= deadline {
            panic!("lifecycle processor did not checkpoint first event");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let listed = db
        .list_reactive_processors(ConsistencyMode::AtLatest)
        .await
        .expect("list processors");
    let row = listed
        .into_iter()
        .find(|r| r.processor_name == "lifecycle_processor")
        .expect("processor listed");
    assert!(row.enabled);
    assert!(row.running);

    let health = db
        .reactive_processor_health("lifecycle_processor", ConsistencyMode::AtLatest)
        .await
        .expect("health");
    assert!(health.running);
    assert!(health.enabled);
    assert!(health.processed_events_total >= 1);
    assert!(health.last_run_completed_micros.is_some());

    db.pause_reactive_processor("lifecycle_processor")
        .await
        .expect("pause");

    let listed = db
        .list_reactive_processors(ConsistencyMode::AtLatest)
        .await
        .expect("list processors paused");
    let row = listed
        .into_iter()
        .find(|r| r.processor_name == "lifecycle_processor")
        .expect("processor listed paused");
    assert!(!row.enabled);
    assert!(!row.running);

    let paused_health = db
        .reactive_processor_health("lifecycle_processor", ConsistencyMode::AtLatest)
        .await
        .expect("paused health");
    assert!(!paused_health.running);
    assert!(!paused_health.enabled);

    db.resume_reactive_processor("lifecycle_processor")
        .await
        .expect("resume");

    let second = db
        .emit_event(
            "arcana",
            "app",
            "hand_settled",
            "life-2".into(),
            "{}".into(),
        )
        .await
        .expect("emit second");
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let lag = db
            .reactive_processor_lag("lifecycle_processor", ConsistencyMode::AtLatest)
            .await
            .expect("lag resumed");
        if lag.checkpoint_seq >= second.commit_seq {
            break;
        }
        if Instant::now() >= deadline {
            panic!("lifecycle processor did not checkpoint second event");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    db.stop_reactive_processor("lifecycle_processor")
        .await
        .expect("stop");
}

#[tokio::test]
async fn reactive_processor_resume_requires_registered_handler() {
    let dir = tempdir().expect("temp");
    let db =
        Arc::new(AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");
    db.start_reactive_processor(
        "resume_requires_handler",
        ReactiveProcessorOptions {
            caller_id: None,
            topic_filter: Some("hand_settled".into()),
            ..ReactiveProcessorOptions::default()
        },
        move |_db, _events| async move { Ok(()) },
    )
    .await
    .expect("start");
    db.pause_reactive_processor("resume_requires_handler")
        .await
        .expect("pause");
    db.shutdown().await.expect("shutdown");
    drop(db);

    let db2 =
        Arc::new(AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("reopen"));
    let err = db2
        .resume_reactive_processor("resume_requires_handler")
        .await
        .expect_err("resume without handler should fail");
    assert!(matches!(err, AedbError::Validation(_)));
}

#[tokio::test]
async fn reactive_processor_scheduler_can_run_periodically_without_events() {
    let dir = tempdir().expect("temp");
    let db =
        Arc::new(AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");

    let ticks = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let ticks_handler = Arc::clone(&ticks);
    db.start_reactive_processor(
        "periodic_processor",
        ReactiveProcessorOptions {
            caller_id: None,
            topic_filter: Some("never_emitted".into()),
            run_on_interval: true,
            run_interval_ms: 10,
            idle_backoff_ms: 10,
            max_retries: 0,
            ..ReactiveProcessorOptions::default()
        },
        move |_db, events| {
            let ticks_handler = Arc::clone(&ticks_handler);
            async move {
                assert!(events.is_empty(), "periodic run should be empty-batch");
                ticks_handler.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
        },
    )
    .await
    .expect("start periodic processor");

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let n = ticks.load(std::sync::atomic::Ordering::Relaxed);
        if n >= 3 {
            break;
        }
        if Instant::now() >= deadline {
            panic!("periodic processor did not run enough ticks");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    db.stop_reactive_processor("periodic_processor")
        .await
        .expect("stop periodic");
}

#[tokio::test]
async fn reactive_processor_slo_status_and_enforcement_detect_breaches() {
    let dir = tempdir().expect("temp");
    let db =
        Arc::new(AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");

    db.start_reactive_processor(
        "slo_breach_processor",
        ReactiveProcessorOptions {
            caller_id: None,
            topic_filter: Some("hand_settled".into()),
            run_on_interval: false,
            max_allowed_lag_commits: Some(0),
            max_allowed_stall_ms: None,
            run_interval_ms: 60_000,
            idle_backoff_ms: 60_000,
            ..ReactiveProcessorOptions::default()
        },
        move |_db, _events| async move { Ok(()) },
    )
    .await
    .expect("start processor");

    db.emit_event(
        "arcana",
        "app",
        "hand_settled",
        "slo-hand".into(),
        "{}".into(),
    )
    .await
    .expect("emit");
    tokio::time::sleep(Duration::from_millis(20)).await;

    let status = db
        .reactive_processor_slo_status("slo_breach_processor", ConsistencyMode::AtLatest)
        .await
        .expect("slo status");
    assert!(status.breached, "expected lag SLO breach");
    assert!(!status.reasons.is_empty());

    let statuses = db
        .list_reactive_processor_slo_statuses(ConsistencyMode::AtLatest)
        .await
        .expect("list slo statuses");
    assert!(
        statuses
            .iter()
            .any(|s| s.processor_name == "slo_breach_processor" && s.breached)
    );

    let enforce = db
        .enforce_reactive_processor_slos(ConsistencyMode::AtLatest)
        .await;
    assert!(matches!(enforce, Err(AedbError::Unavailable { .. })));

    db.stop_reactive_processor("slo_breach_processor")
        .await
        .expect("stop");
}

#[tokio::test]
async fn reactive_processor_slo_status_healthy_when_within_thresholds() {
    let dir = tempdir().expect("temp");
    let db =
        Arc::new(AedbInstance::open_anonymous(AedbConfig::default(), dir.path()).expect("open"));
    db.create_project("arcana").await.expect("project");
    db.create_scope("arcana", "app").await.expect("scope");

    let ticks = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let ticks_handler = Arc::clone(&ticks);
    db.start_reactive_processor(
        "slo_ok_processor",
        ReactiveProcessorOptions {
            caller_id: None,
            topic_filter: Some("none".into()),
            run_on_interval: true,
            max_allowed_lag_commits: Some(10),
            max_allowed_stall_ms: Some(5_000),
            run_interval_ms: 10,
            idle_backoff_ms: 10,
            ..ReactiveProcessorOptions::default()
        },
        move |_db, _events| {
            let ticks_handler = Arc::clone(&ticks_handler);
            async move {
                ticks_handler.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
        },
    )
    .await
    .expect("start");

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let n = ticks.load(std::sync::atomic::Ordering::Relaxed);
        if n >= 2 {
            break;
        }
        if Instant::now() >= deadline {
            panic!("processor did not tick");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let status = db
        .reactive_processor_slo_status("slo_ok_processor", ConsistencyMode::AtLatest)
        .await
        .expect("slo status");
    assert!(!status.breached, "expected no SLO breach");

    db.enforce_reactive_processor_slos(ConsistencyMode::AtLatest)
        .await
        .expect("slo enforce should pass");
    db.stop_reactive_processor("slo_ok_processor")
        .await
        .expect("stop");
}

#[tokio::test]
async fn emit_event_respects_max_event_payload_bytes() {
    let dir = tempdir().expect("temp");
    let config = AedbConfig {
        max_event_payload_bytes: 16,
        ..AedbConfig::default()
    };
    let db = AedbInstance::open_anonymous(config, dir.path()).expect("open");
    db.create_project("p").await.expect("project");
    db.create_scope("p", "app").await.expect("scope");

    let err = db
        .emit_event(
            "p",
            "app",
            "topic",
            "event-1".into(),
            "{\"payload\":\"1234567890\"}".into(),
        )
        .await
        .expect_err("event payload should be bounded");
    assert!(
        matches!(err, AedbError::Validation(ref msg) if msg.contains("max_event_payload_bytes"))
    );
}
