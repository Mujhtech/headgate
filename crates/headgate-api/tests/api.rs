//! Control-API integration tests over the live Postgres store. Opt-in via HG_TEST_PG;
//! requests go straight through the tower service — no listener needed.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use headgate_api::{ApiConfig, router};
use headgate_core::{
    AdmitRequest, Checkpoint, Envelope, Inspect, JobResult, MissedPolicy, Outcome, OutputStore,
    ProgressStore, ProgressUpdate, Schedule, Store,
};
use headgate_postgres::PgStore;
use headgate_workflow::Workflow;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

#[tokio::test]
async fn workflow_control_routes_share_the_idempotency_boundary() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping workflow control API proof");
        return;
    };
    let store = Arc::new(PgStore::connect(&conninfo, 2).expect("connect"));
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let workflow_id = format!("api-workflow-{suffix}");
    let queue = format!("api-workflow-{suffix}");
    let batch = Workflow::new(&workflow_id)
        .coordinator_queue(&queue)
        .add(
            "prepare",
            Envelope {
                kind: "api.workflow.prepare".into(),
                payload: br#"{"secret":"not-in-graph-response"}"#.to_vec(),
                queue: queue.clone(),
                ..Default::default()
            },
            Vec::<String>::new(),
        )
        .add_signal("approval", "approved", ["prepare"])
        .prepare()
        .unwrap();
    store.enqueue(&batch).await.unwrap();
    let app = router(store as Arc<dyn Inspect>, ApiConfig::default());

    let (status, workflows) = call_with_key(
        &app,
        Method::GET,
        "/api/v1/workflows?limit=50",
        None,
        "unused-on-read",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        workflows["workflows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|workflow| workflow["workflow_id"] == workflow_id)
    );

    let (status, snapshot) = call_with_key(
        &app,
        Method::GET,
        &format!("/api/v1/workflows/{workflow_id}"),
        None,
        "unused-on-read",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(snapshot["nodes"].as_array().unwrap().len(), 2);
    assert!(!snapshot.to_string().contains("not-in-graph-response"));
    let (status, dependencies) = call_with_key(
        &app,
        Method::GET,
        &format!("/api/v1/workflows/{workflow_id}/nodes/approval/dependencies"),
        None,
        "unused-on-read",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(dependencies["dependencies"][0]["name"], "prepare");

    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("/api/v1/workflows/{workflow_id}/signals"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({"signal": "approved"}).to_string()))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(request).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );

    let (status, receipt) = call_with_key(
        &app,
        Method::POST,
        &format!("/api/v1/workflows/{workflow_id}/signals"),
        Some(json!({
            "signal": "approved",
            "payload": {"approved": true, "reviewer": "Ada"},
            "source": {"emitter": "admin-console", "actor": "operator-42"}
        })),
        "workflow-signal-1",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(receipt["matched"], 1);
    assert_eq!(receipt["inserted"], true);
    assert_eq!(receipt["emission"]["idempotency_key"], "workflow-signal-1");
    assert_eq!(receipt["emission"]["payload"]["reviewer"], "Ada");
    assert_eq!(receipt["emission"]["source"]["actor"], "operator-42");

    let (status, history) = call_with_key(
        &app,
        Method::GET,
        &format!("/api/v1/workflows/{workflow_id}/signals?limit=100"),
        None,
        "unused-on-read",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(history["signals"].as_array().unwrap().len(), 1);
    assert_eq!(history["signals"][0], receipt["emission"]);

    let (status, replay) = call_with_key(
        &app,
        Method::POST,
        &format!("/api/v1/workflows/{workflow_id}/signals"),
        Some(json!({
            "signal": "approved",
            "payload": {"approved": true, "reviewer": "Ada"},
            "source": {"emitter": "admin-console", "actor": "operator-42"}
        })),
        "workflow-signal-1",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay["inserted"], false);
    assert_eq!(replay["emission"], receipt["emission"]);

    let (status, graft) = call_with_key(
        &app,
        Method::POST,
        &format!("/api/v1/workflows/{workflow_id}/grafts"),
        Some(json!({
            "expected_revision": 1,
            "queue": queue,
            "tasks": [{"name": "audit", "kind": "api.workflow.audit", "payload": "e30="}]
        })),
        "workflow-graft-1",
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(graft["receipt_id"], format!("{workflow_id}:graft:2"));

    let (status, _) = call_with_key(
        &app,
        Method::POST,
        &format!("/api/v1/workflows/{workflow_id}/cancel"),
        Some(json!({"propagate_children": true})),
        "workflow-cancel-1",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

async fn call_with_key(
    app: &axum::Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
    key: &str,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(method.clone())
        .uri(uri)
        .header("idempotency-key", key);
    if body.is_some() {
        req = req.header(header::CONTENT_TYPE, "application/json");
    }
    let req = req
        .body(
            body.map(|b| Body::from(b.to_string()))
                .unwrap_or_else(Body::empty),
        )
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let v = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, v)
}

#[tokio::test]
async fn job_checkpoint_has_an_explicit_operator_endpoint() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping job checkpoint API proof");
        return;
    };
    let store = Arc::new(PgStore::connect(&conninfo, 2).expect("connect"));
    {
        let tx = store.begin().await.unwrap();
        tx.client()
            .unwrap()
            .execute(
                "DELETE FROM headgate_job WHERE ulid = 'api-checkpoint'",
                &[],
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    store
        .enqueue(&[Envelope {
            id: "api-checkpoint".into(),
            kind: "api.checkpoint".into(),
            queue: "api-checkpoint".into(),
            payload: b"{}".to_vec(),
            fingerprint: headgate_core::fingerprint("api.checkpoint", b"api-checkpoint"),
            scheduled_at_ms: 1,
            retention_ms: 0,
            ..Default::default()
        }])
        .await
        .unwrap();
    let lease = store
        .admit(AdmitRequest {
            worker: "api-checkpoint-worker".into(),
            lease_id: "api-checkpoint-lease".into(),
            queues: vec!["api-checkpoint".into()],
            capacity: 1,
            lease: std::time::Duration::from_secs(30),
            quantum: 1,
        })
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .claims[0]
        .lease_ref();
    store
        .checkpoint(
            &lease,
            &Checkpoint {
                last_completed_step: Some("download".into()),
                completed_steps: vec!["download".into()],
                in_progress_step: Some("transform".into()),
                cursor_step: Some("transform".into()),
                cursor: Some(br#"{"offset":42}"#.to_vec()),
                schema_version: 2,
                step_set_hash: "sha256:steps".into(),
                crashes_by_step: vec![("transform".into(), 1)],
            },
        )
        .await
        .unwrap();
    let inspect: Arc<dyn Inspect> = store.clone();
    let app = router(inspect, ApiConfig::default());

    let (status, body) = call(
        &app,
        Method::GET,
        "/api/v1/jobs/api-checkpoint/checkpoint",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["completed_steps"], json!(["download"]));
    assert_eq!(body["in_progress_step"], "transform");
    assert_eq!(body["cursor"], "eyJvZmZzZXQiOjQyfQ==");
    assert_eq!(body["crashes_by_step"]["transform"], 1);

    let (status, body) = call(&app, Method::GET, "/api/v1/jobs/api-checkpoint", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.get("checkpoint").is_none(),
        "ordinary job detail leaked checkpoint"
    );

    store
        .ack(&lease, Outcome::Success, None, None)
        .await
        .unwrap();
}

async fn call(
    app: &axum::Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    // Distinct per call — reusing a key would (correctly!) replay a previous enqueue.
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    call_with_key(app, method, uri, body, &format!("test-{seq}")).await
}

fn b64(s: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(s)
}

#[tokio::test]
async fn mid_run_output_has_an_explicit_payload_endpoint() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping mid-run output API proof");
        return;
    };
    let store = Arc::new(PgStore::connect(&conninfo, 2).expect("connect"));
    {
        let tx = store.begin().await.unwrap();
        tx.client()
            .unwrap()
            .execute("DELETE FROM headgate_job WHERE ulid = 'api-output'", &[])
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    store
        .enqueue(&[Envelope {
            id: "api-output".into(),
            kind: "api.output".into(),
            queue: "api-output".into(),
            payload: b"{}".to_vec(),
            fingerprint: headgate_core::fingerprint("api.output", b"api-output"),
            scheduled_at_ms: 1,
            retention_ms: 0,
            ..Default::default()
        }])
        .await
        .unwrap();
    let claim = store
        .admit(AdmitRequest {
            worker: "api-output-worker".into(),
            lease_id: "api-output-lease".into(),
            queues: vec!["api-output".into()],
            capacity: 1,
            lease: std::time::Duration::from_secs(30),
            quantum: 1,
        })
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .claims[0]
        .lease_ref();
    let lease = claim;
    let output = OutputStore::write_job_output(
        store.as_ref(),
        &lease,
        &JobResult {
            schema_version: 7,
            bytes: vec![0, 0xff],
        },
    )
    .await
    .unwrap();
    let inspect: Arc<dyn Inspect> = store.clone();
    let app = router(inspect, ApiConfig::default());

    let (status, body) = call(&app, Method::GET, "/api/v1/jobs/api-output/output", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["schema_version"], 7);
    assert_eq!(body["bytes"], "AP8=");
    assert_eq!(body["fence"], output.fence);
    assert_eq!(body["updated_at_ms"], output.updated_at_ms);

    let (status, body) = call(&app, Method::GET, "/api/v1/jobs/api-output", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.get("output").is_none(),
        "ordinary job detail leaked output"
    );

    store
        .ack(&lease, Outcome::Success, None, None)
        .await
        .unwrap();
}

#[tokio::test]
async fn job_progress_has_an_explicit_operator_endpoint() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping job progress API proof");
        return;
    };
    let store = Arc::new(PgStore::connect(&conninfo, 2).expect("connect"));
    {
        let tx = store.begin().await.unwrap();
        tx.client()
            .unwrap()
            .execute("DELETE FROM headgate_job WHERE ulid = 'api-progress'", &[])
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    store
        .enqueue(&[Envelope {
            id: "api-progress".into(),
            kind: "api.progress".into(),
            queue: "api-progress".into(),
            payload: b"{}".to_vec(),
            fingerprint: headgate_core::fingerprint("api.progress", b"api-progress"),
            scheduled_at_ms: 1,
            retention_ms: 0,
            ..Default::default()
        }])
        .await
        .unwrap();
    let lease = store
        .admit(AdmitRequest {
            worker: "api-progress-worker".into(),
            lease_id: "api-progress-lease".into(),
            queues: vec!["api-progress".into()],
            capacity: 1,
            lease: std::time::Duration::from_secs(30),
            quantum: 1,
        })
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .claims[0]
        .lease_ref();
    let progress = ProgressStore::write_job_progress(
        store.as_ref(),
        &lease,
        &ProgressUpdate {
            current: 42,
            total: 100,
            message: Some("encoding frame 420".into()),
        },
    )
    .await
    .unwrap();
    let inspect: Arc<dyn Inspect> = store.clone();
    let app = router(inspect, ApiConfig::default());

    let (status, body) = call(
        &app,
        Method::GET,
        "/api/v1/jobs/api-progress/progress",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["current"], 42);
    assert_eq!(body["total"], 100);
    assert_eq!(body["message"], "encoding frame 420");
    assert_eq!(body["fence"], progress.fence);
    assert_eq!(body["updated_at_ms"], progress.updated_at_ms);

    let (status, body) = call(&app, Method::GET, "/api/v1/jobs/api-progress", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.get("progress").is_none(),
        "ordinary job detail leaked progress"
    );

    store
        .ack(&lease, Outcome::Success, None, None)
        .await
        .unwrap();
}

#[tokio::test]
async fn enqueue_outage_is_service_unavailable_not_a_bad_request() {
    let store = Arc::new(
        PgStore::connect(
            "host=127.0.0.1 port=1 user=headgate dbname=headgate connect_timeout=1",
            1,
        )
        .expect("construct lazy pool"),
    );
    let inspect: Arc<dyn Inspect> = store;
    let breaker = Arc::new(
        headgate::CircuitBreaker::new(headgate::CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: std::time::Duration::from_secs(60),
            half_open_max_calls: 1,
        })
        .unwrap(),
    );
    let config = ApiConfig {
        enqueue_circuit_breaker: Some(breaker.clone()),
        ..ApiConfig::default()
    };
    let app = router(inspect, config);
    let (status, body) = call_with_key(
        &app,
        Method::POST,
        "/api/v1/jobs",
        Some(json!({"id": "api-outage", "kind": "outage", "payload": b64("{}")})),
        "api-outage",
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|message| message.starts_with("store unavailable:")),
        "typed outage body changed: {body}"
    );

    let (status, body) = call_with_key(
        &app,
        Method::POST,
        "/api/v1/jobs",
        Some(json!({"id": "api-circuit", "kind": "outage", "payload": b64("{}")})),
        "api-circuit",
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"], "enqueue circuit open");
    assert_eq!(body["state"], "open");
    assert!(
        body["retry_after_ms"].as_u64().is_some_and(|ms| ms > 0),
        "open circuit must tell the caller when probes resume: {body}"
    );
    assert_eq!(breaker.snapshot().state, headgate::CircuitState::Open);
}

#[tokio::test]
async fn enqueue_authorization_guards_http_and_periodic_paths() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping enqueue authorization API proof");
        return;
    };
    let store = Arc::new(PgStore::connect(&conninfo, 2).expect("connect"));
    {
        let tx = store.begin().await.unwrap();
        tx.client()
            .unwrap()
            .batch_execute(
                "DELETE FROM headgate_job WHERE queue IN ('api-auth', 'api-auth-periodic');
                 DELETE FROM headgate_schedule WHERE id IN ('api-auth-denied', 'api-auth-existing');",
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let authorizer: Arc<dyn headgate::EnqueueAuthorizer> = Arc::new(
        |context: &headgate::EnqueueContext, envelope: &headgate::Envelope| {
            context.source == headgate::EnqueueSource::Http
                && context
                    .identity
                    .as_ref()
                    .is_some_and(|identity| identity.subject == "service:mailer")
                && envelope.kind == "mail.send"
        },
    );
    let inspect: Arc<dyn Inspect> = store.clone();
    let config = ApiConfig {
        enqueue_authorizer: authorizer,
        ..ApiConfig::default()
    };
    let app = router(inspect, config).layer(axum::Extension(headgate::EnqueueIdentity::new(
        "service:mailer",
    )));

    let (status, body) = call_with_key(
        &app,
        Method::POST,
        "/api/v1/jobs",
        Some(json!({
            "id": "api-auth-http-denied",
            "kind": "billing.charge",
            "payload": b64("{}"),
            "queue": "api-auth"
        })),
        "api-auth-1",
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(
        body,
        json!({"error": "enqueue forbidden", "kind": "billing.charge"})
    );
    assert!(
        store
            .get_job("api-auth-http-denied", false)
            .await
            .unwrap()
            .is_none(),
        "a forbidden request must not touch the store"
    );

    let (status, _) = call_with_key(
        &app,
        Method::POST,
        "/api/v1/jobs",
        Some(json!({
            "id": "api-auth-http-allowed",
            "kind": "mail.send",
            "payload": b64("{}"),
            "queue": "api-auth"
        })),
        "api-auth-2",
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = call_with_key(
        &app,
        Method::PUT,
        "/api/v1/periodic/api-auth-denied",
        Some(json!({"kind": "billing.charge", "spec": "@every:60000"})),
        "api-auth-3",
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(
        store
            .list_schedules()
            .await
            .unwrap()
            .iter()
            .all(|schedule| schedule.id != "api-auth-denied"),
        "a forbidden periodic definition must not create a future bypass"
    );

    store
        .upsert_schedule(&Schedule {
            id: "api-auth-existing".into(),
            kind: "billing.charge".into(),
            payload: b"{}".to_vec(),
            queue: "api-auth-periodic".into(),
            partition_key: String::new(),
            rate_class: String::new(),
            priority: 0,
            max_attempts: 25,
            retention_ms: 86_400_000,
            spec: "@every:60000".into(),
            next_run_ms: i64::MAX / 2,
            last_enqueued_ms: None,
            on_missed: MissedPolicy::Skip,
            backfill_limit: 0,
            paused: false,
        })
        .await
        .unwrap();
    let before: i64 = store
        .counts(Some("api-auth-periodic"))
        .await
        .unwrap()
        .counts
        .iter()
        .map(|(_, count)| count)
        .sum();
    let (status, body) = call_with_key(
        &app,
        Method::POST,
        "/api/v1/periodic/api-auth-existing/run",
        None,
        "api-auth-4",
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let after: i64 = store
        .counts(Some("api-auth-periodic"))
        .await
        .unwrap()
        .counts
        .iter()
        .map(|(_, count)| count)
        .sum();
    assert_eq!(before, after, "manual periodic run bypassed authorization");
}

#[tokio::test]
async fn control_api_end_to_end() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping API test");
        return;
    };
    let store = Arc::new(PgStore::connect(&conninfo, 4).expect("connect"));
    {
        let tx = store.begin().await.unwrap();
        tx.client()
            .unwrap()
            .batch_execute(
                "DELETE FROM headgate_job WHERE queue LIKE 'api-%';
                 DELETE FROM headgate_queue_state WHERE queue LIKE 'api-%';
                 DELETE FROM headgate_rate_bucket WHERE name = 'api-stripe';
                 DELETE FROM headgate_concurrency_limit WHERE name = 'api-cl';",
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let inspect: Arc<dyn Inspect> = store.clone();
    let app = router(inspect, ApiConfig::default());

    // meta declares only what the backend honors (runtime capability boundary).
    let (st, v) = call(&app, Method::GET, "/api/v1/meta", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["backend"], "postgres");
    assert!(
        v["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c == "inspect")
    );

    // A mutating call without Idempotency-Key is a 400.
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/jobs")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({"kind": "k", "payload": b64("{}")}).to_string(),
        ))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );

    // JSON preserves presence, so an explicit zero is invalid even though the binary
    // envelope reserves zero as the backwards-compatible "field omitted" sentinel.
    let (st, v) = call(
        &app,
        Method::POST,
        "/api/v1/jobs",
        Some(json!({"kind": "api:msg", "payload": b64("{}"), "weight": 0})),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(v["error"], "weight must be >= 1");

    let (st, v) = call(
        &app,
        Method::POST,
        "/api/v1/jobs",
        Some(json!({"kind": "api:msg", "payload": b64("{}"), "unique_replace": 4})),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(
        v["error"],
        "unique_replace requires caller-supplied unique_key"
    );

    // Queue weight and saturation strategy are fleet policy the gate reads, so
    // invariant 16 requires an operational writer and a read-back surface.
    let (st, _) = call(
        &app,
        Method::PUT,
        "/api/v1/queues/api-weighted",
        Some(json!({"weight": 3})),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, queues) = call(&app, Method::GET, "/api/v1/queues", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        queues
            .as_array()
            .unwrap()
            .iter()
            .find(|q| q["queue"] == "api-weighted")
            .unwrap()["weight"],
        3
    );

    let (st, _) = call(
        &app,
        Method::PUT,
        "/api/v1/concurrency-limits/api-cl",
        Some(json!({
            "queue": "api-weighted",
            "max_concurrent": 2,
            "on_saturated": "cancel_running"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, limits) = call(&app, Method::GET, "/api/v1/concurrency-limits", None).await;
    assert_eq!(st, StatusCode::OK);
    let limit = limits
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == "api-cl")
        .unwrap();
    assert_eq!(limit["queue"], "api-weighted");
    assert_eq!(limit["max_concurrent"], 2);
    assert_eq!(limit["on_saturated"], "cancel_running");

    // Enqueue over HTTP; a retried POST with the same key replays the same job.
    let enq = json!({ "kind": "api:msg", "payload": b64("{\"n\":1}"), "queue": "api-q" });
    let (st, v1) = call_with_key(
        &app,
        Method::POST,
        "/api/v1/jobs",
        Some(enq.clone()),
        "retry-me",
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{v1}");
    let (st, v2) = call_with_key(&app, Method::POST, "/api/v1/jobs", Some(enq), "retry-me").await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(
        v1["id"], v2["id"],
        "same Idempotency-Key must replay, not duplicate"
    );
    assert_eq!(v2["replayed"], true);
    let id = v1["id"].as_str().unwrap().to_string();

    // Detail: payload only on request (invariant 9).
    let (st, v) = call(&app, Method::GET, &format!("/api/v1/jobs/{id}"), None).await;
    assert_eq!(st, StatusCode::OK);
    assert!(
        v.get("payload").is_none(),
        "payload must be withheld by default"
    );
    let (_, v) = call(
        &app,
        Method::GET,
        &format!("/api/v1/jobs/{id}?include_payload=true"),
        None,
    )
    .await;
    assert_eq!(v["payload"], b64("{\"n\":1}"));

    // Admission explain: available and nothing blocking → admissible.
    let (st, v) = call(
        &app,
        Method::GET,
        &format!("/api/v1/jobs/{id}/admission"),
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["admissible"], true, "{v}");

    // Pause the queue → the same job is now blocked_by queue_paused.
    let (st, _) = call(&app, Method::POST, "/api/v1/queues/api-q/pause", None).await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    let (_, v) = call(
        &app,
        Method::GET,
        &format!("/api/v1/jobs/{id}/admission"),
        None,
    )
    .await;
    assert_eq!(v["blocked_by"], "queue_paused");
    assert_eq!(
        v["estimated_admission_ms"],
        Value::Null,
        "won't clear on its own"
    );
    call(&app, Method::POST, "/api/v1/queues/api-q/resume", None).await;

    // Rate-class kill switch (invariant 16): pause the class, job is blocked_by
    // rate_class with no ETA; unpause, and it clears.
    let (st, _) = call(
        &app,
        Method::POST,
        "/api/v1/jobs",
        Some(json!({
            "kind": "api:limited", "payload": b64("{}"), "queue": "api-q",
            "rate_class": "api-stripe", "id": "api-rc-1"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED);
    let (st, _) = call(
        &app,
        Method::PUT,
        "/api/v1/rate-classes/api-stripe",
        Some(json!({ "limit": 5, "window_ms": 1000, "burst": 5, "paused": true })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (_, v) = call(&app, Method::GET, "/api/v1/jobs/api-rc-1/admission", None).await;
    assert_eq!(v["blocked_by"], "rate_class", "{v}");
    assert_eq!(v["estimated_admission_ms"], Value::Null);
    let (_, v) = call(&app, Method::GET, "/api/v1/rate-classes", None).await;
    let rc = v
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "api-stripe")
        .unwrap();
    assert_eq!(rc["paused"], true);
    let (st, _) = call(
        &app,
        Method::PUT,
        "/api/v1/rate-classes/api-stripe",
        Some(json!({ "limit": 5, "window_ms": 1000, "burst": 5 })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    // Unpaused: tokens refill from 0, so the ETA is finite now.
    // Round 32h: this used to be wrapped in `if v["admissible"] == false { .. }`, which
    // skips itself for `true` AND for `Value::Null` — the value `call` returns for a
    // non-JSON or error response. It was the suite's ONLY coverage of the finite-ETA
    // computation and it was opt-out by construction. Both arms are asserted now: the
    // job is either admissible (the class refilled far enough) or blocked BY THE RATE
    // CLASS with a finite ETA. Nothing else is a legal answer, `null` included.
    let (_, v) = call(&app, Method::GET, "/api/v1/jobs/api-rc-1/admission", None).await;
    match v["admissible"].as_bool() {
        Some(true) => assert_eq!(v["blocked_by"], Value::Null, "{v}"),
        Some(false) => {
            assert_eq!(v["blocked_by"], "rate_class", "{v}");
            assert!(
                v["estimated_admission_ms"].as_i64().is_some(),
                "an unpaused class must give a FINITE eta: {v}"
            );
        }
        None => panic!("/admission must answer with a boolean `admissible`: {v}"),
    }

    // A window that would divide by zero is rejected at the boundary (boundary validation).
    let (st, _) = call(
        &app,
        Method::PUT,
        "/api/v1/rate-classes/api-stripe",
        Some(json!({ "limit": 5, "window_ms": 0 })),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // Counts and list see the jobs.
    let (_, v) = call(&app, Method::GET, "/api/v1/jobs/counts?queue=api-q", None).await;
    assert_eq!(v["counts"]["available"], 2, "{v}");
    let (_, v) = call(&app, Method::GET, "/api/v1/jobs?queue=api-q&limit=1", None).await;
    assert_eq!(v["jobs"].as_array().unwrap().len(), 1);
    assert!(v["next_cursor"].is_string(), "a full page carries a cursor");

    // Cancellation is terminal for automatic processing, but an explicit operator retry
    // revives the same job identity and makes it available again.
    let (st, _) = call(
        &app,
        Method::POST,
        &format!("/api/v1/jobs/{id}/cancel"),
        None,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    let (st, v) = call(
        &app,
        Method::POST,
        &format!("/api/v1/jobs/{id}/retry"),
        None,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT, "cancelled retry failed: {v}");
    let (st, v) = call(&app, Method::GET, &format!("/api/v1/jobs/{id}"), None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        v["state"], "available",
        "operator retry must revive the job"
    );
    let (st, v) = call(
        &app,
        Method::POST,
        "/api/v1/jobs/actions",
        Some(json!({ "action": "delete", "ids": [id, "api-rc-1", "no-such-job"] })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["succeeded"].as_array().unwrap().len(), 2, "{v}");
    assert_eq!(v["failed"][0]["id"], "no-such-job");

    // Unknown job → 404; liveness/readiness answer.
    let (st, _) = call(&app, Method::GET, "/api/v1/jobs/nope/admission", None).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, _) = call(&app, Method::GET, "/api/v1/healthz", None).await;
    assert_eq!(st, StatusCode::OK);
    let (st, _) = call(&app, Method::GET, "/api/v1/readyz", None).await;
    assert_eq!(st, StatusCode::OK);
}

#[tokio::test]
async fn phase4_periodic_bulk_workers_search() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping API phase-4 test");
        return;
    };
    let store = Arc::new(PgStore::connect(&conninfo, 4).expect("connect"));
    {
        let tx = store.begin().await.unwrap();
        tx.client()
            .unwrap()
            .batch_execute(
                "DELETE FROM headgate_job WHERE queue LIKE 'p4-%';
                 DELETE FROM headgate_schedule WHERE id LIKE 'p4-%';
                 DELETE FROM headgate_operation WHERE id LIKE 'hg%';
                 DELETE FROM headgate_worker WHERE worker_id LIKE 'p4-%';",
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let inspect: Arc<dyn Inspect> = store.clone();
    let app = router(inspect, ApiConfig::default());

    // Periodic upsert validates the spec; the entry lists; fire-now enqueues and a
    // retried fire with the same key replays.
    let (st, v) = call(
        &app,
        Method::PUT,
        "/api/v1/periodic/p4-daily",
        Some(json!({
            "kind": "p4:tick", "spec": "not a cron"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "bad spec rejected: {v}");
    let (st, _) = call(
        &app,
        Method::PUT,
        "/api/v1/periodic/p4-daily",
        Some(json!({
            "kind": "p4:tick", "spec": "@every:3600000", "queue": "p4-q",
            "payload": b64("{}")
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (_, v) = call(&app, Method::GET, "/api/v1/periodic", None).await;
    assert!(
        v.as_array().unwrap().iter().any(|s| s["id"] == "p4-daily"),
        "{v}"
    );
    let (st, r1) = call_with_key(
        &app,
        Method::POST,
        "/api/v1/periodic/p4-daily/run",
        None,
        "fire-1",
    )
    .await;
    assert_eq!(st, StatusCode::ACCEPTED);
    let (_, r2) = call_with_key(
        &app,
        Method::POST,
        "/api/v1/periodic/p4-daily/run",
        None,
        "fire-1",
    )
    .await;
    assert_eq!(r1["id"], r2["id"], "same key replays the same fired job");

    // Bulk: empty selector is rejected; a real selector becomes an operation the duty
    // executes in batches; dry_run completes immediately with an estimate.
    let (st, v) = call(
        &app,
        Method::POST,
        "/api/v1/jobs/bulk",
        Some(json!({
            "action": "cancel", "selector": {}
        })),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "empty selector: {v}");
    for i in 0..3 {
        call(&app, Method::POST, "/api/v1/jobs", Some(json!({
            "kind": "p4:bulk", "payload": b64("{}"), "queue": "p4-bulk", "id": format!("p4-b{i}")
        }))).await;
    }
    let (st, v) = call(
        &app,
        Method::POST,
        "/api/v1/jobs/bulk",
        Some(json!({
            "action": "cancel", "selector": {"queue": "p4-bulk"}, "dry_run": true
        })),
    )
    .await;
    assert_eq!(st, StatusCode::ACCEPTED);
    assert_eq!(v["status"], "completed");
    assert_eq!(v["total_estimated"], 3, "{v}");
    let (st, v) = call(
        &app,
        Method::POST,
        "/api/v1/jobs/bulk",
        Some(json!({
            "action": "cancel", "selector": {"queue": "p4-bulk"}
        })),
    )
    .await;
    assert_eq!(st, StatusCode::ACCEPTED);
    let op_id = v["id"].as_str().unwrap().to_string();
    // Drive the executor directly (in production the "operations" duty does this).
    store.run_pending_operations(1000).await.unwrap();
    let (_, v) = call(
        &app,
        Method::GET,
        &format!("/api/v1/operations/{op_id}"),
        None,
    )
    .await;
    assert_eq!(v["status"], "completed", "{v}");
    assert_eq!(v["affected"], 3);
    let (_, v) = call(&app, Method::GET, "/api/v1/jobs/counts?queue=p4-bulk", None).await;
    assert_eq!(v["counts"]["cancelled"], 3, "{v}");

    // The q search grammar: field terms AND a bare kind-prefix term.
    call(
        &app,
        Method::POST,
        "/api/v1/jobs",
        Some(json!({
            "kind": "p4:search", "payload": b64("{}"), "queue": "p4-q", "id": "p4-s1", "priority": 7
        })),
    )
    .await;
    let (_, v) = call(
        &app,
        Method::GET,
        "/api/v1/jobs?q=queue:p4-q%20priority:7",
        None,
    )
    .await;
    assert_eq!(v["jobs"].as_array().unwrap().len(), 1, "{v}");
    assert_eq!(v["jobs"][0]["id"], "p4-s1");
    // A bare term (no colon) is a kind prefix. Kinds that CONTAIN colons need the
    // explicit kind: field — a colon term always parses as field:value.
    let (_, v) = call(&app, Method::GET, "/api/v1/jobs?q=p4&queue=p4-q", None).await;
    assert!(
        v["jobs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|j| j["id"] == "p4-s1"),
        "bare term is a kind prefix: {v}"
    );
    let (_, v) = call(&app, Method::GET, "/api/v1/jobs?q=kind:p4:search", None).await;
    assert_eq!(
        v["jobs"].as_array().unwrap().len(),
        1,
        "kind: takes colon values: {v}"
    );
    let (st, _) = call(&app, Method::GET, "/api/v1/jobs?q=bogus:x", None).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // Reschedule moves a scheduled job's run time; payload edit changes the
    // fingerprint with the payload.
    call(
        &app,
        Method::POST,
        "/api/v1/jobs",
        Some(json!({
            "kind": "p4:later", "payload": b64("{\"v\":1}"), "queue": "p4-q", "id": "p4-r1",
            "scheduled_at_ms": 99999999999999i64
        })),
    )
    .await;
    let (st, _) = call(
        &app,
        Method::POST,
        "/api/v1/jobs/p4-r1/reschedule",
        Some(json!({ "scheduled_at_ms": 12345 })),
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    let (_, v) = call(&app, Method::GET, "/api/v1/jobs/p4-r1", None).await;
    assert_eq!(v["scheduled_at_ms"], 12345);
    let old_fp = v["fingerprint"].as_str().unwrap().to_string();
    let (st, _) = call(
        &app,
        Method::PUT,
        "/api/v1/jobs/p4-r1/payload",
        Some(json!({ "payload": b64("{\"v\":2}") })),
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    let (_, v) = call(
        &app,
        Method::GET,
        "/api/v1/jobs/p4-r1?include_payload=true",
        None,
    )
    .await;
    assert_eq!(v["payload"], b64("{\"v\":2}"));
    assert_ne!(
        v["fingerprint"].as_str().unwrap(),
        old_fp,
        "fingerprint follows the payload"
    );

    // Workers: a heartbeat registers; the listing shows it.
    store
        .heartbeat_worker(&headgate_core::WorkerMeta {
            worker_id: "p4-w1".into(),
            host: "testhost".into(),
            pid: 42,
            queues: vec!["p4-q".into()],
            concurrency: 8,
            started_at_ms: 1,
            heartbeat_at_ms: 0,
            // round 32: the cluster view's / backlog metrics's additive beat payload.
            inflight: 6,
            polls: 10,
            empty_polls: 2,
            status: "running".into(),
            duties_active: true,
            pending_command: None,
        })
        .await
        .unwrap();
    let (_, v) = call(&app, Method::GET, "/api/v1/workers", None).await;
    assert!(
        v.as_array()
            .unwrap()
            .iter()
            .any(|w| w["worker_id"] == "p4-w1"),
        "{v}"
    );

    // surveyed policy behavior the CLUSTER VIEW (round 32). The operational answer this endpoint exists
    // for is "which queues have ZERO live workers" — asserted here in both directions:
    // p4-q is covered by the worker just registered, and p4-bulk (which has jobs and
    // no worker) is PRESENT with live_workers = 0 rather than simply missing.
    //
    // STATE assertions, not global counts: this database is shared with the other test
    // binaries, which run real workers that register real rows. So the fleet totals are
    // asserted as ">= this worker's contribution" and the exact aggregation arithmetic
    // is pinned where it can be deterministic — scripts/test-admission.sh seeds a fixed
    // two-worker registry and byte-diffs /cluster across both language servers.
    // Round 32h: the fleet totals used to be `>= 8` / `>= 6`, which SIBLING binaries
    // satisfy on their own — runtime.rs registers a capacity-4 worker with inflight > 0,
    // bounded_pool.rs a capacity-6 one, signals.rs another — so deleting the
    // heartbeat_worker call above would not have failed them. The check that cannot be
    // satisfied by someone else's row is per-worker: find p4-w1 in the listing and pin
    // ITS numbers, then assert the fleet totals are at least that AND at least as large
    // as the listing they aggregate. `is_number()` is dropped: it passes for 0, so the
    // aggregation arithmetic was not checked at all.
    let (_, ws) = call(&app, Method::GET, "/api/v1/workers", None).await;
    let mine = ws
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["worker_id"] == "p4-w1")
        .unwrap_or_else(|| panic!("p4-w1 must be listed: {ws}"));
    assert_eq!(mine["concurrency"], 8, "{mine}");
    assert_eq!(mine["inflight"], 6, "{mine}");
    assert_eq!(mine["status"], "running", "{mine}");
    assert_eq!(mine["duties_active"], true, "{mine}");
    assert!(mine["pending_command"].is_null(), "{mine}");
    let live: i64 = ws.as_array().unwrap().len() as i64;
    let (_, v) = call(&app, Method::GET, "/api/v1/cluster", None).await;
    assert_eq!(
        v["workers"]["live"].as_i64().unwrap(),
        live,
        "/cluster's live count must equal the /workers listing it aggregates: {v} {ws}"
    );
    assert!(v["capacity_total"].as_i64().unwrap() >= 8, "{v}");
    assert!(v["inflight_total"].as_i64().unwrap() >= 6, "{v}");
    // utilization is inflight/capacity over LIVE workers — a ratio of sums, so it is
    // strictly positive here (p4-w1 alone contributes 6 of its 8) and never > 1.
    let util = v["utilization"].as_f64().unwrap();
    assert!(
        util > 0.0 && util <= 1.0,
        "utilization must be a real ratio of sums: {v}"
    );
    assert!(v["empty_poll_ratio"].as_f64().unwrap() >= 0.0, "{v}");
    let qs = v["queues"].as_array().unwrap();
    let covered = qs
        .iter()
        .find(|q| q["queue"] == "p4-q")
        .expect("p4-q must be listed");
    assert!(covered["live_workers"].as_i64().unwrap() >= 1, "{v}");
    let uncovered = qs
        .iter()
        .find(|q| q["queue"] == "p4-bulk")
        .expect("a queue with no live worker must be LISTED, not omitted");
    assert_eq!(uncovered["live_workers"], 0, "{v}");

    // Schedule delete.
    let (st, _) = call(&app, Method::DELETE, "/api/v1/periodic/p4-daily", None).await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    let (st, _) = call(&app, Method::DELETE, "/api/v1/periodic/p4-daily", None).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn sse_events_stream_queue_activity() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping SSE test");
        return;
    };
    // connect() carries LISTEN config, so this store is Notifying — the SSE feed.
    let store = Arc::new(PgStore::connect(&conninfo, 4).expect("connect"));
    let inspect: Arc<dyn Inspect> = store.clone();
    let app = router(inspect, ApiConfig::default());

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );

    // Feed enqueues until the subscription (established when the body is first polled)
    // observes one — the poll-fallback contract again: missed pushes cost latency only.
    let feeder = {
        let store = store.clone();
        tokio::spawn(async move {
            for i in 0..40u32 {
                let _ = store
                    .enqueue(&[headgate_core::Envelope {
                        id: format!("sse-{}-{i}", std::process::id()),
                        kind: "sse".into(),
                        payload: vec![0],
                        queue: "sse-q".into(),
                        fingerprint: "fp-sse".into(),
                        scheduled_at_ms: 1,
                        ..Default::default()
                    }])
                    .await;
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            }
        })
    };

    use http_body_util::BodyExt;
    let mut body = resp.into_body();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(8);
    let mut seen = String::new();
    let got = loop {
        let frame = tokio::time::timeout_at(deadline, body.frame())
            .await
            .expect("an SSE event within 8s")
            .expect("stream open")
            .expect("frame ok");
        if let Some(data) = frame.data_ref() {
            seen.push_str(&String::from_utf8_lossy(data));
            if seen.contains("queue_activity") && seen.contains("sse-q") {
                break true;
            }
        }
    };
    feeder.abort();
    assert!(
        got,
        "expected a queue_activity event naming sse-q, saw: {seen}"
    );
}

/// authorization boundary read-only mode: every mutating route 403s with one message; GETs still serve.
#[tokio::test]
async fn read_only_mode_rejects_mutations() {
    use http_body_util::BodyExt;
    use tower::util::ServiceExt;
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping read-only test");
        return;
    };
    let store = std::sync::Arc::new(headgate_postgres::PgStore::connect(&conninfo, 2).unwrap());
    let inspect: std::sync::Arc<dyn headgate_core::Inspect> = store;
    let app = headgate_api::router(
        inspect,
        headgate_api::ApiConfig {
            read_only: true,
            ..Default::default()
        },
    );
    let res = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/api/v1/queues/ro-q/pause")
                .header("Idempotency-Key", "ro-1")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), 403);
    let body =
        String::from_utf8(res.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    assert!(body.contains("read-only mode"), "{body}");
    let res = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/v1/meta")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "GETs still serve in read-only mode");
}

// ---------------------------------------------------------------------------
// ROUND 32L, TASK 3.3 — GET /cluster, END TO END from a REAL worker's polling.
//
// The autoscaling row's NOTE recorded the gap exactly: "The /cluster fixtures still write
// polls/empty_polls DIRECTLY, so the API's AGGREGATION over several workers and the ring
// are proven separately, never end to end." Two halves that both pass while the WIRE
// between them is cut — nothing asserted that the numbers /cluster reports ever came from
// a worker that actually polled anything.
//
// This runs the real `Worker` loop against the live store, with nothing written by hand:
// the ring fills from real admissions, the real heartbeat copies it into
// `headgate_worker`, and the assertions read it back out of the HTTP response.
//
// THE DISCRIMINATING FACT is the ring BOUND. The worker below polls many hundreds of
// times; `POLL_WINDOW` is 128, so a rolling window reports 128 and a LIFETIME counter —
// which is what this was before round 32k, and what a regression would restore — reports
// the lifetime total. That difference is what an operator sees as "shrink the fleet"
// while the fleet is saturated.
//
// FLEET TOTALS AND CONCURRENT TESTS. `/cluster` sums every worker the store calls live
// (15 minutes), and sibling test binaries legitimately register their own. So the fleet
// assertions are an IDENTITY against the live set read at the same moment rather than an
// absolute number — and the per-worker assertions, which are the end-to-end claim, are
// scoped to this test's worker id. Nothing here deletes another test's row.
#[tokio::test]
async fn a_real_workers_polling_is_the_number_that_reaches_cluster() {
    use headgate::{Registry, Worker, WorkerConfig};
    use headgate_core::{Envelope, Task};

    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping cluster end-to-end test");
        return;
    };
    const WID: &str = "t33l-w1";
    const Q: &str = "t33l-q";

    struct Noop;
    impl Task for Noop {
        const TYPE: &'static str = "t33l:noop";
        fn encode(&self) -> Result<Vec<u8>, headgate_core::CodecError> {
            Ok(vec![])
        }
        fn decode(_: &[u8]) -> Result<Self, headgate_core::CodecError> {
            Ok(Noop)
        }
    }

    let store = Arc::new(PgStore::connect(&conninfo, 6).expect("connect"));
    {
        let tx = store.begin().await.unwrap();
        tx.client()
            .unwrap()
            .execute("DELETE FROM headgate_job WHERE queue = $1", &[&Q])
            .await
            .unwrap();
        tx.client()
            .unwrap()
            .execute("DELETE FROM headgate_worker WHERE worker_id = $1", &[&WID])
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let app = router(store.clone() as Arc<dyn Inspect>, ApiConfig::default());

    let mut reg = Registry::new();
    reg.register::<Noop, _, _>(|_ctx, _t| async move { Ok(()) })
        .unwrap();
    let cfg = WorkerConfig {
        queues: vec![Q.into()],
        capacity: 1,
        // heartbeat = lease / 3 = 100ms, so the ring reaches the registry promptly.
        lease: std::time::Duration::from_millis(300),
        poll: headgate::BackoffConfig {
            floor: std::time::Duration::from_millis(1),
            ceiling: std::time::Duration::from_millis(5),
            multiplier: 2.0,
            jitter: 0.0,
        },
        // This worker must not sweep other tests' jobs; it is here to POLL.
        run_duties: false,
        worker_id: Some(WID.into()),
        ..Default::default()
    };
    let (worker, handle) = Worker::new(store.clone(), reg, cfg);
    let running = tokio::spawn(worker.run());

    // A bounded wait on an OBSERVABLE store value, never a fixed sleep.
    async fn wait_worker(
        store: &Arc<PgStore>,
        wid: &str,
        secs: u64,
        cond: impl Fn(&headgate_core::WorkerMeta) -> bool,
    ) -> headgate_core::WorkerMeta {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
        loop {
            let ws = store.list_workers(900_000).await.unwrap();
            if let Some(w) = ws.into_iter().find(|w| w.worker_id == wid)
                && cond(&w)
            {
                return w;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "worker {wid} never reached the condition"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    // ---- PHASE 1: idle. The queue is empty, so every admission comes back with zero.
    let idle = wait_worker(&store, WID, 30, |w| w.polls >= 128).await;
    assert_eq!(
        idle.polls, 128,
        "the ring is BOUNDED at POLL_WINDOW and that bound must survive the trip into the \
         registry: this worker has polled far more than 128 times, so a lifetime counter \
         would report thousands here and tell an operator to shrink a fleet on ancient news"
    );
    assert_eq!(
        idle.empty_polls, 128,
        "an idle worker's whole window is empty polls"
    );

    // ...and the HTTP surface reports the fleet sum over the live set, mine included.
    let (st, body) = call(&app, Method::GET, "/api/v1/cluster", None).await;
    assert_eq!(st, StatusCode::OK);
    let live = store.list_workers(900_000).await.unwrap();
    let sum_polls: i64 = live.iter().map(|w| w.polls as i64).sum();
    let sum_empty: i64 = live.iter().map(|w| w.empty_polls as i64).sum();
    assert!(
        sum_polls >= 128,
        "the live set must include this worker's 128"
    );
    assert_eq!(
        body["polls_total"].as_i64().unwrap(),
        sum_polls,
        "polls_total is the SUM over live workers — the numbers real workers reported"
    );
    assert_eq!(body["empty_polls_total"].as_i64().unwrap(), sum_empty);
    // backlog metrics: a ratio of SUMS, never a mean of per-worker ratios.
    let want_ratio = sum_empty as f64 / sum_polls as f64;
    assert!(
        (body["empty_poll_ratio"].as_f64().unwrap() - want_ratio).abs() < 1e-9,
        "empty_poll_ratio must be empty_polls_total / polls_total; got {} want {want_ratio}",
        body["empty_poll_ratio"]
    );
    assert!(
        body["queues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|q| q["queue"] == Q && q["live_workers"].as_i64().unwrap() >= 1),
        "the queue this real worker serves must show live coverage: {}",
        body["queues"]
    );

    // ---- PHASE 2: load it. Now the SAME ring must roll the empty polls out, and the
    // change must reach /cluster. A lifetime counter would still be reporting ~1.0 here,
    // which is the exact operational lie backlog metrics exists to prevent.
    let jobs: Vec<Envelope> = (0..300)
        .map(|i| {
            headgate::prepare_envelope(Envelope {
                id: format!("t33l-{i}"),
                kind: Noop::TYPE.into(),
                payload: vec![],
                queue: Q.into(),
                fingerprint: headgate_core::fingerprint(Noop::TYPE, format!("{i}").as_bytes()),
                scheduled_at_ms: 1,
                retention_ms: 0, // ephemeral: keep the table clean behind us
                ..Default::default()
            })
            .unwrap()
        })
        .collect();
    store.enqueue(&jobs).await.unwrap();

    let busy = wait_worker(&store, WID, 60, |w| w.empty_polls < 32).await;
    assert_eq!(
        busy.polls, 128,
        "the window stays exactly POLL_WINDOW wide under load — it rolls, it does not grow"
    );
    assert!(
        busy.empty_polls < 32,
        "a saturated worker's window must have rolled its idle bits out; got {}/{}",
        busy.empty_polls,
        busy.polls
    );
    let (st, busy_body) = call(&app, Method::GET, "/api/v1/cluster", None).await;
    assert_eq!(st, StatusCode::OK);
    assert!(
        busy_body["empty_polls_total"].as_i64().unwrap() <= sum_empty - 96,
        "the fleet's empty-poll total must fall by this worker's ~96 rolled-out bits: \
         idle={} busy={}",
        sum_empty,
        busy_body["empty_polls_total"]
    );

    handle.shutdown();
    let _ = running.await.expect("worker task");
}
