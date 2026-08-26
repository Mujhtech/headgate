//! Serve the control API contract control API — the router speaks only to the Inspect port, so the
//! same binary fronts any backend.
//!   HG_STORE = "pg" (default) | "redis" | "mysql"
//!   HG_PG = conninfo (pg), HG_REDIS = url + HG_REDIS_PREFIX (redis), HG_MYSQL = url
//!   HG_API_ADDR = listen address (default 127.0.0.1:8091)

use std::sync::Arc;

use headgate_api::{ApiConfig, router};
use headgate_core::Inspect;
use headgate_mysql::MysqlStore;
use headgate_postgres::PgStore;
use headgate_redis::RedisStore;

#[tokio::main]
async fn main() {
    let addr = std::env::var("HG_API_ADDR").unwrap_or_else(|_| "127.0.0.1:8091".into());
    let backend = std::env::var("HG_STORE").unwrap_or_else(|_| "pg".into());
    // what GET /meta reports. It was `ApiConfig::default()`'s hardcoded
    // "postgres" on every backend, so /meta claimed postgres while fronting Redis or
    // MySQL — and the control API contract byte diff could not see it, because both servers were wrong
    // identically. Derived from the SAME string that selects the store below.
    let meta_backend: &'static str = match backend.as_str() {
        "redis" => "redis",
        "mysql" => "mysql",
        _ => "postgres",
    };
    let inspect: Arc<dyn Inspect> = match backend.as_str() {
        "pg" => {
            let conninfo = std::env::var("HG_PG")
                .unwrap_or_else(|_| "host=/tmp port=5432 user=postgres dbname=hg".into());
            Arc::new(PgStore::connect(&conninfo, 8).expect("connect pg"))
        }
        "redis" => {
            let url = std::env::var("HG_REDIS").unwrap_or_else(|_| "redis://127.0.0.1:6380".into());
            let prefix = std::env::var("HG_REDIS_PREFIX").unwrap_or_else(|_| "hg".into());
            Arc::new(
                RedisStore::connect(&url, prefix)
                    .await
                    .expect("connect redis"),
            )
        }
        "mysql" => {
            let url = std::env::var("HG_MYSQL")
                .unwrap_or_else(|_| "mysql://root:hg@127.0.0.1:3307/hg".into());
            Arc::new(MysqlStore::connect(&url).expect("connect mysql"))
        }
        other => panic!("HG_STORE must be pg, redis, or mysql, got `{other}`"),
    };
    let read_only = std::env::var("HG_READ_ONLY").ok().as_deref() == Some("1");
    // authorization boundary an unauthenticated queue console reachable beyond loopback is a breach
    // waiting for a port scan. Failing to start is the correct behavior.
    let loopback =
        addr.starts_with("127.") || addr.starts_with("localhost") || addr.starts_with("[::1]");
    if !loopback && std::env::var("HG_API_ALLOW_REMOTE").ok().as_deref() != Some("1") {
        panic!(
            "refusing to bind {addr}: no authentication ships with this binary (authorization boundary). \
             Put it behind your own auth and set HG_API_ALLOW_REMOTE=1, or bind loopback."
        );
    }
    let app = router(
        inspect,
        ApiConfig {
            read_only,
            backend: meta_backend,
            ..ApiConfig::default()
        },
    )
    // embedded console contract/embeddable-console boundary the embedded console, at /admin, speaking the co-mounted API.
    .nest_service(
        "/admin",
        headgate_ui::router(headgate_ui::Config {
            api_base: "/api/v1".into(),
            read_only,
        }),
    );
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");
    eprintln!("hg-api ({backend}) listening on {addr} — console at http://{addr}/admin");
    axum::serve(listener, app).await.expect("serve");
}
