//! Embeddable headgate operations console.
//!
//! The same static TanStack Start build is packaged by the Go module. It has no direct
//! store access and uses only the configured HTTP API. Mount the router behind the
//! authentication and authorization already protecting the host application's admin
//! routes:
//!
//! ```ignore
//! let app = Router::new()
//!     .nest("/api/v1", headgate_api::router(store, api_cfg))
//!     .nest_service("/admin/jobs", headgate_ui::router(headgate_ui::Config::default()));
//! ```
//!
//! This crate does not provide authentication. UI read-only mode disables controls for
//! clarity, but the API must also use read-only mode to enforce the restriction.

use axum::Router;
use axum::body::Body;
use axum::http::{StatusCode, Uri, header};
use axum::response::Response;
use axum::routing::get;
use include_dir::{Dir, include_dir};

static ASSETS: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/../../ui/dist");
const DEFAULT_CONFIG: &str =
    r#"window.HEADGATE = window.HEADGATE || {apiBase:"/api/v1",readOnly:false};"#;

#[derive(Clone, Debug)]
pub struct Config {
    /// Browser-visible path where the control API is mounted.
    pub api_base: String,
    /// Disables mutating controls. Pair this with API read-only mode for enforcement.
    pub read_only: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            api_base: "/api/v1".into(),
            read_only: false,
        }
    }
}

/// Returns a router that serves the SPA shell and its content-hashed assets.
pub fn router(cfg: Config) -> Router {
    Router::new().fallback(get(move |uri: Uri| {
        let cfg = cfg.clone();
        async move { serve(uri.path(), &cfg) }
    }))
}

fn serve(path: &str, cfg: &Config) -> Response<Body> {
    let asset_path = path.trim_start_matches('/');
    if !asset_path.is_empty() && asset_path != "index.html" {
        if let Some(file) = ASSETS.get_file(asset_path) {
            return response(
                StatusCode::OK,
                content_type(asset_path),
                "public, max-age=31536000, immutable",
                Body::from(file.contents()),
            );
        }
        if asset_path.starts_with("assets/") {
            return response(
                StatusCode::NOT_FOUND,
                "text/plain; charset=utf-8",
                "no-store",
                Body::from("asset not found"),
            );
        }
    }

    let index = ASSETS
        .get_file("index.html")
        .expect("ui/dist/index.html must be produced by pnpm build");
    let template = std::str::from_utf8(index.contents()).expect("console shell must be UTF-8");
    let config_json = serde_json::json!({ "apiBase": cfg.api_base, "readOnly": cfg.read_only })
        .to_string()
        .replace('&', "\\u0026")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029");
    let injected = format!("window.HEADGATE = {config_json};");
    let page = template.replacen(DEFAULT_CONFIG, &injected, 1);
    response(
        StatusCode::OK,
        "text/html; charset=utf-8",
        "no-cache",
        Body::from(page),
    )
}

fn response(
    status: StatusCode,
    content_type: &'static str,
    cache_control: &'static str,
    body: Body,
) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, cache_control)
        .body(body)
        .expect("static response headers are valid")
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next() {
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("woff2") => "font/woff2",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        _ => "application/octet-stream",
    }
}
