use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use tower::util::ServiceExt;

async fn get(router: axum::Router, path: &str) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
    let response = router
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    (status, headers, body)
}

#[tokio::test]
async fn serves_shell_fallback_with_injected_config() {
    let cfg = headgate_ui::Config {
        api_base: "/x/api".into(),
        read_only: true,
    };
    for path in ["/", "/some/deep/link"] {
        let (status, headers, body) = get(headgate_ui::router(cfg.clone()), path).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[header::CONTENT_TYPE], "text/html; charset=utf-8");
        let body = String::from_utf8(body).unwrap();
        assert!(body.contains("headgate console"));
        assert!(body.contains(r#"window.HEADGATE = {"apiBase":"/x/api","readOnly":true};"#));
        assert!(
            body.contains("./assets/"),
            "assets must work below a mount path"
        );
    }
}

#[tokio::test]
async fn serves_hashed_assets_with_immutable_caching() {
    let (_, _, index) = get(headgate_ui::router(Default::default()), "/").await;
    let index = String::from_utf8(index).unwrap();
    let start = index.find("./assets/").unwrap() + 1;
    let end = index[start..].find(['\"', '\'']).unwrap() + start;
    let asset = &index[start..end];
    let (status, headers, body) = get(headgate_ui::router(Default::default()), asset).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body.is_empty());
    assert_eq!(
        headers[header::CACHE_CONTROL],
        "public, max-age=31536000, immutable"
    );
}

#[tokio::test]
async fn missing_asset_does_not_fall_back_to_html() {
    let (status, _, _) = get(
        headgate_ui::router(Default::default()),
        "/assets/not-built.js",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn injected_config_cannot_close_its_script_element() {
    let (_, _, body) = get(
        headgate_ui::router(headgate_ui::Config {
            api_base: "</script><script>alert(1)</script>".into(),
            read_only: false,
        }),
        "/",
    )
    .await;
    let body = String::from_utf8(body).unwrap();
    assert!(!body.contains("</script><script>alert(1)</script>"));
    assert!(body.contains(r#"\u003c/script\u003e"#));
}
