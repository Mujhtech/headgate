use std::sync::Arc;
use std::time::Duration;

use headgate_core::{Envelope, Inspect, Store, StoreError};
use headgate_redis::RedisStore;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{ConnectionAddr, RedisResult};
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

type Upstream = (String, u16);

struct CuttableProxy {
    addr: std::net::SocketAddr,
    accept: JoinHandle<()>,
    connections: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl CuttableProxy {
    async fn start(upstream: Upstream, addr: Option<std::net::SocketAddr>) -> Self {
        let listener = TcpListener::bind(addr.unwrap_or_else(|| "127.0.0.1:0".parse().unwrap()))
            .await
            .expect("bind fault proxy");
        let addr = listener.local_addr().expect("proxy address");
        let connections = Arc::new(Mutex::new(Vec::new()));
        let seen = connections.clone();
        let accept = tokio::spawn(async move {
            while let Ok((mut downstream, _)) = listener.accept().await {
                let upstream = upstream.clone();
                let connection = tokio::spawn(async move {
                    if let Ok(mut upstream) = TcpStream::connect(upstream).await {
                        let _ = copy_bidirectional(&mut downstream, &mut upstream).await;
                    }
                });
                seen.lock().await.push(connection);
            }
        });
        Self {
            addr,
            accept,
            connections,
        }
    }

    async fn cut(self) -> std::net::SocketAddr {
        self.accept.abort();
        let _ = self.accept.await;
        let mut connections = self.connections.lock().await;
        for connection in connections.drain(..) {
            connection.abort();
            let _ = connection.await;
        }
        self.addr
    }
}

async fn manager(client: redis::Client) -> RedisResult<ConnectionManager> {
    ConnectionManager::new_with_config(
        client,
        ConnectionManagerConfig::new()
            .set_number_of_retries(0)
            .set_connection_timeout(Duration::from_millis(500))
            .set_response_timeout(Duration::from_secs(1)),
    )
    .await
}

#[tokio::test]
async fn enqueue_classifies_a_cut_redis_connection_and_never_buffers_the_job() {
    let Ok(url) = std::env::var("HG_TEST_REDIS") else {
        eprintln!("HG_TEST_REDIS not set; skipping redis outage test");
        return;
    };
    let upstream_client = redis::Client::open(url).expect("redis url");
    let upstream = match &upstream_client.get_connection_info().addr {
        ConnectionAddr::Tcp(host, port) => (host.clone(), *port),
        other => panic!("outage proxy requires a plain TCP Redis test server, got {other:?}"),
    };
    let proxy = CuttableProxy::start(upstream.clone(), None).await;
    let mut proxied_info = upstream_client.get_connection_info().clone();
    proxied_info.addr = ConnectionAddr::Tcp("127.0.0.1".into(), proxy.addr.port());
    let proxied_client = redis::Client::open(proxied_info).expect("proxied client");
    let conn = manager(proxied_client)
        .await
        .expect("connect through proxy");
    let prefix = format!("outage:{}", std::process::id());
    let store = RedisStore::new(conn, &prefix);

    let addr = proxy.cut().await;
    let valid = Envelope {
        id: "redis-outage-lost".into(),
        kind: "outage".into(),
        ..Default::default()
    };
    let err = store
        .enqueue(std::slice::from_ref(&valid))
        .await
        .expect_err("cut connection must fail");
    assert!(
        matches!(err, StoreError::Unavailable(_)),
        "cut enqueue must be typed unavailable, got {err:?}"
    );

    let mut invalid = valid.clone();
    invalid.id.clear();
    let err = store
        .enqueue(&[invalid])
        .await
        .expect_err("invalid envelope must fail");
    assert!(
        matches!(err, StoreError::Invalid(_)),
        "invalid envelope while down changed taxonomy: {err:?}"
    );
    let err = store
        .enqueue(&[valid.clone(), valid])
        .await
        .expect_err("duplicate id must fail");
    assert!(
        matches!(err, StoreError::IdConflict { .. }),
        "duplicate id while down changed taxonomy: {err:?}"
    );

    // Bring the SAME endpoint back. The next enqueue is accepted, but the rejected
    // one is absent: neither the adapter nor its connection manager retained work.
    let recovered = CuttableProxy::start(upstream, Some(addr)).await;
    store
        .enqueue(&[Envelope {
            id: "redis-outage-kept".into(),
            kind: "outage".into(),
            ..Default::default()
        }])
        .await
        .expect("enqueue after recovery");
    assert!(
        store
            .get_job("redis-outage-lost", false)
            .await
            .expect("inspect rejected id")
            .is_none(),
        "the failed enqueue was replayed after recovery"
    );
    assert!(
        store
            .get_job("redis-outage-kept", false)
            .await
            .expect("inspect accepted id")
            .is_some(),
        "the post-recovery enqueue did not land"
    );

    recovered.cut().await;
    let mut direct = manager(upstream_client)
        .await
        .expect("direct cleanup client");
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{prefix}:*"))
        .query_async(&mut direct)
        .await
        .expect("list fixture keys");
    if !keys.is_empty() {
        let _: usize = redis::cmd("DEL")
            .arg(keys)
            .query_async(&mut direct)
            .await
            .expect("delete fixture keys");
    }
}
