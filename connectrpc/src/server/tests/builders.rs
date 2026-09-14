//! `Server` / `BoundServer` builder plumbing.

use super::*;

#[test]
fn test_server_creation() {
    let router = Router::new();
    let _server = Server::new(router);
}

/// `Server` proxies the dispatch-config builders so users don't have to
/// drop down to `Server::from_service(ConnectRpcService::new(...).with_*())`.
/// Exercises the chain and verifies the readable knobs (`limits()`, the
/// `http1_keep_alive` field) round-trip; the compression knobs have no
/// public read path so the test only confirms the builders compile and
/// chain.
#[test]
fn test_server_dispatch_config_proxies() {
    use crate::service::Limits;
    use crate::{CompressionPolicy, CompressionRegistry};

    let limits = Limits::default()
        .with_max_request_body_size(1024)
        .with_max_message_size(512);
    let server = Server::new(Router::new())
        .with_limits(limits)
        .with_compression(CompressionRegistry::default())
        .with_compression_policy(CompressionPolicy::default().with_min_size(8192))
        .with_http1_keep_alive(false);

    assert_eq!(server.service().limits().max_request_body_size(), 1024);
    assert_eq!(server.service().limits().max_message_size(), 512);
    assert!(!server.connection_config().http1_keep_alive());
}

/// `Server::with_interceptor` / `with_interceptor_arc` must reach the
/// underlying `ConnectRpcService` chain. The interceptor list has no
/// public read path, so the test pins delegation through `Arc` strong
/// counts: registering a shared `Arc<dyn Interceptor>` on the `Server`
/// must bump the count exactly as registering it on the service
/// directly would, and dropping the `Server` must release it.
#[test]
fn test_server_interceptor_proxies() {
    struct Noop;
    #[async_trait::async_trait]
    impl crate::Interceptor for Noop {}

    let shared: Arc<dyn crate::Interceptor> = Arc::new(Noop);
    assert_eq!(Arc::strong_count(&shared), 1);

    let server = Server::new(Router::new())
        // `with_interceptor` Arc::new()s internally; only proves the
        // proxy compiles and chains.
        .with_interceptor(Noop)
        // `with_interceptor_arc` must store a clone of `shared`.
        .with_interceptor_arc(Arc::clone(&shared));
    assert_eq!(
        Arc::strong_count(&shared),
        2,
        "Server::with_interceptor_arc must reach the underlying service"
    );

    drop(server);
    assert_eq!(Arc::strong_count(&shared), 1);
}

/// Every constructor starts from the default configs, every `with_*`
/// shorthand on `Server` and `BoundServer` edits the field of the same name,
/// and one `ConnectionConfig` / `AcceptConfig` value crosses between the two.
#[tokio::test]
async fn setters_forward_to_the_configs_and_configs_cross_between_server_and_bound_server() {
    async fn listener() -> TcpListener {
        TcpListener::bind("127.0.0.1:0").await.unwrap()
    }
    assert_eq!(
        Server::new(Router::new()).connection_config(),
        &ConnectionConfig::default()
    );
    assert_eq!(
        Server::from_service(ConnectRpcService::new(Router::new())).connection_config(),
        &ConnectionConfig::default()
    );
    assert_eq!(
        Server::from_listener(listener().await).connection_config(),
        &ConnectionConfig::default()
    );
    assert_eq!(
        Server::bind("127.0.0.1:0")
            .await
            .unwrap()
            .connection_config(),
        &ConnectionConfig::default()
    );

    let expected = ConnectionConfig::new()
        .with_http1_keep_alive(false)
        .with_header_read_timeout(Duration::from_secs(2))
        .with_http2_initial_stream_window_size(512 * 1024)
        .with_http2_initial_connection_window_size(1024 * 1024)
        .with_max_concurrent_streams(64)
        .with_http2_keepalive_interval(Duration::from_secs(30))
        .with_http2_keepalive_timeout(Duration::from_secs(5))
        .with_max_connection_age(Duration::from_secs(600))
        .with_max_connection_age_grace(Duration::from_secs(3))
        .with_max_connection_idle(Duration::from_secs(60))
        .with_max_requests_per_connection(NonZeroU64::new(100).unwrap());
    assert!(
        !expected.http2_adaptive_window(),
        "explicit windows turn adaptive off"
    );

    let server = Server::new(Router::new())
        .with_http1_keep_alive(false)
        .with_header_read_timeout(Duration::from_secs(2))
        .with_http2_initial_stream_window_size(512 * 1024)
        .with_http2_initial_connection_window_size(1024 * 1024)
        .with_max_concurrent_streams(64)
        .with_http2_keepalive_interval(Duration::from_secs(30))
        .with_http2_keepalive_timeout(Duration::from_secs(5))
        .with_max_connection_age(Duration::from_secs(600))
        .with_max_connection_age_grace(Duration::from_secs(3))
        .with_max_connection_idle(Duration::from_secs(60))
        .with_max_requests_per_connection(NonZeroU64::new(100).unwrap());
    assert_eq!(server.connection_config(), &expected);
    assert!(
        server
            .with_http2_adaptive_window(true)
            .connection_config()
            .http2_adaptive_window()
    );

    let bound = Server::from_listener(listener().await)
        .with_http1_keep_alive(false)
        .with_header_read_timeout(Duration::from_secs(2))
        .with_http2_initial_stream_window_size(512 * 1024)
        .with_http2_initial_connection_window_size(1024 * 1024)
        .with_max_concurrent_streams(64)
        .with_http2_keepalive_interval(Duration::from_secs(30))
        .with_http2_keepalive_timeout(Duration::from_secs(5))
        .with_max_connection_age(Duration::from_secs(600))
        .with_max_connection_age_grace(Duration::from_secs(3))
        .with_max_connection_idle(Duration::from_secs(60))
        .with_max_requests_per_connection(NonZeroU64::new(100).unwrap());
    assert_eq!(bound.connection_config(), &expected);
    assert!(
        bound
            .with_http2_adaptive_window(true)
            .connection_config()
            .http2_adaptive_window()
    );

    // One value crosses from a `Server` to a `BoundServer` and back.
    let server = Server::new(Router::new()).with_connection_config(expected.clone());
    let bound = Server::from_listener(listener().await)
        .with_connection_config(server.connection_config().clone())
        .with_accept_config(server.accept_config().clone());
    assert_eq!(bound.connection_config(), &expected);
    assert!(!bound.accept_config().is_tls());
    let server =
        Server::new(Router::new()).with_connection_config(bound.connection_config().clone());
    assert_eq!(server.connection_config(), &expected);

    #[cfg(feature = "server-tls")]
    {
        let (server_cfg, _, _) = pki();
        let accept = AcceptConfig::new()
            .with_tls(Arc::clone(&server_cfg))
            .with_tls_handshake_timeout(Duration::from_secs(3));
        let server = Server::new(Router::new()).with_accept_config(accept.clone());
        assert!(server.accept_config().is_tls());
        let bound = Server::from_listener(listener().await).with_accept_config(accept);
        assert_eq!(
            bound.accept_config().tls_handshake_timeout(),
            Duration::from_secs(3)
        );
        // The TLS shorthands edit the same value.
        let bound = Server::from_listener(listener().await)
            .with_tls(server_cfg)
            .with_tls_handshake_timeout(Duration::from_secs(7));
        assert!(bound.accept_config().is_tls());
        assert_eq!(
            bound.accept_config().tls_handshake_timeout(),
            Duration::from_secs(7)
        );
    }
}

/// `Server::serve_connection` serves the caller's stream with the server's
/// own service and stamps requests from the `ConnectionInfo` it was handed.
#[tokio::test]
async fn server_serve_connection_uses_the_servers_service() {
    let captured: Arc<Mutex<Option<u16>>> = Arc::new(Mutex::new(None));
    let seen = Arc::clone(&captured);
    let router = Router::new().route(
        "svc",
        "Echo",
        crate::handler_fn(
            move |ctx: crate::RequestContext, _req: buffa_types::Empty| {
                let seen = Arc::clone(&seen);
                async move {
                    *seen.lock().unwrap() = ctx.peer_addr().map(|peer| peer.port());
                    crate::Response::ok(buffa_types::Empty::default())
                }
            },
        ),
    );
    let server = Server::new(router);

    let (mut client_io, server_io) = tokio::io::duplex(64 << 10);
    let info = ConnectionInfo::new().with_peer_addr("127.0.0.1:4242".parse().unwrap());
    let conn = tokio::spawn(server.serve_connection(server_io, info, std::future::pending()));
    client_io.write_all(ECHO_REQ).await.unwrap();
    let resp = read_http1_response(&mut client_io).await;
    assert!(resp.starts_with(b"HTTP/1.1 200"));
    drop(client_io);
    conn.await.unwrap();
    assert_eq!(captured.lock().unwrap().take(), Some(4242));
}
