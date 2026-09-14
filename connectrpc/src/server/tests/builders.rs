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

    assert_eq!(server.service.limits().max_request_body_size(), 1024);
    assert_eq!(server.service.limits().max_message_size(), 512);
    assert!(!server.http1_keep_alive);
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
