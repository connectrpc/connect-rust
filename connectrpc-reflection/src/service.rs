//! The bridge from a [`Reflector`] to the generated
//! `grpc.reflection.v1.ServerReflection` and
//! `grpc.reflection.v1alpha.ServerReflection` service traits.

use std::sync::Arc;

use connectrpc::Router;

use crate::reflector::Reflector;

/// gRPC-compatible server reflection service backed by a [`Reflector`].
///
/// Implements both the `grpc.reflection.v1` and `grpc.reflection.v1alpha`
/// flavors of the protocol (the messages are structurally identical;
/// older clients — and some current ones, e.g. `grpcurl` fallback paths —
/// still speak `v1alpha`). Register both with [`install`], or register a
/// single version through the generated extension traits.
///
/// ```no_run
/// use connectrpc::Router;
/// use connectrpc_reflection::{Reflector, install};
///
/// // In real code: include_bytes!(concat!(env!("OUT_DIR"), "/app.fds.bin"))
/// # fn descriptor_set_bytes() -> &'static [u8] { &[] }
/// let reflector = Reflector::from_descriptor_set_bytes(descriptor_set_bytes()).unwrap();
/// let router = install(Router::new(), reflector);
/// ```
#[derive(Clone)]
pub struct ReflectionService {
    reflector: Arc<Reflector>,
}

impl ReflectionService {
    /// Wrap a reflector by value; it is moved into a fresh `Arc`.
    #[must_use]
    pub fn new(reflector: Reflector) -> Self {
        Self {
            reflector: Arc::new(reflector),
        }
    }

    /// Wrap a reflector that is already inside an `Arc`.
    #[must_use]
    pub fn from_arc(reflector: Arc<Reflector>) -> Self {
        Self { reflector }
    }
}

/// Register both protocol versions (`v1` and `v1alpha`) on a router.
///
/// This is the recommended setup: clients probe `v1` first and fall back
/// to `v1alpha`, so serving both maximizes compatibility at the cost of
/// two route entries backed by the same index.
///
/// Unlike `connectrpc_health::install_static`, no handle is returned:
/// a [`Reflector`] is immutable once built, so there is nothing to flip
/// at runtime. Both routes are set to [`request_limits`] (16 KiB per
/// request message) via [`apply_request_limits`], which replaces the
/// service-wide limits on those routes even when they are tighter; to tune
/// them, call `apply_request_limits(router, yours)` on the returned router —
/// the later call wins.
#[must_use]
pub fn install(router: Router, reflector: Reflector) -> Router {
    let service = Arc::new(ReflectionService::new(reflector));
    let router = crate::connect::grpc::reflection::v1::ServerReflectionExt::register(
        Arc::clone(&service),
        router,
    );
    let router =
        crate::connect::grpc::reflection::v1alpha::ServerReflectionExt::register(service, router);
    apply_request_limits(router, request_limits())
}

/// The largest request message, after decompression, the reflection routes
/// accept under [`request_limits`]: 16 KiB.
///
/// A `ServerReflectionRequest` carries a host plus one file name, symbol or
/// type name, so a legitimate request is well under a kilobyte; 16 KiB leaves
/// generous headroom while sizing the routes to their actual request profile
/// rather than the general-purpose service-wide default. Reflection is a
/// bidirectional stream, so this bounds each message, not how many a client
/// may send.
pub const MAX_REQUEST_BYTES: usize = 16 * 1024;

/// The per-route [`Limits`](connectrpc::Limits) this crate sets on its
/// routes: message size capped at [`MAX_REQUEST_BYTES`] (and the request
/// body, which only governs non-streaming calls, at that plus the 5-byte
/// envelope), decode budget left at the `connectrpc` default.
#[must_use]
pub fn request_limits() -> connectrpc::Limits {
    connectrpc::Limits::default()
        .with_max_request_body_size(MAX_REQUEST_BYTES + connectrpc::envelope::HEADER_SIZE)
        .with_max_message_size(MAX_REQUEST_BYTES)
}

/// Set whichever of the `v1` and `v1alpha` reflection routes are registered
/// on `router` to `limits`, replacing the service-wide limits for them
/// (whether looser or tighter).
///
/// [`install`] already applies [`request_limits`]. Call this yourself,
/// usually with [`request_limits`], after registering a [`ReflectionService`]
/// through the generated `ServerReflectionExt::register` or
/// [`Router::add_service`](connectrpc::Router::add_service), since those
/// generic registration paths cannot; or call it after either path with your
/// own [`Limits`](connectrpc::Limits) to tune the reflection routes
/// specifically. The later call wins.
///
/// # Panics
///
/// Panics if neither reflection route is registered on `router`.
#[must_use]
pub fn apply_request_limits(mut router: Router, limits: connectrpc::Limits) -> Router {
    let mut applied = false;
    for spec in [
        crate::connect::grpc::reflection::v1::SERVER_REFLECTION_SERVER_REFLECTION_INFO_SPEC,
        crate::connect::grpc::reflection::v1alpha::SERVER_REFLECTION_SERVER_REFLECTION_INFO_SPEC,
    ] {
        if router.has_method(spec.procedure) {
            router = router.with_route_limits(spec.procedure, limits);
            applied = true;
        }
    }
    assert!(
        applied,
        "connectrpc_reflection::apply_request_limits: no reflection route is registered \
         on this router — register `ReflectionService` before applying its limits"
    );
    router
}

/// Implements the generated `ServerReflection` trait for one protocol
/// version. Invoked once per version inside a module that aliases the
/// generated buffa messages as `pb` and the generated connect items as
/// `rpc`; the two versions' messages are field-for-field identical, so
/// the body is shared verbatim.
macro_rules! impl_server_reflection {
    () => {
        impl rpc::ServerReflection for crate::ReflectionService {
            async fn server_reflection_info(
                &self,
                _ctx: ::connectrpc::RequestContext,
                requests: ::connectrpc::ServiceStream<
                    ::connectrpc::StreamMessage<pb::ServerReflectionRequest>,
                >,
            ) -> ::connectrpc::ServiceResult<
                ::connectrpc::ServiceStream<pb::ServerReflectionResponse>,
            > {
                use futures::StreamExt;
                let reflector = ::std::sync::Arc::clone(&self.reflector);
                let responses = requests.map(move |request| {
                    let request = request?.to_owned_message();
                    respond(&reflector, request)
                });
                ::connectrpc::Response::stream_ok(responses)
            }
        }

        /// Answer one reflection request. Malformed requests (no
        /// `message_request` set) terminate the stream with
        /// `invalid_argument`; lookup misses are reported in-band via
        /// `ErrorResponse` with a `not_found` code, per the protocol.
        fn respond(
            reflector: &$crate::reflector::Reflector,
            request: pb::ServerReflectionRequest,
        ) -> Result<pb::ServerReflectionResponse, ::connectrpc::ConnectError> {
            use pb::server_reflection_request::MessageRequest;
            use pb::server_reflection_response::MessageResponse;
            use $crate::reflector::Answer;

            let Some(message_request) = &request.message_request else {
                return Err(::connectrpc::ConnectError::invalid_argument(
                    "ServerReflectionRequest.message_request is not set",
                ));
            };

            let answer = match message_request {
                MessageRequest::FileByFilename(name) => reflector.file_by_filename(name),
                MessageRequest::FileContainingSymbol(symbol) => {
                    reflector.file_containing_symbol(symbol)
                }
                MessageRequest::FileContainingExtension(ext) => {
                    reflector.file_containing_extension(&ext.containing_type, ext.extension_number)
                }
                MessageRequest::AllExtensionNumbersOfType(name) => {
                    reflector.all_extension_numbers_of_type(name)
                }
                MessageRequest::ListServices(_) => reflector.list_services(),
            };

            let message_response = match answer {
                Answer::Files(file_descriptor_proto) => {
                    MessageResponse::from(pb::FileDescriptorResponse {
                        file_descriptor_proto,
                    })
                }
                Answer::ExtensionNumbers { base_type, numbers } => {
                    MessageResponse::from(pb::ExtensionNumberResponse {
                        base_type_name: base_type,
                        extension_number: numbers,
                    })
                }
                Answer::Services(names) => MessageResponse::from(pb::ListServiceResponse {
                    service: names
                        .into_iter()
                        .map(|name| pb::ServiceResponse { name })
                        .collect(),
                }),
                Answer::NotFound(message) => MessageResponse::from(pb::ErrorResponse {
                    // tonic and grpc-go use the gRPC status code numbering
                    // here; 5 is NOT_FOUND.
                    error_code: 5,
                    error_message: message,
                }),
            };

            Ok(pb::ServerReflectionResponse {
                valid_host: request.host.clone(),
                original_request: ::buffa::MessageField::some(request),
                message_response: Some(message_response),
            })
        }
    };
}

mod v1 {
    use crate::connect::grpc::reflection::v1 as rpc;
    use crate::proto::grpc::reflection::v1 as pb;

    impl_server_reflection!();
}

mod v1alpha {
    use crate::connect::grpc::reflection::v1alpha as rpc;
    use crate::proto::grpc::reflection::v1alpha as pb;

    impl_server_reflection!();
}

#[cfg(test)]
mod tests {
    use buffa::Message;
    use buffa_descriptor::generated::descriptor::{
        FileDescriptorProto, FileDescriptorSet, ServiceDescriptorProto,
    };
    use connectrpc::client::{ClientConfig, HttpClient};
    use tokio::net::TcpListener;

    use super::*;
    // Go through the public re-exports rather than the internal generated
    // paths: this doubles as a check that the downstream-facing `wire`
    // module carries everything needed to drive the client.
    use crate::ServerReflectionClient;
    use crate::wire::v1::ServerReflectionRequest;
    use crate::wire::v1::server_reflection_request::MessageRequest;
    use crate::wire::v1::server_reflection_response::MessageResponse;

    fn test_set_bytes() -> Vec<u8> {
        FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("acme/api.proto".into()),
                package: Some("acme.api".into()),
                service: vec![ServiceDescriptorProto {
                    name: Some("Search".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode_to_vec()
    }

    /// Spin up a reflection server on a free port and hand back a v1
    /// client targeting it. The server runs until the test exits.
    async fn spawn_reflection_server() -> ServerReflectionClient<HttpClient> {
        let reflector = Reflector::from_descriptor_set_bytes(&test_set_bytes()).unwrap();
        spawn_router(install(Router::new(), reflector)).await
    }

    async fn spawn_router(router: Router) -> ServerReflectionClient<HttpClient> {
        let app = router.into_axum_router();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let config = ClientConfig::new(format!("http://{addr}").parse().unwrap());
        ServerReflectionClient::new(HttpClient::plaintext(), config)
    }

    fn request(message_request: MessageRequest) -> ServerReflectionRequest {
        ServerReflectionRequest {
            host: "test-host".into(),
            message_request: Some(message_request),
        }
    }

    /// `install` holds both routes to MAX_REQUEST_BYTES per message: an
    /// oversized request ends the stream with `resource_exhausted`.
    #[tokio::test]
    async fn oversized_request_is_refused() {
        let client = spawn_reflection_server().await;
        let mut stream = client.server_reflection_info().await.unwrap();
        stream
            .send(request(MessageRequest::FileContainingSymbol(
                "x".repeat(2 * crate::MAX_REQUEST_BYTES),
            )))
            .await
            .unwrap();
        stream.close_send();
        let err = stream.message().await.unwrap_err();
        assert_eq!(err.code, connectrpc::ErrorCode::ResourceExhausted);
    }

    /// An integrator's own `apply_request_limits` after `install` replaces
    /// the bundled profile: tightened to 1 KiB, a 2 KiB request the default
    /// 16 KiB would serve is refused.
    #[tokio::test]
    async fn integrator_limits_replace_the_bundled_profile() {
        let reflector = Reflector::from_descriptor_set_bytes(&test_set_bytes()).unwrap();
        let router = apply_request_limits(
            install(Router::new(), reflector),
            connectrpc::Limits::default().with_max_message_size(1024),
        );
        let client = spawn_router(router).await;
        let mut stream = client.server_reflection_info().await.unwrap();
        stream
            .send(request(MessageRequest::FileContainingSymbol(
                "x".repeat(2048),
            )))
            .await
            .unwrap();
        stream.close_send();
        let err = stream.message().await.unwrap_err();
        assert_eq!(err.code, connectrpc::ErrorCode::ResourceExhausted);
    }

    /// The wire types are generated with `unknown_fields=false`: an
    /// unrecognized field is accepted and skipped on both decode paths, so
    /// the request the service echoes as `original_request` re-encodes
    /// without it. Guards against a regeneration silently dropping the
    /// option.
    #[test]
    fn unknown_fields_are_skipped_not_echoed() {
        use buffa::view::MessageView;

        use crate::proto::grpc::reflection::v1::ServerReflectionRequestView;

        let known = request(MessageRequest::ListServices(String::new())).encode_to_vec();
        let mut with_unknown = known.clone();
        // Field 15, varint 0 — not defined by `ServerReflectionRequest`.
        with_unknown.extend_from_slice(&[0x78, 0x00]);

        let owned = ServerReflectionRequest::decode_from_slice(&with_unknown).unwrap();
        assert_eq!(owned.host, "test-host");
        assert_eq!(owned.encode_to_vec(), known);

        // The server decodes a view and echoes `to_owned_message()`.
        let view = ServerReflectionRequestView::decode_view(&with_unknown).unwrap();
        assert_eq!(view.to_owned_message().unwrap().encode_to_vec(), known);
    }

    #[tokio::test]
    async fn full_stream_round_trip() {
        let client = spawn_reflection_server().await;
        let mut stream = client.server_reflection_info().await.unwrap();

        stream
            .send(request(MessageRequest::ListServices(String::new())))
            .await
            .unwrap();
        stream
            .send(request(MessageRequest::FileContainingSymbol(
                "acme.api.Search".into(),
            )))
            .await
            .unwrap();
        stream
            .send(request(MessageRequest::FileByFilename("nope.proto".into())))
            .await
            .unwrap();
        stream.close_send();

        // 1: ListServices names both mounted reflection-visible services.
        let resp = stream.message().await.unwrap().unwrap().to_owned_message();
        assert_eq!(resp.valid_host, "test-host");
        assert!(matches!(
            resp.original_request
                .as_option()
                .and_then(|r| r.message_request.as_ref()),
            Some(MessageRequest::ListServices(_))
        ));
        match resp.message_response.unwrap() {
            MessageResponse::ListServicesResponse(list) => {
                let names: Vec<_> = list.service.iter().map(|s| s.name.as_str()).collect();
                assert_eq!(
                    names,
                    [
                        "acme.api.Search",
                        "grpc.reflection.v1.ServerReflection",
                        "grpc.reflection.v1alpha.ServerReflection",
                    ]
                );
            }
            other => panic!("expected list_services_response, got {other:?}"),
        }

        // 2: the symbol resolves to the original file bytes.
        let resp = stream.message().await.unwrap().unwrap().to_owned_message();
        match resp.message_response.unwrap() {
            MessageResponse::FileDescriptorResponse(fd) => {
                assert_eq!(fd.file_descriptor_proto.len(), 1);
                let file =
                    FileDescriptorProto::decode_from_slice(&fd.file_descriptor_proto[0]).unwrap();
                assert_eq!(file.name.as_deref(), Some("acme/api.proto"));
            }
            other => panic!("expected file_descriptor_response, got {other:?}"),
        }

        // 3: misses surface in-band as NOT_FOUND, keeping the stream alive.
        let resp = stream.message().await.unwrap().unwrap().to_owned_message();
        match resp.message_response.unwrap() {
            MessageResponse::ErrorResponse(err) => {
                assert_eq!(err.error_code, 5);
                assert!(err.error_message.contains("nope.proto"));
            }
            other => panic!("expected error_response, got {other:?}"),
        }

        assert!(stream.message().await.unwrap().is_none());
    }

    #[test]
    fn crate_descriptor_set_makes_reflection_self_describing() {
        let reflector = Reflector::from_descriptor_set_bytes(crate::FILE_DESCRIPTOR_SET).unwrap();
        assert_eq!(
            reflector.service_names(),
            [
                crate::SERVER_REFLECTION_SERVICE_NAME,
                crate::SERVER_REFLECTION_V1ALPHA_SERVICE_NAME,
            ]
        );
        assert!(matches!(
            reflector
                .file_containing_symbol("grpc.reflection.v1.ServerReflection.ServerReflectionInfo"),
            crate::reflector::Answer::Files(_)
        ));
    }

    #[tokio::test]
    async fn v1alpha_route_is_served() {
        // The v1alpha messages are wire-identical, so the v1 client with
        // a rewritten service path would also work; the simplest check
        // that `install` mounted the second route is a v1alpha request
        // through the generated v1alpha types over the same transport.
        use crate::connect::grpc::reflection::v1alpha::ServerReflectionClient as AlphaClient;
        use crate::proto::grpc::reflection::v1alpha::ServerReflectionRequest;
        use crate::proto::grpc::reflection::v1alpha::server_reflection_request::MessageRequest as AlphaRequest;
        use crate::proto::grpc::reflection::v1alpha::server_reflection_response::MessageResponse as AlphaResponse;

        let reflector = Reflector::from_descriptor_set_bytes(&test_set_bytes()).unwrap();
        let router = install(Router::new(), reflector);
        let app = router.into_axum_router();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let config = ClientConfig::new(format!("http://{addr}").parse().unwrap());
        let client = AlphaClient::new(HttpClient::plaintext(), config);

        let mut stream = client.server_reflection_info().await.unwrap();
        stream
            .send(ServerReflectionRequest {
                message_request: Some(AlphaRequest::ListServices(String::new())),
                ..Default::default()
            })
            .await
            .unwrap();
        stream.close_send();

        let resp = stream.message().await.unwrap().unwrap().to_owned_message();
        match resp.message_response.unwrap() {
            AlphaResponse::ListServicesResponse(list) => {
                assert_eq!(list.service.len(), 3);
            }
            other => panic!("expected list_services_response, got {other:?}"),
        }
    }
}
