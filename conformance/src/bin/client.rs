//! Conformance client harness for connectrpc-rs.
//!
//! This binary implements the client-side conformance test protocol:
//! 1. Reads `ClientCompatRequest` messages from stdin
//! 2. Makes RPC calls to the reference server as directed
//! 3. Writes `ClientCompatResponse` results to stdout
//!
//! The client uses the connectrpc-rs library's `call_unary` and `call_server_stream`
//! functions, proving that the library correctly implements the Connect protocol.

use std::sync::Arc;

use anyhow::Result;
use base64::Engine;
use buffa::Message;
use buffa_types::google::protobuf::Any;
use connectrpc::client::{
    BoxFuture, CallOptions, ClientBody, ClientConfig, ClientTransport, call_bidi_stream,
    call_client_stream, call_server_stream, call_unary, call_unary_get,
};
use connectrpc::error::{ConnectError, ErrorCode};
use connectrpc::rustls;
use connectrpc::{
    CodecFormat, CompressionRegistry,
    compression::{GzipProvider, ZstdProvider},
};
use connectrpc_conformance::ClientCompatRequest;
use connectrpc_conformance::ClientResponseResult;
use connectrpc_conformance::Codec;
use connectrpc_conformance::Compression;
use connectrpc_conformance::HTTPVersion;
use connectrpc_conformance::Header;
use connectrpc_conformance::Protocol;
use connectrpc_conformance::StreamType;
use connectrpc_conformance::init_type_registry;
use connectrpc_conformance::read_message;
use connectrpc_conformance::write_message;
use connectrpc_conformance::{
    BidiStreamResponseView, ClientStreamResponseView, IdempotentUnaryResponseView,
    ServerStreamResponseView, UnaryResponseView,
};
use hyper::Request;
use hyper::Response;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tracing_subscriber::EnvFilter;

// ============================================================================
// ConformanceTransport — wraps low-level hyper connections as a ClientTransport
// ============================================================================

/// Transport that opens a fresh TCP (+ optional TLS) connection per request.
///
/// This is used by the conformance harness to connect to the reference server
/// with precise control over HTTP version and TLS.
#[derive(Clone)]
struct ConformanceTransport {
    host: String,
    port: u16,
    /// Server's CA cert (PEM) for validating the server. None = plaintext.
    tls_cert: Option<Vec<u8>>,
    /// Client's certificate + key (PEM) for mTLS. None = no client auth.
    client_creds: Option<(Vec<u8>, Vec<u8>)>,
    http_version: HTTPVersion,
}

impl ConformanceTransport {
    /// Build a transport from the conformance request's connection fields.
    /// Centralizes the TLS / mTLS credential extraction.
    fn from_request(req: &ClientCompatRequest, http_version: HTTPVersion) -> Self {
        let use_tls = !req.server_tls_cert.is_empty();
        let client_creds = req.client_tls_creds.as_option().and_then(|creds| {
            if creds.cert.is_empty() || creds.key.is_empty() {
                None
            } else {
                Some((creds.cert.clone(), creds.key.clone()))
            }
        });
        Self {
            host: req.host.clone(),
            port: req.port as u16,
            tls_cert: if use_tls {
                Some(req.server_tls_cert.clone())
            } else {
                None
            },
            client_creds,
            http_version,
        }
    }
}

impl ClientTransport for ConformanceTransport {
    type ResponseBody = hyper::body::Incoming;
    type Error = ConformanceTransportError;

    fn send(
        &self,
        request: Request<ClientBody>,
    ) -> BoxFuture<'static, Result<Response<Self::ResponseBody>, Self::Error>> {
        let host = self.host.clone();
        let port = self.port;
        let tls_cert = self.tls_cert.clone();
        let client_creds = self.client_creds.clone();
        let http_version = self.http_version;

        Box::pin(async move {
            // The library builds a full URI (http://host:port/service/method).
            // The low-level hyper conn API expects path-only URIs.
            let path = request
                .uri()
                .path_and_query()
                .map(|pq| pq.as_str().to_string())
                .unwrap_or_else(|| request.uri().path().to_string());

            let host_header = if port == 80 || port == 443 {
                host.clone()
            } else {
                format!("{host}:{port}")
            };

            // Rebuild request with path-only URI and Host header
            let (mut parts, body) = request.into_parts();
            parts.uri = path
                .parse()
                .map_err(|e| ConformanceTransportError(format!("invalid path: {e}")))?;
            parts.headers.insert(
                http::header::HOST,
                host_header
                    .parse()
                    .map_err(|e| ConformanceTransportError(format!("invalid host header: {e}")))?,
            );
            let request = Request::from_parts(parts, body);

            // Connect
            let addr = format!("{host}:{port}");
            let tcp_stream = TcpStream::connect(&addr)
                .await
                .map_err(|e| ConformanceTransportError(format!("TCP connect failed: {e}")))?;

            if let Some(cert) = &tls_cert {
                let tls_config = build_tls_client_config(cert, client_creds.as_ref(), http_version)
                    .map_err(|e| ConformanceTransportError(format!("TLS config error: {e}")))?;
                let connector = TlsConnector::from(Arc::new(tls_config));
                let server_name = rustls_pki_types::ServerName::try_from(host.as_str())
                    .map_err(|e| ConformanceTransportError(format!("invalid server name: {e}")))?
                    .to_owned();
                let tls_stream = connector
                    .connect(server_name, tcp_stream)
                    .await
                    .map_err(|e| ConformanceTransportError(format!("TLS handshake failed: {e}")))?;
                let io = TokioIo::new(tls_stream);
                send_request(io, request, http_version)
                    .await
                    .map_err(|e| ConformanceTransportError(format!("send failed: {e}")))
            } else {
                let io = TokioIo::new(tcp_stream);
                send_request(io, request, http_version)
                    .await
                    .map_err(|e| ConformanceTransportError(format!("send failed: {e}")))
            }
        })
    }
}

/// Error type for ConformanceTransport.
#[derive(Debug, Clone)]
struct ConformanceTransportError(String);

impl std::fmt::Display for ConformanceTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConformanceTransportError {}

// ============================================================================
// Main + execute_request (unchanged structure)
// ============================================================================

/// Result of executing a single client conformance request.
enum ExecResult {
    Response(ClientResponseResult),
    Error(String),
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("debug".parse()?))
        .with_writer(std::io::stderr)
        .init();

    tracing::info!("Conformance client starting");

    // Initialize the Any type registry for proper proto3 JSON serialization
    init_type_registry();

    // Read requests from stdin until EOF
    loop {
        let request: Option<ClientCompatRequest> = read_message()?;
        let Some(request) = request else {
            tracing::info!("No more requests, exiting");
            break;
        };

        tracing::debug!("Processing test: {}", request.test_name);

        let result = execute_request(&request).await;

        let response = connectrpc_conformance::ClientCompatResponse {
            test_name: request.test_name.clone(),
            result: Some(match result {
                ExecResult::Response(r) => r.into(),
                ExecResult::Error(msg) => connectrpc_conformance::ClientErrorResult {
                    message: msg,
                    ..Default::default()
                }
                .into(),
            }),
            ..Default::default()
        };

        write_message(&response)?;
    }

    Ok(())
}

/// Execute a single conformance request.
async fn execute_request(req: &ClientCompatRequest) -> ExecResult {
    // Map conformance protocol to library protocol
    let protocol = req
        .protocol
        .as_known()
        .unwrap_or(Protocol::PROTOCOL_UNSPECIFIED);
    let wire_protocol = match protocol {
        Protocol::PROTOCOL_CONNECT | Protocol::PROTOCOL_UNSPECIFIED => {
            connectrpc::Protocol::Connect
        }
        Protocol::PROTOCOL_GRPC => connectrpc::Protocol::Grpc,
        Protocol::PROTOCOL_GRPC_WEB => connectrpc::Protocol::GrpcWeb,
    };

    // Validate stream type
    let stream_type = req
        .stream_type
        .as_known()
        .unwrap_or(StreamType::STREAM_TYPE_UNARY);
    if stream_type != StreamType::STREAM_TYPE_UNARY
        && stream_type != StreamType::STREAM_TYPE_SERVER_STREAM
        && stream_type != StreamType::STREAM_TYPE_CLIENT_STREAM
        && stream_type != StreamType::STREAM_TYPE_HALF_DUPLEX_BIDI_STREAM
        && stream_type != StreamType::STREAM_TYPE_FULL_DUPLEX_BIDI_STREAM
    {
        return ExecResult::Error(format!("unsupported stream type: {stream_type:?}"));
    }

    // Validate codec
    let codec = req.codec.as_known().unwrap_or(Codec::CODEC_UNSPECIFIED);
    if codec != Codec::CODEC_PROTO && codec != Codec::CODEC_JSON {
        return ExecResult::Error(format!("unsupported codec: {codec:?}"));
    }

    // Validate compression
    let compression = req
        .compression
        .as_known()
        .unwrap_or(Compression::COMPRESSION_UNSPECIFIED);
    if compression != Compression::COMPRESSION_IDENTITY
        && compression != Compression::COMPRESSION_GZIP
        && compression != Compression::COMPRESSION_ZSTD
        && compression != Compression::COMPRESSION_UNSPECIFIED
    {
        return ExecResult::Error(format!("unsupported compression: {compression:?}"));
    }

    // Validate HTTP version
    let http_version = req
        .http_version
        .as_known()
        .unwrap_or(HTTPVersion::HTTP_VERSION_UNSPECIFIED);
    if http_version == HTTPVersion::HTTP_VERSION_3 {
        return ExecResult::Error("HTTP/3 not supported".to_string());
    }

    // Connect GET: handled inside do_unary_call via req.use_get_http_method.

    // Validate cancel timing
    let cancel_before_close_send = if req.cancel.is_set() {
        use connectrpc_conformance::client_compat_request::cancel::CancelTiming;
        let cancel = &*req.cancel;
        match &cancel.cancel_timing {
            Some(CancelTiming::AfterCloseSendMs(_)) => false,
            Some(CancelTiming::BeforeCloseSend(_)) => {
                if matches!(
                    stream_type,
                    StreamType::STREAM_TYPE_CLIENT_STREAM
                        | StreamType::STREAM_TYPE_HALF_DUPLEX_BIDI_STREAM
                        | StreamType::STREAM_TYPE_FULL_DUPLEX_BIDI_STREAM
                ) {
                    true
                } else {
                    return ExecResult::Error(
                        "cancel before close-send not supported for this stream type".to_string(),
                    );
                }
            }
            Some(CancelTiming::AfterNumResponses(_)) => {
                if matches!(
                    stream_type,
                    StreamType::STREAM_TYPE_SERVER_STREAM
                        | StreamType::STREAM_TYPE_HALF_DUPLEX_BIDI_STREAM
                        | StreamType::STREAM_TYPE_FULL_DUPLEX_BIDI_STREAM
                ) {
                    false
                } else {
                    return ExecResult::Error(
                        "cancel after num responses not supported for unary".to_string(),
                    );
                }
            }
            None => false,
        }
    } else {
        false
    };

    // Raw requests are reference-client only
    if req.raw_request.is_set() {
        return ExecResult::Error("raw_request not supported".to_string());
    }

    // Extract cancel timing info
    let cancel_after_ms = req.cancel.as_option().and_then(|c| {
        use connectrpc_conformance::client_compat_request::cancel::CancelTiming;
        match &c.cancel_timing {
            Some(CancelTiming::AfterCloseSendMs(ms)) => Some(*ms),
            _ => None,
        }
    });
    let cancel_after_responses = req.cancel.as_option().and_then(|c| {
        use connectrpc_conformance::client_compat_request::cancel::CancelTiming;
        match &c.cancel_timing {
            Some(CancelTiming::AfterNumResponses(n)) => Some(*n),
            _ => None,
        }
    });

    // The Unimplemented RPC is always unary, regardless of the config's
    // declared stream_types. The conformance runner generates test cases
    // for it under every stream-type config to verify calling an unknown
    // method works — route by method name, not stream type (matches Go
    // reference: referenceclient/impl.go dispatches on req.Method first).
    if req.method.as_deref() == Some("Unimplemented") {
        return match do_unary_call(
            req,
            wire_protocol,
            codec,
            compression,
            http_version,
            cancel_after_ms,
        )
        .await
        {
            Ok(result) => ExecResult::Response(result),
            Err(e) => ExecResult::Error(format!("client error: {e}")),
        };
    }

    let result = match stream_type {
        StreamType::STREAM_TYPE_SERVER_STREAM => {
            do_server_stream_call(
                req,
                wire_protocol,
                codec,
                compression,
                http_version,
                cancel_after_ms,
                cancel_after_responses,
            )
            .await
        }
        StreamType::STREAM_TYPE_CLIENT_STREAM => {
            if cancel_before_close_send {
                // Cancel before sending all messages -- return immediate canceled
                Ok(ClientResponseResult {
                    response_headers: vec![],
                    payloads: vec![],
                    error: connectrpc_conformance::Error {
                        code: connectrpc_conformance::Code::CODE_CANCELED.into(),
                        message: Some("canceled".to_string()),
                        details: vec![],
                        ..Default::default()
                    }
                    .into(),
                    response_trailers: vec![],
                    num_unsent_requests: 0,
                    http_status_code: None,
                    feedback: vec![],
                    ..Default::default()
                })
            } else {
                do_client_stream_call(
                    req,
                    wire_protocol,
                    codec,
                    compression,
                    http_version,
                    cancel_after_ms,
                )
                .await
            }
        }
        StreamType::STREAM_TYPE_HALF_DUPLEX_BIDI_STREAM
        | StreamType::STREAM_TYPE_FULL_DUPLEX_BIDI_STREAM => {
            let full_duplex = stream_type == StreamType::STREAM_TYPE_FULL_DUPLEX_BIDI_STREAM;
            do_bidi_stream_call(
                req,
                wire_protocol,
                codec,
                compression,
                http_version,
                full_duplex,
                cancel_before_close_send,
                cancel_after_ms,
                cancel_after_responses,
            )
            .await
        }
        _ => {
            do_unary_call(
                req,
                wire_protocol,
                codec,
                compression,
                http_version,
                cancel_after_ms,
            )
            .await
        }
    };

    match result {
        Ok(result) => ExecResult::Response(result),
        Err(e) => ExecResult::Error(format!("client error: {e}")),
    }
}

// ============================================================================
// do_unary_call -- uses the library's call_unary via ConformanceTransport
// ============================================================================

/// Perform a unary RPC call using the library.
async fn do_unary_call(
    req: &ClientCompatRequest,
    wire_protocol: connectrpc::Protocol,
    codec: Codec,
    compression: Compression,
    http_version: HTTPVersion,
    cancel_after_ms: Option<u32>,
) -> Result<ClientResponseResult> {
    let use_tls = !req.server_tls_cert.is_empty();

    // Determine service and method
    let service = req
        .service
        .as_deref()
        .unwrap_or("connectrpc.conformance.v1.ConformanceService");
    let method = req.method.as_deref().unwrap_or("Unary");

    let transport = ConformanceTransport::from_request(req, http_version);

    // Build client config. For unary, always accept all supported encodings
    // for response decompression (matches reference clients — they
    // advertise Accept-Encoding even when not compressing requests).
    let registry = CompressionRegistry::new()
        .register(GzipProvider::new())
        .register(ZstdProvider::new());
    let base_uri: http::Uri = format!(
        "{}://{}:{}",
        if use_tls { "https" } else { "http" },
        req.host,
        req.port
    )
    .parse()?;
    let mut config = ClientConfig::new(base_uri)
        .with_protocol(wire_protocol)
        .with_codec_format(match codec {
            Codec::CODEC_JSON => CodecFormat::Json,
            _ => CodecFormat::Proto,
        })
        .with_compression(registry);
    config = apply_compression(config, compression);

    // Build CallOptions: timeout, max_message_size, custom headers
    let mut options = CallOptions::default();
    if let Some(timeout_ms) = req.timeout_ms {
        options = options.with_timeout(std::time::Duration::from_millis(timeout_ms as u64));
    }
    if req.message_receive_limit > 0 {
        options = options.with_max_message_size(req.message_receive_limit as usize);
    }
    for header in &req.request_headers {
        for val in &header.value {
            if let (Ok(name), Ok(value)) = (
                http::header::HeaderName::from_bytes(header.name.as_bytes()),
                http::header::HeaderValue::from_str(val),
            ) {
                options = options.with_header(name, value);
            }
        }
    }

    // Decode the request message from Any bytes
    let proto_bytes = req
        .request_messages
        .first()
        .map(|m| m.value.as_ref())
        .unwrap_or(&[]);

    // Dispatch based on method to get correct types. Use GET if the request
    // asks for it (Connect protocol only — call_unary_get will error otherwise).
    let use_get = req.use_get_http_method;

    // Macro: picks call_unary or call_unary_get based on use_get. Avoids
    // duplicating the three match arms for the GET/POST choice.
    macro_rules! do_call {
        ($req_ty:ty, $resp_view:ty, $request:expr) => {
            if use_get {
                call_unary_get::<_, $req_ty, $resp_view>(
                    &transport, &config, service, method, $request, options,
                )
                .await?
            } else {
                call_unary::<_, $req_ty, $resp_view>(
                    &transport, &config, service, method, $request, options,
                )
                .await?
            }
        };
    }

    let call_future = async {
        match method {
            "Unary" => {
                use connectrpc_conformance::UnaryRequest;
                let request = UnaryRequest::decode_from_slice(proto_bytes)
                    .map_err(|e| ConnectError::internal(format!("decode request: {e}")))?;
                let resp = do_call!(UnaryRequest, UnaryResponseView<'static>, request);
                let (headers, message, trailers) = resp.into_owned_parts();
                let payload = if message.payload.is_set() {
                    message.payload.as_option().unwrap().clone()
                } else {
                    Default::default()
                };
                Ok((headers, payload, trailers))
            }
            "IdempotentUnary" => {
                use connectrpc_conformance::IdempotentUnaryRequest;
                let request = IdempotentUnaryRequest::decode_from_slice(proto_bytes)
                    .map_err(|e| ConnectError::internal(format!("decode request: {e}")))?;
                let resp = do_call!(
                    IdempotentUnaryRequest,
                    IdempotentUnaryResponseView<'static>,
                    request
                );
                let (headers, message, trailers) = resp.into_owned_parts();
                let payload = if message.payload.is_set() {
                    message.payload.as_option().unwrap().clone()
                } else {
                    Default::default()
                };
                Ok((headers, payload, trailers))
            }
            _ => {
                // Unknown method (e.g., "Unimplemented") -- use UnaryRequest as best-effort
                use connectrpc_conformance::UnaryRequest;
                let request = UnaryRequest::decode_from_slice(proto_bytes).unwrap_or_default();
                let resp = do_call!(UnaryRequest, UnaryResponseView<'static>, request);
                let (headers, message, trailers) = resp.into_owned_parts();
                let payload = if message.payload.is_set() {
                    message.payload.as_option().unwrap().clone()
                } else {
                    Default::default()
                };
                Ok((headers, payload, trailers))
            }
        }
    };

    // Race against timeout and cancel timers.
    let timeout_duration = req
        .timeout_ms
        .map(|ms| std::time::Duration::from_millis(ms as u64));
    let cancel_duration = cancel_after_ms.map(|ms| std::time::Duration::from_millis(ms as u64));

    tokio::pin!(call_future);

    let timed_result = async {
        tokio::select! {
            res = &mut call_future => Ok(res),
            _ = async {
                match timeout_duration {
                    Some(d) => tokio::time::sleep(d).await,
                    None => std::future::pending().await,
                }
            } => {
                Err(connectrpc_conformance::Code::CODE_DEADLINE_EXCEEDED)
            }
            _ = async {
                match cancel_duration {
                    Some(d) => tokio::time::sleep(d).await,
                    None => std::future::pending().await,
                }
            } => {
                Err(connectrpc_conformance::Code::CODE_CANCELED)
            }
        }
    }
    .await;

    match timed_result {
        Ok(Ok((headers, payload, trailers))) => Ok(ClientResponseResult {
            response_headers: headers_to_conformance(&headers),
            payloads: vec![payload],
            error: Default::default(),
            response_trailers: headers_to_conformance(&trailers),
            num_unsent_requests: 0,
            http_status_code: None,
            feedback: vec![],
            ..Default::default()
        }),
        Ok(Err(connect_error)) => Ok(connect_error_to_result(&connect_error)),
        Err(code) => {
            let message = match code {
                connectrpc_conformance::Code::CODE_CANCELED => "canceled",
                _ => "deadline exceeded",
            };
            Ok(ClientResponseResult {
                response_headers: vec![],
                payloads: vec![],
                error: connectrpc_conformance::Error {
                    code: code.into(),
                    message: Some(message.to_string()),
                    details: vec![],
                    ..Default::default()
                }
                .into(),
                response_trailers: vec![],
                num_unsent_requests: 0,
                http_status_code: None,
                feedback: vec![],
                ..Default::default()
            })
        }
    }
}

// ============================================================================
// do_server_stream_call -- uses the library's call_server_stream
// ============================================================================

/// Perform a server-streaming Connect RPC call using the library.
async fn do_server_stream_call(
    req: &ClientCompatRequest,
    wire_protocol: connectrpc::Protocol,
    codec: Codec,
    compression: Compression,
    http_version: HTTPVersion,
    cancel_after_ms: Option<u32>,
    cancel_after_responses: Option<u32>,
) -> Result<ClientResponseResult> {
    let use_tls = !req.server_tls_cert.is_empty();

    // Determine service and method
    let service = req
        .service
        .as_deref()
        .unwrap_or("connectrpc.conformance.v1.ConformanceService");
    let method = req.method.as_deref().unwrap_or("ServerStream");

    let transport = ConformanceTransport::from_request(req, http_version);

    // Build client config. For streaming, only register the requested
    // encoding (the "unexpected-compressed-message" conformance tests
    // verify we reject compression we didn't advertise).
    let registry = registry_for_compression(compression);
    let base_uri: http::Uri = format!(
        "{}://{}:{}",
        if use_tls { "https" } else { "http" },
        req.host,
        req.port
    )
    .parse()?;
    let mut config = ClientConfig::new(base_uri)
        .with_protocol(wire_protocol)
        .with_codec_format(match codec {
            Codec::CODEC_JSON => CodecFormat::Json,
            _ => CodecFormat::Proto,
        })
        .with_compression(registry);
    config = apply_compression(config, compression);

    // Build CallOptions
    let mut options = CallOptions::default();
    if let Some(timeout_ms) = req.timeout_ms {
        options = options.with_timeout(std::time::Duration::from_millis(timeout_ms as u64));
    }
    if req.message_receive_limit > 0 {
        options = options.with_max_message_size(req.message_receive_limit as usize);
    }
    for header in &req.request_headers {
        for val in &header.value {
            if let (Ok(name), Ok(value)) = (
                http::header::HeaderName::from_bytes(header.name.as_bytes()),
                http::header::HeaderValue::from_str(val),
            ) {
                options = options.with_header(name, value);
            }
        }
    }

    // Decode request message
    let proto_bytes = req
        .request_messages
        .first()
        .map(|m| m.value.as_ref())
        .unwrap_or(&[]);

    use connectrpc_conformance::ServerStreamRequest;
    let request = ServerStreamRequest::decode_from_slice(proto_bytes)
        .map_err(|e| anyhow::anyhow!("decode request: {e}"))?;

    // Make the call -- this sends the request and returns a stream handle
    let stream_result = async {
        call_server_stream::<_, ServerStreamRequest, ServerStreamResponseView<'static>>(
            &transport, &config, service, method, request, options,
        )
        .await
    };

    // Race stream creation against timers.
    let timeout_duration = req
        .timeout_ms
        .map(|ms| std::time::Duration::from_millis(ms as u64));
    let cancel_duration = cancel_after_ms.map(|ms| std::time::Duration::from_millis(ms as u64));

    tokio::pin!(stream_result);

    let stream_timed = async {
        tokio::select! {
            res = &mut stream_result => Ok(res),
            _ = async {
                match timeout_duration {
                    Some(d) => tokio::time::sleep(d).await,
                    None => std::future::pending().await,
                }
            } => {
                Err(connectrpc_conformance::Code::CODE_DEADLINE_EXCEEDED)
            }
            _ = async {
                match cancel_duration {
                    Some(d) => tokio::time::sleep(d).await,
                    None => std::future::pending().await,
                }
            } => {
                Err(connectrpc_conformance::Code::CODE_CANCELED)
            }
        }
    }
    .await;

    let mut stream = match stream_timed {
        Ok(Ok(s)) => s,
        Ok(Err(connect_error)) => return Ok(connect_error_to_result(&connect_error)),
        Err(code) => {
            let message = match code {
                connectrpc_conformance::Code::CODE_CANCELED => "canceled",
                _ => "deadline exceeded",
            };
            return Ok(ClientResponseResult {
                response_headers: vec![],
                payloads: vec![],
                error: connectrpc_conformance::Error {
                    code: code.into(),
                    message: Some(message.to_string()),
                    details: vec![],
                    ..Default::default()
                }
                .into(),
                response_trailers: vec![],
                num_unsent_requests: 0,
                http_status_code: None,
                feedback: vec![],
                ..Default::default()
            });
        }
    };

    // Check for unexpected compression (server sent content-encoding
    // but we didn't request it)
    let accepts_compression = compression_encoding_name(compression).is_some();
    let encoding_header = wire_protocol.content_encoding_header();
    let response_encoding = stream
        .headers()
        .get(encoding_header)
        .and_then(|v| v.to_str().ok());
    if response_encoding.is_some() && !accepts_compression {
        let response_headers = headers_to_conformance(stream.headers());
        return Ok(ClientResponseResult {
            response_headers,
            payloads: vec![],
            error: connectrpc_conformance::Error {
                code: connectrpc_conformance::Code::CODE_INTERNAL.into(),
                message: Some(
                    "received compressed response without requesting compression".to_string(),
                ),
                details: vec![],
                ..Default::default()
            }
            .into(),
            response_trailers: vec![],
            num_unsent_requests: 0,
            http_status_code: None,
            feedback: vec![],
            ..Default::default()
        });
    }

    let response_headers = headers_to_conformance(stream.headers());
    let mut payloads = Vec::new();

    // Create timeout/cancel sleep futures for racing during message reads
    let timeout_sleep = timeout_duration.map(tokio::time::sleep);
    let cancel_sleep = cancel_duration.map(tokio::time::sleep);
    tokio::pin!(timeout_sleep);
    tokio::pin!(cancel_sleep);

    // Read messages from the stream
    loop {
        // Check cancel-after-responses BEFORE reading next message
        if let Some(max_responses) = cancel_after_responses
            && payloads.len() as u32 >= max_responses
        {
            return Ok(ClientResponseResult {
                response_headers,
                payloads,
                error: connectrpc_conformance::Error {
                    code: connectrpc_conformance::Code::CODE_CANCELED.into(),
                    message: Some("canceled".to_string()),
                    details: vec![],
                    ..Default::default()
                }
                .into(),
                response_trailers: vec![],
                num_unsent_requests: 0,
                http_status_code: None,
                feedback: vec![],
                ..Default::default()
            });
        }

        // Race message read against timeout/cancel
        let msg = tokio::select! {
            m = stream.message() => m,
            _ = async {
                match timeout_sleep.as_mut().as_pin_mut() {
                    Some(s) => s.await,
                    None => std::future::pending().await,
                }
            } => {
                return Ok(ClientResponseResult {
                    response_headers,
                    payloads: vec![],
                    error: connectrpc_conformance::Error {
                        code: connectrpc_conformance::Code::CODE_DEADLINE_EXCEEDED.into(),
                        message: Some("deadline exceeded".to_string()),
                        details: vec![],
                        ..Default::default()
                    }.into(),
                    response_trailers: vec![],
                    num_unsent_requests: 0,
                    http_status_code: None,
                    feedback: vec![],
                    ..Default::default()
                });
            }
            _ = async {
                match cancel_sleep.as_mut().as_pin_mut() {
                    Some(s) => s.await,
                    None => std::future::pending().await,
                }
            } => {
                return Ok(ClientResponseResult {
                    response_headers,
                    payloads: vec![],
                    error: connectrpc_conformance::Error {
                        code: connectrpc_conformance::Code::CODE_CANCELED.into(),
                        message: Some("canceled".to_string()),
                        details: vec![],
                        ..Default::default()
                    }.into(),
                    response_trailers: vec![],
                    num_unsent_requests: 0,
                    http_status_code: None,
                    feedback: vec![],
                    ..Default::default()
                });
            }
        };

        match msg {
            Ok(Some(resp)) => {
                let resp = resp.to_owned_message();
                let payload = if resp.payload.is_set() {
                    resp.payload.as_option().unwrap().clone()
                } else {
                    Default::default()
                };
                payloads.push(payload);
            }
            Ok(None) => break, // Stream ended normally
            Err(connect_error) => {
                // Stream error -- return what we have with the error
                return Ok(ClientResponseResult {
                    response_headers,
                    payloads,
                    error: connect_error_to_conformance(&connect_error).into(),
                    response_trailers: stream
                        .trailers()
                        .map(headers_to_conformance)
                        .unwrap_or_default(),
                    num_unsent_requests: 0,
                    http_status_code: None,
                    feedback: vec![],
                    ..Default::default()
                });
            }
        }
    }

    // Stream ended -- collect trailers and error from END_STREAM
    let response_trailers = stream
        .trailers()
        .map(headers_to_conformance)
        .unwrap_or_default();
    let stream_error = stream
        .error()
        .map(|e| connect_error_to_conformance(e).into())
        .unwrap_or_default();

    Ok(ClientResponseResult {
        response_headers,
        payloads,
        error: stream_error,
        response_trailers,
        num_unsent_requests: 0,
        http_status_code: None,
        feedback: vec![],
        ..Default::default()
    })
}

// ============================================================================
// do_client_stream_call -- uses the library's call_client_stream
// ============================================================================

/// Perform a client-streaming Connect RPC call using the library.
async fn do_client_stream_call(
    req: &ClientCompatRequest,
    wire_protocol: connectrpc::Protocol,
    codec: Codec,
    compression: Compression,
    http_version: HTTPVersion,
    cancel_after_ms: Option<u32>,
) -> Result<ClientResponseResult> {
    let use_tls = !req.server_tls_cert.is_empty();

    // Determine service and method
    let service = req
        .service
        .as_deref()
        .unwrap_or("connectrpc.conformance.v1.ConformanceService");
    let method = req.method.as_deref().unwrap_or("ClientStream");

    let transport = ConformanceTransport::from_request(req, http_version);

    // Build client config (streaming — only requested encoding).
    let registry = registry_for_compression(compression);
    let base_uri: http::Uri = format!(
        "{}://{}:{}",
        if use_tls { "https" } else { "http" },
        req.host,
        req.port
    )
    .parse()?;
    let mut config = ClientConfig::new(base_uri)
        .with_protocol(wire_protocol)
        .with_codec_format(match codec {
            Codec::CODEC_JSON => CodecFormat::Json,
            _ => CodecFormat::Proto,
        })
        .with_compression(registry);
    config = apply_compression(config, compression);

    // Build CallOptions
    let mut options = CallOptions::default();
    if let Some(timeout_ms) = req.timeout_ms {
        options = options.with_timeout(std::time::Duration::from_millis(timeout_ms as u64));
    }
    if req.message_receive_limit > 0 {
        options = options.with_max_message_size(req.message_receive_limit as usize);
    }
    for header in &req.request_headers {
        for val in &header.value {
            if let (Ok(name), Ok(value)) = (
                http::header::HeaderName::from_bytes(header.name.as_bytes()),
                http::header::HeaderValue::from_str(val),
            ) {
                options = options.with_header(name, value);
            }
        }
    }

    // Decode all request messages
    use connectrpc_conformance::ClientStreamRequest;
    let requests: Vec<ClientStreamRequest> = req
        .request_messages
        .iter()
        .map(|m| {
            ClientStreamRequest::decode_from_slice(m.value.as_ref())
                .map_err(|e| anyhow::anyhow!("decode request: {e}"))
        })
        .collect::<Result<Vec<_>>>()?;

    // Make the call
    let call_future = async {
        let resp = call_client_stream::<_, ClientStreamRequest, ClientStreamResponseView<'static>>(
            &transport,
            &config,
            service,
            method,
            futures::stream::iter(requests),
            options,
        )
        .await?;
        let (headers, message, trailers) = resp.into_owned_parts();
        let payload = if message.payload.is_set() {
            message.payload.as_option().unwrap().clone()
        } else {
            Default::default()
        };
        Ok((headers, payload, trailers))
    };

    // Race against timeout and cancel timers.
    let timeout_duration = req
        .timeout_ms
        .map(|ms| std::time::Duration::from_millis(ms as u64));
    let cancel_duration = cancel_after_ms.map(|ms| std::time::Duration::from_millis(ms as u64));

    tokio::pin!(call_future);

    let timed_result = async {
        tokio::select! {
            res = &mut call_future => Ok(res),
            _ = async {
                match timeout_duration {
                    Some(d) => tokio::time::sleep(d).await,
                    None => std::future::pending().await,
                }
            } => {
                Err(connectrpc_conformance::Code::CODE_DEADLINE_EXCEEDED)
            }
            _ = async {
                match cancel_duration {
                    Some(d) => tokio::time::sleep(d).await,
                    None => std::future::pending().await,
                }
            } => {
                Err(connectrpc_conformance::Code::CODE_CANCELED)
            }
        }
    }
    .await;

    match timed_result {
        Ok(Ok((headers, payload, trailers))) => Ok(ClientResponseResult {
            response_headers: headers_to_conformance(&headers),
            payloads: vec![payload],
            error: Default::default(),
            response_trailers: headers_to_conformance(&trailers),
            num_unsent_requests: 0,
            http_status_code: None,
            feedback: vec![],
            ..Default::default()
        }),
        Ok(Err(connect_error)) => Ok(connect_error_to_result(&connect_error)),
        Err(code) => {
            let message = match code {
                connectrpc_conformance::Code::CODE_CANCELED => "canceled",
                _ => "deadline exceeded",
            };
            Ok(ClientResponseResult {
                response_headers: vec![],
                payloads: vec![],
                error: connectrpc_conformance::Error {
                    code: code.into(),
                    message: Some(message.to_string()),
                    details: vec![],
                    ..Default::default()
                }
                .into(),
                response_trailers: vec![],
                num_unsent_requests: 0,
                http_status_code: None,
                feedback: vec![],
                ..Default::default()
            })
        }
    }
}

// ============================================================================
// do_bidi_stream_call -- uses the library's call_bidi_stream
// ============================================================================

/// Perform a bidirectional-streaming RPC call using the library.
///
/// Mirrors the Go reference client (referenceclient/impl.go:377-500):
/// - send each request (with optional per-request delay)
/// - in full-duplex mode, receive one response after each send
/// - close send, then drain remaining responses
/// - cancel timings: BeforeCloseSend / AfterCloseSendMs / AfterNumResponses
#[allow(clippy::too_many_arguments)]
async fn do_bidi_stream_call(
    req: &ClientCompatRequest,
    wire_protocol: connectrpc::Protocol,
    codec: Codec,
    compression: Compression,
    http_version: HTTPVersion,
    full_duplex: bool,
    cancel_before_close_send: bool,
    cancel_after_ms: Option<u32>,
    cancel_after_responses: Option<u32>,
) -> Result<ClientResponseResult> {
    use connectrpc_conformance::BidiStreamRequest;

    let use_tls = !req.server_tls_cert.is_empty();
    let service = req
        .service
        .as_deref()
        .unwrap_or("connectrpc.conformance.v1.ConformanceService");
    let method = req.method.as_deref().unwrap_or("BidiStream");

    let transport = ConformanceTransport::from_request(req, http_version);

    // Build client config (streaming — only requested encoding).
    let registry = registry_for_compression(compression);
    let base_uri: http::Uri = format!(
        "{}://{}:{}",
        if use_tls { "https" } else { "http" },
        req.host,
        req.port
    )
    .parse()?;
    let mut config = ClientConfig::new(base_uri)
        .with_protocol(wire_protocol)
        .with_codec_format(match codec {
            Codec::CODEC_JSON => CodecFormat::Json,
            _ => CodecFormat::Proto,
        })
        .with_compression(registry);
    config = apply_compression(config, compression);

    // Build CallOptions
    let mut options = CallOptions::default();
    if let Some(timeout_ms) = req.timeout_ms {
        options = options.with_timeout(std::time::Duration::from_millis(timeout_ms as u64));
    }
    if req.message_receive_limit > 0 {
        options = options.with_max_message_size(req.message_receive_limit as usize);
    }
    for header in &req.request_headers {
        for val in &header.value {
            if let (Ok(name), Ok(value)) = (
                http::header::HeaderName::from_bytes(header.name.as_bytes()),
                http::header::HeaderValue::from_str(val),
            ) {
                options = options.with_header(name, value);
            }
        }
    }

    // Decode all request messages upfront
    let requests: Vec<BidiStreamRequest> = req
        .request_messages
        .iter()
        .map(|m| {
            BidiStreamRequest::decode_from_slice(m.value.as_ref())
                .map_err(|e| anyhow::anyhow!("decode request: {e}"))
        })
        .collect::<Result<Vec<_>>>()?;

    let request_delay = std::time::Duration::from_millis(req.request_delay_ms as u64);

    // Open the bidi stream. The response future is stored lazily inside
    // BidiStream and awaited on first message() call — see the library's
    // call_bidi_stream docs.
    let mut stream =
        match call_bidi_stream::<_, BidiStreamRequest, BidiStreamResponseView<'static>>(
            &transport, &config, service, method, options,
        )
        .await
        {
            Ok(s) => s,
            Err(e) => return Ok(connect_error_to_result(&e)),
        };

    // Set up cancel-after-close-send timer (AfterCloseSendMs timing).
    // Started AFTER close_send() is called (see below).
    let cancel_after_close_send_duration =
        cancel_after_ms.map(|ms| std::time::Duration::from_millis(ms as u64));

    // Client-side timeout enforcement: start the clock NOW so the whole
    // call (sends + receives) is bounded. The library sets the header but
    // doesn't enforce — the harness must, matching Go's ctx.Deadline.
    let timeout_sleep = req
        .timeout_ms
        .map(|ms| tokio::time::sleep(std::time::Duration::from_millis(ms as u64)));
    tokio::pin!(timeout_sleep);

    let mut payloads: Vec<connectrpc_conformance::ConformancePayload> = Vec::new();
    let mut proto_error: Option<connectrpc_conformance::Error> = None;
    let mut num_unsent: i32 = 0;
    let mut total_received: u32 = 0;

    // If this stream is to be canceled after N responses (only meaningful
    // for full-duplex), we set this flag once hit and break the send loop.
    let mut cancel_triggered = false;

    // Helper: race a message() poll against the timeout.
    // Ok(msg-result) for a message, Err(()) on deadline.
    // Macro avoids closure lifetime pain with &mut stream + Pin.
    macro_rules! recv_or_timeout {
        () => {
            tokio::select! {
                m = stream.message() => Ok(m),
                _ = async {
                    match timeout_sleep.as_mut().as_pin_mut() {
                        Some(s) => s.await,
                        None => std::future::pending().await,
                    }
                } => Err(()),
            }
        };
    }

    // Helper: construct the deadline-exceeded result. Macro because it
    // moves `payloads` and borrows `stream` — simpler than a closure.
    macro_rules! deadline_exceeded {
        ($num_unsent:expr) => {
            return Ok(ClientResponseResult {
                response_headers: stream
                    .headers()
                    .map(headers_to_conformance)
                    .unwrap_or_default(),
                payloads,
                error: connectrpc_conformance::Error {
                    code: connectrpc_conformance::Code::CODE_DEADLINE_EXCEEDED.into(),
                    message: Some("deadline exceeded".to_string()),
                    details: vec![],
                    ..Default::default()
                }
                .into(),
                response_trailers: vec![],
                num_unsent_requests: $num_unsent,
                http_status_code: None,
                feedback: vec![],
                ..Default::default()
            })
        };
    }

    // Send loop: for each request, sleep the request delay, send it.
    // In full-duplex mode, also receive one response per request.
    for (i, msg) in requests.into_iter().enumerate() {
        if !request_delay.is_zero() {
            tokio::time::sleep(request_delay).await;
        }

        if let Err(e) = stream.send(msg).await {
            // Server closed the stream mid-send. Record unsent count and
            // try to pull the real error from the receive side (Go reference
            // does stream.Receive() here to surface the server error).
            num_unsent = (req.request_messages.len() - i) as i32;
            // message() will await response headers and yield the error.
            // If it also fails (transport-level), we'll have that error.
            // If it succeeds (server sent data before closing), drain below.
            match stream.message().await {
                Ok(_) => {
                    // Unexpected: send failed but receive got a message.
                    // Record the send error as the proto error.
                    proto_error = Some(connect_error_to_conformance(&e));
                }
                Err(recv_err) => {
                    proto_error = Some(connect_error_to_conformance(&recv_err));
                }
            }
            break;
        }

        if full_duplex {
            // Receive exactly one response for this send (race against timeout).
            match recv_or_timeout!() {
                Err(()) => {
                    num_unsent = (req.request_messages.len() - i - 1) as i32;
                    deadline_exceeded!(num_unsent);
                }
                Ok(Ok(Some(resp))) => {
                    let resp = resp.to_owned_message();
                    let payload = if resp.payload.is_set() {
                        resp.payload.as_option().unwrap().clone()
                    } else {
                        Default::default()
                    };
                    payloads.push(payload);
                    total_received += 1;
                    if let Some(max) = cancel_after_responses
                        && total_received >= max
                    {
                        cancel_triggered = true;
                        break;
                    }
                }
                Ok(Ok(None)) => {
                    // Server ended the stream early. Drain handled below.
                    break;
                }
                Ok(Err(e)) => {
                    proto_error = Some(connect_error_to_conformance(&e));
                    break;
                }
            }
        }
    }

    // BeforeCloseSend: drop the stream without closing the send side
    // gracefully. In practice this means we skip close_send and drain,
    // returning a synthesized Canceled. Matches the client_stream handling
    // above in execute_request.
    if cancel_before_close_send || cancel_triggered {
        // Don't close_send; just drop the stream (dropping tx cancels).
        return Ok(ClientResponseResult {
            response_headers: stream
                .headers()
                .map(headers_to_conformance)
                .unwrap_or_default(),
            payloads,
            error: connectrpc_conformance::Error {
                code: connectrpc_conformance::Code::CODE_CANCELED.into(),
                message: Some("canceled".to_string()),
                details: vec![],
                ..Default::default()
            }
            .into(),
            response_trailers: vec![],
            num_unsent_requests: num_unsent,
            http_status_code: None,
            feedback: vec![],
            ..Default::default()
        });
    }

    // Close the send side.
    stream.close_send();

    // If we already hit an error in the send loop, don't drain — just
    // return what we have (matches Go reference's early-return).
    if let Some(err) = proto_error {
        return Ok(ClientResponseResult {
            response_headers: stream
                .headers()
                .map(headers_to_conformance)
                .unwrap_or_default(),
            payloads,
            error: err.into(),
            response_trailers: stream
                .trailers()
                .map(headers_to_conformance)
                .unwrap_or_default(),
            num_unsent_requests: num_unsent,
            http_status_code: None,
            feedback: vec![],
            ..Default::default()
        });
    }

    // Set up AfterCloseSendMs cancel timer, started now (after close_send).
    let cancel_sleep = cancel_after_close_send_duration.map(tokio::time::sleep);
    tokio::pin!(cancel_sleep);

    // Drain remaining responses, racing against the cancel timer.
    loop {
        if let Some(max) = cancel_after_responses
            && total_received >= max
        {
            return Ok(ClientResponseResult {
                response_headers: stream
                    .headers()
                    .map(headers_to_conformance)
                    .unwrap_or_default(),
                payloads,
                error: connectrpc_conformance::Error {
                    code: connectrpc_conformance::Code::CODE_CANCELED.into(),
                    message: Some("canceled".to_string()),
                    details: vec![],
                    ..Default::default()
                }
                .into(),
                response_trailers: vec![],
                num_unsent_requests: 0,
                http_status_code: None,
                feedback: vec![],
                ..Default::default()
            });
        }

        let msg = tokio::select! {
            m = stream.message() => m,
            _ = async {
                match timeout_sleep.as_mut().as_pin_mut() {
                    Some(s) => s.await,
                    None => std::future::pending().await,
                }
            } => {
                deadline_exceeded!(0);
            }
            _ = async {
                match cancel_sleep.as_mut().as_pin_mut() {
                    Some(s) => s.await,
                    None => std::future::pending().await,
                }
            } => {
                return Ok(ClientResponseResult {
                    response_headers: stream
                        .headers()
                        .map(headers_to_conformance)
                        .unwrap_or_default(),
                    payloads,
                    error: connectrpc_conformance::Error {
                        code: connectrpc_conformance::Code::CODE_CANCELED.into(),
                        message: Some("canceled".to_string()),
                        details: vec![],
                        ..Default::default()
                    }.into(),
                    response_trailers: vec![],
                    num_unsent_requests: 0,
                    http_status_code: None,
                    feedback: vec![],
                    ..Default::default()
                });
            }
        };

        match msg {
            Ok(Some(resp)) => {
                let resp = resp.to_owned_message();
                let payload = if resp.payload.is_set() {
                    resp.payload.as_option().unwrap().clone()
                } else {
                    Default::default()
                };
                payloads.push(payload);
                total_received += 1;
            }
            Ok(None) => break, // stream ended normally
            Err(e) => {
                proto_error = Some(connect_error_to_conformance(&e));
                break;
            }
        }
    }

    // Stream ended — collect headers/trailers/error.
    let response_headers = stream
        .headers()
        .map(headers_to_conformance)
        .unwrap_or_default();
    let response_trailers = stream
        .trailers()
        .map(headers_to_conformance)
        .unwrap_or_default();
    let stream_error = proto_error.map(Into::into).unwrap_or_else(|| {
        stream
            .error()
            .map(|e| connect_error_to_conformance(e).into())
            .unwrap_or_default()
    });

    Ok(ClientResponseResult {
        response_headers,
        payloads,
        error: stream_error,
        response_trailers,
        num_unsent_requests: num_unsent,
        http_status_code: None,
        feedback: vec![],
        ..Default::default()
    })
}

// ============================================================================
// Helper functions -- type conversion between library and conformance types
// ============================================================================

/// Convert HTTP headers to conformance Header format.
fn headers_to_conformance(headers: &http::HeaderMap) -> Vec<Header> {
    let mut result: Vec<Header> = Vec::new();
    for (name, value) in headers.iter() {
        let name_str = name.as_str().to_string();
        let val_str = value.to_str().unwrap_or("").to_string();
        if let Some(h) = result.iter_mut().find(|h| h.name == name_str) {
            h.value.push(val_str);
        } else {
            result.push(Header {
                name: name_str,
                value: vec![val_str],
                ..Default::default()
            });
        }
    }
    result
}

/// Convert a ConnectError to a conformance ClientResponseResult.
fn connect_error_to_result(err: &ConnectError) -> ClientResponseResult {
    // Extract headers/trailers from the error
    let response_headers = headers_to_conformance(err.response_headers());
    let response_trailers = headers_to_conformance(err.trailers());

    ClientResponseResult {
        response_headers,
        payloads: vec![],
        error: connect_error_to_conformance(err).into(),
        response_trailers,
        num_unsent_requests: 0,
        http_status_code: None,
        feedback: vec![],
        ..Default::default()
    }
}

/// Build a compression registry for the requested encoding.
///
/// For IDENTITY/UNSPECIFIED, returns an empty registry (the
/// "unexpected-compressed-message" conformance tests verify that a
/// client which didn't advertise an encoding rejects compressed responses).
/// For GZIP/ZSTD, returns a registry with only that provider.
fn registry_for_compression(compression: Compression) -> CompressionRegistry {
    match compression {
        Compression::COMPRESSION_GZIP => CompressionRegistry::new().register(GzipProvider::new()),
        Compression::COMPRESSION_ZSTD => CompressionRegistry::new().register(ZstdProvider::new()),
        _ => CompressionRegistry::new(),
    }
}

/// Map a conformance Compression enum to the wire encoding name (for
/// `compress_requests()`). Returns None for IDENTITY/UNSPECIFIED.
fn compression_encoding_name(compression: Compression) -> Option<&'static str> {
    match compression {
        Compression::COMPRESSION_GZIP => Some("gzip"),
        Compression::COMPRESSION_ZSTD => Some("zstd"),
        _ => None,
    }
}

/// Apply request compression config if the test specified an encoding.
/// `with_min_size(0)` forces compression even for tiny/empty bodies so the
/// conformance runner sees the advertised encoding applied.
fn apply_compression(config: ClientConfig, compression: Compression) -> ClientConfig {
    match compression_encoding_name(compression) {
        Some(enc) => config
            .compress_requests(enc)
            .with_compression_policy(connectrpc::CompressionPolicy::default().with_min_size(0)),
        None => config,
    }
}

/// Convert a ConnectError to a conformance Error.
fn connect_error_to_conformance(err: &ConnectError) -> connectrpc_conformance::Error {
    let code = error_code_to_conformance(err.code);
    let details = err
        .details
        .iter()
        .map(|d| {
            let value = d
                .value
                .as_deref()
                .and_then(|v| {
                    base64::engine::general_purpose::STANDARD
                        .decode(v)
                        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(v))
                        .ok()
                })
                .unwrap_or_default();
            let type_url = if d.type_url.contains('/') {
                d.type_url.clone()
            } else {
                format!("type.googleapis.com/{}", d.type_url)
            };
            Any {
                type_url,
                value: value.into(),
                ..Default::default()
            }
        })
        .collect();

    connectrpc_conformance::Error {
        code: code.into(),
        message: err.message.clone(),
        details,
        ..Default::default()
    }
}

/// Map library ErrorCode to conformance Code.
fn error_code_to_conformance(code: ErrorCode) -> connectrpc_conformance::Code {
    match code {
        ErrorCode::Canceled => connectrpc_conformance::Code::CODE_CANCELED,
        ErrorCode::Unknown => connectrpc_conformance::Code::CODE_UNKNOWN,
        ErrorCode::InvalidArgument => connectrpc_conformance::Code::CODE_INVALID_ARGUMENT,
        ErrorCode::DeadlineExceeded => connectrpc_conformance::Code::CODE_DEADLINE_EXCEEDED,
        ErrorCode::NotFound => connectrpc_conformance::Code::CODE_NOT_FOUND,
        ErrorCode::AlreadyExists => connectrpc_conformance::Code::CODE_ALREADY_EXISTS,
        ErrorCode::PermissionDenied => connectrpc_conformance::Code::CODE_PERMISSION_DENIED,
        ErrorCode::ResourceExhausted => connectrpc_conformance::Code::CODE_RESOURCE_EXHAUSTED,
        ErrorCode::FailedPrecondition => connectrpc_conformance::Code::CODE_FAILED_PRECONDITION,
        ErrorCode::Aborted => connectrpc_conformance::Code::CODE_ABORTED,
        ErrorCode::OutOfRange => connectrpc_conformance::Code::CODE_OUT_OF_RANGE,
        ErrorCode::Unimplemented => connectrpc_conformance::Code::CODE_UNIMPLEMENTED,
        ErrorCode::Internal => connectrpc_conformance::Code::CODE_INTERNAL,
        ErrorCode::Unavailable => connectrpc_conformance::Code::CODE_UNAVAILABLE,
        ErrorCode::DataLoss => connectrpc_conformance::Code::CODE_DATA_LOSS,
        ErrorCode::Unauthenticated => connectrpc_conformance::Code::CODE_UNAUTHENTICATED,
        _ => connectrpc_conformance::Code::CODE_UNKNOWN,
    }
}

// ============================================================================
// Low-level transport helpers (kept from original)
// ============================================================================

/// Send an HTTP request over a connected IO stream.
async fn send_request<I>(
    io: I,
    request: Request<ClientBody>,
    http_version: HTTPVersion,
) -> Result<hyper::Response<hyper::body::Incoming>>
where
    I: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    match http_version {
        HTTPVersion::HTTP_VERSION_1 => {
            let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
            tokio::spawn(conn);
            Ok(sender.send_request(request).await?)
        }
        _ => {
            let (mut sender, conn) =
                hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .handshake(io)
                    .await?;
            tokio::spawn(conn);
            Ok(sender.send_request(request).await?)
        }
    }
}

/// Build a rustls ClientConfig that trusts the given PEM server CA cert,
/// optionally with client credentials for mTLS.
fn build_tls_client_config(
    server_ca_pem: &[u8],
    client_creds: Option<&(Vec<u8>, Vec<u8>)>,
    http_version: HTTPVersion,
) -> Result<rustls::ClientConfig> {
    let certs = rustls_pemfile::certs(&mut &*server_ca_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("failed to parse server CA PEM certs: {e}"))?;

    let mut root_store = rustls::RootCertStore::empty();
    for cert in certs {
        root_store
            .add(cert)
            .map_err(|e| anyhow::anyhow!("failed to add cert to root store: {e}"))?;
    }

    let builder = rustls::ClientConfig::builder().with_root_certificates(root_store);

    // mTLS: if client credentials are provided, present them to the server.
    let mut config = if let Some((client_cert_pem, client_key_pem)) = client_creds {
        let client_certs = rustls_pemfile::certs(&mut client_cert_pem.as_slice())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow::anyhow!("failed to parse client cert PEM: {e}"))?;
        let client_key = rustls_pemfile::private_key(&mut client_key_pem.as_slice())
            .map_err(|e| anyhow::anyhow!("failed to parse client key PEM: {e}"))?
            .ok_or_else(|| anyhow::anyhow!("no private key found in client key PEM"))?;
        builder
            .with_client_auth_cert(client_certs, client_key)
            .map_err(|e| anyhow::anyhow!("failed to set client auth cert: {e}"))?
    } else {
        builder.with_no_client_auth()
    };

    // Set ALPN protocols based on HTTP version
    config.alpn_protocols = match http_version {
        HTTPVersion::HTTP_VERSION_1 => vec![b"http/1.1".to_vec()],
        _ => vec![b"h2".to_vec()],
    };

    Ok(config)
}
