//! Conformance server harness for connectrpc-rs.
//!
//! This binary implements the server-side conformance test protocol:
//! 1. Reads `ServerCompatRequest` from stdin
//! 2. Starts an HTTP server implementing `ConformanceService`
//! 3. Writes `ServerCompatResponse` with host:port to stdout
//! 4. Handles test RPC calls from the conformance runner

use std::sync::Arc;

use anyhow::Result;
use base64::Engine;
use buffa::Message;
use buffa_types::google::protobuf::Any;
use connectrpc::ConnectError;
use connectrpc::ConnectRpcService;
use connectrpc::Limits;
use connectrpc::Router;
use connectrpc::error::ErrorDetail;
use connectrpc::rustls;
use connectrpc::server::Server;
use connectrpc::{
    RequestContext, Response, ServiceRequest, ServiceResult, ServiceStream, StreamMessage,
};
use connectrpc_conformance::BidiStreamRequest;
use connectrpc_conformance::BidiStreamResponse;
use connectrpc_conformance::ClientStreamRequest;
use connectrpc_conformance::ClientStreamResponse;
use connectrpc_conformance::ConformancePayload;
use connectrpc_conformance::ConformanceService;
use connectrpc_conformance::ConformanceServiceExt;
use connectrpc_conformance::HTTPVersion;
use connectrpc_conformance::Header;
use connectrpc_conformance::IdempotentUnaryResponse;
use connectrpc_conformance::Protocol;
use connectrpc_conformance::ServerCompatRequest;
use connectrpc_conformance::ServerCompatResponse;
use connectrpc_conformance::ServerStreamResponse;
use connectrpc_conformance::StreamResponseDefinition;
use connectrpc_conformance::UnaryResponse;
use connectrpc_conformance::UnaryResponseDefinition;
use connectrpc_conformance::UnimplementedResponse;
use connectrpc_conformance::init_type_registry;
use connectrpc_conformance::read_message;
use connectrpc_conformance::write_message;
use connectrpc_conformance::{
    IdempotentUnaryRequest, ServerStreamRequest, UnaryRequest, UnimplementedRequest,
};
use futures::StreamExt;
use http::HeaderName;
use http::HeaderValue;
use tracing_subscriber::EnvFilter;

/// Parse a gRPC timeout header value to milliseconds.
///
/// Format: `<digits><unit>` where unit is H/M/S/m/u/n.
fn grpc_timeout_to_ms(s: &str) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let (digits, unit) = s.split_at(s.len() - 1);
    let value = digits.parse::<i64>().ok()?;
    match unit {
        "H" => Some(value * 3_600_000),
        "M" => Some(value * 60_000),
        "S" => Some(value * 1_000),
        "m" => Some(value),
        "u" => Some(value / 1_000),
        "n" => Some(value / 1_000_000),
        _ => None,
    }
}

/// Implementation of the ConformanceService.
struct ConformanceServiceImpl;

/// Build request info from the context and encoded request bytes.
fn build_request_info(
    ctx: &RequestContext,
    request_bytes: &[u8],
    type_url: &str,
) -> connectrpc_conformance::conformance_payload::RequestInfo {
    // Collect header values by name, merging duplicates into a single Header
    let mut header_map: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for (name, value) in ctx.headers().iter() {
        header_map
            .entry(name.to_string())
            .or_default()
            .push(value.to_str().unwrap_or("").to_string());
    }

    let request_headers: Vec<_> = header_map
        .into_iter()
        .map(|(name, value)| Header {
            name,
            value,
            ..Default::default()
        })
        .collect();

    // Extract timeout from protocol-specific header.
    // Connect uses connect-timeout-ms (value in milliseconds).
    // gRPC/gRPC-Web uses grpc-timeout (value with unit suffix, e.g., "5000m").
    let timeout_ms = ctx
        .headers()
        .get("connect-timeout-ms")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .or_else(|| {
            ctx.headers()
                .get("grpc-timeout")
                .and_then(|v| v.to_str().ok())
                .and_then(grpc_timeout_to_ms)
        });

    connectrpc_conformance::conformance_payload::RequestInfo {
        request_headers,
        timeout_ms,
        requests: vec![Any {
            type_url: type_url.to_string(),
            value: request_bytes.to_vec().into(),
            ..Default::default()
        }],
        connect_get_info: Default::default(),
        ..Default::default()
    }
}

/// Create an error detail containing the request info.
fn request_info_error_detail(
    request_info: connectrpc_conformance::conformance_payload::RequestInfo,
) -> ErrorDetail {
    // Serialize RequestInfo directly (not wrapped in ConformancePayload)
    let request_info_bytes = request_info.encode_to_vec();

    // Connect protocol requires unpadded base64 for error detail values
    // Use just the fully-qualified type name without the type.googleapis.com prefix
    ErrorDetail {
        type_url: "connectrpc.conformance.v1.ConformancePayload.RequestInfo".to_string(),
        value: Some(base64::engine::general_purpose::STANDARD_NO_PAD.encode(&request_info_bytes)),
        debug: None,
    }
}

/// Apply response delay if specified in the definition (for timeout testing).
async fn apply_response_delay(def: &UnaryResponseDefinition) {
    if def.response_delay_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(
            def.response_delay_ms as u64,
        ))
        .await;
    }
}

/// Convert the proto `Header` list to an `http::HeaderMap`.
fn to_header_map(headers: &[Header]) -> http::HeaderMap {
    let mut out = http::HeaderMap::new();
    for header in headers {
        if let Ok(name) = HeaderName::try_from(&header.name) {
            for val in &header.value {
                if let Ok(value) = HeaderValue::try_from(val) {
                    out.append(name.clone(), value);
                }
            }
        }
    }
    out
}

/// Wrap a body with the response headers/trailers from the unary definition.
fn unary_response<B>(body: B, def: &UnaryResponseDefinition) -> Response<B> {
    let mut resp = Response::new(body);
    resp.headers = to_header_map(&def.response_headers);
    resp.trailers = to_header_map(&def.response_trailers);
    resp
}

/// Wrap a body with the response headers/trailers from the stream definition.
fn stream_response<B>(body: B, def: &StreamResponseDefinition) -> Response<B> {
    let mut resp = Response::new(body);
    resp.headers = to_header_map(&def.response_headers);
    resp.trailers = to_header_map(&def.response_trailers);
    resp
}

impl ConformanceService for ConformanceServiceImpl {
    async fn unary(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UnaryRequest>,
    ) -> ServiceResult<UnaryResponse> {
        tracing::debug!("Received unary request");

        // Build request info with headers
        let request_info = build_request_info(
            &ctx,
            request.bytes(),
            "type.googleapis.com/connectrpc.conformance.v1.UnaryRequest",
        );

        let request = request.to_owned_message();
        let def = request.response_definition.as_option();

        if let Some(def) = def {
            apply_response_delay(def).await;
        }

        let data = match def.and_then(|d| d.response.as_ref()) {
            Some(connectrpc_conformance::unary_response_definition::Response::ResponseData(d)) => {
                d.clone()
            }
            Some(connectrpc_conformance::unary_response_definition::Response::Error(e)) => {
                let def = def.unwrap();
                let code = e
                    .code
                    .as_known()
                    .unwrap_or(connectrpc_conformance::Code::CODE_INTERNAL);
                let msg = e.message.clone().unwrap_or_default();
                return Err(ConnectError::new(code_to_connect_error(code), msg)
                    .with_detail(request_info_error_detail(request_info))
                    .with_headers(to_header_map(&def.response_headers))
                    .with_trailers(to_header_map(&def.response_trailers)));
            }
            None => vec![],
        };

        let payload = ConformancePayload {
            data,
            request_info: request_info.into(),
            ..Default::default()
        };

        let body = UnaryResponse {
            payload: payload.into(),
            ..Default::default()
        };

        Ok(match def {
            Some(def) => unary_response(body, def),
            None => body.into(),
        })
    }

    async fn idempotent_unary(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, IdempotentUnaryRequest>,
    ) -> ServiceResult<IdempotentUnaryResponse> {
        tracing::debug!("Received idempotent_unary request");

        // Build request info with headers
        let request_info = build_request_info(
            &ctx,
            request.bytes(),
            "type.googleapis.com/connectrpc.conformance.v1.IdempotentUnaryRequest",
        );

        let request = request.to_owned_message();
        let def = request.response_definition.as_option();

        if let Some(def) = def {
            apply_response_delay(def).await;
        }

        let data = match def.and_then(|d| d.response.as_ref()) {
            Some(connectrpc_conformance::unary_response_definition::Response::ResponseData(d)) => {
                d.clone()
            }
            Some(connectrpc_conformance::unary_response_definition::Response::Error(e)) => {
                let def = def.unwrap();
                let code = e
                    .code
                    .as_known()
                    .unwrap_or(connectrpc_conformance::Code::CODE_INTERNAL);
                let msg = e.message.clone().unwrap_or_default();
                return Err(ConnectError::new(code_to_connect_error(code), msg)
                    .with_detail(request_info_error_detail(request_info))
                    .with_headers(to_header_map(&def.response_headers))
                    .with_trailers(to_header_map(&def.response_trailers)));
            }
            None => vec![],
        };

        let payload = ConformancePayload {
            data,
            request_info: request_info.into(),
            ..Default::default()
        };

        let body = IdempotentUnaryResponse {
            payload: payload.into(),
            ..Default::default()
        };

        Ok(match def {
            Some(def) => unary_response(body, def),
            None => body.into(),
        })
    }

    async fn unimplemented(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, UnimplementedRequest>,
    ) -> ServiceResult<UnimplementedResponse> {
        // This endpoint should always return unimplemented
        Err(ConnectError::unimplemented(
            "ConformanceService.Unimplemented is not implemented",
        ))
    }

    async fn server_stream(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ServerStreamRequest>,
    ) -> ServiceResult<ServiceStream<ServerStreamResponse>> {
        tracing::debug!("Received server_stream request");

        // Build request info with headers
        let request_info = build_request_info(
            &ctx,
            request.bytes(),
            "type.googleapis.com/connectrpc.conformance.v1.ServerStreamRequest",
        );

        let request = request.to_owned_message();

        // Handle response definition
        if !request.response_definition.is_set() {
            // No response definition - return empty stream
            return Response::stream_ok(futures::stream::empty());
        }
        let def = &*request.response_definition;

        // Get response data
        let response_data = def.response_data.clone();
        let response_delay_ms = def.response_delay_ms;
        let error_def = if def.error.is_set() {
            Some(def.error.as_option().unwrap().clone())
        } else {
            None
        };

        // If no response data and error is specified, return immediate error
        if response_data.is_empty() {
            if let Some(e) = &error_def {
                let code = e
                    .code
                    .as_known()
                    .unwrap_or(connectrpc_conformance::Code::CODE_INTERNAL);
                let msg = e.message.clone().unwrap_or_default();
                let error = ConnectError::new(code_to_connect_error(code), msg)
                    .with_detail(request_info_error_detail(request_info))
                    .with_headers(to_header_map(&def.response_headers))
                    .with_trailers(to_header_map(&def.response_trailers));
                return Err(error);
            }

            // No error, return empty stream with headers/trailers from def
            return Ok(stream_response(
                Box::pin(futures::stream::empty()) as ServiceStream<_>,
                def,
            ));
        }

        // Create stream that yields responses
        let stream = futures::stream::unfold(
            (
                response_data,
                request_info,
                error_def,
                response_delay_ms,
                0usize,
            ),
            async move |(data, req_info, err, delay_ms, idx)| {
                // Apply delay before each response
                if delay_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms as u64)).await;
                }

                if idx < data.len() {
                    // Build response with payload
                    let payload = if idx == 0 {
                        // First response includes request info
                        ConformancePayload {
                            data: data[idx].clone(),
                            request_info: req_info.clone().into(),
                            ..Default::default()
                        }
                    } else {
                        // Subsequent responses only include data
                        ConformancePayload {
                            data: data[idx].clone(),
                            request_info: Default::default(),
                            ..Default::default()
                        }
                    };

                    let response = ServerStreamResponse {
                        payload: payload.into(),
                        ..Default::default()
                    };

                    Some((Ok(response), (data, req_info, err, delay_ms, idx + 1)))
                } else if let Some(e) = err {
                    // After all responses, yield error if specified
                    let code = e
                        .code
                        .as_known()
                        .unwrap_or(connectrpc_conformance::Code::CODE_INTERNAL);
                    let msg = e.message.clone().unwrap_or_default();
                    let error = ConnectError::new(code_to_connect_error(code), msg);
                    Some((Err(error), (data, req_info, None, delay_ms, idx + 1)))
                } else {
                    // End of stream
                    None
                }
            },
        );

        Ok(stream_response(Box::pin(stream) as ServiceStream<_>, def))
    }

    async fn client_stream(
        &self,
        ctx: RequestContext,
        requests: ServiceStream<StreamMessage<ClientStreamRequest>>,
    ) -> ServiceResult<ClientStreamResponse> {
        tracing::debug!("Received client_stream request");

        // The conformance protocol requires all request messages to be available
        // before building the response (e.g. to report per-message details), so we
        // collect the entire stream up front. This is expected -- it doesn't exercise
        // truly incremental streaming, but the underlying dispatch *does* deliver
        // messages incrementally via a channel-backed stream.
        let mut all_requests: Vec<(Vec<u8>, ClientStreamRequest)> = Vec::new();
        let mut requests = requests;
        while let Some(r) = requests.next().await {
            let item = r?;
            let bytes = item.bytes().to_vec();
            let msg = item.to_owned_message();
            all_requests.push((bytes, msg));
        }

        // Build request info from all messages
        let request_headers: Vec<_> = {
            let mut header_map: std::collections::HashMap<String, Vec<String>> =
                std::collections::HashMap::new();
            for (name, value) in ctx.headers().iter() {
                header_map
                    .entry(name.to_string())
                    .or_default()
                    .push(value.to_str().unwrap_or("").to_string());
            }
            header_map
                .into_iter()
                .map(|(name, value)| Header {
                    name,
                    value,
                    ..Default::default()
                })
                .collect()
        };

        let timeout_ms = ctx
            .headers()
            .get("connect-timeout-ms")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i64>().ok())
            .or_else(|| {
                ctx.headers()
                    .get("grpc-timeout")
                    .and_then(|v| v.to_str().ok())
                    .and_then(grpc_timeout_to_ms)
            });

        let request_info = connectrpc_conformance::conformance_payload::RequestInfo {
            request_headers,
            timeout_ms,
            requests: all_requests
                .iter()
                .map(|(bytes, _)| Any {
                    type_url: "type.googleapis.com/connectrpc.conformance.v1.ClientStreamRequest"
                        .to_string(),
                    value: bytes.clone().into(),
                    ..Default::default()
                })
                .collect(),
            connect_get_info: Default::default(),
            ..Default::default()
        };

        // The first message's response_definition tells us how to respond
        let def = all_requests.first().and_then(|(_, r)| {
            if r.response_definition.is_set() {
                Some(r.response_definition.as_option().unwrap().clone())
            } else {
                None
            }
        });

        if let Some(def) = &def {
            apply_response_delay(def).await;
        }

        let data = match def.as_ref().and_then(|d| d.response.as_ref()) {
            Some(connectrpc_conformance::unary_response_definition::Response::ResponseData(d)) => {
                d.clone()
            }
            Some(connectrpc_conformance::unary_response_definition::Response::Error(e)) => {
                let def = def.as_ref().unwrap();
                let code = e
                    .code
                    .as_known()
                    .unwrap_or(connectrpc_conformance::Code::CODE_INTERNAL);
                let msg = e.message.clone().unwrap_or_default();
                return Err(ConnectError::new(code_to_connect_error(code), msg)
                    .with_detail(request_info_error_detail(request_info))
                    .with_headers(to_header_map(&def.response_headers))
                    .with_trailers(to_header_map(&def.response_trailers)));
            }
            None => vec![],
        };

        let payload = ConformancePayload {
            data,
            request_info: request_info.into(),
            ..Default::default()
        };

        let body = ClientStreamResponse {
            payload: payload.into(),
            ..Default::default()
        };

        Ok(match &def {
            Some(def) => unary_response(body, def),
            None => body.into(),
        })
    }

    async fn bidi_stream(
        &self,
        ctx: RequestContext,
        requests: ServiceStream<StreamMessage<BidiStreamRequest>>,
    ) -> ServiceResult<ServiceStream<BidiStreamResponse>> {
        tracing::debug!("Received bidi_stream request");

        // Convert StreamMessage items to owned types. The conformance protocol
        // requires per-message byte buffers for the response payload, so we
        // need the owned form here regardless.
        let mut requests = requests.map(|r| {
            let item = r?;
            let bytes = item.bytes().to_vec();
            let msg = item.to_owned_message();
            Ok((bytes, msg))
        });

        // We need to read at least the first message to get the response definition
        // and full_duplex flag.
        let (first_msg_bytes, first_msg) = match requests.next().await {
            Some(Ok(pair)) => pair,
            Some(Err(e)) => return Err(e),
            None => {
                // Empty request stream — return empty response stream
                return Response::stream_ok(futures::stream::empty());
            }
        };

        let def = if first_msg.response_definition.is_set() {
            Some(first_msg.response_definition.as_option().unwrap().clone())
        } else {
            None
        };
        let full_duplex = first_msg.full_duplex;

        // Response headers/trailers from the definition; applied to whichever
        // Response<B> arm we return.
        let wrap = |stream: ServiceStream<BidiStreamResponse>| match &def {
            Some(d) => stream_response(stream, d),
            None => Response::new(stream),
        };
        let (resp_headers, resp_trailers) = match &def {
            Some(d) => (
                to_header_map(&d.response_headers),
                to_header_map(&d.response_trailers),
            ),
            None => (http::HeaderMap::new(), http::HeaderMap::new()),
        };

        // Build initial request info from headers
        let request_headers: Vec<_> = {
            let mut header_map: std::collections::HashMap<String, Vec<String>> =
                std::collections::HashMap::new();
            for (name, value) in ctx.headers().iter() {
                header_map
                    .entry(name.to_string())
                    .or_default()
                    .push(value.to_str().unwrap_or("").to_string());
            }
            header_map
                .into_iter()
                .map(|(name, value)| Header {
                    name,
                    value,
                    ..Default::default()
                })
                .collect()
        };

        let timeout_ms = ctx
            .headers()
            .get("connect-timeout-ms")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i64>().ok())
            .or_else(|| {
                ctx.headers()
                    .get("grpc-timeout")
                    .and_then(|v| v.to_str().ok())
                    .and_then(grpc_timeout_to_ms)
            });

        if full_duplex {
            // Full-duplex mode: for each request, immediately send the corresponding
            // response(s). Echo request properties in each response.
            let response_data = def
                .as_ref()
                .map(|d| d.response_data.clone())
                .unwrap_or_default();
            let response_delay_ms = def.as_ref().map(|d| d.response_delay_ms).unwrap_or(0);
            let error_def = def.as_ref().and_then(|d| {
                if d.error.is_set() {
                    Some(d.error.as_option().unwrap().clone())
                } else {
                    None
                }
            });

            // If no response data and error is specified, return immediate error
            if response_data.is_empty()
                && let Some(e) = &error_def
            {
                let request_info = connectrpc_conformance::conformance_payload::RequestInfo {
                    request_headers: request_headers.clone(),
                    timeout_ms,
                    requests: vec![Any {
                        type_url: "type.googleapis.com/connectrpc.conformance.v1.BidiStreamRequest"
                            .to_string(),
                        value: first_msg_bytes.into(),
                        ..Default::default()
                    }],
                    connect_get_info: Default::default(),
                    ..Default::default()
                };
                let code = e
                    .code
                    .as_known()
                    .unwrap_or(connectrpc_conformance::Code::CODE_INTERNAL);
                let msg = e.message.clone().unwrap_or_default();
                let error = ConnectError::new(code_to_connect_error(code), msg)
                    .with_detail(request_info_error_detail(request_info))
                    .with_headers(resp_headers)
                    .with_trailers(resp_trailers);
                return Err(error);
            }

            // We need to yield responses for the first message, then for each subsequent message.
            // Build a stream that processes request messages one at a time.
            let stream = futures::stream::unfold(
                (
                    requests,
                    Some((first_msg_bytes, first_msg)), // first message to process
                    request_headers,
                    timeout_ms,
                    response_data,
                    response_delay_ms,
                    error_def,
                    0usize, // response data index
                    0usize, // request count (how many requests we've seen)
                    false,  // done
                ),
                async move |(
                    mut reqs,
                    pending_msg,
                    req_headers,
                    timeout,
                    resp_data,
                    delay_ms,
                    err_def,
                    mut data_idx,
                    mut req_count,
                    done,
                )| {
                    if done {
                        return None;
                    }

                    // Get the next request message to process
                    let (msg_bytes, _msg) = if let Some(pair) = pending_msg {
                        pair
                    } else {
                        match reqs.next().await {
                            Some(Ok(pair)) => pair,
                            Some(Err(e)) => {
                                return Some((
                                    Err(e),
                                    (
                                        reqs,
                                        None,
                                        req_headers,
                                        timeout,
                                        resp_data,
                                        delay_ms,
                                        err_def,
                                        data_idx,
                                        req_count,
                                        true,
                                    ),
                                ));
                            }
                            None => {
                                // No more requests. If there's an error to send, do it now.
                                if let Some(e) = err_def {
                                    let code = e
                                        .code
                                        .as_known()
                                        .unwrap_or(connectrpc_conformance::Code::CODE_INTERNAL);
                                    let msg = e.message.clone().unwrap_or_default();
                                    let error = ConnectError::new(code_to_connect_error(code), msg);
                                    return Some((
                                        Err(error),
                                        (
                                            reqs,
                                            None,
                                            req_headers,
                                            timeout,
                                            resp_data,
                                            delay_ms,
                                            None,
                                            data_idx,
                                            req_count,
                                            true,
                                        ),
                                    ));
                                }
                                return None;
                            }
                        }
                    };

                    req_count += 1;

                    // Apply delay before response
                    if delay_ms > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms as u64)).await;
                    }

                    // Build request info for this response
                    let request_info = if req_count == 1 {
                        // First response includes full request info
                        Some(connectrpc_conformance::conformance_payload::RequestInfo {
                            request_headers: req_headers.clone(),
                            timeout_ms: timeout,
                            requests: vec![Any {
                                type_url: "type.googleapis.com/connectrpc.conformance.v1.BidiStreamRequest".to_string(),
                                value: msg_bytes.into(),
                                ..Default::default()
                            }],
                            connect_get_info: Default::default(),
                            ..Default::default()
                        })
                    } else {
                        // Subsequent responses include request info with just this message
                        Some(connectrpc_conformance::conformance_payload::RequestInfo {
                            request_headers: vec![],
                            timeout_ms: None,
                            requests: vec![Any {
                                type_url: "type.googleapis.com/connectrpc.conformance.v1.BidiStreamRequest".to_string(),
                                value: msg_bytes.into(),
                                ..Default::default()
                            }],
                            connect_get_info: Default::default(),
                            ..Default::default()
                        })
                    };

                    // Get response data for this index
                    let data = if data_idx < resp_data.len() {
                        resp_data[data_idx].clone()
                    } else {
                        vec![]
                    };
                    data_idx += 1;

                    let payload = ConformancePayload {
                        data,
                        request_info: request_info.map(|ri| ri.into()).unwrap_or_default(),
                        ..Default::default()
                    };

                    let response = BidiStreamResponse {
                        payload: payload.into(),
                        ..Default::default()
                    };

                    Some((
                        Ok(response),
                        (
                            reqs,
                            None,
                            req_headers,
                            timeout,
                            resp_data,
                            delay_ms,
                            err_def,
                            data_idx,
                            req_count,
                            false,
                        ),
                    ))
                },
            );

            Ok(wrap(Box::pin(stream)))
        } else {
            // Half-duplex mode: collect all requests first, then send responses.
            // Echo all request properties in the first response only.
            let mut all_requests: Vec<(Vec<u8>, BidiStreamRequest)> =
                vec![(first_msg_bytes, first_msg)];
            while let Some(result) = requests.next().await {
                match result {
                    Ok(pair) => {
                        all_requests.push(pair);
                    }
                    Err(e) => return Err(e),
                }
            }

            let request_info = connectrpc_conformance::conformance_payload::RequestInfo {
                request_headers,
                timeout_ms,
                requests: all_requests
                    .iter()
                    .map(|(bytes, _)| Any {
                        type_url: "type.googleapis.com/connectrpc.conformance.v1.BidiStreamRequest"
                            .to_string(),
                        value: bytes.clone().into(),
                        ..Default::default()
                    })
                    .collect(),
                connect_get_info: Default::default(),
                ..Default::default()
            };

            let Some(ref def) = def else {
                // No response definition — return empty stream
                return Ok(wrap(Box::pin(futures::stream::empty())));
            };

            let response_data = def.response_data.clone();
            let response_delay_ms = def.response_delay_ms;
            let error_def = if def.error.is_set() {
                Some(def.error.as_option().unwrap().clone())
            } else {
                None
            };

            // If no response data and error is specified, return immediate error
            if response_data.is_empty() {
                if let Some(e) = &error_def {
                    let code = e
                        .code
                        .as_known()
                        .unwrap_or(connectrpc_conformance::Code::CODE_INTERNAL);
                    let msg = e.message.clone().unwrap_or_default();
                    let error = ConnectError::new(code_to_connect_error(code), msg)
                        .with_detail(request_info_error_detail(request_info))
                        .with_headers(resp_headers)
                        .with_trailers(resp_trailers);
                    return Err(error);
                }

                // No error, return empty stream
                return Ok(wrap(Box::pin(futures::stream::empty())));
            }

            // Create stream that yields responses
            let stream = futures::stream::unfold(
                (
                    response_data,
                    request_info,
                    error_def,
                    response_delay_ms,
                    0usize,
                ),
                async move |(data, req_info, err, delay_ms, idx)| {
                    // Apply delay before each response
                    if delay_ms > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms as u64)).await;
                    }

                    if idx < data.len() {
                        let payload = if idx == 0 {
                            // First response includes request info
                            ConformancePayload {
                                data: data[idx].clone(),
                                request_info: req_info.clone().into(),
                                ..Default::default()
                            }
                        } else {
                            ConformancePayload {
                                data: data[idx].clone(),
                                request_info: Default::default(),
                                ..Default::default()
                            }
                        };

                        let response = BidiStreamResponse {
                            payload: payload.into(),
                            ..Default::default()
                        };

                        Some((Ok(response), (data, req_info, err, delay_ms, idx + 1)))
                    } else if let Some(e) = err {
                        let code = e
                            .code
                            .as_known()
                            .unwrap_or(connectrpc_conformance::Code::CODE_INTERNAL);
                        let msg = e.message.clone().unwrap_or_default();
                        let error = ConnectError::new(code_to_connect_error(code), msg);
                        Some((Err(error), (data, req_info, None, delay_ms, idx + 1)))
                    } else {
                        None
                    }
                },
            );

            Ok(wrap(Box::pin(stream)))
        }
    }
}

fn code_to_connect_error(code: connectrpc_conformance::Code) -> connectrpc::error::ErrorCode {
    use connectrpc::error::ErrorCode;
    use connectrpc_conformance::Code;

    match code {
        Code::CODE_CANCELED => ErrorCode::Canceled,
        Code::CODE_UNKNOWN => ErrorCode::Unknown,
        Code::CODE_INVALID_ARGUMENT => ErrorCode::InvalidArgument,
        Code::CODE_DEADLINE_EXCEEDED => ErrorCode::DeadlineExceeded,
        Code::CODE_NOT_FOUND => ErrorCode::NotFound,
        Code::CODE_ALREADY_EXISTS => ErrorCode::AlreadyExists,
        Code::CODE_PERMISSION_DENIED => ErrorCode::PermissionDenied,
        Code::CODE_RESOURCE_EXHAUSTED => ErrorCode::ResourceExhausted,
        Code::CODE_FAILED_PRECONDITION => ErrorCode::FailedPrecondition,
        Code::CODE_ABORTED => ErrorCode::Aborted,
        Code::CODE_OUT_OF_RANGE => ErrorCode::OutOfRange,
        Code::CODE_UNIMPLEMENTED => ErrorCode::Unimplemented,
        Code::CODE_INTERNAL => ErrorCode::Internal,
        Code::CODE_UNAVAILABLE => ErrorCode::Unavailable,
        Code::CODE_DATA_LOSS => ErrorCode::DataLoss,
        Code::CODE_UNAUTHENTICATED => ErrorCode::Unauthenticated,
        Code::CODE_UNSPECIFIED => ErrorCode::Unknown,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging to stderr (stdout is used for protocol)
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("debug".parse()?))
        .with_writer(std::io::stderr)
        .init();

    tracing::info!("Conformance server starting");

    // Initialize the Any type registry so that google.protobuf.Any fields
    // in conformance responses are serialized with inlined message fields
    // (proto3 canonical JSON) rather than a raw "value" base64 fallback.
    init_type_registry();

    // Read the server configuration from stdin
    let request: ServerCompatRequest =
        read_message()?.ok_or_else(|| anyhow::anyhow!("Expected ServerCompatRequest on stdin"))?;

    tracing::info!(
        "Received config: protocol={:?}, http_version={:?}, use_tls={}",
        request
            .protocol
            .as_known()
            .unwrap_or(Protocol::PROTOCOL_UNSPECIFIED),
        request
            .http_version
            .as_known()
            .unwrap_or(HTTPVersion::HTTP_VERSION_UNSPECIFIED),
        request.use_tls
    );

    let _protocol = request
        .protocol
        .as_known()
        .unwrap_or(Protocol::PROTOCOL_UNSPECIFIED);

    // Parse TLS credentials if requested
    let tls_config = if request.use_tls {
        let creds = request
            .server_creds
            .as_option()
            .ok_or_else(|| anyhow::anyhow!("use_tls is true but no server_creds provided"))?;

        let certs = rustls_pemfile::certs(&mut creds.cert.as_slice())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow::anyhow!("failed to parse PEM certificates: {e}"))?;

        let key = rustls_pemfile::private_key(&mut creds.key.as_slice())
            .map_err(|e| anyhow::anyhow!("failed to parse PEM private key: {e}"))?
            .ok_or_else(|| anyhow::anyhow!("no private key found in PEM data"))?;

        // mTLS: if the conformance runner provides a client CA cert, build
        // a verifier that requires and validates client certificates against
        // it. Otherwise, no client auth (standard TLS).
        let mut config = if !request.client_tls_cert.is_empty() {
            let client_ca_certs = rustls_pemfile::certs(&mut request.client_tls_cert.as_slice())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| anyhow::anyhow!("failed to parse client CA cert PEM: {e}"))?;
            let mut client_roots = rustls::RootCertStore::empty();
            for ca in client_ca_certs {
                client_roots
                    .add(ca)
                    .map_err(|e| anyhow::anyhow!("failed to add client CA to root store: {e}"))?;
            }
            let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(client_roots))
                .build()
                .map_err(|e| anyhow::anyhow!("failed to build client cert verifier: {e}"))?;
            rustls::ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .map_err(|e| anyhow::anyhow!("failed to build TLS config (mTLS): {e}"))?
        } else {
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| anyhow::anyhow!("failed to build TLS config: {e}"))?
        };

        // Advertise h2 and http/1.1 via ALPN so clients can negotiate HTTP/2
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

        Some(Arc::new(config))
    } else {
        None
    };

    // Create the conformance service
    let service = Arc::new(ConformanceServiceImpl);

    // Build the router using the new service.register(router) pattern
    let router = service.register(Router::new());

    // Bind to an available port using our custom server
    let mut bound = Server::bind("127.0.0.1:0")
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    if let Some(config) = &tls_config {
        bound = bound.with_tls(Arc::clone(config));
    }
    let server_addr = bound.local_addr()?;
    tracing::info!("Server listening on {}", server_addr);

    // Include the PEM cert if TLS is enabled so the runner can trust our server
    let pem_cert = if request.server_creds.is_set() {
        request.server_creds.cert.clone()
    } else {
        vec![]
    };

    let response = ServerCompatResponse {
        host: "127.0.0.1".to_string(),
        port: server_addr.port() as u32,
        pem_cert,
        ..Default::default()
    };
    write_message(&response)?;

    tracing::info!(
        "Sent ServerCompatResponse (port {}, tls={}), starting to serve requests",
        server_addr.port(),
        tls_config.is_some()
    );

    // Configure message size limits if requested by the conformance runner
    let limits = if request.message_receive_limit > 0 {
        Limits::default().max_message_size(request.message_receive_limit as usize)
    } else {
        Limits::default()
    };

    // Serve requests using our custom hyper-based server
    let connect_service = ConnectRpcService::new(router).with_limits(limits);
    bound
        .serve_with_service(connect_service)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    Ok(())
}
