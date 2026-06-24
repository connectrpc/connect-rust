//! `ClientTransport` backed by the browser Fetch API via `web-sys`.

use bytes::Bytes;
use connectrpc::client::{ClientBody, ClientTransport};
use futures::future::BoxFuture;
use http::{Request, Response};
use http_body_util::BodyExt;
use send_wrapper::SendWrapper;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

/// Fetch-based transport for browser environments.
#[derive(Clone, Copy)]
pub struct FetchTransport;

/// Transport error preserving the original error source.
///
/// `JsValue` doesn't implement `Display` or `Error`, so the `Js` variant
/// eagerly converts to a string.
#[derive(Debug)]
pub enum FetchError {
    Js(String),
    Connect(Box<connectrpc::ConnectError>),
    Http(http::Error),
    HeaderToStr(http::header::ToStrError),
    InvalidStatusCode(http::status::InvalidStatusCode),
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Js(s) => f.write_str(s),
            Self::Connect(e) => write!(f, "{e}"),
            Self::Http(e) => write!(f, "{e}"),
            Self::HeaderToStr(e) => write!(f, "{e}"),
            Self::InvalidStatusCode(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for FetchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Js(_) => None,
            Self::Connect(e) => Some(e),
            Self::Http(e) => Some(e),
            Self::HeaderToStr(e) => Some(e),
            Self::InvalidStatusCode(e) => Some(e),
        }
    }
}

impl From<wasm_bindgen::JsValue> for FetchError {
    fn from(val: wasm_bindgen::JsValue) -> Self {
        Self::Js(val.as_string().unwrap_or_else(|| format!("{val:?}")))
    }
}

impl From<connectrpc::ConnectError> for FetchError {
    fn from(e: connectrpc::ConnectError) -> Self {
        Self::Connect(Box::new(e))
    }
}

impl From<http::Error> for FetchError {
    fn from(e: http::Error) -> Self {
        Self::Http(e)
    }
}

impl From<http::header::ToStrError> for FetchError {
    fn from(e: http::header::ToStrError) -> Self {
        Self::HeaderToStr(e)
    }
}

impl From<http::status::InvalidStatusCode> for FetchError {
    fn from(e: http::status::InvalidStatusCode) -> Self {
        Self::InvalidStatusCode(e)
    }
}

impl ClientTransport for FetchTransport {
    type ResponseBody = http_body_util::Full<Bytes>;
    type Error = FetchError;

    fn send(
        &self,
        request: Request<ClientBody>,
    ) -> BoxFuture<'static, Result<Response<Self::ResponseBody>, Self::Error>> {
        // SendWrapper bridges the Send bound on BoxFuture with web-sys's
        // !Send JS types. Safe on wasm32 because it is single-threaded.
        Box::pin(SendWrapper::new(fetch(request)))
    }
}

async fn fetch(
    request: Request<ClientBody>,
) -> Result<Response<http_body_util::Full<Bytes>>, FetchError> {
    // Build the request: collect the body and translate http types into
    // web-sys equivalents that the browser Fetch API expects.
    let (parts, body) = request.into_parts();
    let body_bytes = body.collect().await?.to_bytes();

    let headers = web_sys::Headers::new()?;
    for (name, value) in &parts.headers {
        let value = value.to_str()?;
        headers.append(name.as_str(), value)?;
    }

    let init = web_sys::RequestInit::new();
    init.set_method(parts.method.as_str());
    init.set_headers(&headers);
    init.set_body(&js_sys::Uint8Array::from(body_bytes.as_ref()));

    let js_req = web_sys::Request::new_with_str_and_init(&parts.uri.to_string(), &init)?;

    // Execute the fetch by looking up the global `fetch` function (works in
    // both window and worker scopes) and await the returned response.
    let global = js_sys::global();
    let fetch_fn: js_sys::Function = js_sys::Reflect::get(&global, &"fetch".into())?.dyn_into()?;
    let js_resp: web_sys::Response = JsFuture::from(
        fetch_fn
            .call1(&wasm_bindgen::JsValue::undefined(), &js_req)?
            .dyn_into::<js_sys::Promise>()?,
    )
    .await?
    .dyn_into()?;

    // Convert the JS response back into an http::Response: status code,
    // headers, and a fully-buffered body.
    let status = http::StatusCode::from_u16(js_resp.status())?;

    let mut builder = Response::builder().status(status);
    if let Some(iter) = js_sys::try_iter(&js_resp.headers())? {
        for entry in iter {
            let pair: js_sys::Array = entry?.into();
            let Some(key) = pair.get(0).as_string() else {
                continue;
            };
            let Some(val) = pair.get(1).as_string() else {
                continue;
            };
            // If the content-encoding is gzip, we skip it because the browser automatically
            // decompresses the response body, so we don't need to handle it ourselves. If we
            // didn't skip it, the client would try to decompress an already decompressed body,
            // which would result in an error.
            if key.to_lowercase() == "content-encoding" && val == "gzip" {
                continue;
            }
            builder = builder.header(key, val);
        }
    }

    let body_buf = JsFuture::from(js_resp.array_buffer()?).await?;
    let body_bytes = Bytes::from(js_sys::Uint8Array::new(&body_buf).to_vec());

    Ok(builder.body(http_body_util::Full::new(body_bytes))?)
}
