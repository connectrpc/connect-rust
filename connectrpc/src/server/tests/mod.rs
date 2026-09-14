use super::*;
use std::sync::Mutex;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

mod builders;
mod header_read_timeout;
mod http2;
mod max_connection_age;
mod max_connection_idle;
mod max_requests;
mod peer;
mod shutdown;

/// Hand-crafted Connect unary request (`POST /svc/Echo`, empty proto
/// body, `Connection: close`). Used by the peer-info tests to probe the
/// server over raw TCP/TLS without pulling in an HTTP client dep.
const ECHO_REQ: &[u8] = concat!(
    "POST /svc/Echo HTTP/1.1\r\n",
    "Host: localhost\r\n",
    "Content-Type: application/proto\r\n",
    "Content-Length: 0\r\n",
    "Connection: close\r\n",
    "\r\n",
)
.as_bytes();

const KEEPALIVE_ECHO_REQ: &[u8] = concat!(
    "POST /svc/Echo HTTP/1.1\r\n",
    "Host: localhost\r\n",
    "Content-Type: application/proto\r\n",
    "Content-Length: 0\r\n",
    "Connection: keep-alive\r\n",
    "\r\n",
)
.as_bytes();

/// Send one unary request over an h2 connection and await its response.
async fn send_unary(send_request: &mut h2::client::SendRequest<Bytes>, addr: SocketAddr) {
    let req = http::Request::builder()
        .method(http::Method::POST)
        .uri(format!("http://{addr}/svc/Unknown"))
        .body(())
        .unwrap();
    let (resp, _) = send_request.send_request(req, true).unwrap();
    resp.await.unwrap();
}

fn slow_router() -> (
    Router,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let chans = Arc::new(Mutex::new(Some((entered_tx, release_rx))));
    let router = Router::new().route(
        "svc",
        "Slow",
        crate::handler_fn(
            move |_ctx: crate::RequestContext, _req: buffa_types::Empty| {
                let chans = Arc::clone(&chans);
                async move {
                    let taken = chans.lock().unwrap().take();
                    if let Some((entered_tx, release_rx)) = taken {
                        entered_tx.send(()).ok();
                        release_rx.await.ok();
                    }
                    crate::Response::ok(buffa_types::Empty::default())
                }
            },
        ),
    );
    (router, entered_rx, release_tx)
}

async fn yield_to_tasks() {
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
}

async fn drain_h2_body(mut resp: http::Response<h2::RecvStream>) {
    while let Some(chunk) = resp.body_mut().data().await {
        chunk.expect("h2 response body failed");
    }
}

async fn read_http1_response(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut resp = Vec::new();
    let mut buf = [0; 1024];
    loop {
        let read = stream.read(&mut buf).await.unwrap();
        assert!(read > 0, "connection closed before full response arrived");
        resp.extend_from_slice(&buf[..read]);

        let Some(header_end) = find_header_end(&resp) else {
            continue;
        };
        let body_start = header_end + 4;
        let content_length = content_length(&resp[..header_end]).unwrap_or(0);
        if resp.len() >= body_start + content_length {
            return resp;
        }
    }
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn content_length(headers: &[u8]) -> Option<usize> {
    std::str::from_utf8(headers).ok()?.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse().ok())
            .flatten()
    })
}
