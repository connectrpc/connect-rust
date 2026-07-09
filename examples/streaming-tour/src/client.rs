//! Streaming-tour client: calls each NumberService RPC and prints the
//! result. Pair with `streaming-tour-server`.

use connectrpc::client::{ClientConfig, HttpClient};

pub mod proto {
    connectrpc::include_generated!();
}

use proto::anthropic::connectrpc::tour::v1::*;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let url = std::env::var("TOUR_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".into());
    let base_uri: http::Uri = url.parse()?;

    let http = HttpClient::plaintext();
    let config = ClientConfig::new(base_uri);
    let client = NumberServiceClient::new(http, config);

    // --- Unary ---
    // Edition 2023 default presence is EXPLICIT, so scalar fields are
    // Option<T>: wrap on the way in, unwrap_or_default() on the way out.
    let resp = client
        .square(SquareRequest {
            value: Some(7),
            ..Default::default()
        })
        .await?;
    println!("Square(7) -> {}", resp.view().squared.unwrap_or_default());

    // --- Server streaming ---
    let mut range = client
        .range(RangeRequest {
            start: Some(10),
            count: Some(5),
            ..Default::default()
        })
        .await?;
    print!("Range(start=10, count=5) -> [");
    let mut first = true;
    while let Some(msg) = range.message().await? {
        if !first {
            print!(", ");
        }
        print!("{}", msg.view().value.unwrap_or_default());
        first = false;
    }
    println!("]");

    // --- Client streaming ---
    // `sum` takes an async stream of requests; adapt the in-hand array
    // with `stream_iter` (a live producer would pass a channel-backed
    // stream instead).
    let inputs = [3, 5, 7, 9];
    let messages = connectrpc::client::stream_iter(inputs.map(|v| SumRequest {
        value: Some(v),
        ..Default::default()
    }));
    let resp = client.sum(messages).await?;
    println!(
        "Sum({inputs:?}) -> {}",
        resp.view().total.unwrap_or_default()
    );

    // --- Bidirectional streaming ---
    let mut bidi = client.running_sum().await?;
    print!("RunningSum([2, 4, 6, 8]) -> [");
    let mut first = true;
    for v in [2, 4, 6, 8] {
        bidi.send(RunningSumRequest {
            value: Some(v),
            ..Default::default()
        })
        .await?;
        if let Some(msg) = bidi.message().await? {
            if !first {
                print!(", ");
            }
            print!("{}", msg.view().total.unwrap_or_default());
            first = false;
        }
    }
    bidi.close_send();
    println!("]");

    Ok(())
}
