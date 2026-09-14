fn main() {
    connectrpc_build::Config::new()
        .files(&["proto/anthropic/connectrpc/custom_accept_loop/v1/placement.proto"])
        .includes(&["proto/"])
        .include_file("_connectrpc.rs")
        .compile()
        .unwrap();
}
