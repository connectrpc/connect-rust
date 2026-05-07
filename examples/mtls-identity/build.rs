fn main() {
    connectrpc_build::Config::new()
        .files(&["proto/anthropic/connectrpc/mtls_identity/v1/identity.proto"])
        .includes(&["proto/"])
        .include_file("_connectrpc.rs")
        .compile()
        .unwrap();
}
