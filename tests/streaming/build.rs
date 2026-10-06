fn main() {
    connectrpc_build::Config::new()
        .files(&[
            "proto/echo.proto",
            "proto/shadow_child.proto",
            "proto/shadow_services.proto",
            "proto/shadow_types.proto",
        ])
        .includes(&["proto/"])
        .include_file("_connectrpc.rs")
        .compile()
        .unwrap();
}
