fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../rpc/proto/bench.proto");
    println!("cargo:rerun-if-changed=../rpc/proto/fortune.proto");
    println!("cargo:rerun-if-env-changed=GRPC_RUST_PROTOC_DIR");

    // Google protobuf messages + grpc-rust client stubs + tonic-protobuf
    // server stubs.
    grpc_protobuf_build::CodeGen::new()
        .include("../rpc/proto")
        .inputs(["bench.proto", "fortune.proto"])
        .compile()
        .expect("grpc-protobuf codegen failed");

    // tonic + prost client stubs for the client-stack bench, compiled with the
    // same protoc as the grpc-rust stubs: the cmake-built one, or the prebuilt
    // one from GRPC_RUST_PROTOC_DIR when the plugin build is disabled. Only if
    // neither applies does prost-build fall back to $PROTOC / PATH.
    let mut prost_config = tonic_prost_build::Config::new();
    #[cfg(feature = "build-protoc-plugin")]
    prost_config.protoc_executable(protoc_gen_rust_grpc::protoc());
    #[cfg(not(feature = "build-protoc-plugin"))]
    if let Some(dir) = std::env::var_os("GRPC_RUST_PROTOC_DIR") {
        prost_config.protoc_executable(std::path::Path::new(&dir).join("protoc"));
    }
    tonic_prost_build::configure()
        .build_server(false)
        .compile_with_config(
            prost_config,
            &["../rpc/proto/bench.proto"],
            &["../rpc/proto"],
        )
        .expect("tonic-prost codegen failed");
}
