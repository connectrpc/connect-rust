fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../rpc/proto/bench.proto");
    println!("cargo:rerun-if-changed=../rpc/proto/fortune.proto");
    println!("cargo:rerun-if-env-changed=GRPC_RUST_PROTOC_DIR");

    grpc_protobuf_build::CodeGen::new()
        .include("../rpc/proto")
        .inputs(["bench.proto", "fortune.proto"])
        .compile()
        .expect("grpc-protobuf codegen failed");
}
