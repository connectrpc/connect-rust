//! Protobuf JSON mapping. Kept in its own test binary because the buffa type
//! registry it installs is process-global.

use buffa::type_registry::{TypeRegistry, set_type_registry};
use buffa_types::google::protobuf::{Any, Duration};
use connectrpc_types::google::rpc::{self, Code, RetryInfo, Status};

#[test]
fn status_details_render_as_expanded_json_once_registered() {
    let mut registry = TypeRegistry::new();
    buffa_types::register_wkt_types(&mut registry);
    rpc::register_types(&mut registry);
    set_type_registry(registry);

    let retry = RetryInfo {
        retry_delay: buffa::MessageField::some(Duration {
            seconds: 2,
            nanos: 500_000_000,
            ..Default::default()
        }),
        ..Default::default()
    };
    let status = Status {
        code: Code::UNAVAILABLE as i32,
        message: "backend draining".to_owned(),
        details: vec![Any::pack(&retry, RetryInfo::TYPE_URL)],
        ..Default::default()
    };

    let json = serde_json::to_value(&status).expect("serialize Status");
    assert_eq!(
        json,
        serde_json::json!({
            "code": 14,
            "message": "backend draining",
            "details": [{
                "@type": "type.googleapis.com/google.rpc.RetryInfo",
                "retryDelay": "2.500s",
            }],
        })
    );

    let parsed: Status = serde_json::from_value(json).expect("deserialize Status");
    assert_eq!(parsed, status);
}
