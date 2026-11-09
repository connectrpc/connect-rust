//! The property this crate exists for: `google.rpc` types that interoperate
//! with the rest of the ecosystem. The `google.protobuf` types they use must
//! be `buffa-types`' own, not a second copy generated here, so these assign
//! values built from `buffa_types` across the boundary rather than merely
//! constructing them — a locally generated `Any` or `Duration` would not
//! type-check.

use buffa::{Message, MessageField, MessageName};
use buffa_types::google::protobuf::{Any, Duration};
use connectrpc_types::google::rpc::{BadRequest, Code, ErrorInfo, RetryInfo, Status, bad_request};

fn retry_after(seconds: i64) -> RetryInfo {
    RetryInfo {
        retry_delay: MessageField::some(Duration {
            seconds,
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn status_details_take_a_buffa_types_any() {
    let status = Status {
        code: Code::UNAVAILABLE as i32,
        message: "try again".to_owned(),
        details: vec![Any::pack(&retry_after(5), RetryInfo::TYPE_URL)],
        ..Default::default()
    };

    assert_eq!(status.details.len(), 1);
}

#[test]
fn retry_info_takes_a_buffa_types_duration() {
    let retry = retry_after(5);

    assert_eq!(retry.retry_delay.as_option().map(|d| d.seconds), Some(5));
}

#[test]
fn the_error_details_payloads_are_all_here() {
    // `Status.details` is only useful if what goes in it exists, so the whole
    // package is generated rather than status.proto alone.
    let bad = BadRequest {
        field_violations: vec![bad_request::FieldViolation {
            field: "page_size".to_owned(),
            description: "must be between 1 and 100".to_owned(),
            ..Default::default()
        }],
        ..Default::default()
    };

    assert_eq!(bad.field_violations[0].field, "page_size");
}

#[test]
fn code_is_the_grpc_code_enum() {
    assert_eq!(Code::OK as i32, 0);
    assert_eq!(Code::NOT_FOUND as i32, 5);
    assert_eq!(Code::UNAVAILABLE as i32, 14);
    assert_eq!(Code::UNAUTHENTICATED as i32, 16);
}

#[test]
fn names_match_the_proto_package() {
    // `ErrorDetail::from_message` and `Any::pack` take these, and a peer in
    // another language resolves the detail by them.
    assert_eq!(Status::FULL_NAME, "google.rpc.Status");
    assert_eq!(
        bad_request::FieldViolation::FULL_NAME,
        "google.rpc.BadRequest.FieldViolation"
    );
    assert_eq!(
        RetryInfo::TYPE_URL,
        "type.googleapis.com/google.rpc.RetryInfo"
    );
}

#[test]
fn status_round_trips_on_the_wire_with_its_details() {
    let info = ErrorInfo {
        reason: "QUOTA_EXCEEDED".to_owned(),
        domain: "example.com".to_owned(),
        ..Default::default()
    };
    let status = Status {
        code: Code::RESOURCE_EXHAUSTED as i32,
        message: "quota exceeded".to_owned(),
        details: vec![
            Any::pack(&info, ErrorInfo::TYPE_URL),
            Any::pack(&retry_after(30), RetryInfo::TYPE_URL),
        ],
        ..Default::default()
    };

    let decoded = Status::decode_from_slice(&status.encode_to_vec()).expect("decode Status");
    assert_eq!(decoded, status);

    // Each detail unpacks by its type URL, and only as the type it names.
    let unpacked: Option<ErrorInfo> = decoded.details[0]
        .unpack_if(ErrorInfo::TYPE_URL)
        .expect("decode ErrorInfo");
    assert_eq!(unpacked, Some(info));
    let mismatched: Option<ErrorInfo> = decoded.details[1]
        .unpack_if(ErrorInfo::TYPE_URL)
        .expect("type URL mismatch is not an error");
    assert_eq!(mismatched, None);
}

#[cfg(feature = "views")]
#[test]
fn status_decodes_as_a_borrowing_view() {
    use buffa::view::MessageView;
    use connectrpc_types::google::rpc::StatusView;

    // The `grpc-status-details-bin` trailer is a serialized `Status`; a view
    // reads it without copying the message or the detail payloads.
    let status = Status {
        code: Code::NOT_FOUND as i32,
        message: "shelf 7 does not exist".to_owned(),
        details: vec![Any::pack(&retry_after(1), RetryInfo::TYPE_URL)],
        ..Default::default()
    };
    let bytes = status.encode_to_vec();

    let view = StatusView::decode_view(&bytes).expect("decode StatusView");
    assert_eq!(view.code, Code::NOT_FOUND as i32);
    assert_eq!(view.message, "shelf 7 does not exist");
    assert_eq!(view.details.len(), 1);
    assert_eq!(view.to_owned_message().expect("view to owned"), status);
}

#[test]
fn unknown_fields_survive_a_relay() {
    // A proxy that decodes a `Status` and re-encodes it must not drop fields
    // added by a newer schema. Field 99 is not in status.proto.
    let mut bytes = Status {
        code: Code::INTERNAL as i32,
        ..Default::default()
    }
    .encode_to_vec();
    bytes.extend_from_slice(&[0x98, 0x06, 0x2a]); // field 99, varint 42

    let relayed = Status::decode_from_slice(&bytes)
        .expect("decode Status")
        .encode_to_vec();
    assert_eq!(relayed, bytes);
}

#[test]
fn paths_match_what_extern_path_codegen_emits() {
    // A consumer generated with
    // `extern_path=.google.rpc=::connectrpc_types::google::rpc` names these
    // exact paths, owned types and views alike. They hold only while the
    // generated tree is mounted at the crate root, so a change to the mount
    // in lib.rs breaks this test rather than every consumer's build.
    use connectrpc_types::google::rpc;

    fn named<T>() {}
    named::<rpc::Status>();
    named::<rpc::Code>();
    named::<rpc::bad_request::FieldViolation>();
    #[cfg(feature = "views")]
    {
        named::<rpc::__buffa::view::StatusView<'static>>();
        named::<rpc::__buffa::view::bad_request::FieldViolationView<'static>>();
    }
}
