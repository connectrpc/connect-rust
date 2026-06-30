//! Shared types and helpers for the filter (redaction) benchmark.

use buffa::Map;

pub use crate::connect::anthropic::connectrpc::filter::v1::*;
pub use crate::proto::anthropic::connectrpc::filter::v1::{Record, RecordView};

/// True if any sensitive field is non-empty.
pub fn has_sensitive(r: &RecordView<'_>) -> bool {
    !r.email.is_empty() || !r.ssn.is_empty() || !r.notes.is_empty()
}

/// Clear sensitive fields in place.
pub fn scrub(r: &mut Record) {
    r.email.clear();
    r.ssn.clear();
    r.notes.clear();
}

/// Build a sample record. `sensitive` toggles whether `email` is set
/// (the trigger for the redact path).
pub fn sample_record(i: u32, sensitive: bool) -> Record {
    Record {
        id: format!("rec-{i:08}"),
        name: "Some Reasonably Long Display Name For Padding".into(),
        description: "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod".into(),
        email: if sensitive {
            "user@example.invalid".into()
        } else {
            String::new()
        },
        ssn: String::new(),
        notes: String::new(),
        tags: vec![
            "alpha".into(),
            "beta".into(),
            "gamma".into(),
            "delta".into(),
        ],
        attributes: Map::from_iter([
            ("region".into(), "us-west-2".into()),
            ("tier".into(), "gold".into()),
            ("source".into(), "api".into()),
        ]),
        ..Default::default()
    }
}

/// Build a batch of `n` records where `pct` percent have a sensitive
/// field set, interleaved across the batch.
///
/// The sensitive records are scattered via a coprime stride (`i * 37 %
/// n`) rather than front-loaded, so the `has_sensitive` branch in the
/// view path isn't trivially predictable. With `n == 100` the count is
/// exact (37 is coprime to 100, so the stride permutes 0..100).
pub fn sample_records(n: usize, pct: u32) -> Vec<Record> {
    let threshold = n * pct as usize / 100;
    (0..n)
        .map(|i| {
            let sensitive = (i * 37) % n < threshold;
            sample_record(i as u32, sensitive)
        })
        .collect()
}

/// Build a batch of `n` encoded request bytes where `pct` percent have a
/// sensitive field set. See [`sample_records`] for the interleave.
pub fn sample_batch(n: usize, pct: u32) -> Vec<bytes::Bytes> {
    use buffa::Message as _;
    sample_records(n, pct)
        .iter()
        .map(|r| r.encode_to_bytes())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_records_exact_count_and_interleaved() {
        for pct in [0, 10, 50, 100] {
            let recs = sample_records(100, pct);
            let sensitive: Vec<bool> = recs.iter().map(|r| !r.email.is_empty()).collect();
            assert_eq!(
                sensitive.iter().filter(|&&s| s).count(),
                pct as usize,
                "{pct}%"
            );
            if pct == 50 {
                // Not front-loaded: at least one of the first 10 is
                // clean and one of the last 10 is sensitive.
                assert!(sensitive[..10].iter().any(|&s| !s));
                assert!(sensitive[90..].iter().any(|&s| s));
            }
        }
    }
}
