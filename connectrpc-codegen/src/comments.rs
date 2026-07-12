//! Sanitization of proto source comments for rustdoc emission.
//!
//! Ported from buffa-codegen's crate-private comment sanitizer (as of buffa
//! v0.8.1, `buffa-codegen/src/comments.rs`) so that service and method
//! comments get the same treatment buffa gives message and field comments:
//! user-written markdown fences pass through, indented blocks are fenced as
//! `text`, and markdown/HTML metacharacters in prose are escaped so
//! arbitrary proto comments cannot break a consumer's `cargo doc`
//! (intra-doc-link and HTML-tag lints) or inadvertently become doctests.
//! Several deliberate divergences harden on buffa's behavior: every code
//! fence is made inert so rustdoc can never compile comment content as a
//! doctest (see [`fence_info`]), an unterminated fence is closed at the end
//! of the comment, and fence open/close detection follows CommonMark
//! (closers need matching tick counts and no info string; markers indented
//! 4+ spaces are code, not fences). This
//! module is transitional: if buffa exports an equivalently hardened
//! sanitizer publicly, this fork should be deleted in favor of it.
//! Proto cross-reference resolution
//! (`[display][ref]`) is not ported — connect codegen has no proto→Rust
//! type map in service scope — so bracketed refs fall back to escaped
//! literal text, which is also buffa's fallback for unresolvable refs.

/// Sanitize a proto comment for embedding in `#[doc]` attributes.
///
/// Line-oriented text transform: the output is joined with `\n` and carries
/// no leading-space normalization — `doc_attrs` adds the uniform single
/// space prettyplease needs. Handles, in priority order per line:
///
/// - user-written ``` fences: content passes through unescaped, while the
///   opener's info string is rewritten by [`fence_info`] so rustdoc never
///   compiles it; an unterminated fence is closed at the end of the
///   comment;
/// - indented blocks (4 spaces / tab): wrapped in a ```` ```text ````
///   fence and de-indented, so protoc-style examples render as code and
///   can never be run as doctests;
/// - blank lines: preserved (paragraph breaks);
/// - prose: escaped via [`sanitize_line`].
pub(crate) fn sanitize_comment(text: &str) -> String {
    let raw_lines: Vec<&str> = text.lines().collect();
    let mut lines: Vec<String> = Vec::with_capacity(raw_lines.len());
    let mut in_code_block = false;
    let mut in_user_fence = false;
    let mut user_fence_ticks = 0;

    for (idx, line) in raw_lines.iter().enumerate() {
        if in_user_fence {
            // Per CommonMark, only a run of at least the opener's tick
            // count with no info string closes the fence; anything else
            // (shorter runs, ```lang lines) is fence content.
            if let Some((ticks, info)) = fence_marker(line)
                && ticks >= user_fence_ticks
                && info.trim().is_empty()
            {
                in_user_fence = false;
            }
            lines.push((*line).to_string());
            continue;
        }

        if in_code_block {
            if is_indented(line) {
                lines.push(strip_indent(line));
                continue;
            }
            // A blank line inside an indented block only closes it when no
            // more indented content follows.
            if line.is_empty() {
                let next_is_indented = raw_lines[idx + 1..]
                    .iter()
                    .find(|l| !l.is_empty())
                    .is_some_and(|l| is_indented(l));
                if next_is_indented {
                    lines.push(String::new());
                    continue;
                }
            }
            lines.push("```".to_string());
            in_code_block = false;
            // Fall through: this line still needs classifying.
        }

        if let Some((ticks, info)) = fence_marker(line) {
            in_user_fence = true;
            user_fence_ticks = ticks;
            let indent = &line[..line.len() - line.trim_start().len()];
            let ticks_str = "`".repeat(ticks);
            lines.push(format!("{indent}{ticks_str}{}", fence_info(info)));
        } else if is_indented(line) {
            lines.push("```text".to_string());
            in_code_block = true;
            lines.push(strip_indent(line));
        } else {
            lines.push(sanitize_line(line));
        }
    }

    if in_code_block {
        lines.push("```".to_string());
    }
    if in_user_fence {
        // An unterminated fence would swallow the rest of the doc block
        // (including any generator-authored text appended after the proto
        // comment); close it with a matching-length fence.
        lines.push("`".repeat(user_fence_ticks));
    }

    lines.join("\n")
}

/// The info string to emit for a fence opener, so that rustdoc never
/// *compiles* the fence body. Proto comments are untrusted input: their
/// examples have no imports, name proto types rather than Rust ones, and
/// would be compiled — and run — by the consumer's `cargo test --doc`.
///
/// Deciding "is this Rust?" the way rustdoc does is a trap: an explicit
/// `rust` keeps the block Rust even beside an unknown word
/// (`rust,noplayground`), error-code tokens (`compile_fail,E0277`) and
/// `{class=…}` attributes keep it Rust too, and the verdict is even
/// order-dependent. So this does not classify at all — it makes every
/// fence inert:
///
/// - `ignore-<target>` tokens are dropped. rustdoc reads them as a target
///   *list* that replaces a plain `ignore`, so the block would still be
///   compiled for every other target.
/// - a bare `ignore` is then ensured. It is the only attribute that
///   reliably stops compilation: `no_run` still type-checks,
///   `should_panic` runs, and `compile_fail` merely inverts the verdict.
///
/// The author's language annotation is preserved, so a ` ```rust ` fence
/// still gets Rust syntax highlighting as ` ```rust,ignore `. An `ignore`
/// on a non-Rust fence is inert, and an unannotated fence becomes `text`
/// rather than guessing a language — rustdoc highlights nothing but Rust,
/// so identifying JSON or YAML would buy no rendering benefit.
///
/// (Every claim above was verified against rustdoc directly.)
fn fence_info(info: &str) -> String {
    let mut dropped_target_ignore = false;
    let mut tokens: Vec<&str> = Vec::new();
    for tok in info.split([',', ' ', '\t']).filter(|t| !t.is_empty()) {
        if tok.starts_with("ignore-") {
            dropped_target_ignore = true;
        } else {
            tokens.push(tok);
        }
    }

    if tokens.is_empty() {
        // A fence annotated only with `ignore-<target>` was Rust, so keep
        // it highlighted; an unannotated one is language-agnostic.
        return if dropped_target_ignore {
            "rust,ignore".to_string()
        } else {
            "text".to_string()
        };
    }

    if !tokens.contains(&"ignore") {
        tokens.push("ignore");
    }
    tokens.join(",")
}

/// An indented-code-block line: 4+ spaces or a tab (CommonMark).
fn is_indented(line: &str) -> bool {
    line.starts_with("    ") || line.starts_with('\t')
}

/// Remove one level of code-block indentation, but keep it on a line that
/// would otherwise read as a fence marker — inside the synthetic ```text
/// fence a de-indented ``` run would close it early, while CommonMark
/// ignores fence markers indented 4+ spaces.
fn strip_indent(line: &str) -> String {
    let stripped = line
        .strip_prefix("    ")
        .or_else(|| line.strip_prefix('\t'))
        .unwrap_or(line);
    if stripped.trim_start().starts_with("```") {
        (*line).to_string()
    } else {
        stripped.to_string()
    }
}

/// If `line` is a fence marker (an optionally ≤3-space-indented run of 3+
/// backticks), return the run length and the info string after it. Lines
/// indented 4+ spaces are indented code, not fences (CommonMark).
fn fence_marker(line: &str) -> Option<(usize, &str)> {
    if is_indented(line) {
        return None;
    }
    let trimmed = line.trim_start();
    let info = trimmed.trim_start_matches('`');
    let ticks = trimmed.len() - info.len();
    (ticks >= 3).then_some((ticks, info))
}

/// Escape one prose line for rustdoc.
///
/// Backtick code spans pass through verbatim; complete inline links
/// (`[text](url)`) and autolinks (`<http(s)://…>`) are preserved; bare
/// `http(s)://` URLs are wrapped in `<…>` (rustdoc's `bare_urls` lint);
/// everything else that rustdoc would interpret — `[`, `]`, `<`, `>`,
/// `\` — is escaped.
fn sanitize_line(line: &str) -> String {
    let bytes = line.as_bytes();
    let mut out = String::with_capacity(line.len());
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];

        if b == b'`' {
            let run_start = i;
            while i < bytes.len() && bytes[i] == b'`' {
                i += 1;
            }
            let run_len = i - run_start;
            if let Some(close_end) = find_backtick_closer(bytes, i, run_len) {
                out.push_str(&line[run_start..close_end]);
                i = close_end;
            } else {
                out.push_str(&line[run_start..i]);
            }
            continue;
        }

        match b {
            b'\\' => {
                // The comment author is escaping the next char; pass the
                // pair through verbatim.
                out.push('\\');
                i += 1;
                if i < bytes.len() {
                    i += push_char_at(&mut out, line, i);
                }
            }
            b'[' => {
                if let Some(end) = find_inline_link_end(bytes, i) {
                    out.push_str(&line[i..=end]);
                    i = end + 1;
                } else {
                    out.push_str("\\[");
                    i += 1;
                }
            }
            b']' => {
                out.push_str("\\]");
                i += 1;
            }
            b'<' => {
                if let Some(end) = find_autolink_end(bytes, i) {
                    out.push_str(&line[i..=end]);
                    i = end + 1;
                } else {
                    out.push_str("\\<");
                    i += 1;
                }
            }
            b'>' => {
                out.push_str("\\>");
                i += 1;
            }
            b'h' => {
                if let Some(end) = find_bare_url_end(bytes, i) {
                    out.push('<');
                    out.push_str(&line[i..end]);
                    out.push('>');
                    i = end;
                } else {
                    out.push('h');
                    i += 1;
                }
            }
            _ => {
                i += push_char_at(&mut out, line, i);
            }
        }
    }
    out
}

/// Push the UTF-8 char at byte index `i` of `s` into `out`, returning its
/// byte length. `i` must be a char boundary and `< s.len()`.
fn push_char_at(out: &mut String, s: &str, i: usize) -> usize {
    let ch = s[i..]
        .chars()
        .next()
        .expect("i is in bounds and on a char boundary");
    out.push(ch);
    ch.len_utf8()
}

/// Starting at `from` (just past an opening run of `run_len` backticks),
/// return the past-the-end index of the matching closing run, or `None`.
fn find_backtick_closer(bytes: &[u8], from: usize, run_len: usize) -> Option<usize> {
    let mut i = from;
    while i < bytes.len() {
        if bytes[i] == b'`' {
            let start = i;
            while i < bytes.len() && bytes[i] == b'`' {
                i += 1;
            }
            if i - start == run_len {
                return Some(i);
            }
        } else {
            i += 1;
        }
    }
    None
}

/// If `bytes[start..]` is a complete `[text](url)`, return the index of the
/// closing `)`. Nested `(`/`)` inside the URL are balanced one level deep so
/// fragments like `…#method()` survive.
fn find_inline_link_end(bytes: &[u8], start: usize) -> Option<usize> {
    debug_assert_eq!(bytes[start], b'[');
    let mut j = start + 1;
    while j < bytes.len() && bytes[j] != b']' {
        if bytes[j] == b'[' {
            return None;
        }
        j += 1;
    }
    if j + 1 >= bytes.len() || bytes[j + 1] != b'(' {
        return None;
    }
    let mut depth = 1i32;
    let mut k = j + 2;
    while k < bytes.len() {
        match bytes[k] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(k);
                }
            }
            _ => {}
        }
        k += 1;
    }
    None
}

/// Does `rest` begin with an `http(s)://` scheme?
fn has_url_scheme(rest: &[u8]) -> bool {
    rest.starts_with(b"http://") || rest.starts_with(b"https://")
}

/// If `bytes[start..]` is `<http(s)://…>`, return the index of the `>`.
fn find_autolink_end(bytes: &[u8], start: usize) -> Option<usize> {
    debug_assert_eq!(bytes[start], b'<');
    let rest = &bytes[start + 1..];
    if !has_url_scheme(rest) {
        return None;
    }
    rest.iter().position(|&b| b == b'>').map(|p| start + 1 + p)
}

/// If `bytes[start..]` begins a bare `http(s)://` URL, return the
/// past-the-end byte index. The URL ends at whitespace, `)`, or an angle
/// bracket (which would corrupt the `<…>` autolink wrapper), and trailing
/// sentence punctuation is treated as prose rather than URL.
fn find_bare_url_end(bytes: &[u8], start: usize) -> Option<usize> {
    if !has_url_scheme(&bytes[start..]) {
        return None;
    }
    let mut j = start;
    while j < bytes.len()
        && !bytes[j].is_ascii_whitespace()
        && !matches!(bytes[j], b')' | b'<' | b'>')
    {
        j += 1;
    }
    while j > start && matches!(bytes[j - 1], b'.' | b',' | b';' | b':' | b'!' | b'?') {
        j -= 1;
    }
    Some(j)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brackets_escaped() {
        assert_eq!(
            sanitize_line("see [Note] for details"),
            "see \\[Note\\] for details"
        );
    }

    #[test]
    fn angle_brackets_escaped() {
        assert_eq!(
            sanitize_line("a map<string, int32> field"),
            "a map\\<string, int32\\> field"
        );
    }

    #[test]
    fn author_escapes_pass_through() {
        assert_eq!(sanitize_line(r"a \[not a link\]"), r"a \[not a link\]");
        assert_eq!(sanitize_line(r"trailing \"), r"trailing \");
    }

    #[test]
    fn code_spans_pass_through() {
        assert_eq!(
            sanitize_line("use `[T; N]` or `<T>`"),
            "use `[T; N]` or `<T>`"
        );
    }

    #[test]
    fn unmatched_backtick_run_passes_through() {
        assert_eq!(sanitize_line("stray ` tick [x]"), "stray ` tick \\[x\\]");
    }

    #[test]
    fn inline_links_preserved() {
        assert_eq!(
            sanitize_line("see [docs](https://example.com/a#m()) here"),
            "see [docs](https://example.com/a#m()) here"
        );
    }

    #[test]
    fn autolinks_preserved() {
        assert_eq!(
            sanitize_line("visit <https://example.com> now"),
            "visit <https://example.com> now"
        );
    }

    #[test]
    fn bare_urls_wrapped() {
        assert_eq!(
            sanitize_line("docs at https://example.com/x, see there"),
            "docs at <https://example.com/x>, see there"
        );
        assert_eq!(
            sanitize_line("(https://example.com)"),
            "(<https://example.com>)"
        );
        assert_eq!(
            sanitize_line("see https://example.com."),
            "see <https://example.com>."
        );
    }

    #[test]
    fn ref_style_link_falls_back_to_escaping() {
        assert_eq!(
            sanitize_line("see [Note][commented.Note]"),
            "see \\[Note\\]\\[commented.Note\\]"
        );
    }

    #[test]
    fn user_fences_pass_through() {
        // Fence *content* is never escaped; only the info string gains the
        // `ignore` that keeps rustdoc from compiling it.
        let input = "Example:\n```json\n{\"a\": [1]}\n```\ndone [x]";
        let expected = "Example:\n```json,ignore\n{\"a\": [1]}\n```\ndone \\[x\\]";
        assert_eq!(sanitize_comment(input), expected);
    }

    #[test]
    fn bare_fence_defaults_to_text() {
        let input = "Example:\n```\n{\"a\": 1}\n```\ndone";
        let expected = "Example:\n```text\n{\"a\": 1}\n```\ndone";
        assert_eq!(sanitize_comment(input), expected);
    }

    #[test]
    fn unterminated_fence_closed() {
        let input = "para\n```\nstuff";
        assert_eq!(sanitize_comment(input), "para\n```text\nstuff\n```");
    }

    #[test]
    fn longer_fence_closed_with_matching_ticks() {
        let input = "````\nnested ``` inside";
        assert_eq!(sanitize_comment(input), "````text\nnested ``` inside\n````");
    }

    #[test]
    fn rust_fence_is_marked_ignore_but_keeps_highlighting() {
        // `rust,ignore` is still syntax-highlighted by rustdoc, but never
        // compiled — a proto comment's Rust example has no imports and
        // would fail in the consumer's `cargo test --doc`.
        let input = "```rust\nlet x = Note::default();\n```";
        assert_eq!(
            sanitize_comment(input),
            "```rust,ignore\nlet x = Note::default();\n```"
        );
    }

    /// Every one of these still reaches rustdoc's compiler without an
    /// `ignore`: `no_run` type-checks, `should_panic` runs, `compile_fail`
    /// inverts the verdict, an error code or an mdBook-style word keeps the
    /// block Rust, and `ignore-<target>` compiles on every other target —
    /// even when a plain `ignore` sits beside it, because rustdoc lets the
    /// target list replace it. All verified against rustdoc directly.
    #[test]
    fn every_fence_is_made_inert() {
        let cases = [
            ("rust", "rust,ignore"),
            ("no_run", "no_run,ignore"),
            ("should_panic", "should_panic,ignore"),
            ("compile_fail", "compile_fail,ignore"),
            ("compile_fail,E0277", "compile_fail,E0277,ignore"),
            ("rust,noplayground", "rust,noplayground,ignore"),
            ("edition2018", "edition2018,ignore"),
            // `ignore-<target>` is dropped, not kept alongside `ignore`.
            ("ignore-wasm32", "rust,ignore"),
            ("rust,ignore-wasm32", "rust,ignore"),
            ("ignore-wasm32,ignore", "ignore"),
            // Non-Rust fences keep their language; the added `ignore` is
            // inert for them, and uniformity beats classifying.
            ("json", "json,ignore"),
            ("proto", "proto,ignore"),
            // Whitespace-separated info strings normalize to commas.
            ("rust no_run", "rust,no_run,ignore"),
        ];
        for (info, expected) in cases {
            assert_eq!(
                sanitize_comment(&format!("```{info}\nbody\n```")),
                format!("```{expected}\nbody\n```"),
                "info string: {info}"
            );
        }
    }

    #[test]
    fn already_ignored_fences_are_untouched() {
        for info in ["rust,ignore", "ignore", "ignore,json"] {
            let input = format!("```{info}\nbody\n```");
            assert_eq!(sanitize_comment(&input), input, "info string: {info}");
        }
    }

    #[test]
    fn indented_fence_opener_keeps_indent() {
        assert_eq!(
            sanitize_comment("  ```rust\n  x\n  ```"),
            "  ```rust,ignore\n  x\n  ```"
        );
    }

    #[test]
    fn four_tick_fence_keeps_inner_three_tick_line() {
        let input = "````\n```\ncode\n````";
        assert_eq!(sanitize_comment(input), "````text\n```\ncode\n````");
    }

    #[test]
    fn info_string_line_inside_fence_is_content() {
        // The inner ```rust line is fence content, not a closer, so it is
        // left alone; only the opener is rewritten.
        let input = "```json\n{}\n```rust\nx\n```";
        assert_eq!(
            sanitize_comment(input),
            "```json,ignore\n{}\n```rust\nx\n```"
        );
    }

    #[test]
    fn fence_directly_after_indented_block_is_tracked() {
        let input = "    x = 1\n```\nnot rust\n```";
        assert_eq!(
            sanitize_comment(input),
            "```text\nx = 1\n```\n```text\nnot rust\n```"
        );
    }

    #[test]
    fn indented_fence_marker_is_code_not_fence() {
        let input = "    ```\n    x";
        assert_eq!(sanitize_comment(input), "```text\n    ```\nx\n```");
    }

    #[test]
    fn fence_line_inside_indented_block_keeps_indent() {
        let input = "    code\n    ```\n    more";
        assert_eq!(sanitize_comment(input), "```text\ncode\n    ```\nmore\n```");
    }

    #[test]
    fn url_with_angle_bracket_stops_at_bracket() {
        assert_eq!(
            sanitize_line("see http://example.com/a>b"),
            "see <http://example.com/a>\\>b"
        );
    }

    #[test]
    fn indented_block_fenced_as_text() {
        let input = "Usage:\n    req = Note{}\n    send(req)\nDone.";
        let expected = "Usage:\n```text\nreq = Note{}\nsend(req)\n```\nDone.";
        assert_eq!(sanitize_comment(input), expected);
    }

    #[test]
    fn blank_line_inside_indented_block_kept_open() {
        let input = "    a\n\n    b\nprose";
        let expected = "```text\na\n\nb\n```\nprose";
        assert_eq!(sanitize_comment(input), expected);
    }

    #[test]
    fn trailing_indented_block_closed() {
        let input = "text\n    code";
        assert_eq!(sanitize_comment(input), "text\n```text\ncode\n```");
    }

    #[test]
    fn multibyte_chars_survive_around_metachars() {
        assert_eq!(
            sanitize_line("émoji 🦀 <T> — done"),
            "émoji 🦀 \\<T\\> — done"
        );
        assert_eq!(
            sanitize_line("précis at https://例え.jp/パス end"),
            "précis at <https://例え.jp/パス> end"
        );
    }

    #[test]
    fn blank_lines_preserved() {
        assert_eq!(
            sanitize_comment("para one\n\npara two"),
            "para one\n\npara two"
        );
    }
}
