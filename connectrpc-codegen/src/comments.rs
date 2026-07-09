//! Sanitization of proto source comments for rustdoc emission.
//!
//! Ported from buffa-codegen's crate-private comment sanitizer (as of buffa
//! v0.8.1, `buffa-codegen/src/comments.rs`) so that service and method
//! comments get the same treatment buffa gives message and field comments:
//! user-written markdown fences pass through, indented blocks are fenced as
//! `text`, and markdown/HTML metacharacters in prose are escaped so
//! arbitrary proto comments cannot break a consumer's `cargo doc`
//! (intra-doc-link and HTML-tag lints) or inadvertently become doctests.
//! Several deliberate divergences harden on buffa's behavior: fences
//! without a language annotation default to `text` (rustdoc would
//! otherwise compile their content as a Rust doctest), an unterminated
//! fence is closed at the end of the comment, and fence open/close
//! detection follows CommonMark (closers need matching tick counts and no
//! info string; markers indented 4+ spaces are code, not fences). This
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
/// - user-written ``` fences: content passes through unescaped; an opener
///   with no language annotation gains `text`, and an unterminated fence
///   is closed at the end of the comment;
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
            if info.trim().is_empty() {
                // rustdoc treats a fence with no info string as a Rust
                // doctest; default it to `text` so arbitrary comment
                // content is never compiled by a consumer's `cargo test`.
                lines.push(format!("{}text", line.trim_end()));
            } else {
                lines.push((*line).to_string());
            }
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
        let input = "Example:\n```json\n{\"a\": [1]}\n```\ndone [x]";
        let expected = "Example:\n```json\n{\"a\": [1]}\n```\ndone \\[x\\]";
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
    fn four_tick_fence_keeps_inner_three_tick_line() {
        let input = "````\n```\ncode\n````";
        assert_eq!(sanitize_comment(input), "````text\n```\ncode\n````");
    }

    #[test]
    fn info_string_line_inside_fence_is_content() {
        let input = "```json\n{}\n```rust\nx\n```";
        assert_eq!(sanitize_comment(input), input);
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
