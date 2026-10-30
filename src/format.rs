use chrono::{DateTime, Utc};
use mxbot_common::format::html_escape;

use crate::config::LimitsConfig;
use crate::email::{get_plain_body, ParsedEmail};
use crate::html_clean::sanitize_html;

/// Format a ParsedEmail into a (plain_text, html) pair for posting to Matrix.
pub fn format_email(email: &ParsedEmail, limits: &LimitsConfig) -> (String, String) {
    let max_quote = limits.effective_max_quote_lines();
    let is_reply = email.thread_root.is_some();

    let subject = clean_subject(&email.subject);
    let sender = format_sender(email.from_name.as_deref(), &email.from_email);
    let date_str = format_date(email.date.as_ref());

    let body_raw = get_plain_body(email);
    let body_collapsed = collapse_quotes(&body_raw, max_quote);

    // Build plain text
    let prefix = if is_reply { "↩️ Re: " } else { "📧 " };
    let plain = format!(
        "{}{}\n👤 {}\n🕒 {}\n\n{}",
        prefix, subject, sender, date_str, body_collapsed
    );

    // Build HTML
    let subject_escaped = html_escape(&subject);
    let sender_escaped = html_escape(&sender);
    let date_escaped = html_escape(&date_str);

    let body_html = build_html_body(email, &body_collapsed, max_quote);

    let header_html = if is_reply {
        format!(
            "↩️ <strong>Re: {}</strong><br>\n👤 <em>{}</em><br>\n🕒 <em>{}</em><br><br>\n",
            subject_escaped, sender_escaped, date_escaped
        )
    } else {
        format!(
            "📧 <strong>{}</strong><br>\n👤 <em>{}</em><br>\n🕒 <em>{}</em><br><br>\n",
            subject_escaped, sender_escaped, date_escaped
        )
    };

    let html = format!("{}{}", header_html, body_html);

    (plain, html)
}

/// A Matrix event may be at most this many bytes, as the server stores it in JSON.
const MATRIX_EVENT_MAX_BYTES: usize = 65_535;

/// Room left in an event around the email's text and HTML: the event's own fields as the
/// server stores it (IDs, auth and previous events, hashes, signatures), the thread relation,
/// and in an encrypted room the encryption envelope (sender key, session and device ID) and
/// the Megolm framing (MAC, signature).
const EVENT_OVERHEAD_BYTES: usize = 8 * 1024;

/// The message content (text and HTML) of one posted email stays below this many bytes,
/// measured as the serialized JSON, escapes included (`"`, `\`, control characters). In an
/// encrypted room that JSON is encrypted and base64-encoded, about 4/3 of its size, so this is
/// (65,535 − overhead) · 3/4, about 43 KB. Before this, a long email failed with M_TOO_LARGE
/// every time and was dropped from the retry queue.
pub const MAX_EVENT_CONTENT_BYTES: usize = (MATRIX_EVENT_MAX_BYTES - EVENT_OVERHEAD_BYTES) * 3 / 4;

/// What a shortened email ends with.
const SHORTENED_NOTE: &str = "✂️ [Shortened: the full email is too long for Matrix]";

/// The email's text and HTML as they are when they fit into one event (`max_bytes` of
/// serialized content). Otherwise the whole text with HTML made from it, as the sanitized HTML
/// is often several times the text. Only when even that is too big, the text cut to fit (at a
/// line end where possible), with a note, and HTML made from that.
pub fn fit_to_event(plain: String, html: String, max_bytes: usize) -> (String, String) {
    if content_json_len(&plain, &html) <= max_bytes {
        return (plain, html);
    }
    let plain_html = plain_lines_to_html(&plain);
    let mut size = content_json_len(&plain, &plain_html);
    if size <= max_bytes {
        return (plain, plain_html);
    }
    let mut keep = plain.len();
    loop {
        // In proportion to how far the last try was over, and a tenth more: escapes and the
        // note do not shrink with the text. Always less than before, so this ends.
        keep = keep.saturating_mul(max_bytes) / size * 9 / 10;
        let mut cut = &plain[..plain.floor_char_boundary(keep)];
        if let Some(line_end) = cut.rfind('\n') {
            if line_end >= cut.len() / 2 {
                cut = &cut[..line_end];
            }
        }
        let short_plain = format!("{cut}\n\n{SHORTENED_NOTE}");
        let short_html = format!(
            "{}<br>\n<em>{}</em>",
            plain_lines_to_html(cut),
            html_escape(SHORTENED_NOTE)
        );
        size = content_json_len(&short_plain, &short_html);
        if size <= max_bytes || keep == 0 {
            return (short_plain, short_html);
        }
    }
}

/// The size of `m.room.message` content with this text and HTML as JSON, the way the event is
/// sent (and, in an encrypted room, encrypted).
fn content_json_len(plain: &str, html: &str) -> usize {
    #[derive(serde::Serialize)]
    struct Content<'a> {
        msgtype: &'a str,
        body: &'a str,
        format: &'a str,
        formatted_body: &'a str,
    }
    let content = Content {
        msgtype: "m.text",
        body: plain,
        format: "org.matrix.custom.html",
        formatted_body: html,
    };
    // Serializing strings cannot fail; should it, the content counts as too big.
    serde_json::to_vec(&content).map_or(usize::MAX, |json| json.len())
}

/// Plain lines as HTML: escaped, one `<br>` per line.
fn plain_lines_to_html(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + text.len() / 8);
    for line in text.lines() {
        out.push_str(&html_escape(line));
        out.push_str("<br>\n");
    }
    out
}

/// Format sender as "Name <email>" or just "email".
pub fn format_sender(name: Option<&str>, email: &str) -> String {
    match name {
        Some(n) if !n.is_empty() => format!("{} <{}>", n, email),
        _ => email.to_owned(),
    }
}

/// Format a UTC DateTime as "Thu 21 May 14:30" or "(unknown date)" if None.
pub fn format_date(date: Option<&DateTime<Utc>>) -> String {
    match date {
        Some(d) => d.format("%a %d %b %H:%M").to_string(),
        None => "(unknown date)".to_owned(),
    }
}

/// Collapse `>` prefixed quote blocks to at most max_lines lines, appending
/// "💬 [N more lines quoted]" if truncated.
pub fn collapse_quotes(text: &str, max_lines: usize) -> String {
    let mut result = Vec::new();
    let mut quote_block: Vec<&str> = Vec::new();

    let flush_quotes = |block: &mut Vec<&str>, result: &mut Vec<String>| {
        if block.is_empty() {
            return;
        }
        let total = block.len();
        let shown = total.min(max_lines);
        for line in &block[..shown] {
            result.push((*line).to_owned());
        }
        if total > shown {
            result.push(format!("💬 [{} more lines quoted]", total - shown));
        }
        block.clear();
    };

    for line in text.lines() {
        if line.starts_with('>') {
            quote_block.push(line);
        } else {
            flush_quotes(&mut quote_block, &mut result);
            result.push(line.to_owned());
        }
    }
    flush_quotes(&mut quote_block, &mut result);

    result.join("\n")
}

/// Clean subject: strip duplicate Re: prefixes and list prefixes like [foo].
pub fn clean_subject(subject: &str) -> String {
    let mut s = subject.trim().to_owned();

    // Remove list prefixes like [listname] at the start
    while s.starts_with('[') {
        if let Some(end) = s.find(']') {
            s = s[end + 1..].trim().to_owned();
        } else {
            break;
        }
    }

    // Remove duplicate Re: / RE: / re: prefixes (leave at most one)
    let re_count = count_re_prefix(&s);
    if re_count > 1 {
        // Strip all Re: prefixes, we'll let the thread context convey reply status
        for _ in 0..re_count {
            if let Some(stripped) = strip_one_re(&s) {
                s = stripped;
            }
        }
    }

    s.trim().to_owned()
}

fn count_re_prefix(s: &str) -> usize {
    let mut count = 0;
    let mut rest = s;
    loop {
        let lower = rest.to_lowercase();
        if lower.starts_with("re:") {
            rest = rest[3..].trim();
            count += 1;
        } else if lower.starts_with("re[") {
            // handles Re[2]: style
            if let Some(end) = rest.find("]:") {
                rest = rest[end + 2..].trim();
                count += 1;
            } else {
                break;
            }
        } else {
            break;
        }
    }
    count
}

fn strip_one_re(s: &str) -> Option<String> {
    let lower = s.to_lowercase();
    if lower.starts_with("re:") {
        Some(s[3..].trim().to_owned())
    } else if lower.starts_with("re[") {
        s.find("]:").map(|end| s[end + 2..].trim().to_owned())
    } else {
        None
    }
}

fn build_html_body(email: &ParsedEmail, plain_collapsed: &str, max_quote: usize) -> String {
    // Prefer sanitized HTML body if available; otherwise convert plain to HTML
    if let Some(ref html_raw) = email.body_html {
        let sanitized = sanitize_html(html_raw);
        // Wrap in a div for consistent rendering
        format!("<div>{}</div>", sanitized)
    } else {
        // Convert collapsed plain text to HTML
        plain_to_html(plain_collapsed, max_quote)
    }
}

fn plain_to_html(text: &str, max_quote: usize) -> String {
    let mut result = String::new();
    let mut quote_block: Vec<&str> = Vec::new();

    let flush_quotes = |block: &mut Vec<&str>, out: &mut String| {
        if block.is_empty() {
            return;
        }
        let total = block.len();
        let shown = total.min(max_quote);
        out.push_str("<blockquote>");
        for line in &block[..shown] {
            let trimmed = line.trim_start_matches('>').trim();
            out.push_str(&html_escape(trimmed));
            out.push_str("<br>\n");
        }
        if total > shown {
            out.push_str(&format!(
                "<em>💬 {} more lines quoted</em><br>\n",
                total - shown
            ));
        }
        out.push_str("</blockquote>\n");
        block.clear();
    };

    for line in text.lines() {
        if line.starts_with('>') {
            quote_block.push(line);
        } else {
            flush_quotes(&mut quote_block, &mut result);
            if line.is_empty() {
                result.push_str("<br>\n");
            } else {
                result.push_str(&html_escape(line));
                result.push_str("<br>\n");
            }
        }
    }
    flush_quotes(&mut quote_block, &mut result);

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_sender_with_name() {
        assert_eq!(
            format_sender(Some("Alice"), "alice@example.com"),
            "Alice <alice@example.com>"
        );
    }

    #[test]
    fn test_format_sender_without_name() {
        assert_eq!(
            format_sender(None, "alice@example.com"),
            "alice@example.com"
        );
    }

    #[test]
    fn test_format_sender_empty_name() {
        assert_eq!(
            format_sender(Some(""), "alice@example.com"),
            "alice@example.com"
        );
    }

    #[test]
    fn test_clean_subject_strips_list_prefix() {
        assert_eq!(clean_subject("[mylist] Hello world"), "Hello world");
        assert_eq!(clean_subject("[mylist] [sub] Hello world"), "Hello world");
    }

    #[test]
    fn test_clean_subject_strips_multiple_re() {
        assert_eq!(clean_subject("Re: Re: Re: Hello"), "Hello");
        assert_eq!(clean_subject("RE: RE: Test subject"), "Test subject");
    }

    #[test]
    fn test_clean_subject_single_re_kept() {
        // Single Re: is kept as-is (caller decides thread context)
        let result = clean_subject("Re: Hello");
        assert_eq!(result, "Re: Hello");
    }

    #[test]
    fn test_clean_subject_list_and_re() {
        assert_eq!(clean_subject("[list] Re: Hello"), "Re: Hello");
    }

    #[test]
    fn test_collapse_quotes_under_limit() {
        let text = "> line 1\n> line 2\nRegular text";
        let out = collapse_quotes(text, 5);
        assert!(out.contains("> line 1"));
        assert!(out.contains("> line 2"));
        assert!(out.contains("Regular text"));
        assert!(!out.contains("more lines quoted"));
    }

    #[test]
    fn test_collapse_quotes_over_limit() {
        let text = "> q1\n> q2\n> q3\n> q4\n> q5\n> q6\n> q7\nRegular";
        let out = collapse_quotes(text, 3);
        assert!(out.contains("> q1"));
        assert!(out.contains("> q2"));
        assert!(out.contains("> q3"));
        assert!(out.contains("4 more lines quoted"));
        assert!(!out.contains("> q4"));
    }

    #[test]
    fn test_collapse_quotes_no_quotes() {
        let text = "Line 1\nLine 2\nLine 3";
        let out = collapse_quotes(text, 5);
        assert_eq!(out, text);
    }

    #[test]
    fn test_format_date_none() {
        assert_eq!(format_date(None), "(unknown date)");
    }

    #[test]
    fn test_format_date_some() {
        use chrono::TimeZone;
        let dt = Utc.with_ymd_and_hms(2026, 5, 21, 14, 30, 0).unwrap();
        let out = format_date(Some(&dt));
        assert!(out.contains("21"));
        assert!(out.contains("14:30"));
    }

    #[test]
    fn short_email_is_posted_unchanged() {
        let (plain, html) = fit_to_event(
            "hello".into(),
            "<b>hello</b>".into(),
            MAX_EVENT_CONTENT_BYTES,
        );
        assert_eq!(plain, "hello");
        assert_eq!(html, "<b>hello</b>");
    }

    /// Content JSON as the SDK sends it, to check `content_json_len` and the limits against.
    fn sent_json(plain: &str, html: &str) -> String {
        use mxbot_common::matrix_sdk::ruma::events::room::message::RoomMessageEventContent;
        serde_json::to_string(&RoomMessageEventContent::text_html(plain, html)).unwrap()
    }

    #[test]
    fn content_size_is_measured_like_the_sent_json() {
        let plain = "a \"quote\", a \\ backslash, \u{1} control, <tag> & ä\n";
        let html = html_escape(plain);
        assert_eq!(
            content_json_len(plain, &html),
            sent_json(plain, &html).len()
        );
    }

    /// A mail with a normal amount of text and sanitized HTML several times as big.
    fn mail(text_bytes: usize, html_bytes: usize) -> (String, String) {
        let line = "Liebe Leute, hier steht ein ganz normaler Satz aus der Mail.\n";
        let mut plain = "📧 Subject\n👤 a@example.org\n🕒 date\n\n".to_owned();
        while plain.len() < text_bytes {
            plain.push_str(line);
        }
        let mut html = "<div>".to_owned();
        while html.len() < html_bytes {
            html.push_str(r#"<p style="margin:0;font-family:Arial">Liebe Leute, ein Satz.</p>"#);
        }
        html.push_str("</div>");
        (plain, html)
    }

    #[test]
    fn mail_that_fits_is_posted_byte_identical() {
        // 7 KB text and 26 KB HTML: used to be cut to about 11 KB with the note.
        let (plain, html) = mail(7_000, 26_000);
        let (fitted_plain, fitted_html) =
            fit_to_event(plain.clone(), html.clone(), MAX_EVENT_CONTENT_BYTES);
        assert_eq!(fitted_plain, plain);
        assert_eq!(fitted_html, html);
    }

    #[test]
    fn mail_with_too_much_html_keeps_its_whole_text() {
        let (plain, html) = mail(7_000, 60_000);
        assert!(content_json_len(&plain, &html) > MAX_EVENT_CONTENT_BYTES);
        let (fitted_plain, fitted_html) =
            fit_to_event(plain.clone(), html, MAX_EVENT_CONTENT_BYTES);
        assert_eq!(fitted_plain, plain);
        assert_eq!(fitted_html, plain_lines_to_html(&plain));
        assert!(!fitted_plain.contains(SHORTENED_NOTE));
        assert!(!fitted_html.contains(SHORTENED_NOTE));
    }

    #[test]
    fn long_email_is_shortened_below_the_event_limit() {
        // Over 400 KB like the bodies that used to fail with M_TOO_LARGE, with characters
        // that need escaping and multi-byte characters.
        let line = "Zeile mit <Klammern> & Umlauten äöü, die lang genug ist\n";
        let body: String = line.repeat(8_000);
        let plain = format!("📧 Subject\n👤 a@example.org\n🕒 date\n\n{body}");
        let html = format!("<div>{}</div>", html_escape(&body));
        let (plain, html) = fit_to_event(plain, html, MAX_EVENT_CONTENT_BYTES);
        assert!(sent_json(&plain, &html).len() <= MAX_EVENT_CONTENT_BYTES);
        // Not cut far below the limit.
        assert!(sent_json(&plain, &html).len() > MAX_EVENT_CONTENT_BYTES * 3 / 4);
        assert!(plain.starts_with("📧 Subject\n👤 a@example.org"));
        assert!(plain.ends_with(SHORTENED_NOTE));
        assert!(html.contains("Zeile mit &lt;Klammern&gt; &amp; Umlauten äöü"));
        assert!(html.ends_with(&format!("<em>{SHORTENED_NOTE}</em>")));
        // Cut at a line end: the last kept line is whole.
        let kept = plain.strip_suffix(SHORTENED_NOTE).unwrap().trim_end();
        assert!(kept.ends_with("lang genug ist"));
    }

    #[test]
    fn email_full_of_escapes_fits_an_encrypted_event() {
        // Every character here grows in JSON (`"` and `\` to 2 bytes, control characters to
        // 6) or in HTML (`<` and `"` to 4 and 6 bytes, then escaped again in JSON).
        let line = "\"\\\u{1}\u{1f}<\"\\\t\u{7}<>\n";
        let plain = format!("📧 Subject\n\n{}", line.repeat(20_000));
        let html = html_escape(&plain);
        let (plain, html) = fit_to_event(plain, html, MAX_EVENT_CONTENT_BYTES);
        assert!(plain.ends_with(SHORTENED_NOTE));
        let content = sent_json(&plain, &html);
        assert!(
            content.len() <= MAX_EVENT_CONTENT_BYTES,
            "{}",
            content.len()
        );

        // The event this becomes in an encrypted room: the plaintext JSON (with the thread
        // relation) Megolm-encrypted and base64-encoded (4/3, plus about 100 bytes of
        // framing), inside the m.room.encrypted event as the server stores it.
        let relates_to = r#""m.relates_to":{"rel_type":"m.thread","event_id":"$aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","is_falling_back":true,"m.in_reply_to":{"event_id":"$aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}"#;
        let inner = format!(
            r#"{{"type":"m.room.message","room_id":"!aaaaaaaaaaaaaaaaaa:example.org","content":{},{relates_to}}}}}"#,
            content.strip_suffix('}').unwrap()
        );
        let ciphertext = "A".repeat((inner.len() + 100).div_ceil(3) * 4);
        let pdu = format!(
            r#"{{"auth_events":[{ids}],"prev_events":[{ids}],"content":{{"algorithm":"m.megolm.v1.aes-sha2","ciphertext":"{ciphertext}","device_id":"AAAAAAAAAA","sender_key":"{key}","session_id":"{key}",{relates_to}}},"depth":123456,"hashes":{{"sha256":"{key}"}},"origin":"example.org","origin_server_ts":1761800000000,"room_id":"!aaaaaaaaaaaaaaaaaa:example.org","sender":"@email-bot:example.org","signatures":{{"example.org":{{"ed25519:a_AAAA":"{sig}"}}}},"type":"m.room.encrypted","unsigned":{{"age_ts":1761800000000}}}}"#,
            ids = [r#""$aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa""#; 10].join(","),
            key = "a".repeat(43),
            sig = "a".repeat(86),
        );
        assert!(pdu.len() <= MATRIX_EVENT_MAX_BYTES, "{}", pdu.len());
    }

    #[test]
    fn one_huge_line_is_cut_on_a_character_boundary() {
        let plain = "ä".repeat(40_000);
        let (plain, html) = fit_to_event(plain, String::new(), 1_000);
        assert!(content_json_len(&plain, &html) <= 1_000);
        assert!(plain.starts_with("ää"));
    }
}
