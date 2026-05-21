use chrono::{DateTime, Utc};

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
        "{}{}\nFrom: {} · {}\n\n{}",
        prefix, subject, sender, date_str, body_collapsed
    );

    // Build HTML
    let subject_escaped = html_escape(&subject);
    let sender_escaped = html_escape(&sender);
    let date_escaped = html_escape(&date_str);

    let body_html = build_html_body(email, &body_collapsed, max_quote);

    let header_html = if is_reply {
        format!(
            "↩️ <strong>Re: {}</strong><br>\n<em>{}</em> · <em>{}</em><br><br>\n",
            subject_escaped, sender_escaped, date_escaped
        )
    } else {
        format!(
            "📧 <strong>{}</strong><br>\n<em>{}</em> · <em>{}</em><br><br>\n",
            subject_escaped, sender_escaped, date_escaped
        )
    };

    let html = format!("{}{}", header_html, body_html);

    (plain, html)
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
/// "[... N more lines quoted]" if truncated.
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
            result.push(format!("[... {} more lines quoted]", total - shown));
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

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
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
                "<em>[... {} more lines quoted]</em><br>\n",
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
        assert_eq!(
            clean_subject("[mylist] [sub] Hello world"),
            "Hello world"
        );
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
}
