/// Sanitize HTML for Matrix: strip dangerous tags/attributes using ammonia.
pub fn sanitize_html(html: &str) -> String {
    ammonia::clean(html)
}

/// Convert HTML to plain text for Matrix plain-text fallback.
pub fn html_to_plain(html: &str) -> String {
    html2text::from_read(html.as_bytes(), 80).unwrap_or_default()
}

/// Simple tag-stripping fallback for testing / emergency use.
#[cfg_attr(not(test), allow(dead_code))]
fn strip_tags_fallback(html: &str) -> String {
    let mut result = String::with_capacity(html.len());
    let mut in_tag = false;
    for ch in html.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => result.push(ch),
            _ => {}
        }
    }
    // Decode basic HTML entities
    result
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_strips_script() {
        let dirty = r#"<p>Hello</p><script>alert('xss')</script>"#;
        let clean = sanitize_html(dirty);
        assert!(!clean.contains("<script>"));
        assert!(clean.contains("Hello"));
    }

    #[test]
    fn test_html_to_plain_basic() {
        let html = "<p>Hello <strong>world</strong>!</p>";
        let plain = html_to_plain(html);
        assert!(plain.contains("Hello"));
        assert!(plain.contains("world"));
    }

    #[test]
    fn test_strip_tags_fallback_entities() {
        let html = "<p>A &amp; B &lt;tag&gt;</p>";
        let plain = strip_tags_fallback(html);
        assert!(plain.contains("A & B <tag>"));
    }
}
