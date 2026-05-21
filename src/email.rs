use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use mail_parser::MimeHeaders;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::config::{LimitsConfig, MailingListConfig};
use crate::db::Db;
use crate::html_clean::html_to_plain;

#[derive(Debug, Clone)]
pub struct RawEmail {
    pub uid: u32,
    pub mailbox: String,
    pub raw: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attachment {
    pub filename: String,
    pub content_type: String,
    #[serde(with = "serde_bytes_base64")]
    pub data: Vec<u8>,
}

// Custom serde module for Vec<u8> as base64 string
mod serde_bytes_base64 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(data: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(data);
        s.serialize_str(&encoded)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        use base64::Engine;
        let s = String::deserialize(d)?;
        base64::engine::general_purpose::STANDARD
            .decode(&s)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedEmail {
    pub uid: u32,
    pub mailbox: String,
    pub message_id: String,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub subject: String,
    pub from_name: Option<String>,
    pub from_email: String,
    pub date: Option<DateTime<Utc>>,
    pub list_id: Option<String>,
    pub body_plain: Option<String>,
    pub body_html: Option<String>,
    pub attachments: Vec<Attachment>,
    pub thread_root: Option<String>,
    /// Set to "matrix" for emails sent by this bridge — used for loop prevention.
    #[serde(default)]
    pub bridge_origin: Option<String>,
}

fn clean_message_id(id: &str) -> String {
    id.trim().trim_start_matches('<').trim_end_matches('>').to_owned()
}

fn extract_from(msg: &mail_parser::Message) -> (Option<String>, String) {
    if let Some(addr) = msg.from() {
        match addr {
            mail_parser::Address::List(list) => {
                if let Some(a) = list.first() {
                    let name = a.name.as_deref().map(|s: &str| s.to_owned());
                    let email = a.address.as_deref().unwrap_or("unknown").to_owned();
                    return (name, email);
                }
            }
            mail_parser::Address::Group(groups) => {
                for group in groups {
                    if let Some(a) = group.addresses.first() {
                        let name = a.name.as_deref().map(|s: &str| s.to_owned());
                        let email = a.address.as_deref().unwrap_or("unknown").to_owned();
                        return (name, email);
                    }
                }
            }
        }
    }
    (None, "unknown".to_owned())
}

fn extract_references(msg: &mail_parser::Message) -> Vec<String> {
    use mail_parser::HeaderName;

    let mut refs = Vec::new();

    // Extract from References header
    for header in msg.headers() {
        if header.name == HeaderName::References {
            match &header.value {
                mail_parser::HeaderValue::Text(t) => {
                    for part in t.split_whitespace() {
                        let cleaned = clean_message_id(part);
                        if !cleaned.is_empty() {
                            refs.push(cleaned);
                        }
                    }
                }
                mail_parser::HeaderValue::TextList(list) => {
                    for item in list {
                        let cleaned = clean_message_id(item);
                        if !cleaned.is_empty() {
                            refs.push(cleaned);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    refs
}

fn extract_in_reply_to(msg: &mail_parser::Message) -> Option<String> {
    use mail_parser::HeaderName;

    for header in msg.headers() {
        if header.name == HeaderName::InReplyTo {
            match &header.value {
                mail_parser::HeaderValue::Text(t) => {
                    let cleaned = clean_message_id(t);
                    if !cleaned.is_empty() {
                        return Some(cleaned);
                    }
                }
                mail_parser::HeaderValue::TextList(list) => {
                    if let Some(first) = list.first() {
                        let cleaned = clean_message_id(first);
                        if !cleaned.is_empty() {
                            return Some(cleaned);
                        }
                    }
                }
                _ => {}
            }
        }
    }
    None
}

fn extract_bridge_origin(msg: &mail_parser::Message) -> Option<String> {
    if let Some(mail_parser::HeaderValue::Text(t)) = msg.header("X-Bridge-Origin") {
        let val = t.as_ref().trim().to_lowercase();
        if !val.is_empty() {
            return Some(val);
        }
    }
    None
}

fn extract_list_id(msg: &mail_parser::Message) -> Option<String> {
    use mail_parser::{Address, HeaderName, HeaderValue};

    // RFC 2919 List-Id can be plain text ("listname.lists.example.com") or an
    // address-like value ("Description <listname.lists.example.com>").
    // mail-parser returns the latter as HeaderValue::Address, not Text.
    let extract = |value: &HeaderValue| -> Option<String> {
        match value {
            HeaderValue::Text(t) => Some(t.as_ref().to_owned()),
            HeaderValue::Address(Address::List(list)) => {
                list.first()?.address.as_deref().map(str::to_owned)
            }
            HeaderValue::Address(Address::Group(groups)) => {
                groups.first()?.addresses.first()?.address.as_deref().map(str::to_owned)
            }
            _ => None,
        }
    };

    for header in msg.headers() {
        if header.name == HeaderName::Other("List-Id".into()) || header.name == HeaderName::ListId {
            if let Some(v) = extract(&header.value) {
                return Some(v);
            }
        }
    }

    // Fallback via string lookup
    if let Some(val) = msg.header("List-Id") {
        if let Some(v) = extract(val) {
            return Some(v);
        }
    }

    None
}

pub fn parse(raw: &RawEmail, limits: &LimitsConfig) -> Result<ParsedEmail> {
    debug!(uid = raw.uid, mailbox = %raw.mailbox, bytes = raw.raw.len(), "MIME: parsing email");

    let msg = mail_parser::MessageParser::default()
        .parse(&raw.raw)
        .ok_or_else(|| anyhow!("mail-parser failed to parse email uid={} (raw bytes: {})", raw.uid, raw.raw.len()))?;

    let message_id = clean_message_id(msg.message_id().unwrap_or(""));
    let message_id = if message_id.is_empty() {
        let generated = format!("uid-{}", raw.uid);
        warn!(uid = raw.uid, mailbox = %raw.mailbox, generated_id = %generated, "MIME: no Message-Id header — using generated ID");
        generated
    } else {
        message_id
    };

    let in_reply_to = extract_in_reply_to(&msg);
    let references = extract_references(&msg);

    let subject = msg.subject().unwrap_or("(no subject)").to_owned();
    let (from_name, from_email) = extract_from(&msg);

    let date = msg.date().and_then(|d| DateTime::from_timestamp(d.to_timestamp(), 0));

    let list_id = extract_list_id(&msg);
    let bridge_origin = extract_bridge_origin(&msg);

    debug!(
        uid = raw.uid,
        message_id = %message_id,
        subject = %subject,
        from = %from_email,
        has_in_reply_to = in_reply_to.is_some(),
        references_count = references.len(),
        has_list_id = list_id.is_some(),
        bridge_origin = ?bridge_origin,
        "MIME: headers parsed"
    );

    let max_body = limits.effective_max_body_bytes();

    let body_plain = msg.body_text(0).map(|b| {
        let s: &str = &b;
        if s.len() > max_body {
            warn!(
                "Truncating plain body of uid={} from {} to {} bytes",
                raw.uid,
                s.len(),
                max_body
            );
            let truncated = &s[..max_body];
            // Truncate at last valid UTF-8 boundary
            match std::str::from_utf8(truncated.as_bytes()) {
                Ok(v) => format!("{}\n[truncated]", v),
                Err(e) => format!("{}\n[truncated]", &truncated[..e.valid_up_to()]),
            }
        } else {
            s.to_owned()
        }
    });

    let body_html = msg.body_html(0).map(|b| {
        let s: &str = &b;
        if s.len() > max_body {
            s[..max_body].to_owned()
        } else {
            s.to_owned()
        }
    });

    let max_attach = limits.effective_max_attachment_bytes();
    let mut attachments = Vec::new();

    let mut i = 0usize;
    loop {
        match msg.attachment(i as u32) {
            Some(att) => {
                i += 1;
                let filename = att
                    .attachment_name()
                    .unwrap_or("attachment")
                    .to_owned();
                let content_type = if let Some(ct) = att.content_type() {
                    let subtype = ct.c_subtype.as_deref().unwrap_or("octet-stream");
                    format!("{}/{}", ct.c_type, subtype)
                } else {
                    "application/octet-stream".to_owned()
                };
                let data = att.contents();
                if data.len() > max_attach {
                    warn!(
                        "Skipping attachment '{}' of uid={}: {} bytes exceeds limit {}",
                        filename,
                        raw.uid,
                        data.len(),
                        max_attach
                    );
                    continue;
                }
                attachments.push(Attachment {
                    filename,
                    content_type,
                    data: data.to_vec(),
                });
            }
            None => break,
        }
    }

    debug!(
        uid = raw.uid,
        message_id = %message_id,
        has_body_plain = body_plain.is_some(),
        has_body_html = body_html.is_some(),
        attachment_count = attachments.len(),
        "MIME: parse complete"
    );

    Ok(ParsedEmail {
        uid: raw.uid,
        mailbox: raw.mailbox.clone(),
        message_id,
        in_reply_to,
        references,
        subject,
        from_name,
        from_email,
        date,
        list_id,
        body_plain,
        body_html,
        attachments,
        thread_root: None,
        bridge_origin,
    })
}

pub fn is_mailing_list_email(parsed: &ParsedEmail, config: &MailingListConfig) -> bool {
    // Check List-Id header
    if let (Some(ref list_id), Some(ref cfg_list_id)) = (&parsed.list_id, &config.list_id) {
        if list_id.contains(cfg_list_id.as_str()) {
            return true;
        }
    }

    // Fallback: check sender domain
    if let Some(ref domains) = config.sender_domains {
        let email_lower = parsed.from_email.to_lowercase();
        for domain in domains {
            if email_lower.ends_with(&format!("@{}", domain.to_lowercase())) {
                return true;
            }
        }
    }

    // Fallback: exact sender match
    if let Some(ref emails) = config.sender_emails {
        let email_lower = parsed.from_email.to_lowercase();
        for addr in emails {
            if email_lower == addr.to_lowercase() {
                return true;
            }
        }
    }

    // Fallback: subject prefix
    if let Some(ref prefix) = config.subject_prefix {
        if parsed.subject.starts_with(prefix.as_str()) {
            return true;
        }
    }

    false
}

pub async fn reconstruct_thread(parsed: &mut ParsedEmail, db: &Db) {
    let mut candidates: Vec<String> = Vec::new();

    if let Some(ref irt) = parsed.in_reply_to.clone() {
        candidates.push(irt.clone());
    }
    // References are oldest → newest; walk newest → oldest to prefer most recent ancestor
    for r in parsed.references.iter().rev() {
        if !candidates.contains(r) {
            candidates.push(r.clone());
        }
    }

    debug!(
        message_id = %parsed.message_id,
        candidate_count = candidates.len(),
        candidates = ?candidates,
        "thread_reconstruct: searching for thread root"
    );

    for candidate in &candidates {
        match db.get_thread_root(candidate).await {
            Ok(Some((_event_id, thread_root_id))) => {
                debug!(
                    message_id = %parsed.message_id,
                    matched_candidate = %candidate,
                    thread_root_id = %thread_root_id,
                    "thread_reconstruct: found thread root"
                );
                parsed.thread_root = Some(thread_root_id);
                return;
            }
            Ok(None) => {
                debug!(message_id = %parsed.message_id, candidate = %candidate, "thread_reconstruct: candidate not in DB");
                continue;
            }
            Err(e) => {
                warn!(
                    message_id = %parsed.message_id,
                    candidate = %candidate,
                    error = %e,
                    "thread_reconstruct: DB error"
                );
                continue;
            }
        }
    }

    debug!(message_id = %parsed.message_id, "thread_reconstruct: no thread root found — will become new thread root");
    parsed.thread_root = None;
}

pub fn get_plain_body(email: &ParsedEmail) -> String {
    if let Some(ref plain) = email.body_plain {
        return plain.clone();
    }
    if let Some(ref html) = email.body_html {
        return html_to_plain(html);
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{LimitsConfig, MailingListConfig};

    fn make_limits() -> LimitsConfig {
        LimitsConfig {
            max_attachment_bytes: Some(1024 * 1024),
            max_body_bytes: Some(512 * 1024),
            max_quote_lines: Some(5),
        }
    }

    #[test]
    fn test_clean_message_id_strips_brackets() {
        assert_eq!(
            clean_message_id("<abc@example.com>"),
            "abc@example.com"
        );
        assert_eq!(clean_message_id("abc@example.com"), "abc@example.com");
        assert_eq!(clean_message_id("  <abc@def>  "), "abc@def");
    }

    #[test]
    fn test_parse_minimal_email() {
        let raw_bytes = b"From: test@example.com\r\nTo: dest@example.com\r\nSubject: Hello\r\nMessage-ID: <test123@example.com>\r\n\r\nBody text here.";
        let raw = RawEmail {
            uid: 1,
            mailbox: "INBOX".to_owned(),
            raw: raw_bytes.to_vec(),
        };
        let limits = make_limits();
        let parsed = parse(&raw, &limits).unwrap();
        assert_eq!(parsed.message_id, "test123@example.com");
        assert_eq!(parsed.subject, "Hello");
        assert_eq!(parsed.from_email, "test@example.com");
        assert!(parsed.body_plain.as_deref().unwrap_or("").contains("Body text here."));
    }

    #[test]
    fn test_parse_empty_body() {
        let raw_bytes = b"From: test@example.com\r\nSubject: Empty\r\nMessage-ID: <empty@example.com>\r\n\r\n";
        let raw = RawEmail {
            uid: 2,
            mailbox: "INBOX".to_owned(),
            raw: raw_bytes.to_vec(),
        };
        let limits = make_limits();
        let parsed = parse(&raw, &limits).unwrap();
        assert_eq!(parsed.subject, "Empty");
        // body_plain should be None or empty
        let body = parsed.body_plain.as_deref().unwrap_or("");
        assert!(body.is_empty() || body.len() < 5);
    }

    #[test]
    fn test_parse_missing_headers() {
        let raw_bytes = b"From: nobody@example.com\r\n\r\nContent here";
        let raw = RawEmail {
            uid: 3,
            mailbox: "INBOX".to_owned(),
            raw: raw_bytes.to_vec(),
        };
        let limits = make_limits();
        let parsed = parse(&raw, &limits).unwrap();
        assert_eq!(parsed.subject, "(no subject)");
        assert!(parsed.message_id.starts_with("uid-"));
    }

    #[test]
    fn test_is_mailing_list_email_list_id() {
        let email = ParsedEmail {
            uid: 1,
            mailbox: "INBOX".to_owned(),
            message_id: "id".to_owned(),
            in_reply_to: None,
            references: vec![],
            subject: "Test".to_owned(),
            from_name: None,
            from_email: "user@example.com".to_owned(),
            date: None,
            list_id: Some("<mylist.lists.example.com>".to_owned()),
            body_plain: None,
            body_html: None,
            attachments: vec![],
            thread_root: None,
            bridge_origin: None,
        };

        let config = MailingListConfig {
            list_id: Some("mylist.lists.example.com".to_owned()),
            sender_domains: None,
            sender_emails: None,
            subject_prefix: None,
        };

        assert!(is_mailing_list_email(&email, &config));
    }

    #[test]
    fn test_is_mailing_list_email_sender_domain() {
        let email = ParsedEmail {
            uid: 1,
            mailbox: "INBOX".to_owned(),
            message_id: "id".to_owned(),
            in_reply_to: None,
            references: vec![],
            subject: "Test".to_owned(),
            from_name: None,
            from_email: "bot@lists.example.com".to_owned(),
            date: None,
            list_id: None,
            body_plain: None,
            body_html: None,
            attachments: vec![],
            thread_root: None,
            bridge_origin: None,
        };

        let config = MailingListConfig {
            list_id: None,
            sender_domains: Some(vec!["lists.example.com".to_owned()]),
            sender_emails: None,
            subject_prefix: None,
        };

        assert!(is_mailing_list_email(&email, &config));
    }

    #[test]
    fn test_is_mailing_list_email_subject_prefix() {
        let email = ParsedEmail {
            uid: 1,
            mailbox: "INBOX".to_owned(),
            message_id: "id".to_owned(),
            in_reply_to: None,
            references: vec![],
            subject: "[mylist] Hello everyone".to_owned(),
            from_name: None,
            from_email: "user@example.com".to_owned(),
            date: None,
            list_id: None,
            body_plain: None,
            body_html: None,
            attachments: vec![],
            thread_root: None,
            bridge_origin: None,
        };

        let config = MailingListConfig {
            list_id: None,
            sender_domains: None,
            sender_emails: None,
            subject_prefix: Some("[mylist]".to_owned()),
        };

        assert!(is_mailing_list_email(&email, &config));
    }

    #[test]
    fn test_is_not_mailing_list_email() {
        let email = ParsedEmail {
            uid: 1,
            mailbox: "INBOX".to_owned(),
            message_id: "id".to_owned(),
            in_reply_to: None,
            references: vec![],
            subject: "Personal email".to_owned(),
            from_name: None,
            from_email: "friend@gmail.com".to_owned(),
            date: None,
            list_id: None,
            body_plain: None,
            body_html: None,
            attachments: vec![],
            thread_root: None,
            bridge_origin: None,
        };

        let config = MailingListConfig {
            list_id: Some("mylist".to_owned()),
            sender_domains: Some(vec!["lists.example.com".to_owned()]),
            sender_emails: None,
            subject_prefix: Some("[mylist]".to_owned()),
        };

        assert!(!is_mailing_list_email(&email, &config));
    }
}
