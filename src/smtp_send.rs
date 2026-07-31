use anyhow::{Context, Result};
use lettre::address::Address;
use lettre::message::{
    header::{ContentType, HeaderName, HeaderValue},
    Mailbox,
};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use uuid::Uuid;

use crate::config::SmtpConfig;

#[derive(Serialize, Deserialize)]
pub struct SmtpReply {
    /// Matrix sender's display name, used in the From header.
    pub display_name: String,
    /// Full Matrix ID of the sender (e.g. "@user:server.org").
    /// Embedded in the From display name and the body attribution block.
    /// serde(default) keeps old retry payloads in the DB deserializable.
    #[serde(default)]
    pub sender_matrix_id: String,
    /// Stable secondary correlation key for mailing-list echoes.
    #[serde(default)]
    pub matrix_event_id: String,
    /// Email Message-Id being directly replied to (stripped of angle brackets).
    #[serde(default)]
    pub in_reply_to: String,
    /// Older ancestor Message-Ids for the References header (oldest first, stripped).
    #[serde(default)]
    pub references: Vec<String>,
    pub subject: String,
    pub body: String,
    /// Pre-generated so the caller can persist it before sending (loop prevention).
    pub our_message_id: String,
}

impl SmtpReply {
    pub fn new(
        display_name: String,
        sender_matrix_id: String,
        matrix_event_id: String,
        in_reply_to: String,
        references: Vec<String>,
        subject: String,
        body: String,
    ) -> Self {
        let our_message_id = format!("{}@email-bridge.local", Uuid::new_v4());
        Self {
            display_name,
            sender_matrix_id,
            matrix_event_id,
            in_reply_to,
            references,
            subject,
            body,
            our_message_id,
        }
    }

    pub fn new_thread(
        display_name: String,
        sender_matrix_id: String,
        matrix_event_id: String,
        subject: String,
        body: String,
    ) -> Self {
        let our_message_id = format!("{}@email-bridge.local", Uuid::new_v4());
        Self {
            display_name,
            sender_matrix_id,
            matrix_event_id,
            in_reply_to: String::new(),
            references: Vec::new(),
            subject,
            body,
            our_message_id,
        }
    }

    pub fn is_thread_reply(&self) -> bool {
        !self.in_reply_to.trim().is_empty()
    }
}

pub async fn send_reply(config: &SmtpConfig, password: &str, reply: &SmtpReply) -> Result<()> {
    let tls_mode = if config.require_smtps {
        "SMTPS (implicit TLS)"
    } else {
        "STARTTLS"
    };

    info!(
        smtp_host = %config.host,
        smtp_port = config.port,
        tls_mode = tls_mode,
        smtp_username = %config.username,
        from_address = %config.from_address,
        to = %config.list_address,
        message_id = %reply.our_message_id,
        in_reply_to = if reply.is_thread_reply() { reply.in_reply_to.as_str() } else { "" },
        subject = %reply.subject,
        display_name = %reply.display_name,
        is_thread_reply = reply.is_thread_reply(),
        "SMTP: sending Matrix-originated email"
    );

    debug!("SMTP: building RFC 5322 message");
    let message = build_email(config, reply)?;
    debug!(raw_bytes = message.formatted().len(), "SMTP: message built");

    let creds = Credentials::new(config.username.clone(), password.to_owned());
    debug!(
        smtp_host = %config.host,
        smtp_port = config.port,
        tls_mode = tls_mode,
        "SMTP: building transport"
    );
    let transport = build_transport(config, creds).with_context(|| {
        format!(
            "SMTP: build_transport for {}:{} ({}) failed",
            config.host, config.port, tls_mode
        )
    })?;
    debug!("SMTP: transport built — connecting and sending");

    let t = std::time::Instant::now();
    let response = transport.send(message).await.with_context(|| {
        format!(
            "SMTP send failed for <{}> via {}:{} ({}) — check credentials and server reachability",
            reply.our_message_id, config.host, config.port, tls_mode
        )
    })?;

    info!(
        smtp_host = %config.host,
        smtp_port = config.port,
        tls_mode = tls_mode,
        message_id = %reply.our_message_id,
        to = %config.list_address,
        smtp_response_code = %response.code(),
        smtp_response = ?response.message().collect::<Vec<_>>(),
        elapsed_ms = t.elapsed().as_millis(),
        "SMTP: relay accepted message"
    );
    Ok(())
}

fn build_email(config: &SmtpConfig, reply: &SmtpReply) -> Result<Message> {
    // From display name: "Alice (Matrix: @alice:example.org)"
    // DMARC is unaffected — the domain inside <addr> stays as our own sending domain.
    // Mailing lists that munge From for DMARC compliance only care about the <addr> domain.
    let name = sanitize_header_value(&reply.display_name);
    let matrix_id: String = reply
        .sender_matrix_id
        .chars()
        .filter(|c| !c.is_control())
        .collect();
    let from_display = if matrix_id.is_empty() {
        name
    } else {
        format!("{} (Matrix: {})", name, matrix_id)
    };

    // Prepend a clear attribution block so mailing list recipients always see who
    // wrote the message, even if the From header is munged by list software.
    let body_with_attribution = if matrix_id.is_empty() {
        reply.body.clone()
    } else {
        format!(
            "[Bridged from Matrix]\nUser: {}\nMatrix ID: {}\n---\n\n{}",
            reply.display_name, reply.sender_matrix_id, reply.body,
        )
    };

    let subject = if reply.is_thread_reply() && !reply.subject.to_lowercase().starts_with("re:") {
        format!("Re: {}", reply.subject)
    } else {
        reply.subject.clone()
    };
    let subject = sanitize_header_value(&subject);

    let reply_headers = if reply.is_thread_reply() {
        // Build deduplicated References chain: ancestors + direct parent (oldest -> newest)
        let irt = strip_angle_brackets(&reply.in_reply_to);
        let mut ref_ids: Vec<String> = reply
            .references
            .iter()
            .map(|r| strip_angle_brackets(r))
            .filter(|r| !r.is_empty())
            .collect();
        if !irt.is_empty() && !ref_ids.contains(&irt) {
            ref_ids.push(irt.clone());
        }
        let references_header = ref_ids
            .iter()
            .map(|r| format!("<{}>", r))
            .collect::<Vec<_>>()
            .join(" ");
        Some((irt, references_header))
    } else {
        None
    };

    let from_addr: Address = config.from_address.parse().with_context(|| {
        format!(
            "SMTP: parsing from_address '{}' failed",
            config.from_address
        )
    })?;
    let list_addr: Address = config.list_address.parse().with_context(|| {
        format!(
            "SMTP: parsing list_address '{}' failed",
            config.list_address
        )
    })?;

    let mut builder = Message::builder()
        .from(Mailbox::new(Some(from_display), from_addr))
        .to(Mailbox::new(None, list_addr.clone()))
        .reply_to(Mailbox::new(None, list_addr))
        .subject(subject)
        .message_id(Some(format!("<{}>", reply.our_message_id)))
        .header(ContentType::TEXT_PLAIN)
        .raw_header(HeaderValue::new(
            HeaderName::new_from_ascii_str("X-Bridge-Origin"),
            "matrix".to_owned(),
        ));

    if !reply.matrix_event_id.is_empty() {
        let matrix_event_id: String = reply
            .matrix_event_id
            .chars()
            .filter(|c| !c.is_control())
            .collect();
        builder = builder.raw_header(HeaderValue::new(
            HeaderName::new_from_ascii_str("X-Matrix-Event-ID"),
            matrix_event_id,
        ));
    }

    if let Some((irt, references_header)) = reply_headers {
        builder = builder.in_reply_to(format!("<{}>", irt));
        if !references_header.is_empty() {
            builder = builder.references(references_header);
        }
    }

    builder
        .body(body_with_attribution)
        .context("SMTP: building RFC 5322 message")
}

fn build_transport(
    config: &SmtpConfig,
    creds: Credentials,
) -> Result<AsyncSmtpTransport<Tokio1Executor>> {
    if config.require_smtps {
        debug!(host = %config.host, port = config.port, "SMTP: using SMTPS (implicit TLS / relay)");
        let t = AsyncSmtpTransport::<Tokio1Executor>::relay(&config.host)
            .with_context(|| format!("SMTP: building SMTPS relay for {} failed", config.host))?
            .port(config.port)
            .credentials(creds)
            .build();
        Ok(t)
    } else {
        debug!(host = %config.host, port = config.port, "SMTP: using STARTTLS");
        let t = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&config.host)
            .with_context(|| format!("SMTP: building STARTTLS relay for {} failed", config.host))?
            .port(config.port)
            .credentials(creds)
            .build();
        Ok(t)
    }
}

fn strip_angle_brackets(s: &str) -> String {
    s.trim()
        .trim_start_matches('<')
        .trim_end_matches('>')
        .to_owned()
}

/// Remove control characters and newlines from a value that goes into an email header.
fn sanitize_header_value(s: &str) -> String {
    s.chars()
        .filter(|c| !c.is_control())
        .collect::<String>()
        .trim()
        .chars()
        .take(64)
        .collect()
}

#[cfg(test)]
mod tests {
    use mail_parser::MessageParser;

    use super::{build_email, SmtpReply};
    use crate::config::SmtpConfig;

    fn config() -> SmtpConfig {
        SmtpConfig {
            host: "smtp.example.org".to_owned(),
            port: 465,
            username: "bridge@example.org".to_owned(),
            from_address: "bridge@example.org".to_owned(),
            list_address: "list@example.net".to_owned(),
            require_smtps: true,
            allow_new_threads_from_matrix: true,
            list_confirmation_timeout_secs: None,
        }
    }

    #[test]
    fn unicode_matrix_subject_is_rfc_encoded() {
        let reply = SmtpReply::new_thread(
            "unaxcornx".to_owned(),
            "@sender:matrix.org".to_owned(),
            "$event:matrix.org".to_owned(),
            "unaxcornx via Matrix: Guest from 6–9 August".to_owned(),
            "Guest from 6–9 August".to_owned(),
        );

        let raw = build_email(&config(), &reply).unwrap().formatted();
        let header_end = raw
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("message has a header/body separator");
        assert!(raw[..header_end].is_ascii());

        let parsed = MessageParser::default().parse(&raw).unwrap();
        assert_eq!(
            parsed.subject(),
            Some("unaxcornx via Matrix: Guest from 6–9 August")
        );
    }

    #[test]
    fn thread_headers_and_loop_guard_are_preserved() {
        let reply = SmtpReply::new(
            "Alice".to_owned(),
            "@alice:matrix.org".to_owned(),
            "$event:matrix.org".to_owned(),
            "parent@example.org".to_owned(),
            vec!["root@example.org".to_owned()],
            "Existing thread".to_owned(),
            "Reply body".to_owned(),
        );

        let raw = build_email(&config(), &reply).unwrap().formatted();
        let raw = String::from_utf8(raw).unwrap();
        assert!(raw.contains("In-Reply-To: <parent@example.org>\r\n"));
        assert!(raw.contains("References: <root@example.org> <parent@example.org>\r\n"));
        assert!(raw.contains("X-Bridge-Origin: matrix\r\n"));
        assert!(raw.contains("X-Matrix-Event-ID: $event:matrix.org\r\n"));
    }
}
