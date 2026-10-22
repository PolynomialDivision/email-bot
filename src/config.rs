use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashSet;

use mxbot_common::matrix_sdk::ruma::OwnedUserId;

pub use mxbot_common::config::MatrixConfig;

#[derive(Deserialize)]
pub struct Config {
    pub matrix: MatrixConfig,
    pub imap: ImapConfig,
    pub mailing_list: MailingListConfig,
    #[serde(default)]
    pub security: SecurityConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
    pub smtp: Option<SmtpConfig>,
}

impl Config {
    /// Values serde accepts but the bot cannot work with.
    pub fn validate(&self) -> Result<()> {
        if self.imap.poll_interval_secs == Some(0) {
            anyhow::bail!("[imap] poll_interval_secs must be at least 1");
        }
        if self
            .smtp
            .as_ref()
            .is_some_and(|smtp| smtp.list_confirmation_timeout_secs == Some(0))
        {
            anyhow::bail!("[smtp] list_confirmation_timeout_secs must be at least 1 (omit it to turn the notice off)");
        }
        Ok(())
    }
}

#[derive(Deserialize)]
pub struct ImapConfig {
    pub host: String,
    #[serde(default = "default_imap_port")]
    pub port: u16,
    pub username: String,
    // password loaded from IMAP_PASSWORD env var at runtime
    pub mailbox: Option<String>,
    pub poll_interval_secs: Option<u64>,
    pub initial_fetch_limit: Option<usize>,
    pub skip_initial_history: Option<bool>,
}

fn default_imap_port() -> u16 {
    993
}

impl ImapConfig {
    pub fn effective_mailbox(&self) -> &str {
        self.mailbox.as_deref().unwrap_or("INBOX")
    }

    pub fn effective_poll_interval_secs(&self) -> u64 {
        self.poll_interval_secs.unwrap_or(60)
    }

    pub fn effective_initial_fetch_limit(&self) -> usize {
        self.initial_fetch_limit.unwrap_or(50)
    }

    pub fn effective_skip_initial_history(&self) -> bool {
        self.skip_initial_history.unwrap_or(true)
    }
}

#[derive(Deserialize, Default, Clone)]
pub struct MailingListConfig {
    pub list_id: Option<String>,
    pub sender_domains: Option<Vec<String>>,
    pub sender_emails: Option<Vec<String>>,
    pub subject_prefix: Option<String>,
}

#[derive(Deserialize, Default, Clone)]
pub struct LimitsConfig {
    pub max_attachment_bytes: Option<usize>,
    pub max_body_bytes: Option<usize>,
    pub max_quote_lines: Option<usize>,
}

impl LimitsConfig {
    pub fn effective_max_attachment_bytes(&self) -> usize {
        self.max_attachment_bytes.unwrap_or(10 * 1024 * 1024)
    }

    pub fn effective_max_body_bytes(&self) -> usize {
        self.max_body_bytes.unwrap_or(512 * 1024)
    }

    pub fn effective_max_quote_lines(&self) -> usize {
        self.max_quote_lines.unwrap_or(5)
    }
}

/// The shared `[security]` table plus the bridge-specific replier list.
#[derive(Deserialize, Default)]
pub struct SecurityConfig {
    #[serde(flatten)]
    pub common: mxbot_common::config::SecurityConfig,
    /// Matrix users allowed to send email replies via the bridge.
    /// Empty means all room members may reply.
    #[serde(default)]
    pub allowed_repliers: Vec<String>,
}

#[derive(Deserialize, Clone)]
pub struct SmtpConfig {
    pub host: String,
    #[serde(default = "default_smtp_port")]
    pub port: u16,
    pub username: String,
    // password loaded from SMTP_PASSWORD env var at runtime
    /// Address used in the From header of bridge-sent emails.
    pub from_address: String,
    /// The mailing list posting address: used as To and Reply-To.
    pub list_address: String,
    /// true = implicit TLS / SMTPS (port 465).
    /// false (default) = STARTTLS (port 587).
    #[serde(default)]
    pub require_smtps: bool,
    /// If true, top-level Matrix messages in allowed rooms are sent to the
    /// mailing list as new email threads. Replies in Matrix threads are always
    /// handled when SMTP is configured.
    #[serde(default)]
    pub allow_new_threads_from_matrix: bool,
    /// If set, notify the Matrix room when no mailing-list copy has returned
    /// within this many seconds. Missing confirmation never triggers a resend.
    pub list_confirmation_timeout_secs: Option<u64>,
}

fn default_smtp_port() -> u16 {
    587
}

pub struct Secrets {
    pub imap_password: String,
    /// Optional: only required when [smtp] is configured.
    pub smtp_password: Option<String>,
}

impl Secrets {
    pub fn from_env() -> Result<Self> {
        let imap_password =
            std::env::var("IMAP_PASSWORD").context("IMAP_PASSWORD env var not set")?;
        let smtp_password = std::env::var("SMTP_PASSWORD").ok();
        Ok(Self {
            imap_password,
            smtp_password,
        })
    }
}

pub fn parse_allowed_repliers(security: &SecurityConfig) -> Result<HashSet<OwnedUserId>> {
    let mut set = HashSet::new();
    for s in &security.allowed_repliers {
        let uid = s
            .parse::<OwnedUserId>()
            .with_context(|| format!("Invalid Matrix user ID in allowed_repliers: {:?}", s))?;
        set.insert(uid);
    }
    Ok(set)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// house-infra's rendered config (build/rendered/…/email-bot.config.toml; addresses,
    /// room and user IDs replaced).
    const HOUSE_INFRA: &str = include_str!("../tests/fixtures/house-infra.config.toml");

    #[test]
    fn house_infra_config_loads_and_validates() {
        let config: Config = toml::from_str(HOUSE_INFRA).unwrap();
        config.validate().unwrap();
        assert_eq!(config.imap.effective_poll_interval_secs(), 60);
        let smtp = config.smtp.as_ref().unwrap();
        assert!(smtp.require_smtps && smtp.allow_new_threads_from_matrix);
        assert_eq!(smtp.list_confirmation_timeout_secs, Some(3600));
        assert_eq!(config.limits.effective_max_body_bytes(), 512 * 1024);
        assert!(parse_allowed_repliers(&config.security).unwrap().is_empty());
    }

    #[test]
    fn a_zero_poll_interval_is_refused() {
        let config: Config = toml::from_str(
            &HOUSE_INFRA.replace("poll_interval_secs = 60", "poll_interval_secs = 0"),
        )
        .unwrap();
        assert!(config.validate().is_err());
    }
}
