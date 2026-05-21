use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashSet;

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

#[derive(Deserialize)]
pub struct MatrixConfig {
    pub homeserver: String,
    pub user_id: String,
    pub access_token: String,
    pub device_id: String,
    pub recovery_key: Option<String>,
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

#[derive(Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EncryptionStrategy {
    AllDevices,
    #[default]
    IdentityBased,
    OnlyTrusted,
}

impl From<EncryptionStrategy> for matrix_sdk_crypto::CollectStrategy {
    fn from(s: EncryptionStrategy) -> Self {
        match s {
            EncryptionStrategy::AllDevices => {
                matrix_sdk_crypto::CollectStrategy::AllDevices
            }
            EncryptionStrategy::IdentityBased => {
                matrix_sdk_crypto::CollectStrategy::IdentityBasedStrategy
            }
            EncryptionStrategy::OnlyTrusted => {
                matrix_sdk_crypto::CollectStrategy::OnlyTrustedDevices
            }
        }
    }
}

// Serde helper: deserializes either the string "all" or a list of strings.
#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
pub(crate) enum RawAllowList {
    Wildcard(String),
    List(Vec<String>),
}
impl Default for RawAllowList {
    fn default() -> Self {
        RawAllowList::Wildcard("all".to_owned())
    }
}

#[derive(Debug, Clone)]
pub enum UserAllowList {
    All,
    Deny,
    Explicit(HashSet<matrix_sdk::ruma::OwnedUserId>),
}
impl UserAllowList {
    pub fn allows(&self, user: &matrix_sdk::ruma::OwnedUserId) -> bool {
        match self {
            Self::All => true,
            Self::Deny => false,
            Self::Explicit(set) => set.contains(user),
        }
    }
    pub fn is_allow_all(&self) -> bool {
        matches!(self, Self::All)
    }
    pub fn is_deny_all(&self) -> bool {
        matches!(self, Self::Deny)
    }
    pub fn explicit_count(&self) -> Option<usize> {
        if let Self::Explicit(set) = self { Some(set.len()) } else { None }
    }
}

#[derive(Debug, Clone)]
pub enum RoomAllowList {
    All,
    Deny,
    Explicit(HashSet<matrix_sdk::ruma::OwnedRoomId>),
}
impl RoomAllowList {
    pub fn allows(&self, room_id: &matrix_sdk::ruma::RoomId) -> bool {
        match self {
            Self::All => true,
            Self::Deny => false,
            Self::Explicit(set) => set.contains(room_id),
        }
    }
    pub fn is_allow_all(&self) -> bool {
        matches!(self, Self::All)
    }
    pub fn is_deny_all(&self) -> bool {
        matches!(self, Self::Deny)
    }
    pub fn explicit_count(&self) -> Option<usize> {
        if let Self::Explicit(set) = self { Some(set.len()) } else { None }
    }
}

#[derive(Deserialize, Default)]
pub struct SecurityConfig {
    #[serde(default)]
    pub admin_users: Vec<String>,
    /// "all" = accept invites from any user; [] = reject all invites; explicit list = allowlist.
    #[serde(default)]
    pub allowed_inviters: RawAllowList,
    /// "all" = operate in any room; [] = operate in no room; explicit list = allowlist.
    #[serde(default)]
    pub allowed_rooms: RawAllowList,
    /// Matrix users allowed to send email replies via the bridge.
    /// Empty means all room members may reply.
    #[serde(default)]
    pub allowed_repliers: Vec<String>,
    #[serde(default)]
    pub encryption_strategy: EncryptionStrategy,
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
    /// The mailing list posting address — used as To and Reply-To.
    pub list_address: String,
    /// true = implicit TLS / SMTPS (port 465).
    /// false (default) = STARTTLS (port 587).
    #[serde(default)]
    pub require_smtps: bool,
}

fn default_smtp_port() -> u16 {
    587
}

pub struct Secrets {
    pub imap_password: String,
    /// Optional — only required when [smtp] is configured.
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

pub fn parse_admin_users(
    security: &SecurityConfig,
) -> HashSet<matrix_sdk::ruma::OwnedUserId> {
    security
        .admin_users
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect()
}

pub fn parse_allowed_inviters(security: &SecurityConfig) -> Result<UserAllowList> {
    match &security.allowed_inviters {
        RawAllowList::Wildcard(s) if s == "all" => Ok(UserAllowList::All),
        RawAllowList::Wildcard(s) => anyhow::bail!(
            "Invalid allowed_inviters value: {:?} (expected \"all\" or a list of Matrix user IDs)",
            s
        ),
        RawAllowList::List(list) if list.is_empty() => Ok(UserAllowList::Deny),
        RawAllowList::List(list) => {
            let mut set = HashSet::new();
            for s in list {
                let uid = s
                    .parse::<matrix_sdk::ruma::OwnedUserId>()
                    .with_context(|| {
                        format!("Invalid Matrix user ID in allowed_inviters: {:?}", s)
                    })?;
                set.insert(uid);
            }
            Ok(UserAllowList::Explicit(set))
        }
    }
}

pub fn parse_allowed_rooms(security: &SecurityConfig) -> Result<RoomAllowList> {
    match &security.allowed_rooms {
        RawAllowList::Wildcard(s) if s == "all" => Ok(RoomAllowList::All),
        RawAllowList::Wildcard(s) => anyhow::bail!(
            "Invalid allowed_rooms value: {:?} (expected \"all\" or a list of Matrix room IDs)",
            s
        ),
        RawAllowList::List(list) if list.is_empty() => Ok(RoomAllowList::Deny),
        RawAllowList::List(list) => {
            let mut set = HashSet::new();
            for s in list {
                let rid = s
                    .parse::<matrix_sdk::ruma::OwnedRoomId>()
                    .with_context(|| {
                        format!("Invalid Matrix room ID in allowed_rooms: {:?}", s)
                    })?;
                set.insert(rid);
            }
            Ok(RoomAllowList::Explicit(set))
        }
    }
}

pub fn parse_allowed_repliers(
    security: &SecurityConfig,
) -> HashSet<matrix_sdk::ruma::OwnedUserId> {
    security
        .allowed_repliers
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect()
}
