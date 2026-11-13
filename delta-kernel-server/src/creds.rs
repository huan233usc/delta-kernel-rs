//! Storage credentials from Unity Catalog (SPEC v9 D2).
//!
//! The client forwards its workspace host and bearer token (`GetSnapshotRequest.auth`). For a
//! cloud table the server exchanges them for a temporary storage credential:
//!
//! - `table_id` is a UC table UUID: `POST {host}/api/2.1/unity-catalog/temporary-table-credentials`
//!   with `{table_id, operation: "READ"}`;
//! - otherwise: `POST {host}/api/2.1/unity-catalog/temporary-path-credentials` with `{url,
//!   operation: "PATH_READ"}`.
//!
//! Credentials are cached per `(sha256(token), table key)` until 60 s before they expire. The
//! token itself is never stored, logged, or put into an error message. Every request for a cloud
//! table goes through [`CredentialProvider::credential_for`] before the snapshot cache is
//! consulted, so a cache hit still requires a valid credential for the caller's token (the POC's
//! authz check). `file://` tables need no credential.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use delta_kernel::object_store::{self, DynObjectStore};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use url::Url;

use crate::errors::ServerError;
use crate::local_io::LocalIo;
use crate::proto::error::Code;
use crate::proto::{self};

/// Credentials are dropped this long before UC's expiration time.
pub const EXPIRY_MARGIN: Duration = Duration::from_secs(60);

/// UC's credential for one table, plus the IO built on it.
pub struct StorageCredential {
    /// object_store options derived from the UC response (secret values).
    pub options: Vec<(String, String)>,
    /// Epoch milliseconds after which the credential must not be used (already minus the margin).
    pub usable_until_ms: i64,
    pub io: Arc<LocalIo>,
}

impl std::fmt::Debug for StorageCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Option values are secrets: print keys only.
        let keys: Vec<&str> = self.options.iter().map(|(k, _)| k.as_str()).collect();
        f.debug_struct("StorageCredential")
            .field("option_keys", &keys)
            .field("usable_until_ms", &self.usable_until_ms)
            .finish()
    }
}

/// Which UC endpoint a table uses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialRequest {
    Table { table_id: String },
    Path { url: String },
}

impl CredentialRequest {
    pub fn for_table(table_id: &str, location: &Url) -> Self {
        if is_uc_uuid(table_id) {
            Self::Table {
                table_id: table_id.to_string(),
            }
        } else {
            Self::Path {
                url: location.to_string(),
            }
        }
    }

    fn endpoint(&self) -> &'static str {
        match self {
            Self::Table { .. } => "/api/2.1/unity-catalog/temporary-table-credentials",
            Self::Path { .. } => "/api/2.1/unity-catalog/temporary-path-credentials",
        }
    }

    fn body(&self) -> Value {
        match self {
            Self::Table { table_id } => json!({ "table_id": table_id, "operation": "READ" }),
            Self::Path { url } => json!({ "url": url, "operation": "PATH_READ" }),
        }
    }

    fn cache_key(&self) -> &str {
        match self {
            Self::Table { table_id } => table_id,
            Self::Path { url } => url,
        }
    }
}

/// UC table ids are UUIDs (`8-4-4-4-12` hex).
pub fn is_uc_uuid(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 36
        && b.iter().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => *c == b'-',
            _ => c.is_ascii_hexdigit(),
        })
}

/// Hex SHA-256 of a bearer token: the only form of the token the server keeps.
pub fn token_hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

/// Maps a UC temporary-credentials response to object_store options for `scheme`.
pub fn storage_options(scheme: &str, resp: &Value) -> Result<Vec<(String, String)>, String> {
    let s = |v: &Value, k: &str| -> Result<String, String> {
        v.get(k)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| format!("UC credential response is missing {k}"))
    };
    match scheme {
        "s3" | "s3a" => {
            let c = resp
                .get("aws_temp_credentials")
                .ok_or("UC response has no aws_temp_credentials for an s3 table")?;
            Ok(vec![
                ("aws_access_key_id".into(), s(c, "access_key_id")?),
                ("aws_secret_access_key".into(), s(c, "secret_access_key")?),
                ("aws_session_token".into(), s(c, "session_token")?),
            ])
        }
        "abfss" | "abfs" | "az" | "azure" => {
            let c = resp
                .get("azure_user_delegation_sas")
                .ok_or("UC response has no azure_user_delegation_sas for an abfss table")?;
            Ok(vec![("azure_storage_sas_token".into(), s(c, "sas_token")?)])
        }
        "gs" => {
            let c = resp
                .get("gcp_oauth_token")
                .ok_or("UC response has no gcp_oauth_token for a gs table")?;
            Ok(vec![("google_bearer_token".into(), s(c, "oauth_token")?)])
        }
        other => Err(format!("no UC credential mapping for scheme {other}")),
    }
}

/// Per-request timeout of server-side cloud IO. Local ops cannot be preempted the way a forwarded
/// op's deadline is enforced, so the object store's HTTP client bounds each call instead.
pub const LOCAL_IO_TIMEOUT: &str = "60s";

/// Mozilla's root CAs, bundled into the binary. Every HTTPS client the server builds (UC and the
/// object stores) trusts them in addition to the system store, so the server also runs in a
/// minimal container without `/etc/ssl/certs`.
fn bundled_roots() -> impl Iterator<Item = &'static [u8]> {
    webpki_root_certs::TLS_SERVER_ROOT_CERTS
        .iter()
        .map(|c| c.as_ref())
}

/// Builds the UC HTTP client. Fails only if no usable root certificate can be loaded at all.
pub fn build_uc_http_client() -> Result<reqwest::Client, String> {
    let roots = bundled_roots()
        .map(reqwest::Certificate::from_der)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("bundled root certificates: {e}"))?;
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .tls_certs_merge(roots)
        .build()
        .map_err(|e| format!("{e:?}"))
}

/// Storage schemes the server builds object stores for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CloudScheme {
    S3,
    Azure,
    Gcs,
}

impl CloudScheme {
    const ALL: [CloudScheme; 3] = [CloudScheme::S3, CloudScheme::Azure, CloudScheme::Gcs];

    fn of(location: &Url) -> Result<Self, String> {
        match object_store::ObjectStoreScheme::parse(location) {
            Ok((object_store::ObjectStoreScheme::AmazonS3, _)) => Ok(Self::S3),
            Ok((object_store::ObjectStoreScheme::MicrosoftAzure, _)) => Ok(Self::Azure),
            Ok((object_store::ObjectStoreScheme::GoogleCloudStorage, _)) => Ok(Self::Gcs),
            _ => Err(format!(
                "no cloud object store for scheme {}",
                location.scheme()
            )),
        }
    }

    /// The canonical object_store name of `key` for this store, or `None` if the store does not
    /// take it.
    fn canonical_key(self, key: &str) -> Option<String> {
        use object_store::aws::AmazonS3ConfigKey;
        use object_store::azure::AzureConfigKey;
        use object_store::gcp::GoogleConfigKey;
        let key = key.to_ascii_lowercase();
        match self {
            Self::S3 => key
                .parse::<AmazonS3ConfigKey>()
                .ok()
                .map(|k| k.as_ref().to_string()),
            Self::Azure => key
                .parse::<AzureConfigKey>()
                .ok()
                .map(|k| k.as_ref().to_string()),
            Self::Gcs => key
                .parse::<GoogleConfigKey>()
                .ok()
                .map(|k| k.as_ref().to_string()),
        }
    }
}

/// Client (transport) options an operator may set for every cloud store.
pub const CLIENT_OPTION_ALLOWLIST: &[&str] = &[
    "allow_http",
    "timeout",
    "connect_timeout",
    "read_timeout",
    "pool_idle_timeout",
    "pool_max_idle_per_host",
    "http1_only",
    "http2_only",
    "proxy_url",
    "proxy_ca_certificate",
    "proxy_excludes",
];

impl CloudScheme {
    fn name(self) -> &'static str {
        match self {
            Self::S3 => "S3",
            Self::Azure => "Azure",
            Self::Gcs => "GCS",
        }
    }

    /// Store-specific addressing keys an operator may set (canonical names). Nothing that selects
    /// or supplies a credential: the store must authenticate with the UC-vended credential only.
    fn addressing_allowlist(self) -> &'static [&'static str] {
        match self {
            Self::S3 => &[
                "aws_endpoint",
                "aws_region",
                "aws_virtual_hosted_style_request",
            ],
            Self::Azure => &["azure_storage_endpoint"],
            Self::Gcs => &["google_base_url"],
        }
    }

    fn allows(self, canonical: &str) -> bool {
        self.addressing_allowlist().contains(&canonical)
            || CLIENT_OPTION_ALLOWLIST.contains(&canonical)
    }

    /// Auth settings applied after the vended credential, overriding everything: the store
    /// authenticates with the vended credential and never falls back to an unsigned or
    /// server-side identity.
    fn forced_auth(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::S3 => &[("aws_skip_signature", "false")],
            Self::Azure => &[
                ("azure_credential_type", "sas_token"),
                ("azure_skip_signature", "false"),
                ("azure_storage_use_emulator", "false"),
                ("azure_use_azure_cli", "false"),
            ],
            Self::Gcs => &[("google_skip_signature", "false")],
        }
    }

    /// Vended keys a store must have (canonical names, as produced by [`storage_options`]).
    fn required_vended(self) -> &'static [&'static str] {
        match self {
            Self::S3 => &[
                "aws_access_key_id",
                "aws_secret_access_key",
                "aws_session_token",
            ],
            Self::Azure => &["azure_storage_sas_key"],
            Self::Gcs => &["google_bearer_token"],
        }
    }
}

/// The final operator allowlist, per cloud (README, startup errors).
pub fn storage_option_allowlist() -> String {
    let per_cloud: Vec<String> = CloudScheme::ALL
        .iter()
        .map(|s| format!("{}: {}", s.name(), s.addressing_allowlist().join(", ")))
        .collect();
    format!(
        "{}; any store: {}",
        per_cloud.join("; "),
        CLIENT_OPTION_ALLOWLIST.join(", ")
    )
}

/// Server-side object_store options (`--storage-option`, `--s3-*`), applied to every cloud store
/// the server builds, under the UC-vended credential options. Only allowlisted transport and
/// addressing keys are accepted (see [`storage_option_allowlist`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageOverrides(Vec<(String, String)>);

impl StorageOverrides {
    /// Validates every key against the allowlist. A key must be an object_store key of at least
    /// one store, and allowlisted for every store that takes it (`token` is an S3 session token
    /// and an Azure bearer token: rejected). A key applies only to the stores that take it.
    pub fn new(options: Vec<(String, String)>) -> Result<Self, String> {
        for (k, _) in &options {
            let parsed: Vec<(CloudScheme, String)> = CloudScheme::ALL
                .iter()
                .filter_map(|s| s.canonical_key(k).map(|c| (*s, c)))
                .collect();
            if parsed.is_empty() {
                return Err(format!(
                    "unknown object_store option {k:?}; allowed: {}",
                    storage_option_allowlist()
                ));
            }
            if let Some((scheme, canonical)) = parsed.iter().find(|(s, c)| !s.allows(c)) {
                return Err(format!(
                    "--storage-option {k:?} ({} key {canonical}) is not allowed: operator options \
                     may only set transport/addressing, never authentication or credential \
                     sources (the store must use the UC-vended credential); allowed: {}",
                    scheme.name(),
                    storage_option_allowlist()
                ));
            }
        }
        Ok(Self(options))
    }

    /// Parses `KEY=VALUE`.
    pub fn parse_pair(s: &str) -> Result<(String, String), String> {
        match s.split_once('=') {
            Some((k, v)) if !k.trim().is_empty() => Ok((k.trim().to_string(), v.to_string())),
            _ => Err(format!("expected KEY=VALUE, got {s:?}")),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Option keys (values may be secrets).
    pub fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|(k, _)| k.as_str()).collect()
    }
}

/// The options a cloud store is built with, by canonical key, in precedence order: the default
/// [`LOCAL_IO_TIMEOUT`], the allowlisted server overrides, the UC-vended options, then the
/// forced auth settings of the store. The vended credential must be complete; it is then the
/// only credential the store can use.
pub fn merge_store_options(
    location: &Url,
    overrides: &StorageOverrides,
    vended: &[(String, String)],
) -> Result<Vec<(String, String)>, String> {
    let scheme = CloudScheme::of(location)?;
    let mut merged: Vec<(String, String)> = Vec::new();
    let mut put = |key: String, value: &str| {
        merged.retain(|(existing, _)| *existing != key);
        merged.push((key, value.to_string()));
    };
    put("timeout".to_string(), LOCAL_IO_TIMEOUT);
    for (k, v) in &overrides.0 {
        // `new` validated the keys; re-check so no construction path can pass an auth key.
        if let Some(key) = scheme.canonical_key(k).filter(|c| scheme.allows(c)) {
            put(key, v);
        }
    }
    let mut vended_keys = Vec::new();
    for (k, v) in vended {
        let key = scheme
            .canonical_key(k)
            .ok_or_else(|| format!("vended option {k} is not a {} key", scheme.name()))?;
        vended_keys.push(key.clone());
        put(key, v);
    }
    for (k, v) in scheme.forced_auth() {
        put((*k).to_string(), v);
    }
    if let Some(missing) = scheme
        .required_vended()
        .iter()
        .find(|k| !vended_keys.iter().any(|v| v == *k))
    {
        return Err(format!(
            "UC-vended credential for a {} table is missing {missing}",
            scheme.name()
        ));
    }
    Ok(merged)
}

/// Builds the object store for a cloud table from UC-derived options plus the server overrides.
/// With an S3 endpoint and no `aws_virtual_hosted_style_request`, requests are path-style
/// (`{endpoint}/{bucket}/{key}`), which is what LocalStack and MinIO expect.
pub fn build_store(
    location: &Url,
    overrides: &StorageOverrides,
    vended: &[(String, String)],
) -> Result<Arc<DynObjectStore>, String> {
    let options = merge_store_options(location, overrides, vended)?;
    let mut client = object_store::ClientOptions::new();
    for der in bundled_roots() {
        client = client.with_root_certificate(
            object_store::Certificate::from_der(der)
                .map_err(|e| format!("bundled root certificate: {e}"))?,
        );
    }
    let err = |e: object_store::Error| format!("object store for {}: {e}", location.scheme());
    let store: Arc<DynObjectStore> = match CloudScheme::of(location)? {
        CloudScheme::S3 => {
            let mut b = object_store::aws::AmazonS3Builder::new()
                .with_url(location.as_str())
                .with_client_options(client);
            for (k, v) in options {
                b = b.with_config(k.parse().map_err(err)?, v);
            }
            Arc::new(b.build().map_err(err)?)
        }
        CloudScheme::Azure => {
            let mut b = object_store::azure::MicrosoftAzureBuilder::new()
                .with_url(location.as_str())
                .with_client_options(client);
            for (k, v) in options {
                b = b.with_config(k.parse().map_err(err)?, v);
            }
            Arc::new(b.build().map_err(err)?)
        }
        CloudScheme::Gcs => {
            let mut b = object_store::gcp::GoogleCloudStorageBuilder::new()
                .with_url(location.as_str())
                .with_client_options(client);
            for (k, v) in options {
                b = b.with_config(k.parse().map_err(err)?, v);
            }
            Arc::new(b.build().map_err(err)?)
        }
    };
    Ok(store)
}

/// Longest UC-provided text kept in an error message.
pub const MAX_UC_TEXT: usize = 256;

/// Makes UC-provided text safe for errors and logs: the caller's token and anything that looks
/// like a credential (`Bearer …`, `dapi…` PATs, JWTs, long opaque tokens) are replaced by
/// `[REDACTED]`, control characters are dropped, and the result is cut to [`MAX_UC_TEXT`] chars.
pub fn sanitize_uc_text(text: &str, token: &str) -> String {
    let text = if token.is_empty() {
        text.to_string()
    } else {
        text.replace(token, "[REDACTED]")
    };
    let looks_secret = |w: &str| {
        let core = w.trim_matches(|c: char| !c.is_ascii_alphanumeric());
        let lower = core.to_ascii_lowercase();
        lower.starts_with("dapi")
            || core.starts_with("eyJ")
            || (core.len() >= 24
                && core
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || "-_.=+/".contains(c)))
    };
    let mut out: Vec<String> = Vec::new();
    let mut redact_next = false;
    for word in text.split_whitespace() {
        if redact_next || looks_secret(word) {
            out.push("[REDACTED]".into());
            redact_next = false;
            continue;
        }
        if word.eq_ignore_ascii_case("bearer") || word.eq_ignore_ascii_case("bearer:") {
            redact_next = true;
        }
        out.push(word.chars().filter(|c| !c.is_control()).collect());
    }
    let joined = out.join(" ");
    if joined.chars().count() > MAX_UC_TEXT {
        let cut: String = joined.chars().take(MAX_UC_TEXT).collect();
        format!("{cut}…")
    } else {
        joined
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

pub struct CredentialProvider {
    /// `Err` if no TLS root could be loaded: UC calls then fail with UNAVAILABLE instead of the
    /// server refusing to start.
    http: Result<reqwest::Client, String>,
    /// Server-side object_store options for every cloud store.
    overrides: StorageOverrides,
    /// Replaces `https://{workspace_host}` as the UC base URL (tests, proxies).
    endpoint_override: Option<String>,
    /// Treat `file://` tables like cloud tables (test hook: exercises the UC path locally).
    credentials_for_file: bool,
    cache: Mutex<HashMap<(String, String), Arc<StorageCredential>>>,
    /// UC calls made (tests).
    pub fetches: AtomicU64,
}

impl CredentialProvider {
    pub fn new(
        endpoint_override: Option<String>,
        credentials_for_file: bool,
        overrides: StorageOverrides,
    ) -> Self {
        let http = build_uc_http_client();
        if let Err(e) = &http {
            tracing::error!(error = %e, "cannot build the UC HTTP client; UC calls will fail");
        }
        Self {
            http,
            overrides,
            endpoint_override,
            credentials_for_file,
            cache: Mutex::default(),
            fetches: AtomicU64::new(0),
        }
    }

    pub fn clear(&self) {
        self.cache.lock().unwrap().clear();
    }

    /// Returns the IO for `location` after checking (or fetching) the caller's credential.
    /// `Ok(None)` for tables that need no credential (`file://`).
    pub async fn credential_for(
        &self,
        auth: Option<&proto::Auth>,
        table_id: &str,
        location: &Url,
    ) -> Result<Option<Arc<StorageCredential>>, ServerError> {
        let is_file = location.scheme() == "file";
        if is_file && !self.credentials_for_file {
            return Ok(None);
        }
        let Some(auth) = auth.filter(|a| !a.bearer_token.is_empty()) else {
            return Err(ServerError::new(
                Code::PermissionDenied,
                "GetSnapshotRequest.auth is required for cloud storage tables",
            ));
        };
        let req = CredentialRequest::for_table(table_id, location);
        let key = (token_hash(&auth.bearer_token), req.cache_key().to_string());
        if let Some(c) = self.cache.lock().unwrap().get(&key)
            && c.usable_until_ms > now_ms()
        {
            return Ok(Some(c.clone()));
        }
        let resp = self.fetch(auth, &req).await?;
        let expiration = resp
            .get("expiration_time")
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                ServerError::internal("UC credential response has no expiration_time")
            })?;
        let usable_until_ms = expiration - EXPIRY_MARGIN.as_millis() as i64;
        let (options, io) = if is_file {
            // Test hook: the credential is checked and cached, but local files need none.
            (Vec::new(), Arc::new(LocalIo::local_fs()))
        } else {
            let options =
                storage_options(location.scheme(), &resp).map_err(ServerError::internal)?;
            let store =
                build_store(location, &self.overrides, &options).map_err(ServerError::internal)?;
            (options, Arc::new(LocalIo::new(store)))
        };
        let cred = Arc::new(StorageCredential {
            options,
            usable_until_ms,
            io,
        });
        if usable_until_ms > now_ms() {
            self.cache.lock().unwrap().insert(key, cred.clone());
        } else {
            tracing::warn!(
                table = req.cache_key(),
                "UC credential expires within the margin; not cached"
            );
        }
        Ok(Some(cred))
    }

    fn base_url(&self, auth: &proto::Auth) -> String {
        match &self.endpoint_override {
            Some(base) => base.trim_end_matches('/').to_string(),
            None => {
                let host = auth.workspace_host.trim_end_matches('/');
                if host.starts_with("http://") || host.starts_with("https://") {
                    host.to_string()
                } else {
                    format!("https://{host}")
                }
            }
        }
    }

    async fn fetch(
        &self,
        auth: &proto::Auth,
        req: &CredentialRequest,
    ) -> Result<Value, ServerError> {
        self.fetches.fetch_add(1, Ordering::Relaxed);
        let url = format!("{}{}", self.base_url(auth), req.endpoint());
        let unavailable = |what: String| {
            ServerError::new(
                Code::Unavailable,
                format!(
                    "UC unavailable: {}",
                    sanitize_uc_text(&what, &auth.bearer_token)
                ),
            )
        };
        let http = self
            .http
            .as_ref()
            .map_err(|e| unavailable(format!("UC HTTP client unavailable: {e}")))?;
        let resp = http
            .post(&url)
            .bearer_auth(&auth.bearer_token)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(req.body().to_string())
            .send()
            .await
            // reqwest errors carry the URL but never request headers.
            .map_err(|e| unavailable(e.without_url().to_string()))?;
        let status = resp.status();
        let body = resp
            .bytes()
            .await
            .map_err(|e| unavailable(e.without_url().to_string()))?;
        let parsed: Option<Value> = serde_json::from_slice(&body).ok();
        // UC error text is untrusted: it may echo request data. Only a sanitized, bounded copy
        // reaches ServerError (and therefore logs and the client).
        let uc_message = parsed
            .as_ref()
            .map(|v| {
                format!(
                    "{} {}",
                    v.get("error_code").and_then(Value::as_str).unwrap_or(""),
                    v.get("message").and_then(Value::as_str).unwrap_or("")
                )
            })
            .map(|m| sanitize_uc_text(&m, &auth.bearer_token))
            .unwrap_or_default();
        match status.as_u16() {
            200..=299 => {
                parsed.ok_or_else(|| ServerError::internal("UC returned a non-JSON credential"))
            }
            401 | 403 => Err(ServerError::new(
                Code::PermissionDenied,
                format!(
                    "UC denied storage credentials ({status}): {}",
                    uc_message.trim()
                ),
            )),
            404 => Err(ServerError::new(
                Code::TableNotFound,
                format!("UC: table or path not found: {}", uc_message.trim()),
            )),
            s if s >= 500 => Err(unavailable(format!("{status} {}", uc_message.trim()))),
            _ => Err(ServerError::internal(format!(
                "UC credential request failed ({status}): {}",
                uc_message.trim()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_detection_picks_endpoint() {
        let loc = Url::parse("s3://b/t/").unwrap();
        assert!(matches!(
            CredentialRequest::for_table("123e4567-e89b-12d3-a456-426614174000", &loc),
            CredentialRequest::Table { .. }
        ));
        for not_uuid in [
            "main.default.t",
            "s3://b/t/",
            "123e4567e89b12d3a456426614174000",
            "",
        ] {
            assert_eq!(
                CredentialRequest::for_table(not_uuid, &loc),
                CredentialRequest::Path {
                    url: "s3://b/t/".into()
                }
            );
        }
    }

    fn none() -> StorageOverrides {
        StorageOverrides::default()
    }

    fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn opts(v: Vec<(String, String)>) -> HashMap<String, String> {
        v.into_iter().collect()
    }

    #[test]
    fn aws_mapping_builds_s3_store() {
        let resp = json!({"aws_temp_credentials": {"access_key_id": "AK", "secret_access_key": "SK", "session_token": "ST"}, "expiration_time": 1});
        let o = storage_options("s3", &resp).unwrap();
        let m = opts(o.clone());
        assert_eq!(m["aws_access_key_id"], "AK");
        assert_eq!(m["aws_secret_access_key"], "SK");
        assert_eq!(m["aws_session_token"], "ST");
        build_store(&Url::parse("s3://bucket/t/").unwrap(), &none(), &o).unwrap();
        assert!(storage_options("s3", &json!({})).is_err());
    }

    #[test]
    fn azure_mapping_builds_abfss_store() {
        let resp = json!({"azure_user_delegation_sas": {"sas_token": "sv=2024&sig=x"}});
        let o = storage_options("abfss", &resp).unwrap();
        assert_eq!(opts(o.clone())["azure_storage_sas_token"], "sv=2024&sig=x");
        build_store(
            &Url::parse("abfss://c@acct.dfs.core.windows.net/t/").unwrap(),
            &none(),
            &o,
        )
        .unwrap();
        assert!(storage_options("abfss", &json!({"aws_temp_credentials": {}})).is_err());
    }

    #[test]
    fn gcp_mapping_builds_gs_store() {
        let resp = json!({"gcp_oauth_token": {"oauth_token": "ya29.x"}});
        let o = storage_options("gs", &resp).unwrap();
        assert_eq!(opts(o.clone())["google_bearer_token"], "ya29.x");
        build_store(&Url::parse("gs://bucket/t/").unwrap(), &none(), &o).unwrap();
    }

    #[test]
    fn uc_text_is_sanitized_and_bounded() {
        let tok = "dapiabc123def456";
        let s = sanitize_uc_text(
            &format!("denied for {tok}; header was Bearer {tok} jwt eyJhbGciOi.x.y ok\u{7}"),
            tok,
        );
        assert!(!s.contains(tok) && !s.contains("eyJ"), "{s}");
        assert!(s.contains("denied for") && s.contains("[REDACTED]"));
        // Token text that is not on a word boundary is still removed.
        assert!(!sanitize_uc_text(&format!("x{tok}y"), tok).contains(tok));
        // A long opaque value is redacted even if it is not the caller's token.
        assert!(
            !sanitize_uc_text("key opaqueOpaqueOpaqueOpaqueValue123 here", "t")
                .contains("opaqueOpaque")
        );
        let long = "word ".repeat(200);
        assert!(sanitize_uc_text(&long, "t").chars().count() <= MAX_UC_TEXT + 1);
        assert_eq!(
            sanitize_uc_text("PERMISSION_DENIED no access", "t"),
            "PERMISSION_DENIED no access"
        );
    }

    #[test]
    fn unknown_scheme_is_rejected() {
        assert!(storage_options("hdfs", &json!({})).is_err());
    }

    #[test]
    fn debug_hides_option_values() {
        let c = StorageCredential {
            options: vec![("aws_secret_access_key".into(), "TOPSECRET".into())],
            usable_until_ms: 0,
            io: Arc::new(LocalIo::local_fs()),
        };
        let d = format!("{c:?}");
        assert!(d.contains("aws_secret_access_key") && !d.contains("TOPSECRET"));
        assert_eq!(token_hash("abc").len(), 64);
        assert_ne!(token_hash("abc"), "abc");
    }

    fn s3_vended() -> Vec<(String, String)> {
        pairs(&[
            ("aws_access_key_id", "AK"),
            ("aws_secret_access_key", "SK"),
            ("aws_session_token", "ST"),
        ])
    }

    #[test]
    fn server_options_merge_under_vended_credentials() {
        let overrides = StorageOverrides::new(pairs(&[
            ("aws_endpoint", "http://localstack:4566"),
            ("AWS_ALLOW_HTTP", "true"),
            ("region", "us-east-1"),
            ("timeout", "5s"),
            // Azure-only: does not reach an S3 store.
            ("azure_storage_endpoint", "http://azurite:10000"),
        ]))
        .unwrap();
        let s3 = Url::parse("s3://test/t/").unwrap();
        let merged = merge_store_options(&s3, &overrides, &s3_vended()).unwrap();
        let m = opts(merged.clone());
        assert_eq!(
            m.len(),
            merged.len(),
            "one entry per canonical key: {merged:?}"
        );
        assert_eq!(m["aws_access_key_id"], "AK");
        assert_eq!(m["aws_secret_access_key"], "SK");
        assert_eq!(m["aws_session_token"], "ST");
        assert_eq!(m["aws_endpoint"], "http://localstack:4566");
        assert_eq!(m["allow_http"], "true");
        assert_eq!(m["aws_region"], "us-east-1");
        assert_eq!(
            m["timeout"], "5s",
            "a server option replaces the default timeout"
        );
        assert_eq!(m["aws_skip_signature"], "false", "forced");
        assert!(!m.contains_key("azure_storage_endpoint"));

        // No overrides: the vended credential, the default timeout, the forced auth.
        let m = opts(merge_store_options(&s3, &none(), &s3_vended()).unwrap());
        assert_eq!(m.len(), 5, "{m:?}");
        assert_eq!(m["timeout"], LOCAL_IO_TIMEOUT);

        // The Azure store keeps its own keys and the generic client keys only, and is forced to
        // authenticate with the vended SAS.
        let az = Url::parse("abfss://c@acct.dfs.core.windows.net/t/").unwrap();
        let sas = pairs(&[("azure_storage_sas_token", "sv=1&sig=x")]);
        let m = opts(merge_store_options(&az, &overrides, &sas).unwrap());
        assert_eq!(m["azure_storage_endpoint"], "http://azurite:10000");
        assert_eq!(m["azure_storage_sas_key"], "sv=1&sig=x");
        assert_eq!(m["azure_credential_type"], "sas_token");
        assert_eq!(m["azure_skip_signature"], "false");
        assert_eq!(m["azure_storage_use_emulator"], "false");
        assert_eq!(m["azure_use_azure_cli"], "false");
        assert!(!m.keys().any(|k| k.starts_with("aws_")), "{m:?}");

        let gs = Url::parse("gs://bucket/t/").unwrap();
        let tok = pairs(&[("google_bearer_token", "ya29.vended")]);
        let m = opts(merge_store_options(&gs, &none(), &tok).unwrap());
        assert_eq!(m["google_bearer_token"], "ya29.vended");
        assert_eq!(m["google_skip_signature"], "false");
    }

    /// Defense in depth: even an override list that bypassed `StorageOverrides::new` cannot put
    /// an auth key into a store, and forced auth wins over everything.
    #[test]
    fn merge_drops_unvalidated_auth_keys_and_forces_vended_auth() {
        let raw = StorageOverrides(pairs(&[
            ("aws_access_key_id", "SERVER-KEY"),
            ("aws_skip_signature", "true"),
            ("aws_web_identity_token_file", "/var/run/token"),
            ("azure_credential_type", "managed_identity"),
            ("azure_storage_account_key", "SERVER-ACCOUNT-KEY"),
            ("azure_use_azure_cli", "true"),
            ("google_service_account", "/sa.json"),
            ("google_skip_signature", "true"),
        ]));
        let s3 = Url::parse("s3://test/t/").unwrap();
        let m = opts(merge_store_options(&s3, &raw, &s3_vended()).unwrap());
        assert_eq!(m["aws_access_key_id"], "AK");
        assert_eq!(m["aws_skip_signature"], "false");
        assert!(!m.contains_key("aws_web_identity_token_file"), "{m:?}");
        let az = Url::parse("abfss://c@acct.dfs.core.windows.net/t/").unwrap();
        let m = opts(
            merge_store_options(&az, &raw, &pairs(&[("azure_storage_sas_token", "sig=v")]))
                .unwrap(),
        );
        assert_eq!(m["azure_credential_type"], "sas_token");
        assert_eq!(m["azure_use_azure_cli"], "false");
        assert!(!m.contains_key("azure_storage_account_key"), "{m:?}");
        let gs = Url::parse("gs://bucket/t/").unwrap();
        let m =
            opts(merge_store_options(&gs, &raw, &pairs(&[("google_bearer_token", "v")])).unwrap());
        assert_eq!(m["google_skip_signature"], "false");
        assert!(!m.contains_key("google_service_account"), "{m:?}");

        // A store is never built without the full vended credential.
        assert!(merge_store_options(&s3, &none(), &[]).is_err());
        assert!(merge_store_options(&s3, &none(), &pairs(&[("aws_access_key_id", "AK")])).is_err());
        assert!(merge_store_options(&az, &none(), &[]).is_err());
        assert!(merge_store_options(&gs, &none(), &[]).is_err());
    }

    /// Every auth / credential-source key of each cloud is rejected at startup.
    #[test]
    fn operator_auth_options_are_rejected_per_cloud() {
        let s3 = [
            "aws_access_key_id",
            "aws_secret_access_key",
            "aws_session_token",
            "token",
            "aws_skip_signature",
            "aws_imdsv1_fallback",
            "aws_metadata_endpoint",
            "aws_web_identity_token_file",
            "aws_role_arn",
            "aws_role_session_name",
            "aws_endpoint_url_sts",
            "aws_container_credentials_relative_uri",
            "aws_container_credentials_full_uri",
            "aws_container_authorization_token_file",
            "aws_unsigned_payload",
            "aws_s3_express",
            "aws_request_payer",
            "aws_server_side_encryption",
        ];
        let azure = [
            "azure_credential_type",
            "azure_storage_account_key",
            "access_key",
            "azure_storage_client_id",
            "azure_storage_client_secret",
            "azure_storage_tenant_id",
            "azure_storage_authority_host",
            "azure_storage_sas_token",
            "azure_storage_token",
            "bearer_token",
            "azure_storage_use_emulator",
            "azure_msi_endpoint",
            "azure_object_id",
            "azure_msi_resource_id",
            "azure_federated_token_file",
            "azure_use_azure_cli",
            "azure_skip_signature",
            "azure_use_fabric_endpoint",
            "azure_fabric_token_service_url",
            "azure_fabric_session_token",
            "azure_storage_account_name",
        ];
        let gcs = [
            "google_service_account",
            "google_service_account_key",
            "google_application_credentials",
            "google_bearer_token",
            "google_skip_signature",
        ];
        let client = [
            "allow_invalid_certificates",
            "disable_system_certificates",
            "user_agent",
            "default_content_type",
        ];
        for key in s3.iter().chain(&azure).chain(&gcs).chain(&client) {
            let err = StorageOverrides::new(pairs(&[(key, "x")]))
                .expect_err(&format!("{key} must be rejected"));
            assert!(
                err.contains("not allowed") && err.contains("allowed: S3: aws_endpoint"),
                "{key}: {err}"
            );
        }
        for key in [
            "aws_endpoint",
            "endpoint",
            "aws_region",
            "aws_virtual_hosted_style_request",
            "aws_allow_http",
            "azure_storage_endpoint",
            "google_base_url",
            "timeout",
            "connect_timeout",
            "proxy_url",
        ] {
            StorageOverrides::new(pairs(&[(key, "x")]))
                .unwrap_or_else(|e| panic!("{key} must be allowed: {e}"));
        }
    }

    #[test]
    fn unknown_or_malformed_storage_options_are_rejected() {
        let err = StorageOverrides::new(pairs(&[("aws_endpointt", "x")])).unwrap_err();
        assert!(err.contains("unknown object_store option"), "{err}");
        assert_eq!(
            StorageOverrides::parse_pair("aws_endpoint=http://h:1/?a=b").unwrap(),
            ("aws_endpoint".to_string(), "http://h:1/?a=b".to_string())
        );
        assert!(StorageOverrides::parse_pair("novalue").is_err());
        assert!(StorageOverrides::parse_pair("=v").is_err());
    }

    /// One HTTP/1.1 request on a fresh port: answers 404 and returns the raw request head.
    async fn capture_one_request() -> (String, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 16 * 1024];
            let mut n = 0;
            while !buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                n += sock.read(&mut buf[n..]).await.unwrap();
            }
            sock.write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
            String::from_utf8_lossy(&buf[..n]).to_string()
        });
        (endpoint, server)
    }

    async fn get_not_found(store: &DynObjectStore, path: &str) {
        use object_store::ObjectStoreExt;
        let err = store
            .get(&object_store::path::Path::from(path))
            .await
            .unwrap_err();
        assert!(matches!(err, object_store::Error::NotFound { .. }), "{err}");
    }

    /// The Azure store authenticates with the vended SAS only: the SAS is in the query string and
    /// no Authorization header (shared key / bearer) is sent.
    #[tokio::test]
    async fn azure_store_uses_only_vended_sas() {
        let (endpoint, server) = capture_one_request().await;
        let overrides = StorageOverrides::new(pairs(&[
            ("azure_storage_endpoint", &endpoint),
            ("allow_http", "true"),
        ]))
        .unwrap();
        let store = build_store(
            &Url::parse("abfss://c@acct.dfs.core.windows.net/t/").unwrap(),
            &overrides,
            &pairs(&[("azure_storage_sas_token", "sv=2024-01-01&sig=VENDEDSIG")]),
        )
        .unwrap();
        get_not_found(store.as_ref(), "t/_delta_log/_last_checkpoint").await;
        let request = server.await.unwrap();
        let first = request.lines().next().unwrap();
        assert!(first.starts_with("GET /"), "{request}");
        assert!(first.contains("t/_delta_log/_last_checkpoint"), "{request}");
        assert!(first.contains("sig=VENDEDSIG"), "{request}");
        assert!(
            !request.to_ascii_lowercase().contains("\r\nauthorization:"),
            "{request}"
        );
    }

    /// The GCS store authenticates with the vended OAuth token only.
    #[tokio::test]
    async fn gcs_store_uses_only_vended_bearer_token() {
        let (endpoint, server) = capture_one_request().await;
        let overrides = StorageOverrides::new(pairs(&[
            ("google_base_url", &endpoint),
            ("allow_http", "true"),
        ]))
        .unwrap();
        let store = build_store(
            &Url::parse("gs://bucket/t/").unwrap(),
            &overrides,
            &pairs(&[("google_bearer_token", "ya29.VENDED")]),
        )
        .unwrap();
        get_not_found(store.as_ref(), "t/_delta_log/_last_checkpoint").await;
        let request = server.await.unwrap();
        assert!(
            request
                .lines()
                .next()
                .unwrap()
                .starts_with("GET /bucket/t%2F%5Fdelta%5Flog%2F%5Flast%5Fcheckpoint "),
            "{request}"
        );
        let auth: Vec<&str> = request
            .lines()
            .filter(|l| l.to_ascii_lowercase().starts_with("authorization:"))
            .collect();
        assert_eq!(auth.len(), 1, "{request}");
        assert!(auth[0].ends_with("Bearer ya29.VENDED"), "{request}");
    }

    /// The S3 store built for a UC-vended credential sends requests to `--s3-endpoint`, path-style
    /// (`/{bucket}/{key}`), over plain HTTP, signed with the vended key, not to AWS.
    #[tokio::test]
    async fn s3_store_uses_endpoint_path_style() {
        let (endpoint, server) = capture_one_request().await;
        let overrides = StorageOverrides::new(pairs(&[
            ("aws_endpoint", &endpoint),
            ("aws_allow_http", "true"),
            ("aws_region", "us-east-1"),
        ]))
        .unwrap();
        let vended = pairs(&[
            ("aws_access_key_id", "VENDEDKEY"),
            ("aws_secret_access_key", "SK"),
            ("aws_session_token", "ST"),
        ]);
        let store = build_store(
            &Url::parse("s3://test/warehouse/t/").unwrap(),
            &overrides,
            &vended,
        )
        .unwrap();
        get_not_found(store.as_ref(), "warehouse/t/_delta_log/_last_checkpoint").await;
        let request = server.await.unwrap();
        let first = request.lines().next().unwrap();
        assert_eq!(
            first,
            "GET /test/warehouse/t/_delta_log/_last_checkpoint HTTP/1.1"
        );
        let lower = request.to_ascii_lowercase();
        assert!(
            lower.contains(&format!("host: {}", endpoint.trim_start_matches("http://"))),
            "{request}"
        );
        assert!(lower.contains("credential=vendedkey/"), "{request}");
        assert!(lower.contains("x-amz-security-token: st"), "{request}");
    }

    #[test]
    fn https_clients_do_not_need_a_system_trust_store() {
        // The bundled roots alone satisfy the TLS verifier; this is what lets the binary start in
        // a container without /etc/ssl/certs (see tests/local_io.rs for the env-var run).
        assert!(bundled_roots().count() > 100);
        build_uc_http_client().unwrap();
    }
}
