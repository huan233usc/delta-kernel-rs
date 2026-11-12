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

/// Builds the object store for a cloud table from UC-derived options.
pub fn build_store(
    location: &Url,
    options: &[(String, String)],
) -> Result<Arc<DynObjectStore>, String> {
    let with_timeout = options.iter().cloned().chain(std::iter::once((
        "timeout".to_string(),
        LOCAL_IO_TIMEOUT.to_string(),
    )));
    let (store, _) = object_store::parse_url_opts(location, with_timeout)
        .map_err(|e| format!("object store for {}: {e}", location.scheme()))?;
    Ok(Arc::new(store))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

pub struct CredentialProvider {
    http: reqwest::Client,
    /// Replaces `https://{workspace_host}` as the UC base URL (tests, proxies).
    endpoint_override: Option<String>,
    /// Treat `file://` tables like cloud tables (test hook: exercises the UC path locally).
    credentials_for_file: bool,
    cache: Mutex<HashMap<(String, String), Arc<StorageCredential>>>,
    /// UC calls made (tests).
    pub fetches: AtomicU64,
}

impl CredentialProvider {
    pub fn new(endpoint_override: Option<String>, credentials_for_file: bool) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("build UC http client"),
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
            let store = build_store(location, &options).map_err(ServerError::internal)?;
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
        let unavailable = |what: String| ServerError {
            code: Code::Internal,
            message: format!("UC unavailable: {what}"),
            retryable: true,
        };
        let resp = self
            .http
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
        let uc_message = parsed
            .as_ref()
            .map(|v| {
                format!(
                    "{} {}",
                    v.get("error_code").and_then(Value::as_str).unwrap_or(""),
                    v.get("message").and_then(Value::as_str).unwrap_or("")
                )
            })
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
        build_store(&Url::parse("s3://bucket/t/").unwrap(), &o).unwrap();
        assert!(storage_options("s3", &json!({})).is_err());
    }

    #[test]
    fn azure_mapping_builds_abfss_store() {
        let resp = json!({"azure_user_delegation_sas": {"sas_token": "sv=2024&sig=x"}});
        let o = storage_options("abfss", &resp).unwrap();
        assert_eq!(opts(o.clone())["azure_storage_sas_token"], "sv=2024&sig=x");
        build_store(
            &Url::parse("abfss://c@acct.dfs.core.windows.net/t/").unwrap(),
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
        build_store(&Url::parse("gs://bucket/t/").unwrap(), &o).unwrap();
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
}
