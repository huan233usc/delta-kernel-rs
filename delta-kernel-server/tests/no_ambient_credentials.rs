//! Stores built from a UC-vended credential reach no ambient credential source on any path:
//! not for reads, not for presigning. Its own test binary (single test) because it sets
//! process-wide environment variables that point every metadata / credential endpoint object_store
//! knows about at a local trap listener.

use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::signer::Signer;
use delta_kernel::object_store::{self, ObjectStoreExt};
use delta_kernel_server::creds::{CloudStore, StorageOverrides, build_cloud_store};
use url::Url;

/// An HTTP listener that counts connections and records request heads.
struct Listener {
    addr: String,
    hits: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<String>>>,
}

impl Listener {
    fn start(status: &'static str) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let hits = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let (h, r) = (hits.clone(), requests.clone());
        std::thread::spawn(move || {
            for sock in listener.incoming() {
                let Ok(mut sock) = sock else { continue };
                h.fetch_add(1, Ordering::SeqCst);
                let mut buf = vec![0u8; 16 * 1024];
                let mut n = 0;
                while !buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                    match sock.read(&mut buf[n..]) {
                        Ok(0) | Err(_) => break,
                        Ok(m) => n += m,
                    }
                }
                r.lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buf[..n]).to_string());
                let _ = sock.write_all(
                    format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                        .as_bytes(),
                );
            }
        });
        Self {
            addr,
            hits,
            requests,
        }
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn take_requests(&self) -> Vec<String> {
        std::mem::take(&mut self.requests.lock().unwrap())
    }
}

fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
    v.iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

async fn get_not_found(store: &dyn object_store::ObjectStore, path: &str) {
    let err = store.get(&Path::from(path)).await.unwrap_err();
    assert!(matches!(err, object_store::Error::NotFound { .. }), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vended_stores_never_reach_ambient_credential_sources() {
    let trap = Listener::start("500 Internal Server Error");
    let storage = Listener::start("404 Not Found");
    let missing = std::env::temp_dir().join("dks-no-such-credentials-file.json");
    let trap_url = trap.url();
    // SAFETY: set before any store is built; this binary runs a single test, so no other thread
    // reads the environment concurrently.
    unsafe {
        for (k, v) in [
            // AWS: static keys, web identity, ECS / EKS container credentials, IMDS.
            ("AWS_ACCESS_KEY_ID", "AMBIENTKEY"),
            ("AWS_SECRET_ACCESS_KEY", "AMBIENTSECRET"),
            ("AWS_SESSION_TOKEN", "AMBIENTTOKEN"),
            ("AWS_WEB_IDENTITY_TOKEN_FILE", missing.to_str().unwrap()),
            ("AWS_ROLE_ARN", "arn:aws:iam::000000000000:role/ambient"),
            ("AWS_CONTAINER_CREDENTIALS_FULL_URI", &trap_url),
            ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/creds"),
            ("AWS_EC2_METADATA_SERVICE_ENDPOINT", &trap_url),
            ("AWS_ENDPOINT_URL", &trap_url),
            // Azure: MSI / workload identity, account key, emulator.
            ("IDENTITY_ENDPOINT", &trap_url),
            ("MSI_ENDPOINT", &trap_url),
            ("AZURE_STORAGE_ACCOUNT_KEY", "QU1CSUVOVA=="),
            ("AZURE_CLIENT_ID", "ambient"),
            ("AZURE_FEDERATED_TOKEN_FILE", missing.to_str().unwrap()),
            ("AZURITE_BLOB_STORAGE_URL", &trap_url),
            // GCS: metadata server and application-default credentials.
            ("GCE_METADATA_HOST", &trap.addr),
            ("GCE_METADATA_ROOT", &trap.addr),
            ("GCE_METADATA_IP", &trap.addr),
            ("GOOGLE_APPLICATION_CREDENTIALS", missing.to_str().unwrap()),
            ("SERVICE_ACCOUNT", missing.to_str().unwrap()),
        ] {
            std::env::set_var(k, v);
        }
    }
    let path = "t/_delta_log/_last_checkpoint";
    let get = reqwest::Method::GET;
    let hour = Duration::from_secs(3600);

    // S3: reads and presigned URLs use only the vended key.
    let s3 = build_cloud_store(
        &Url::parse("s3://test/t/").unwrap(),
        &StorageOverrides::new(pairs(&[
            ("aws_endpoint", &storage.url()),
            ("aws_allow_http", "true"),
            ("aws_region", "us-east-1"),
        ]))
        .unwrap(),
        &pairs(&[
            ("aws_access_key_id", "VENDEDKEY"),
            ("aws_secret_access_key", "VENDEDSECRET"),
            ("aws_session_token", "VENDEDTOKEN"),
        ]),
    )
    .unwrap();
    let CloudStore::S3(s3) = s3 else { panic!() };
    let url = s3
        .signed_url(get.clone(), &Path::from(path), hour)
        .await
        .unwrap();
    let q = url.query().unwrap_or_default();
    assert!(q.contains("VENDEDKEY") && !q.contains("AMBIENT"), "{url}");
    assert!(q.contains("VENDEDTOKEN"), "{url}");
    get_not_found(&s3, path).await;
    let reqs = storage.take_requests();
    assert_eq!(reqs.len(), 1, "{reqs:?}");
    let lower = reqs[0].to_ascii_lowercase();
    assert!(lower.contains("credential=vendedkey/"), "{}", reqs[0]);
    assert!(!lower.contains("ambient"), "{}", reqs[0]);

    // Azure: reads use only the vended SAS; presigning a SAS credential fails locally.
    let az = build_cloud_store(
        &Url::parse("abfss://c@acct.dfs.core.windows.net/t/").unwrap(),
        &StorageOverrides::new(pairs(&[
            ("azure_storage_endpoint", &storage.url()),
            ("allow_http", "true"),
        ]))
        .unwrap(),
        &pairs(&[("azure_storage_sas_token", "sv=2024-01-01&sig=VENDEDSIG")]),
    )
    .unwrap();
    let CloudStore::Azure(az) = az else { panic!() };
    az.signed_url(get.clone(), &Path::from(path), hour)
        .await
        .unwrap_err();
    get_not_found(&az, path).await;
    let reqs = storage.take_requests();
    assert_eq!(reqs.len(), 1, "{reqs:?}");
    assert!(reqs[0].lines().next().unwrap().contains("sig=VENDEDSIG"));
    assert!(!reqs[0].to_ascii_lowercase().contains("\r\nauthorization:"));

    // GCS: reads use only the vended token; presigning fails without asking the metadata server.
    let gs = build_cloud_store(
        &Url::parse("gs://bucket/t/").unwrap(),
        &StorageOverrides::new(pairs(&[
            ("google_base_url", &storage.url()),
            ("allow_http", "true"),
        ]))
        .unwrap(),
        &pairs(&[("google_bearer_token", "ya29.VENDED")]),
    )
    .unwrap();
    let CloudStore::Gcs(gs) = gs else { panic!() };
    let err = gs
        .signed_url(get.clone(), &Path::from(path), hour)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not supported"), "{err}");
    get_not_found(&gs, path).await;
    let reqs = storage.take_requests();
    assert_eq!(reqs.len(), 1, "{reqs:?}");
    assert!(
        reqs[0].contains("\r\nauthorization: Bearer ya29.VENDED\r\n"),
        "{}",
        reqs[0]
    );

    // Give any stray background credential fetch a moment, then check the trap.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        trap.hits.load(Ordering::SeqCst),
        0,
        "an ambient credential endpoint was contacted: {:?}",
        trap.take_requests()
    );
}
