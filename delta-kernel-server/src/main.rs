//! `delta-kernel-server` binary.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use delta_kernel_server::cache::{
    CacheConfig, Core, DEFAULT_L2_BUILD_DEADLINE, DEFAULT_LOCAL_IO_THREADS, IoMode, KERNEL_REV,
    L2_RENEWAL_FLUSH_TIMEOUT,
};
use delta_kernel_server::creds::StorageOverrides;
use delta_kernel_server::file_store::FileStore;
use delta_kernel_server::service::{KernelService, bind, serve, write_endpoint_file};
use delta_kernel_server::store::{DEFAULT_L2_TTL, L2, RedisStore};

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Out-of-process delta_kernel snapshot / scan-plan server (POC)"
)]
struct Args {
    /// Address to listen on. Port 0 picks a free port (see --endpoint-file).
    #[arg(long, default_value = "127.0.0.1:50051")]
    listen: SocketAddr,
    /// After binding, atomically write the bound `host:port` to this file.
    #[arg(long)]
    endpoint_file: Option<PathBuf>,
    /// Identifier reported by Ping and in ServerStats. Defaults to `dks-<pid>-<nanos>`.
    #[arg(long)]
    instance_id: Option<String>,
    /// Enable the Ping / GetDebugStats / ResetCache / InjectFault test hooks.
    #[arg(long, default_value_t = false)]
    enable_test_hooks: bool,
    /// Max cached snapshot instances per table.
    #[arg(long, default_value_t = 2)]
    max_instances_per_table: usize,
    /// Global bound on estimated cache memory, in bytes (LRU eviction across tables).
    #[arg(long, default_value_t = 4 << 30)]
    max_cache_bytes: usize,
    /// Where FileListing / ReadBytes / HeadFile / ParquetFooter run: on the server (`local`) or
    /// on the client like every other op (`forward`). QueryPlan ops are always forwarded.
    #[arg(long, value_enum, default_value_t = IoMode::Local)]
    io_mode: IoMode,
    /// UC base URL used instead of `https://{auth.workspace_host}` for temporary credentials.
    #[arg(long)]
    uc_endpoint_override: Option<String>,
    /// Worker threads for server-side IO. Also the cap on in-flight server-side ops, including
    /// ones abandoned after their deadline; when all are busy, requests fail with UNAVAILABLE.
    #[arg(long, default_value_t = DEFAULT_LOCAL_IO_THREADS as u16, value_parser = clap::value_parser!(u16).range(1..))]
    local_io_threads: u16,
    /// Allowlisted object_store transport/addressing option for every cloud store, as KEY=VALUE
    /// (repeatable), e.g. `aws_endpoint=http://localstack:4566`, `aws_allow_http=true`,
    /// `aws_region=us-east-1`. Auth / credential-source keys are rejected; stores always use the
    /// UC-vended credential. TRUSTED OPERATOR ONLY: `allow_http`, `proxy_url` and
    /// `proxy_ca_certificate` can expose vended credentials on the wire (plain HTTP, or a proxy
    /// that can read or intercept traffic); use them only for trusted private endpoints/proxies.
    #[arg(long = "storage-option", value_name = "KEY=VALUE", value_parser = StorageOverrides::parse_pair)]
    storage_options: Vec<(String, String)>,
    /// S3 endpoint for every s3:// table (`aws_endpoint`). Requests are path-style unless
    /// `--storage-option aws_virtual_hosted_style_request=true`.
    #[arg(long, value_name = "URL")]
    s3_endpoint: Option<String>,
    /// Allow plain-HTTP S3 endpoints (`aws_allow_http=true`). Vended credentials (session token,
    /// signed requests) then travel unencrypted: only for trusted private endpoints (LocalStack).
    #[arg(long, default_value_t = false)]
    s3_allow_http: bool,
    /// S3 region (`aws_region`).
    #[arg(long, value_name = "REGION")]
    s3_region: Option<String>,
    /// Redis URL of the shared L2 snapshot store (SPEC v10.3), e.g. `redis://10.0.0.5:6379/0`.
    /// Off by default. L2 failures never fail a request; an unreachable URL fails startup.
    #[arg(long, value_name = "URL", conflicts_with = "l2_dir")]
    l2_redis_url: Option<String>,
    /// Directory of a file-backed L2 store (SPEC v12), shared by every server process pointed at
    /// it: `<dir>/<kernel_rev>/{snap,ver,latest,plan}/`, same keys and never-store rules as Redis.
    /// Build locks and authz entries are not persisted. Off by default; an uncreatable directory
    /// fails startup.
    #[arg(long, value_name = "DIR")]
    l2_dir: Option<PathBuf>,
    /// Before binding the listen port, rehydrate every snapshot, plan and latest entry under
    /// `<l2-dir>/<kernel_rev>/` into the in-process cache (from snapshot hints, zero IO). Logs the
    /// entry count and duration. Requests still go through authz first.
    #[arg(long, default_value_t = false, requires = "l2_dir")]
    l2_preload: bool,
    /// Read `--l2-dir` (preload and lookups on L1 misses) but never write under it: no entries,
    /// no temporary files, no directories, no locks (SPEC v12.1). New builds stay in L1.
    #[arg(long, default_value_t = false, requires = "l2_dir")]
    l2_read_only: bool,
    /// Cap on the snapshot instances `--l2-preload` rehydrates (each table's first choice before
    /// any table's second). Their plans and latest pointers come along.
    #[arg(long, value_name = "N", requires = "l2_preload")]
    l2_preload_max_entries: Option<usize>,
    /// TTL of L2 snapshot, version-index and plan entries. Default: 6 h on Redis; no expiry on
    /// the file store (`--l2-dir`), whose entries must survive restarts.
    #[arg(long, value_name = "SECS")]
    l2_ttl_secs: Option<u64>,
    /// How long a replica holds the L2 build lock, and how long other replicas wait for its
    /// result before building themselves.
    #[arg(long, default_value_t = DEFAULT_L2_BUILD_DEADLINE.as_millis() as u64)]
    l2_build_deadline_ms: u64,
    /// Test only (requires --enable-test-hooks): require UC credentials for file:// tables too.
    #[arg(
        long,
        hide = true,
        default_value_t = false,
        requires = "enable_test_hooks"
    )]
    test_uc_credentials_for_file: bool,
    /// Test only (requires --enable-test-hooks): sleep this long at the end of `--l2-preload`,
    /// before the port is bound.
    #[arg(long, hide = true, requires_all = ["enable_test_hooks", "l2_preload"])]
    test_preload_delay_ms: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        // Plain `key=value` fields in log files (e.g. the `L2 preload finished` line).
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .init();
    let args = Args::parse();
    // Convenience flags first: an explicit --storage-option for the same key wins.
    let mut storage_options: Vec<(String, String)> = Vec::new();
    if let Some(endpoint) = &args.s3_endpoint {
        storage_options.push(("aws_endpoint".into(), endpoint.clone()));
    }
    if args.s3_allow_http {
        storage_options.push(("aws_allow_http".into(), "true".into()));
    }
    if let Some(region) = &args.s3_region {
        storage_options.push(("aws_region".into(), region.clone()));
    }
    storage_options.extend(args.storage_options.iter().cloned());
    let storage_options = StorageOverrides::new(storage_options)?;
    if !storage_options.is_empty() {
        // Keys only: values may be secrets.
        tracing::info!(keys = ?storage_options.keys(), "server-side object_store options");
    }
    let instance_id = args.instance_id.unwrap_or_else(|| {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        format!("dks-{}-{nanos}", std::process::id())
    });
    let ttl = args.l2_ttl_secs.map(Duration::from_secs);
    let mut file_l2: Option<Arc<FileStore>> = None;
    let l2: Option<Arc<L2>> = match (&args.l2_redis_url, &args.l2_dir) {
        (Some(url), _) => {
            let store = RedisStore::connect(url, KERNEL_REV, ttl.unwrap_or(DEFAULT_L2_TTL))
                .await
                .map_err(|e| format!("--l2-redis-url: {e}"))?;
            // The URL may carry a password: log the host part only.
            let host = url.rsplit('@').next().unwrap_or_default();
            tracing::info!(l2 = %host, "L2 snapshot store connected");
            Some(Arc::new(store))
        }
        (None, Some(dir)) => {
            let store = if args.l2_read_only {
                FileStore::open_read_only(dir, KERNEL_REV, ttl)
            } else {
                FileStore::open(dir, KERNEL_REV, ttl)
            };
            let store = Arc::new(store.map_err(|e| format!("--l2-dir: {e}"))?);
            tracing::info!(
                dir = %store.root().display(),
                ttl_secs = ?args.l2_ttl_secs,
                read_only = args.l2_read_only,
                "L2 file store opened"
            );
            file_l2 = Some(store.clone());
            Some(store)
        }
        (None, None) => None,
    };
    let l2_kind = match (&args.l2_redis_url, &args.l2_dir) {
        (Some(_), _) => "redis",
        (None, Some(_)) => "file",
        (None, None) => "off",
    };
    let core = Core::with_l2(
        instance_id.clone(),
        CacheConfig {
            max_instances_per_table: args.max_instances_per_table,
            max_cache_bytes: args.max_cache_bytes,
            io_mode: args.io_mode,
            uc_endpoint_override: args.uc_endpoint_override.clone(),
            credentials_for_file: args.test_uc_credentials_for_file,
            local_io_threads: args.local_io_threads as usize,
            storage_options,
            l2_ttl: ttl.unwrap_or(DEFAULT_L2_TTL),
            l2_build_deadline: Duration::from_millis(args.l2_build_deadline_ms.max(1)),
        },
        l2,
    );
    // SPEC v12: the cache is warm before the port accepts connections. A failed preload leaves
    // the server cold (WARN) rather than down.
    if args.l2_preload {
        let store = file_l2
            .as_ref()
            .expect("clap: --l2-preload requires --l2-dir");
        match core.preload(store, args.l2_preload_max_entries).await {
            Ok(report) => {
                if let Some(ms) = args.test_preload_delay_ms {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                }
                if !report.other_revisions.is_empty() {
                    tracing::warn!(
                        other_kernel_revs = ?report.other_revisions,
                        "L2 dir has entries of other kernel revisions; ignored"
                    );
                }
                tracing::info!(
                    entries = report.entries(),
                    snapshots = report.snapshots,
                    latest = report.latest,
                    plans = report.plans,
                    rejected = report.rejected,
                    expired = report.expired,
                    skipped = report.skipped,
                    catalog_latest_in_l2 = report.catalog_latest_in_l2,
                    duration_ms = report.duration.as_secs_f64() * 1000.0,
                    "L2 preload finished"
                );
            }
            Err(e) => tracing::warn!(error = %e, "L2 preload failed; starting cold"),
        }
    }
    let (listener, addr) = bind(args.listen).await?;
    if let Some(path) = &args.endpoint_file {
        write_endpoint_file(path, addr)?;
    }
    tracing::info!(
        %addr,
        %instance_id,
        kernel_rev = KERNEL_REV,
        test_hooks = args.enable_test_hooks,
        io_mode = ?args.io_mode,
        l2 = l2_kind,
        "delta-kernel-server listening"
    );
    let shutdown = async {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
        tracing::info!("shutting down");
    };
    serve(
        listener,
        KernelService::new(core.clone(), args.enable_test_hooks),
        shutdown,
    )
    .await?;
    // Latest renewals still queued for the background L2 writer: best effort, bounded.
    core.flush_l2_renewals(L2_RENEWAL_FLUSH_TIMEOUT).await;
    Ok(())
}
