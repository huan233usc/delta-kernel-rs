//! `delta-kernel-server` binary.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use delta_kernel_server::cache::{
    CacheConfig, Core, DEFAULT_L2_BUILD_DEADLINE, DEFAULT_LOCAL_IO_THREADS, IoMode, KERNEL_REV,
};
use delta_kernel_server::creds::StorageOverrides;
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
    #[arg(long, value_name = "URL")]
    l2_redis_url: Option<String>,
    /// TTL of L2 snapshot, version-index and plan keys.
    #[arg(long, default_value_t = DEFAULT_L2_TTL.as_secs())]
    l2_ttl_secs: u64,
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
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
    let l2: Option<Arc<L2>> = match &args.l2_redis_url {
        Some(url) => {
            let store = RedisStore::connect(url, KERNEL_REV, Duration::from_secs(args.l2_ttl_secs))
                .await
                .map_err(|e| format!("--l2-redis-url: {e}"))?;
            // The URL may carry a password: log the host part only.
            let host = url.rsplit('@').next().unwrap_or_default();
            tracing::info!(l2 = %host, "L2 snapshot store connected");
            Some(Arc::new(store))
        }
        None => None,
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
            l2_ttl: Duration::from_secs(args.l2_ttl_secs),
            l2_build_deadline: Duration::from_millis(args.l2_build_deadline_ms.max(1)),
        },
        l2,
    );
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
        l2 = args.l2_redis_url.is_some(),
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
        KernelService::new(core, args.enable_test_hooks),
        shutdown,
    )
    .await?;
    Ok(())
}
