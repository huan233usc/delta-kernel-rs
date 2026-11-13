//! `delta-kernel-server` binary.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use delta_kernel_server::cache::{CacheConfig, Core, DEFAULT_LOCAL_IO_THREADS, IoMode, KERNEL_REV};
use delta_kernel_server::creds::StorageOverrides;
use delta_kernel_server::service::{KernelService, bind, serve, write_endpoint_file};

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
    let core = Core::new(
        instance_id.clone(),
        CacheConfig {
            max_instances_per_table: args.max_instances_per_table,
            max_cache_bytes: args.max_cache_bytes,
            io_mode: args.io_mode,
            uc_endpoint_override: args.uc_endpoint_override.clone(),
            credentials_for_file: args.test_uc_credentials_for_file,
            local_io_threads: args.local_io_threads as usize,
            storage_options,
        },
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
