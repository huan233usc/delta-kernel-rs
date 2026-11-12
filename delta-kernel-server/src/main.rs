//! `delta-kernel-server` binary.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use delta_kernel_server::cache::{CacheConfig, Core, DEFAULT_LOCAL_IO_THREADS, IoMode, KERNEL_REV};
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
