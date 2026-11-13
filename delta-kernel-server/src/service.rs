//! tonic implementation of `DeltaKernelService`.

use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status, Streaming};

use crate::cache::Core;
use crate::executor::StreamPort;
use crate::proto::delta_kernel_service_server::{DeltaKernelService, DeltaKernelServiceServer};
use crate::proto::{self, client_msg, server_msg};

/// Max gRPC message size in both directions (SPEC §6.3): CRC-branch plans inline every Add.
pub const MAX_MESSAGE_BYTES: usize = 256 << 20;

#[derive(Clone)]
pub struct KernelService {
    core: Arc<Core>,
    enable_test_hooks: bool,
}

impl KernelService {
    pub fn new(core: Arc<Core>, enable_test_hooks: bool) -> Self {
        Self {
            core,
            enable_test_hooks,
        }
    }

    pub fn into_server(self) -> DeltaKernelServiceServer<Self> {
        DeltaKernelServiceServer::new(self)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
    }

    /// `Some(status)` when test hooks are disabled.
    fn test_hooks_denied(&self) -> Option<Status> {
        (!self.enable_test_hooks).then(|| {
            Status::permission_denied(
                "test hooks are disabled; start the server with --enable-test-hooks",
            )
        })
    }
}

type ServerMsgStream = Pin<Box<dyn Stream<Item = Result<proto::ServerMsg, Status>> + Send>>;

#[tonic::async_trait]
impl DeltaKernelService for KernelService {
    type GetSnapshotStream = ServerMsgStream;

    async fn get_snapshot(
        &self,
        request: Request<Streaming<proto::ClientMsg>>,
    ) -> Result<Response<Self::GetSnapshotStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(64);
        let core = self.core.clone();
        tokio::spawn(async move {
            let req = match inbound.message().await {
                Ok(Some(proto::ClientMsg {
                    msg: Some(client_msg::Msg::Request(req)),
                })) => req,
                Ok(_) => {
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "first ClientMsg must be a GetSnapshotRequest",
                        )))
                        .await;
                    return;
                }
                Err(status) => {
                    tracing::debug!(%status, "stream ended before request");
                    return;
                }
            };
            let deadline = match Core::check_client(req.client.as_ref()) {
                Ok(d) => d,
                Err(e) => {
                    let _ = tx
                        .send(Ok(proto::ServerMsg {
                            msg: Some(server_msg::Msg::Error(e.to_proto())),
                        }))
                        .await;
                    return;
                }
            };
            // The port owns the only sender: the response stream ends when the port closes.
            let port = StreamPort::new(tx, deadline);
            let reader_port = port.clone();
            let reader = tokio::spawn(async move {
                loop {
                    match inbound.message().await {
                        Ok(Some(proto::ClientMsg {
                            msg: Some(client_msg::Msg::OpResult(r)),
                        })) => reader_port.deliver(r),
                        Ok(Some(_)) => {
                            tracing::warn!(port = reader_port.id(), "ignoring unexpected ClientMsg")
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
                reader_port.close();
            });
            let msg = match core.handle(req, &port).await {
                Ok(resp) => server_msg::Msg::Response(resp),
                Err(e) => {
                    tracing::info!(error = %e, "request failed");
                    server_msg::Msg::Error(e.to_proto())
                }
            };
            let delay = core.faults.take_response_delay_ms();
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay as u64)).await;
            }
            port.finish(msg).await;
            reader.abort();
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn ping(
        &self,
        _request: Request<proto::PingRequest>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        if let Some(denied) = self.test_hooks_denied() {
            return Err(denied);
        }
        Ok(Response::new(proto::PingResponse {
            instance_id: self.core.instance_id.clone(),
        }))
    }

    async fn get_debug_stats(
        &self,
        _request: Request<proto::DebugStatsRequest>,
    ) -> Result<Response<proto::DebugStats>, Status> {
        if let Some(denied) = self.test_hooks_denied() {
            return Err(denied);
        }
        let c = &self.core.counters;
        Ok(Response::new(proto::DebugStats {
            builds_started: c.builds_started.load(Ordering::Relaxed),
            builds_coalesced: c.builds_coalesced.load(Ordering::Relaxed),
            ops_issued_total: c.ops_issued_total.load(Ordering::Relaxed),
            cached_instances_per_table: self.core.cached_instances_per_table(),
            ops_local_total: c.ops_local_total.load(Ordering::Relaxed),
            latest_renewals: c.latest_renewals.load(Ordering::Relaxed),
        }))
    }

    async fn reset_cache(
        &self,
        _request: Request<proto::ResetCacheRequest>,
    ) -> Result<Response<proto::ResetCacheResponse>, Status> {
        if let Some(denied) = self.test_hooks_denied() {
            return Err(denied);
        }
        self.core.reset_cache();
        Ok(Response::new(proto::ResetCacheResponse {}))
    }

    async fn inject_fault(
        &self,
        request: Request<proto::FaultSpec>,
    ) -> Result<Response<proto::InjectFaultResponse>, Status> {
        if let Some(denied) = self.test_hooks_denied() {
            return Err(denied);
        }
        self.core.faults.arm(request.get_ref());
        Ok(Response::new(proto::InjectFaultResponse {}))
    }
}

/// Binds `addr` and returns the listener plus the bound address (resolves port 0).
pub async fn bind(addr: SocketAddr) -> std::io::Result<(TcpListener, SocketAddr)> {
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    Ok((listener, local))
}

/// Serves on an already-bound listener until `shutdown` resolves.
pub async fn serve(
    listener: TcpListener,
    service: KernelService,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(service.into_server())
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await
}

/// Writes `host:port` to `path` atomically (write a sibling temp file, then rename).
pub fn write_endpoint_file(path: &Path, addr: SocketAddr) -> std::io::Result<()> {
    let file_name = path
        .file_name()
        .ok_or_else(|| std::io::Error::other("endpoint file path has no file name"))?;
    let tmp = path.with_file_name(format!(
        ".{}.tmp.{}",
        file_name.to_string_lossy(),
        std::process::id()
    ));
    std::fs::write(&tmp, format!("{addr}\n"))?;
    std::fs::rename(&tmp, path)
}
