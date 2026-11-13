//! tonic implementation of `DeltaKernelService`.

use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_stream::{Stream, StreamExt as _};
use tonic::{Request, Response, Status, Streaming};

use crate::cache::Core;
use crate::errors::ServerError;
use crate::executor::{Outbound, StreamPort};
use crate::proto;
use crate::proto::delta_kernel_service_server::{DeltaKernelService, DeltaKernelServiceServer};

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

/// A client message of any of the three op-carrying RPCs, split into its parts.
enum Frame<R> {
    Request(R),
    OpResult(proto::OpResult),
    Other,
}

/// The request-or-op-result oneof every op-carrying RPC's client message has (SPEC v10.1).
trait ClientFrame: Send + 'static {
    type Request: Send + 'static;
    const NAME: &'static str;
    fn split(self) -> Frame<Self::Request>;
    fn client(req: &Self::Request) -> Option<&proto::ClientInfo>;
}

macro_rules! client_frame {
    ($msg:ty, $oneof:ident, $req:ty, $name:literal) => {
        impl ClientFrame for $msg {
            type Request = $req;
            const NAME: &'static str = $name;
            fn split(self) -> Frame<$req> {
                match self.msg {
                    Some(proto::$oneof::Msg::Request(r)) => Frame::Request(r),
                    Some(proto::$oneof::Msg::OpResult(r)) => Frame::OpResult(r),
                    None => Frame::Other,
                }
            }
            fn client(req: &$req) -> Option<&proto::ClientInfo> {
                req.client.as_ref()
            }
        }
    };
}

client_frame!(
    proto::SnapshotClientMsg,
    snapshot_client_msg,
    proto::GetSnapshotRequest,
    "GetSnapshotRequest"
);
client_frame!(
    proto::ScanPlanClientMsg,
    scan_plan_client_msg,
    proto::GetScanPlanRequest,
    "GetScanPlanRequest"
);
client_frame!(
    proto::TimeTravelClientMsg,
    time_travel_client_msg,
    proto::ResolveTimeTravelRequest,
    "ResolveTimeTravelRequest"
);

fn unexpected(what: &str) -> proto::Error {
    ServerError::internal(format!("internal: unexpected {what} message for this RPC")).to_proto()
}

fn snapshot_msg(o: Outbound) -> proto::SnapshotServerMsg {
    use proto::snapshot_server_msg::Msg;
    let msg = match o {
        Outbound::Op(op) => Msg::Op(op),
        Outbound::Snapshot(r) => Msg::Response(*r),
        Outbound::Error(e) => Msg::Error(e),
        Outbound::ScanPlan(_) | Outbound::TimeTravel(_) => Msg::Error(unexpected("response")),
    };
    proto::SnapshotServerMsg { msg: Some(msg) }
}

fn scan_plan_msg(o: Outbound) -> proto::ScanPlanServerMsg {
    use proto::scan_plan_server_msg::Msg;
    let msg = match o {
        Outbound::Op(op) => Msg::Op(op),
        Outbound::ScanPlan(r) => Msg::Response(*r),
        Outbound::Error(e) => Msg::Error(e),
        Outbound::Snapshot(_) | Outbound::TimeTravel(_) => Msg::Error(unexpected("response")),
    };
    proto::ScanPlanServerMsg { msg: Some(msg) }
}

fn time_travel_msg(o: Outbound) -> proto::TimeTravelServerMsg {
    use proto::time_travel_server_msg::Msg;
    let msg = match o {
        Outbound::Op(op) => Msg::Op(op),
        Outbound::TimeTravel(r) => Msg::Response(*r),
        Outbound::Error(e) => Msg::Error(e),
        Outbound::Snapshot(_) | Outbound::ScanPlan(_) => Msg::Error(unexpected("response")),
    };
    proto::TimeTravelServerMsg { msg: Some(msg) }
}

type Handler<R> =
    fn(Arc<Core>, R, Arc<StreamPort>) -> BoxFuture<'static, Result<Outbound, ServerError>>;
type OutStream<M> = Pin<Box<dyn Stream<Item = Result<M, Status>> + Send>>;

/// Drives one op-carrying stream: reads the request, validates `ClientInfo`, routes `OpResult`s to
/// blocked ops, runs `handler`, and sends its response (or error) as the last message.
// tonic's stream item is `Result<_, Status>`; `Status` is large by design.
#[allow(clippy::result_large_err)]
fn drive<C: ClientFrame, M: Send + 'static>(
    core: Arc<Core>,
    mut inbound: Streaming<C>,
    handler: Handler<C::Request>,
    map: fn(Outbound) -> M,
) -> OutStream<M> {
    let (tx, rx) = mpsc::channel::<Result<Outbound, Status>>(64);
    tokio::spawn(async move {
        let req = match inbound.message().await {
            Ok(Some(m)) => match m.split() {
                Frame::Request(r) => r,
                _ => {
                    let _ = tx
                        .send(Err(Status::invalid_argument(format!(
                            "first message must be a {}",
                            C::NAME
                        ))))
                        .await;
                    return;
                }
            },
            Ok(None) => return,
            Err(status) => {
                tracing::debug!(%status, "stream ended before request");
                return;
            }
        };
        let deadline = match Core::check_client(C::client(&req)) {
            Ok(d) => d,
            Err(e) => {
                let _ = tx.send(Ok(Outbound::Error(e.to_proto()))).await;
                return;
            }
        };
        // The port owns the only sender: the response stream ends when the port closes.
        let port = StreamPort::new(tx, deadline);
        let reader_port = port.clone();
        let reader = tokio::spawn(async move {
            while let Ok(Some(m)) = inbound.message().await {
                match m.split() {
                    Frame::OpResult(r) => reader_port.deliver(r),
                    _ => tracing::warn!(port = reader_port.id(), "ignoring unexpected message"),
                }
            }
            reader_port.close();
        });
        let msg = match handler(core.clone(), req, port.clone()).await {
            Ok(msg) => msg,
            Err(e) => {
                tracing::info!(error = %e, rpc = C::NAME, "request failed");
                Outbound::Error(e.to_proto())
            }
        };
        let delay = core.faults.take_response_delay_ms();
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay as u64)).await;
        }
        port.finish(msg).await;
        reader.abort();
    });
    Box::pin(ReceiverStream::new(rx).map(move |r| r.map(map)))
}

#[tonic::async_trait]
impl DeltaKernelService for KernelService {
    type GetSnapshotStream = OutStream<proto::SnapshotServerMsg>;
    type GetScanPlanStream = OutStream<proto::ScanPlanServerMsg>;
    type ResolveTimeTravelStream = OutStream<proto::TimeTravelServerMsg>;

    async fn get_snapshot(
        &self,
        request: Request<Streaming<proto::SnapshotClientMsg>>,
    ) -> Result<Response<Self::GetSnapshotStream>, Status> {
        Ok(Response::new(drive(
            self.core.clone(),
            request.into_inner(),
            |core, req, port| {
                Box::pin(async move {
                    core.handle(req, &port)
                        .await
                        .map(|r| Outbound::Snapshot(Box::new(r)))
                })
            },
            snapshot_msg,
        )))
    }

    async fn get_scan_plan(
        &self,
        request: Request<Streaming<proto::ScanPlanClientMsg>>,
    ) -> Result<Response<Self::GetScanPlanStream>, Status> {
        Ok(Response::new(drive(
            self.core.clone(),
            request.into_inner(),
            |core, req, port| {
                Box::pin(async move {
                    core.handle_scan_plan(req, &port)
                        .await
                        .map(|r| Outbound::ScanPlan(Box::new(r)))
                })
            },
            scan_plan_msg,
        )))
    }

    async fn resolve_time_travel(
        &self,
        request: Request<Streaming<proto::TimeTravelClientMsg>>,
    ) -> Result<Response<Self::ResolveTimeTravelStream>, Status> {
        Ok(Response::new(drive(
            self.core.clone(),
            request.into_inner(),
            |core, req, port| {
                Box::pin(async move {
                    core.handle_time_travel(req, &port)
                        .await
                        .map(|r| Outbound::TimeTravel(Box::new(r)))
                })
            },
            time_travel_msg,
        )))
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
        let l1 = self.core.l1.stats();
        Ok(Response::new(proto::DebugStats {
            builds_started: c.builds_started.load(Ordering::Relaxed),
            builds_coalesced: c.builds_coalesced.load(Ordering::Relaxed),
            ops_issued_total: c.ops_issued_total.load(Ordering::Relaxed),
            cached_instances_per_table: self.core.cached_instances_per_table(),
            ops_local_total: c.ops_local_total.load(Ordering::Relaxed),
            latest_renewals: c.latest_renewals.load(Ordering::Relaxed),
            l1_entries: l1.entries,
            l1_bytes: l1.bytes,
            l2_hits: c.l2_hits.load(Ordering::Relaxed),
            l2_misses: c.l2_misses.load(Ordering::Relaxed),
            entry_bytes: l1.entry_bytes,
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
