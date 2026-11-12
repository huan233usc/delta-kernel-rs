//! [`RemotePlanExecutor`]: a kernel [`PlanExecutor`] that ships every [`Operation`] to a client
//! stream and blocks (on a `spawn_blocking` thread) for the matching `OpResult`.
//!
//! A kernel job (a snapshot build or a plan generation) owns one [`OpChannel`]. The channel holds
//! the candidate client streams that are waiting for that job's result, in join order. The first
//! live candidate drives the job. If it closes, the in-flight op is re-sent to the next candidate,
//! so the job continues instead of restarting (SPEC §7 "请求合并"). When no candidate is left, or
//! the driver does not answer within its op deadline, the executor returns
//! [`KernelError::Cancelled`], which kernel propagates instead of treating as "no CRC".

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, Ordering};
use std::sync::mpsc::{RecvTimeoutError, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use delta_kernel::arrow::ipc::reader::StreamReader;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::plans::proto::schema as proto_schema;
use delta_kernel::schema::StructType;
use delta_kernel::{
    EngineData, FileMeta, IoOperation, KernelError, KernelResult, Operation, ParquetFooter,
    PlanExecutor, PlanResult,
};
use prost::Message as _;
use tokio::sync::mpsc;
use tonic::Status;
use url::Url;

use crate::errors::{CancelReason, codes, kernel_error_from_code};
use crate::proto::{self, op_result, server_msg};
use crate::stats::{Faults, ServerCounters};

/// Outbound half of a GetSnapshot stream.
pub type OutboundTx = mpsc::Sender<Result<proto::ServerMsg, Status>>;

static NEXT_OP_ID: AtomicI64 = AtomicI64::new(1);
static NEXT_PORT_ID: AtomicU64 = AtomicU64::new(1);

/// Server-side endpoint of one client GetSnapshot stream that can carry ops.
pub struct StreamPort {
    id: u64,
    out: Mutex<Option<OutboundTx>>,
    state: Mutex<PortState>,
    op_deadline: Duration,
    ops_issued: AtomicI32,
    client_op_time_us: AtomicI64,
}

struct PortState {
    alive: bool,
    pending: HashMap<i64, SyncSender<proto::OpResult>>,
}

enum CallError {
    StreamGone,
    Deadline,
}

impl StreamPort {
    pub fn new(out: OutboundTx, op_deadline: Duration) -> Arc<Self> {
        Arc::new(Self {
            id: NEXT_PORT_ID.fetch_add(1, Ordering::Relaxed),
            out: Mutex::new(Some(out)),
            state: Mutex::new(PortState {
                alive: true,
                pending: HashMap::new(),
            }),
            op_deadline,
            ops_issued: AtomicI32::new(0),
            client_op_time_us: AtomicI64::new(0),
        })
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn is_alive(&self) -> bool {
        self.state.lock().unwrap().alive
    }

    pub fn ops_issued(&self) -> i32 {
        self.ops_issued.load(Ordering::Relaxed)
    }

    pub fn client_op_time_us(&self) -> i64 {
        self.client_op_time_us.load(Ordering::Relaxed)
    }

    /// Routes an `OpResult` from the client to the blocked op, if it is still waiting.
    pub fn deliver(&self, result: proto::OpResult) {
        let tx = self.state.lock().unwrap().pending.remove(&result.op_id);
        match tx {
            Some(tx) => {
                let _ = tx.try_send(result);
            }
            None => tracing::warn!(port = self.id, op_id = result.op_id, "unexpected OpResult"),
        }
    }

    /// Marks the stream dead: every op blocked on it wakes up with `StreamGone`, and the outbound
    /// half is dropped so the client sees end-of-stream.
    pub fn close(&self) {
        {
            let mut st = self.state.lock().unwrap();
            st.alive = false;
            st.pending.clear();
        }
        self.out.lock().unwrap().take();
    }

    /// Terminates the stream with an error status (fault injection).
    fn kill(&self, why: &str) {
        let out = self.out.lock().unwrap().clone();
        if let Some(out) = out {
            let _ = out.try_send(Err(Status::aborted(format!("injected fault: {why}"))));
        }
        self.close();
    }

    /// Sends a terminal message (response or error) and closes the stream.
    pub async fn finish(&self, msg: server_msg::Msg) {
        let out = self.out.lock().unwrap().clone();
        if let Some(out) = out {
            let _ = out.send(Ok(proto::ServerMsg { msg: Some(msg) })).await;
        }
        self.close();
    }

    /// Sends one op and blocks for its result. Must run on a blocking thread.
    fn call(
        &self,
        op_id: i64,
        operation: &[u8],
        listing_hint: Option<proto::ListingHint>,
        faults: &Faults,
    ) -> Result<proto::OpResult, CallError> {
        let (tx, rx) = sync_channel(1);
        {
            let mut st = self.state.lock().unwrap();
            if !st.alive {
                return Err(CallError::StreamGone);
            }
            st.pending.insert(op_id, tx);
        }
        let out = self.out.lock().unwrap().clone();
        let Some(out) = out else {
            return Err(CallError::StreamGone);
        };
        let ops_before = self.ops_issued.load(Ordering::Relaxed);
        let fault = faults.take_stream_fault(ops_before);
        if fault == Some(false) {
            self.kill("fail_after_n_ops");
            return Err(CallError::StreamGone);
        }
        let msg = proto::ServerMsg {
            msg: Some(server_msg::Msg::Op(proto::OpRequest {
                op_id,
                operation: operation.to_vec(),
                listing_hint,
            })),
        };
        if out.blocking_send(Ok(msg)).is_err() {
            self.close();
            return Err(CallError::StreamGone);
        }
        self.ops_issued.fetch_add(1, Ordering::Relaxed);
        if fault == Some(true) {
            self.kill("kill_stream_mid_op");
        }
        let started = Instant::now();
        let res = rx.recv_timeout(self.op_deadline);
        self.client_op_time_us
            .fetch_add(started.elapsed().as_micros() as i64, Ordering::Relaxed);
        match res {
            Ok(r) => Ok(r),
            Err(RecvTimeoutError::Disconnected) => Err(CallError::StreamGone),
            Err(RecvTimeoutError::Timeout) => {
                self.state.lock().unwrap().pending.remove(&op_id);
                Err(CallError::Deadline)
            }
        }
    }
}

/// The op channel of one kernel job: candidate streams plus the job's error bookkeeping.
pub struct OpChannel {
    candidates: Mutex<Vec<Arc<StreamPort>>>,
    cancel: Mutex<Option<CancelReason>>,
    tainted: AtomicBool,
    first_op_failure: Mutex<Option<proto::KernelError>>,
    /// Target version of a snapshot build, used to fill `ListingHint.end_version`.
    target_version: Option<i64>,
    counters: Arc<ServerCounters>,
    faults: Arc<Faults>,
}

impl OpChannel {
    pub fn new(
        driver: Arc<StreamPort>,
        target_version: Option<i64>,
        counters: Arc<ServerCounters>,
        faults: Arc<Faults>,
    ) -> Arc<Self> {
        Arc::new(Self {
            candidates: Mutex::new(vec![driver]),
            cancel: Mutex::new(None),
            tainted: AtomicBool::new(false),
            first_op_failure: Mutex::new(None),
            target_version,
            counters,
            faults,
        })
    }

    /// Adds a waiting stream that can take over the job if the current driver goes away.
    pub fn join(&self, port: Arc<StreamPort>) {
        self.candidates.lock().unwrap().push(port);
    }

    /// Removes a stream that no longer waits for this job.
    pub fn leave(&self, port: &StreamPort) {
        self.candidates
            .lock()
            .unwrap()
            .retain(|p| p.id() != port.id());
    }

    fn current_driver(&self) -> Option<Arc<StreamPort>> {
        let mut c = self.candidates.lock().unwrap();
        c.retain(|p| p.is_alive());
        c.first().cloned()
    }

    /// Whether a non-`FileNotFound` op error was observed; such results must not be cached.
    pub fn is_tainted(&self) -> bool {
        self.tainted.load(Ordering::Relaxed)
    }

    pub fn cancel_reason(&self) -> Option<CancelReason> {
        *self.cancel.lock().unwrap()
    }

    pub fn first_op_failure(&self) -> Option<proto::KernelError> {
        self.first_op_failure.lock().unwrap().clone()
    }

    fn set_cancel(&self, reason: CancelReason) {
        self.cancel.lock().unwrap().get_or_insert(reason);
    }

    /// Records a client-reported error. `FileNotFound` is an expected answer (e.g. a missing
    /// `_last_checkpoint`), so it neither taints the job nor counts as an op failure.
    fn observe_error(&self, err: &proto::KernelError) {
        if err.code == codes::FILE_NOT_FOUND {
            return;
        }
        self.tainted.store(true, Ordering::Relaxed);
        self.first_op_failure
            .lock()
            .unwrap()
            .get_or_insert_with(|| err.clone());
    }

    fn listing_hint(&self, url: &Url) -> Option<proto::ListingHint> {
        let in_log = url.path().contains("/_delta_log/");
        in_log.then_some(proto::ListingHint {
            end_version: self.target_version,
            stop_at_first_non_versioned: true,
        })
    }
}

/// Expected result shape of an op, captured before the op is moved into proto bytes.
enum OpShape {
    Files,
    Bytes,
    Footer,
    Data,
}

/// [`PlanExecutor`] that forwards ops to the job's driving client stream.
pub struct RemotePlanExecutor {
    channel: Arc<OpChannel>,
}

impl RemotePlanExecutor {
    pub fn new(channel: Arc<OpChannel>) -> Self {
        Self { channel }
    }

    fn convert(&self, shape: OpShape, result: proto::OpResult) -> KernelResult<PlanResult> {
        let ch = &self.channel;
        let Some(result) = result.result else {
            let err = proto::KernelError {
                code: codes::INTERNAL,
                message: "OpResult without a result".into(),
            };
            ch.observe_error(&err);
            return Err(kernel_error_from_code(err.code, err.message));
        };
        let shape_err = |what: &str| {
            let err = proto::KernelError {
                code: codes::INTERNAL,
                message: format!("OpResult shape mismatch: {what}"),
            };
            ch.observe_error(&err);
            Err(kernel_error_from_code(err.code, err.message))
        };
        match (shape, result) {
            (_, op_result::Result::Error(err)) => {
                ch.observe_error(&err);
                Err(kernel_error_from_code(err.code, err.message))
            }
            (OpShape::Files, op_result::Result::Items(list)) => {
                let items: Vec<KernelResult<FileMeta>> = list
                    .items
                    .into_iter()
                    .map(|item| match item.v {
                        Some(proto::item::V::File(f)) => file_meta_from_status(&f),
                        Some(proto::item::V::Error(err)) => {
                            ch.observe_error(&err);
                            Err(kernel_error_from_code(err.code, err.message))
                        }
                        _ => Err(KernelError::internal_error("expected a file item")),
                    })
                    .collect();
                Ok(PlanResult::FileMeta(Box::new(items.into_iter())))
            }
            (OpShape::Bytes, op_result::Result::Items(list)) => {
                let items: Vec<KernelResult<Bytes>> = list
                    .items
                    .into_iter()
                    .map(|item| match item.v {
                        Some(proto::item::V::Data(d)) => Ok(Bytes::from(d)),
                        Some(proto::item::V::Error(err)) => {
                            ch.observe_error(&err);
                            Err(kernel_error_from_code(err.code, err.message))
                        }
                        _ => Err(KernelError::internal_error("expected a data item")),
                    })
                    .collect();
                Ok(PlanResult::Bytes(Box::new(items.into_iter())))
            }
            (OpShape::Footer, op_result::Result::FooterSchema(bytes)) => {
                let proto = proto_schema::StructType::decode(bytes.as_slice())
                    .map_err(KernelError::generic_err)?;
                let schema = Arc::new(StructType::try_from(proto)?);
                Ok(PlanResult::ParquetFooter(ParquetFooter { schema }))
            }
            (OpShape::Data, op_result::Result::Arrow(arrow)) => {
                let mut items: Vec<KernelResult<Box<dyn EngineData>>> = Vec::new();
                if !arrow.ipc_stream.is_empty() {
                    let reader = StreamReader::try_new(Cursor::new(arrow.ipc_stream), None)?;
                    for batch in reader {
                        let batch = batch?;
                        items.push(Ok(Box::new(ArrowEngineData::new(batch))));
                    }
                }
                if let Some(err) = arrow.trailing_error {
                    ch.observe_error(&err);
                    items.push(Err(kernel_error_from_code(err.code, err.message)));
                }
                Ok(PlanResult::Data(Box::new(items.into_iter())))
            }
            (_, op_result::Result::Items(_)) => shape_err("unexpected items"),
            (_, op_result::Result::Arrow(_)) => shape_err("unexpected arrow"),
            (_, op_result::Result::FooterSchema(_)) => shape_err("unexpected footer"),
        }
    }
}

/// Parses a client-reported file status into kernel [`FileMeta`].
pub fn file_meta_from_status(f: &proto::FileStatus) -> KernelResult<FileMeta> {
    Ok(FileMeta {
        location: Url::parse(&f.location)?,
        last_modified: f.last_modified,
        size: u64::try_from(f.size)
            .map_err(|_| KernelError::generic(format!("negative file size for {}", f.location)))?,
    })
}

impl PlanExecutor for RemotePlanExecutor {
    fn execute_op(&self, op: Operation) -> KernelResult<PlanResult> {
        let (shape, hint) = match &op {
            Operation::IoOperation(IoOperation::FileListing { url }) => {
                (OpShape::Files, self.channel.listing_hint(url))
            }
            Operation::IoOperation(IoOperation::HeadFile { .. }) => (OpShape::Files, None),
            Operation::IoOperation(IoOperation::ReadBytes { .. }) => (OpShape::Bytes, None),
            Operation::IoOperation(IoOperation::ParquetFooter { .. }) => (OpShape::Footer, None),
            Operation::QueryPlan(_) => (OpShape::Data, None),
            Operation::IoOperation(IoOperation::WriteBytes { .. })
            | Operation::IoOperation(IoOperation::AtomicCopy { .. }) => {
                return Err(KernelError::unsupported(
                    "delta-kernel-server is read-only: write ops are not forwarded",
                ));
            }
        };
        let bytes = op.to_proto_bytes();
        let op_id = NEXT_OP_ID.fetch_add(1, Ordering::Relaxed);
        loop {
            let Some(port) = self.channel.current_driver() else {
                self.channel.set_cancel(CancelReason::StreamsGone);
                return Err(KernelError::Cancelled);
            };
            self.channel
                .counters
                .ops_issued_total
                .fetch_add(1, Ordering::Relaxed);
            match port.call(op_id, &bytes, hint, &self.channel.faults) {
                Ok(result) => return self.convert(shape, result),
                Err(CallError::StreamGone) => {
                    tracing::info!(
                        port = port.id(),
                        op_id,
                        "driver stream gone; handing op over"
                    );
                    self.channel.leave(&port);
                }
                Err(CallError::Deadline) => {
                    self.channel.set_cancel(CancelReason::Deadline);
                    return Err(KernelError::Cancelled);
                }
            }
        }
    }
}
