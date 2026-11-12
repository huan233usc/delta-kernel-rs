//! [`RemotePlanExecutor`]: the kernel [`PlanExecutor`] of a server job.
//!
//! In `local` IO mode (SPEC v9) simple IO ops (`FileListing`, `ReadBytes`, `HeadFile`,
//! `ParquetFooter`) run on the server through [`LocalIo`]; `QueryPlan` ops are shipped to a client
//! stream, and the executor blocks (on a `spawn_blocking` thread) for the matching `OpResult`. In
//! `forward` mode every op is shipped. Both paths share the same error bookkeeping: per-item
//! errors, `FileNotFound` is benign, any other error taints the job.
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

use crate::errors::{CancelReason, codes, kernel_error_from_code, to_wire_kernel_error};
use crate::local_io::{LocalIo, apply_listing_hint};
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
    /// Ops executed on the server by jobs this stream started (SPEC v9 `ops_local`).
    ops_local: AtomicI32,
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
            ops_local: AtomicI32::new(0),
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

    pub fn ops_local(&self) -> i32 {
        self.ops_local.load(Ordering::Relaxed)
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
    /// The stream that started the job; local ops are attributed to it.
    starter: Arc<StreamPort>,
    candidates: Mutex<Vec<Arc<StreamPort>>>,
    cancel: Mutex<Option<CancelReason>>,
    tainted: AtomicBool,
    first_op_failure: Mutex<Option<proto::KernelError>>,
    /// Whether `first_op_failure` came from a server-side op (SPEC v9.1: UNAVAILABLE).
    first_failure_local: AtomicBool,
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
            starter: driver.clone(),
            candidates: Mutex::new(vec![driver]),
            cancel: Mutex::new(None),
            tainted: AtomicBool::new(false),
            first_op_failure: Mutex::new(None),
            first_failure_local: AtomicBool::new(false),
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

    /// Whether the first op failure was a server-side IO error (not a client-reported one).
    pub fn first_failure_is_local(&self) -> bool {
        self.first_failure_local.load(Ordering::Relaxed)
    }

    fn set_cancel(&self, reason: CancelReason) {
        self.cancel.lock().unwrap().get_or_insert(reason);
    }

    /// Records a client-reported error. `FileNotFound` is an expected answer (e.g. a missing
    /// `_last_checkpoint`), so it neither taints the job nor counts as an op failure.
    fn observe_error(&self, err: &proto::KernelError) {
        self.observe(err, false);
    }

    fn observe(&self, err: &proto::KernelError, local: bool) {
        if err.code == codes::FILE_NOT_FOUND {
            return;
        }
        self.tainted.store(true, Ordering::Relaxed);
        let mut first = self.first_op_failure.lock().unwrap();
        if first.is_none() {
            *first = Some(err.clone());
            self.first_failure_local.store(local, Ordering::Relaxed);
        }
    }

    fn listing_hint(&self, url: &Url) -> Option<proto::ListingHint> {
        listing_hint_for(url, self.target_version)
    }
}

/// Hint for listings of the `_delta_log` directory itself. Subdirectories such as
/// `_delta_log/_sidecars/` hold non-versioned names and get no hint.
pub fn listing_hint_for(url: &Url, target_version: Option<i64>) -> Option<proto::ListingHint> {
    let path = url.path();
    let dir = path.rsplit_once('/').map_or("", |(dir, _)| dir);
    dir.ends_with("/_delta_log").then_some(proto::ListingHint {
        end_version: target_version,
        stop_at_first_non_versioned: true,
    })
}

/// Raw result of a server-side op, before error bookkeeping.
enum LocalOut {
    List(KernelResult<Vec<KernelResult<FileMeta>>>),
    Read(KernelResult<Vec<KernelResult<Bytes>>>),
    Head(KernelResult<FileMeta>),
    Footer(KernelResult<ParquetFooter>),
}

/// Expected result shape of an op, captured before the op is moved into proto bytes.
enum OpShape {
    Files,
    Bytes,
    Footer,
    Data,
}

/// [`PlanExecutor`] of one job: local simple IO (when `local` is set) plus forwarded ops.
pub struct RemotePlanExecutor {
    channel: Arc<OpChannel>,
    local: Option<Arc<LocalIo>>,
}

impl RemotePlanExecutor {
    /// `local = None` forwards every op (`--io-mode forward`).
    pub fn new(channel: Arc<OpChannel>, local: Option<Arc<LocalIo>>) -> Self {
        Self { channel, local }
    }

    /// Records a server-side error with the same rules as a client-reported one.
    fn observe_local(&self, e: &KernelError) {
        self.channel.observe(&to_wire_kernel_error(e), true);
    }

    fn observe_items<T>(&self, items: &[KernelResult<T>]) {
        for e in items.iter().filter_map(|r| r.as_ref().err()) {
            if crate::errors::kernel_error_code(e) == codes::FILE_NOT_FOUND {
                self.channel
                    .counters
                    .local_not_found_items
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.observe_local(e);
        }
    }

    /// Executes a simple IO op on the server.
    fn execute_local(&self, local: &Arc<LocalIo>, io: IoOperation) -> KernelResult<PlanResult> {
        let ch = &self.channel;
        if matches!(
            io,
            IoOperation::WriteBytes { .. } | IoOperation::AtomicCopy { .. }
        ) {
            return Err(KernelError::unsupported(
                "delta-kernel-server is read-only: write ops are rejected",
            ));
        }
        ch.counters.ops_local_total.fetch_add(1, Ordering::Relaxed);
        ch.starter.ops_local.fetch_add(1, Ordering::Relaxed);
        let listing_url = match &io {
            IoOperation::FileListing { url } => Some(url.clone()),
            _ => None,
        };
        let raw = match self.run_local_bounded(local, io) {
            Some(raw) => raw,
            None => {
                // SPEC v9.1: a local op past the driver's op deadline fails the whole job with
                // DEADLINE (driver and every waiter), and the result is never cached.
                tracing::info!(
                    deadline_ms = ch.starter.op_deadline.as_millis() as u64,
                    "server-side IO op exceeded the op deadline"
                );
                ch.tainted.store(true, Ordering::Relaxed);
                ch.set_cancel(CancelReason::Deadline);
                return Err(KernelError::Cancelled);
            }
        };
        match raw {
            LocalOut::List(Ok(items)) => {
                let url = listing_url.expect("listing op");
                let items = match ch.listing_hint(&url) {
                    Some(hint) => {
                        let listed = items.len();
                        let kept = apply_listing_hint(&url, items, &hint);
                        ch.counters
                            .listing_items_dropped
                            .fetch_add((listed - kept.len()) as i64, Ordering::Relaxed);
                        kept
                    }
                    None => items,
                };
                self.observe_items(&items);
                Ok(PlanResult::FileMeta(Box::new(items.into_iter())))
            }
            LocalOut::Read(Ok(items)) => {
                self.observe_items(&items);
                Ok(PlanResult::Bytes(Box::new(items.into_iter())))
            }
            // Same shape as a forwarded HeadFile: one item, possibly an error.
            LocalOut::Head(item) => {
                if let Err(e) = &item {
                    self.observe_local(e);
                }
                Ok(PlanResult::FileMeta(Box::new(std::iter::once(item))))
            }
            LocalOut::Footer(Ok(footer)) => Ok(PlanResult::ParquetFooter(footer)),
            LocalOut::List(Err(e)) | LocalOut::Read(Err(e)) | LocalOut::Footer(Err(e)) => {
                self.observe_local(&e);
                Err(e)
            }
        }
    }

    /// Runs the raw local op on its own thread and waits at most the driver's op deadline.
    /// `None` on timeout; the abandoned op finishes in the background and its result is dropped.
    fn run_local_bounded(&self, local: &Arc<LocalIo>, io: IoOperation) -> Option<LocalOut> {
        let (tx, rx) = sync_channel(1);
        let local = local.clone();
        let delay = self.channel.faults.local_io_delay();
        let spawned = std::thread::Builder::new()
            .name("dks-local-io".into())
            .spawn(move || {
                if !delay.is_zero() {
                    std::thread::sleep(delay);
                }
                let out = match io {
                    IoOperation::FileListing { url } => LocalOut::List(local.list(&url)),
                    IoOperation::ReadBytes { files } => LocalOut::Read(local.read(files)),
                    IoOperation::HeadFile { url } => LocalOut::Head(local.head(&url)),
                    IoOperation::ParquetFooter { file } => LocalOut::Footer(local.footer(&file)),
                    IoOperation::WriteBytes { .. } | IoOperation::AtomicCopy { .. } => {
                        unreachable!("write ops are rejected before dispatch")
                    }
                };
                let _ = tx.send(out);
            });
        if let Err(e) = spawned {
            return Some(LocalOut::Footer(Err(KernelError::generic(format!(
                "spawn local IO thread: {e}"
            )))));
        }
        match rx.recv_timeout(self.channel.starter.op_deadline) {
            Ok(out) => Some(out),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => Some(LocalOut::Footer(Err(
                KernelError::internal_error("local IO thread panicked"),
            ))),
        }
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
        let op = match (&self.local, op) {
            (Some(local), Operation::IoOperation(io)) => return self.execute_local(local, io),
            (_, op) => op,
        };
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

#[cfg(test)]
mod tests {
    use url::Url;

    use super::listing_hint_for;

    #[test]
    fn listing_hint_only_for_delta_log_root() {
        let hint = |u: &str| listing_hint_for(&Url::parse(u).unwrap(), Some(7));
        let root = hint("file:///t/_delta_log/00000000000000000003").unwrap();
        assert_eq!(root.end_version, Some(7));
        assert!(root.stop_at_first_non_versioned);
        assert!(hint("file:///t/_delta_log/").is_some());
        assert!(hint("file:/t%20x/_delta_log/00000000000000000000").is_some());
        assert!(hint("file:///t/_delta_log/_sidecars/").is_none());
        assert!(hint("file:///t/_delta_log/_sidecars/00000000000000000006.checkpoint").is_none());
        assert!(hint("file:///t/_delta_log/_staged_commits/").is_none());
        assert!(hint("file:///t/part-0.parquet").is_none());
        assert!(hint("file:///t/x_delta_log/0001").is_none());
    }
}
