//! Loopback test support: an in-process server, a test client that answers ops with kernel's
//! in-process `SyncPlanExecutor`, fixtures, and plan-semantics comparison helpers.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use delta_kernel::arrow::ipc::writer::StreamWriter;
use delta_kernel::arrow::json::ArrayWriter;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::plans::PlanBasedEngine;
use delta_kernel::engine::sync::plan::SyncPlanExecutor;
use delta_kernel::plans::proto::{plan as proto_plan, schema as proto_schema};
use delta_kernel::scan::{PartitionValuesOptions, StatsOptions};
use delta_kernel::{IoOperation, KernelResult, Operation, PlanExecutor, Snapshot, SnapshotRef};
use delta_kernel_server::cache::{CacheConfig, Core, IoMode, KERNEL_FEATURES, KERNEL_REV};
use delta_kernel_server::decode::{decode_operation, decode_plan};
use delta_kernel_server::errors::{codes, to_wire_kernel_error};
use delta_kernel_server::normalize::normalize_plan_bytes;
use delta_kernel_server::proto::delta_kernel_service_client::DeltaKernelServiceClient;
use delta_kernel_server::proto::{
    self, item, op_result, scan_plan_result, snapshot_client_msg, snapshot_server_msg,
    stats_options,
};
use delta_kernel_server::service::{KernelService, MAX_MESSAGE_BYTES, bind, serve};
use prost::Message as _;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use url::Url;

pub type Client = DeltaKernelServiceClient<Channel>;

// ------------------------------------------------------------------------------------------------
// Server
// ------------------------------------------------------------------------------------------------

pub struct TestServer {
    pub addr: SocketAddr,
    pub core: Arc<Core>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl TestServer {
    pub async fn start() -> Self {
        Self::start_with(CacheConfig::default(), true).await
    }

    /// Server in the given IO mode (`start()` is `local`, the default).
    pub async fn start_mode(io_mode: IoMode) -> Self {
        Self::start_with(
            CacheConfig {
                io_mode,
                ..CacheConfig::default()
            },
            true,
        )
        .await
    }

    /// Waits until `builds_started` reaches `n` (a build is in flight).
    pub async fn wait_builds_started(&self, n: i64) {
        for _ in 0..500 {
            if self.stats().await.builds_started >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("build {n} never started");
    }

    /// Waits until `n` path-based latest re-listings have started (in-process counter).
    pub async fn wait_relists_started(&self, n: i64) {
        use std::sync::atomic::Ordering;
        for _ in 0..500 {
            if self
                .core
                .counters
                .latest_relists_started
                .load(Ordering::Relaxed)
                >= n
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("re-listing {n} never started");
    }

    pub async fn start_with(config: CacheConfig, test_hooks: bool) -> Self {
        Self::start_full("loopback-test", config, None, test_hooks).await
    }

    /// A server with an L2 tier; `instance_id` distinguishes replicas sharing it.
    pub async fn start_l2(instance_id: &str, l2: Arc<delta_kernel_server::store::L2>) -> Self {
        Self::start_full(instance_id, CacheConfig::default(), Some(l2), true).await
    }

    pub async fn start_full(
        instance_id: &str,
        config: CacheConfig,
        l2: Option<Arc<delta_kernel_server::store::L2>>,
        test_hooks: bool,
    ) -> Self {
        let core = Core::with_l2(instance_id.into(), config, l2);
        let (listener, addr) = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let (tx, rx) = oneshot::channel::<()>();
        let svc = KernelService::new(core.clone(), test_hooks);
        tokio::spawn(async move {
            serve(listener, svc, async {
                let _ = rx.await;
            })
            .await
            .unwrap();
        });
        Self {
            addr,
            core,
            shutdown: Some(tx),
        }
    }

    pub async fn client(&self) -> Client {
        connect(self.addr).await
    }

    pub async fn stats(&self) -> proto::DebugStats {
        self.client()
            .await
            .get_debug_stats(proto::DebugStatsRequest {})
            .await
            .unwrap()
            .into_inner()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

pub async fn connect(addr: SocketAddr) -> Client {
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    DeltaKernelServiceClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
}

// ------------------------------------------------------------------------------------------------
// Fixtures
// ------------------------------------------------------------------------------------------------

pub fn data_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../kernel/tests/data")
}

pub struct Fixture {
    pub _dir: tempfile::TempDir,
    pub root: PathBuf,
    pub url: Url,
    pub table_id: String,
}

impl Fixture {
    pub fn log_dir(&self) -> PathBuf {
        self.root.join("_delta_log")
    }
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).unwrap();
        }
    }
}

/// A private, mutable copy of `kernel/tests/data/<name>` (directory or `.tar.zst`). `name` may
/// contain a sub-path such as `partition_cm/name`.
pub fn fixture(name: &str) -> Fixture {
    fixture_at(name, "table")
}

/// Like [`fixture`], but a directory fixture is copied into `table_dir` (e.g. a name with spaces).
pub fn fixture_at(name: &str, table_dir: &str) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let src = data_dir().join(name);
    let root = if src.is_dir() {
        let root = dir.path().join(table_dir);
        copy_dir(&src, &root);
        root
    } else {
        let tarball = data_dir().join(format!("{name}.tar.zst"));
        let tar = zstd::Decoder::new(std::fs::File::open(&tarball).unwrap()).unwrap();
        tar::Archive::new(tar).unpack(dir.path()).unwrap();
        dir.path().join(name)
    };
    let root = root.canonicalize().unwrap();
    let url = Url::from_directory_path(&root).unwrap();
    Fixture {
        _dir: dir,
        root,
        url,
        table_id: format!("tbl-{}", name.replace('/', "-")),
    }
}

// ------------------------------------------------------------------------------------------------
// Requests
// ------------------------------------------------------------------------------------------------

pub fn client_info() -> proto::ClientInfo {
    proto::ClientInfo {
        dbr_version: "loopback-test".into(),
        kernel_rev: KERNEL_REV.into(),
        kernel_features: KERNEL_FEATURES.iter().map(|s| s.to_string()).collect(),
        op_deadline_ms: 30_000,
    }
}

pub fn spec_json() -> proto::ScanPlanSpec {
    proto::ScanPlanSpec {
        stats: Some(proto::StatsOptions {
            kind: stats_options::Kind::JsonOnly as i32,
            extra_indexed_columns: vec![],
        }),
        partition_values: proto::PartitionValuesMode::WithStruct as i32,
        timestamp_timezone: None,
    }
}

pub fn spec_struct() -> proto::ScanPlanSpec {
    proto::ScanPlanSpec {
        stats: Some(proto::StatsOptions {
            kind: stats_options::Kind::AllStructWithExtraIndexed as i32,
            extra_indexed_columns: vec![],
        }),
        partition_values: proto::PartitionValuesMode::WithStruct as i32,
        timestamp_timezone: None,
    }
}

pub fn default_specs() -> Vec<proto::ScanPlanSpec> {
    vec![spec_json(), spec_struct()]
}

/// What DBR's `RemoteKernelClient` does to every location before sending it: Hadoop
/// `Path.makeQualified(..).toString`. Probed against hadoop-client-api:
/// `file:///tmp/t%20x/a.json -> file:/tmp/t%20x/a.json` (escapes kept, authority dropped) and
/// `/tmp/t x -> file:/tmp/t x` for a raw table path.
pub fn hadoop_qualified(location: &str) -> String {
    let url = Url::parse(location).unwrap();
    assert_eq!(url.scheme(), "file");
    format!("file:{}", url.path())
}

/// The table location as DBR sends it: the user's raw path, Hadoop-qualified (no trailing slash,
/// spaces unescaped).
pub fn hadoop_table_location(fx: &Fixture) -> String {
    format!("file:{}", fx.root.display())
}

/// A segment in the shape the DBR client sends: every location Hadoop-qualified, and (when
/// `jnr_mtime` is set) last_modified truncated to whole seconds, as a coarser listing would.
pub fn client_form_segment(seg: &proto::LogSegmentInfo, jnr_mtime: bool) -> proto::LogSegmentInfo {
    let conv = |f: &proto::FileStatus| proto::FileStatus {
        location: hadoop_qualified(&f.location),
        size: f.size,
        last_modified: if jnr_mtime {
            f.last_modified / 1000 * 1000 + 1
        } else {
            f.last_modified
        },
    };
    proto::LogSegmentInfo {
        version: seg.version,
        checkpoint_version: seg.checkpoint_version,
        checkpoint_parts: seg.checkpoint_parts.iter().map(conv).collect(),
        deltas: seg.deltas.iter().map(conv).collect(),
        delta_is_staged: seg.delta_is_staged.clone(),
    }
}

/// Path-based latest request (SPEC v7).
pub fn latest_request(
    fx: &Fixture,
    max_staleness_ms: i64,
    plans: Vec<proto::ScanPlanSpec>,
) -> proto::GetSnapshotRequest {
    let mut req = request(fx, 0, plans);
    req.at = Some(proto::VersionSpec {
        spec: Some(proto::version_spec::Spec::Latest(proto::Latest {
            max_staleness_ms,
        })),
    });
    req
}

/// Exact-version request carrying `prev`'s log segment, as the P1-b client always does.
pub fn request_with_segment(
    fx: &Fixture,
    version: i64,
    plans: Vec<proto::ScanPlanSpec>,
    prev: &proto::GetSnapshotResponse,
) -> proto::GetSnapshotRequest {
    let mut req = request(fx, version, plans);
    req.expected_log_segment = prev.info.as_ref().unwrap().log_segment.clone();
    req
}

/// `catalog_latest` request with the given log tail files (paths relative to `_delta_log/`).
pub fn catalog_request(
    fx: &Fixture,
    max_catalog_version: i64,
    tail: &[&str],
    plans: Vec<proto::ScanPlanSpec>,
) -> proto::GetSnapshotRequest {
    let log_tail = tail
        .iter()
        .map(|rel| {
            let path = fx.log_dir().join(rel);
            let meta = std::fs::metadata(&path).unwrap();
            let mtime = meta
                .modified()
                .unwrap()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            proto::FileStatus {
                location: Url::from_file_path(&path).unwrap().to_string(),
                size: meta.len() as i64,
                last_modified: mtime,
            }
        })
        .collect();
    let mut req = request(fx, 0, plans);
    req.at = Some(proto::VersionSpec {
        spec: Some(proto::version_spec::Spec::CatalogLatest(
            proto::CatalogLatest {
                max_catalog_version,
                log_tail,
            },
        )),
    });
    req
}

pub fn request(
    fx: &Fixture,
    version: i64,
    plans: Vec<proto::ScanPlanSpec>,
) -> proto::GetSnapshotRequest {
    proto::GetSnapshotRequest {
        table: Some(proto::TableRef {
            table_id: fx.table_id.clone(),
            location: fx.url.to_string(),
        }),
        at: Some(proto::VersionSpec {
            spec: Some(proto::version_spec::Spec::Version(version)),
        }),
        prefetch_plans: plans,
        client: Some(client_info()),
        expected_log_segment: None,
        auth: None,
    }
}

// ------------------------------------------------------------------------------------------------
// Test client
// ------------------------------------------------------------------------------------------------

/// How the test client answers ops.
#[derive(Clone, Default)]
pub struct Behavior {
    /// Sleep before answering each op.
    pub op_delay: Duration,
    /// Drop the whole stream when op number `n + 1` arrives (after answering `n` ops).
    pub kill_after_ops: Option<usize>,
    /// Drop the whole stream when the first op of this kind (e.g. "FileListing") arrives.
    pub kill_on_kind: Option<&'static str>,
    /// Never answer any op (the stream stays open).
    pub never_answer: bool,
    /// Answer every op with a whole-op error of this code.
    pub fail_all_ops_code: Option<i32>,
    /// Answer `ReadBytes` slices whose URL ends with the suffix with a per-item error code.
    pub fail_read_suffix: Option<(String, i32)>,
    /// Notified when the first op arrives.
    pub first_op: Option<Arc<Notify>>,
}

/// One op as the client saw it.
#[derive(Clone, Debug)]
pub struct OpLog {
    pub op_id: i64,
    pub kind: &'static str,
    pub urls: Vec<String>,
    /// Raw `Operation` bytes as received.
    pub operation: Vec<u8>,
    pub listing_hint: Option<proto::ListingHint>,
    /// FileListing: locations returned after applying the hint.
    pub returned_files: Vec<String>,
    pub item_error_codes: Vec<i32>,
    pub op_error_code: Option<i32>,
}

#[derive(Debug)]
pub enum Ended {
    Error(proto::Error),
    /// The client dropped its stream; carries the op that was in flight at that moment.
    Killed(Box<proto::OpRequest>),
    EndOfStream,
    Status(tonic::Status),
}

pub struct Outcome {
    pub result: Result<proto::GetSnapshotResponse, Ended>,
    pub ops: Vec<OpLog>,
}

impl Outcome {
    pub fn ok(self) -> (proto::GetSnapshotResponse, Vec<OpLog>) {
        match self.result {
            Ok(r) => (r, self.ops),
            Err(e) => panic!("expected a response, got {e:?}"),
        }
    }

    pub fn err(self) -> proto::Error {
        match self.result {
            Err(Ended::Error(e)) => e,
            Ok(_) => panic!("expected an error, got a response"),
            Err(other) => panic!("expected a protocol error, got {other:?}"),
        }
    }
}

/// A server message of any of the three op-carrying RPCs.
pub enum ServerFrame<R> {
    Op(proto::OpRequest),
    Response(R),
    Error(proto::Error),
}

/// Answers ops on an open stream per `behavior` until the terminal message.
async fn drive_ops<CM: Send + 'static, SM: Send + 'static, R>(
    tx: mpsc::Sender<CM>,
    mut inbound: tonic::Streaming<SM>,
    split: impl Fn(SM) -> Option<ServerFrame<R>>,
    wrap: impl Fn(proto::OpResult) -> CM,
    behavior: Behavior,
) -> (Result<R, Ended>, Vec<OpLog>) {
    let mut ops = Vec::new();
    let mut seen = 0usize;
    let behavior = Arc::new(behavior);
    loop {
        let msg = match inbound.message().await {
            Ok(Some(m)) => match split(m) {
                Some(f) => f,
                None => continue,
            },
            Ok(None) => return (Err(Ended::EndOfStream), ops),
            Err(status) => return (Err(Ended::Status(status)), ops),
        };
        match msg {
            ServerFrame::Op(op) => {
                seen += 1;
                if seen == 1
                    && let Some(n) = &behavior.first_op
                {
                    n.notify_one();
                }
                let kill_on_kind = behavior
                    .kill_on_kind
                    .is_some_and(|k| op_kind(&op.operation) == k);
                if kill_on_kind || behavior.kill_after_ops.is_some_and(|k| seen > k) {
                    drop(tx);
                    drop(inbound);
                    return (Err(Ended::Killed(Box::new(op))), ops);
                }
                if behavior.never_answer {
                    continue;
                }
                if !behavior.op_delay.is_zero() {
                    tokio::time::sleep(behavior.op_delay).await;
                }
                let b = behavior.clone();
                let (result, log) = tokio::task::spawn_blocking(move || execute_op(&op, &b))
                    .await
                    .unwrap();
                ops.push(log);
                if tx.send(wrap(result)).await.is_err() {
                    return (Err(Ended::EndOfStream), ops);
                }
            }
            ServerFrame::Response(r) => return (Ok(r), ops),
            ServerFrame::Error(e) => return (Err(Ended::Error(e)), ops),
        }
    }
}

/// Runs one GetSnapshot stream to completion, answering ops per `behavior`.
pub async fn run(
    client: &mut Client,
    req: proto::GetSnapshotRequest,
    behavior: Behavior,
) -> Outcome {
    let (tx, rx) = mpsc::channel::<proto::SnapshotClientMsg>(16);
    tx.send(proto::SnapshotClientMsg {
        msg: Some(snapshot_client_msg::Msg::Request(req)),
    })
    .await
    .unwrap();
    let inbound = match client.get_snapshot(ReceiverStream::new(rx)).await {
        Ok(r) => r.into_inner(),
        Err(status) => {
            return Outcome {
                result: Err(Ended::Status(status)),
                ops: vec![],
            };
        }
    };
    let (result, ops) = drive_ops(
        tx,
        inbound,
        |m: proto::SnapshotServerMsg| {
            m.msg.map(|m| match m {
                snapshot_server_msg::Msg::Op(op) => ServerFrame::Op(op),
                snapshot_server_msg::Msg::Response(r) => ServerFrame::Response(r),
                snapshot_server_msg::Msg::Error(e) => ServerFrame::Error(e),
            })
        },
        |r| proto::SnapshotClientMsg {
            msg: Some(snapshot_client_msg::Msg::OpResult(r)),
        },
        behavior,
    )
    .await;
    Outcome { result, ops }
}

/// Result of a GetScanPlan or ResolveTimeTravel stream.
pub struct RpcOutcome<R> {
    pub result: Result<R, Ended>,
    pub ops: Vec<OpLog>,
}

impl<R> RpcOutcome<R> {
    pub fn ok(self) -> (R, Vec<OpLog>) {
        match self.result {
            Ok(r) => (r, self.ops),
            Err(e) => panic!("expected a response, got {e:?}"),
        }
    }

    pub fn err(self) -> proto::Error {
        match self.result {
            Err(Ended::Error(e)) => e,
            Ok(_) => panic!("expected an error, got a response"),
            Err(other) => panic!("expected a protocol error, got {other:?}"),
        }
    }
}

/// Runs one GetScanPlan stream to completion.
pub async fn run_scan_plan(
    client: &mut Client,
    req: proto::GetScanPlanRequest,
    behavior: Behavior,
) -> RpcOutcome<proto::GetScanPlanResponse> {
    use proto::{scan_plan_client_msg as c, scan_plan_server_msg as s};
    let (tx, rx) = mpsc::channel::<proto::ScanPlanClientMsg>(16);
    tx.send(proto::ScanPlanClientMsg {
        msg: Some(c::Msg::Request(req)),
    })
    .await
    .unwrap();
    let inbound = match client.get_scan_plan(ReceiverStream::new(rx)).await {
        Ok(r) => r.into_inner(),
        Err(status) => {
            return RpcOutcome {
                result: Err(Ended::Status(status)),
                ops: vec![],
            };
        }
    };
    let (result, ops) = drive_ops(
        tx,
        inbound,
        |m: proto::ScanPlanServerMsg| {
            m.msg.map(|m| match m {
                s::Msg::Op(op) => ServerFrame::Op(op),
                s::Msg::Response(r) => ServerFrame::Response(r),
                s::Msg::Error(e) => ServerFrame::Error(e),
            })
        },
        |r| proto::ScanPlanClientMsg {
            msg: Some(c::Msg::OpResult(r)),
        },
        behavior,
    )
    .await;
    RpcOutcome { result, ops }
}

/// Runs one ResolveTimeTravel stream to completion.
pub async fn run_time_travel(
    client: &mut Client,
    req: proto::ResolveTimeTravelRequest,
    behavior: Behavior,
) -> RpcOutcome<proto::ResolveTimeTravelResponse> {
    use proto::{time_travel_client_msg as c, time_travel_server_msg as s};
    let (tx, rx) = mpsc::channel::<proto::TimeTravelClientMsg>(16);
    tx.send(proto::TimeTravelClientMsg {
        msg: Some(c::Msg::Request(req)),
    })
    .await
    .unwrap();
    let inbound = match client.resolve_time_travel(ReceiverStream::new(rx)).await {
        Ok(r) => r.into_inner(),
        Err(status) => {
            return RpcOutcome {
                result: Err(Ended::Status(status)),
                ops: vec![],
            };
        }
    };
    let (result, ops) = drive_ops(
        tx,
        inbound,
        |m: proto::TimeTravelServerMsg| {
            m.msg.map(|m| match m {
                s::Msg::Op(op) => ServerFrame::Op(op),
                s::Msg::Response(r) => ServerFrame::Response(r),
                s::Msg::Error(e) => ServerFrame::Error(e),
            })
        },
        |r| proto::TimeTravelClientMsg {
            msg: Some(c::Msg::OpResult(r)),
        },
        behavior,
    )
    .await;
    RpcOutcome { result, ops }
}

/// A GetScanPlan request for `spec` on the snapshot a GetSnapshot response names.
pub fn plan_request(
    resp: &proto::GetSnapshotResponse,
    spec: proto::ScanPlanSpec,
) -> proto::GetScanPlanRequest {
    proto::GetScanPlanRequest {
        r#ref: resp.r#ref.clone(),
        spec: Some(spec),
        client: Some(client_info()),
        auth: None,
    }
}

/// Kind of a forwarded op, by decoding its bytes.
pub fn op_kind(bytes: &[u8]) -> &'static str {
    match decode_operation(bytes).expect("decode forwarded operation") {
        Operation::IoOperation(IoOperation::FileListing { .. }) => "FileListing",
        Operation::IoOperation(IoOperation::HeadFile { .. }) => "HeadFile",
        Operation::IoOperation(IoOperation::ReadBytes { .. }) => "ReadBytes",
        Operation::IoOperation(IoOperation::ParquetFooter { .. }) => "ParquetFooter",
        Operation::QueryPlan(_) => "QueryPlan",
        Operation::IoOperation(_) => "Write",
    }
}

/// Delta log version encoded in a file name (`<20 digits>.<...>`), if any.
pub fn file_version(location: &str) -> Option<i64> {
    let name = location.rsplit('/').next()?;
    let (digits, rest) = (name.get(..20)?, name.get(20..)?);
    (digits.bytes().all(|b| b.is_ascii_digit()) && rest.starts_with('.'))
        .then(|| digits.parse().ok())
        .flatten()
}

/// Truncates a sorted listing per `ListingHint` (SPEC §5): stop at the first non-versioned name
/// if asked, and stop once versions pass `end_version`.
fn apply_listing_hint(
    files: Vec<delta_kernel::FileMeta>,
    hint: &proto::ListingHint,
) -> Vec<delta_kernel::FileMeta> {
    let mut out = Vec::new();
    for f in files {
        match file_version(f.location.as_str()) {
            None if hint.stop_at_first_non_versioned => break,
            Some(v) if hint.end_version.is_some_and(|end| v > end) => break,
            _ => out.push(f),
        }
    }
    out
}

fn whole_error(op_id: i64, code: i32, message: String) -> proto::OpResult {
    proto::OpResult {
        op_id,
        result: Some(op_result::Result::Error(proto::KernelError {
            code,
            message,
        })),
    }
}

fn items(op_id: i64, items: Vec<item::V>) -> proto::OpResult {
    proto::OpResult {
        op_id,
        result: Some(op_result::Result::Items(proto::ItemList {
            items: items
                .into_iter()
                .map(|v| proto::Item { v: Some(v) })
                .collect(),
        })),
    }
}

fn file_status(m: &delta_kernel::FileMeta) -> proto::FileStatus {
    proto::FileStatus {
        location: m.location.to_string(),
        size: m.size as i64,
        last_modified: m.last_modified,
    }
}

/// Executes one forwarded op with kernel's in-process `SyncPlanExecutor`, mirroring what the DBR
/// `OpDispatcher` does (SPEC §7 op table): per-item errors for listings and reads, the whole
/// Arrow result materialized into one IPC stream.
pub fn execute_op(op: &proto::OpRequest, b: &Behavior) -> (proto::OpResult, OpLog) {
    let exec = SyncPlanExecutor::default();
    let operation = decode_operation(&op.operation).expect("decode forwarded operation");
    let mut log = OpLog {
        op_id: op.op_id,
        kind: "",
        urls: vec![],
        operation: op.operation.clone(),
        listing_hint: op.listing_hint,
        returned_files: vec![],
        item_error_codes: vec![],
        op_error_code: None,
    };
    if let Some(code) = b.fail_all_ops_code {
        log.kind = "Injected";
        log.op_error_code = Some(code);
        return (
            whole_error(op.op_id, code, "injected op failure".into()),
            log,
        );
    }
    let result = match operation {
        Operation::IoOperation(IoOperation::FileListing { url }) => {
            log.kind = "FileListing";
            log.urls.push(url.to_string());
            match exec
                .execute_op(Operation::IoOperation(IoOperation::file_listing(url)))
                .and_then(|r| r.into_file_meta())
            {
                Ok(iter) => {
                    let mut files = Vec::new();
                    let mut errors = Vec::new();
                    for f in iter {
                        match f {
                            Ok(f) => files.push(f),
                            Err(e) => errors.push(to_wire_kernel_error(&e)),
                        }
                    }
                    files.sort_by(|a, b| a.location.as_str().cmp(b.location.as_str()));
                    if let Some(hint) = &op.listing_hint {
                        files = apply_listing_hint(files, hint);
                    }
                    log.returned_files = files.iter().map(|f| f.location.to_string()).collect();
                    let mut v: Vec<item::V> = files
                        .iter()
                        .map(|f| item::V::File(file_status(f)))
                        .collect();
                    log.item_error_codes = errors.iter().map(|e| e.code).collect();
                    v.extend(errors.into_iter().map(item::V::Error));
                    items(op.op_id, v)
                }
                Err(e) => {
                    let w = to_wire_kernel_error(&e);
                    log.op_error_code = Some(w.code);
                    whole_error(op.op_id, w.code, w.message)
                }
            }
        }
        Operation::IoOperation(IoOperation::HeadFile { url }) => {
            log.kind = "HeadFile";
            log.urls.push(url.to_string());
            let r = exec
                .execute_op(Operation::IoOperation(IoOperation::head_file(url)))
                .and_then(|r| r.into_file_meta())
                .and_then(|mut it| {
                    it.next().unwrap_or_else(|| {
                        Err(delta_kernel::KernelError::FileNotFound("head".into()))
                    })
                });
            match r {
                Ok(f) => items(op.op_id, vec![item::V::File(file_status(&f))]),
                Err(e) => {
                    let w = to_wire_kernel_error(&e);
                    log.item_error_codes.push(w.code);
                    items(op.op_id, vec![item::V::Error(w)])
                }
            }
        }
        Operation::IoOperation(IoOperation::ReadBytes { files }) => {
            log.kind = "ReadBytes";
            let mut v = Vec::new();
            for slice in files {
                log.urls.push(slice.0.to_string());
                if let Some((suffix, code)) = &b.fail_read_suffix
                    && slice.0.as_str().ends_with(suffix.as_str())
                {
                    log.item_error_codes.push(*code);
                    v.push(item::V::Error(proto::KernelError {
                        code: *code,
                        message: "injected transient read failure".into(),
                    }));
                    continue;
                }
                let r = exec
                    .execute_op(Operation::IoOperation(IoOperation::read_bytes(vec![slice])))
                    .and_then(|r| r.into_bytes())
                    .and_then(|mut it| {
                        it.next()
                            .unwrap_or_else(|| Err(delta_kernel::KernelError::generic("no bytes")))
                    });
                match r {
                    Ok(bytes) => v.push(item::V::Data(bytes.to_vec())),
                    Err(e) => {
                        let w = to_wire_kernel_error(&e);
                        log.item_error_codes.push(w.code);
                        v.push(item::V::Error(w));
                    }
                }
            }
            items(op.op_id, v)
        }
        Operation::IoOperation(IoOperation::ParquetFooter { file }) => {
            log.kind = "ParquetFooter";
            log.urls.push(file.location.to_string());
            match exec.read_parquet_footer(file) {
                Ok(footer) => proto::OpResult {
                    op_id: op.op_id,
                    result: Some(op_result::Result::FooterSchema(
                        proto_schema::StructType::from(footer.schema.as_ref()).encode_to_vec(),
                    )),
                },
                Err(e) => {
                    let w = to_wire_kernel_error(&e);
                    log.op_error_code = Some(w.code);
                    whole_error(op.op_id, w.code, w.message)
                }
            }
        }
        Operation::QueryPlan(plan) => {
            log.kind = "QueryPlan";
            match exec
                .execute_op(Operation::QueryPlan(plan))
                .and_then(|r| r.into_data())
            {
                Ok(iter) => {
                    let mut writer: Option<StreamWriter<Vec<u8>>> = None;
                    let mut trailing = None;
                    for data in iter {
                        match data.and_then(ArrowEngineData::try_from_engine_data) {
                            Ok(d) => {
                                let batch = d.record_batch();
                                let w = writer.get_or_insert_with(|| {
                                    StreamWriter::try_new(Vec::new(), &batch.schema()).unwrap()
                                });
                                w.write(batch).expect("ipc write");
                            }
                            Err(e) => {
                                trailing = Some(to_wire_kernel_error(&e));
                                break;
                            }
                        }
                    }
                    let ipc_stream = match writer {
                        Some(mut w) => {
                            w.finish().unwrap();
                            w.into_inner().unwrap()
                        }
                        None => Vec::new(),
                    };
                    if let Some(t) = &trailing {
                        log.item_error_codes.push(t.code);
                    }
                    proto::OpResult {
                        op_id: op.op_id,
                        result: Some(op_result::Result::Arrow(proto::ArrowResult {
                            ipc_stream,
                            trailing_error: trailing,
                        })),
                    }
                }
                Err(e) => {
                    let w = to_wire_kernel_error(&e);
                    log.op_error_code = Some(w.code);
                    whole_error(op.op_id, w.code, w.message)
                }
            }
        }
        Operation::IoOperation(_) => {
            log.kind = "Write";
            log.op_error_code = Some(codes::UNSUPPORTED);
            whole_error(op.op_id, codes::UNSUPPORTED, "read-only client".into())
        }
    };
    (result, log)
}

// ------------------------------------------------------------------------------------------------
// In-process reference and comparison
// ------------------------------------------------------------------------------------------------

pub fn local_engine() -> PlanBasedEngine {
    PlanBasedEngine::new(None, Arc::new(SyncPlanExecutor::default()))
}

/// The in-process catalog-managed build JNR performs (`kernel_snapshot_builder_set_log_tail` and
/// `..._with_max_catalog_version` on the FFI builder): same builder calls, default CRC replay.
pub fn local_catalog_snapshot(
    fx: &Fixture,
    req: &proto::GetSnapshotRequest,
) -> delta_kernel::Result<SnapshotRef> {
    let Some(proto::version_spec::Spec::CatalogLatest(c)) =
        req.at.as_ref().and_then(|a| a.spec.as_ref())
    else {
        panic!("not a catalog_latest request");
    };
    let log_tail = c
        .log_tail
        .iter()
        .map(|f| {
            delta_kernel_server::executor::file_meta_from_status(f)
                .and_then(delta_kernel::LogPath::try_new)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut builder = Snapshot::builder_for(fx.url.as_str());
    if !log_tail.is_empty() {
        builder = builder.with_log_tail(log_tail);
    }
    builder
        .with_max_catalog_version(c.max_catalog_version as u64)
        .build(&local_engine())
}

pub fn local_snapshot(fx: &Fixture, version: Option<i64>) -> SnapshotRef {
    let builder = Snapshot::builder_for(fx.url.as_str());
    let builder = match version {
        Some(v) => builder.at_version(v as u64),
        None => builder,
    };
    builder.build(&local_engine()).unwrap()
}

pub fn latest_version(fx: &Fixture) -> i64 {
    local_snapshot(fx, None).version() as i64
}

fn kernel_options(spec: &proto::ScanPlanSpec) -> (StatsOptions, PartitionValuesOptions) {
    delta_kernel_server::plan::scan_options(spec).unwrap()
}

/// The in-process plan for `spec` on `snapshot`, or `None` if statically empty.
pub fn local_plan(
    snapshot: &SnapshotRef,
    spec: &proto::ScanPlanSpec,
) -> KernelResult<Option<delta_kernel::plans::Plan>> {
    let (stats, partition) = kernel_options(spec);
    snapshot
        .clone()
        .scan_builder()
        .with_stats(stats)
        .with_partition_values(partition)
        .build()?
        .declarative_metadata_scan_plan(&local_engine())
}

pub fn execute_plan(plan: delta_kernel::plans::Plan) -> Vec<RecordBatch> {
    SyncPlanExecutor::default()
        .execute_op(Operation::QueryPlan(plan))
        .and_then(|r| r.into_data())
        .unwrap()
        .map(|d| {
            ArrowEngineData::try_from_engine_data(d.unwrap())
                .unwrap()
                .record_batch()
                .clone()
        })
        .collect()
}

/// Live add rows as JSON values, sorted by path.
pub fn rows_sorted_by_path(batches: &[RecordBatch]) -> Vec<serde_json::Value> {
    let mut rows = Vec::new();
    for batch in batches {
        let mut w = ArrayWriter::new(Vec::new());
        w.write(batch).unwrap();
        w.finish().unwrap();
        let bytes = w.into_inner();
        if bytes.is_empty() {
            continue;
        }
        let v: Vec<serde_json::Value> = serde_json::from_slice(&bytes).unwrap();
        rows.extend(v);
    }
    let path_of = |v: &serde_json::Value| -> String {
        v.get("path")
            .or_else(|| v.get("add").and_then(|a| a.get("path")))
            .and_then(|p| p.as_str())
            .unwrap_or_default()
            .to_string()
    };
    rows.sort_by_key(path_of);
    rows
}

/// What a plan result compares as: `Some(bytes)` for a plan, `None` for statically empty.
pub fn plan_bytes(r: &proto::ScanPlanResult) -> Option<&[u8]> {
    match r.result.as_ref().expect("plan result") {
        scan_plan_result::Result::Plan(b) => Some(b),
        scan_plan_result::Result::Empty(_) => None,
        scan_plan_result::Result::Unsupported(e) => panic!("unexpected unsupported plan: {e:?}"),
    }
}

/// SPEC §8 correctness: the remote plan executes to the same live add rows as the in-process plan
/// on the same snapshot, and both plans are equal after normalization. Also checks SnapshotInfo
/// against an in-process computation.
pub fn assert_parity(
    fx: &Fixture,
    resp: &proto::GetSnapshotResponse,
    specs: &[proto::ScanPlanSpec],
) -> usize {
    assert_parity_with(&local_snapshot(fx, Some(resp.version)), resp, specs)
}

/// [`assert_parity`] against a given in-process snapshot (e.g. a catalog-managed build).
pub fn assert_parity_with(
    local: &SnapshotRef,
    resp: &proto::GetSnapshotResponse,
    specs: &[proto::ScanPlanSpec],
) -> usize {
    assert_eq!(
        resp.version,
        local.version() as i64,
        "snapshot version differs"
    );
    let local = local.clone();
    let local_info =
        delta_kernel_server::info::compute_snapshot_info(&local, &local_engine()).unwrap();
    assert_eq!(
        resp.info.as_ref().unwrap(),
        &local_info,
        "SnapshotInfo differs from in-process computation"
    );
    assert_eq!(resp.prefetched_plans.len(), specs.len());
    let mut total_rows = 0;
    for (spec, remote) in specs.iter().zip(&resp.prefetched_plans) {
        let local_plan = local_plan(&local, spec).unwrap();
        match (plan_bytes(remote), local_plan) {
            (None, None) => {}
            (Some(bytes), Some(lp)) => {
                let local_bytes = proto_plan::Plan::from(&lp).encode_to_vec();
                assert_eq!(
                    normalize_plan_bytes(bytes).unwrap(),
                    normalize_plan_bytes(&local_bytes).unwrap(),
                    "normalized plan bytes differ"
                );
                let remote_rows = rows_sorted_by_path(&execute_plan(decode_plan(bytes).unwrap()));
                let local_rows = rows_sorted_by_path(&execute_plan(lp));
                assert_eq!(remote_rows, local_rows, "live add rows differ");
                total_rows += remote_rows.len();
            }
            (r, l) => panic!(
                "plan presence differs: remote={} local={}",
                r.is_some(),
                l.is_some()
            ),
        }
    }
    total_rows
}

pub fn resolved(resp: &proto::GetSnapshotResponse) -> proto::ResolveKind {
    proto::ResolveKind::try_from(resp.resolved_by).unwrap()
}

/// Ops forwarded to the client stream.
pub fn ops_issued(resp: &proto::GetSnapshotResponse) -> i32 {
    resp.stats.as_ref().unwrap().ops_issued
}

/// Ops the server executed itself (SPEC v9).
pub fn ops_local(resp: &proto::GetSnapshotResponse) -> i32 {
    resp.stats.as_ref().unwrap().ops_local
}

/// `(forwarded, local)` op counts of a response.
pub fn op_counts(resp: &proto::GetSnapshotResponse) -> (i32, i32) {
    (ops_issued(resp), ops_local(resp))
}

/// Makes a file unreadable until the guard drops (a real local IO error, not FileNotFound).
pub struct Unreadable(PathBuf, std::fs::Permissions);

impl Unreadable {
    pub fn new(path: PathBuf) -> Self {
        use std::os::unix::fs::PermissionsExt as _;
        let original = std::fs::metadata(&path).unwrap().permissions();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
        Self(path, original)
    }
}

impl Drop for Unreadable {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(&self.0, self.1.clone());
    }
}

/// Appends a commit with one add of a (nonexistent) data file; metadata plans never read it.
pub fn append_commit(fx: &Fixture, version: i64, file_name: &str, partition: Option<(&str, &str)>) {
    let pv = match partition {
        Some((k, v)) => format!("{{\"{k}\":\"{v}\"}}"),
        None => "{}".into(),
    };
    let commit = format!(
        "{{\"commitInfo\":{{\"timestamp\":{ts},\"operation\":\"WRITE\"}}}}\n\
         {{\"add\":{{\"path\":\"{file_name}\",\"partitionValues\":{pv},\"size\":1234,\
         \"modificationTime\":{ts},\"dataChange\":true,\
         \"stats\":\"{{\\\"numRecords\\\":1}}\"}}}}\n",
        ts = 1_700_000_000_000i64 + version,
    );
    std::fs::write(fx.log_dir().join(format!("{version:020}.json")), commit).unwrap();
}

/// Writes a checkpoint at `version` with kernel's own checkpoint writer (default engine), on a
/// plain thread outside the test's tokio runtime.
pub fn write_checkpoint(fx: &Fixture, version: i64) {
    let snapshot = local_snapshot(fx, Some(version));
    std::thread::spawn(move || {
        use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let store = Arc::new(delta_kernel::object_store::local::LocalFileSystem::new());
        let engine = delta_kernel_default_engine::DefaultEngineBuilder::new(store)
            .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(rt.handle().clone())))
            .build();
        snapshot.checkpoint(&engine, None).unwrap();
    })
    .join()
    .unwrap();
}

/// Shared op log for tests that inspect what several streams saw.
pub type SharedLog = Arc<Mutex<Vec<OpLog>>>;

// ------------------------------------------------------------------------------------------------
// Mock Unity Catalog (SPEC v9 credentials)
// ------------------------------------------------------------------------------------------------

/// How the mock UC answers a token.
#[derive(Clone, Debug)]
pub enum UcAnswer {
    /// 200 with a credential valid for `ttl` from now.
    Grant { ttl: Duration },
    /// HTTP status with a UC-style error body.
    Status(u16),
    /// HTTP status with this `message` in the UC error body (e.g. one echoing the token).
    StatusMessage(u16, String),
}

/// One request the mock UC received.
#[derive(Clone, Debug)]
pub struct UcCall {
    pub path: String,
    pub body: serde_json::Value,
    pub authorization: String,
}

/// Minimal HTTP/1.1 server standing in for UC's temporary-credentials endpoints.
pub struct MockUc {
    pub base_url: String,
    pub calls: Arc<Mutex<Vec<UcCall>>>,
    pub answers: Arc<Mutex<std::collections::HashMap<String, UcAnswer>>>,
}

impl MockUc {
    pub fn start() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let calls: Arc<Mutex<Vec<UcCall>>> = Arc::default();
        let answers: Arc<Mutex<std::collections::HashMap<String, UcAnswer>>> = Arc::default();
        let (c, a) = (calls.clone(), answers.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let (c, a) = (c.clone(), a.clone());
                std::thread::spawn(move || {
                    let _ = serve_one(stream, &c, &a);
                });
            }
        });
        Self {
            base_url,
            calls,
            answers,
        }
    }

    pub fn answer(&self, token: &str, answer: UcAnswer) {
        self.answers
            .lock()
            .unwrap()
            .insert(token.to_string(), answer);
    }

    pub fn calls(&self) -> Vec<UcCall> {
        self.calls.lock().unwrap().clone()
    }
}

fn serve_one(
    mut stream: std::net::TcpStream,
    calls: &Mutex<Vec<UcCall>>,
    answers: &Mutex<std::collections::HashMap<String, UcAnswer>>,
) -> std::io::Result<()> {
    use std::io::{BufRead as _, BufReader, Read as _, Write as _};
    let mut reader = BufReader::new(stream.try_clone()?);
    loop {
        let mut request_line = String::new();
        if reader.read_line(&mut request_line)? == 0 {
            return Ok(());
        }
        let path = request_line
            .split_whitespace()
            .nth(1)
            .unwrap_or_default()
            .to_string();
        let (mut len, mut authorization) = (0usize, String::new());
        loop {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            let line = line.trim_end();
            if line.is_empty() {
                break;
            }
            let (k, v) = line.split_once(':').unwrap_or((line, ""));
            match k.to_ascii_lowercase().as_str() {
                "content-length" => len = v.trim().parse().unwrap_or(0),
                "authorization" => authorization = v.trim().to_string(),
                _ => {}
            }
        }
        let mut body = vec![0; len];
        reader.read_exact(&mut body)?;
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
        calls.lock().unwrap().push(UcCall {
            path,
            body,
            authorization: authorization.clone(),
        });
        let token = authorization.strip_prefix("Bearer ").unwrap_or_default();
        let answer = answers
            .lock()
            .unwrap()
            .get(token)
            .cloned()
            .unwrap_or(UcAnswer::Status(401));
        let (status, payload) = match answer {
            UcAnswer::Grant { ttl } => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                (
                    200,
                    serde_json::json!({
                        "expiration_time": now + ttl.as_millis() as i64,
                        "url": "file:///ignored",
                    }),
                )
            }
            UcAnswer::Status(code) => (
                code,
                serde_json::json!({"error_code": "PERMISSION_DENIED", "message": "mock UC says no"}),
            ),
            UcAnswer::StatusMessage(code, message) => (
                code,
                serde_json::json!({"error_code": "PERMISSION_DENIED", "message": message}),
            ),
        };
        let payload = payload.to_string();
        write!(
            stream,
            "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{payload}",
            payload.len()
        )?;
        stream.flush()?;
    }
}

/// A server whose `file://` tables go through the UC credential path against `uc`.
pub async fn server_with_uc(uc: &MockUc) -> TestServer {
    TestServer::start_with(
        CacheConfig {
            uc_endpoint_override: Some(uc.base_url.clone()),
            credentials_for_file: true,
            ..CacheConfig::default()
        },
        true,
    )
    .await
}

pub fn with_auth(mut req: proto::GetSnapshotRequest, token: &str) -> proto::GetSnapshotRequest {
    req.auth = Some(proto::Auth {
        workspace_host: "workspace.example.invalid".into(),
        bearer_token: token.into(),
    });
    req
}

/// The [`SnapshotStore`] contract every tier implements (L1, MemoryStore, Redis).
pub async fn store_contract<S: delta_kernel_server::store::SnapshotStore>(
    store: &S,
    info: &proto::SnapshotInfo,
    payload: S::Payload,
) {
    use delta_kernel_server::store::{LatestRecord, PlanPut, StoredSnapshot, now_ms};
    let table = delta_kernel_server::cache::l2_table_key("", &info.table_root);
    let seg = delta_kernel_server::info::segment_id(info.log_segment.as_ref().unwrap()).digest();
    let tier = store.tier();
    assert!(
        store.get_snapshot(&table, &seg).await.unwrap().is_none(),
        "{tier}"
    );
    let snap = StoredSnapshot {
        seg_id: seg.clone(),
        version: 1,
        info: info.clone(),
        payload: Some(payload),
        built_at_ms: now_ms(),
        confirmed_at_ms: now_ms(),
    };
    store.put_snapshot(&table, &snap).await.unwrap();
    let got = store.get_snapshot(&table, &seg).await.unwrap().expect(tier);
    assert_eq!(
        (got.version, &got.info, &got.seg_id),
        (1, info, &seg),
        "{tier}"
    );
    assert!(got.payload.is_some(), "{tier}");
    let at = store
        .snapshot_at_version(&table, 1)
        .await
        .unwrap()
        .expect(tier);
    assert_eq!(at.seg_id, seg, "{tier}");
    assert!(
        store
            .snapshot_at_version(&table, 2)
            .await
            .unwrap()
            .is_none(),
        "{tier}"
    );
    store
        .confirm_snapshot(&table, &seg, now_ms() + 5)
        .await
        .unwrap();
    assert!(
        store
            .get_snapshot(&table, &seg)
            .await
            .unwrap()
            .unwrap()
            .confirmed_at_ms
            >= got.confirmed_at_ms,
        "{tier}"
    );

    let rec = LatestRecord {
        seg_id: seg.clone(),
        version: 1,
        confirmed_at_ms: now_ms(),
        tail_id: String::new(),
    };
    store
        .put_latest(&table, &rec, Duration::from_secs(1))
        .await
        .unwrap();
    let latest = store.get_latest(&table).await.unwrap().expect(tier);
    assert_eq!((latest.seg_id, latest.version), (seg.clone(), 1), "{tier}");

    let plan = proto::ScanPlanResult {
        result: Some(proto::scan_plan_result::Result::Plan(vec![7u8; 1000])),
        cache_hit: false,
    };
    assert_eq!(
        store.put_plan(&seg, "spec-a", &plan).await.unwrap(),
        PlanPut::Stored,
        "{tier}"
    );
    assert_eq!(
        store
            .get_plan(&seg, "spec-a")
            .await
            .unwrap()
            .unwrap()
            .result,
        plan.result,
        "{tier}"
    );
    assert!(
        store.get_plan(&seg, "spec-b").await.unwrap().is_none(),
        "{tier}"
    );
    let empty = proto::ScanPlanResult {
        result: Some(proto::scan_plan_result::Result::Empty(
            proto::StaticallyEmpty {},
        )),
        cache_hit: false,
    };
    store.put_plan(&seg, "spec-e", &empty).await.unwrap();
    assert_eq!(
        store
            .get_plan(&seg, "spec-e")
            .await
            .unwrap()
            .unwrap()
            .result,
        empty.result,
        "{tier}"
    );

    let ttl = Duration::from_secs(30);
    assert!(
        store.try_lock(&table, "v1", "me:1", ttl).await.unwrap(),
        "{tier}"
    );
    assert!(
        !store.try_lock(&table, "v1", "other:2", ttl).await.unwrap(),
        "{tier}"
    );
    store.unlock(&table, "v1", "other:2").await.unwrap();
    assert!(
        !store.try_lock(&table, "v1", "other:2", ttl).await.unwrap(),
        "{tier}: not owner"
    );
    store.unlock(&table, "v1", "me:1").await.unwrap();
    assert!(
        store.try_lock(&table, "v1", "other:2", ttl).await.unwrap(),
        "{tier}"
    );
    store.unlock(&table, "v1", "other:2").await.unwrap();

    let th = delta_kernel_server::creds::token_hash("tok");
    assert!(
        store.get_authz(&th, &table).await.unwrap().is_none(),
        "{tier}"
    );
    store
        .put_authz(&th, &table, now_ms() + 60_000)
        .await
        .unwrap();
    assert!(
        store.get_authz(&th, &table).await.unwrap().is_some(),
        "{tier}"
    );
    store
        .put_authz(&th, "other-table", now_ms() - 1)
        .await
        .unwrap();
    assert!(
        store.get_authz(&th, "other-table").await.unwrap().is_none(),
        "{tier}: expired"
    );
}
