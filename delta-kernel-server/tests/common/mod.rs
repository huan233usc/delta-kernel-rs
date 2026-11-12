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
use delta_kernel_server::cache::{CacheConfig, Core, KERNEL_FEATURES, KERNEL_REV};
use delta_kernel_server::decode::{decode_operation, decode_plan};
use delta_kernel_server::errors::{codes, to_wire_kernel_error};
use delta_kernel_server::normalize::normalize_plan_bytes;
use delta_kernel_server::proto::delta_kernel_service_client::DeltaKernelServiceClient;
use delta_kernel_server::proto::{
    self, client_msg, item, op_result, scan_plan_result, server_msg, stats_options,
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

    pub async fn start_with(config: CacheConfig, test_hooks: bool) -> Self {
        let core = Core::new("loopback-test".into(), config);
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
    _dir: tempfile::TempDir,
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
    let dir = tempfile::tempdir().unwrap();
    let src = data_dir().join(name);
    let root = if src.is_dir() {
        let root = dir.path().join("table");
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
        plans,
        client: Some(client_info()),
        expected_log_segment: None,
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
    pub kind: &'static str,
    pub urls: Vec<String>,
    pub item_error_codes: Vec<i32>,
    pub op_error_code: Option<i32>,
}

#[derive(Debug)]
pub enum Ended {
    Error(proto::Error),
    Killed,
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

/// Runs one GetSnapshot stream to completion, answering ops per `behavior`.
pub async fn run(
    client: &mut Client,
    req: proto::GetSnapshotRequest,
    behavior: Behavior,
) -> Outcome {
    let (tx, rx) = mpsc::channel::<proto::ClientMsg>(16);
    tx.send(proto::ClientMsg {
        msg: Some(client_msg::Msg::Request(req)),
    })
    .await
    .unwrap();
    let mut inbound = match client.get_snapshot(ReceiverStream::new(rx)).await {
        Ok(r) => r.into_inner(),
        Err(status) => {
            return Outcome {
                result: Err(Ended::Status(status)),
                ops: vec![],
            };
        }
    };
    let mut ops = Vec::new();
    let mut seen = 0usize;
    let behavior = Arc::new(behavior);
    loop {
        let msg = match inbound.message().await {
            Ok(Some(proto::ServerMsg { msg: Some(m) })) => m,
            Ok(Some(_)) => continue,
            Ok(None) => {
                return Outcome {
                    result: Err(Ended::EndOfStream),
                    ops,
                };
            }
            Err(status) => {
                return Outcome {
                    result: Err(Ended::Status(status)),
                    ops,
                };
            }
        };
        match msg {
            server_msg::Msg::Op(op) => {
                seen += 1;
                if seen == 1
                    && let Some(n) = &behavior.first_op
                {
                    n.notify_one();
                }
                if behavior.kill_after_ops.is_some_and(|k| seen > k) {
                    drop(tx);
                    drop(inbound);
                    return Outcome {
                        result: Err(Ended::Killed),
                        ops,
                    };
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
                if tx
                    .send(proto::ClientMsg {
                        msg: Some(client_msg::Msg::OpResult(result)),
                    })
                    .await
                    .is_err()
                {
                    return Outcome {
                        result: Err(Ended::EndOfStream),
                        ops,
                    };
                }
            }
            server_msg::Msg::Response(r) => return Outcome { result: Ok(r), ops },
            server_msg::Msg::Error(e) => {
                return Outcome {
                    result: Err(Ended::Error(e)),
                    ops,
                };
            }
        }
    }
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
        kind: "",
        urls: vec![],
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
    let local = local_snapshot(fx, Some(resp.version));
    let local_info =
        delta_kernel_server::info::compute_snapshot_info(&local, &local_engine()).unwrap();
    assert_eq!(
        resp.info.as_ref().unwrap(),
        &local_info,
        "SnapshotInfo differs from in-process computation"
    );
    assert_eq!(resp.plans.len(), specs.len());
    let mut total_rows = 0;
    for (spec, remote) in specs.iter().zip(&resp.plans) {
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

pub fn ops_issued(resp: &proto::GetSnapshotResponse) -> i32 {
    resp.stats.as_ref().unwrap().ops_issued
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
