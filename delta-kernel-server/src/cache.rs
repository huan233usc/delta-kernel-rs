//! Snapshot cache tiers, request coalescing, and request resolution (SPEC §7, v10.2–10.3).
//!
//! - **L1** ([`L1Cache`]): cache entry ([`Instance`]) = `{Arc<Snapshot>, SnapshotInfo, plans: spec
//!   hash -> result}`, plus the in-flight coalescing table. `SnapshotInfo` is computed once at
//!   build time with the build's own op channel.
//! - **L2** (optional, [`crate::store`]): `SnapshotInfo`, the encoded kernel snapshot hint and
//!   plans, shared across replicas. An L1 miss that hits L2 rebuilds the native snapshot from the
//!   hint with zero ops ([`crate::hint`]).
//! - Snapshot builds coalesce on `(table_id, location, target)` in process and, with L2, across
//!   replicas through the L2 `lock` key. Plan generation coalesces on `(instance, spec hash)`.
//!   Every waiter's stream joins the job's [`OpChannel`] so it can take over the in-flight op if
//!   the driving stream goes away.
//! - Builds that saw a non-`FileNotFound` op error (tainted) and instances that contain staged
//!   commits are returned to the waiting requests but never inserted into L1 or written to L2.
//! - L1 eviction: at most `max_instances_per_table` per table, then a global LRU bounded by
//!   `max_cache_bytes` of estimated memory.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use delta_kernel::engine::plans::PlanBasedEngine;
use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use delta_kernel::{LogPath, Snapshot, SnapshotRef};
use sha2::Digest as _;
use tokio::sync::{OnceCell, watch};

use crate::creds::{
    CredCheck, CredentialProvider, StorageCredential, StorageOverrides, is_uc_uuid,
};
use crate::errors::{
    CATALOG_STATE_REQUIRED_PREFIX, LOCAL_IO_FAILED_PREFIX, OP_FAILED_PREFIX, ServerError,
    classify_build_error, classify_kernel_error,
};
use crate::executor::{LocalCtx, OpChannel, RemotePlanExecutor, StreamPort};
use crate::info::{
    SegmentId, compute_snapshot_info, has_staged_commits, log_segment_info, segment_id,
};
use crate::local_io::{LocalIo, LocalIoPool};
use crate::plan::{PlanOutcome, generate_plan, spec_key};
use crate::proto::error::Code;
use crate::proto::{self, version_spec};
use crate::stats::{Faults, ServerCounters};
use crate::store::{
    DEFAULT_L2_TTL, L2, LatestRecord, PlanPut, SnapshotStore, StoreResult, StoredSnapshot, now_ms,
};
use crate::timetravel::{self, TimeTravelQuery};

/// Kernel revision this server is built from (SPEC §4). Overridable at build time.
pub const KERNEL_REV: &str = match option_env!("DELTA_KERNEL_SERVER_KERNEL_REV") {
    Some(rev) => rev,
    None => "4633926ff40ff39e5ba8bc690a965b4fb793c073",
};

/// Kernel feature set this server is built with; must equal the DBR `delta_kernel_ffi` build.
pub const KERNEL_FEATURES: &[&str] = &[
    "adaptive-metadata-in-dev",
    "declarative-plans",
    "default-engine-base",
    "delta-kernel-unity-catalog",
    "geo-type-in-dev",
    "internal-api",
];

const DEFAULT_OP_DEADLINE: Duration = Duration::from_secs(60);
/// Default `--l2-build-deadline-ms`.
pub const DEFAULT_L2_BUILD_DEADLINE: Duration = Duration::from_secs(120);
/// How often a replica that lost the L2 build lock re-reads L2.
const L2_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub max_instances_per_table: usize,
    pub max_cache_bytes: usize,
    /// Where simple IO ops run (SPEC v9).
    pub io_mode: IoMode,
    /// UC base URL replacing `https://{workspace_host}` (tests).
    pub uc_endpoint_override: Option<String>,
    /// Test hook: require and check UC credentials for `file://` tables too.
    pub credentials_for_file: bool,
    /// Worker threads (and in-flight permits) for server-side IO.
    pub local_io_threads: usize,
    /// object_store options for every cloud store the server builds (`--storage-option`,
    /// `--s3-*`); UC-vended credential options take precedence.
    pub storage_options: StorageOverrides,
    /// TTL of L2 `snap` / `ver` / `plan` keys (SPEC v10.3).
    pub l2_ttl: Duration,
    /// TTL of the cross-replica L2 build lock, and how long a replica that lost it waits for the
    /// holder's result before building itself.
    pub l2_build_deadline: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_instances_per_table: 2,
            max_cache_bytes: 4 << 30,
            io_mode: IoMode::Local,
            uc_endpoint_override: None,
            credentials_for_file: false,
            local_io_threads: DEFAULT_LOCAL_IO_THREADS,
            storage_options: StorageOverrides::default(),
            l2_ttl: DEFAULT_L2_TTL,
            l2_build_deadline: DEFAULT_L2_BUILD_DEADLINE,
        }
    }
}

/// Default `--local-io-threads`.
pub const DEFAULT_LOCAL_IO_THREADS: usize = 8;

/// `--io-mode`: run simple IO ops on the server (`local`, default) or forward every op to the
/// client (`forward`, the pre-v9 behavior, kept for A/B runs).
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum IoMode {
    Local,
    Forward,
}

type TableKey = (String, String);

/// Canonical identity of a catalog log tail: `(version, location, size)` sorted by version.
pub type TailId = Vec<(i64, String, i64)>;

/// What an in-flight build resolves; with the table it forms the coalescing key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum BuildTarget {
    /// Exact version.
    Version(i64),
    /// `catalog_latest`: ratified version plus canonical log tail (SPEC v6).
    Catalog(i64, TailId),
    /// Path-based latest: one re-listing shared by every concurrent request (SPEC v7).
    Latest,
}

impl BuildTarget {
    /// `{target}` of the L2 `lock` key.
    fn lock_target(&self) -> String {
        match self {
            Self::Version(v) => format!("v{v}"),
            Self::Catalog(v, tail) => format!("c{v}:{}", tail_digest(tail)),
            Self::Latest => "latest".into(),
        }
    }
}

type BuildKey = (TableKey, BuildTarget);

/// The last confirmed path-based latest instance of a table (SPEC v7).
struct LatestEntry {
    instance: Arc<Instance>,
    confirmed_at: Instant,
}

fn sha_hex(bytes: &[u8]) -> String {
    hex::encode(sha2::Sha256::digest(bytes))
}

/// `{spec_hash}` of the L2 `plan` key, and the L1 plan-cache key.
pub fn spec_hash(spec: &proto::ScanPlanSpec) -> String {
    sha_hex(&spec_key(spec))
}

/// Hex digest of a canonical catalog log tail (`latest.tail_id`, lock targets).
pub fn tail_digest(tail: &TailId) -> String {
    sha_hex(&serde_json::to_vec(tail).expect("tail ids always serialize"))
}

/// `{table_key}` of L2 keys: the UC table id when it is a UUID, else sha256 of the canonical
/// location (SPEC v10.3).
pub fn l2_table_key(table_id: &str, location: &str) -> String {
    if is_uc_uuid(table_id) {
        table_id.to_string()
    } else {
        sha_hex(location.as_bytes())
    }
}

/// Canonicalizes a request's catalog log tail (SPEC v6 §7): `(version, location, size)` sorted by
/// version, with the version taken from kernel's own log-path parsing and the location in parsed
/// URL form.
pub fn tail_id(log_tail: &[LogPath]) -> TailId {
    let mut id: TailId = log_tail
        .iter()
        .map(|p| {
            let parsed = ParsedLogPath::from(p.clone());
            (
                parsed.version as i64,
                parsed.location.location.to_string(),
                parsed.location.size as i64,
            )
        })
        .collect();
    id.sort();
    id
}

/// Whether a catalog log tail contains a staged (unpublished) commit, per kernel's parsing.
fn tail_has_staged(log_tail: &[LogPath]) -> bool {
    log_tail
        .iter()
        .any(|p| ParsedLogPath::from(p.clone()).file_type == LogPathFileType::StagedCommit)
}

/// A cached snapshot instance.
pub struct Instance {
    pub snapshot: SnapshotRef,
    pub info: proto::SnapshotInfo,
    /// Plans by spec hash.
    plans: Mutex<HashMap<String, PlanSlot>>,
    last_used: AtomicU64,
    /// Tick of the last build or confirmation that produced this instance; picks the instance an
    /// exact-version request without `expected_log_segment` gets (SPEC v8).
    confirmed: AtomicU64,
    /// Canonical log tail of a `catalog_latest` build; `None` for exact-version builds.
    catalog_tail: Option<TailId>,
    /// Canonical identity of `info.log_segment`, compared against `expected_log_segment`.
    segment_id: Option<SegmentId>,
    /// Digest of `segment_id`: `SnapshotRef.segment_id` and the L2 `seg_id`.
    pub seg_hex: String,
    /// L2 `table_key` of the instance's table.
    l2_table: String,
    /// Set once the instance is in L1. Only such instances (never staged-tail or tainted
    /// results) write plans to L2.
    cached: AtomicBool,
}

enum PlanSlot {
    Ready(proto::ScanPlanResult),
    InFlight(Arc<Job<proto::ScanPlanResult>>),
}

impl Instance {
    fn version(&self) -> i64 {
        self.snapshot.version() as i64
    }

    fn new(
        snapshot: SnapshotRef,
        info: proto::SnapshotInfo,
        catalog_tail: Option<TailId>,
        l2_table: String,
        tick: u64,
    ) -> Self {
        let segment_id = info.log_segment.as_ref().map(segment_id);
        let seg_hex = segment_id
            .as_ref()
            .map(SegmentId::digest)
            .unwrap_or_default();
        Self {
            snapshot,
            info,
            plans: Mutex::default(),
            last_used: AtomicU64::new(tick),
            confirmed: AtomicU64::new(tick),
            catalog_tail,
            segment_id,
            seg_hex,
            l2_table,
            cached: AtomicBool::new(false),
        }
    }

    /// Whether the client's expected segment names the same files as this instance's.
    fn matches_segment(&self, expected: &proto::LogSegmentInfo) -> bool {
        self.segment_id.as_ref() == Some(&segment_id(expected))
    }

    /// Estimated memory: snapshot heap (includes a CRC's inline `allFiles`), `SnapshotInfo` and
    /// plan bytes.
    pub fn estimated_bytes(&self) -> usize {
        let plans: usize = self
            .plans
            .lock()
            .unwrap()
            .values()
            .map(|slot| match slot {
                PlanSlot::Ready(r) => prost::Message::encoded_len(r),
                PlanSlot::InFlight(_) => 0,
            })
            .sum();
        self.snapshot.estimated_owned_heap_size_bytes()
            + prost::Message::encoded_len(&self.info)
            + plans
    }
}

/// A coalesced kernel job whose result every waiter receives.
pub struct Job<T> {
    channel: Arc<OpChannel>,
    done: watch::Sender<Option<Result<T, ServerError>>>,
}

impl<T: Clone> Job<T> {
    fn new(channel: Arc<OpChannel>) -> Arc<Self> {
        Arc::new(Self {
            channel,
            done: watch::channel(None).0,
        })
    }

    async fn wait(&self) -> Result<T, ServerError> {
        let mut rx = self.done.subscribe();
        let v = rx
            .wait_for(|v| v.is_some())
            .await
            .map_err(|_| ServerError::internal("job dropped"))?;
        v.clone().expect("checked is_some")
    }

    fn complete(&self, result: Result<T, ServerError>) {
        self.done.send_replace(Some(result));
    }
}

/// Server-side IO of a request, possibly not fetched yet.
#[derive(Clone)]
pub enum IoSource {
    Ready(Option<LocalCtx>),
    /// Authorized through an L2 `authz` entry: the storage credential is fetched from UC only if
    /// a job actually needs server-side IO (an L1/L2 hit needs none).
    Lazy(Arc<LazyIo>),
}

pub struct LazyIo {
    core: Weak<Core>,
    auth: proto::Auth,
    table_id: String,
    url: url::Url,
    cell: OnceCell<Result<Option<LocalCtx>, ServerError>>,
}

impl IoSource {
    async fn get(&self) -> Result<Option<LocalCtx>, ServerError> {
        match self {
            Self::Ready(io) => Ok(io.clone()),
            Self::Lazy(lazy) => lazy
                .cell
                .get_or_init(|| async {
                    let core = lazy
                        .core
                        .upgrade()
                        .ok_or_else(|| ServerError::internal("server shutting down"))?;
                    let cred = core
                        .creds
                        .credential_for(Some(&lazy.auth), &lazy.table_id, &lazy.url)
                        .await?;
                    Ok(core.io_ctx(cred))
                })
                .await
                .clone(),
        }
    }
}

/// Everything a snapshot build job needs besides its op channel.
struct BuildInputs {
    key: TableKey,
    location: String,
    target: Target,
    build_key: Option<BuildKey>,
    expected: Option<proto::LogSegmentInfo>,
    /// Cached instance to build incrementally from.
    base: Option<Arc<Instance>>,
    io: IoSource,
}

/// Result of the blocking part of a build.
enum Built {
    /// A new snapshot, its info, how it was built, and its encoded hint (`None` without L2).
    New(
        SnapshotRef,
        Box<proto::SnapshotInfo>,
        proto::ResolveKind,
        Option<Result<Vec<u8>, String>>,
    ),
    /// Path-based latest re-listing found no new commit.
    Renewed(Arc<Instance>),
}

enum BuildFailure {
    Build(delta_kernel::Error),
    Kernel(delta_kernel::KernelError),
}

/// What a finished build writes to L2.
enum L2Write {
    None,
    /// A new cacheable instance, with its encoded hint.
    Build(Option<Result<Vec<u8>, String>>),
    /// A latest re-listing that renewed the confirmed instance.
    Renewal,
}

#[derive(Clone)]
struct BuildOutcome {
    instance: Arc<Instance>,
    kind: proto::ResolveKind,
}

/// What an L2 read found for a build target.
#[derive(Default)]
struct L2Lookup {
    hit: Option<L2Hit>,
    /// A path-based latest entry older than the staleness bound: a base to re-list from.
    stale_base: Option<StoredSnapshot<Vec<u8>>>,
    /// The `latest` record read, if any (baseline while waiting for another replica).
    latest_seen: Option<LatestRecord>,
}

struct L2Hit {
    snap: StoredSnapshot<Vec<u8>>,
    /// For latest targets: age of the confirmation.
    latest_age: Option<Duration>,
}

#[derive(Default)]
struct CacheState {
    tables: HashMap<TableKey, Vec<Arc<Instance>>>,
    inflight: HashMap<BuildKey, Arc<Job<BuildOutcome>>>,
    latest: HashMap<TableKey, LatestEntry>,
}

impl CacheState {
    /// The table's latest entry if its instance is still cached; drops it once evicted.
    fn live_latest(&mut self, key: &TableKey) -> Option<&LatestEntry> {
        let cached = self.latest.get(key).is_some_and(|e| {
            self.tables
                .get(key)
                .is_some_and(|insts| insts.iter().any(|i| Arc::ptr_eq(i, &e.instance)))
        });
        if !cached {
            self.latest.remove(key);
        }
        self.latest.get(key)
    }
}

/// The in-process tier (SPEC v10.3 L1): instances, the latest map and the in-flight table.
pub struct L1Cache {
    state: Mutex<CacheState>,
    max_instances_per_table: usize,
    max_cache_bytes: usize,
    clock: AtomicU64,
    /// [`SnapshotStore`] lock and authz entries (the trait view; `Core` coalesces through
    /// `inflight` and checks credentials through [`CredentialProvider`]).
    locks: Mutex<HashMap<(String, String), (String, Instant)>>,
    authz: Mutex<HashMap<(String, String), i64>>,
}

/// L1 sizes for `GetDebugStats`.
pub struct L1Stats {
    pub entries: i64,
    pub bytes: i64,
    pub entry_bytes: HashMap<String, i64>,
}

impl L1Cache {
    fn new(max_instances_per_table: usize, max_cache_bytes: usize) -> Self {
        Self {
            state: Mutex::default(),
            max_instances_per_table,
            max_cache_bytes,
            clock: AtomicU64::new(0),
            locks: Mutex::default(),
            authz: Mutex::default(),
        }
    }

    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Instances cached per table id (debug stats).
    pub fn cached_instances_per_table(&self) -> HashMap<String, i32> {
        let st = self.state.lock().unwrap();
        st.tables
            .iter()
            .map(|((id, _), v)| (id.clone(), v.len() as i32))
            .collect()
    }

    pub fn reset(&self) {
        let mut st = self.state.lock().unwrap();
        st.tables.clear();
        st.latest.clear();
    }

    /// Entry count, total estimated bytes and estimated bytes per `seg_id`.
    pub fn stats(&self) -> L1Stats {
        let st = self.state.lock().unwrap();
        let mut entry_bytes = HashMap::new();
        let mut bytes = 0i64;
        let mut entries = 0i64;
        for inst in st.tables.values().flatten() {
            let b = inst.estimated_bytes() as i64;
            entries += 1;
            bytes += b;
            *entry_bytes.entry(inst.seg_hex.clone()).or_default() += b;
        }
        L1Stats {
            entries,
            bytes,
            entry_bytes,
        }
    }

    /// Inserts an instance, deduplicating on `(version, log_segment)` so an equal instance keeps
    /// its plan cache, then applies eviction. Returns the instance now in the cache.
    fn insert(&self, key: &TableKey, instance: Arc<Instance>) -> Arc<Instance> {
        let inserted = {
            let mut st = self.state.lock().unwrap();
            let insts = st.tables.entry(key.clone()).or_default();
            if let Some(existing) = insts.iter().find(|i| {
                i.version() == instance.version()
                    && i.segment_id == instance.segment_id
                    && i.catalog_tail == instance.catalog_tail
            }) {
                // A fresh build of the same files re-confirms the cached instance.
                existing.confirmed.store(self.tick(), Ordering::Relaxed);
                existing.clone()
            } else {
                instance.cached.store(true, Ordering::Relaxed);
                insts.push(instance.clone());
                while insts.len() > self.max_instances_per_table.max(1) {
                    let (lru, _) = insts
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, i)| i.last_used.load(Ordering::Relaxed))
                        .expect("non-empty");
                    let evicted = insts.remove(lru);
                    tracing::info!(table = %key.0, version = evicted.version(), "evicted (per-table)");
                }
                instance
            }
        };
        self.evict_global();
        inserted
    }

    /// Records `instance` as the table's confirmed latest as of `at`. Monotonic like the L2
    /// pointer ([`crate::store::latest_replaces`]): never a lower version, and at the same version
    /// only a later confirmation.
    fn confirm_latest(&self, key: &TableKey, instance: Arc<Instance>, at: Instant) {
        let mut st = self.state.lock().unwrap();
        let newer = st.latest.get(key).is_some_and(|e| {
            e.instance.version() > instance.version()
                || (e.instance.version() == instance.version() && e.confirmed_at > at)
        });
        if !newer {
            instance.confirmed.store(self.tick(), Ordering::Relaxed);
            st.latest.insert(
                key.clone(),
                LatestEntry {
                    instance,
                    confirmed_at: at,
                },
            );
        }
    }

    /// Global LRU over all tables by estimated memory. Always keeps at least one instance.
    fn evict_global(&self) {
        let mut st = self.state.lock().unwrap();
        loop {
            let all: Vec<(TableKey, usize, u64, usize)> = st
                .tables
                .iter()
                .flat_map(|(k, insts)| {
                    insts.iter().enumerate().map(|(idx, i)| {
                        (
                            k.clone(),
                            idx,
                            i.last_used.load(Ordering::Relaxed),
                            i.estimated_bytes(),
                        )
                    })
                })
                .collect();
            let total: usize = all.iter().map(|x| x.3).sum();
            if total <= self.max_cache_bytes || all.len() <= 1 {
                break;
            }
            let (k, idx, _, bytes) = all.into_iter().min_by_key(|x| x.2).expect("non-empty");
            let insts = st.tables.get_mut(&k).expect("present");
            insts.remove(idx);
            if insts.is_empty() {
                st.tables.remove(&k);
            }
            tracing::info!(table = %k.0, bytes, "evicted (global LRU)");
        }
        st.tables.retain(|_, v| !v.is_empty());
    }

    fn find_by_seg(&self, l2_table: &str, seg_hex: &str) -> Option<(TableKey, Arc<Instance>)> {
        let st = self.state.lock().unwrap();
        st.tables.iter().find_map(|(k, insts)| {
            insts
                .iter()
                .find(|i| i.l2_table == l2_table && i.seg_hex == seg_hex)
                .map(|i| (k.clone(), i.clone()))
        })
    }

    fn stored(inst: &Instance) -> StoredSnapshot<SnapshotRef> {
        StoredSnapshot {
            seg_id: inst.seg_hex.clone(),
            version: inst.version(),
            info: inst.info.clone(),
            payload: Some(inst.snapshot.clone()),
            built_at_ms: 0,
            confirmed_at_ms: inst.confirmed.load(Ordering::Relaxed) as i64,
        }
    }
}

/// The trait view of L1 (contract tests; `Core` uses the inherent API above so lookup-or-join
/// stays atomic). `confirmed_at_ms` is L1's logical confirmation tick.
#[async_trait]
impl SnapshotStore for L1Cache {
    type Payload = SnapshotRef;

    fn tier(&self) -> &'static str {
        "l1"
    }

    async fn get_snapshot(
        &self,
        table: &str,
        seg_id: &str,
    ) -> StoreResult<Option<StoredSnapshot<SnapshotRef>>> {
        Ok(self
            .find_by_seg(table, seg_id)
            .map(|(_, i)| Self::stored(&i)))
    }

    async fn snapshot_at_version(
        &self,
        table: &str,
        version: i64,
    ) -> StoreResult<Option<StoredSnapshot<SnapshotRef>>> {
        let st = self.state.lock().unwrap();
        Ok(st
            .tables
            .values()
            .flatten()
            .filter(|i| i.l2_table == table && i.version() == version)
            .max_by_key(|i| i.confirmed.load(Ordering::Relaxed))
            .map(|i| Self::stored(i)))
    }

    async fn put_snapshot(
        &self,
        table: &str,
        snap: &StoredSnapshot<SnapshotRef>,
    ) -> StoreResult<()> {
        let snapshot = snap
            .payload
            .clone()
            .ok_or("L1 entries need a native snapshot")?;
        let root = snapshot.table_root().to_string();
        let key = if is_uc_uuid(table) {
            (table.to_string(), root)
        } else {
            (root.clone(), root)
        };
        let inst = Arc::new(Instance::new(
            snapshot,
            snap.info.clone(),
            None,
            table.to_string(),
            self.tick(),
        ));
        self.insert(&key, inst);
        Ok(())
    }

    async fn confirm_snapshot(&self, table: &str, seg_id: &str, _at_ms: i64) -> StoreResult<()> {
        if let Some((_, i)) = self.find_by_seg(table, seg_id) {
            i.confirmed.store(self.tick(), Ordering::Relaxed);
        }
        Ok(())
    }

    async fn get_latest(&self, table: &str, tail_id: &str) -> StoreResult<Option<LatestRecord>> {
        let mut st = self.state.lock().unwrap();
        let key = st
            .latest
            .iter()
            .find(|(_, e)| {
                e.instance.l2_table == table
                    && e.instance
                        .catalog_tail
                        .as_ref()
                        .map(tail_digest)
                        .unwrap_or_default()
                        == tail_id
            })
            .map(|(k, _)| k.clone());
        Ok(key.and_then(|k| {
            st.live_latest(&k).map(|e| LatestRecord {
                seg_id: e.instance.seg_hex.clone(),
                version: e.instance.version(),
                confirmed_at_ms: now_ms() - e.confirmed_at.elapsed().as_millis() as i64,
                tail_id: e
                    .instance
                    .catalog_tail
                    .as_ref()
                    .map(tail_digest)
                    .unwrap_or_default(),
            })
        }))
    }

    async fn put_latest(
        &self,
        table: &str,
        rec: &LatestRecord,
        _staleness: Duration,
    ) -> StoreResult<()> {
        let (key, inst) = self
            .find_by_seg(table, &rec.seg_id)
            .ok_or("L1 latest must name a cached instance")?;
        let age = Duration::from_millis((now_ms() - rec.confirmed_at_ms).max(0) as u64);
        let at = Instant::now().checked_sub(age).unwrap_or_else(Instant::now);
        self.confirm_latest(&key, inst, at);
        Ok(())
    }

    async fn get_plan(
        &self,
        seg_id: &str,
        spec_hash: &str,
    ) -> StoreResult<Option<proto::ScanPlanResult>> {
        let st = self.state.lock().unwrap();
        Ok(st
            .tables
            .values()
            .flatten()
            .find(|i| i.seg_hex == seg_id)
            .and_then(|i| match i.plans.lock().unwrap().get(spec_hash) {
                Some(PlanSlot::Ready(r)) => Some(r.clone()),
                _ => None,
            }))
    }

    async fn put_plan(
        &self,
        seg_id: &str,
        spec_hash: &str,
        plan: &proto::ScanPlanResult,
    ) -> StoreResult<PlanPut> {
        let st = self.state.lock().unwrap();
        let inst = st
            .tables
            .values()
            .flatten()
            .find(|i| i.seg_hex == seg_id)
            .ok_or("L1 plans need a cached instance")?;
        inst.plans
            .lock()
            .unwrap()
            .insert(spec_hash.to_string(), PlanSlot::Ready(plan.clone()));
        Ok(PlanPut::Stored)
    }

    async fn try_lock(
        &self,
        table: &str,
        target: &str,
        owner: &str,
        ttl: Duration,
    ) -> StoreResult<bool> {
        let mut locks = self.locks.lock().unwrap();
        let key = (table.to_string(), target.to_string());
        if locks
            .get(&key)
            .is_some_and(|(_, until)| Instant::now() < *until)
        {
            return Ok(false);
        }
        locks.insert(key, (owner.to_string(), Instant::now() + ttl));
        Ok(true)
    }

    async fn unlock(&self, table: &str, target: &str, owner: &str) -> StoreResult<()> {
        let mut locks = self.locks.lock().unwrap();
        let key = (table.to_string(), target.to_string());
        if locks.get(&key).is_some_and(|(o, _)| o == owner) {
            locks.remove(&key);
        }
        Ok(())
    }

    async fn get_authz(&self, token_hash: &str, table: &str) -> StoreResult<Option<i64>> {
        Ok(self
            .authz
            .lock()
            .unwrap()
            .get(&(token_hash.to_string(), table.to_string()))
            .copied()
            .filter(|until| *until > now_ms()))
    }

    async fn put_authz(&self, token_hash: &str, table: &str, until_ms: i64) -> StoreResult<()> {
        self.authz
            .lock()
            .unwrap()
            .insert((token_hash.to_string(), table.to_string()), until_ms);
        Ok(())
    }
}

/// Server core shared by all streams.
pub struct Core {
    pub instance_id: String,
    pub config: CacheConfig,
    pub counters: Arc<ServerCounters>,
    pub faults: Arc<Faults>,
    pub creds: CredentialProvider,
    local_fs: Arc<LocalIo>,
    /// Bounded pool all server-side IO runs on (`--local-io-threads`).
    pub io_pool: Arc<LocalIoPool>,
    pub l1: L1Cache,
    l2: Option<Arc<L2>>,
    job_seq: AtomicU64,
}

/// What a request resolved to before plans are generated.
enum Resolution {
    Hit(Arc<Instance>),
    Wait(Arc<Job<BuildOutcome>>, bool),
}

/// How the target snapshot is built.
#[derive(Clone)]
enum Target {
    Version(i64),
    CatalogLatest {
        max_catalog_version: i64,
        log_tail: Vec<LogPath>,
        tail_id: TailId,
        tail_has_staged: bool,
    },
    Latest {
        max_staleness: Duration,
    },
}

fn parse_log_tail(files: &[proto::FileStatus]) -> Result<Vec<LogPath>, ServerError> {
    files
        .iter()
        .map(|f| {
            crate::executor::file_meta_from_status(f)
                .and_then(LogPath::try_new)
                .map_err(|e| ServerError::internal(format!("bad log_tail entry: {e}")))
        })
        .collect()
}

fn catalog_target(max_catalog_version: i64, log_tail: Vec<LogPath>) -> Target {
    Target::CatalogLatest {
        max_catalog_version,
        tail_id: tail_id(&log_tail),
        tail_has_staged: tail_has_staged(&log_tail),
        log_tail,
    }
}

fn parse_target(at: Option<&proto::VersionSpec>) -> Result<Target, ServerError> {
    Ok(match at.and_then(|a| a.spec.as_ref()) {
        Some(version_spec::Spec::Version(v)) => Target::Version(*v),
        Some(version_spec::Spec::CatalogLatest(c)) => {
            catalog_target(c.max_catalog_version, parse_log_tail(&c.log_tail)?)
        }
        Some(version_spec::Spec::Latest(l)) => Target::Latest {
            max_staleness: Duration::from_millis(l.max_staleness_ms.max(0) as u64),
        },
        None => Target::Latest {
            max_staleness: Duration::ZERO,
        },
    })
}

/// ResolveTimeTravel on a catalog-managed table without `catalog_latest` (v10 review B2).
fn catalog_state_required() -> ServerError {
    ServerError::new(
        Code::Unsupported,
        format!(
            "{}catalog-managed table: ResolveTimeTravel needs catalog_latest; the published \
             filesystem latest is not the table's latest",
            timetravel::LOG_HISTORY_PREFIX
        ),
    )
}

fn stale_ref(message: impl Into<String>) -> ServerError {
    ServerError::new(Code::StaleRef, message)
}

impl Core {
    pub fn new(instance_id: String, config: CacheConfig) -> Arc<Self> {
        Self::with_l2(instance_id, config, None)
    }

    /// A core with an L2 tier shared with other replicas (SPEC v10.3).
    pub fn with_l2(instance_id: String, config: CacheConfig, l2: Option<Arc<L2>>) -> Arc<Self> {
        let creds = CredentialProvider::new(
            config.uc_endpoint_override.clone(),
            config.credentials_for_file,
            config.storage_options.clone(),
        );
        let io_pool = LocalIoPool::new(config.local_io_threads);
        let l1 = L1Cache::new(config.max_instances_per_table, config.max_cache_bytes);
        Arc::new(Self {
            instance_id,
            config,
            counters: Arc::default(),
            faults: Arc::default(),
            creds,
            local_fs: Arc::new(LocalIo::local_fs()),
            io_pool,
            l1,
            l2,
            job_seq: AtomicU64::new(1),
        })
    }

    pub fn has_l2(&self) -> bool {
        self.l2.is_some()
    }

    fn tick(&self) -> u64 {
        self.l1.tick()
    }

    /// Instances cached per table id (debug stats).
    pub fn cached_instances_per_table(&self) -> HashMap<String, i32> {
        self.l1.cached_instances_per_table()
    }

    /// Clears L1 (instances, latest confirmations). L2 is shared with other replicas and is not
    /// touched.
    pub fn reset_cache(&self) {
        self.l1.reset();
    }

    /// Validates the client's kernel build; any mismatch is `VERSION_MISMATCH`.
    pub fn check_client(client: Option<&proto::ClientInfo>) -> Result<Duration, ServerError> {
        let Some(client) = client else {
            return Err(ServerError::new(
                Code::VersionMismatch,
                "ClientInfo is required",
            ));
        };
        if client.kernel_rev != KERNEL_REV {
            return Err(ServerError::new(
                Code::VersionMismatch,
                format!(
                    "kernel_rev mismatch: client {} server {KERNEL_REV}",
                    client.kernel_rev
                ),
            ));
        }
        let mut theirs: Vec<&str> = client.kernel_features.iter().map(String::as_str).collect();
        theirs.sort_unstable();
        theirs.dedup();
        if theirs != KERNEL_FEATURES {
            return Err(ServerError::new(
                Code::VersionMismatch,
                format!("kernel_features mismatch: client {theirs:?} server {KERNEL_FEATURES:?}"),
            ));
        }
        Ok(if client.op_deadline_ms > 0 {
            Duration::from_millis(client.op_deadline_ms as u64)
        } else {
            DEFAULT_OP_DEADLINE
        })
    }

    /// `(cache key, normalized location, parsed URL)` of a request's table.
    fn table_key(table: &proto::TableRef) -> Result<(TableKey, String, url::Url), ServerError> {
        let location = normalize_location(&table.location)?;
        let table_id = if table.table_id.is_empty() {
            location.clone()
        } else {
            table.table_id.clone()
        };
        let url = url::Url::parse(&location)
            .map_err(|e| ServerError::new(Code::TableNotFound, format!("bad location: {e}")))?;
        Ok(((table_id, location.clone()), location, url))
    }

    fn io_ctx(&self, cred: Option<Arc<StorageCredential>>) -> Option<LocalCtx> {
        match self.config.io_mode {
            IoMode::Forward => None,
            IoMode::Local => Some(LocalCtx {
                io: cred.map_or_else(|| self.local_fs.clone(), |c| c.io.clone()),
                pool: self.io_pool.clone(),
            }),
        }
    }

    /// The authz check, first on every request including cache hits (SPEC v9, v9.1, v10.2):
    /// in-process credential cache, then the shared L2 `authz` entry, then UC.
    async fn authorize(
        self: &Arc<Self>,
        auth: Option<&proto::Auth>,
        key: &TableKey,
        url: &url::Url,
    ) -> Result<IoSource, ServerError> {
        match self.creds.check_cached(auth, &key.0, url)? {
            CredCheck::NotNeeded => Ok(IoSource::Ready(self.io_ctx(None))),
            CredCheck::Cached(c) => Ok(IoSource::Ready(self.io_ctx(Some(c)))),
            CredCheck::Miss { token_hash } => {
                let l2_table = l2_table_key(&key.0, &key.1);
                if let Some(l2) = &self.l2 {
                    match l2.get_authz(&token_hash, &l2_table).await {
                        Ok(Some(_)) => {
                            self.counters.l2_authz_hits.fetch_add(1, Ordering::Relaxed);
                            let auth = auth.cloned().expect("check_cached requires auth");
                            return Ok(IoSource::Lazy(Arc::new(LazyIo {
                                core: Arc::downgrade(self),
                                auth,
                                table_id: key.0.clone(),
                                url: url.clone(),
                                cell: OnceCell::new(),
                            })));
                        }
                        Ok(None) => {}
                        Err(e) => self.l2_error("get authz", &e),
                    }
                }
                let cred = self.creds.credential_for(auth, &key.0, url).await?;
                if let (Some(l2), Some(c)) = (&self.l2, &cred)
                    && let Err(e) = l2
                        .put_authz(&token_hash, &l2_table, c.usable_until_ms)
                        .await
                {
                    self.l2_error("put authz", &e);
                }
                Ok(IoSource::Ready(self.io_ctx(cred)))
            }
        }
    }

    fn l2_error(&self, what: &str, e: &str) {
        self.counters.l2_errors.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(error = %e, "L2 {what} failed; treated as a miss");
    }

    fn stats(
        &self,
        port: &StreamPort,
        started: Instant,
        resolve_us: i64,
        plan_us: i64,
    ) -> proto::ServerStats {
        proto::ServerStats {
            ops_issued: port.ops_issued(),
            server_time_us: started.elapsed().as_micros() as i64,
            client_op_time_us: port.client_op_time_us(),
            instance_id: self.instance_id.clone(),
            ops_local: port.ops_local(),
            resolve_us,
            plan_us,
        }
    }

    fn snapshot_ref(&self, table: &proto::TableRef, instance: &Instance) -> proto::SnapshotRef {
        proto::SnapshotRef {
            table: Some(table.clone()),
            version: instance.version(),
            log_segment: instance.info.log_segment.clone(),
            segment_id: instance.seg_hex.clone(),
            instance_id: self.instance_id.clone(),
        }
    }

    /// Resolves one GetSnapshot request whose stream is `port`. Prefetched plans are exactly one
    /// GetScanPlan per spec on the same stream (SPEC v10.2).
    pub async fn handle(
        self: &Arc<Self>,
        req: proto::GetSnapshotRequest,
        port: &Arc<StreamPort>,
    ) -> Result<proto::GetSnapshotResponse, ServerError> {
        let started = Instant::now();
        let table = req
            .table
            .clone()
            .ok_or_else(|| ServerError::internal("GetSnapshotRequest.table is required"))?;
        let (key, location, url) = Self::table_key(&table)?;
        let io = self.authorize(req.auth.as_ref(), &key, &url).await?;
        let target = parse_target(req.at.as_ref())?;
        let (instance, resolved_by) = self
            .resolve_instance(
                &key,
                &location,
                &target,
                req.expected_log_segment.as_ref(),
                port,
                &io,
            )
            .await?;
        let resolve_us = started.elapsed().as_micros() as i64;

        let plan_started = Instant::now();
        let mut plans = Vec::with_capacity(req.prefetch_plans.len());
        for spec in &req.prefetch_plans {
            plans.push(self.plan_for(&instance, spec, port, &io).await?);
        }
        let plan_us = plan_started.elapsed().as_micros() as i64;
        instance.last_used.store(self.tick(), Ordering::Relaxed);
        // Plans grew the instance; re-apply the memory bound.
        self.l1.evict_global();

        Ok(proto::GetSnapshotResponse {
            version: instance.version(),
            info: Some(instance.info.clone()),
            prefetched_plans: plans,
            resolved_by: resolved_by as i32,
            kernel_rev: KERNEL_REV.to_string(),
            stats: Some(self.stats(port, started, resolve_us, plan_us)),
            r#ref: Some(self.snapshot_ref(&table, &instance)),
        })
    }

    /// Resolves one GetScanPlan request (SPEC v10.2): authz, then the ref's instance through L1,
    /// L2 or an exact rebuild, which must have the ref's canonical segment (else `STALE_REF`),
    /// then the coalesced, cached plan.
    pub async fn handle_scan_plan(
        self: &Arc<Self>,
        req: proto::GetScanPlanRequest,
        port: &Arc<StreamPort>,
    ) -> Result<proto::GetScanPlanResponse, ServerError> {
        let started = Instant::now();
        let r = req
            .r#ref
            .as_ref()
            .ok_or_else(|| ServerError::internal("GetScanPlanRequest.ref is required"))?;
        let table = r
            .table
            .as_ref()
            .ok_or_else(|| ServerError::internal("SnapshotRef.table is required"))?;
        let spec = req
            .spec
            .as_ref()
            .ok_or_else(|| ServerError::internal("GetScanPlanRequest.spec is required"))?;
        let (key, location, url) = Self::table_key(table)?;
        let io = self.authorize(req.auth.as_ref(), &key, &url).await?;
        let seg = r
            .log_segment
            .as_ref()
            .ok_or_else(|| ServerError::internal("SnapshotRef.log_segment is required"))?;
        let wanted = segment_id(seg);
        let well_formed = r.segment_id.len() == 64
            && r.segment_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        if !well_formed {
            return Err(stale_ref(
                "SnapshotRef.segment_id must be 64 lowercase hex digits (sha256)",
            ));
        }
        if r.segment_id != wanted.digest() {
            return Err(stale_ref(
                "SnapshotRef.segment_id does not match its log_segment",
            ));
        }
        if seg.version != r.version {
            return Err(stale_ref(
                "SnapshotRef.version does not match its log_segment",
            ));
        }
        // Staged-tail instances are never cached: rebuild from the ref's own staged deltas, the
        // catalog log tail the snapshot was built with.
        let staged_tail: Vec<proto::FileStatus> = seg
            .deltas
            .iter()
            .enumerate()
            .filter(|(i, _)| seg.delta_is_staged.get(*i).copied().unwrap_or(false))
            .map(|(_, f)| f.clone())
            .collect();
        let target = if staged_tail.is_empty() {
            Target::Version(r.version)
        } else {
            catalog_target(r.version, parse_log_tail(&staged_tail)?)
        };
        if r.instance_id == self.instance_id {
            self.counters
                .ref_same_instance
                .fetch_add(1, Ordering::Relaxed);
        }
        let (instance, _) = self
            .resolve_instance(&key, &location, &target, Some(seg), port, &io)
            .await?;
        if instance.segment_id.as_ref() != Some(&wanted) {
            return Err(stale_ref(format!(
                "SnapshotRef names a log segment the table no longer resolves to at version {} \
                 (resolved {}); call GetSnapshot again",
                r.version, instance.seg_hex
            )));
        }
        let resolve_us = started.elapsed().as_micros() as i64;
        let plan_started = Instant::now();
        let plan = self.plan_for(&instance, spec, port, &io).await?;
        let plan_us = plan_started.elapsed().as_micros() as i64;
        instance.last_used.store(self.tick(), Ordering::Relaxed);
        self.l1.evict_global();
        Ok(proto::GetScanPlanResponse {
            plan: Some(plan),
            stats: Some(self.stats(port, started, resolve_us, plan_us)),
        })
    }

    /// Resolves one ResolveTimeTravel request on the latest (or `catalog_latest`) instance, with
    /// JNR `getActiveCommitAtTimestamp` / `checkVersionExists` semantics (SPEC v10.2). The result
    /// is not cached; only the latest instance is.
    pub async fn handle_time_travel(
        self: &Arc<Self>,
        req: proto::ResolveTimeTravelRequest,
        port: &Arc<StreamPort>,
    ) -> Result<proto::ResolveTimeTravelResponse, ServerError> {
        let started = Instant::now();
        let table = req
            .table
            .as_ref()
            .ok_or_else(|| ServerError::internal("ResolveTimeTravelRequest.table is required"))?;
        let query = TimeTravelQuery::from_request(&req)?;
        let (key, location, url) = Self::table_key(table)?;
        let io = self.authorize(req.auth.as_ref(), &key, &url).await?;
        let (target, earliest_ratified) = match &req.catalog_latest {
            Some(c) => {
                let tail = parse_log_tail(&c.log_tail)?;
                let earliest = tail_id(&tail).first().map(|(v, _, _)| *v as u64);
                (catalog_target(c.max_catalog_version, tail), earliest)
            }
            // A fresh latest, like JNR's uncached snapshot manager: at most one re-listing.
            None => (
                Target::Latest {
                    max_staleness: Duration::ZERO,
                },
                None,
            ),
        };
        let path_based = req.catalog_latest.is_none();
        let resolved = self
            .resolve_instance(&key, &location, &target, None, port, &io)
            .await;
        // Without catalog_latest the server can only see the published filesystem latest. For a
        // catalog-managed table that is not the table's latest: refuse (the client falls back to
        // its catalog path) whether kernel rejected the build or a cached instance exists.
        let instance = match resolved {
            Ok((instance, _))
                if path_based && instance.snapshot.table_configuration().is_catalog_managed() =>
            {
                return Err(catalog_state_required());
            }
            Ok((instance, _)) => instance,
            Err(e)
                if path_based
                    && e.code == Code::Unsupported
                    && e.message.starts_with(CATALOG_STATE_REQUIRED_PREFIX) =>
            {
                return Err(catalog_state_required());
            }
            Err(e) => return Err(e),
        };
        let resolve_us = started.elapsed().as_micros() as i64;

        let io = io.get().await?;
        let channel = OpChannel::new(
            port.clone(),
            None,
            self.counters.clone(),
            self.faults.clone(),
        );
        let snapshot = instance.snapshot.clone();
        let job_channel = channel.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            let engine =
                PlanBasedEngine::new(None, Arc::new(RemotePlanExecutor::new(job_channel, io)));
            timetravel::resolve(&snapshot, &engine, &query, earliest_ratified)
        })
        .await
        .map_err(|e| ServerError::internal(format!("time travel panicked: {e}")))?;
        let resolved = match outcome {
            Ok(_) if channel.cancel_reason().is_some() => Err(deadline_or_cancel(&channel)),
            Ok(r) => Ok(r),
            Err(timetravel::TimeTravelError::Api(err)) => Err(err),
            Err(timetravel::TimeTravelError::Kernel(e)) => match timetravel::history_error(&e) {
                Some(err)
                    if channel.first_op_failure().is_none()
                        && channel.cancel_reason().is_none() =>
                {
                    Err(err)
                }
                _ => Err(local_unavailable(
                    &channel,
                    classify_kernel_error(
                        &e,
                        channel.cancel_reason(),
                        channel.first_op_failure().as_ref(),
                    ),
                )),
            },
        }?;
        Ok(proto::ResolveTimeTravelResponse {
            version: resolved.version,
            commit_timestamp_ms: resolved.commit_timestamp_ms,
            earliest_recreatable_version: resolved.earliest_version,
            latest_version: instance.version(),
            stats: Some(self.stats(port, started, resolve_us, 0)),
        })
    }

    /// L1 lookup or join/start a build, and wait for it.
    async fn resolve_instance(
        self: &Arc<Self>,
        key: &TableKey,
        location: &str,
        target: &Target,
        expected: Option<&proto::LogSegmentInfo>,
        port: &Arc<StreamPort>,
        io: &IoSource,
    ) -> Result<(Arc<Instance>, proto::ResolveKind), ServerError> {
        match self.resolve(key, location, target, expected, port, io) {
            Resolution::Hit(inst) => Ok((inst, proto::ResolveKind::CacheHit)),
            Resolution::Wait(job, coalesced) => {
                let outcome = job.wait().await;
                job.channel.leave(port);
                let outcome = outcome?;
                let kind = if coalesced {
                    proto::ResolveKind::Coalesced
                } else {
                    outcome.kind
                };
                Ok((outcome.instance, kind))
            }
        }
    }

    /// L1 lookup or join/start a build. Never blocks on IO.
    fn resolve(
        self: &Arc<Self>,
        key: &TableKey,
        location: &str,
        target: &Target,
        expected: Option<&proto::LogSegmentInfo>,
        port: &Arc<StreamPort>,
        io: &IoSource,
    ) -> Resolution {
        let mut st = self.l1.state.lock().unwrap();
        // Exact-version requests hit only when the client's expected log segment is present and
        // names the same files as the cached one (SPEC v6/v7: absent => miss; compared by
        // canonical identity). `catalog_latest` hits and coalesces on (max_catalog_version,
        // canonical log tail); a tail with staged commits is neither served from cache nor
        // coalesced. Path-based latest is served from the table's last confirmed latest instance
        // within `max_staleness`, otherwise re-listed once for all concurrent requests.
        let (target_version, build_key): (Option<i64>, Option<BuildKey>) = match target {
            Target::Version(v) => (Some(*v), Some((key.clone(), BuildTarget::Version(*v)))),
            Target::CatalogLatest {
                max_catalog_version,
                tail_has_staged: true,
                ..
            } => (Some(*max_catalog_version), None),
            Target::CatalogLatest {
                max_catalog_version,
                tail_id,
                ..
            } => (
                Some(*max_catalog_version),
                Some((
                    key.clone(),
                    BuildTarget::Catalog(*max_catalog_version, tail_id.clone()),
                )),
            ),
            Target::Latest { .. } => (None, Some((key.clone(), BuildTarget::Latest))),
        };

        if let Some(bk) = &build_key {
            let hit = match (&bk.1, target) {
                // With `expected_log_segment`: strict canonical match. Without it (SPEC v8): the
                // most recently built/confirmed instance at that version.
                (BuildTarget::Version(v), _) => st.tables.get(key).and_then(|insts| {
                    let at_version = insts.iter().filter(|i| i.version() == *v);
                    match expected {
                        Some(e) => at_version.into_iter().find(|i| i.matches_segment(e)),
                        None => at_version
                            .into_iter()
                            .max_by_key(|i| i.confirmed.load(Ordering::Relaxed)),
                    }
                    .cloned()
                }),
                (BuildTarget::Catalog(v, t), _) => st.tables.get(key).and_then(|insts| {
                    insts
                        .iter()
                        .find(|i| {
                            i.version() == *v
                                && i.catalog_tail.as_ref() == Some(t)
                                && expected.is_none_or(|e| i.matches_segment(e))
                        })
                        .cloned()
                }),
                (BuildTarget::Latest, Target::Latest { max_staleness }) => st
                    .live_latest(key)
                    .filter(|e| e.confirmed_at.elapsed() <= *max_staleness)
                    .map(|e| e.instance.clone()),
                (BuildTarget::Latest, _) => None,
            };
            if let Some(inst) = hit {
                inst.last_used.store(self.tick(), Ordering::Relaxed);
                return Resolution::Hit(inst);
            }
            if let Some(job) = st.inflight.get(bk) {
                job.channel.join(port.clone());
                self.counters
                    .builds_coalesced
                    .fetch_add(1, Ordering::Relaxed);
                return Resolution::Wait(job.clone(), true);
            }
        }

        // Incremental base (exact-version builds only): the newest cached instance strictly
        // older than the target.
        let base = match target {
            Target::Version(v) => st.tables.get(key).and_then(|insts| {
                insts
                    .iter()
                    .filter(|i| i.version() < *v)
                    .max_by_key(|i| i.version())
                    .cloned()
            }),
            // Re-list from the last confirmed latest instance, if it is still cached.
            Target::Latest { .. } => st.live_latest(key).map(|e| e.instance.clone()),
            Target::CatalogLatest { .. } => None,
        };
        let channel = OpChannel::new(
            port.clone(),
            target_version,
            self.counters.clone(),
            self.faults.clone(),
        );
        let job = Job::new(channel);
        if let Some(bk) = &build_key {
            st.inflight.insert(bk.clone(), job.clone());
        }
        drop(st);
        // A latest re-listing is a build only if it finds new commits; it is counted when its
        // outcome is known (`spawn_build`). Other builds are counted once L2 missed.
        if matches!(target, Target::Latest { .. }) && base.is_some() {
            self.counters
                .latest_relists_started
                .fetch_add(1, Ordering::Relaxed);
        }
        self.spawn_build(
            job.clone(),
            BuildInputs {
                key: key.clone(),
                location: location.to_string(),
                target: target.clone(),
                build_key,
                expected: expected.cloned(),
                base,
                io: io.clone(),
            },
        );
        Resolution::Wait(job, false)
    }

    /// Reads L2 for a build target. Errors are logged and read as a miss. `waiting` (with the
    /// first read's latest record as baseline) accepts any newer latest confirmation: the
    /// re-listing of the replica holding the build lock.
    async fn l2_lookup(
        &self,
        l2: &L2,
        table: &str,
        target: &Target,
        expected: Option<&proto::LogSegmentInfo>,
        waiting: Option<Option<&LatestRecord>>,
    ) -> L2Lookup {
        let mut out = L2Lookup::default();
        let found: StoreResult<()> = async {
            match target {
                Target::Version(v) => {
                    let snap = match expected {
                        Some(e) => l2.get_snapshot(table, &segment_id(e).digest()).await?,
                        None => l2.snapshot_at_version(table, *v).await?,
                    };
                    out.hit = snap.filter(|s| s.version == *v).map(|snap| L2Hit {
                        snap,
                        latest_age: None,
                    });
                }
                Target::Latest { max_staleness } => {
                    let Some(rec) = l2.get_latest(table, "").await? else {
                        return Ok(());
                    };
                    out.latest_seen = Some(rec.clone());
                    if !rec.tail_id.is_empty() {
                        return Ok(());
                    }
                    let age_ms = (now_ms() - rec.confirmed_at_ms).max(0);
                    let newer_than_baseline = match waiting {
                        Some(None) => true,
                        Some(Some(b)) => rec.confirmed_at_ms > b.confirmed_at_ms,
                        None => false,
                    };
                    let fresh = age_ms as u128 <= max_staleness.as_millis() || newer_than_baseline;
                    let snap = l2.get_snapshot(table, &rec.seg_id).await?;
                    if fresh {
                        out.hit = snap.map(|snap| L2Hit {
                            snap,
                            latest_age: Some(Duration::from_millis(age_ms as u64)),
                        });
                    } else {
                        out.stale_base = snap;
                    }
                }
                Target::CatalogLatest {
                    max_catalog_version,
                    tail_id,
                    tail_has_staged: false,
                    ..
                } => {
                    let Some(rec) = l2.get_latest(table, &tail_digest(tail_id)).await? else {
                        return Ok(());
                    };
                    out.latest_seen = Some(rec.clone());
                    if rec.version != *max_catalog_version || rec.tail_id != tail_digest(tail_id) {
                        return Ok(());
                    }
                    out.hit = l2
                        .get_snapshot(table, &rec.seg_id)
                        .await?
                        .filter(|s| expected.is_none_or(|e| segment_id(e).digest() == s.seg_id))
                        .map(|snap| L2Hit {
                            snap,
                            latest_age: None,
                        });
                }
                // Staged tails never reach L2.
                Target::CatalogLatest { .. } => {}
            }
            Ok(())
        }
        .await;
        if let Err(e) = found {
            self.l2_error("lookup", &e);
        }
        out
    }

    /// Rebuilds a native instance from an L2 entry with zero ops and checks that it is the entry's
    /// segment.
    async fn instance_from_l2(
        &self,
        key: &TableKey,
        location: &str,
        snap: &StoredSnapshot<Vec<u8>>,
        catalog_tail: Option<TailId>,
    ) -> Result<Arc<Instance>, String> {
        let hint = snap
            .payload
            .clone()
            .ok_or("entry has no snapshot hint (not encodable losslessly)")?;
        let root = location.to_string();
        let version = snap.version;
        let snapshot =
            tokio::task::spawn_blocking(move || crate::hint::rebuild(&hint, &root, version))
                .await
                .map_err(|e| format!("hint rebuild panicked: {e}"))??;
        let rebuilt = segment_id(&log_segment_info(snapshot.log_segment())).digest();
        if rebuilt != snap.seg_id
            || snapshot.version() as i64 != snap.version
            || has_staged_commits(&snapshot)
        {
            return Err(format!(
                "hint rebuilt segment {rebuilt} at version {}, entry is {} at {}",
                snapshot.version(),
                snap.seg_id,
                snap.version
            ));
        }
        Ok(Arc::new(Instance::new(
            snapshot,
            snap.info.clone(),
            catalog_tail,
            l2_table_key(&key.0, &key.1),
            self.tick(),
        )))
    }

    /// Writes a fresh, cacheable build result to L2 (never staged-tail or tainted results).
    async fn l2_write_build(
        &self,
        l2: &L2,
        table: &str,
        instance: &Instance,
        hint: Option<Result<Vec<u8>, String>>,
        target: &Target,
        started_ms: i64,
    ) {
        let hint = match hint {
            Some(Ok(h)) => Some(h),
            Some(Err(why)) => {
                self.counters
                    .l2_hint_unencodable
                    .fetch_add(1, Ordering::Relaxed);
                tracing::info!(reason = %why, "snapshot hint not stored in L2; info and plans only");
                None
            }
            None => None,
        };
        let now = now_ms();
        let snap = StoredSnapshot {
            seg_id: instance.seg_hex.clone(),
            version: instance.version(),
            info: instance.info.clone(),
            payload: hint,
            built_at_ms: now,
            confirmed_at_ms: now,
        };
        if let Err(e) = l2.put_snapshot(table, &snap).await {
            self.l2_error("put snapshot", &e);
            return;
        }
        self.l2_write_latest(l2, table, instance, target, started_ms)
            .await;
    }

    async fn l2_write_latest(
        &self,
        l2: &L2,
        table: &str,
        instance: &Instance,
        target: &Target,
        confirmed_at_ms: i64,
    ) {
        let (tail_id, staleness) = match target {
            Target::Latest { max_staleness } => (String::new(), *max_staleness),
            Target::CatalogLatest {
                tail_id,
                tail_has_staged: false,
                ..
            } => (tail_digest(tail_id), Duration::ZERO),
            _ => return,
        };
        let rec = LatestRecord {
            seg_id: instance.seg_hex.clone(),
            version: instance.version(),
            confirmed_at_ms,
            tail_id,
        };
        if let Err(e) = l2.put_latest(table, &rec, staleness).await {
            self.l2_error("put latest", &e);
        }
    }

    fn spawn_build(self: &Arc<Self>, job: Arc<Job<BuildOutcome>>, inputs: BuildInputs) {
        let BuildInputs {
            key,
            location,
            target,
            build_key,
            expected,
            base,
            io,
        } = inputs;
        let core = self.clone();
        tokio::spawn(async move {
            let channel = job.channel.clone();
            let (catalog_tail, staged_tail) = match &target {
                Target::CatalogLatest {
                    tail_id,
                    tail_has_staged,
                    ..
                } => (Some(tail_id.clone()), *tail_has_staged),
                _ => (None, false),
            };
            let is_latest = matches!(target, Target::Latest { .. });
            let is_relist = is_latest && base.is_some();
            // Confirmation time of a latest result: when its listing started, not when it ended.
            let started = Instant::now();
            let started_ms = now_ms();
            let l2_table = l2_table_key(&key.0, &key.1);
            // Staged-tail targets have no build key and never touch L2.
            let l2 = core.l2.clone().filter(|_| build_key.is_some());
            let mut base = base;
            let mut lock: Option<(String, String)> = None;

            if let Some(l2) = &l2 {
                let mut found = core
                    .l2_lookup(l2.as_ref(), &l2_table, &target, expected.as_ref(), None)
                    .await;
                if found.hit.is_none()
                    && let Some((_, bt)) = &build_key
                {
                    // Cross-replica single flight (SPEC v10.3 `lock`): one replica builds, the
                    // others poll L2 for its result until the build deadline.
                    let lock_target = bt.lock_target();
                    let owner = format!(
                        "{}:{}",
                        core.instance_id,
                        core.job_seq.fetch_add(1, Ordering::Relaxed)
                    );
                    let deadline = core.config.l2_build_deadline;
                    match l2.try_lock(&l2_table, &lock_target, &owner, deadline).await {
                        Ok(true) => lock = Some((lock_target, owner)),
                        Ok(false) => {
                            core.counters.l2_lock_waits.fetch_add(1, Ordering::Relaxed);
                            let baseline = found.latest_seen.clone();
                            let until = Instant::now() + deadline;
                            while Instant::now() < until && channel.cancel_reason().is_none() {
                                tokio::time::sleep(L2_POLL_INTERVAL).await;
                                found = core
                                    .l2_lookup(
                                        l2.as_ref(),
                                        &l2_table,
                                        &target,
                                        expected.as_ref(),
                                        Some(baseline.as_ref()),
                                    )
                                    .await;
                                if found.hit.is_some() {
                                    break;
                                }
                            }
                        }
                        Err(e) => core.l2_error("lock", &e),
                    }
                }
                if let Some(hit) = found.hit.take() {
                    match core
                        .instance_from_l2(&key, &location, &hit.snap, catalog_tail.clone())
                        .await
                    {
                        Ok(inst) => {
                            core.counters.l2_hits.fetch_add(1, Ordering::Relaxed);
                            let inst = core.l1.insert(&key, inst);
                            if is_latest {
                                let age = hit.latest_age.unwrap_or_default();
                                let at = Instant::now().checked_sub(age).unwrap_or(started);
                                core.l1.confirm_latest(&key, inst.clone(), at);
                            }
                            if let Some((t, owner)) = &lock
                                && let Err(e) = l2.unlock(&l2_table, t, owner).await
                            {
                                core.l2_error("unlock", &e);
                            }
                            if let Some(bk) = &build_key {
                                core.l1.state.lock().unwrap().inflight.remove(bk);
                            }
                            job.complete(Ok(BuildOutcome {
                                instance: inst,
                                kind: proto::ResolveKind::CacheHit,
                            }));
                            return;
                        }
                        Err(why) => {
                            tracing::info!(reason = %why, "L2 entry not usable; building");
                            core.counters.l2_misses.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                } else {
                    core.counters.l2_misses.fetch_add(1, Ordering::Relaxed);
                }
                // An L2 latest older than the staleness bound is still a base to re-list from.
                if base.is_none()
                    && is_latest
                    && let Some(stale) = &found.stale_base
                {
                    match core.instance_from_l2(&key, &location, stale, None).await {
                        Ok(inst) => base = Some(inst),
                        Err(why) => tracing::info!(reason = %why, "stale L2 latest not usable"),
                    }
                }
            }

            if !is_relist {
                core.counters.builds_started.fetch_add(1, Ordering::Relaxed);
            }
            let encode_hint = l2.is_some() && !staged_tail;
            let built: Result<Result<Built, BuildFailure>, ServerError> = match io.get().await {
                Err(e) => Err(e),
                Ok(io) => {
                    let location = location.clone();
                    let target = target.clone();
                    let channel = channel.clone();
                    tokio::task::spawn_blocking(move || {
                        build_blocking(
                            channel,
                            io,
                            &location,
                            &target,
                            base,
                            is_latest,
                            encode_hint,
                        )
                    })
                    .await
                    .map_err(|join_err| {
                        ServerError::internal(format!("snapshot build panicked: {join_err}"))
                    })
                }
            };
            let result = core.finish_build(
                built,
                &job.channel,
                &key,
                catalog_tail,
                staged_tail,
                is_latest,
                started,
            );
            // Write to L2 before releasing the lock, so replicas waiting on it find the result.
            if let (Some(l2), Ok((outcome, write))) = (&l2, &result) {
                match write {
                    L2Write::Build(hint) => {
                        core.l2_write_build(
                            l2.as_ref(),
                            &l2_table,
                            &outcome.instance,
                            hint.clone(),
                            &target,
                            started_ms,
                        )
                        .await
                    }
                    L2Write::Renewal => {
                        if let Err(e) = l2
                            .confirm_snapshot(&l2_table, &outcome.instance.seg_hex, now_ms())
                            .await
                        {
                            core.l2_error("confirm", &e);
                        }
                        core.l2_write_latest(
                            l2.as_ref(),
                            &l2_table,
                            &outcome.instance,
                            &target,
                            started_ms,
                        )
                        .await;
                    }
                    L2Write::None => {}
                }
            }
            if let (Some(l2), Some((t, owner))) = (&l2, &lock)
                && let Err(e) = l2.unlock(&l2_table, t, owner).await
            {
                core.l2_error("unlock", &e);
            }
            let result = result.map(|(o, _)| o);
            if is_relist {
                let counter = match &result {
                    Ok(BuildOutcome {
                        kind: proto::ResolveKind::CacheHit,
                        ..
                    }) => &core.counters.latest_renewals,
                    _ => &core.counters.builds_started,
                };
                counter.fetch_add(1, Ordering::Relaxed);
            }
            if let Some(bk) = &build_key {
                core.l1.state.lock().unwrap().inflight.remove(bk);
            }
            job.complete(result);
        });
    }

    /// Turns a finished build into the job's result and inserts cacheable instances into L1.
    #[allow(clippy::too_many_arguments)]
    fn finish_build(
        &self,
        built: Result<Result<Built, BuildFailure>, ServerError>,
        channel: &OpChannel,
        key: &TableKey,
        catalog_tail: Option<TailId>,
        staged_tail: bool,
        is_latest: bool,
        started: Instant,
    ) -> Result<(BuildOutcome, L2Write), ServerError> {
        match built? {
            // A deadline anywhere in the job fails it, even if kernel got past the op.
            Ok(_) if channel.cancel_reason().is_some() => Err(deadline_or_cancel(channel)),
            Ok(Built::Renewed(base)) => {
                let tainted = channel.is_tainted();
                // A base adopted from a stale L2 latest is not in L1 yet.
                let base = if tainted {
                    base
                } else {
                    let base = self.l1.insert(key, base);
                    self.l1.confirm_latest(key, base.clone(), started);
                    base
                };
                base.last_used.store(self.tick(), Ordering::Relaxed);
                let write = if tainted {
                    L2Write::None
                } else {
                    L2Write::Renewal
                };
                Ok((
                    BuildOutcome {
                        instance: base,
                        kind: proto::ResolveKind::CacheHit,
                    },
                    write,
                ))
            }
            // A getter failure caused by a client storage error is a failed op, not a property
            // of the table.
            Ok(Built::New(_, info, _, _))
                if info.clustering_error.is_some() && channel.first_op_failure().is_some() =>
            {
                let op_err = channel.first_op_failure();
                Err(local_unavailable(
                    channel,
                    classify_kernel_error(
                        &delta_kernel::KernelError::generic(
                            info.clustering_error.map(|e| e.message).unwrap_or_default(),
                        ),
                        channel.cancel_reason(),
                        op_err.as_ref(),
                    ),
                ))
            }
            Ok(Built::New(snapshot, info, kind, hint)) => {
                let cacheable =
                    !channel.is_tainted() && !staged_tail && !has_staged_commits(&snapshot);
                let instance = Arc::new(Instance::new(
                    snapshot,
                    *info,
                    catalog_tail,
                    l2_table_key(&key.0, &key.1),
                    self.tick(),
                ));
                if cacheable {
                    let inserted = self.l1.insert(key, instance);
                    if is_latest {
                        self.l1.confirm_latest(key, inserted.clone(), started);
                    }
                    Ok((
                        BuildOutcome {
                            instance: inserted,
                            kind,
                        },
                        L2Write::Build(hint),
                    ))
                } else {
                    tracing::info!(
                        table = %key.0,
                        tainted = channel.is_tainted(),
                        "build result not cached"
                    );
                    Ok((BuildOutcome { instance, kind }, L2Write::None))
                }
            }
            Err(BuildFailure::Build(e)) => Err(local_unavailable(
                channel,
                classify_build_error(
                    &e,
                    channel.cancel_reason(),
                    channel.first_op_failure().as_ref(),
                ),
            )),
            Err(BuildFailure::Kernel(e)) => Err(local_unavailable(
                channel,
                classify_kernel_error(
                    &e,
                    channel.cancel_reason(),
                    channel.first_op_failure().as_ref(),
                ),
            )),
        }
    }

    /// Returns the plan for `spec` on `instance`, generating it (coalesced) if needed.
    async fn plan_for(
        self: &Arc<Self>,
        instance: &Arc<Instance>,
        spec: &proto::ScanPlanSpec,
        port: &Arc<StreamPort>,
        io: &IoSource,
    ) -> Result<proto::ScanPlanResult, ServerError> {
        let key = spec_hash(spec);
        let job = {
            let mut plans = instance.plans.lock().unwrap();
            match plans.get(&key) {
                Some(PlanSlot::Ready(r)) => {
                    let mut r = r.clone();
                    r.cache_hit = true;
                    return Ok(r);
                }
                Some(PlanSlot::InFlight(job)) => {
                    job.channel.join(port.clone());
                    job.clone()
                }
                None => {
                    let channel = OpChannel::new(
                        port.clone(),
                        None,
                        self.counters.clone(),
                        self.faults.clone(),
                    );
                    let job = Job::new(channel);
                    plans.insert(key.clone(), PlanSlot::InFlight(job.clone()));
                    self.spawn_plan(job.clone(), instance.clone(), spec.clone(), key, io.clone());
                    job
                }
            }
        };
        let result = job.wait().await;
        job.channel.leave(port);
        result
    }

    fn spawn_plan(
        self: &Arc<Self>,
        job: Arc<Job<proto::ScanPlanResult>>,
        instance: Arc<Instance>,
        spec: proto::ScanPlanSpec,
        key: String,
        io: IoSource,
    ) {
        let core = self.clone();
        tokio::spawn(async move {
            // Only instances in L1 (never staged-tail or tainted results) share plans via L2.
            let l2 = core
                .l2
                .clone()
                .filter(|_| instance.cached.load(Ordering::Relaxed));
            if let Some(l2) = &l2 {
                match l2.get_plan(&instance.seg_hex, &key).await {
                    Ok(Some(r)) => {
                        core.counters.l2_plan_hits.fetch_add(1, Ordering::Relaxed);
                        instance
                            .plans
                            .lock()
                            .unwrap()
                            .insert(key, PlanSlot::Ready(r.clone()));
                        job.complete(Ok(proto::ScanPlanResult {
                            cache_hit: true,
                            ..r
                        }));
                        return;
                    }
                    Ok(None) => {
                        core.counters.l2_plan_misses.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => core.l2_error("get plan", &e),
                }
            }
            let result = match io.get().await {
                Err(e) => Err(e),
                Ok(io) => {
                    let channel = job.channel.clone();
                    let snapshot = instance.snapshot.clone();
                    let outcome = tokio::task::spawn_blocking(move || {
                        let engine = PlanBasedEngine::new(
                            None,
                            Arc::new(RemotePlanExecutor::new(channel, io)),
                        );
                        generate_plan(&snapshot, &spec, &engine)
                    })
                    .await;
                    let channel = &job.channel;
                    match outcome {
                        Ok(PlanOutcome::Result(_)) if channel.cancel_reason().is_some() => {
                            Err(deadline_or_cancel(channel))
                        }
                        Ok(PlanOutcome::Result(r)) => Ok(r),
                        Ok(PlanOutcome::Failed(e)) => Err(local_unavailable(
                            channel,
                            classify_kernel_error(
                                &e,
                                channel.cancel_reason(),
                                channel.first_op_failure().as_ref(),
                            ),
                        )),
                        Err(join_err) => Err(ServerError::internal(format!(
                            "plan generation panicked: {join_err}"
                        ))),
                    }
                }
            };
            let tainted = job.channel.is_tainted();
            {
                let mut plans = instance.plans.lock().unwrap();
                match &result {
                    Ok(r) if !tainted => {
                        plans.insert(key.clone(), PlanSlot::Ready(r.clone()));
                    }
                    _ => {
                        plans.remove(&key);
                    }
                }
            }
            if let (Some(l2), Ok(r)) = (&l2, &result)
                && !tainted
            {
                match l2.put_plan(&instance.seg_hex, &key, r).await {
                    Ok(PlanPut::Stored) => {}
                    Ok(PlanPut::TooLarge(len)) => {
                        core.counters
                            .l2_plans_too_large
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::info!(compressed_bytes = len, "plan too large for L2; not stored");
                    }
                    Err(e) => core.l2_error("put plan", &e),
                }
            }
            job.complete(result);
        });
    }
}

/// The blocking part of a build: kernel snapshot build, `SnapshotInfo`, and (with L2) the
/// encoded hint.
fn build_blocking(
    channel: Arc<OpChannel>,
    io: Option<LocalCtx>,
    location: &str,
    target: &Target,
    base: Option<Arc<Instance>>,
    is_latest: bool,
    encode_hint: bool,
) -> Result<Built, BuildFailure> {
    let engine = PlanBasedEngine::new(None, Arc::new(RemotePlanExecutor::new(channel, io)));
    let kind = if base.is_some() {
        proto::ResolveKind::Incremental
    } else {
        proto::ResolveKind::FullBuild
    };
    let latest_base = base.clone().filter(|_| is_latest);
    let snapshot = match (target, base) {
        (Target::Version(v), Some(base)) => Snapshot::builder_from(base.snapshot.clone())
            .at_version(*v as u64)
            .build(&engine),
        (Target::Version(v), None) => Snapshot::builder_for(location)
            .at_version(*v as u64)
            .build(&engine),
        (
            Target::CatalogLatest {
                max_catalog_version,
                log_tail,
                ..
            },
            _,
        ) => Snapshot::builder_for(location)
            .with_max_catalog_version(*max_catalog_version as u64)
            .with_log_tail(log_tail.clone())
            .build(&engine),
        (Target::Latest { .. }, Some(base)) => {
            Snapshot::builder_from(base.snapshot.clone()).build(&engine)
        }
        (Target::Latest { .. }, None) => Snapshot::builder_for(location).build(&engine),
    }
    .map_err(BuildFailure::Build)?;
    // Latest re-listing found no new commit: keep serving the confirmed instance (and its plans)
    // without recomputing SnapshotInfo.
    if let Some(base) = latest_base
        && snapshot.version() == base.snapshot.version()
    {
        return Ok(Built::Renewed(base));
    }
    let info = compute_snapshot_info(&snapshot, &engine).map_err(BuildFailure::Kernel)?;
    let hint = (encode_hint && !has_staged_commits(&snapshot))
        .then(|| crate::hint::encode(&snapshot, location));
    Ok(Built::New(snapshot, Box::new(info), kind, hint))
}

/// SPEC v9.1: a job that failed because a server-side IO op failed is `UNAVAILABLE` (retryable),
/// not `OP_FAILED` (which reports client-side op failures).
fn local_unavailable(channel: &OpChannel, err: ServerError) -> ServerError {
    if err.code == Code::OpFailed && channel.first_failure_is_local() {
        ServerError::new(
            Code::Unavailable,
            err.message
                .replacen(OP_FAILED_PREFIX, LOCAL_IO_FAILED_PREFIX, 1),
        )
    } else {
        err
    }
}

/// The error for a job whose op channel was cancelled although kernel returned a result.
fn deadline_or_cancel(channel: &OpChannel) -> ServerError {
    classify_kernel_error(
        &delta_kernel::KernelError::Cancelled,
        channel.cancel_reason(),
        None,
    )
}

/// Table locations are directory URLs; make the trailing slash canonical for the cache key.
fn normalize_location(location: &str) -> Result<String, ServerError> {
    let mut url = url::Url::parse(location)
        .map_err(|e| ServerError::new(Code::TableNotFound, format!("bad location: {e}")))?;
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    Ok(url.to_string())
}

// Kernel objects are shared across request threads; make that requirement a compile error.
const _: () = {
    const fn assert_send_sync<T: Send + Sync + ?Sized>() {}
    assert_send_sync::<Snapshot>();
    assert_send_sync::<delta_kernel::scan::Scan>();
};
