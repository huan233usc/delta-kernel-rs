//! Snapshot cache, request coalescing, and request resolution (SPEC §7).
//!
//! - Cache entry ([`Instance`]) = `{Arc<Snapshot>, SnapshotInfo, plans: ScanPlanSpec -> result}`.
//!   `SnapshotInfo` is computed once at build time with the build's own op channel.
//! - Snapshot builds coalesce on `(table_id, location, target version)`; plan generation coalesces
//!   on `(instance, ScanPlanSpec)`. Every waiter's stream joins the job's [`OpChannel`] so it can
//!   take over the in-flight op if the driving stream goes away.
//! - Builds that saw a non-`FileNotFound` op error (tainted) and instances that contain staged
//!   commits are returned to the waiting requests but never inserted into the cache.
//! - Eviction: at most `max_instances_per_table` per table, then a global LRU bounded by
//!   `max_cache_bytes` of estimated memory.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use delta_kernel::engine::plans::PlanBasedEngine;
use delta_kernel::{LogPath, Snapshot, SnapshotRef};
use tokio::sync::watch;

use crate::errors::{ServerError, classify_build_error, classify_kernel_error};
use crate::executor::{OpChannel, RemotePlanExecutor, StreamPort};
use crate::info::{compute_snapshot_info, has_staged_commits};
use crate::plan::{PlanOutcome, generate_plan, spec_key};
use crate::proto::error::Code;
use crate::proto::{self, version_spec};
use crate::stats::{Faults, ServerCounters};

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

#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub max_instances_per_table: usize,
    pub max_cache_bytes: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_instances_per_table: 2,
            max_cache_bytes: 4 << 30,
        }
    }
}

type TableKey = (String, String);

/// Canonical identity of a catalog log tail: `(version, location, size)` sorted by version.
pub type TailId = Vec<(i64, String, i64)>;

/// In-flight build key: table, target version, and the catalog tail for `catalog_latest` builds
/// (`None` for exact-version builds).
type BuildKey = (TableKey, i64, Option<TailId>);

/// Canonicalizes a request's catalog log tail (SPEC v6 §7).
pub fn tail_id(log_tail: &[proto::FileStatus]) -> TailId {
    let mut id: TailId = log_tail
        .iter()
        .map(|f| {
            let version = f
                .location
                .rsplit('/')
                .next()
                .and_then(|name| name.get(..20))
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(-1);
            (version, f.location.clone(), f.size)
        })
        .collect();
    id.sort();
    id
}

/// A cached snapshot instance.
pub struct Instance {
    pub snapshot: SnapshotRef,
    pub info: proto::SnapshotInfo,
    plans: Mutex<HashMap<Vec<u8>, PlanSlot>>,
    last_used: AtomicU64,
    /// Canonical log tail of a `catalog_latest` build; `None` for exact-version builds.
    catalog_tail: Option<TailId>,
}

enum PlanSlot {
    Ready(proto::ScanPlanResult),
    InFlight(Arc<Job<proto::ScanPlanResult>>),
}

impl Instance {
    fn version(&self) -> i64 {
        self.snapshot.version() as i64
    }

    fn segment(&self) -> Option<&proto::LogSegmentInfo> {
        self.info.log_segment.as_ref()
    }

    /// Estimated memory: snapshot heap (includes a CRC's inline `allFiles`) plus plan bytes.
    fn estimated_bytes(&self) -> usize {
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
        self.snapshot.estimated_owned_heap_size_bytes() + plans
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

#[derive(Clone)]
struct BuildOutcome {
    instance: Arc<Instance>,
    kind: proto::ResolveKind,
}

#[derive(Default)]
struct CacheState {
    tables: HashMap<TableKey, Vec<Arc<Instance>>>,
    inflight: HashMap<BuildKey, Arc<Job<BuildOutcome>>>,
}

/// Server core shared by all streams.
pub struct Core {
    pub instance_id: String,
    pub config: CacheConfig,
    pub counters: Arc<ServerCounters>,
    pub faults: Arc<Faults>,
    state: Mutex<CacheState>,
    clock: AtomicU64,
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
    Latest,
}

impl Core {
    pub fn new(instance_id: String, config: CacheConfig) -> Arc<Self> {
        Arc::new(Self {
            instance_id,
            config,
            counters: Arc::default(),
            faults: Arc::default(),
            state: Mutex::default(),
            clock: AtomicU64::new(0),
        })
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

    pub fn reset_cache(&self) {
        self.state.lock().unwrap().tables.clear();
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

    /// Resolves one GetSnapshot request whose stream is `port`.
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
        let location = normalize_location(&table.location)?;
        let table_id = if table.table_id.is_empty() {
            location.clone()
        } else {
            table.table_id.clone()
        };
        let key: TableKey = (table_id, location.clone());
        let target = match req.at.as_ref().and_then(|a| a.spec.as_ref()) {
            Some(version_spec::Spec::Version(v)) => Target::Version(*v),
            Some(version_spec::Spec::CatalogLatest(c)) => Target::CatalogLatest {
                max_catalog_version: c.max_catalog_version,
                tail_id: tail_id(&c.log_tail),
                tail_has_staged: c
                    .log_tail
                    .iter()
                    .any(|f| f.location.contains("/_staged_commits/")),
                log_tail: c
                    .log_tail
                    .iter()
                    .map(|f| {
                        crate::executor::file_meta_from_status(f)
                            .and_then(LogPath::try_new)
                            .map_err(|e| ServerError::internal(format!("bad log_tail entry: {e}")))
                    })
                    .collect::<Result<_, _>>()?,
            },
            Some(version_spec::Spec::Latest(_)) | None => Target::Latest,
        };

        let (instance, resolved_by) = match self.resolve(&key, &location, &target, &req, port) {
            Resolution::Hit(inst) => (inst, proto::ResolveKind::CacheHit),
            Resolution::Wait(job, coalesced) => {
                let outcome = job.wait().await;
                job.channel.leave(port);
                let outcome = outcome?;
                let kind = if coalesced {
                    proto::ResolveKind::Coalesced
                } else {
                    outcome.kind
                };
                (outcome.instance, kind)
            }
        };

        let mut plans = Vec::with_capacity(req.plans.len());
        for spec in &req.plans {
            plans.push(self.plan_for(&instance, spec, port).await?);
        }
        instance.last_used.store(self.tick(), Ordering::Relaxed);
        // Plans grew the instance; re-apply the memory bound.
        self.evict_global();

        Ok(proto::GetSnapshotResponse {
            version: instance.version(),
            info: Some(instance.info.clone()),
            plans,
            resolved_by: resolved_by as i32,
            kernel_rev: KERNEL_REV.to_string(),
            stats: Some(proto::ServerStats {
                ops_issued: port.ops_issued(),
                server_time_us: started.elapsed().as_micros() as i64,
                client_op_time_us: port.client_op_time_us(),
                instance_id: self.instance_id.clone(),
            }),
        })
    }

    /// Cache lookup or join/start a build. Never blocks on IO.
    fn resolve(
        self: &Arc<Self>,
        key: &TableKey,
        location: &str,
        target: &Target,
        req: &proto::GetSnapshotRequest,
        port: &Arc<StreamPort>,
    ) -> Resolution {
        let mut st = self.state.lock().unwrap();
        let expected = req.expected_log_segment.as_ref();
        // Exact-version requests hit only when the client's expected log segment is present and
        // equal to the cached one (SPEC v6: absent => miss). `catalog_latest` hits and coalesces
        // on (max_catalog_version, canonical log tail); a tail with staged commits is neither
        // served from cache nor coalesced. Path-based latest has no known target, so it is
        // neither coalesced nor served from cache (it must re-list to find new commits).
        let (target_version, build_key): (Option<i64>, Option<BuildKey>) = match target {
            Target::Version(v) => (Some(*v), Some((key.clone(), *v, None))),
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
                Some((key.clone(), *max_catalog_version, Some(tail_id.clone()))),
            ),
            Target::Latest => (None, None),
        };

        if let Some(bk) = &build_key {
            let (_, v, tail) = bk;
            let hit = st.tables.get(key).and_then(|insts| {
                insts.iter().find(|i| {
                    i.version() == *v
                        && match tail {
                            None => expected.is_some_and(|e| i.segment() == Some(e)),
                            Some(t) => {
                                i.catalog_tail.as_ref() == Some(t)
                                    && expected.is_none_or(|e| i.segment() == Some(e))
                            }
                        }
                })
            });
            if let Some(inst) = hit {
                let inst = inst.clone();
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
            Target::CatalogLatest { .. } | Target::Latest => None,
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
        self.counters.builds_started.fetch_add(1, Ordering::Relaxed);
        self.spawn_build(
            job.clone(),
            key.clone(),
            location.to_string(),
            target.clone(),
            build_key,
            base,
        );
        Resolution::Wait(job, false)
    }

    fn spawn_build(
        self: &Arc<Self>,
        job: Arc<Job<BuildOutcome>>,
        key: TableKey,
        location: String,
        target: Target,
        build_key: Option<BuildKey>,
        base: Option<Arc<Instance>>,
    ) {
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
            let built = tokio::task::spawn_blocking(move || {
                let engine = PlanBasedEngine::new(None, Arc::new(RemotePlanExecutor::new(channel)));
                let kind = if base.is_some() {
                    proto::ResolveKind::Incremental
                } else {
                    proto::ResolveKind::FullBuild
                };
                let snapshot = match (&target, base) {
                    (Target::Version(v), Some(base)) => {
                        Snapshot::builder_from(base.snapshot.clone())
                            .at_version(*v as u64)
                            .build(&engine)
                    }
                    (Target::Version(v), None) => Snapshot::builder_for(&location)
                        .at_version(*v as u64)
                        .build(&engine),
                    (
                        Target::CatalogLatest {
                            max_catalog_version,
                            log_tail,
                            ..
                        },
                        _,
                    ) => Snapshot::builder_for(&location)
                        .with_max_catalog_version(*max_catalog_version as u64)
                        .with_log_tail(log_tail.clone())
                        .build(&engine),
                    (Target::Latest, _) => Snapshot::builder_for(&location).build(&engine),
                }?;
                let info = compute_snapshot_info(&snapshot, &engine)?;
                Ok::<_, delta_kernel::Error>((snapshot, info, kind))
            })
            .await;
            let channel = &job.channel;
            let result = match built {
                // A getter failure caused by a client storage error is a failed op, not a
                // property of the table.
                Ok(Ok((_, info, _)))
                    if info.clustering_error.is_some() && channel.first_op_failure().is_some() =>
                {
                    let op_err = channel.first_op_failure();
                    Err(classify_kernel_error(
                        &delta_kernel::KernelError::generic(
                            info.clustering_error.map(|e| e.message).unwrap_or_default(),
                        ),
                        channel.cancel_reason(),
                        op_err.as_ref(),
                    ))
                }
                Ok(Ok((snapshot, info, kind))) => {
                    let cacheable =
                        !channel.is_tainted() && !staged_tail && !has_staged_commits(&snapshot);
                    let instance = Arc::new(Instance {
                        snapshot,
                        info,
                        plans: Mutex::default(),
                        last_used: AtomicU64::new(core.tick()),
                        catalog_tail,
                    });
                    let instance = if cacheable {
                        core.insert(&key, instance)
                    } else {
                        tracing::info!(
                            table = %key.0,
                            tainted = channel.is_tainted(),
                            "build result not cached"
                        );
                        instance
                    };
                    Ok(BuildOutcome { instance, kind })
                }
                Ok(Err(e)) => Err(classify_build_error(
                    &e,
                    channel.cancel_reason(),
                    channel.first_op_failure().as_ref(),
                )),
                Err(join_err) => Err(ServerError::internal(format!(
                    "snapshot build panicked: {join_err}"
                ))),
            };
            if let Some(bk) = &build_key {
                core.state.lock().unwrap().inflight.remove(bk);
            }
            job.complete(result);
        });
    }

    /// Inserts an instance, deduplicating on `(version, log_segment)` so an equal instance keeps
    /// its plan cache, then applies eviction. Returns the instance now in the cache.
    fn insert(&self, key: &TableKey, instance: Arc<Instance>) -> Arc<Instance> {
        let inserted = {
            let mut st = self.state.lock().unwrap();
            let insts = st.tables.entry(key.clone()).or_default();
            if let Some(existing) = insts.iter().find(|i| {
                i.version() == instance.version()
                    && i.segment() == instance.segment()
                    && i.catalog_tail == instance.catalog_tail
            }) {
                existing.clone()
            } else {
                insts.push(instance.clone());
                while insts.len() > self.config.max_instances_per_table.max(1) {
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
            if total <= self.config.max_cache_bytes || all.len() <= 1 {
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

    /// Returns the plan for `spec` on `instance`, generating it (coalesced) if needed.
    async fn plan_for(
        self: &Arc<Self>,
        instance: &Arc<Instance>,
        spec: &proto::ScanPlanSpec,
        port: &Arc<StreamPort>,
    ) -> Result<proto::ScanPlanResult, ServerError> {
        let key = spec_key(spec);
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
                    self.spawn_plan(job.clone(), instance.clone(), spec.clone(), key);
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
        key: Vec<u8>,
    ) {
        tokio::spawn(async move {
            let channel = job.channel.clone();
            let snapshot = instance.snapshot.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                let engine = PlanBasedEngine::new(None, Arc::new(RemotePlanExecutor::new(channel)));
                generate_plan(&snapshot, &spec, &engine)
            })
            .await;
            let channel = &job.channel;
            let result = match outcome {
                Ok(PlanOutcome::Result(r)) => Ok(r),
                Ok(PlanOutcome::Failed(e)) => Err(classify_kernel_error(
                    &e,
                    channel.cancel_reason(),
                    channel.first_op_failure().as_ref(),
                )),
                Err(join_err) => Err(ServerError::internal(format!(
                    "plan generation panicked: {join_err}"
                ))),
            };
            {
                let mut plans = instance.plans.lock().unwrap();
                match &result {
                    Ok(r) if !channel.is_tainted() => {
                        plans.insert(key, PlanSlot::Ready(r.clone()));
                    }
                    _ => {
                        plans.remove(&key);
                    }
                }
            }
            job.complete(result);
        });
    }
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
