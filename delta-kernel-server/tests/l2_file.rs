//! SPEC v12 file-backed L2 (`--l2-dir`) and warm starts (`--l2-preload`): the store contract shared
//! with the in-process and Redis L2, the on-disk layout, corrupt / foreign files, concurrent
//! writers, two server processes on one directory, and restarts that rehydrate L1 before the port
//! opens (authz still enforced).

mod common;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use delta_kernel_server::cache::{CacheConfig, Core, KERNEL_REV, l2_table_key};
use delta_kernel_server::file_store::FileStore;
use delta_kernel_server::info::{compute_snapshot_info, segment_id};
use delta_kernel_server::proto::error::Code;
use delta_kernel_server::proto::{self, ResolveKind, scan_plan_result};
use delta_kernel_server::store::{
    L2, LatestRecord, MAX_L2_PLAN_BYTES, PlanPut, SnapshotStore, StoredSnapshot, now_ms,
};

const FOREIGN_REV: &str = "0ther-kernel-rev";

fn info_of(fx: &Fixture, version: i64) -> proto::SnapshotInfo {
    compute_snapshot_info(&local_snapshot(fx, Some(version)), &local_engine()).unwrap()
}

fn plan_of(bytes: Vec<u8>) -> proto::ScanPlanResult {
    proto::ScanPlanResult {
        result: Some(scan_plan_result::Result::Plan(bytes)),
        cache_hit: false,
    }
}

fn hidden_files(root: &Path) -> Vec<String> {
    list_files(root)
        .into_iter()
        .filter(|p| p.split('/').any(|c| c.starts_with('.')))
        .collect()
}

fn file_l2(dir: &Path) -> Arc<FileStore> {
    Arc::new(FileStore::open(dir, KERNEL_REV, None).unwrap())
}

// ------------------------------------------------------------------------------------------------
// Store
// ------------------------------------------------------------------------------------------------

/// The store contract (same suite as L1, MemoryStore and Redis), monotonic latest with concurrent
/// writers on separate store instances (separate processes' view of one directory), the plan size
/// cap, the documented layout, and nothing of `lock` / `authz` on disk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_store_contract_layout_and_never_store_rules() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path(), KERNEL_REV, None).unwrap();
    assert_eq!(store.tier(), "l2-file");
    let fx = fixture("table-with-dv-small");
    let info = info_of(&fx, 1);
    store_contract(&store, &info, b"hint-bytes".to_vec()).await;
    let w1 = FileStore::open(dir.path(), KERNEL_REV, None).unwrap();
    let w2 = FileStore::open(dir.path(), KERNEL_REV, None).unwrap();
    let (s, a, b): (&L2, &L2, &L2) = (&store, &w1, &w2);
    latest_contract(s, &[s, a, b]).await;

    // > 4 MiB after compression: not written.
    let mut big = vec![0u8; MAX_L2_PLAN_BYTES + 1024];
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    for byte in big.iter_mut() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *byte = x as u8;
    }
    let seg = segment_id(info.log_segment.as_ref().unwrap()).digest();
    assert!(matches!(
        store.put_plan(&seg, "big", &plan_of(big)).await.unwrap(),
        PlanPut::TooLarge(n) if n > MAX_L2_PLAN_BYTES
    ));
    assert!(store.get_plan(&seg, "big").await.unwrap().is_none());

    // Layout: <dir>/<kernel_rev>/{snap,ver,latest,plan}/...; no lock or authz entries, no
    // temporary files left behind.
    let tk = l2_table_key("", &info.table_root);
    let files = list_files(dir.path());
    for want in [
        format!("{KERNEL_REV}/snap/{tk}/{seg}.bin"),
        format!("{KERNEL_REV}/ver/{tk}.json"),
        format!("{KERNEL_REV}/latest/{tk}.json"),
        format!("{KERNEL_REV}/plan/{seg}/spec-a.bin"),
        format!("{KERNEL_REV}/plan/{seg}/spec-e.bin"),
    ] {
        assert!(files.contains(&want), "{want} in {files:#?}");
    }
    assert!(
        files.iter().any(|f| f.ends_with("__tailA.json")),
        "catalog-tail pointer file: {files:#?}"
    );
    let dirs: HashSet<&str> = files.iter().map(|f| f.split('/').nth(1).unwrap()).collect();
    assert_eq!(dirs, HashSet::from(["snap", "ver", "latest", "plan"]));
    assert!(!files.iter().any(|f| f.contains("big")));
    assert!(hidden_files(dir.path()).is_empty());
    assert!(
        !files
            .iter()
            .any(|f| f.contains("lock") || f.contains("authz")),
        "{files:#?}"
    );
    // Locks and authz are per process: a new store on the same directory sees neither.
    let th = delta_kernel_server::creds::token_hash("tok");
    store.put_authz(&th, &tk, now_ms() + 60_000).await.unwrap();
    assert!(
        store
            .try_lock(&tk, "v1", "me:1", Duration::from_secs(30))
            .await
            .unwrap()
    );
    let fresh = FileStore::open(dir.path(), KERNEL_REV, None).unwrap();
    assert!(fresh.get_authz(&th, &tk).await.unwrap().is_none());
    assert!(
        fresh
            .try_lock(&tk, "v1", "other:2", Duration::from_secs(30))
            .await
            .unwrap()
    );
    assert_eq!(store.rejected() + w1.rejected() + w2.rejected(), 0);
}

/// With `--l2-ttl-secs`, `snap` and `plan` entries expire like the Redis keys; without it they
/// never do. `latest` pointers never expire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_store_ttl_is_opt_in() {
    let fx = fixture("table-with-dv-small");
    let info = info_of(&fx, 1);
    let tk = l2_table_key("", &info.table_root);
    let seg = segment_id(info.log_segment.as_ref().unwrap()).digest();
    let snap = StoredSnapshot {
        seg_id: seg.clone(),
        table_id: "t".into(),
        version: 1,
        info: info.clone(),
        payload: Some(b"hint".to_vec()),
        built_at_ms: now_ms(),
        confirmed_at_ms: now_ms(),
    };
    let rec = LatestRecord {
        seg_id: seg.clone(),
        version: 1,
        confirmed_at_ms: now_ms(),
        tail_id: String::new(),
    };
    for ttl in [Some(Duration::from_millis(200)), None] {
        let dir = tempfile::tempdir().unwrap();
        let store = FileStore::open(dir.path(), KERNEL_REV, ttl).unwrap();
        store.put_snapshot(&tk, &snap).await.unwrap();
        store
            .put_plan(&seg, "spec", &plan_of(vec![1; 10]))
            .await
            .unwrap();
        store.put_latest(&tk, &rec, Duration::ZERO).await.unwrap();
        assert!(store.get_snapshot(&tk, &seg).await.unwrap().is_some());
        tokio::time::sleep(Duration::from_millis(400)).await;
        let live = ttl.is_none();
        assert_eq!(
            store.get_snapshot(&tk, &seg).await.unwrap().is_some(),
            live,
            "{ttl:?}"
        );
        assert_eq!(
            store.snapshot_at_version(&tk, 1).await.unwrap().is_some(),
            live
        );
        assert_eq!(store.get_plan(&seg, "spec").await.unwrap().is_some(), live);
        assert!(
            store.get_latest(&tk, "").await.unwrap().is_some(),
            "{ttl:?}"
        );
        assert_eq!(store.scan().await.unwrap().expired, u64::from(!live));
        assert_eq!(store.rejected(), 0, "expiry is not corruption");
    }
}

/// Corrupt files and files of another kernel revision are misses (WARN, counted); the next write
/// replaces them. A store never reads another revision's directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_store_rejects_corrupt_and_foreign_files() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStore::open(dir.path(), KERNEL_REV, None).unwrap();
    let fx = fixture("table-with-dv-small");
    let info = info_of(&fx, 1);
    let tk = l2_table_key("", &info.table_root);
    let seg = segment_id(info.log_segment.as_ref().unwrap()).digest();
    let snap = StoredSnapshot {
        seg_id: seg.clone(),
        table_id: "t".into(),
        version: 1,
        info: info.clone(),
        payload: Some(b"hint".to_vec()),
        built_at_ms: now_ms(),
        confirmed_at_ms: now_ms(),
    };
    let rec = LatestRecord {
        seg_id: seg.clone(),
        version: 1,
        confirmed_at_ms: now_ms(),
        tail_id: String::new(),
    };
    let plan = plan_of(vec![9; 100]);
    store.put_snapshot(&tk, &snap).await.unwrap();
    store.put_plan(&seg, "spec", &plan).await.unwrap();
    store.put_latest(&tk, &rec, Duration::ZERO).await.unwrap();
    let root = store.root().to_path_buf();
    let snap_file = root.join(format!("snap/{tk}/{seg}.bin"));
    let plan_file = root.join(format!("plan/{seg}/spec.bin"));
    let latest_file = root.join(format!("latest/{tk}.json"));

    // A flipped byte, a truncated file, broken JSON.
    let mut b = std::fs::read(&snap_file).unwrap();
    let mid = b.len() / 2;
    b[mid] ^= 0xFF;
    std::fs::write(&snap_file, &b).unwrap();
    assert!(store.get_snapshot(&tk, &seg).await.unwrap().is_none());
    assert_eq!(store.rejected(), 1);
    assert!(store.snapshot_at_version(&tk, 1).await.unwrap().is_none());
    assert_eq!(store.rejected(), 2);
    let b = std::fs::read(&plan_file).unwrap();
    std::fs::write(&plan_file, &b[..b.len() - 7]).unwrap();
    assert!(store.get_plan(&seg, "spec").await.unwrap().is_none());
    assert_eq!(store.rejected(), 3);
    std::fs::write(&latest_file, br#"{"format":1,"kernel_rev":"#).unwrap();
    assert!(store.get_latest(&tk, "").await.unwrap().is_none());
    assert_eq!(store.rejected(), 4);

    // Another kernel revision writes its own tree, which this store never reads...
    let other = FileStore::open(dir.path(), FOREIGN_REV, None).unwrap();
    let foreign = StoredSnapshot {
        seg_id: "seg-foreign".into(),
        ..snap.clone()
    };
    other.put_snapshot(&tk, &foreign).await.unwrap();
    other
        .put_latest("tk-foreign", &rec, Duration::ZERO)
        .await
        .unwrap();
    other.put_plan("seg-foreign", "spec", &plan).await.unwrap();
    assert!(
        store
            .get_snapshot(&tk, "seg-foreign")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(store.rejected(), 4, "other revision's tree not read");
    // ...and its files copied into this revision's tree are misses too.
    for rel in [
        format!("snap/{tk}/seg-foreign.bin"),
        "latest/tk-foreign.json".to_string(),
        "plan/seg-foreign/spec.bin".to_string(),
    ] {
        let to = root.join(&rel);
        std::fs::create_dir_all(to.parent().unwrap()).unwrap();
        std::fs::copy(other.root().join(&rel), to).unwrap();
    }
    assert!(
        store
            .get_snapshot(&tk, "seg-foreign")
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.get_latest("tk-foreign", "").await.unwrap().is_none());
    assert!(
        store
            .get_plan("seg-foreign", "spec")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(store.rejected(), 7);
    // A file at another key's path is rejected as well.
    std::fs::copy(&plan_file, root.join(format!("plan/{seg}/moved.bin"))).unwrap();
    assert!(store.get_plan(&seg, "moved").await.unwrap().is_none());
    assert_eq!(store.rejected(), 8);

    // The next write replaces a rejected file.
    store.put_snapshot(&tk, &snap).await.unwrap();
    store.put_plan(&seg, "spec", &plan).await.unwrap();
    store.put_latest(&tk, &rec, Duration::ZERO).await.unwrap();
    assert_eq!(
        store.get_snapshot(&tk, &seg).await.unwrap().unwrap().info,
        info
    );
    assert_eq!(
        store.get_plan(&seg, "spec").await.unwrap().unwrap().result,
        plan.result
    );
    assert_eq!(store.get_latest(&tk, "").await.unwrap().unwrap(), rec);

    // A scan returns the valid entries only and names the other revision.
    let scan = store.scan().await.unwrap();
    let snaps: Vec<_> = scan
        .snapshots
        .iter()
        .map(|(t, s)| (t.clone(), s.seg_id.clone()))
        .collect();
    assert_eq!(snaps, vec![(tk.clone(), seg.clone())]);
    assert_eq!(scan.latest.len(), 1);
    assert_eq!(scan.other_revisions, vec![FOREIGN_REV.to_string()]);
}

/// Concurrent writers to the same keys, across three store instances on one directory (as three
/// processes would be): every read sees a whole entry, the result is one writer's value, latest is
/// still monotonic, and no temporary file is left.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn file_store_concurrent_writers_same_key() {
    let dir = tempfile::tempdir().unwrap();
    let stores: Vec<Arc<FileStore>> = (0..3).map(|_| file_l2(dir.path())).collect();
    let fx = fixture("table-with-dv-small");
    let info = info_of(&fx, 1);
    let tk = l2_table_key("", &info.table_root);
    let seg = segment_id(info.log_segment.as_ref().unwrap()).digest();
    let t0 = now_ms();
    // Payload / plan of writer `i`: `i` repeated, a length that encodes `i`.
    let whole = |v: &[u8], base: usize| {
        !v.is_empty() && v.len() == base + v[0] as usize && v.iter().all(|b| *b == v[0])
    };
    let tasks: Vec<_> = (0..60i64)
        .map(|i| {
            let s = stores[i as usize % 3].clone();
            let (tk, seg, info) = (tk.clone(), seg.clone(), info.clone());
            tokio::spawn(async move {
                let snap = StoredSnapshot {
                    seg_id: seg.clone(),
                    table_id: format!("writer-{i}"),
                    version: 1,
                    info,
                    payload: Some(vec![i as u8; 1000 + i as usize]),
                    built_at_ms: t0 + i,
                    confirmed_at_ms: t0 + i,
                };
                s.put_snapshot(&tk, &snap).await.unwrap();
                s.confirm_snapshot(&tk, &seg, t0 + 1000 + i).await.unwrap();
                s.put_plan(
                    &seg,
                    "same-spec",
                    &plan_of(vec![i as u8; 5000 + i as usize]),
                )
                .await
                .unwrap();
                let rec = LatestRecord {
                    seg_id: seg.clone(),
                    version: i / 6,
                    confirmed_at_ms: t0 + i,
                    tail_id: String::new(),
                };
                s.put_latest(&tk, &rec, Duration::ZERO).await.unwrap();
                // Interleaved readers see whole entries.
                let got = s
                    .get_snapshot(&tk, &seg)
                    .await
                    .unwrap()
                    .expect("whole snap");
                let p = got.payload.unwrap();
                assert!(p.len() == 1000 + p[0] as usize && p.iter().all(|b| *b == p[0]));
                assert_eq!(got.table_id, format!("writer-{}", p[0]));
                let plan = s
                    .get_plan(&seg, "same-spec")
                    .await
                    .unwrap()
                    .expect("whole plan");
                let Some(scan_plan_result::Result::Plan(bytes)) = plan.result else {
                    panic!("plan bytes");
                };
                assert!(bytes.len() == 5000 + bytes[0] as usize);
                assert!(s.get_latest(&tk, "").await.unwrap().unwrap().version <= 9);
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    let fresh = file_l2(dir.path());
    let got = fresh.get_snapshot(&tk, &seg).await.unwrap().unwrap();
    assert!(whole(got.payload.as_ref().unwrap(), 1000));
    assert_eq!(
        got.table_id,
        format!("writer-{}", got.payload.as_ref().unwrap()[0])
    );
    let Some(scan_plan_result::Result::Plan(bytes)) = fresh
        .get_plan(&seg, "same-spec")
        .await
        .unwrap()
        .unwrap()
        .result
    else {
        panic!("plan bytes");
    };
    assert!(whole(&bytes, 5000));
    let latest = fresh.get_latest(&tk, "").await.unwrap().unwrap();
    assert_eq!(
        (latest.version, latest.confirmed_at_ms),
        (9, t0 + 59),
        "monotonic across writers: {latest:?}"
    );
    assert_eq!(
        fresh
            .snapshot_at_version(&tk, 1)
            .await
            .unwrap()
            .unwrap()
            .seg_id,
        seg
    );
    let rejected: u64 = stores.iter().map(|s| s.rejected()).sum::<u64>() + fresh.rejected();
    assert_eq!(rejected, 0, "no torn file was ever read");
    assert!(hidden_files(dir.path()).is_empty());
    let scan = fresh.scan().await.unwrap();
    assert_eq!(
        (scan.snapshots.len(), scan.latest.len(), scan.plans.len()),
        (1, 1, 1)
    );
}

// ------------------------------------------------------------------------------------------------
// Preload, in process
// ------------------------------------------------------------------------------------------------

async fn seed(store: Arc<FileStore>, fxs: &[&Fixture]) -> Vec<proto::GetSnapshotResponse> {
    let server = TestServer::start_l2("seed", store).await;
    let mut c = server.client().await;
    let mut out = vec![];
    for fx in fxs {
        let (r, _) = run(
            &mut c,
            latest_request(fx, 0, default_specs()),
            Behavior::default(),
        )
        .await
        .ok();
        out.push(r);
    }
    out
}

async fn warm_core(
    store: &Arc<FileStore>,
    max: Option<usize>,
) -> (Arc<Core>, delta_kernel_server::cache::PreloadReport) {
    let l2: Arc<L2> = store.clone();
    let core = Core::with_l2("warm".into(), CacheConfig::default(), Some(l2));
    let report = core.preload(store, max).await.unwrap();
    (core, report)
}

/// Preload rehydrates snapshots, plans and latest pointers into L1: the first requests are L1
/// hits (no L2 lookup, no build), the latest one re-lists once. Corrupt and foreign entries are
/// skipped (and rebuilt on demand); `max_entries` caps the instances.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preload_rehydrates_l1_and_skips_bad_entries() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b, c) = (
        fixture("table-with-dv-small"),
        fixture("crc-full"),
        fixture("v2-checkpoints-parquet-with-sidecars"),
    );
    let seeded = seed(file_l2(dir.path()), &[&a, &b, &c]).await;

    let store = file_l2(dir.path());
    let (core, report) = warm_core(&store, None).await;
    eprintln!("preload of 3 fixture tables: {report:?}");
    assert_eq!(
        (
            report.snapshots,
            report.latest,
            report.plans,
            report.rejected,
            report.skipped
        ),
        (3, 3, 6, 0, 0)
    );
    assert_eq!(report.entries(), 12);
    let server = TestServer::start_core(core, true).await;
    let s = server.stats().await;
    assert_eq!((s.l1_entries, s.builds_started), (3, 0));
    let mut cl = server.client().await;
    for (fx, first) in [&a, &b, &c].into_iter().zip(&seeded) {
        let (r, ops) = run(
            &mut cl,
            latest_request(fx, 0, default_specs()),
            Behavior::default(),
        )
        .await
        .ok();
        assert_eq!(resolved(&r), ResolveKind::CacheHit);
        assert_eq!(op_counts(&r), (0, 1), "only the renewal listing");
        assert!(ops.is_empty());
        assert_eq!(r.info, first.info);
        assert!(r.prefetched_plans.iter().all(|p| p.cache_hit));
        for (p, q) in r.prefetched_plans.iter().zip(&first.prefetched_plans) {
            assert_eq!(p.result, q.result);
        }
    }
    // Each renewal looked for a fresher L2 latest (a miss at staleness 0); nothing was read from
    // or built past L1.
    let s = server.stats().await;
    assert_eq!(
        (s.builds_started, s.latest_renewals, s.l2_hits, s.l2_misses),
        (0, 3, 0, 3),
        "served from L1"
    );
    assert_parity(&a, &seeded[0], &default_specs());
    drop(server);

    // Corrupt b's snapshot; replace c's with another kernel revision's file (and its pointer).
    let root = store.root().to_path_buf();
    let seg_of = |r: &proto::GetSnapshotResponse| r.r#ref.as_ref().unwrap().segment_id.clone();
    let tk_of = |fx: &Fixture| l2_table_key(&fx.table_id, fx.url.as_str());
    let b_snap = root.join(format!("snap/{}/{}.bin", tk_of(&b), seg_of(&seeded[1])));
    let mut bytes = std::fs::read(&b_snap).unwrap();
    let n = bytes.len();
    bytes[n / 3] ^= 0x55;
    std::fs::write(&b_snap, bytes).unwrap();
    let other = Arc::new(FileStore::open(dir.path(), FOREIGN_REV, None).unwrap());
    seed(other.clone(), &[&c]).await;
    for rel in [
        format!("snap/{}/{}.bin", tk_of(&c), seg_of(&seeded[2])),
        format!("latest/{}.json", tk_of(&c)),
    ] {
        std::fs::copy(other.root().join(&rel), root.join(&rel)).unwrap();
    }
    let store = file_l2(dir.path());
    let (core, report) = warm_core(&store, None).await;
    assert_eq!(
        (
            report.snapshots,
            report.latest,
            report.plans,
            report.rejected
        ),
        (1, 1, 2, 3),
        "b snap corrupt; c snap and pointer foreign: {report:?}"
    );
    assert_eq!(report.other_revisions, vec![FOREIGN_REV.to_string()]);
    let server = TestServer::start_core(core, true).await;
    let mut cl = server.client().await;
    let (ra, _) = run(&mut cl, latest_request(&a, 0, vec![]), Behavior::default())
        .await
        .ok();
    assert_eq!(resolved(&ra), ResolveKind::CacheHit);
    for (fx, first) in [(&b, &seeded[1]), (&c, &seeded[2])] {
        let (r, _) = run(&mut cl, latest_request(fx, 0, vec![]), Behavior::default())
            .await
            .ok();
        assert_eq!(resolved(&r), ResolveKind::FullBuild, "bad entry: rebuilt");
        assert_eq!(r.info, first.info);
    }
    drop(server);
    // The rebuilds replaced the bad files: everything rehydrates again. Then the cap.
    let store = file_l2(dir.path());
    let (_, report) = warm_core(&store, None).await;
    assert_eq!(
        (report.snapshots, report.latest, report.rejected),
        (3, 3, 0)
    );
    let (core, report) = warm_core(&store, Some(1)).await;
    assert_eq!((report.snapshots, report.skipped), (1, 2), "{report:?}");
    assert_eq!(core.l1.stats().entries, 1);
}

/// Per table, at most `max_instances_per_table` instances are rehydrated, the latest-pointed one
/// first. A `catalog_latest` pointer stays on the L2 path (only its tail digest is stored): the
/// first such request is still a zero-op CACHE_HIT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preload_per_table_bound_and_catalog_latest() {
    let dir = tempfile::tempdir().unwrap();
    let fx = fixture("table-with-dv-small");
    let cm = cm_fixture();
    cm_publish(&cm, 1);
    let tail = vec![cm_publish(&cm, 2)];
    let (v0, v1, cm2) = {
        let server = TestServer::start_l2("seed", file_l2(dir.path())).await;
        let mut c = server.client().await;
        let mut got = vec![];
        for req in [
            request(&fx, 0, vec![spec_json()]),
            request(&fx, 1, vec![spec_json()]),
            latest_request(&fx, 0, vec![]),
            cm_request(&cm, 2, &tail),
        ] {
            got.push(run(&mut c, req, Behavior::default()).await.ok().0);
        }
        assert_eq!(got[2].version, 1);
        assert_eq!(resolved(&got[3]), ResolveKind::FullBuild);
        (got[0].clone(), got[1].clone(), got[3].clone())
    };
    let store = file_l2(dir.path());
    let l2: Arc<L2> = store.clone();
    let core = Core::with_l2(
        "warm".into(),
        CacheConfig {
            max_instances_per_table: 1,
            ..CacheConfig::default()
        },
        Some(l2),
    );
    let report = core.preload(&store, None).await.unwrap();
    assert_eq!(
        (
            report.snapshots,
            report.latest,
            report.skipped,
            report.catalog_latest_in_l2
        ),
        (2, 1, 1, 1),
        "{report:?}"
    );
    let server = TestServer::start_core(core, true).await;
    let mut c = server.client().await;
    // The latest-pointed v1 was kept, v0 skipped.
    let (r, _) = run(
        &mut c,
        request_with_segment(&fx, 1, vec![spec_json()], &v1),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r), ResolveKind::CacheHit);
    assert_eq!(op_counts(&r), (0, 0));
    assert!(r.prefetched_plans[0].cache_hit);
    let s = server.stats().await;
    assert_eq!((s.l2_hits, s.builds_started), (0, 0));
    let (r, _) = run(&mut c, cm_request(&cm, 2, &tail), Behavior::default())
        .await
        .ok();
    assert_eq!(resolved(&r), ResolveKind::CacheHit);
    assert_eq!(op_counts(&r), (0, 0));
    assert_eq!(r.info, cm2.info);
    assert_eq!(server.stats().await.l2_hits, 1, "catalog_latest via L2");
    let (r, _) = run(
        &mut c,
        request_with_segment(&fx, 0, vec![], &v0),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r), ResolveKind::CacheHit, "skipped v0: an L2 hit");
    assert_eq!(op_counts(&r), (0, 0));
}

// ------------------------------------------------------------------------------------------------
// Server processes
// ------------------------------------------------------------------------------------------------

struct ServerProc {
    child: Child,
    addr: SocketAddr,
    log: PathBuf,
    _dir: tempfile::TempDir,
}

fn server_command(instance_id: &str, l2_dir: &Path, extra: &[&str]) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_delta-kernel-server"));
    cmd.args(["--enable-test-hooks", "--instance-id", instance_id])
        .arg("--l2-dir")
        .arg(l2_dir)
        .args(extra);
    cmd
}

impl ServerProc {
    /// Starts a server on a free port and waits for its endpoint file.
    async fn start(instance_id: &str, l2_dir: &Path, extra: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let endpoint = dir.path().join("endpoint");
        let log = dir.path().join("stderr.log");
        let mut child = server_command(instance_id, l2_dir, extra)
            .args(["--listen", "127.0.0.1:0"])
            .arg("--endpoint-file")
            .arg(&endpoint)
            .stderr(std::fs::File::create(&log).unwrap())
            .spawn()
            .unwrap();
        for _ in 0..800 {
            if let Ok(s) = std::fs::read_to_string(&endpoint) {
                return Self {
                    child,
                    addr: s.trim().parse().unwrap(),
                    log,
                    _dir: dir,
                };
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let _ = child.kill();
        let _ = child.wait();
        panic!(
            "server {instance_id} did not start: {}",
            std::fs::read_to_string(&log).unwrap_or_default()
        );
    }

    async fn stats(&self) -> proto::DebugStats {
        connect(self.addr)
            .await
            .get_debug_stats(proto::DebugStatsRequest {})
            .await
            .unwrap()
            .into_inner()
    }

    /// SIGTERM, wait, and return the server's log.
    fn stop(mut self) -> String {
        let _ = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status();
        let _ = self.child.wait();
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// `s` without ANSI escape sequences (`ESC [ ... letter`).
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// `name=value` of a tracing log line.
fn log_field<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    let at = line.find(&format!(" {name}="))? + name.len() + 2;
    line[at..].split_whitespace().next()
}

/// Starts a `--l2-preload` server on a fixed port and connects as soon as it accepts. Returns the
/// process, the time from spawn to the first accepted connection, and the preload log line.
async fn start_preloaded(
    instance_id: &str,
    l2_dir: &Path,
    delay_ms: u64,
    extra: &[&str],
) -> (ServerProc, Duration, String) {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("stderr.log");
    let port = free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let delay = delay_ms.to_string();
    let spawned = Instant::now();
    let mut child = server_command(instance_id, l2_dir, extra)
        .args(["--listen", &addr.to_string(), "--l2-preload"])
        .args(["--test-preload-delay-ms", &delay])
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&log).unwrap())
        .spawn()
        .unwrap();
    let mut accepted = None;
    let mut refused = 0;
    while spawned.elapsed() < Duration::from_secs(60) {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(_) => {
                accepted = Some(spawned.elapsed());
                break;
            }
            Err(_) => refused += 1,
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!(
                "server exited {status}: {}",
                std::fs::read_to_string(&log).unwrap_or_default()
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let accepted = accepted.expect("server never accepted");
    assert!(
        refused > 0,
        "the port was not open while the server started"
    );
    let logs = strip_ansi(&std::fs::read_to_string(&log).unwrap());
    let preload_at = logs.find("L2 preload finished").expect("preload log line");
    let listening_at = logs
        .find("delta-kernel-server listening")
        .expect("listening log line");
    assert!(preload_at < listening_at, "preload finished before bind");
    let line = logs[preload_at..].lines().next().unwrap().to_string();
    (
        ServerProc {
            child,
            addr,
            log,
            _dir: dir,
        },
        accepted,
        line,
    )
}

/// Two server processes on one `--l2-dir`: one build. The second process serves the snapshot,
/// its prefetched plans, a GetScanPlan and the latest pointer from the directory with zero ops.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_server_processes_share_l2_dir_with_one_build() {
    let l2 = tempfile::tempdir().unwrap();
    let a = ServerProc::start("proc-a", l2.path(), &[]).await;
    let b = ServerProc::start("proc-b", l2.path(), &[]).await;
    let fx = fixture("v2-checkpoints-parquet-with-sidecars");
    let version = latest_version(&fx);
    let req = request(&fx, version, default_specs());
    let mut ca = connect(a.addr).await;
    let mut cb = connect(b.addr).await;
    let (ra, _) = run(&mut ca, req.clone(), Behavior::default()).await.ok();
    assert_eq!(resolved(&ra), ResolveKind::FullBuild);
    let (rb, ops) = run(&mut cb, req, Behavior::default()).await.ok();
    assert_eq!(resolved(&rb), ResolveKind::CacheHit);
    assert_eq!(op_counts(&rb), (0, 0));
    assert!(ops.is_empty());
    assert_eq!(rb.info, ra.info);
    assert!(rb.prefetched_plans.iter().all(|p| p.cache_hit));
    assert_parity(&fx, &rb, &default_specs());
    let (sa, sb) = (a.stats().await, b.stats().await);
    assert_eq!(
        (sa.builds_started, sb.builds_started),
        (1, 0),
        "one build across processes"
    );
    assert_eq!((sb.l2_hits, sb.l2_misses), (1, 0));
    // A plan B's process generates is served to A's process from the directory.
    let other_spec = proto::ScanPlanSpec {
        timestamp_timezone: Some("America/Los_Angeles".into()),
        ..spec_json()
    };
    let (p, _) = run_scan_plan(
        &mut cb,
        plan_request(&rb, other_spec.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert!(!p.plan.as_ref().unwrap().cache_hit);
    let (p, _) = run_scan_plan(&mut ca, plan_request(&ra, other_spec), Behavior::default())
        .await
        .ok();
    assert!(p.plan.as_ref().unwrap().cache_hit, "L2 plan hit");
    let st = p.stats.unwrap();
    assert_eq!((st.ops_issued, st.ops_local), (0, 0));
    // Latest within staleness, through the shared pointer.
    let fx2 = fixture("table-with-dv-small");
    let (l1, _) = run(
        &mut ca,
        latest_request(&fx2, 60_000, vec![]),
        Behavior::default(),
    )
    .await
    .ok();
    let (l2r, _) = run(
        &mut cb,
        latest_request(&fx2, 60_000, vec![]),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&l2r), ResolveKind::CacheHit);
    assert_eq!(op_counts(&l2r), (0, 0));
    assert_eq!(l2r.version, l1.version);
    assert_eq!(b.stats().await.builds_started, 0);
    a.stop();
    b.stop();
}

/// Restart with `--l2-preload`: the port does not accept until preload has finished; then the first
/// requests are L1 hits: latest = CACHE_HIT with only the renewal listing (local), exact version
/// zero ops, GetScanPlan for a preloaded plan a hit with zero ops; no build and no L2 lookup.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn preload_restart_is_warm_before_the_port_opens() {
    let l2 = tempfile::tempdir().unwrap();
    let fx = fixture("table-with-dv-small");
    let (latest, v0) = {
        let p = ServerProc::start("seed", l2.path(), &[]).await;
        let mut c = connect(p.addr).await;
        let (latest, _) = run(
            &mut c,
            latest_request(&fx, 0, default_specs()),
            Behavior::default(),
        )
        .await
        .ok();
        let (v0, _) = run(
            &mut c,
            request(&fx, 0, vec![spec_json()]),
            Behavior::default(),
        )
        .await
        .ok();
        assert_eq!((latest.version, v0.version), (1, 0));
        p.stop();
        (latest, v0)
    };
    const DELAY_MS: u64 = 1500;
    let (p, accepted, line) = start_preloaded("warm", l2.path(), DELAY_MS, &[]).await;
    eprintln!("accepted after {accepted:?}; {line}");
    assert!(
        accepted >= Duration::from_millis(DELAY_MS),
        "accepted after {accepted:?}, before preload ended"
    );
    assert_eq!(log_field(&line, "snapshots"), Some("2"), "{line}");
    assert_eq!(log_field(&line, "latest"), Some("1"), "{line}");
    assert_eq!(log_field(&line, "plans"), Some("3"), "{line}");
    assert_eq!(log_field(&line, "entries"), Some("6"), "{line}");
    assert_eq!(log_field(&line, "rejected"), Some("0"), "{line}");
    assert!(log_field(&line, "duration_ms").is_some());
    let s = p.stats().await;
    assert_eq!((s.l1_entries, s.builds_started), (2, 0));

    let mut c = connect(p.addr).await;
    let (r, ops) = run(
        &mut c,
        latest_request(&fx, 0, default_specs()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r), ResolveKind::CacheHit);
    assert_eq!(
        op_counts(&r),
        (0, 1),
        "latest: only the renewal listing, local"
    );
    assert!(ops.is_empty());
    assert_eq!(r.info, latest.info);
    assert!(r.prefetched_plans.iter().all(|p| p.cache_hit));
    let (r, _) = run(
        &mut c,
        request_with_segment(&fx, 0, vec![spec_json()], &v0),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r), ResolveKind::CacheHit);
    assert_eq!(op_counts(&r), (0, 0), "exact version: zero ops");
    assert!(r.prefetched_plans[0].cache_hit);
    let (plan, _) = run_scan_plan(
        &mut c,
        plan_request(&latest, spec_struct()),
        Behavior::default(),
    )
    .await
    .ok();
    assert!(plan.plan.as_ref().unwrap().cache_hit, "preloaded plan");
    assert_eq!(
        plan.plan.as_ref().unwrap().result,
        latest.prefetched_plans[1].result
    );
    let st = plan.stats.unwrap();
    assert_eq!((st.ops_issued, st.ops_local), (0, 0));
    // The renewal looked for a fresher L2 latest (a miss at staleness 0); no other L2 lookup.
    let s = p.stats().await;
    assert_eq!(
        (s.builds_started, s.latest_renewals, s.l2_hits, s.l2_misses),
        (0, 1, 0, 1),
        "all from L1"
    );
    p.stop();
}

/// A preloaded entry of a credentialed table is still behind authz: a denied token gets
/// PERMISSION_DENIED and no data (GetSnapshot and GetScanPlan), as does a request without auth;
/// the allowed token is checked with UC by this process before its zero-op hit.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn preload_keeps_authz_for_cloud_entries() {
    const GOOD: &str = "tok-preload-good";
    const BAD: &str = "tok-preload-bad";
    let uc = MockUc::start();
    uc.answer(
        GOOD,
        UcAnswer::Grant {
            ttl: Duration::from_secs(3600),
        },
    );
    uc.answer(BAD, UcAnswer::Status(403));
    let uc_args = [
        "--test-uc-credentials-for-file",
        "--uc-endpoint-override",
        uc.base_url.as_str(),
    ];
    let l2 = tempfile::tempdir().unwrap();
    let mut fx = fixture("table-with-dv-small");
    fx.table_id = "1b4e28ba-2fa1-11d2-883f-0016d3cca427".into();
    let first = {
        let p = ServerProc::start("seed", l2.path(), &uc_args).await;
        let (r, _) = run(
            &mut connect(p.addr).await,
            with_auth(request(&fx, 1, vec![spec_json()]), GOOD),
            Behavior::default(),
        )
        .await
        .ok();
        p.stop();
        r
    };
    assert_eq!(uc.calls().len(), 1);
    let files = list_files(l2.path());
    assert!(
        files
            .iter()
            .any(|f| f.contains("/snap/1b4e28ba-2fa1-11d2-883f-0016d3cca427/")),
        "UC table keyed by its id: {files:#?}"
    );
    assert!(!files.iter().any(|f| f.contains("authz")));
    let (p, _, line) = start_preloaded("warm", l2.path(), 0, &uc_args).await;
    assert_eq!(log_field(&line, "snapshots"), Some("1"), "{line}");
    assert_eq!(uc.calls().len(), 1, "preload makes no UC call");
    let mut c = connect(p.addr).await;
    for (auth, calls) in [(Some(BAD), 2), (None, 2)] {
        let req = request_with_segment(&fx, 1, vec![spec_json()], &first);
        let req = match auth {
            Some(t) => with_auth(req, t),
            None => req,
        };
        let out = run(&mut c, req, Behavior::default()).await;
        assert!(out.ops.is_empty());
        let err = out.err();
        assert_eq!(err.code, Code::PermissionDenied as i32, "{auth:?}: {err:?}");
        assert_eq!(uc.calls().len(), calls);
    }
    let mut plan_req = plan_request(&first, spec_json());
    plan_req.auth = with_auth(request(&fx, 1, vec![]), BAD).auth;
    let err = run_scan_plan(&mut c, plan_req, Behavior::default())
        .await
        .err();
    assert_eq!(err.code, Code::PermissionDenied as i32);
    let (r, _) = run(
        &mut c,
        with_auth(
            request_with_segment(&fx, 1, vec![spec_json()], &first),
            GOOD,
        ),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r), ResolveKind::CacheHit);
    assert_eq!(op_counts(&r), (0, 0));
    assert_eq!(r.info, first.info);
    assert_eq!(
        uc.calls()
            .iter()
            .filter(|c| c.authorization.ends_with(GOOD))
            .count(),
        2,
        "this process checked the good token with UC itself"
    );
    let s = p.stats().await;
    assert_eq!((s.builds_started, s.l2_hits), (0, 0));
    p.stop();
}
