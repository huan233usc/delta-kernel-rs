//! SPEC v10 L0 tests: GetScanPlan by SnapshotRef, prefetch semantics, ResolveTimeTravel, the
//! two-tier store (L2 = in-process `MemoryStore` here; `tests/l2_redis.rs` runs against Redis),
//! snapshot-hint round trips and measured entry sizes.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::*;
use delta_kernel::history_manager::{
    HistoryCommitType, first_version_after, get_earliest_commit, latest_version_as_of,
};
use delta_kernel_server::cache::CacheConfig;
use delta_kernel_server::info::segment_id;
use delta_kernel_server::proto::error::Code;
use delta_kernel_server::proto::{self, ResolveKind};
use delta_kernel_server::store::{DEFAULT_L2_TTL, L2, MemoryStore};
use prost::Message as _;

fn seg(resp: &proto::GetSnapshotResponse) -> &proto::LogSegmentInfo {
    resp.info.as_ref().unwrap().log_segment.as_ref().unwrap()
}

/// `resp` with `plans` as its prefetched plans, for the parity helpers.
fn with_plans(
    resp: &proto::GetSnapshotResponse,
    plans: Vec<proto::ScanPlanResult>,
) -> proto::GetSnapshotResponse {
    let mut r = resp.clone();
    r.prefetched_plans = plans;
    r
}

fn plan_ops(r: &proto::GetScanPlanResponse) -> (i32, i32) {
    let s = r.stats.as_ref().unwrap();
    (s.ops_issued, s.ops_local)
}

fn memory_l2() -> Arc<L2> {
    Arc::new(MemoryStore::new(DEFAULT_L2_TTL))
}

// ------------------------------------------------------------------------------------------------
// GetScanPlan
// ------------------------------------------------------------------------------------------------

/// The ref names the instance; GetScanPlan generates the plan once, then serves it from cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_plan_by_ref_hits_instance_and_caches_plan() {
    let fx = fixture("table-with-dv-small");
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let (snap, _) = run(&mut c, request(&fx, 1, vec![]), Behavior::default())
        .await
        .ok();
    assert!(snap.prefetched_plans.is_empty());
    let r = snap.r#ref.as_ref().expect("v10 response carries a ref");
    assert_eq!(r.version, 1);
    assert_eq!(r.log_segment.as_ref(), Some(seg(&snap)));
    assert_eq!(r.segment_id, segment_id(seg(&snap)).digest());
    assert_eq!(r.segment_id.len(), 64, "hex sha256");
    let ping = c.ping(proto::PingRequest {}).await.unwrap().into_inner();
    assert_eq!(r.instance_id, ping.instance_id);
    let stats = snap.stats.as_ref().unwrap();
    assert!(stats.resolve_us > 0 && stats.plan_us >= 0, "{stats:?}");

    let builds = server.stats().await.builds_started;
    let (p1, _) = run_scan_plan(
        &mut c,
        plan_request(&snap, spec_json()),
        Behavior::default(),
    )
    .await
    .ok();
    let plan1 = p1.plan.clone().unwrap();
    assert!(!plan1.cache_hit);
    assert!(p1.stats.as_ref().unwrap().plan_us > 0);
    assert_parity(&fx, &with_plans(&snap, vec![plan1]), &[spec_json()]);
    assert_eq!(
        server.stats().await.builds_started,
        builds,
        "same instance: no rebuild"
    );

    let (p2, _) = run_scan_plan(
        &mut c,
        plan_request(&snap, spec_json()),
        Behavior::default(),
    )
    .await
    .ok();
    assert!(p2.plan.as_ref().unwrap().cache_hit);
    assert_eq!(plan_ops(&p2), (0, 0));
    assert_eq!(
        p2.plan.as_ref().unwrap().result,
        p1.plan.as_ref().unwrap().result
    );
}

/// Prefetch == one GetScanPlan per spec on the same stream: semantically equal plans.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prefetch_equals_get_scan_plan() {
    for name in [
        "table-with-dv-small",
        "v2-checkpoints-parquet-with-sidecars",
        "crc-full",
        "basic_partitioned",
    ] {
        let fx = fixture(name);
        let version = latest_version(&fx);
        let specs = default_specs();
        // Prefetched on one server, GetScanPlan on another: no shared plan cache.
        let a = TestServer::start().await;
        let b = TestServer::start().await;
        let mut ca = a.client().await;
        let mut cb = b.client().await;
        let (pre, _) = run(
            &mut ca,
            request(&fx, version, specs.clone()),
            Behavior::default(),
        )
        .await
        .ok();
        let (snap, _) = run(&mut cb, request(&fx, version, vec![]), Behavior::default())
            .await
            .ok();
        let mut plans = Vec::new();
        for spec in &specs {
            let (p, _) = run_scan_plan(
                &mut cb,
                plan_request(&snap, spec.clone()),
                Behavior::default(),
            )
            .await
            .ok();
            plans.push(p.plan.unwrap());
        }
        assert_eq!(pre.info, snap.info, "{name}");
        let n1 = assert_parity(&fx, &pre, &specs);
        let n2 = assert_parity(&fx, &with_plans(&snap, plans), &specs);
        assert_eq!(n1, n2, "{name}: prefetched and GetScanPlan rows");
    }
}

/// L1 miss (reset) and eviction: the ref is rebuilt at its exact version and still resolves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_plan_rebuilds_evicted_or_reset_instance() {
    let fx = fixture("table-with-dv-small");
    let server = TestServer::start_with(
        CacheConfig {
            max_instances_per_table: 1,
            ..CacheConfig::default()
        },
        true,
    )
    .await;
    let mut c = server.client().await;
    let (v0, _) = run(&mut c, request(&fx, 0, vec![]), Behavior::default())
        .await
        .ok();
    // v1 evicts v0 (one instance per table).
    run(&mut c, request(&fx, 1, vec![]), Behavior::default())
        .await
        .ok();
    let before = server.stats().await.builds_started;
    let (p, ops) = run_scan_plan(&mut c, plan_request(&v0, spec_json()), Behavior::default())
        .await
        .ok();
    assert_eq!(
        server.stats().await.builds_started,
        before + 1,
        "evicted ref rebuilt"
    );
    assert!(plan_ops(&p).0 + plan_ops(&p).1 > 0 || !ops.is_empty());
    assert_parity(&fx, &with_plans(&v0, vec![p.plan.unwrap()]), &[spec_json()]);

    server.core.reset_cache();
    let (p, _) = run_scan_plan(
        &mut c,
        plan_request(&v0, spec_struct()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(
        server.stats().await.builds_started,
        before + 2,
        "reset ref rebuilt"
    );
    assert_parity(
        &fx,
        &with_plans(&v0, vec![p.plan.unwrap()]),
        &[spec_struct()],
    );
}

/// The table at the ref's version no longer has the ref's segment: STALE_REF, not a plan for
/// other files. A tampered segment_id is STALE_REF too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_plan_stale_ref() {
    let fx = fixture("table-with-dv-small");
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let (snap, _) = run(&mut c, request(&fx, 1, vec![]), Behavior::default())
        .await
        .ok();
    assert!(seg(&snap).checkpoint_version.is_none());

    let good = snap.r#ref.as_ref().unwrap().segment_id.clone();
    for bad in [
        String::new(),
        "0".repeat(64),
        good[..63].to_string(),
        format!("{good}0"),
        good.to_ascii_uppercase(),
        format!("{}g", &good[..63]),
    ] {
        let mut tampered = plan_request(&snap, spec_json());
        tampered.r#ref.as_mut().unwrap().segment_id = bad.clone();
        let err = run_scan_plan(&mut c, tampered, Behavior::default())
            .await
            .err();
        assert_eq!(
            err.code,
            Code::StaleRef as i32,
            "segment_id {bad:?}: {err:?}"
        );
    }

    // A checkpoint at v1 changes what v1 resolves to; the cached instance is dropped.
    write_checkpoint(&fx, 1);
    server.core.reset_cache();
    let err = run_scan_plan(
        &mut c,
        plan_request(&snap, spec_json()),
        Behavior::default(),
    )
    .await
    .err();
    assert_eq!(err.code, Code::StaleRef as i32, "{err:?}");
    assert!(!err.retryable);
    // The client recovers with a fresh GetSnapshot.
    let (fresh, _) = run(&mut c, request(&fx, 1, vec![]), Behavior::default())
        .await
        .ok();
    assert_eq!(seg(&fresh).checkpoint_version, Some(1));
    let (p, _) = run_scan_plan(
        &mut c,
        plan_request(&fresh, spec_json()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_parity(
        &fx,
        &with_plans(&fresh, vec![p.plan.unwrap()]),
        &[spec_json()],
    );
}

/// Version 0 of a `catalogManaged` table (which requires in-commit timestamps).
const CM_V0: &str = r#"{"commitInfo":{"timestamp":1700000000000,"inCommitTimestamp":1700000000000,"operation":"CREATE TABLE","txnId":"cm-0"}}
{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["catalogManaged"],"writerFeatures":["catalogManaged","inCommitTimestamp"]}}
{"metaData":{"id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableInCommitTimestamps":"true","io.unitycatalog.tableId":"cm-table"},"createdTime":1700000000000}}
"#;

const CM_TS0: i64 = 1_700_000_000_000;

fn cm_fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("cm_table");
    std::fs::create_dir_all(root.join("_delta_log/_staged_commits")).unwrap();
    std::fs::write(
        root.join("_delta_log").join(format!("{:020}.json", 0)),
        CM_V0,
    )
    .unwrap();
    let root = root.canonicalize().unwrap();
    let url = url::Url::from_directory_path(&root).unwrap();
    Fixture {
        _dir: dir,
        root,
        url,
        table_id: "cm-table".into(),
    }
}

/// Commit `version` with ICT `CM_TS0 + version * 1000`.
fn cm_commit(version: i64) -> String {
    let ts = CM_TS0 + version * 1000;
    format!(
        "{{\"commitInfo\":{{\"timestamp\":{ts},\"inCommitTimestamp\":{ts},\
         \"operation\":\"WRITE\",\"txnId\":\"cm-{version}\"}}}}\n\
         {{\"add\":{{\"path\":\"file_{version}.parquet\",\"partitionValues\":{{}},\"size\":1234,\
         \"modificationTime\":{ts},\"dataChange\":true,\"stats\":\"{{\\\"numRecords\\\":1}}\"}}}}\n"
    )
}

fn cm_publish(fx: &Fixture, version: i64) -> String {
    let name = format!("{version:020}.json");
    std::fs::write(fx.log_dir().join(&name), cm_commit(version)).unwrap();
    name
}

fn cm_stage(fx: &Fixture, version: i64) -> String {
    let name = format!("_staged_commits/{version:020}.6e5c4a07-8a1f-4b4e-9d5c-{version:012}.json");
    std::fs::write(fx.log_dir().join(&name), cm_commit(version)).unwrap();
    name
}

fn cm_request(fx: &Fixture, mcv: i64, tail: &[String]) -> proto::GetSnapshotRequest {
    let tail: Vec<&str> = tail.iter().map(String::as_str).collect();
    catalog_request(fx, mcv, &tail, vec![])
}

/// A staged-tail ref is rebuilt from its own staged deltas and never cached; the plan equals the
/// in-process catalog-managed build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_plan_staged_ref_rebuilt_from_staged_deltas() {
    let fx = cm_fixture();
    cm_publish(&fx, 1);
    let v2 = cm_stage(&fx, 2);
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let req = cm_request(&fx, 2, &[v2]);
    let (snap, _) = run(&mut c, req.clone(), Behavior::default()).await.ok();
    assert_eq!(seg(&snap).delta_is_staged.last(), Some(&true));
    assert!(server.core.cached_instances_per_table().is_empty());
    let local = local_catalog_snapshot(&fx, &req).unwrap();
    for round in 0..2 {
        let before = server.stats().await.builds_started;
        let (p, _) = run_scan_plan(
            &mut c,
            plan_request(&snap, spec_json()),
            Behavior::default(),
        )
        .await
        .ok();
        assert_eq!(
            server.stats().await.builds_started,
            before + 1,
            "round {round}: staged ref is rebuilt every time"
        );
        assert!(!p.plan.as_ref().unwrap().cache_hit);
        assert_parity_with(
            &local,
            &with_plans(&snap, vec![p.plan.unwrap()]),
            &[spec_json()],
        );
        assert!(
            server.core.cached_instances_per_table().is_empty(),
            "staged instance never cached"
        );
    }
}

/// GetScanPlan checks credentials first, also when the instance and plan are cached.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_plan_authz_first() {
    const GOOD: &str = "tok-plan-good";
    const BAD: &str = "tok-plan-bad";
    let uc = MockUc::start();
    uc.answer(
        GOOD,
        UcAnswer::Grant {
            ttl: Duration::from_secs(3600),
        },
    );
    uc.answer(BAD, UcAnswer::Status(403));
    let server = server_with_uc(&uc).await;
    let fx = fixture("table-with-dv-small");
    let mut c = server.client().await;
    let (snap, _) = run(
        &mut c,
        with_auth(request(&fx, 1, vec![spec_json()]), GOOD),
        Behavior::default(),
    )
    .await
    .ok();
    let mut req = plan_request(&snap, spec_json());
    let err = run_scan_plan(&mut c, req.clone(), Behavior::default())
        .await
        .err();
    assert_eq!(err.code, Code::PermissionDenied as i32, "no auth: {err:?}");
    req.auth = Some(proto::Auth {
        workspace_host: "unused".into(),
        bearer_token: BAD.into(),
    });
    let err = run_scan_plan(&mut c, req.clone(), Behavior::default())
        .await
        .err();
    assert_eq!(
        err.code,
        Code::PermissionDenied as i32,
        "bad token: {err:?}"
    );
    req.auth.as_mut().unwrap().bearer_token = GOOD.into();
    let (p, _) = run_scan_plan(&mut c, req, Behavior::default()).await.ok();
    assert!(p.plan.unwrap().cache_hit);
}

// ------------------------------------------------------------------------------------------------
// ResolveTimeTravel
// ------------------------------------------------------------------------------------------------

const TT_BASE: i64 = 1_600_000_000_000;

fn set_mtime(path: &std::path::Path, ms: i64) {
    let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.set_modified(UNIX_EPOCH + Duration::from_millis(ms as u64))
        .unwrap();
}

/// A table with commits 0..n. Commit v's timestamp is `TT_BASE + v * 1000`: its ICT when `ict`,
/// else its file mtime (an ICT table gets unrelated, descending mtimes to prove they are unused).
fn tt_table(n: i64, ict: bool) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("tt");
    let log = root.join("_delta_log");
    std::fs::create_dir_all(&log).unwrap();
    for v in 0..n {
        let ts = TT_BASE + v * 1000;
        let ict_field = if ict {
            format!(",\"inCommitTimestamp\":{ts}")
        } else {
            String::new()
        };
        let mut body = format!(
            "{{\"commitInfo\":{{\"timestamp\":{ts}{ict_field},\"operation\":\"WRITE\"}}}}\n"
        );
        if v == 0 {
            let (protocol, conf) = if ict {
                (
                    r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":7,"writerFeatures":["inCommitTimestamp"]}}"#,
                    r#""delta.enableInCommitTimestamps":"true""#,
                )
            } else {
                (
                    r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
                    "",
                )
            };
            body.push_str(protocol);
            body.push('\n');
            body.push_str(&format!(
                "{{\"metaData\":{{\"id\":\"tt-table\",\"format\":{{\"provider\":\"parquet\",\"options\":{{}}}},\"schemaString\":\"{{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{{}}}}]}}\",\"partitionColumns\":[],\"configuration\":{{{conf}}},\"createdTime\":{ts}}}}}\n"
            ));
        }
        body.push_str(&format!(
            "{{\"add\":{{\"path\":\"f{v}.parquet\",\"partitionValues\":{{}},\"size\":100,\"modificationTime\":{ts},\"dataChange\":true}}}}\n"
        ));
        let path = log.join(format!("{v:020}.json"));
        std::fs::write(&path, body).unwrap();
        set_mtime(&path, if ict { TT_BASE + 100_000 - v * 7 } else { ts });
    }
    let root = root.canonicalize().unwrap();
    let url = url::Url::from_directory_path(&root).unwrap();
    Fixture {
        _dir: dir,
        root,
        url,
        table_id: "tt-table".into(),
    }
}

fn tt_request(
    fx: &Fixture,
    target: proto::resolve_time_travel_request::Target,
    last: bool,
    recreatable: bool,
    earliest: bool,
) -> proto::ResolveTimeTravelRequest {
    proto::ResolveTimeTravelRequest {
        table: Some(proto::TableRef {
            table_id: fx.table_id.clone(),
            location: fx.url.to_string(),
        }),
        target: Some(target),
        can_return_last_commit: last,
        must_be_recreatable: recreatable,
        can_return_earliest_commit: earliest,
        catalog_latest: None,
        client: Some(client_info()),
        auth: None,
    }
}

/// In-process JNR `getActiveCommitAtTimestamp` on the latest snapshot.
fn jnr_active_commit(
    snapshot: &delta_kernel::Snapshot,
    t: i64,
    last: bool,
    recreatable: bool,
    earliest: bool,
) -> Result<(i64, i64), ()> {
    let engine = local_engine();
    let latest_ts = snapshot.reader().get_timestamp(&engine).unwrap();
    if latest_ts < t {
        return if last {
            Ok((snapshot.version() as i64, latest_ts))
        } else {
            Err(())
        };
    }
    let ty = if recreatable {
        HistoryCommitType::Recreatable
    } else {
        HistoryCommitType::Published
    };
    match latest_version_as_of(snapshot, &engine, t, ty) {
        Ok(c) => Ok((c.version as i64, c.timestamp)),
        Err(_) if earliest => match first_version_after(snapshot, &engine, t, ty) {
            Ok(c) if c.timestamp > t => Ok((c.version as i64, c.timestamp)),
            _ => Err(()),
        },
        Err(_) => Err(()),
    }
}

/// Timestamp and version queries on ICT and non-ICT tables equal the in-process JNR answers,
/// including timestamps before the first and after the last commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolve_time_travel_matches_in_process_kernel() {
    use proto::resolve_time_travel_request::Target;
    for ict in [false, true] {
        let fx = tt_table(5, ict);
        let server = TestServer::start().await;
        let mut c = server.client().await;
        let local = local_snapshot(&fx, None);
        let ts = |v: i64| TT_BASE + v * 1000;
        let cases: Vec<(i64, Option<i64>)> = vec![
            (ts(0), Some(0)),
            (ts(2), Some(2)),
            (ts(2) + 500, Some(2)),
            (ts(4), Some(4)),
            (ts(0) - 1, None),
            (ts(4) + 1, None),
        ];
        for (t, exact) in cases {
            for (last, recreatable, earliest) in [
                (false, false, false),
                (true, false, false),
                (false, true, true),
                (true, true, true),
            ] {
                let expected = jnr_active_commit(&local, t, last, recreatable, earliest);
                let got = run_time_travel(
                    &mut c,
                    tt_request(&fx, Target::TimestampMs(t), last, recreatable, earliest),
                    Behavior::default(),
                )
                .await;
                let ctx =
                    format!("ict={ict} t={t} last={last} rec={recreatable} earliest={earliest}");
                match (expected, got.result) {
                    (Ok((v, cts)), Ok(r)) => {
                        assert_eq!((r.version, r.commit_timestamp_ms), (v, cts), "{ctx}");
                        assert_eq!(r.latest_version, 4, "{ctx}");
                        if let Some(e) = exact {
                            assert_eq!(v, e, "{ctx}: in-process expectation");
                        }
                    }
                    (Err(()), Err(Ended::Error(e))) => {
                        assert_eq!(e.code, Code::Unsupported as i32, "{ctx}: {e:?}");
                        assert!(e.message.starts_with("LOG_HISTORY: "), "{ctx}: {e:?}");
                    }
                    (exp, got) => panic!("{ctx}: in-process {exp:?}, server {got:?}"),
                }
            }
        }
        // Before the first commit with can_return_earliest -> v0; after the last with
        // can_return_last -> v4 (explicit, not only via the in-process mirror).
        let (r, _) = run_time_travel(
            &mut c,
            tt_request(&fx, Target::TimestampMs(ts(0) - 1), false, false, true),
            Behavior::default(),
        )
        .await
        .ok();
        assert_eq!((r.version, r.commit_timestamp_ms), (0, ts(0)), "ict={ict}");
        let (r, _) = run_time_travel(
            &mut c,
            tt_request(&fx, Target::TimestampMs(ts(9)), true, false, false),
            Behavior::default(),
        )
        .await
        .ok();
        assert_eq!((r.version, r.commit_timestamp_ms), (4, ts(4)), "ict={ict}");

        // check_version: bounds for the client's VersionNotFound check.
        for recreatable in [false, true] {
            let (r, _) = run_time_travel(
                &mut c,
                tt_request(&fx, Target::CheckVersion(3), false, recreatable, false),
                Behavior::default(),
            )
            .await
            .ok();
            let ty = if recreatable {
                HistoryCommitType::Recreatable
            } else {
                HistoryCommitType::Published
            };
            let want = get_earliest_commit(
                &local_engine(),
                &fx.url.join("_delta_log/").unwrap(),
                None,
                ty,
            )
            .unwrap() as i64;
            assert_eq!(
                (r.version, r.earliest_recreatable_version, r.latest_version),
                (3, want, 4)
            );
        }
        // The latest instance is cached and reused; results are not.
        assert_eq!(server.core.cached_instances_per_table().len(), 1);
    }
}

/// With early commits removed behind a checkpoint, the earliest published and recreatable
/// commits differ from 0 and a timestamp before them follows can_return_earliest_commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolve_time_travel_after_log_cleanup() {
    use proto::resolve_time_travel_request::Target;
    let fx = tt_table(5, true);
    write_checkpoint(&fx, 2);
    for v in 0..2 {
        std::fs::remove_file(fx.log_dir().join(format!("{v:020}.json"))).unwrap();
    }
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let local = local_snapshot(&fx, None);
    for recreatable in [false, true] {
        let (r, _) = run_time_travel(
            &mut c,
            tt_request(&fx, Target::CheckVersion(1), false, recreatable, false),
            Behavior::default(),
        )
        .await
        .ok();
        assert_eq!((r.earliest_recreatable_version, r.latest_version), (2, 4));
        for (t, earliest) in [
            (TT_BASE + 1000, false),
            (TT_BASE + 1000, true),
            (TT_BASE + 3500, false),
        ] {
            let expected = jnr_active_commit(&local, t, false, recreatable, earliest);
            let got = run_time_travel(
                &mut c,
                tt_request(&fx, Target::TimestampMs(t), false, recreatable, earliest),
                Behavior::default(),
            )
            .await;
            match (expected, got.result) {
                (Ok((v, _)), Ok(r)) => assert_eq!(r.version, v),
                (Err(()), Err(Ended::Error(e))) => assert_eq!(e.code, Code::Unsupported as i32),
                (e, g) => panic!("t={t} rec={recreatable} earliest={earliest}: {e:?} vs {g:?}"),
            }
        }
    }
}

/// Catalog-managed: resolution runs on the catalog_latest snapshot, so a staged commit's ICT
/// resolves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolve_time_travel_catalog_managed_staged() {
    use proto::resolve_time_travel_request::Target;
    let fx = cm_fixture();
    cm_publish(&fx, 1);
    let v2 = cm_stage(&fx, 2);
    let snap_req = cm_request(&fx, 2, &[v2]);
    let Some(proto::version_spec::Spec::CatalogLatest(cl)) =
        snap_req.at.clone().and_then(|a| a.spec)
    else {
        unreachable!()
    };
    let local = local_catalog_snapshot(&fx, &snap_req).unwrap();
    let server = TestServer::start().await;
    let mut c = server.client().await;
    for t in [CM_TS0 + 2000, CM_TS0 + 1500, CM_TS0 + 999_999] {
        let mut req = tt_request(&fx, Target::TimestampMs(t), true, false, false);
        req.catalog_latest = Some(cl.clone());
        let (r, _) = run_time_travel(&mut c, req, Behavior::default()).await.ok();
        let (v, ts) = jnr_active_commit(&local, t, true, false, false).unwrap();
        assert_eq!((r.version, r.commit_timestamp_ms), (v, ts), "t={t}");
        assert_eq!(r.latest_version, 2);
    }
    let mut req = tt_request(&fx, Target::TimestampMs(CM_TS0 + 2000), false, false, false);
    req.catalog_latest = Some(cl.clone());
    let (r, _) = run_time_travel(&mut c, req, Behavior::default()).await.ok();
    assert_eq!(r.version, 2, "the staged commit's ICT");
    let mut req = tt_request(&fx, Target::CheckVersion(2), false, false, false);
    req.catalog_latest = Some(cl);
    let (r, _) = run_time_travel(&mut c, req, Behavior::default()).await.ok();
    assert_eq!((r.earliest_recreatable_version, r.latest_version), (0, 2));
    assert!(
        server.core.cached_instances_per_table().is_empty(),
        "staged tail not cached"
    );
}

/// v10 review B2: without catalog_latest, a catalog-managed table must not be answered from the
/// published filesystem latest (here v1, while the catalog's latest is the staged v2).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolve_time_travel_catalog_managed_without_catalog_latest_is_refused() {
    use proto::resolve_time_travel_request::Target;
    let fx = cm_fixture();
    cm_publish(&fx, 1);
    cm_stage(&fx, 2);
    let server = TestServer::start().await;
    let mut c = server.client().await;
    for target in [
        Target::TimestampMs(CM_TS0 + 2000),
        Target::TimestampMs(CM_TS0 + 1000),
        Target::CheckVersion(1),
    ] {
        let err = run_time_travel(
            &mut c,
            tt_request(&fx, target, true, false, true),
            Behavior::default(),
        )
        .await
        .err();
        assert_eq!(err.code, Code::Unsupported as i32, "{target:?}: {err:?}");
        assert!(
            err.message.starts_with("LOG_HISTORY: "),
            "{target:?}: {err:?}"
        );
        assert!(err.message.contains("catalog_latest"), "{err:?}");
    }
    assert!(server.core.cached_instances_per_table().is_empty());
}

/// GetSnapshot (path-based latest) on a catalog-managed table without catalog_latest: UNSUPPORTED
/// with the CATALOG_STATE_REQUIRED prefix (the client uses its catalog path).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_snapshot_catalog_managed_without_catalog_latest() {
    let fx = cm_fixture();
    cm_publish(&fx, 1);
    let server = TestServer::start().await;
    let mut c = server.client().await;
    for staleness in [0, 60_000] {
        let err = run(
            &mut c,
            latest_request(&fx, staleness, vec![]),
            Behavior::default(),
        )
        .await
        .err();
        assert_eq!(err.code, Code::Unsupported as i32, "{err:?}");
        assert!(
            err.message.starts_with("CATALOG_STATE_REQUIRED: "),
            "{err:?}"
        );
        assert!(!err.retryable);
    }
}

/// Other kernel MaxCatalogVersion failures are request bugs: they keep a non-fallback code and no
/// CATALOG_STATE_REQUIRED prefix. (A requested version above max_catalog_version cannot be sent
/// through the API, which never combines both; `errors.rs` unit-tests that classification.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn other_max_catalog_version_errors_are_not_catalog_state_required() {
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let assert_no_fallback = |what: &str, err: proto::Error| {
        assert_ne!(err.code, Code::Unsupported as i32, "{what}: {err:?}");
        assert!(
            !err.message.contains("CATALOG_STATE_REQUIRED"),
            "{what}: {err:?}"
        );
        assert!(!err.message.contains("LOG_HISTORY"), "{what}: {err:?}");
    };

    // Malformed catalog_latest: the tail ends at v1 but max_catalog_version is 2.
    let fx = cm_fixture();
    let v1 = cm_stage(&fx, 1);
    let err = run(
        &mut c,
        cm_request(&fx, 2, std::slice::from_ref(&v1)),
        Behavior::default(),
    )
    .await
    .err();
    assert!(err.message.contains("Max catalog version"), "{err:?}");
    assert_eq!(err.code, Code::Internal as i32, "{err:?}");
    assert_no_fallback("tail below its bound", err);
    // The same tail through ResolveTimeTravel is not refused as catalog-state-missing either.
    let mut req = tt_request(
        &fx,
        proto::resolve_time_travel_request::Target::CheckVersion(1),
        false,
        false,
        false,
    );
    let Some(proto::version_spec::Spec::CatalogLatest(cl)) =
        cm_request(&fx, 2, &[v1]).at.and_then(|a| a.spec)
    else {
        unreachable!()
    };
    req.catalog_latest = Some(cl);
    let err = run_time_travel(&mut c, req, Behavior::default())
        .await
        .err();
    assert_no_fallback("time travel with a malformed tail", err);

    // max_catalog_version on a non-catalog-managed table: whatever kernel decides, it is not a
    // missing-catalog-state fallback.
    let fx = fixture("table-with-dv-small");
    for (mcv, tail) in [(5i64, vec![]), (5, vec![format!("{:020}.json", 1)])] {
        let tail: Vec<&str> = tail.iter().map(String::as_str).collect();
        let out = run(
            &mut c,
            catalog_request(&fx, mcv, &tail, vec![]),
            Behavior::default(),
        )
        .await;
        if let Err(Ended::Error(err)) = out.result {
            assert_no_fallback(&format!("non-CM mcv={mcv} tail={tail:?}"), err);
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Snapshot hints: round trip, plan parity on rebuilt snapshots, measured sizes
// ------------------------------------------------------------------------------------------------

const HINT_FIXTURES: &[&str] = &[
    "table-with-dv-small",
    "table-without-dv-small",
    "basic_partitioned",
    "app-txn-checkpoint",
    "crc-full",
    "with_checkpoint_no_last_checkpoint",
    "v2-checkpoints-parquet-with-sidecars",
    "v2-checkpoints-json-with-sidecars",
    "v2-classic-checkpoint-parquet",
    "table-with-column-defaults",
    "type-widening",
    "with-short-dv",
    "v1-multi-part-struct-stats-only",
    "parsed-stats",
    "compacted-log-files-table",
];

/// Every fixture's hint encodes losslessly (encode verifies decode == original component by
/// component), rebuilds with zero IO, and the rebuilt snapshot yields the same SnapshotInfo and
/// plans as the original. Prints the measured entry sizes.
#[test]
fn snapshot_hints_round_trip_and_sizes() {
    let mut rows = Vec::new();
    for name in HINT_FIXTURES {
        let fx = fixture(name);
        let original = local_snapshot(&fx, None);
        let root = fx.url.to_string();
        let hint = delta_kernel_server::hint::encode(&original, &root)
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let rebuilt =
            delta_kernel_server::hint::rebuild(&hint, &root, original.version() as i64).unwrap();
        let engine = local_engine();
        let info_a = delta_kernel_server::info::compute_snapshot_info(&original, &engine).unwrap();
        let info_b = delta_kernel_server::info::compute_snapshot_info(&rebuilt, &engine).unwrap();
        assert_eq!(info_a, info_b, "{name}: SnapshotInfo");
        assert_eq!(
            original.crc_at_version(),
            rebuilt.crc_at_version(),
            "{name}: CRC state"
        );
        if *name == "crc-full" {
            assert!(
                original
                    .crc_at_version()
                    .and_then(|c| c.all_files())
                    .is_some_and(|a| !a.is_empty()),
                "crc-full exercises the inline allFiles path"
            );
        }
        let mut plan_zstd = Vec::new();
        for spec in default_specs() {
            let a = local_plan(&original, &spec).unwrap();
            let b = local_plan(&rebuilt, &spec).unwrap();
            match (a, b) {
                (None, None) => plan_zstd.push(0),
                (Some(a), Some(b)) => {
                    let ab = delta_kernel::plans::proto::plan::Plan::from(&a).encode_to_vec();
                    let bb = delta_kernel::plans::proto::plan::Plan::from(&b).encode_to_vec();
                    assert_eq!(
                        delta_kernel_server::normalize::normalize_plan_bytes(&ab).unwrap(),
                        delta_kernel_server::normalize::normalize_plan_bytes(&bb).unwrap(),
                        "{name}: plan"
                    );
                    plan_zstd.push(zstd::encode_all(ab.as_slice(), 3).unwrap().len());
                }
                (a, b) => panic!("{name}: plan presence {} vs {}", a.is_some(), b.is_some()),
            }
        }
        rows.push(format!(
            "{name:<40} v{:<3} info {:>6} B  hint(zstd) {:>6} B  plans(zstd) {:?} B  snapshot heap {:>8} B",
            original.version(),
            info_a.encoded_len(),
            hint.len(),
            plan_zstd,
            original.estimated_owned_heap_size_bytes()
        ));
    }
    eprintln!("measured entry sizes:\n{}", rows.join("\n"));
}

// ------------------------------------------------------------------------------------------------
// L2 (in-process MemoryStore; the same Core code path as Redis)
// ------------------------------------------------------------------------------------------------

/// Replica B (or a restarted server) hits L2 and serves info and plans with zero ops of either
/// kind; a plan not in L2 is generated on the hint-rebuilt snapshot and matches in-process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn l2_hit_after_restart_has_zero_ops() {
    let fx = fixture("v2-checkpoints-parquet-with-sidecars");
    let version = latest_version(&fx);
    let l2 = memory_l2();
    let specs = vec![spec_json()];
    let first = {
        let a = TestServer::start_l2("replica-a", l2.clone()).await;
        let mut c = a.client().await;
        let (r, _) = run(
            &mut c,
            request(&fx, version, specs.clone()),
            Behavior::default(),
        )
        .await
        .ok();
        assert_eq!(resolved(&r), ResolveKind::FullBuild);
        let s = a.stats().await;
        assert_eq!((s.l2_hits, s.l2_misses), (0, 1));
        r
    };
    for exact_segment in [false, true] {
        let b = TestServer::start_l2("replica-b", l2.clone()).await;
        let mut c = b.client().await;
        let req = if exact_segment {
            request_with_segment(&fx, version, specs.clone(), &first)
        } else {
            request(&fx, version, specs.clone())
        };
        let (r, ops) = run(&mut c, req, Behavior::default()).await.ok();
        assert_eq!(resolved(&r), ResolveKind::CacheHit, "exact={exact_segment}");
        assert_eq!(op_counts(&r), (0, 0), "L2 hit: zero ops");
        assert!(ops.is_empty());
        assert_eq!(r.info, first.info);
        assert_eq!(
            r.prefetched_plans[0].result,
            first.prefetched_plans[0].result
        );
        assert!(r.prefetched_plans[0].cache_hit);
        assert_eq!(
            r.r#ref.as_ref().unwrap().segment_id,
            first.r#ref.as_ref().unwrap().segment_id
        );
        assert_eq!(r.r#ref.as_ref().unwrap().instance_id, "replica-b");
        let s = b.stats().await;
        assert_eq!((s.builds_started, s.l2_hits), (0, 1));
        assert_eq!(s.l1_entries, 1);
        assert!(s.l1_bytes > 0);
        assert_eq!(
            s.entry_bytes
                .get(&r.r#ref.as_ref().unwrap().segment_id)
                .copied(),
            Some(s.l1_bytes)
        );
        // A plan L2 lacks: generated on the hint-rebuilt snapshot.
        let (p, _) = run_scan_plan(&mut c, plan_request(&r, spec_struct()), Behavior::default())
            .await
            .ok();
        assert_parity(
            &fx,
            &with_plans(&r, vec![p.plan.unwrap()]),
            &[spec_struct()],
        );
    }
    // GetScanPlan on a third replica by ref alone: L2 instance + L2 plan, zero ops.
    let c3 = TestServer::start_l2("replica-c", l2.clone()).await;
    let mut c = c3.client().await;
    let (p, _) = run_scan_plan(
        &mut c,
        plan_request(&first, spec_json()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(plan_ops(&p), (0, 0));
    assert!(p.plan.as_ref().unwrap().cache_hit);
    assert_eq!(c3.core.counters.l2_plan_hits.load(Ordering::Relaxed), 1);
}

/// Path-based latest within staleness is an L2 hit with zero ops; beyond it, the stale L2 entry is
/// the base of one incremental re-listing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn l2_latest_hit_and_stale_base() {
    let fx = fixture("table-with-dv-small");
    let l2 = memory_l2();
    let a = TestServer::start_l2("a", l2.clone()).await;
    let mut ca = a.client().await;
    let (r1, _) = run(
        &mut ca,
        latest_request(&fx, 60_000, vec![]),
        Behavior::default(),
    )
    .await
    .ok();
    let b = TestServer::start_l2("b", l2.clone()).await;
    let mut cb = b.client().await;
    let (r2, _) = run(
        &mut cb,
        latest_request(&fx, 60_000, vec![]),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r2), ResolveKind::CacheHit);
    assert_eq!(op_counts(&r2), (0, 0));
    assert_eq!(r2.version, r1.version);
    // New commit; staleness 0 on a third replica: L2 latest is stale, used as the re-list base.
    append_commit(&fx, r1.version + 1, "new.parquet", None);
    let c3 = TestServer::start_l2("c", l2.clone()).await;
    let mut cc = c3.client().await;
    let (r3, _) = run(&mut cc, latest_request(&fx, 0, vec![]), Behavior::default())
        .await
        .ok();
    assert_eq!(r3.version, r1.version + 1);
    assert_eq!(
        resolved(&r3),
        ResolveKind::Incremental,
        "re-listed from the L2 base"
    );
}

/// Concurrent replicas on one L2: one build, the other waits on the L2 lock and adopts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn l2_two_replicas_single_build() {
    let fx = fixture("table-with-dv-small");
    let l2 = memory_l2();
    let a = TestServer::start_l2("a", l2.clone()).await;
    let b = TestServer::start_l2("b", l2.clone()).await;
    let mut ca = a.client().await;
    let mut cb = b.client().await;
    let slow = || Behavior {
        op_delay: Duration::from_millis(40),
        ..Default::default()
    };
    // A starts first and holds the lock while its (forwarded-op) build is slow.
    let ra = tokio::spawn({
        let req = request(&fx, 1, vec![spec_json()]);
        async move { run(&mut ca, req, slow()).await }
    });
    a.wait_builds_started(1).await;
    let rb = run(
        &mut cb,
        request(&fx, 1, vec![spec_json()]),
        Behavior::default(),
    )
    .await;
    let (ra, _) = ra.await.unwrap().ok();
    let (rb, ops_b) = rb.ok();
    let (sa, sb) = (a.stats().await, b.stats().await);
    assert_eq!(
        sa.builds_started + sb.builds_started,
        1,
        "one build across replicas"
    );
    assert_eq!(sb.l2_hits, 1);
    assert_eq!(b.core.counters.l2_lock_waits.load(Ordering::Relaxed), 1);
    assert_eq!(ra.info, rb.info);
    assert_eq!(resolved(&rb), ResolveKind::CacheHit);
    // B's snapshot came from L2 (zero ops); its plan may also have come from L2.
    assert!(
        ops_b.iter().all(|o| o.kind == "QueryPlan"),
        "only plan ops at most"
    );
}

/// Never written to L2: staged-tail results and tainted builds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn l2_never_stores_staged_or_tainted() {
    let mem = Arc::new(MemoryStore::new(DEFAULT_L2_TTL));
    let l2: Arc<L2> = mem.clone();
    let server = TestServer::start_l2("a", l2.clone()).await;
    let mut c = server.client().await;
    let fx = cm_fixture();
    cm_publish(&fx, 1);
    let v2 = cm_stage(&fx, 2);
    let (snap, _) = run(&mut c, cm_request(&fx, 2, &[v2]), Behavior::default())
        .await
        .ok();
    run_scan_plan(
        &mut c,
        plan_request(&snap, spec_json()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(
        mem.key_count(),
        0,
        "staged-tail result and its plan not in L2"
    );

    // Tainted: a non-FileNotFound failure on an optional read (the CRC) is tolerated by kernel
    // but taints the build. Forward mode so the client answers the read.
    let fx = fixture("crc-full");
    let fwd = TestServer::start_full(
        "fwd",
        CacheConfig {
            io_mode: delta_kernel_server::cache::IoMode::Forward,
            ..CacheConfig::default()
        },
        Some(l2.clone()),
        true,
    )
    .await;
    let mut c = fwd.client().await;
    let version = latest_version(&fx);
    let out = run(
        &mut c,
        request(&fx, version, vec![spec_json()]),
        Behavior {
            fail_read_suffix: Some((".crc".into(), delta_kernel_server::errors::codes::IO)),
            ..Default::default()
        },
    )
    .await;
    if out.result.is_ok() {
        assert!(
            fwd.core.cached_instances_per_table().is_empty(),
            "tainted: not in L1"
        );
        assert_eq!(mem.key_count(), 0, "tainted: not in L2");
    }
}

/// The SnapshotStore contract, run against L1, the in-process L2 and (tests/l2_redis.rs) Redis.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_store_contract_l1_and_memory() {
    let fx = fixture("table-with-dv-small");
    let snapshot = local_snapshot(&fx, Some(1));
    let info =
        delta_kernel_server::info::compute_snapshot_info(&snapshot, &local_engine()).unwrap();
    let mem = MemoryStore::new(DEFAULT_L2_TTL);
    store_contract(&mem, &info, b"hint-bytes".to_vec()).await;
    latest_contract(&mem, &[&mem]).await;
    let server = TestServer::start().await;
    store_contract(&server.core.l1, &info, snapshot.clone()).await;
}

/// Two replicas, one token: after A's UC exchange, B serves an L2 hit authorized by the shared
/// `authz` entry without calling UC; a different token still goes to UC (and is denied).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn l2_authz_shared_across_replicas() {
    const GOOD: &str = "tok-l2-good";
    const BAD: &str = "tok-l2-bad";
    let uc = MockUc::start();
    uc.answer(
        GOOD,
        UcAnswer::Grant {
            ttl: Duration::from_secs(3600),
        },
    );
    uc.answer(BAD, UcAnswer::Status(403));
    let l2 = memory_l2();
    let config = CacheConfig {
        uc_endpoint_override: Some(uc.base_url.clone()),
        credentials_for_file: true,
        ..CacheConfig::default()
    };
    let a = TestServer::start_full("a", config.clone(), Some(l2.clone()), true).await;
    let b = TestServer::start_full("b", config, Some(l2.clone()), true).await;
    let fx = fixture("table-with-dv-small");
    let mut ca = a.client().await;
    let (ra, _) = run(
        &mut ca,
        with_auth(request(&fx, 1, vec![spec_json()]), GOOD),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(uc.calls().len(), 1);
    let mut cb = b.client().await;
    let (rb, _) = run(
        &mut cb,
        with_auth(request(&fx, 1, vec![spec_json()]), GOOD),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(uc.calls().len(), 1, "B authorized by L2 authz, no UC call");
    assert_eq!(op_counts(&rb), (0, 0));
    assert_eq!(rb.info, ra.info);
    assert_eq!(b.core.counters.l2_authz_hits.load(Ordering::Relaxed), 1);
    let err = run(
        &mut cb,
        with_auth(request(&fx, 1, vec![]), BAD),
        Behavior::default(),
    )
    .await
    .err();
    assert_eq!(err.code, Code::PermissionDenied as i32);
    assert_eq!(uc.calls().len(), 2);
    let _ = SystemTime::now();
}
