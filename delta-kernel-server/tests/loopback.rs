//! L0a loopback tests (SPEC §8): an in-process tonic server on 127.0.0.1:0 and a test client that
//! executes every forwarded op with kernel's in-process `SyncPlanExecutor` over local fixtures.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use delta_kernel_server::cache::CacheConfig;
use delta_kernel_server::errors::codes;
use delta_kernel_server::normalize::normalize_plan_bytes;
use delta_kernel_server::proto::error::Code;
use delta_kernel_server::proto::{self, ResolveKind, scan_plan_result};
use prost::Message as _;
use tokio::sync::Notify;

// ------------------------------------------------------------------------------------------------
// (a) semantic parity over the fixture matrix
// ------------------------------------------------------------------------------------------------

/// FULL_BUILD at the latest version, then parity with the in-process plan and SnapshotInfo.
async fn parity_case(name: &str) -> (Fixture, proto::GetSnapshotResponse, Vec<OpLog>) {
    let fx = fixture(name);
    parity_on(fx).await
}

async fn parity_on(fx: Fixture) -> (Fixture, proto::GetSnapshotResponse, Vec<OpLog>) {
    let server = TestServer::start().await;
    let mut client = server.client().await;
    let version = latest_version(&fx);
    let specs = default_specs();
    let (resp, ops) = run(
        &mut client,
        request(&fx, version, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resp.version, version);
    assert_eq!(resolved(&resp), ResolveKind::FullBuild);
    assert!(ops_issued(&resp) > 0);
    assert_eq!(ops_issued(&resp) as usize, ops.len());
    assert_parity(&fx, &resp, &specs);
    (fx, resp, ops)
}

fn read_urls(ops: &[OpLog]) -> Vec<&str> {
    ops.iter()
        .filter(|o| o.kind == "ReadBytes" || o.kind == "ParquetFooter" || o.kind == "QueryPlan")
        .flat_map(|o| o.urls.iter().map(String::as_str))
        .collect()
}

fn info(resp: &proto::GetSnapshotResponse) -> &proto::SnapshotInfo {
    resp.info.as_ref().unwrap()
}

fn segment(resp: &proto::GetSnapshotResponse) -> &proto::LogSegmentInfo {
    info(resp).log_segment.as_ref().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parity_v2_checkpoint_with_sidecars() {
    let (_fx, resp, ops) = parity_case("v2-checkpoints-parquet-with-sidecars").await;
    assert!(segment(&resp).checkpoint_version.is_some());
    let plans_mention_sidecars = resp
        .plans
        .iter()
        .any(|p| String::from_utf8_lossy(plan_bytes(p).unwrap_or_default()).contains("_sidecars/"));
    let ops_mention_sidecars = read_urls(&ops).iter().any(|u| u.contains("_sidecars/"));
    assert!(
        plans_mention_sidecars || ops_mention_sidecars,
        "sidecar files must be read by an op or scanned by the plan"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parity_v2_checkpoint_without_sidecars() {
    let (_fx, resp, _) = parity_case("v2-checkpoints-parquet-without-sidecars").await;
    assert!(segment(&resp).checkpoint_version.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parity_classic_checkpoint() {
    let (_fx, resp, _) = parity_case("v2-classic-checkpoint-parquet").await;
    assert!(segment(&resp).checkpoint_version.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parity_crc_full() {
    let (_fx, resp, _) = parity_case("crc-full").await;
    assert!(info(&resp).crc.is_some(), "crc-full must expose CRC info");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parity_crc_malformed() {
    // kernel/tests/data/crc-malformed only holds the bad .crc; graft it onto crc-full's log so the
    // table is buildable and kernel has to ignore the unparseable CRC.
    let fx = fixture("crc-full");
    std::fs::copy(
        data_dir().join("crc-malformed/_delta_log/00000000000000000000.crc"),
        fx.log_dir().join("00000000000000000000.crc"),
    )
    .unwrap();
    let (_fx, resp, _) = parity_on(fx).await;
    assert!(
        info(&resp).crc.is_none(),
        "malformed CRC must not be exposed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parity_no_last_checkpoint() {
    // Fewer than 10 commits: `_last_checkpoint` does not exist, so its read comes back as a
    // per-item FileNotFound and the build must still succeed (SPEC B1 regression).
    let (fx, resp, ops) = parity_case("table-without-dv-small").await;
    assert!(!fx.log_dir().join("_last_checkpoint").exists());
    assert!(segment(&resp).checkpoint_version.is_none());
    let fnf = ops.iter().any(|o| {
        o.urls.iter().any(|u| u.ends_with("_last_checkpoint"))
            && o.item_error_codes.contains(&codes::FILE_NOT_FOUND)
    });
    assert!(fnf, "expected a per-item FileNotFound for _last_checkpoint");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parity_checkpoint_without_last_checkpoint_hint() {
    let (_fx, resp, _) = parity_case("with_checkpoint_no_last_checkpoint").await;
    assert_eq!(segment(&resp).checkpoint_version, Some(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parity_deletion_vectors() {
    let (_fx, resp, _) = parity_case("table-with-dv-small").await;
    let features = info(&resp)
        .protocol
        .as_ref()
        .unwrap()
        .reader_features
        .clone()
        .unwrap_or_default()
        .features;
    assert!(
        features.iter().any(|f| f == "deletionVectors"),
        "{features:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parity_column_mapping_name() {
    let (_fx, resp, _) = parity_case("partition_cm/name").await;
    let config = &info(&resp).metadata.as_ref().unwrap().configuration;
    assert_eq!(
        config.get("delta.columnMapping.mode").map(String::as_str),
        Some("name")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parity_column_mapping_id() {
    let (_fx, resp, _) = parity_case("partition_cm/id").await;
    let config = &info(&resp).metadata.as_ref().unwrap().configuration;
    assert_eq!(
        config.get("delta.columnMapping.mode").map(String::as_str),
        Some("id")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parity_partitioned() {
    let (_fx, resp, _) = parity_case("basic_partitioned").await;
    assert_eq!(
        info(&resp).metadata.as_ref().unwrap().partition_columns,
        vec!["letter".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parity_app_txn_checkpoint() {
    let (_fx, resp, _) = parity_case("app-txn-checkpoint").await;
    assert_eq!(segment(&resp).checkpoint_version, Some(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timestamp_partition_with_timezone_is_unsupported() {
    let fx = fixture("timestamp-partitioned-table");
    let server = TestServer::start().await;
    let mut client = server.client().await;
    let version = latest_version(&fx);
    let mut tz = spec_json();
    tz.timestamp_timezone = Some("America/Los_Angeles".into());
    let specs = vec![tz.clone(), spec_json()];
    let (resp, _) = run(
        &mut client,
        request(&fx, version, specs),
        Behavior::default(),
    )
    .await
    .ok();
    match resp.plans[0].result.as_ref().unwrap() {
        scan_plan_result::Result::Unsupported(e) => {
            assert_eq!(e.code, Code::Unsupported as i32);
            assert!(e.message.contains("TIMESTAMP"), "{}", e.message);
        }
        other => panic!("expected unsupported, got {other:?}"),
    }
    // Without a timezone the same table plans normally and matches in-process.
    assert_parity(&fx, &resp_with_plans(&resp, 1..2), &[spec_json()]);
    // The unsupported result is deterministic and cached like any other plan.
    let (again, _) = run(
        &mut client,
        request(&fx, version, vec![tz]),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&again), ResolveKind::CacheHit);
    assert_eq!(ops_issued(&again), 0);
    assert!(again.plans[0].cache_hit);
}

fn resp_with_plans(
    resp: &proto::GetSnapshotResponse,
    range: std::ops::Range<usize>,
) -> proto::GetSnapshotResponse {
    let mut r = resp.clone();
    r.plans = resp.plans[range].to_vec();
    r
}

// ------------------------------------------------------------------------------------------------
// (b) FULL_BUILD -> CACHE_HIT (0 ops) -> INCREMENTAL -> checkpoint
// ------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolve_sequence_full_hit_incremental_checkpoint() {
    let fx = fixture("basic_partitioned");
    let server = TestServer::start().await;
    let mut client = server.client().await;
    let specs = default_specs();
    let v0 = latest_version(&fx);

    let (r1, _) = run(
        &mut client,
        request(&fx, v0, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r1), ResolveKind::FullBuild);
    let rows_v0 = assert_parity(&fx, &r1, &specs);

    let (r2, ops2) = run(
        &mut client,
        request(&fx, v0, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r2), ResolveKind::CacheHit);
    assert_eq!(ops_issued(&r2), 0);
    assert!(ops2.is_empty());
    assert!(r2.plans.iter().all(|p| p.cache_hit));
    assert_eq!(
        r2.info, r1.info,
        "a hit serves the SnapshotInfo computed at build time"
    );

    // A hit also honours the client's expected log segment.
    let mut with_segment = request(&fx, v0, specs.clone());
    with_segment.expected_log_segment = Some(segment(&r1).clone());
    let (r2b, _) = run(&mut client, with_segment, Behavior::default())
        .await
        .ok();
    assert_eq!(resolved(&r2b), ResolveKind::CacheHit);

    let v1 = v0 + 1;
    append_commit(
        &fx,
        v1,
        "letter=z/part-new-1.parquet",
        Some(("letter", "z")),
    );
    let (r3, _) = run(
        &mut client,
        request(&fx, v1, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r3), ResolveKind::Incremental);
    assert_eq!(r3.version, v1);
    let rows_v1 = assert_parity(&fx, &r3, &specs);
    assert_eq!(rows_v1, rows_v0 + specs.len(), "one new live add per plan");

    // Checkpoint at v1, then one more commit: the incremental build must pick up the new
    // checkpoint and still match in-process.
    write_checkpoint(&fx, v1);
    let v2 = v1 + 1;
    append_commit(
        &fx,
        v2,
        "letter=z/part-new-2.parquet",
        Some(("letter", "z")),
    );
    let (r4, _) = run(
        &mut client,
        request(&fx, v2, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert!(
        matches!(
            resolved(&r4),
            ResolveKind::Incremental | ResolveKind::FullBuild
        ),
        "{:?}",
        resolved(&r4)
    );
    assert_eq!(segment(&r4).checkpoint_version, Some(v1));
    assert_parity(&fx, &r4, &specs);

    // A mismatched expected segment is a miss, not a hit.
    let mut stale = request(&fx, v0, specs.clone());
    stale.expected_log_segment = Some(segment(&r4).clone());
    let (r5, _) = run(&mut client, stale, Behavior::default()).await.ok();
    assert_ne!(resolved(&r5), ResolveKind::CacheHit);
}

// ------------------------------------------------------------------------------------------------
// (c) coalescing, (d) takeover
// ------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_streams_coalesce_into_one_build() {
    let fx = Arc::new(fixture("v2-checkpoints-parquet-with-sidecars"));
    let server = TestServer::start().await;
    let before = server.stats().await;
    let version = latest_version(&fx);
    let specs = default_specs();

    // The driver answers slowly, so the build is still in flight when the others arrive.
    let first_op = Arc::new(Notify::new());
    let mut c0 = server.client().await;
    let req = request(&fx, version, specs.clone());
    let driver_behavior = Behavior {
        op_delay: Duration::from_millis(150),
        first_op: Some(first_op.clone()),
        ..Default::default()
    };
    let driver = tokio::spawn(async move { run(&mut c0, req, driver_behavior).await });
    first_op.notified().await;

    let mut waiters = Vec::new();
    for _ in 0..7 {
        let mut c = server.client().await;
        let req = request(&fx, version, specs.clone());
        waiters.push(tokio::spawn(async move {
            run(&mut c, req, Behavior::default()).await
        }));
    }
    let (driver_resp, _) = driver.await.unwrap().ok();
    let mut kinds = vec![resolved(&driver_resp)];
    for w in waiters {
        let (r, _) = w.await.unwrap().ok();
        assert_eq!(r.version, version);
        assert_eq!(r.info, driver_resp.info);
        let norm = |r: &proto::GetSnapshotResponse| -> Vec<Vec<u8>> {
            r.plans
                .iter()
                .map(|p| normalize_plan_bytes(plan_bytes(p).unwrap()).unwrap())
                .collect()
        };
        assert_eq!(norm(&r), norm(&driver_resp));
        kinds.push(resolved(&r));
    }
    let after = server.stats().await;
    assert_eq!(
        after.builds_started - before.builds_started,
        1,
        "exactly one build for 8 concurrent streams"
    );
    assert_eq!(after.builds_coalesced - before.builds_coalesced, 7);
    assert_eq!(kinds[0], ResolveKind::FullBuild);
    assert!(
        kinds[1..].iter().all(|k| *k == ResolveKind::Coalesced),
        "{kinds:?}"
    );
    assert_parity(&fx, &driver_resp, &specs);
}

/// The driving stream dies mid-build; the waiting stream receives the in-flight op and finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn driver_killed_mid_build_waiter_takes_over() {
    let fx = Arc::new(fixture("v2-classic-checkpoint-parquet"));
    let server = TestServer::start().await;
    let before = server.stats().await;
    let version = latest_version(&fx);
    let specs = default_specs();

    let first_op = Arc::new(Notify::new());
    let mut c0 = server.client().await;
    let req = request(&fx, version, specs.clone());
    let driver_behavior = Behavior {
        op_delay: Duration::from_millis(100),
        kill_after_ops: Some(2),
        first_op: Some(first_op.clone()),
        ..Default::default()
    };
    let driver = tokio::spawn(async move { run(&mut c0, req, driver_behavior).await });
    first_op.notified().await;

    let mut c1 = server.client().await;
    let waiter = run(
        &mut c1,
        request(&fx, version, specs.clone()),
        Behavior::default(),
    )
    .await;
    let driver = driver.await.unwrap();
    assert!(matches!(driver.result, Err(Ended::Killed)));
    assert_eq!(driver.ops.len(), 2);
    let (resp, waiter_ops) = waiter.ok();
    assert_eq!(resolved(&resp), ResolveKind::Coalesced);
    assert!(
        !waiter_ops.is_empty(),
        "the waiter must have served the remaining ops"
    );
    let after = server.stats().await;
    assert_eq!(
        after.builds_started - before.builds_started,
        1,
        "no restart"
    );
    assert_parity(&fx, &resp, &specs);
    // The build that survived the takeover is cached.
    let (hit, _) = run(
        &mut c1,
        request(&fx, version, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&hit), ResolveKind::CacheHit);
}

/// Same takeover, but the server kills the driving stream with the op in flight (InjectFault).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn injected_kill_mid_op_hands_op_to_waiter() {
    let fx = Arc::new(fixture("crc-full"));
    let server = TestServer::start().await;
    let version = latest_version(&fx);
    let specs = default_specs();
    server
        .client()
        .await
        .inject_fault(proto::FaultSpec {
            delay_before_response_ms: 0,
            fail_after_n_ops: 1,
            kill_stream_mid_op: true,
        })
        .await
        .unwrap();

    let first_op = Arc::new(Notify::new());
    let mut c0 = server.client().await;
    let req = request(&fx, version, specs.clone());
    let driver_behavior = Behavior {
        op_delay: Duration::from_millis(200),
        first_op: Some(first_op.clone()),
        ..Default::default()
    };
    let driver = tokio::spawn(async move { run(&mut c0, req, driver_behavior).await });
    first_op.notified().await;
    let mut c1 = server.client().await;
    let waiter = run(
        &mut c1,
        request(&fx, version, specs.clone()),
        Behavior::default(),
    )
    .await;
    let driver = driver.await.unwrap();
    assert!(
        matches!(
            driver.result,
            Err(Ended::Status(_)) | Err(Ended::EndOfStream)
        ),
        "driver stream must be killed by the server"
    );
    let (resp, _) = waiter.ok();
    assert_eq!(resolved(&resp), ResolveKind::Coalesced);
    assert_parity(&fx, &resp, &specs);
}

/// With no other waiter, losing the driver fails the build (CANCELLED to nobody) and caches
/// nothing; the next request builds again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn driver_killed_without_waiters_caches_nothing() {
    let fx = fixture("table-with-dv-small");
    let server = TestServer::start().await;
    let version = latest_version(&fx);
    let mut c = server.client().await;
    let killed = run(
        &mut c,
        request(&fx, version, default_specs()),
        Behavior {
            kill_after_ops: Some(1),
            ..Default::default()
        },
    )
    .await;
    assert!(matches!(killed.result, Err(Ended::Killed)));
    // Wait until the abandoned build has unwound.
    for _ in 0..100 {
        if server.core.cached_instances_per_table().is_empty()
            && server.stats().await.builds_started == 1
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (resp, _) = run(
        &mut c,
        request(&fx, version, default_specs()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&resp), ResolveKind::FullBuild);
}

// ------------------------------------------------------------------------------------------------
// (e) OP_FAILED and DEADLINE
// ------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn op_error_is_op_failed() {
    let fx = fixture("table-without-dv-small");
    let server = TestServer::start().await;
    let version = latest_version(&fx);
    let mut c = server.client().await;
    let err = run(
        &mut c,
        request(&fx, version, default_specs()),
        Behavior {
            fail_all_ops_code: Some(codes::IO),
            ..Default::default()
        },
    )
    .await
    .err();
    assert_eq!(err.code, Code::OpFailed as i32, "{err:?}");
    assert!(
        err.message.contains("injected op failure"),
        "{}",
        err.message
    );
    // Nothing poisoned: a healthy client builds fine afterwards.
    let (resp, _) = run(
        &mut c,
        request(&fx, version, default_specs()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&resp), ResolveKind::FullBuild);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_never_answers_is_deadline() {
    let fx = fixture("table-without-dv-small");
    let server = TestServer::start().await;
    let version = latest_version(&fx);
    let mut c = server.client().await;
    let mut req = request(&fx, version, default_specs());
    req.client.as_mut().unwrap().op_deadline_ms = 300;
    let started = std::time::Instant::now();
    let err = run(
        &mut c,
        req,
        Behavior {
            never_answer: true,
            ..Default::default()
        },
    )
    .await
    .err();
    assert_eq!(err.code, Code::Deadline as i32, "{err:?}");
    assert!(err.retryable);
    assert!(started.elapsed() < Duration::from_secs(10));
    let (resp, _) = run(
        &mut c,
        request(&fx, version, default_specs()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&resp), ResolveKind::FullBuild);
}

// ------------------------------------------------------------------------------------------------
// (f) FileNotFound is benign; a transient CRC failure taints the build
// ------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn last_checkpoint_not_found_does_not_taint() {
    let fx = fixture("basic_partitioned");
    let server = TestServer::start().await;
    let version = latest_version(&fx);
    let mut c = server.client().await;
    let (r1, ops) = run(
        &mut c,
        request(&fx, version, default_specs()),
        Behavior::default(),
    )
    .await
    .ok();
    assert!(ops.iter().any(|o| {
        o.urls.iter().any(|u| u.ends_with("_last_checkpoint"))
            && o.item_error_codes == vec![codes::FILE_NOT_FOUND]
    }));
    assert_eq!(resolved(&r1), ResolveKind::FullBuild);
    let (r2, _) = run(
        &mut c,
        request(&fx, version, default_specs()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(
        resolved(&r2),
        ResolveKind::CacheHit,
        "FileNotFound must not taint"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transient_crc_failure_returns_result_but_does_not_cache() {
    let fx = fixture("crc-full");
    let server = TestServer::start().await;
    let version = latest_version(&fx);
    let specs = default_specs();
    let mut c = server.client().await;
    let (r1, ops) = run(
        &mut c,
        request(&fx, version, specs.clone()),
        Behavior {
            fail_read_suffix: Some((".crc".into(), codes::IO)),
            ..Default::default()
        },
    )
    .await
    .ok();
    assert!(
        ops.iter().any(|o| o.item_error_codes.contains(&codes::IO)),
        "the CRC read must have been attempted and failed"
    );
    assert!(
        info(&r1).crc.is_none(),
        "kernel falls back to log replay without the CRC"
    );
    assert!(
        server.core.cached_instances_per_table().is_empty(),
        "tainted build not cached"
    );

    let (r2, _) = run(
        &mut c,
        request(&fx, version, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_ne!(resolved(&r2), ResolveKind::CacheHit);
    assert!(info(&r2).crc.is_some(), "a clean rebuild sees the CRC");
    assert_parity(&fx, &r2, &specs);
    let (r3, _) = run(&mut c, request(&fx, version, specs), Behavior::default())
        .await
        .ok();
    assert_eq!(resolved(&r3), ResolveKind::CacheHit);
}

// ------------------------------------------------------------------------------------------------
// (g) VERSION_MISMATCH
// ------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kernel_rev_or_feature_mismatch_is_version_mismatch() {
    let fx = fixture("table-without-dv-small");
    let server = TestServer::start().await;
    let mut c = server.client().await;

    let mut req = request(&fx, 0, default_specs());
    req.client.as_mut().unwrap().kernel_rev = "0000000000000000000000000000000000000000".into();
    let (outcome_err, ops) = {
        let o = run(&mut c, req, Behavior::default()).await;
        let ops = o.ops.clone();
        (o.err(), ops)
    };
    assert_eq!(outcome_err.code, Code::VersionMismatch as i32);
    assert!(
        ops.is_empty(),
        "no op may be issued before the version check"
    );

    let mut req = request(&fx, 0, default_specs());
    req.client
        .as_mut()
        .unwrap()
        .kernel_features
        .retain(|f| f != "geo-type-in-dev");
    let err = run(&mut c, req, Behavior::default()).await.err();
    assert_eq!(err.code, Code::VersionMismatch as i32);
    assert!(err.message.contains("kernel_features"), "{}", err.message);

    // Feature order does not matter.
    let mut req = request(&fx, 0, default_specs());
    req.client.as_mut().unwrap().kernel_features.reverse();
    run(&mut c, req, Behavior::default()).await.ok();
}

// ------------------------------------------------------------------------------------------------
// (h) eviction
// ------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_table_eviction_beyond_max_instances() {
    let fx = fixture("with_checkpoint_no_last_checkpoint");
    let server = TestServer::start_with(
        CacheConfig {
            max_instances_per_table: 2,
            ..CacheConfig::default()
        },
        true,
    )
    .await;
    let mut c = server.client().await;
    let specs = vec![spec_json()];
    for v in [1, 2, 3] {
        run(&mut c, request(&fx, v, specs.clone()), Behavior::default())
            .await
            .ok();
    }
    let stats = server.stats().await;
    assert_eq!(stats.cached_instances_per_table.get(&fx.table_id), Some(&2));
    // v1 was least recently used and is gone; v3 is still cached.
    let (r3, _) = run(&mut c, request(&fx, 3, specs.clone()), Behavior::default())
        .await
        .ok();
    assert_eq!(resolved(&r3), ResolveKind::CacheHit);
    let (r1, _) = run(&mut c, request(&fx, 1, specs.clone()), Behavior::default())
        .await
        .ok();
    assert_eq!(resolved(&r1), ResolveKind::FullBuild);
    assert_parity(&fx, &r1, &specs);
    assert_eq!(
        server
            .stats()
            .await
            .cached_instances_per_table
            .get(&fx.table_id),
        Some(&2)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_memory_bound_evicts_lru_across_tables() {
    let a = fixture("table-without-dv-small");
    let b = fixture("basic_partitioned");
    let server = TestServer::start_with(
        CacheConfig {
            max_instances_per_table: 2,
            max_cache_bytes: 1,
        },
        true,
    )
    .await;
    let mut c = server.client().await;
    let va = latest_version(&a);
    let vb = latest_version(&b);
    run(
        &mut c,
        request(&a, va, vec![spec_json()]),
        Behavior::default(),
    )
    .await
    .ok();
    run(
        &mut c,
        request(&b, vb, vec![spec_json()]),
        Behavior::default(),
    )
    .await
    .ok();
    let cached = server.stats().await.cached_instances_per_table;
    assert_eq!(cached.values().sum::<i32>(), 1, "{cached:?}");
    assert_eq!(
        cached.get(&b.table_id),
        Some(&1),
        "the most recent instance survives"
    );
}

// ------------------------------------------------------------------------------------------------
// (i) determinism probe
// ------------------------------------------------------------------------------------------------

/// SPEC §8 "前置实测": is a plan's proto encoding byte-stable? Prints the finding; asserts only
/// that normalization makes it stable.
#[test]
fn determinism_probe() {
    let fx = fixture("partition_cm/name");
    let snapshot = local_snapshot(&fx, None);
    for spec in [spec_json(), spec_struct()] {
        let plan = local_plan(&snapshot, &spec).unwrap().unwrap();
        let proto_plan = delta_kernel::plans::proto::plan::Plan::from(&plan);
        let same_object: std::collections::HashSet<Vec<u8>> =
            (0..100).map(|_| proto_plan.encode_to_vec()).collect();
        let same_plan: std::collections::HashSet<Vec<u8>> = (0..100)
            .map(|_| delta_kernel::plans::proto::plan::Plan::from(&plan).encode_to_vec())
            .collect();
        let mut rebuilt_raw = std::collections::HashSet::new();
        let mut rebuilt_norm = std::collections::HashSet::new();
        for _ in 0..100 {
            let p = local_plan(&snapshot, &spec).unwrap().unwrap();
            let bytes = delta_kernel::plans::proto::plan::Plan::from(&p).encode_to_vec();
            rebuilt_norm.insert(normalize_plan_bytes(&bytes).unwrap());
            rebuilt_raw.insert(bytes);
        }
        eprintln!(
            "DETERMINISM_PROBE stats={:?}: distinct raw encodings: same prost object x100 = {}, \
             same kernel Plan re-converted x100 = {}, plan rebuilt x100 = {}; \
             distinct normalized (rebuilt x100) = {}",
            spec.stats.as_ref().unwrap().kind,
            same_object.len(),
            same_plan.len(),
            rebuilt_raw.len(),
            rebuilt_norm.len()
        );
        assert_eq!(rebuilt_norm.len(), 1, "normalized encoding must be stable");
    }
}

// ------------------------------------------------------------------------------------------------
// Test hooks and the binary
// ------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hooks_require_flag() {
    let server = TestServer::start_with(CacheConfig::default(), false).await;
    let status = server
        .client()
        .await
        .ping(proto::PingRequest {})
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_cache_drops_instances() {
    let fx = fixture("table-without-dv-small");
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let v = latest_version(&fx);
    run(
        &mut c,
        request(&fx, v, vec![spec_json()]),
        Behavior::default(),
    )
    .await
    .ok();
    c.reset_cache(proto::ResetCacheRequest {}).await.unwrap();
    let (r, _) = run(
        &mut c,
        request(&fx, v, vec![spec_json()]),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r), ResolveKind::FullBuild);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_endpoint_file_ping_and_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let endpoint = dir.path().join("endpoint");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_delta-kernel-server"))
        .args(["--listen", "127.0.0.1:0", "--instance-id", "bin-test"])
        .arg("--endpoint-file")
        .arg(&endpoint)
        .args(["--enable-test-hooks", "--max-instances-per-table", "3"])
        .spawn()
        .unwrap();
    let mut addr = None;
    for _ in 0..200 {
        if let Ok(s) = std::fs::read_to_string(&endpoint) {
            addr = Some(s.trim().parse().unwrap());
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let addr = addr.expect("endpoint file written");
    let mut c = connect(addr).await;
    let ping = c.ping(proto::PingRequest {}).await.unwrap().into_inner();
    assert_eq!(ping.instance_id, "bin-test");
    let fx = fixture("table-with-dv-small");
    let v = latest_version(&fx);
    let (resp, _) = run(
        &mut c,
        request(&fx, v, default_specs()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resp.stats.as_ref().unwrap().instance_id, "bin-test");
    assert_parity(&fx, &resp, &default_specs());
    // SIGTERM shuts it down cleanly.
    let pid = child.id().to_string();
    std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .unwrap();
    let status = child.wait().unwrap();
    assert!(status.success(), "{status:?}");
}
