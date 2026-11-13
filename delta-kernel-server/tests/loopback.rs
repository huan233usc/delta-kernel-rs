//! L0a loopback tests (SPEC §8): an in-process tonic server on 127.0.0.1:0 and a test client that
//! executes every forwarded op with kernel's in-process `SyncPlanExecutor` over local fixtures.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use delta_kernel_server::cache::{CacheConfig, IoMode};
use delta_kernel_server::errors::codes;
use delta_kernel_server::normalize::normalize_plan_bytes;
use delta_kernel_server::proto::error::Code;
use delta_kernel_server::proto::{self, ResolveKind, scan_plan_result};
use prost::Message as _;
use tokio::sync::Notify;

// ------------------------------------------------------------------------------------------------
// (a) semantic parity over the fixture matrix
// ------------------------------------------------------------------------------------------------

/// FULL_BUILD at the latest version (default `local` IO mode), then parity with the in-process
/// plan and SnapshotInfo.
async fn parity_case(name: &str) -> (Fixture, proto::GetSnapshotResponse, Vec<OpLog>) {
    parity_on_mode(fixture(name), IoMode::Local).await
}

async fn parity_on(fx: Fixture) -> (Fixture, proto::GetSnapshotResponse, Vec<OpLog>) {
    parity_on_mode(fx, IoMode::Local).await
}

/// Parity in `mode`. Local mode: the client sees only QueryPlan ops and the server ran the rest.
async fn parity_on_mode(
    fx: Fixture,
    mode: IoMode,
) -> (Fixture, proto::GetSnapshotResponse, Vec<OpLog>) {
    let server = TestServer::start_mode(mode).await;
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
    assert_eq!(
        ops_issued(&resp) as usize,
        ops.len(),
        "forwarded = ops the client saw"
    );
    match mode {
        IoMode::Local => {
            assert!(
                ops_local(&resp) > 0,
                "listing / _last_checkpoint ran on the server"
            );
            assert!(
                ops.iter().all(|o| o.kind == "QueryPlan"),
                "local mode forwards QueryPlan only: {:?}",
                ops.iter().map(|o| o.kind).collect::<Vec<_>>()
            );
        }
        IoMode::Forward => {
            assert!(ops_issued(&resp) > 0);
            assert_eq!(ops_local(&resp), 0);
        }
    }
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
    let (_fx, local, _) = parity_case("v2-checkpoints-parquet-with-sidecars").await;
    assert!(segment(&local).checkpoint_version.is_some());
    // Forward mode shows which files are read: sidecars must be among them.
    let (_fx, resp, ops) = parity_on_mode(
        fixture("v2-checkpoints-parquet-with-sidecars"),
        IoMode::Forward,
    )
    .await;
    assert_eq!(info(&resp).metadata, info(&local).metadata);
    assert_eq!(info(&resp).crc, info(&local).crc);
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
    // per-item FileNotFound and the build must still succeed (SPEC B1 regression). Local mode is
    // covered by `local_last_checkpoint_not_found_is_benign`; forward mode shows the item here.
    let (fx, resp, ops) = parity_on_mode(fixture("table-without-dv-small"), IoMode::Forward).await;
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
        request_with_segment(&fx, version, vec![tz], &resp),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&again), ResolveKind::CacheHit);
    assert_eq!(
        op_counts(&again),
        (0, 0),
        "cache hit: nothing forwarded, nothing local"
    );
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

    // SPEC v8: same version without an expected log segment hits the cached instance.
    let (r2_absent, absent_ops) = run(
        &mut client,
        request(&fx, v0, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r2_absent), ResolveKind::CacheHit);
    assert!(absent_ops.is_empty());
    assert_eq!(segment(&r2_absent), segment(&r1));

    let (r2, ops2) = run(
        &mut client,
        request_with_segment(&fx, v0, specs.clone(), &r1),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r2), ResolveKind::CacheHit);
    assert_eq!(
        op_counts(&r2),
        (0, 0),
        "cache hit: nothing forwarded, nothing local"
    );
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
    coalesce_case(IoMode::Local).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_streams_coalesce_into_one_build_forward() {
    coalesce_case(IoMode::Forward).await;
}

async fn coalesce_case(mode: IoMode) {
    let fx = Arc::new(fixture("v2-checkpoints-parquet-with-sidecars"));
    let server = TestServer::start_mode(mode).await;
    let before = server.stats().await;
    let version = latest_version(&fx);
    let specs = default_specs();

    // The build is slow (slow client answers, or slow server-side IO), so it is still in flight
    // when the others arrive.
    let first_op = Arc::new(Notify::new());
    let mut c0 = server.client().await;
    let req = request(&fx, version, specs.clone());
    let driver_behavior = match mode {
        IoMode::Forward => Behavior {
            op_delay: Duration::from_millis(150),
            first_op: Some(first_op.clone()),
            ..Default::default()
        },
        IoMode::Local => {
            server
                .core
                .faults
                .set_local_io_delay(Duration::from_millis(150));
            Behavior::default()
        }
    };
    let driver = tokio::spawn(async move { run(&mut c0, req, driver_behavior).await });
    match mode {
        IoMode::Forward => first_op.notified().await,
        IoMode::Local => server.wait_builds_started(before.builds_started + 1).await,
    }

    let mut waiters = Vec::new();
    for _ in 0..7 {
        let mut c = server.client().await;
        let req = request(&fx, version, specs.clone());
        waiters.push(tokio::spawn(async move {
            run(&mut c, req, Behavior::default()).await
        }));
    }
    let (driver_resp, _) = driver.await.unwrap().ok();
    server.core.faults.set_local_io_delay(Duration::ZERO);
    let mut kinds = vec![resolved(&driver_resp)];
    let mut local_ops = ops_local(&driver_resp) as i64;
    for w in waiters {
        let (r, _) = w.await.unwrap().ok();
        assert_eq!(r.version, version);
        // A waiter may start a plan job (plans coalesce separately), so it can own local ops.
        local_ops += ops_local(&r) as i64;
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
    assert_eq!(
        after.ops_local_total - before.ops_local_total,
        local_ops,
        "every server-side op is attributed to exactly one request"
    );
    if mode == IoMode::Local {
        assert!(ops_local(&driver_resp) > 0, "the driver ran the build's IO");
    }
    assert_eq!(kinds[0], ResolveKind::FullBuild);
    assert!(
        kinds[1..].iter().all(|k| *k == ResolveKind::Coalesced),
        "{kinds:?}"
    );
    assert_parity(&fx, &driver_resp, &specs);
}

/// The driving stream dies mid-build; the waiting stream receives the in-flight op and finishes.
/// In local mode only QueryPlan ops travel over streams, so the fixture must issue several.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn driver_killed_mid_build_waiter_takes_over() {
    takeover_case(IoMode::Local, "with_checkpoint_no_last_checkpoint").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn driver_killed_mid_build_waiter_takes_over_forward() {
    takeover_case(IoMode::Forward, "v2-classic-checkpoint-parquet").await;
}

async fn takeover_case(mode: IoMode, name: &str) {
    let fx = Arc::new(fixture(name));
    let server = TestServer::start_mode(mode).await;
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
    assert!(matches!(driver.result, Err(Ended::Killed(_))));
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
        request_with_segment(&fx, version, specs.clone(), &resp),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&hit), ResolveKind::CacheHit);
}

/// Same takeover, but the server kills the driving stream with the op in flight (InjectFault).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn injected_kill_mid_op_hands_op_to_waiter() {
    injected_kill_case(IoMode::Local, "with_checkpoint_no_last_checkpoint").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn injected_kill_mid_op_hands_op_to_waiter_forward() {
    injected_kill_case(IoMode::Forward, "crc-full").await;
}

async fn injected_kill_case(mode: IoMode, name: &str) {
    let fx = Arc::new(fixture(name));
    let server = TestServer::start_mode(mode).await;
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
    assert!(matches!(killed.result, Err(Ended::Killed(_))));
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
    assert!(
        err.message
            .starts_with(delta_kernel_server::errors::OP_FAILED_PREFIX)
            && err.message.contains("(IO error)")
            && !err.message.contains("(code "),
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
    let server = TestServer::start_mode(IoMode::Forward).await;
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
        request_with_segment(&fx, version, default_specs(), &r1),
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

/// Forward mode: the client reports a per-item IO error for the CRC read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transient_crc_failure_returns_result_but_does_not_cache() {
    transient_crc_case(IoMode::Forward).await;
}

/// Local mode: the server's own read of the CRC fails (file unreadable, not FileNotFound).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_io_error_taints_and_is_not_cached() {
    transient_crc_case(IoMode::Local).await;
}

async fn transient_crc_case(mode: IoMode) {
    let fx = fixture("crc-full");
    let server = TestServer::start_mode(mode).await;
    let version = latest_version(&fx);
    let specs = default_specs();
    let mut c = server.client().await;
    let (r1, ops) = match mode {
        IoMode::Forward => {
            let out = run(
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
                out.1
                    .iter()
                    .any(|o| o.item_error_codes.contains(&codes::IO)),
                "the CRC read must have been attempted and failed"
            );
            out
        }
        IoMode::Local => {
            let _guard = Unreadable::new(fx.log_dir().join("00000000000000000000.crc"));
            run(
                &mut c,
                request(&fx, version, specs.clone()),
                Behavior::default(),
            )
            .await
            .ok()
        }
    };
    if mode == IoMode::Local {
        // Without the CRC kernel replays the log: those reads are the only forwarded ops.
        assert!(ops.iter().all(|o| o.kind == "QueryPlan"));
        assert_eq!(ops_issued(&r1) as usize, ops.len());
        assert!(ops_local(&r1) > 0);
    }
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
    let (r3, _) = run(
        &mut c,
        request_with_segment(&fx, version, specs, &r2),
        Behavior::default(),
    )
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
    let mut responses = Vec::new();
    for v in [1, 2, 3] {
        let (r, _) = run(&mut c, request(&fx, v, specs.clone()), Behavior::default())
            .await
            .ok();
        responses.push(r);
    }
    let stats = server.stats().await;
    assert_eq!(stats.cached_instances_per_table.get(&fx.table_id), Some(&2));
    // v1 was least recently used and is gone; v3 is still cached.
    let (r3, _) = run(
        &mut c,
        request_with_segment(&fx, 3, specs.clone(), &responses[2]),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r3), ResolveKind::CacheHit);
    let (r1, _) = run(
        &mut c,
        request_with_segment(&fx, 1, specs.clone(), &responses[0]),
        Behavior::default(),
    )
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
            ..CacheConfig::default()
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
        // Acceptance rule (SPEC v6): compare only normalized bytes or executed rows. Re-encoding
        // one prost object is stable; the raw counts across conversions are recorded above (the
        // ALL_STRUCT plan typically shows 2) but not asserted, since they depend on HashMap seeds.
        assert_eq!(
            same_object.len(),
            1,
            "re-encoding one prost object must be stable"
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

// ------------------------------------------------------------------------------------------------
// Round-1 review additions
// ------------------------------------------------------------------------------------------------

/// B1: a FileListing op (which carries a ListingHint) is in flight when the driver dies. The next
/// waiter must receive the very same op: same op_id, same Operation bytes, same hint.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handoff_resends_same_listing_op_with_same_hint() {
    // FileListing is only forwarded in `forward` IO mode.
    let fx = Arc::new(fixture("with_checkpoint_no_last_checkpoint"));
    let server = TestServer::start_mode(IoMode::Forward).await;
    let version = latest_version(&fx);
    let specs = default_specs();

    let first_op = Arc::new(Notify::new());
    let mut c0 = server.client().await;
    let req = request(&fx, version, specs.clone());
    let driver_behavior = Behavior {
        op_delay: Duration::from_millis(200),
        kill_on_kind: Some("FileListing"),
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
    let Err(Ended::Killed(in_flight)) = driver.result else {
        panic!("driver must have dropped its stream on the FileListing op");
    };
    assert_eq!(op_kind(&in_flight.operation), "FileListing");
    let hint = in_flight
        .listing_hint
        .expect("_delta_log listing carries a hint");
    assert_eq!(hint.end_version, Some(version));
    assert!(hint.stop_at_first_non_versioned);

    let (resp, waiter_ops) = waiter.ok();
    let first = &waiter_ops[0];
    assert_eq!(
        first.op_id, in_flight.op_id,
        "same op handed over, not a new one"
    );
    assert_eq!(first.operation, in_flight.operation);
    assert_eq!(first.listing_hint, in_flight.listing_hint);
    assert_eq!(resolved(&resp), ResolveKind::Coalesced);
    assert_parity(&fx, &resp, &specs);
}

/// B2 (as amended by SPEC v8): with `expected_log_segment` the hit needs a canonical match;
/// without it the cached instance is returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_version_hit_requires_matching_expected_segment() {
    let fx = fixture("table-with-dv-small");
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let v = latest_version(&fx);
    let specs = default_specs();
    let (r1, _) = run(&mut c, request(&fx, v, specs.clone()), Behavior::default())
        .await
        .ok();
    assert_eq!(resolved(&r1), ResolveKind::FullBuild);

    // SPEC v8: absent segment -> the cached instance, 0 ops, its segment in the response.
    let (absent, absent_ops) = run(&mut c, request(&fx, v, specs.clone()), Behavior::default())
        .await
        .ok();
    assert_eq!(
        resolved(&absent),
        ResolveKind::CacheHit,
        "absent segment hits"
    );
    assert_eq!(
        op_counts(&absent),
        (0, 0),
        "cache hit: nothing forwarded, nothing local"
    );
    assert!(absent_ops.is_empty());
    assert_eq!(segment(&absent), segment(&r1));

    let mut wrong = segment(&r1).clone();
    wrong.deltas[0].size += 1;
    let mut req = request(&fx, v, specs.clone());
    req.expected_log_segment = Some(wrong);
    let (mismatch, _) = run(&mut c, req, Behavior::default()).await.ok();
    assert_ne!(
        resolved(&mismatch),
        ResolveKind::CacheHit,
        "mismatched segment is a miss"
    );

    let (hit, ops) = run(
        &mut c,
        request_with_segment(&fx, v, specs.clone(), &r1),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&hit), ResolveKind::CacheHit);
    assert!(ops.is_empty());
    // The misses did not add duplicate instances for the same (version, segment).
    assert_eq!(
        server.core.cached_instances_per_table().get(&fx.table_id),
        Some(&1)
    );
}

const CL_TAIL_A: &[&str] = &["00000000000000000003.json"];
const CL_TAIL_B: &[&str] = &["00000000000000000002.json", "00000000000000000003.json"];

/// B3: identical non-staged catalog tail -> CACHE_HIT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_latest_identical_tail_hits() {
    let fx = fixture("with_checkpoint_no_last_checkpoint");
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let specs = default_specs();
    let (r1, _) = run(
        &mut c,
        catalog_request(&fx, 3, CL_TAIL_A, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(r1.version, 3);
    assert_eq!(resolved(&r1), ResolveKind::FullBuild);
    assert_parity(&fx, &r1, &specs);
    let (r2, ops) = run(
        &mut c,
        catalog_request(&fx, 3, CL_TAIL_A, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r2), ResolveKind::CacheHit);
    assert!(ops.is_empty());
}

/// B3: same max_catalog_version, different tail -> no shared snapshot, neither from cache nor
/// from an in-flight build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_latest_different_tail_does_not_share() {
    let fx = Arc::new(fixture("with_checkpoint_no_last_checkpoint"));
    let server = TestServer::start().await;
    let specs = default_specs();
    let mut c = server.client().await;
    run(
        &mut c,
        catalog_request(&fx, 3, CL_TAIL_A, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    let (rb, _) = run(
        &mut c,
        catalog_request(&fx, 3, CL_TAIL_B, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(
        resolved(&rb),
        ResolveKind::FullBuild,
        "cached tail A must not serve tail B"
    );
    // Exact-version tail-less lookups still find a matching instance by segment.
    let (exact, _) = run(
        &mut c,
        request_with_segment(&fx, 3, specs.clone(), &rb),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&exact), ResolveKind::CacheHit);

    // In flight: a slow tail-A build must not absorb a concurrent tail-B request.
    server.core.reset_cache();
    let before = server.stats().await;
    let first_op = Arc::new(Notify::new());
    let mut c0 = server.client().await;
    let req = catalog_request(&fx, 3, CL_TAIL_A, specs.clone());
    let slow = Behavior {
        op_delay: Duration::from_millis(150),
        first_op: Some(first_op.clone()),
        ..Default::default()
    };
    let a = tokio::spawn(async move { run(&mut c0, req, slow).await });
    first_op.notified().await;
    let (b, _) = run(
        &mut c,
        catalog_request(&fx, 3, CL_TAIL_B, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    a.await.unwrap().ok();
    assert_eq!(resolved(&b), ResolveKind::FullBuild);
    let after = server.stats().await;
    assert_eq!(after.builds_started - before.builds_started, 2);
    assert_eq!(after.builds_coalesced - before.builds_coalesced, 0);
}

/// B3: a tail with a staged commit is neither cached nor coalesced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_latest_staged_tail_not_cached_not_coalesced() {
    let fx = Arc::new(fixture("with_checkpoint_no_last_checkpoint"));
    let staged_dir = fx.log_dir().join("_staged_commits");
    std::fs::create_dir_all(&staged_dir).unwrap();
    let staged = "_staged_commits/00000000000000000004.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json";
    std::fs::write(
        fx.log_dir().join(staged),
        "{\"commitInfo\":{\"timestamp\":1700000000004,\"operation\":\"WRITE\"}}\n\
         {\"add\":{\"path\":\"staged-part.parquet\",\"partitionValues\":{},\"size\":10,\
         \"modificationTime\":1700000000004,\"dataChange\":true}}\n",
    )
    .unwrap();
    let server = TestServer::start().await;
    let specs = default_specs();
    let mut c = server.client().await;

    let (r1, _) = run(
        &mut c,
        catalog_request(&fx, 4, &[staged], specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(r1.version, 4);
    assert_eq!(segment(&r1).delta_is_staged.last(), Some(&true));
    assert!(
        server.core.cached_instances_per_table().is_empty(),
        "staged: not cached"
    );
    let (r2, _) = run(
        &mut c,
        catalog_request(&fx, 4, &[staged], specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r2), ResolveKind::FullBuild);

    let before = server.stats().await;
    let first_op = Arc::new(Notify::new());
    let mut c0 = server.client().await;
    let req = catalog_request(&fx, 4, &[staged], specs.clone());
    let slow = Behavior {
        op_delay: Duration::from_millis(150),
        first_op: Some(first_op.clone()),
        ..Default::default()
    };
    let a = tokio::spawn(async move { run(&mut c0, req, slow).await });
    first_op.notified().await;
    let (b, _) = run(
        &mut c,
        catalog_request(&fx, 4, &[staged], specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    a.await.unwrap().ok();
    assert_eq!(
        resolved(&b),
        ResolveKind::FullBuild,
        "staged: not coalesced"
    );
    let after = server.stats().await;
    assert_eq!(after.builds_started - before.builds_started, 2);
    assert_eq!(after.builds_coalesced - before.builds_coalesced, 0);
    assert!(server.core.cached_instances_per_table().is_empty());
}

// ------------------------------------------------------------------------------------------------
// Catalog-managed tables: `catalog_latest` parity with the in-process (JNR) build
// ------------------------------------------------------------------------------------------------

/// Version 0 of a `catalogManaged` table (which requires in-commit timestamps).
const CM_V0: &str = r#"{"commitInfo":{"timestamp":1700000000000,"inCommitTimestamp":1700000000000,"operation":"CREATE TABLE","txnId":"cm-0"}}
{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["catalogManaged"],"writerFeatures":["catalogManaged","inCommitTimestamp"]}}
{"metaData":{"id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableInCommitTimestamps":"true","io.unitycatalog.tableId":"cm-table"},"createdTime":1700000000000}}
"#;

/// A catalog-managed table with only its (published) version 0.
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

/// One commit adding `file_<version>.parquet`.
fn cm_commit(version: i64) -> String {
    let ts = 1_700_000_000_000i64 + version;
    format!(
        "{{\"commitInfo\":{{\"timestamp\":{ts},\"inCommitTimestamp\":{ts},\
         \"operation\":\"WRITE\",\"txnId\":\"cm-{version}\"}}}}\n\
         {{\"add\":{{\"path\":\"file_{version}.parquet\",\"partitionValues\":{{}},\"size\":1234,\
         \"modificationTime\":{ts},\"dataChange\":true,\"stats\":\"{{\\\"numRecords\\\":1}}\"}}}}\n"
    )
}

/// Publishes `version` (writes `_delta_log/<version>.json`); returns the tail-relative name.
fn cm_publish(fx: &Fixture, version: i64) -> String {
    let name = format!("{version:020}.json");
    std::fs::write(fx.log_dir().join(&name), cm_commit(version)).unwrap();
    name
}

/// Stages `version` under `_staged_commits/`; returns the tail-relative name.
fn cm_stage(fx: &Fixture, version: i64) -> String {
    let name = format!("_staged_commits/{version:020}.6e5c4a07-8a1f-4b4e-9d5c-{version:012}.json");
    std::fs::write(fx.log_dir().join(&name), cm_commit(version)).unwrap();
    name
}

fn cm_request(
    fx: &Fixture,
    max_catalog_version: i64,
    tail: &[String],
    specs: &[proto::ScanPlanSpec],
) -> proto::GetSnapshotRequest {
    let tail: Vec<&str> = tail.iter().map(String::as_str).collect();
    catalog_request(fx, max_catalog_version, &tail, specs.to_vec())
}

/// Live add paths of the JSON-stats plan of a response.
fn live_paths(resp: &proto::GetSnapshotResponse) -> Vec<String> {
    let bytes = plan_bytes(&resp.plans[0]).expect("non-empty plan");
    rows_sorted_by_path(&execute_plan(
        delta_kernel_server::decode::decode_plan(bytes).unwrap(),
    ))
    .iter()
    .map(|row| {
        row.get("path")
            .or_else(|| row.get("add").and_then(|a| a.get("path")))
            .and_then(|p| p.as_str())
            .unwrap()
            .to_string()
    })
    .collect()
}

/// CM2/CM3 at L0: the server's cache lags UC (it holds the published v1); UC's tail fills in
/// unpublished commits. The staged-tail build equals the in-process JNR build
/// (`with_log_tail` + `with_max_catalog_version`) and is neither cached nor coalesced, while the
/// lagging cached instance stays served for its own catalog state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_latest_staged_tail_matches_in_process_build_and_is_not_cached() {
    let fx = cm_fixture();
    cm_publish(&fx, 1);
    // v2 is ratified, staged and already backfilled (UC still lists the staged file); v3 is
    // ratified but unpublished.
    let v2_staged = cm_stage(&fx, 2);
    cm_publish(&fx, 2);
    let v3_staged = cm_stage(&fx, 3);
    let server = TestServer::start().await;
    let specs = default_specs();
    let mut c = server.client().await;

    // The cache lags: UC once reported v1 with nothing unpublished.
    let lag_req = cm_request(&fx, 1, &[], &specs);
    let (lag, _) = run(&mut c, lag_req.clone(), Behavior::default()).await.ok();
    assert_eq!((lag.version, resolved(&lag)), (1, ResolveKind::FullBuild));
    assert_parity_with(
        &local_catalog_snapshot(&fx, &lag_req).unwrap(),
        &lag,
        &specs,
    );
    assert_eq!(
        server.core.cached_instances_per_table().get(&fx.table_id),
        Some(&1)
    );

    for tail in [
        vec![v2_staged.clone(), v3_staged.clone()],
        // Mixed: v2 named by its published file, v3 still staged.
        vec![format!("{:020}.json", 2), v3_staged.clone()],
    ] {
        let before = server.stats().await;
        let req = cm_request(&fx, 3, &tail, &specs);
        let (r, ops) = run(&mut c, req.clone(), Behavior::default()).await.ok();
        assert_eq!(r.version, 3);
        assert_eq!(resolved(&r), ResolveKind::FullBuild, "{tail:?}");
        assert!(ops.iter().all(|o| o.kind == "QueryPlan"), "{ops:?}");
        // Equal to the in-process build JNR performs, including SnapshotInfo and the plans.
        let local = local_catalog_snapshot(&fx, &req).unwrap();
        assert_parity_with(&local, &r, &specs);
        assert_eq!(
            live_paths(&r),
            ["file_1.parquet", "file_2.parquet", "file_3.parquet"]
        );
        let seg = segment(&r);
        assert_eq!(seg.delta_is_staged.last(), Some(&true));
        assert!(
            seg.deltas
                .last()
                .unwrap()
                .location
                .contains("/_staged_commits/"),
            "the staged tail entry is the segment's last delta"
        );
        let after = server.stats().await;
        assert_eq!(after.builds_started - before.builds_started, 1);
        assert_eq!(
            server.core.cached_instances_per_table().get(&fx.table_id),
            Some(&1),
            "a staged-tail instance is never cached"
        );
        // Asking again builds again.
        let (again, _) = run(&mut c, req, Behavior::default()).await.ok();
        assert_eq!(resolved(&again), ResolveKind::FullBuild);
        assert_eq!(again.info, r.info);
    }

    // The lagging instance still serves its own (published-only) catalog state.
    let (hit, ops) = run(&mut c, lag_req, Behavior::default()).await.ok();
    assert_eq!((hit.version, resolved(&hit)), (1, ResolveKind::CacheHit));
    assert!(ops.is_empty());
    assert_eq!(hit.info, lag.info);
}

/// CM1 at L0: a published-only tail is cached under its canonical identity. The bound wins over
/// a newer unratified file on disk, a repeat is a 0-op CACHE_HIT, and a different tail at the same
/// version is a different entry. Both equal the in-process JNR build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_latest_published_tail_matches_in_process_build_and_hits() {
    let fx = cm_fixture();
    let tail = vec![cm_publish(&fx, 1), cm_publish(&fx, 2)];
    // Written by a writer whose commit UC never ratified: invisible at max_catalog_version 2.
    cm_publish(&fx, 3);
    let server = TestServer::start().await;
    let specs = default_specs();
    let mut c = server.client().await;

    for (n, tail) in [tail, vec![]].into_iter().enumerate() {
        let req = cm_request(&fx, 2, &tail, &specs);
        let (r1, _) = run(&mut c, req.clone(), Behavior::default()).await.ok();
        assert_eq!(r1.version, 2);
        assert_eq!(resolved(&r1), ResolveKind::FullBuild, "{tail:?}");
        let local = local_catalog_snapshot(&fx, &req).unwrap();
        assert_parity_with(&local, &r1, &specs);
        assert_eq!(live_paths(&r1), ["file_1.parquet", "file_2.parquet"]);
        assert!(segment(&r1).delta_is_staged.iter().all(|s| !s));

        let (r2, ops) = run(&mut c, req, Behavior::default()).await.ok();
        assert_eq!(resolved(&r2), ResolveKind::CacheHit);
        assert_eq!(op_counts(&r2), (0, 0));
        assert!(ops.is_empty());
        assert_eq!(r2.info, r1.info);
        assert_parity_with(&local, &r2, &specs);
        assert_eq!(
            server.core.cached_instances_per_table().get(&fx.table_id),
            Some(&(n as i32 + 1)),
            "one instance per canonical tail"
        );
    }
    assert_eq!(server.stats().await.builds_started, 2);
}

/// A latest re-listing that finds no new commit resolves to CACHE_HIT and is a renewal, not a
/// build; one that finds a new commit is a build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn latest_renewal_counts_as_renewal_not_build() {
    let fx = fixture("basic_partitioned");
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let specs = default_specs();
    let counts = |s: &proto::DebugStats| (s.builds_started, s.latest_renewals);

    let (r1, _) = run(
        &mut c,
        latest_request(&fx, 0, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r1), ResolveKind::FullBuild);
    assert_eq!(counts(&server.stats().await), (1, 0));

    let (r2, _) = run(
        &mut c,
        latest_request(&fx, 0, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r2), ResolveKind::CacheHit);
    assert_eq!(op_counts(&r2), (0, 1), "one renewal listing");
    assert_eq!(counts(&server.stats().await), (1, 1));

    // Within staleness: no listing at all, neither counter moves.
    let (r3, _) = run(
        &mut c,
        latest_request(&fx, 60_000, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r3), ResolveKind::CacheHit);
    assert_eq!(counts(&server.stats().await), (1, 1));

    append_commit(&fx, r1.version + 1, "new.parquet", Some(("letter", "z")));
    let (r4, _) = run(&mut c, latest_request(&fx, 0, specs), Behavior::default())
        .await
        .ok();
    assert_eq!(resolved(&r4), ResolveKind::Incremental);
    assert_eq!(counts(&server.stats().await), (2, 1));
    assert_eq!(
        server
            .core
            .counters
            .latest_relists_started
            .load(std::sync::atomic::Ordering::Relaxed),
        2
    );
}

/// N1: the server sends a ListingHint for `_delta_log` listings and the client's truncated
/// result still yields the correct snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listing_hint_is_sent_and_honored() {
    // Client-side hint handling (forward mode); server-side: `listing_hint_honored_server_side`.
    let fx = fixture("with_checkpoint_no_last_checkpoint");
    let server = TestServer::start_mode(IoMode::Forward).await;
    let mut c = server.client().await;
    let specs = default_specs();
    let (resp, ops) = run(&mut c, request(&fx, 1, specs.clone()), Behavior::default())
        .await
        .ok();
    let listings: Vec<&OpLog> = ops.iter().filter(|o| o.kind == "FileListing").collect();
    assert!(!listings.is_empty());
    for l in &listings {
        let hint = l
            .listing_hint
            .expect("every _delta_log listing carries a hint");
        assert_eq!(hint.end_version, Some(1));
        assert!(hint.stop_at_first_non_versioned);
        assert!(
            l.returned_files
                .iter()
                .all(|f| file_version(f).is_some_and(|v| v <= 1)),
            "{:?}",
            l.returned_files
        );
    }
    // The log dir has commits 2, 3 and a checkpoint at 2; the hint cut them off.
    assert!(fx.log_dir().join("00000000000000000003.json").exists());
    assert_eq!(resp.version, 1);
    assert_parity(&fx, &resp, &specs);

    // Sidecar directory listings get no hint (their file names are not versioned).
    let sc = fixture("v2-checkpoints-parquet-with-sidecars");
    let (_, ops) = run(
        &mut c,
        request(&sc, latest_version(&sc), specs),
        Behavior::default(),
    )
    .await
    .ok();
    // Kernel reads sidecars by path from the checkpoint manifest and never lists
    // `_delta_log/_sidecars/`; the hint rule for such a listing is unit-tested in
    // `executor::tests::listing_hint_only_for_delta_log_root`. Here: the sidecars were actually
    // read, and every listing that did happen is a `_delta_log/` listing carrying a hint.
    assert!(
        ops.iter()
            .any(|o| o.urls.iter().any(|u| u.contains("/_delta_log/_sidecars/"))),
        "the v2 checkpoint's sidecars must be read"
    );
    let listings: Vec<&OpLog> = ops.iter().filter(|o| o.kind == "FileListing").collect();
    assert!(!listings.is_empty());
    for l in listings {
        assert!(l.urls.iter().all(|u| !u.contains("/_sidecars/")));
        assert!(l.listing_hint.is_some(), "{:?}", l.urls);
    }
}

/// N2: a missed op deadline fails the whole build for the driver and every waiter; nothing is
/// handed over and nothing is cached.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadline_fails_driver_and_all_waiters_without_handoff() {
    let fx = Arc::new(fixture("table-without-dv-small"));
    let server = TestServer::start().await;
    let version = latest_version(&fx);
    let first_op = Arc::new(Notify::new());
    let mut c0 = server.client().await;
    let mut req = request(&fx, version, default_specs());
    req.client.as_mut().unwrap().op_deadline_ms = 500;
    let silent = Behavior {
        never_answer: true,
        first_op: Some(first_op.clone()),
        ..Default::default()
    };
    let driver = tokio::spawn(async move { run(&mut c0, req, silent).await });
    first_op.notified().await;
    let mut waiters = Vec::new();
    for _ in 0..3 {
        let mut c = server.client().await;
        let req = request(&fx, version, default_specs());
        waiters.push(tokio::spawn(async move {
            run(&mut c, req, Behavior::default()).await
        }));
    }
    let d = driver.await.unwrap().err();
    assert_eq!(d.code, Code::Deadline as i32, "{d:?}");
    for w in waiters {
        let o = w.await.unwrap();
        assert!(o.ops.is_empty(), "no op may be handed over on a deadline");
        let e = o.err();
        assert_eq!(e.code, Code::Deadline as i32, "{e:?}");
    }
    assert!(server.core.cached_instances_per_table().is_empty());
    assert_eq!(server.stats().await.builds_started, 1);
}

// ------------------------------------------------------------------------------------------------
// Round-3: L1 integration findings and the v7 latest cache
// ------------------------------------------------------------------------------------------------

/// Builds a request the way DBR's `RemoteKernelClient` does: Hadoop-qualified table location and
/// segment locations.
fn client_request(
    fx: &Fixture,
    version: i64,
    segment: proto::LogSegmentInfo,
) -> proto::GetSnapshotRequest {
    let mut req = request(fx, version, default_specs());
    req.table.as_mut().unwrap().location = hadoop_table_location(fx);
    req.expected_log_segment = Some(segment);
    req
}

/// F1 (`P1A-CACHE-EXACT-NO-HIT`): the DBR client re-qualifies every location through Hadoop
/// (`file:///p` -> `file:/p`) before sending `expected_log_segment`, so a raw proto comparison
/// never matched. The table directory has a space to cover escaping as well.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_hit_with_client_qualified_segment() {
    for name in ["v2-classic-checkpoint-parquet", "table-without-dv-small"] {
        let fx = fixture_at_tar_or_dir(name);
        let server = TestServer::start().await;
        let mut c = server.client().await;
        let v = latest_version(&fx);

        // First request: the segment of the client's own in-process snapshot (JNR), in client
        // form and with a coarser mtime, exactly like assertSnapshotAndPlanParity.
        let jnr = local_snapshot(&fx, Some(v));
        let jnr_seg = delta_kernel_server::info::log_segment_info(jnr.log_segment());
        let (r1, _) = run(
            &mut c,
            client_request(&fx, v, client_form_segment(&jnr_seg, true)),
            Behavior::default(),
        )
        .await
        .ok();
        assert_eq!(resolved(&r1), ResolveKind::FullBuild, "{name}");
        assert!(
            first_file(segment(&r1)).location.starts_with("file:///"),
            "server reports url-crate form"
        );

        // Warm request: the server's segment echoed back after the client's toProto rewrite.
        let warm = client_form_segment(segment(&r1), false);
        assert!(
            first_file(&warm).location.starts_with("file:/")
                && !first_file(&warm).location.starts_with("file://")
        );
        let (r2, ops) = run(&mut c, client_request(&fx, v, warm), Behavior::default())
            .await
            .ok();
        assert_eq!(resolved(&r2), ResolveKind::CacheHit, "{name}");
        assert!(ops.is_empty());

        // The JNR-derived segment also identifies the same files.
        let (r3, _) = run(
            &mut c,
            client_request(&fx, v, client_form_segment(&jnr_seg, true)),
            Behavior::default(),
        )
        .await
        .ok();
        assert_eq!(resolved(&r3), ResolveKind::CacheHit, "{name}");

        // Safety: a different segment still misses.
        let base = client_form_segment(segment(&r1), false);
        let mut bigger = base.clone();
        first_file_mut(&mut bigger).size += 1;
        let mut fewer = base.clone();
        if fewer.deltas.is_empty() {
            fewer.checkpoint_parts.remove(0);
        } else {
            fewer.deltas.remove(0);
            fewer.delta_is_staged.remove(0);
        }
        let mut other_ckpt = base.clone();
        other_ckpt.checkpoint_version = Some(other_ckpt.checkpoint_version.map_or(0, |c| c + 1));
        let mut renamed = base.clone();
        let f = first_file_mut(&mut renamed);
        f.location = f.location.replace("/_delta_log/", "/_delta_log/x");
        for (what, seg) in [
            ("size", bigger),
            ("missing delta", fewer),
            ("checkpoint version", other_ckpt),
            ("location", renamed),
        ] {
            let (r, _) = run(&mut c, client_request(&fx, v, seg), Behavior::default())
                .await
                .ok();
            assert_ne!(
                resolved(&r),
                ResolveKind::CacheHit,
                "{name}: {what} differs"
            );
        }
    }
}

fn first_file(seg: &proto::LogSegmentInfo) -> &proto::FileStatus {
    seg.deltas.first().or(seg.checkpoint_parts.first()).unwrap()
}

fn first_file_mut(seg: &mut proto::LogSegmentInfo) -> &mut proto::FileStatus {
    if seg.deltas.is_empty() {
        &mut seg.checkpoint_parts[0]
    } else {
        &mut seg.deltas[0]
    }
}

fn fixture_at_tar_or_dir(name: &str) -> Fixture {
    if data_dir().join(name).is_dir() {
        fixture_at(name, "table with space")
    } else {
        fixture(name)
    }
}

/// F2 (`P1A-SNAPSHOTINFO-ACTIVE-DOMAIN-METADATA-MISMATCH`): `active_domain_metadata` mirrors JNR
/// `getActiveDomainMetadataMap` = ffi `visit_domain_metadata` = kernel `get_all_domain_metadata`:
/// active user domains only, no `delta.*` system domains, no removed domains. Liquid table built
/// by log replay (no CRC).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn liquid_active_domain_metadata_matches_jnr_getter() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("liquid");
    let log = root.join("_delta_log");
    std::fs::create_dir_all(&log).unwrap();
    let schema = r#"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"#;
    let commit0 = [
        r#"{"commitInfo":{"timestamp":1700000000000,"operation":"CREATE TABLE"}}"#.to_string(),
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":7,"writerFeatures":["domainMetadata","clustering"]}}"#.to_string(),
        format!(
            r#"{{"metaData":{{"id":"liquid-1","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{schema}","partitionColumns":[],"configuration":{{}},"createdTime":1700000000000}}}}"#
        ),
        r#"{"domainMetadata":{"domain":"delta.clustering","configuration":"{\"clusteringColumns\":[[\"region\"]]}","removed":false}}"#.to_string(),
        r#"{"domainMetadata":{"domain":"com.databricks.liquid","configuration":"{\"v\":1}","removed":false}}"#.to_string(),
        r#"{"domainMetadata":{"domain":"com.databricks.liquid.shuffledToFileCompressionRatio","configuration":"{\"r\":2.5}","removed":false}}"#.to_string(),
        r#"{"domainMetadata":{"domain":"myApp.gone","configuration":"{}","removed":false}}"#.to_string(),
        r#"{"add":{"path":"part-0.parquet","partitionValues":{},"size":10,"modificationTime":1700000000000,"dataChange":true,"stats":"{\"numRecords\":1}"}}"#.to_string(),
    ];
    std::fs::write(
        log.join(format!("{:020}.json", 0)),
        commit0.join("\n") + "\n",
    )
    .unwrap();
    let commit1 = [
        r#"{"commitInfo":{"timestamp":1700000000001,"operation":"WRITE"}}"#,
        r#"{"domainMetadata":{"domain":"myApp.gone","configuration":"{}","removed":true}}"#,
        r#"{"domainMetadata":{"domain":"com.databricks.liquid","configuration":"{\"v\":2}","removed":false}}"#,
    ];
    std::fs::write(
        log.join(format!("{:020}.json", 1)),
        commit1.join("\n") + "\n",
    )
    .unwrap();
    let root = root.canonicalize().unwrap();
    let fx = Fixture {
        url: url::Url::from_directory_path(&root).unwrap(),
        root,
        table_id: "tbl-liquid".into(),
        _dir: dir,
    };

    let server = TestServer::start().await;
    let mut c = server.client().await;
    let specs = default_specs();
    let (resp, _) = run(&mut c, request(&fx, 1, specs.clone()), Behavior::default())
        .await
        .ok();
    let info = info(&resp);
    let mut keys: Vec<&str> = info
        .active_domain_metadata
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "com.databricks.liquid",
            "com.databricks.liquid.shuffledToFileCompressionRatio"
        ],
        "user domains only: no delta.*, no removed"
    );
    assert_eq!(
        info.active_domain_metadata["com.databricks.liquid"].configuration,
        r#"{"v":2}"#
    );
    assert!(info.active_domain_metadata.values().all(|d| !d.removed));

    // Exactly what the in-process getter behind JNR returns.
    let local = local_snapshot(&fx, Some(1));
    let mut jnr: Vec<(String, String)> = local
        .reader()
        .get_all_domain_metadata(&local_engine())
        .unwrap()
        .iter()
        .map(|d| (d.domain().to_string(), d.configuration().to_string()))
        .collect();
    jnr.sort();
    let mut remote: Vec<(String, String)> = info
        .active_domain_metadata
        .values()
        .map(|d| (d.domain.clone(), d.configuration.clone()))
        .collect();
    remote.sort();
    assert_eq!(remote, jnr);

    // The system domain still drives clustering columns.
    let cols: Vec<Vec<String>> = info
        .clustering_columns
        .iter()
        .map(|c| c.logical_path.as_ref().unwrap().parts.clone())
        .collect();
    assert_eq!(cols, vec![vec!["region".to_string()]]);
    assert_parity(&fx, &resp, &specs);
}

/// F2 on the CRC path: crc-full's CRC lists delta.clustering, delta.rowTracking and
/// myApp.metadata; only the user domain is reported.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crc_active_domain_metadata_excludes_system_domains() {
    let fx = fixture("crc-full");
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let (resp, _) = run(
        &mut c,
        request(&fx, 0, default_specs()),
        Behavior::default(),
    )
    .await
    .ok();
    let keys: Vec<&String> = info(&resp).active_domain_metadata.keys().collect();
    assert_eq!(keys, vec!["myApp.metadata"]);
    assert_eq!(info(&resp).row_tracking_high_water_mark, Some(9));
}

// --- F3: v7 server-side path-based latest cache ---

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn latest_within_staleness_hits_with_zero_ops() {
    let fx = fixture("table-with-dv-small");
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let specs = default_specs();
    let (r1, _) = run(
        &mut c,
        latest_request(&fx, 60_000, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r1), ResolveKind::FullBuild);
    assert_eq!(r1.version, latest_version(&fx));
    assert_parity(&fx, &r1, &specs);
    let (r2, ops) = run(
        &mut c,
        latest_request(&fx, 60_000, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r2), ResolveKind::CacheHit);
    assert_eq!(
        op_counts(&r2),
        (0, 0),
        "cache hit: nothing forwarded, nothing local"
    );
    assert!(ops.is_empty());
    assert_eq!(r2.version, r1.version);
    assert!(r2.plans.iter().all(|p| p.cache_hit));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn latest_stale_unchanged_renews_with_one_listing() {
    let fx = fixture("v2-classic-checkpoint-parquet");
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let specs = default_specs();
    let (r1, _) = run(
        &mut c,
        latest_request(&fx, 0, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r1), ResolveKind::FullBuild);
    let (r2, ops) = run(
        &mut c,
        latest_request(&fx, 0, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r2), ResolveKind::CacheHit);
    // The renewal listing ran on the server: 0 forwarded, 1 local.
    assert_eq!(op_counts(&r2), (0, 1));
    assert!(ops.is_empty());
    assert_eq!(r2.version, r1.version);
    assert_eq!(r2.info, r1.info, "renewal serves the stored SnapshotInfo");
    assert!(
        r2.plans.iter().all(|p| p.cache_hit),
        "renewal keeps the plan cache"
    );
    // The renewal re-confirmed the instance: a staleness window now hits with 0 ops.
    let (r3, _) = run(
        &mut c,
        latest_request(&fx, 60_000, specs),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r3), ResolveKind::CacheHit);
    assert_eq!(op_counts(&r3), (0, 0));
}

/// Forward mode: the renewal listing is the one forwarded op.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn latest_stale_unchanged_renews_with_one_listing_forward() {
    let fx = fixture("v2-classic-checkpoint-parquet");
    let server = TestServer::start_mode(IoMode::Forward).await;
    let mut c = server.client().await;
    let specs = default_specs();
    run(
        &mut c,
        latest_request(&fx, 0, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    let (r2, ops) = run(&mut c, latest_request(&fx, 0, specs), Behavior::default())
        .await
        .ok();
    assert_eq!(resolved(&r2), ResolveKind::CacheHit);
    assert_eq!(op_counts(&r2), (1, 0));
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].kind, "FileListing");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn latest_stale_with_new_commit_is_incremental() {
    let fx = fixture("basic_partitioned");
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let specs = default_specs();
    let v0 = latest_version(&fx);
    let (r1, _) = run(
        &mut c,
        latest_request(&fx, 0, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(r1.version, v0);
    append_commit(
        &fx,
        v0 + 1,
        "letter=z/part-latest.parquet",
        Some(("letter", "z")),
    );

    // Inside the staleness window the new commit is not seen yet.
    let (r2, _) = run(
        &mut c,
        latest_request(&fx, 60_000, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!((resolved(&r2), r2.version), (ResolveKind::CacheHit, v0));

    let (r3, _) = run(
        &mut c,
        latest_request(&fx, 0, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r3), ResolveKind::Incremental);
    assert_eq!(r3.version, v0 + 1);
    assert_parity(&fx, &r3, &specs);
    let (r4, _) = run(
        &mut c,
        latest_request(&fx, 60_000, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(
        (resolved(&r4), r4.version, op_counts(&r4)),
        (ResolveKind::CacheHit, v0 + 1, (0, 0))
    );
    // The new latest instance also serves exact-version requests with its segment.
    let (r5, _) = run(
        &mut c,
        request_with_segment(&fx, v0 + 1, specs, &r3),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r5), ResolveKind::CacheHit);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_latest_requests_share_one_listing() {
    let fx = Arc::new(fixture("v2-checkpoints-parquet-with-sidecars"));
    let server = TestServer::start().await;
    let specs = default_specs();
    let mut c = server.client().await;
    run(
        &mut c,
        latest_request(&fx, 0, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    let before = server.stats().await;

    // The renewal listing runs on the server; slow it down so the others pile up behind it.
    server
        .core
        .faults
        .set_local_io_delay(Duration::from_millis(300));
    let mut c0 = server.client().await;
    let req = latest_request(&fx, 0, specs.clone());
    let driver = tokio::spawn(async move { run(&mut c0, req, Behavior::default()).await });
    server.wait_relists_started(1).await;
    let mut waiters = Vec::new();
    for _ in 0..7 {
        let mut c = server.client().await;
        let req = latest_request(&fx, 0, specs.clone());
        waiters.push(tokio::spawn(async move {
            run(&mut c, req, Behavior::default()).await
        }));
    }
    let (d, d_ops) = driver.await.unwrap().ok();
    server.core.faults.set_local_io_delay(Duration::ZERO);
    assert_eq!(resolved(&d), ResolveKind::CacheHit);
    assert_eq!(
        op_counts(&d),
        (0, 1),
        "the one renewal listing, on the server"
    );
    assert!(d_ops.is_empty());
    for w in waiters {
        let (r, ops) = w.await.unwrap().ok();
        assert_eq!(resolved(&r), ResolveKind::Coalesced);
        assert_eq!(r.version, d.version);
        assert_eq!(op_counts(&r), (0, 0));
        assert!(ops.is_empty());
    }
    let after = server.stats().await;
    assert_eq!(
        after.ops_local_total - before.ops_local_total,
        1,
        "8 concurrent latest requests -> one listing"
    );
    assert_eq!(after.ops_issued_total, before.ops_issued_total);
    // The shared re-listing found no new commit: a renewal, not a build.
    assert_eq!(after.builds_started, before.builds_started);
    assert_eq!(after.latest_renewals - before.latest_renewals, 1);
    assert_eq!(after.builds_coalesced - before.builds_coalesced, 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn latest_after_eviction_is_full_build() {
    let fx = fixture("with_checkpoint_no_last_checkpoint");
    let server = TestServer::start_with(
        CacheConfig {
            max_instances_per_table: 1,
            ..CacheConfig::default()
        },
        true,
    )
    .await;
    let mut c = server.client().await;
    let specs = vec![spec_json()];
    let (r1, _) = run(
        &mut c,
        latest_request(&fx, 60_000, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!((resolved(&r1), r1.version), (ResolveKind::FullBuild, 3));
    // Before eviction the confirmed latest instance is served with 0 ops.
    let (warm, warm_ops) = run(
        &mut c,
        latest_request(&fx, 60_000, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&warm), ResolveKind::CacheHit);
    assert_eq!(
        op_counts(&warm),
        (0, 0),
        "cache hit: nothing forwarded, nothing local"
    );
    assert!(warm_ops.is_empty());
    // An exact-version build of v1 evicts the latest instance (one instance per table).
    run(&mut c, request(&fx, 1, specs.clone()), Behavior::default())
        .await
        .ok();
    let (r2, _) = run(
        &mut c,
        latest_request(&fx, 60_000, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(
        resolved(&r2),
        ResolveKind::FullBuild,
        "evicted latest cannot be served"
    );
    assert_eq!(r2.version, 3);
    assert_parity(&fx, &r2, &specs);
    // ResetCache also forgets the latest confirmation.
    c.reset_cache(proto::ResetCacheRequest {}).await.unwrap();
    let (r3, _) = run(
        &mut c,
        latest_request(&fx, 60_000, specs),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r3), ResolveKind::FullBuild);
}

// ------------------------------------------------------------------------------------------------
// Round-4: SPEC v8 absent-segment rule, latest follow-ups
// ------------------------------------------------------------------------------------------------

/// SPEC v8: two cached instances of one version (before/after a checkpoint at a lower version
/// appears). A request without `expected_log_segment` deterministically gets the most recently
/// built/confirmed one; a cache hit on the older one does not change that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn absent_segment_returns_most_recent_instance() {
    let fx = fixture("basic_partitioned");
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let specs = default_specs();
    let v = latest_version(&fx);
    assert!(v >= 1);

    let (old, _) = run(&mut c, request(&fx, v, specs.clone()), Behavior::default())
        .await
        .ok();
    assert_eq!(resolved(&old), ResolveKind::FullBuild);
    assert_eq!(segment(&old).checkpoint_version, None);

    // A checkpoint at v-1 now exists, so a fresh build of v reads a different segment.
    write_checkpoint(&fx, v - 1);
    let local = local_snapshot(&fx, Some(v));
    let new_seg = delta_kernel_server::info::log_segment_info(local.log_segment());
    assert_eq!(new_seg.checkpoint_version, Some(v - 1));
    let mut req = request(&fx, v, specs.clone());
    req.expected_log_segment = Some(new_seg.clone());
    let (new, _) = run(&mut c, req, Behavior::default()).await.ok();
    assert_eq!(
        resolved(&new),
        ResolveKind::FullBuild,
        "mismatched segment builds"
    );
    assert_eq!(segment(&new).checkpoint_version, Some(v - 1));
    assert_parity(&fx, &new, &specs);
    assert_eq!(
        server.core.cached_instances_per_table().get(&fx.table_id),
        Some(&2),
        "both segments of version {v} are cached"
    );

    for _ in 0..3 {
        let (r, ops) = run(&mut c, request(&fx, v, specs.clone()), Behavior::default())
            .await
            .ok();
        assert_eq!(resolved(&r), ResolveKind::CacheHit);
        assert!(ops.is_empty());
        assert_eq!(segment(&r), segment(&new), "most recent instance wins");
    }

    // Hitting the older instance explicitly still works and does not make it "most recent".
    let (old_hit, _) = run(
        &mut c,
        request_with_segment(&fx, v, specs.clone(), &old),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&old_hit), ResolveKind::CacheHit);
    assert_eq!(segment(&old_hit), segment(&old));
    let (r, _) = run(&mut c, request(&fx, v, specs.clone()), Behavior::default())
        .await
        .ok();
    assert_eq!(segment(&r), segment(&new));
}

/// SPEC v7/v8: a tainted latest result is returned but neither cached nor confirmed; the next
/// latest request inside the staleness window builds again, and only a clean build confirms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn latest_tainted_result_does_not_confirm() {
    let fx = fixture("crc-full");
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let specs = default_specs();

    let (tainted, _) = {
        // A real server-side read failure (not FileNotFound) on the CRC.
        let _guard = Unreadable::new(fx.log_dir().join("00000000000000000000.crc"));
        run(
            &mut c,
            latest_request(&fx, 60_000, specs.clone()),
            Behavior::default(),
        )
        .await
        .ok()
    };
    assert_eq!(resolved(&tainted), ResolveKind::FullBuild);
    assert!(info(&tainted).crc.is_none());
    assert!(server.core.cached_instances_per_table().is_empty());

    let (clean, _) = run(
        &mut c,
        latest_request(&fx, 60_000, specs.clone()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(
        resolved(&clean),
        ResolveKind::FullBuild,
        "a tainted result must not confirm latest"
    );
    assert!(info(&clean).crc.is_some());
    assert_parity(&fx, &clean, &specs);

    let (warm, warm_ops) = run(
        &mut c,
        latest_request(&fx, 60_000, specs),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&warm), ResolveKind::CacheHit);
    assert!(warm_ops.is_empty());
}
