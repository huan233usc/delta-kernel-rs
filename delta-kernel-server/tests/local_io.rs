//! SPEC v9 L0 tests: hybrid server-side IO and UC storage credentials (against a mock UC).

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use delta_kernel_server::cache::IoMode;
use delta_kernel_server::proto::error::Code;
use delta_kernel_server::proto::{self, ResolveKind};

const UC_TABLE_UUID: &str = "123e4567-e89b-12d3-a456-426614174000";

fn segment(resp: &proto::GetSnapshotResponse) -> &proto::LogSegmentInfo {
    resp.info.as_ref().unwrap().log_segment.as_ref().unwrap()
}

/// Cold build in both modes: local mode forwards exactly the QueryPlan ops forward mode sends,
/// and runs every other op on the server.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_build_forwards_only_query_plans() {
    for name in [
        "v2-checkpoints-parquet-with-sidecars",
        "table-with-dv-small",
        "with_checkpoint_no_last_checkpoint",
        "crc-full",
    ] {
        let fwd_fx = fixture(name);
        let fwd = TestServer::start_mode(IoMode::Forward).await;
        let v = latest_version(&fwd_fx);
        let (fwd_resp, fwd_ops) = run(
            &mut fwd.client().await,
            request(&fwd_fx, v, default_specs()),
            Behavior::default(),
        )
        .await
        .ok();
        let fwd_query_plans = fwd_ops.iter().filter(|o| o.kind == "QueryPlan").count() as i32;
        assert_eq!(op_counts(&fwd_resp), (fwd_ops.len() as i32, 0));

        let fx = fixture(name);
        let local = TestServer::start().await;
        let (resp, ops) = run(
            &mut local.client().await,
            request(&fx, v, default_specs()),
            Behavior::default(),
        )
        .await
        .ok();
        let (forwarded, local_ops) = op_counts(&resp);
        eprintln!(
            "OP_COUNTS {name} v={v}: forward mode forwarded={} | local mode forwarded={forwarded} \
             local={local_ops}",
            fwd_ops.len()
        );
        assert!(ops.iter().all(|o| o.kind == "QueryPlan"), "{name}");
        assert_eq!(forwarded as usize, ops.len(), "{name}");
        assert_eq!(
            forwarded, fwd_query_plans,
            "{name}: forwarded = QueryPlan ops only"
        );
        assert!(local_ops > 0, "{name}");
        assert_eq!(
            forwarded + local_ops,
            fwd_ops.len() as i32,
            "{name}: same ops, split differently"
        );
        assert_eq!(
            local.stats().await.ops_local_total,
            local_ops as i64,
            "{name}: DebugStats.ops_local_total"
        );
        assert_parity(&fx, &resp, &default_specs());
    }
}

/// `_last_checkpoint` is missing: the server's own ReadBytes gets a per-item FileNotFound, which
/// is benign (build succeeds and is cached).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_last_checkpoint_not_found_is_benign() {
    let fx = fixture("table-without-dv-small");
    assert!(!fx.log_dir().join("_last_checkpoint").exists());
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let v = latest_version(&fx);
    let (r1, _) = run(
        &mut c,
        request(&fx, v, default_specs()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&r1), ResolveKind::FullBuild);
    assert!(ops_local(&r1) > 0);
    assert!(
        server
            .core
            .counters
            .local_not_found_items
            .load(Ordering::Relaxed)
            >= 1,
        "the _last_checkpoint read came back as a per-item FileNotFound"
    );
    assert_parity(&fx, &r1, &default_specs());
    let (r2, ops) = run(
        &mut c,
        request_with_segment(&fx, v, default_specs(), &r1),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(
        resolved(&r2),
        ResolveKind::CacheHit,
        "FileNotFound does not taint"
    );
    assert!(ops.is_empty());
}

/// The server truncates its own `_delta_log` listing per the hint it computed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listing_hint_honored_server_side() {
    let fx = fixture("with_checkpoint_no_last_checkpoint");
    let server = TestServer::start().await;
    let mut c = server.client().await;
    let dropped = || {
        server
            .core
            .counters
            .listing_items_dropped
            .load(Ordering::Relaxed)
    };
    // The log has commits 0-3 and a checkpoint at 2; a build at v1 must not see 2.* and 3.*.
    let (r1, _) = run(
        &mut c,
        request(&fx, 1, default_specs()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(r1.version, 1);
    assert_eq!(segment(&r1).checkpoint_version, None);
    assert!(dropped() >= 3, "dropped {} listing entries", dropped());
    assert_parity(&fx, &r1, &default_specs());
    // Latest has no end version and the log has no non-versioned files: nothing more dropped.
    let before = dropped();
    let (r2, _) = run(
        &mut c,
        request(&fx, 3, default_specs()),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(segment(&r2).checkpoint_version, Some(2));
    assert_eq!(dropped(), before);
}

/// Forward-mode smoke: the pre-v9 behavior still works end to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forward_mode_smoke() {
    for name in ["crc-full", "basic_partitioned"] {
        let fx = fixture(name);
        let server = TestServer::start_mode(IoMode::Forward).await;
        let mut c = server.client().await;
        let v = latest_version(&fx);
        let (r1, ops) = run(
            &mut c,
            request(&fx, v, default_specs()),
            Behavior::default(),
        )
        .await
        .ok();
        assert!(ops.iter().any(|o| o.kind == "FileListing"));
        assert_eq!(ops_local(&r1), 0);
        assert_eq!(server.stats().await.ops_local_total, 0);
        assert_parity(&fx, &r1, &default_specs());
        let (r2, _) = run(
            &mut c,
            request_with_segment(&fx, v, default_specs(), &r1),
            Behavior::default(),
        )
        .await
        .ok();
        assert_eq!(resolved(&r2), ResolveKind::CacheHit);
    }
}

// ------------------------------------------------------------------------------------------------
// UC credentials
// ------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc_endpoint_table_vs_path() {
    let uc = MockUc::start();
    uc.answer(
        "tok-a",
        UcAnswer::Grant {
            ttl: Duration::from_secs(3600),
        },
    );
    let server = server_with_uc(&uc).await;
    let mut c = server.client().await;

    // Non-UUID table id: path credentials for the table location.
    let fx = fixture("crc-full");
    run(
        &mut c,
        with_auth(request(&fx, 0, default_specs()), "tok-a"),
        Behavior::default(),
    )
    .await
    .ok();
    let calls = uc.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0].path,
        "/api/2.1/unity-catalog/temporary-path-credentials"
    );
    assert_eq!(calls[0].body["operation"], "PATH_READ");
    assert_eq!(calls[0].body["url"], fx.url.as_str());
    assert_eq!(calls[0].authorization, "Bearer tok-a");

    // UC table UUID: table credentials by id.
    let fx2 = fixture("table-with-dv-small");
    let mut req = with_auth(request(&fx2, 1, default_specs()), "tok-a");
    req.table.as_mut().unwrap().table_id = UC_TABLE_UUID.into();
    let (resp, _) = run(&mut c, req, Behavior::default()).await.ok();
    assert_parity(&fx2, &resp, &default_specs());
    let calls = uc.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[1].path,
        "/api/2.1/unity-catalog/temporary-table-credentials"
    );
    assert_eq!(calls[1].body["table_id"], UC_TABLE_UUID);
    assert_eq!(calls[1].body["operation"], "READ");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc_credentials_cached_per_token_until_expiry() {
    let uc = MockUc::start();
    // 61.5 s TTL leaves 1.5 s of use after the 60 s safety margin.
    uc.answer(
        "tok-a",
        UcAnswer::Grant {
            ttl: Duration::from_millis(61_500),
        },
    );
    uc.answer(
        "tok-b",
        UcAnswer::Grant {
            ttl: Duration::from_secs(3600),
        },
    );
    let server = server_with_uc(&uc).await;
    let mut c = server.client().await;
    let fx = fixture("crc-full");

    let (r1, _) = run(
        &mut c,
        with_auth(request(&fx, 0, default_specs()), "tok-a"),
        Behavior::default(),
    )
    .await
    .ok();
    let hit = || with_auth(request_with_segment(&fx, 0, default_specs(), &r1), "tok-a");
    let (r2, _) = run(&mut c, hit(), Behavior::default()).await.ok();
    assert_eq!(resolved(&r2), ResolveKind::CacheHit);
    assert_eq!(
        uc.calls().len(),
        1,
        "same token: credential served from cache"
    );

    run(
        &mut c,
        with_auth(request_with_segment(&fx, 0, default_specs(), &r1), "tok-b"),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(uc.calls().len(), 2, "another token is checked with UC");

    tokio::time::sleep(Duration::from_millis(2_000)).await;
    let (r3, _) = run(&mut c, hit(), Behavior::default()).await.ok();
    assert_eq!(resolved(&r3), ResolveKind::CacheHit);
    assert_eq!(
        uc.calls().len(),
        3,
        "expired (minus margin) credential is fetched again"
    );

    // A credential that already expires inside the margin is used once and never cached.
    uc.answer(
        "tok-c",
        UcAnswer::Grant {
            ttl: Duration::from_secs(30),
        },
    );
    for n in 4..=5 {
        let req = with_auth(request_with_segment(&fx, 0, default_specs(), &r1), "tok-c");
        run(&mut c, req, Behavior::default()).await.ok();
        assert_eq!(uc.calls().len(), n);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc_403_is_permission_denied_before_any_io() {
    let uc = MockUc::start();
    uc.answer("tok-denied", UcAnswer::Status(403));
    let server = server_with_uc(&uc).await;
    let mut c = server.client().await;
    let fx = fixture("table-with-dv-small");
    let out = run(
        &mut c,
        with_auth(request(&fx, 1, default_specs()), "tok-denied"),
        Behavior::default(),
    )
    .await;
    assert!(out.ops.is_empty());
    let err = out.err();
    assert_eq!(err.code, Code::PermissionDenied as i32, "{err:?}");
    assert!(!err.retryable);
    assert!(!err.message.contains("tok-denied"));
    let stats = server.stats().await;
    assert_eq!((stats.builds_started, stats.ops_local_total), (0, 0));

    // 401 maps the same way; a missing token is denied without calling UC.
    uc.answer("tok-401", UcAnswer::Status(401));
    let err = run(
        &mut c,
        with_auth(request(&fx, 1, default_specs()), "tok-401"),
        Behavior::default(),
    )
    .await
    .err();
    assert_eq!(err.code, Code::PermissionDenied as i32);
    let calls_before = uc.calls().len();
    let err = run(
        &mut c,
        request(&fx, 1, default_specs()),
        Behavior::default(),
    )
    .await
    .err();
    assert_eq!(err.code, Code::PermissionDenied as i32);
    assert_eq!(uc.calls().len(), calls_before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc_5xx_is_retryable() {
    let uc = MockUc::start();
    uc.answer("tok-a", UcAnswer::Status(503));
    let server = server_with_uc(&uc).await;
    let mut c = server.client().await;
    let fx = fixture("crc-full");
    let err = run(
        &mut c,
        with_auth(request(&fx, 0, default_specs()), "tok-a"),
        Behavior::default(),
    )
    .await
    .err();
    assert!(err.retryable, "{err:?}");
    assert!(err.message.contains("UC unavailable"), "{}", err.message);

    // Connection refused is the same class.
    let dead = MockUc::start();
    let base = dead.base_url.clone();
    drop(dead);
    let server = TestServer::start_with(
        delta_kernel_server::cache::CacheConfig {
            uc_endpoint_override: Some(base.replace(
                base.rsplit(':').next().unwrap(),
                "1", // port 1: nothing listens
            )),
            credentials_for_file: true,
            ..Default::default()
        },
        true,
    )
    .await;
    let err = run(
        &mut server.client().await,
        with_auth(request(&fx, 0, default_specs()), "tok-a"),
        Behavior::default(),
    )
    .await
    .err();
    assert!(err.retryable, "{err:?}");
}

/// A snapshot cached for one caller is not served to a caller whose token UC rejects.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cache_hit_requires_valid_credential() {
    let uc = MockUc::start();
    uc.answer(
        "tok-good",
        UcAnswer::Grant {
            ttl: Duration::from_secs(3600),
        },
    );
    uc.answer("tok-bad", UcAnswer::Status(403));
    let server = server_with_uc(&uc).await;
    let mut c = server.client().await;
    let fx = fixture("table-with-dv-small");
    let (r1, _) = run(
        &mut c,
        with_auth(request(&fx, 1, default_specs()), "tok-good"),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(server.core.cached_instances_per_table().len(), 1);
    let before = server.stats().await;

    for req in [
        with_auth(
            request_with_segment(&fx, 1, default_specs(), &r1),
            "tok-bad",
        ),
        with_auth(request(&fx, 1, default_specs()), "tok-bad"),
        with_auth(latest_request(&fx, 60_000, default_specs()), "tok-bad"),
    ] {
        let out = run(&mut c, req, Behavior::default()).await;
        assert!(out.ops.is_empty());
        assert_eq!(out.err().code, Code::PermissionDenied as i32);
    }
    let after = server.stats().await;
    assert_eq!(after.builds_started, before.builds_started);
    assert_eq!(after.ops_local_total, before.ops_local_total);

    let (ok, _) = run(
        &mut c,
        with_auth(
            request_with_segment(&fx, 1, default_specs(), &r1),
            "tok-good",
        ),
        Behavior::default(),
    )
    .await
    .ok();
    assert_eq!(resolved(&ok), ResolveKind::CacheHit);
}

/// The bearer token never appears in server logs, even at trace level, on success or denial.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_absent_from_logs() {
    const GOOD: &str = "dapi-SECRET-good-7f3a9c";
    const BAD: &str = "dapi-SECRET-bad-1b2c3d";
    let uc = MockUc::start();
    uc.answer(
        GOOD,
        UcAnswer::Grant {
            ttl: Duration::from_secs(3600),
        },
    );
    uc.answer(BAD, UcAnswer::Status(403));
    let dir = tempfile::tempdir().unwrap();
    let endpoint = dir.path().join("endpoint");
    let log = dir.path().join("stderr.log");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_delta-kernel-server"))
        .args([
            "--listen",
            "127.0.0.1:0",
            "--enable-test-hooks",
            "--test-uc-credentials-for-file",
        ])
        .arg("--uc-endpoint-override")
        .arg(&uc.base_url)
        .arg("--endpoint-file")
        .arg(&endpoint)
        .env("RUST_LOG", "trace")
        .stderr(std::fs::File::create(&log).unwrap())
        .spawn()
        .unwrap();
    let mut addr = None;
    for _ in 0..400 {
        if let Ok(s) = std::fs::read_to_string(&endpoint) {
            addr = Some(s.trim().parse().unwrap());
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let mut c = connect(addr.expect("endpoint file written")).await;
    let fx = fixture("table-with-dv-small");
    let (r1, _) = run(
        &mut c,
        with_auth(request(&fx, 1, default_specs()), GOOD),
        Behavior::default(),
    )
    .await
    .ok();
    run(
        &mut c,
        with_auth(request_with_segment(&fx, 1, default_specs(), &r1), GOOD),
        Behavior::default(),
    )
    .await
    .ok();
    let err = run(
        &mut c,
        with_auth(request(&fx, 1, default_specs()), BAD),
        Behavior::default(),
    )
    .await
    .err();
    assert_eq!(err.code, Code::PermissionDenied as i32);
    let pid = child.id().to_string();
    std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .unwrap();
    child.wait().unwrap();
    let logs = std::fs::read_to_string(&log).unwrap();
    assert!(
        logs.len() > 1000,
        "trace logging was on ({} bytes)",
        logs.len()
    );
    assert!(logs.contains("temporary-path-credentials") || logs.contains("snap.build"));
    for secret in [GOOD, BAD, "SECRET"] {
        assert!(
            !logs.contains(secret),
            "token fragment {secret} found in server logs"
        );
    }
}
