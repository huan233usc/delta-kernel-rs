//! SPEC v9.1: server-side IO runs on a bounded pool. Its own test binary (single test), so the
//! process-wide thread count below only sees this server's pool.

mod common;

use std::time::{Duration, Instant};

use common::*;
use delta_kernel_server::cache::CacheConfig;
use delta_kernel_server::local_io::WORKER_THREAD_NAME;
use delta_kernel_server::proto::ResolveKind;
use delta_kernel_server::proto::error::Code;

/// Live threads of this process named like the pool's workers.
fn pool_threads() -> usize {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|t| std::fs::read_to_string(t.ok()?.path().join("comm")).ok())
        .filter(|comm| comm.trim() == WORKER_THREAD_NAME)
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stuck_local_io_is_bounded_and_saturation_fails_fast() {
    const POOL: usize = 2;
    let server = TestServer::start_with(
        CacheConfig {
            local_io_threads: POOL,
            ..CacheConfig::default()
        },
        true,
    )
    .await;
    let pool = server.core.io_pool.clone();
    assert_eq!(pool_threads(), 0, "workers start lazily");
    let fixtures: Vec<Fixture> = (0..8).map(|_| fixture("table-with-dv-small")).collect();
    let mut c = server.client().await;
    let req = |fx: &Fixture| {
        let mut r = request(fx, 1, default_specs());
        r.client.as_mut().unwrap().op_deadline_ms = 200;
        r
    };

    // Every server-side op now hangs well past the deadline.
    server
        .core
        .faults
        .set_local_io_delay(Duration::from_millis(2_000));

    // POOL requests: each first op times out (DEADLINE) and stays in flight, holding a worker.
    for fx in &fixtures[..POOL] {
        let err = run(&mut c, req(fx), Behavior::default()).await.err();
        assert_eq!(err.code, Code::Deadline as i32, "{err:?}");
    }
    assert_eq!(pool.in_flight(), POOL, "abandoned ops keep their permits");
    assert_eq!(pool_threads(), POOL);

    // More requests: no free worker, fail fast with retryable UNAVAILABLE, no new threads.
    for fx in &fixtures[POOL..] {
        let started = Instant::now();
        let err = run(&mut c, req(fx), Behavior::default()).await.err();
        assert_eq!(err.code, Code::Unavailable as i32, "{err:?}");
        assert!(err.retryable);
        assert!(err.message.contains("saturated"), "{}", err.message);
        assert!(
            started.elapsed() < Duration::from_millis(150),
            "saturation must not wait for the deadline ({:?})",
            started.elapsed()
        );
        assert!(pool_threads() <= POOL, "{} pool threads", pool_threads());
        assert_eq!(pool.workers(), POOL);
    }
    assert!(server.core.cached_instances_per_table().is_empty());

    // The stuck ops finish; their permits come back and the pool recovers.
    server.core.faults.set_local_io_delay(Duration::ZERO);
    let deadline = Instant::now() + Duration::from_secs(10);
    while pool.in_flight() > 0 {
        assert!(Instant::now() < deadline, "abandoned ops never returned");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for fx in &fixtures[..2] {
        let (ok, _) = run(&mut c, request(fx, 1, default_specs()), Behavior::default())
            .await
            .ok();
        assert_eq!(resolved(&ok), ResolveKind::FullBuild);
        assert!(ops_local(&ok) > 0);
        assert_parity(fx, &ok, &default_specs());
    }
    assert_eq!(pool_threads(), POOL, "the same workers are reused");
    assert_eq!(pool.in_flight(), 0);
}
