//! SPEC v10.3 L2 against a real Redis: `docker run redis:7` on a random port. Every test skips
//! with a message when docker (or the image) is unavailable, unless `DKS_REQUIRE_REDIS_TESTS=1`,
//! which turns the skip into a failure (gates run with it set, so these tests cannot pass
//! vacuously).

mod common;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Duration;

use common::*;
use delta_kernel_server::cache::{KERNEL_REV, spec_hash};
use delta_kernel_server::proto::{self, ResolveKind};
use delta_kernel_server::store::{
    DEFAULT_L2_TTL, L2, MAX_L2_PLAN_BYTES, PlanPut, RedisStore, SnapshotStore,
};

/// A throwaway `redis:7` container, removed on drop.
struct Redis {
    id: String,
    url: String,
}

/// Skips (returns `None`) or, with `DKS_REQUIRE_REDIS_TESTS=1`, fails.
fn skip<T>(why: String) -> Option<T> {
    if std::env::var("DKS_REQUIRE_REDIS_TESTS").is_ok_and(|v| v == "1") {
        panic!("DKS_REQUIRE_REDIS_TESTS=1 but {why}");
    }
    eprintln!("SKIP: {why}");
    None
}

impl Redis {
    fn start() -> Option<Self> {
        let docker_ok = Command::new("docker")
            .args(["info", "--format", "{{.ServerVersion}}"])
            .output()
            .is_ok_and(|o| o.status.success());
        if !docker_ok {
            return skip("docker is unavailable; L2 Redis tests need `docker run redis:7`".into());
        }
        let out = match Command::new("docker")
            .args(["run", "-d", "--rm", "-p", "127.0.0.1::6379", "redis:7"])
            .output()
        {
            Ok(o) => o,
            Err(e) => return skip(format!("cannot run docker: {e}")),
        };
        if !out.status.success() {
            return skip(format!(
                "cannot start redis:7 ({}); L2 Redis tests need the image",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let port = Command::new("docker")
            .args(["port", &id, "6379/tcp"])
            .output()
            .unwrap();
        let addr = String::from_utf8_lossy(&port.stdout)
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .to_string();
        Some(Self {
            id,
            url: format!("redis://{addr}/0"),
        })
    }

    async fn store(&self) -> RedisStore {
        for _ in 0..100 {
            if let Ok(s) = RedisStore::connect(&self.url, KERNEL_REV, DEFAULT_L2_TTL).await {
                let mut c = redis::Client::open(self.url.as_str())
                    .unwrap()
                    .get_multiplexed_async_connection()
                    .await;
                if let Ok(c) = c.as_mut()
                    && redis::cmd("PING").query_async::<String>(c).await.is_ok()
                {
                    return s;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("redis at {} never became ready", self.url);
    }

    async fn conn(&self) -> redis::aio::MultiplexedConnection {
        redis::Client::open(self.url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap()
    }
}

impl Drop for Redis {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.id]).output();
    }
}

macro_rules! redis_or_skip {
    () => {
        match Redis::start() {
            Some(r) => r,
            None => return,
        }
    };
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redis_store_contract_and_plan_size_cap() {
    let redis = redis_or_skip!();
    let store = redis.store().await;
    let fx = fixture("table-with-dv-small");
    let snapshot = local_snapshot(&fx, Some(1));
    let info =
        delta_kernel_server::info::compute_snapshot_info(&snapshot, &local_engine()).unwrap();
    store_contract(&store, &info, b"hint-bytes".to_vec()).await;
    // Concurrent writers on separate connections, as replicas would be.
    let w1 = redis.store().await;
    let w2 = redis.store().await;
    latest_contract(&store, &[&store, &w1, &w2]).await;
    // > 4 MiB after compression (random bytes do not compress): not written.
    let mut big = vec![0u8; MAX_L2_PLAN_BYTES + 1024];
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    for b in big.iter_mut() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = x as u8;
    }
    let plan = proto::ScanPlanResult {
        result: Some(proto::scan_plan_result::Result::Plan(big)),
        cache_hit: false,
    };
    assert!(matches!(
        store.put_plan("seg", "big", &plan).await.unwrap(),
        PlanPut::TooLarge(_)
    ));
    assert!(store.get_plan("seg", "big").await.unwrap().is_none());
}

/// The key schema and TTLs of SPEC v10.3, as written by a real build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redis_key_schema_and_ttls() {
    let redis = redis_or_skip!();
    let l2: Arc<L2> = Arc::new(redis.store().await);
    let server = TestServer::start_l2("schema", l2).await;
    let fx = fixture("table-with-dv-small");
    let mut c = server.client().await;
    let (r, _) = run(
        &mut c,
        latest_request(&fx, 5_000, vec![spec_json()]),
        Behavior::default(),
    )
    .await
    .ok();
    let seg = r.r#ref.as_ref().unwrap().segment_id.clone();
    let tk = delta_kernel_server::cache::l2_table_key(&fx.table_id, fx.url.as_str());
    let p = format!("dks:{KERNEL_REV}:");
    let mut conn = redis.conn().await;
    let keys: HashSet<String> = redis::cmd("KEYS")
        .arg(format!("{p}*"))
        .query_async::<Vec<String>>(&mut conn)
        .await
        .unwrap()
        .into_iter()
        .collect();
    let want: HashSet<String> = [
        format!("{p}snap:{tk}:{seg}"),
        format!("{p}ver:{tk}"),
        format!("{p}latest:{tk}"),
        format!("{p}plan:{seg}:{}", spec_hash(&spec_json())),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        keys, want,
        "no lock left behind, no credential or token stored"
    );
    let snap: HashMap<String, Vec<u8>> = redis::cmd("HGETALL")
        .arg(format!("{p}snap:{tk}:{seg}"))
        .query_async(&mut conn)
        .await
        .unwrap();
    let fields: HashSet<&str> = snap.keys().map(String::as_str).collect();
    assert_eq!(
        fields,
        ["info", "seg", "hint", "version", "built_at", "confirmed_at"]
            .into_iter()
            .collect()
    );
    let pttl = |k: String| {
        let mut conn = conn.clone();
        async move {
            redis::cmd("PTTL")
                .arg(k)
                .query_async::<i64>(&mut conn)
                .await
                .unwrap()
        }
    };
    let six_h = DEFAULT_L2_TTL.as_millis() as i64;
    for k in [
        format!("{p}snap:{tk}:{seg}"),
        format!("{p}ver:{tk}"),
        format!("{p}plan:{seg}:{}", spec_hash(&spec_json())),
    ] {
        let t = pttl(k.clone()).await;
        assert!(t > six_h - 60_000 && t <= six_h, "{k}: {t}");
    }
    // latest: 2 x staleness (10 s), at least 60 s.
    let t = pttl(format!("{p}latest:{tk}")).await;
    assert!(t > 50_000 && t <= 60_000, "latest ttl {t}");
    let plan: Vec<u8> = redis::cmd("GET")
        .arg(format!("{p}plan:{seg}:{}", spec_hash(&spec_json())))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(&plan[..4], &[0x28, 0xB5, 0x2F, 0xFD], "zstd frame");
}

// ---------- server processes ----------

struct ServerProc {
    child: Child,
    addr: SocketAddr,
    _dir: tempfile::TempDir,
}

impl ServerProc {
    async fn start(instance_id: &str, redis_url: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let endpoint = dir.path().join("endpoint");
        let mut child = Command::new(env!("CARGO_BIN_EXE_delta-kernel-server"))
            .args([
                "--listen",
                "127.0.0.1:0",
                "--enable-test-hooks",
                "--instance-id",
                instance_id,
            ])
            .arg("--l2-redis-url")
            .arg(redis_url)
            .arg("--endpoint-file")
            .arg(&endpoint)
            .spawn()
            .unwrap();
        for _ in 0..400 {
            if let Ok(s) = std::fs::read_to_string(&endpoint) {
                return Self {
                    child,
                    addr: s.trim().parse().unwrap(),
                    _dir: dir,
                };
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let _ = child.kill();
        let _ = child.wait();
        panic!("server {instance_id} did not start");
    }

    async fn stats(&self) -> proto::DebugStats {
        connect(self.addr)
            .await
            .get_debug_stats(proto::DebugStatsRequest {})
            .await
            .unwrap()
            .into_inner()
    }

    fn stop(mut self) {
        let _ = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status();
        let _ = self.child.wait();
    }
}

/// Two server processes on one Redis: one build; the second adopts it from L2.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_server_processes_share_l2_with_one_build() {
    let redis = redis_or_skip!();
    redis.store().await;
    let a = ServerProc::start("proc-a", &redis.url).await;
    let b = ServerProc::start("proc-b", &redis.url).await;
    let fx = fixture("v2-checkpoints-parquet-with-sidecars");
    let version = latest_version(&fx);
    let req = request(&fx, version, default_specs());
    let mut ca = connect(a.addr).await;
    let mut cb = connect(b.addr).await;
    let ra = tokio::spawn({
        let req = req.clone();
        async move {
            run(
                &mut ca,
                req,
                Behavior {
                    op_delay: Duration::from_millis(40),
                    ..Default::default()
                },
            )
            .await
        }
    });
    for _ in 0..400 {
        if a.stats().await.builds_started >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let (rb, _) = run(&mut cb, req, Behavior::default()).await.ok();
    let (ra, _) = ra.await.unwrap().ok();
    let (sa, sb) = (a.stats().await, b.stats().await);
    assert_eq!(
        sa.builds_started + sb.builds_started,
        1,
        "one build across processes: a={sa:?} b={sb:?}"
    );
    assert_eq!(sb.l2_hits, 1);
    assert_eq!(resolved(&rb), ResolveKind::CacheHit);
    assert_eq!(ra.info, rb.info);
    assert_parity(&fx, &rb, &default_specs());
    a.stop();
    b.stop();
}

/// A restarted server process serves the snapshot and its plans from L2 with zero ops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_process_hits_l2_with_zero_ops() {
    let redis = redis_or_skip!();
    redis.store().await;
    let fx = fixture("crc-full");
    let version = latest_version(&fx);
    let req = request(&fx, version, default_specs());
    let first = {
        let p = ServerProc::start("before-restart", &redis.url).await;
        let (r, _) = run(&mut connect(p.addr).await, req.clone(), Behavior::default())
            .await
            .ok();
        assert_eq!(resolved(&r), ResolveKind::FullBuild);
        p.stop();
        r
    };
    let p = ServerProc::start("after-restart", &redis.url).await;
    let mut c = connect(p.addr).await;
    let (r, ops) = run(&mut c, req, Behavior::default()).await.ok();
    assert_eq!(resolved(&r), ResolveKind::CacheHit);
    assert_eq!(op_counts(&r), (0, 0), "ops_issued and ops_local");
    assert!(ops.is_empty());
    assert_eq!(r.info, first.info);
    for (a, b) in r.prefetched_plans.iter().zip(&first.prefetched_plans) {
        assert_eq!(a.result, b.result);
    }
    let s = p.stats().await;
    assert_eq!((s.builds_started, s.l2_hits, s.l2_misses), (0, 1, 0));
    // GetScanPlan by ref on the restarted process: zero ops as well.
    let (plan, _) = run_scan_plan(
        &mut c,
        plan_request(&first, spec_json()),
        Behavior::default(),
    )
    .await
    .ok();
    let st = plan.stats.unwrap();
    assert_eq!((st.ops_issued, st.ops_local), (0, 0));
    p.stop();
}
