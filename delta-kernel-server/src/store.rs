//! Snapshot store tiers (SPEC v10.3).
//!
//! [`SnapshotStore`] is the contract both tiers implement:
//!
//! - **L1**, in process ([`crate::cache::L1Cache`]): payload = the native `Arc<Snapshot>`. It also
//!   owns the in-flight coalescing table; `Core` drives it through its synchronous inherent API so
//!   lookup-or-join stays atomic, and the trait impl exposes the same state.
//! - **L2**, optional and shared across replicas: payload = the encoded kernel snapshot hint
//!   ([`crate::hint`]). [`RedisStore`] is the production backend (`--l2-redis-url`);
//!   [`MemoryStore`] has identical semantics in process (tests, two `Core`s sharing one L2).
//!
//! L2 key schema (all keys prefixed `dks:{kernel_rev}:`):
//!
//! | key | type | value | TTL |
//! |---|---|---|---|
//! | `snap:{table_key}:{seg_id}` | hash | `info`, `seg`, `hint`, `version`, `built_at`, `confirmed_at` | `ttl` (6h) |
//! | `ver:{table_key}` | zset | member `seg_id`, score `version` | `ttl` |
//! | `latest:{table_key}` (path-based) / `latest:{table_key}:{tail_id}` (`catalog_latest`) | hash | `seg_id`, `version`, `confirmed_at_ms`, `tail_id`; monotonic ([`latest_replaces`]) | 2 × staleness, ≥ 60s |
//! | `plan:{seg_id}:{spec_hash}` | string | zstd(plan bytes) \| `EMPTY` \| `UNSUPPORTED:<reason>` | `ttl` |
//! | `lock:{table_key}:{target}` | string (`SET NX PX`) | `{replica}:{job}` | build deadline |
//! | `authz:{sha256(token)}:{table_key}` | string | authorized-until (ms) | until − now |
//!
//! L2 failures never fail a request: callers log them and treat the lookup as a miss.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use prost::Message as _;
use redis::AsyncCommands as _;

use crate::proto::{self, scan_plan_result};

pub type StoreResult<T> = Result<T, String>;

/// Plans whose compressed form exceeds this are not written to L2 (CRC `allFiles` inlining makes
/// plan size proportional to the file count).
pub const MAX_L2_PLAN_BYTES: usize = 4 << 20;
/// Default TTL of `snap`, `ver` and `plan` keys.
pub const DEFAULT_L2_TTL: Duration = Duration::from_secs(6 * 3600);
/// Minimum TTL of a `latest` key.
pub const MIN_LATEST_TTL: Duration = Duration::from_secs(60);

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

/// One snapshot entry. `payload` is tier-specific: the native snapshot in L1, the encoded hint in
/// L2 (`None` when the hint could not be encoded losslessly; the entry then serves info and plans
/// only and is not used to rebuild a native instance).
#[derive(Clone, Debug)]
pub struct StoredSnapshot<P> {
    pub seg_id: String,
    pub version: i64,
    /// Carries `log_segment`, the canonical identity behind `seg_id`.
    pub info: proto::SnapshotInfo,
    pub payload: Option<P>,
    pub built_at_ms: i64,
    pub confirmed_at_ms: i64,
}

/// The last confirmed latest (path-based) or `catalog_latest` result of a table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LatestRecord {
    pub seg_id: String,
    pub version: i64,
    pub confirmed_at_ms: i64,
    /// Hex digest of the canonical catalog log tail for `catalog_latest`; empty for path-based.
    pub tail_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanPut {
    Stored,
    TooLarge(usize),
}

/// Whether `new` may replace `cur` as a table's latest pointer of one target (path-based, or one
/// catalog tail): a higher version, or the same version and tail confirmed later. A slower writer
/// holding an older listing never moves the pointer back.
pub fn latest_replaces(cur: &LatestRecord, new: &LatestRecord) -> bool {
    new.version > cur.version
        || (new.version == cur.version
            && new.tail_id == cur.tail_id
            && new.confirmed_at_ms > cur.confirmed_at_ms)
}

/// `latest` TTL for a staleness bound: 2 × staleness, at least [`MIN_LATEST_TTL`].
pub fn latest_ttl(staleness: Duration) -> Duration {
    (staleness * 2).max(MIN_LATEST_TTL)
}

#[async_trait]
pub trait SnapshotStore: Send + Sync {
    type Payload: Clone + Send + Sync + 'static;

    fn tier(&self) -> &'static str;

    async fn get_snapshot(
        &self,
        table: &str,
        seg_id: &str,
    ) -> StoreResult<Option<StoredSnapshot<Self::Payload>>>;
    /// The entry at `version` with the greatest `confirmed_at` (SPEC v8 rule for requests without
    /// an expected segment).
    async fn snapshot_at_version(
        &self,
        table: &str,
        version: i64,
    ) -> StoreResult<Option<StoredSnapshot<Self::Payload>>>;
    async fn put_snapshot(
        &self,
        table: &str,
        snap: &StoredSnapshot<Self::Payload>,
    ) -> StoreResult<()>;
    /// Re-confirms an existing entry (a fresh build of the same files).
    async fn confirm_snapshot(&self, table: &str, seg_id: &str, at_ms: i64) -> StoreResult<()>;
    /// The latest pointer of one target: path-based (`tail_id` empty) or one catalog tail.
    async fn get_latest(&self, table: &str, tail_id: &str) -> StoreResult<Option<LatestRecord>>;
    /// Monotonic per target: replaces the stored pointer only if [`latest_replaces`].
    async fn put_latest(
        &self,
        table: &str,
        rec: &LatestRecord,
        staleness: Duration,
    ) -> StoreResult<()>;
    async fn get_plan(
        &self,
        seg_id: &str,
        spec_hash: &str,
    ) -> StoreResult<Option<proto::ScanPlanResult>>;
    async fn put_plan(
        &self,
        seg_id: &str,
        spec_hash: &str,
        plan: &proto::ScanPlanResult,
    ) -> StoreResult<PlanPut>;
    /// `SET NX PX`: whether `owner` now holds the build lock of `(table, target)`.
    async fn try_lock(
        &self,
        table: &str,
        target: &str,
        owner: &str,
        ttl: Duration,
    ) -> StoreResult<bool>;
    /// Releases the lock if `owner` still holds it.
    async fn unlock(&self, table: &str, target: &str, owner: &str) -> StoreResult<()>;
    /// Authorized-until (ms) of a token hash for a table, if recorded and not expired.
    async fn get_authz(&self, token_hash: &str, table: &str) -> StoreResult<Option<i64>>;
    async fn put_authz(&self, token_hash: &str, table: &str, until_ms: i64) -> StoreResult<()>;
}

/// The L2 trait object.
pub type L2 = dyn SnapshotStore<Payload = Vec<u8>>;

// ---------- plan value encoding (shared by both L2 backends) ----------

/// L2 string value of a plan result, or `None` if it is too large.
pub fn encode_plan(plan: &proto::ScanPlanResult) -> StoreResult<Result<Vec<u8>, usize>> {
    Ok(match &plan.result {
        Some(scan_plan_result::Result::Plan(bytes)) => {
            let z = zstd::encode_all(bytes.as_slice(), 3).map_err(|e| e.to_string())?;
            if z.len() > MAX_L2_PLAN_BYTES {
                Err(z.len())
            } else {
                Ok(z)
            }
        }
        Some(scan_plan_result::Result::Empty(_)) => Ok(b"EMPTY".to_vec()),
        Some(scan_plan_result::Result::Unsupported(e)) => {
            Ok(format!("UNSUPPORTED:{}", e.message).into_bytes())
        }
        None => return Err("plan result without a value".into()),
    })
}

pub fn decode_plan(v: &[u8]) -> StoreResult<proto::ScanPlanResult> {
    let result = if v == b"EMPTY" {
        scan_plan_result::Result::Empty(proto::StaticallyEmpty {})
    } else if let Some(reason) = v.strip_prefix(b"UNSUPPORTED:") {
        scan_plan_result::Result::Unsupported(proto::Error {
            code: proto::error::Code::Unsupported as i32,
            message: String::from_utf8_lossy(reason).into_owned(),
            retryable: false,
        })
    } else {
        scan_plan_result::Result::Plan(zstd::decode_all(v).map_err(|e| e.to_string())?)
    };
    Ok(proto::ScanPlanResult {
        result: Some(result),
        cache_hit: false,
    })
}

// ---------- in-process L2 ----------

struct Expiring<T> {
    value: T,
    expires: Instant,
}

impl<T> Expiring<T> {
    fn live(&self) -> bool {
        Instant::now() < self.expires
    }
}

#[derive(Default)]
struct MemoryState {
    snaps: HashMap<(String, String), Expiring<StoredSnapshot<Vec<u8>>>>,
    latest: HashMap<(String, String), Expiring<LatestRecord>>,
    plans: HashMap<(String, String), Expiring<Vec<u8>>>,
    locks: HashMap<(String, String), Expiring<String>>,
    authz: HashMap<(String, String), Expiring<i64>>,
}

/// In-process L2 with the same key semantics and TTLs as [`RedisStore`].
pub struct MemoryStore {
    ttl: Duration,
    state: Mutex<MemoryState>,
}

impl MemoryStore {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            state: Mutex::default(),
        }
    }

    /// Number of live keys (tests).
    pub fn key_count(&self) -> usize {
        let st = self.state.lock().unwrap();
        st.snaps.values().filter(|e| e.live()).count()
            + st.latest.values().filter(|e| e.live()).count()
            + st.plans.values().filter(|e| e.live()).count()
    }
}

#[async_trait]
impl SnapshotStore for MemoryStore {
    type Payload = Vec<u8>;

    fn tier(&self) -> &'static str {
        "l2-memory"
    }

    async fn get_snapshot(
        &self,
        table: &str,
        seg_id: &str,
    ) -> StoreResult<Option<StoredSnapshot<Vec<u8>>>> {
        let st = self.state.lock().unwrap();
        Ok(st
            .snaps
            .get(&(table.to_string(), seg_id.to_string()))
            .filter(|e| e.live())
            .map(|e| e.value.clone()))
    }

    async fn snapshot_at_version(
        &self,
        table: &str,
        version: i64,
    ) -> StoreResult<Option<StoredSnapshot<Vec<u8>>>> {
        let st = self.state.lock().unwrap();
        Ok(st
            .snaps
            .iter()
            .filter(|((t, _), e)| t == table && e.live() && e.value.version == version)
            .max_by_key(|(_, e)| e.value.confirmed_at_ms)
            .map(|(_, e)| e.value.clone()))
    }

    async fn put_snapshot(&self, table: &str, snap: &StoredSnapshot<Vec<u8>>) -> StoreResult<()> {
        self.state.lock().unwrap().snaps.insert(
            (table.to_string(), snap.seg_id.clone()),
            Expiring {
                value: snap.clone(),
                expires: Instant::now() + self.ttl,
            },
        );
        Ok(())
    }

    async fn confirm_snapshot(&self, table: &str, seg_id: &str, at_ms: i64) -> StoreResult<()> {
        let mut st = self.state.lock().unwrap();
        if let Some(e) = st.snaps.get_mut(&(table.to_string(), seg_id.to_string())) {
            e.value.confirmed_at_ms = e.value.confirmed_at_ms.max(at_ms);
            e.expires = Instant::now() + self.ttl;
        }
        Ok(())
    }

    async fn get_latest(&self, table: &str, tail_id: &str) -> StoreResult<Option<LatestRecord>> {
        let st = self.state.lock().unwrap();
        Ok(st
            .latest
            .get(&(table.to_string(), tail_id.to_string()))
            .filter(|e| e.live())
            .map(|e| e.value.clone()))
    }

    async fn put_latest(
        &self,
        table: &str,
        rec: &LatestRecord,
        staleness: Duration,
    ) -> StoreResult<()> {
        let mut st = self.state.lock().unwrap();
        let key = (table.to_string(), rec.tail_id.clone());
        let replace = match st.latest.get(&key).filter(|e| e.live()) {
            Some(cur) => latest_replaces(&cur.value, rec),
            None => true,
        };
        if replace {
            st.latest.insert(
                key,
                Expiring {
                    value: rec.clone(),
                    expires: Instant::now() + latest_ttl(staleness),
                },
            );
        }
        Ok(())
    }

    async fn get_plan(
        &self,
        seg_id: &str,
        spec_hash: &str,
    ) -> StoreResult<Option<proto::ScanPlanResult>> {
        let v = {
            let st = self.state.lock().unwrap();
            st.plans
                .get(&(seg_id.to_string(), spec_hash.to_string()))
                .filter(|e| e.live())
                .map(|e| e.value.clone())
        };
        v.map(|v| decode_plan(&v)).transpose()
    }

    async fn put_plan(
        &self,
        seg_id: &str,
        spec_hash: &str,
        plan: &proto::ScanPlanResult,
    ) -> StoreResult<PlanPut> {
        match encode_plan(plan)? {
            Err(len) => Ok(PlanPut::TooLarge(len)),
            Ok(v) => {
                self.state.lock().unwrap().plans.insert(
                    (seg_id.to_string(), spec_hash.to_string()),
                    Expiring {
                        value: v,
                        expires: Instant::now() + self.ttl,
                    },
                );
                Ok(PlanPut::Stored)
            }
        }
    }

    async fn try_lock(
        &self,
        table: &str,
        target: &str,
        owner: &str,
        ttl: Duration,
    ) -> StoreResult<bool> {
        let mut st = self.state.lock().unwrap();
        let key = (table.to_string(), target.to_string());
        if st.locks.get(&key).is_some_and(|e| e.live()) {
            return Ok(false);
        }
        st.locks.insert(
            key,
            Expiring {
                value: owner.to_string(),
                expires: Instant::now() + ttl,
            },
        );
        Ok(true)
    }

    async fn unlock(&self, table: &str, target: &str, owner: &str) -> StoreResult<()> {
        let mut st = self.state.lock().unwrap();
        let key = (table.to_string(), target.to_string());
        if st.locks.get(&key).is_some_and(|e| e.value == owner) {
            st.locks.remove(&key);
        }
        Ok(())
    }

    async fn get_authz(&self, token_hash: &str, table: &str) -> StoreResult<Option<i64>> {
        let st = self.state.lock().unwrap();
        Ok(st
            .authz
            .get(&(token_hash.to_string(), table.to_string()))
            .filter(|e| e.live())
            .map(|e| e.value))
    }

    async fn put_authz(&self, token_hash: &str, table: &str, until_ms: i64) -> StoreResult<()> {
        let ttl_ms = until_ms - now_ms();
        if ttl_ms <= 0 {
            return Ok(());
        }
        self.state.lock().unwrap().authz.insert(
            (token_hash.to_string(), table.to_string()),
            Expiring {
                value: until_ms,
                expires: Instant::now() + Duration::from_millis(ttl_ms as u64),
            },
        );
        Ok(())
    }
}

// ---------- Redis L2 ----------

/// Redis-backed L2 (`--l2-redis-url`). One multiplexed, auto-reconnecting connection.
pub struct RedisStore {
    conn: redis::aio::ConnectionManager,
    prefix: String,
    ttl: Duration,
}

fn rerr(e: redis::RedisError) -> String {
    format!("redis: {e}")
}

impl RedisStore {
    pub async fn connect(url: &str, kernel_rev: &str, ttl: Duration) -> StoreResult<Self> {
        let client = redis::Client::open(url).map_err(rerr)?;
        let conn = redis::aio::ConnectionManager::new(client)
            .await
            .map_err(rerr)?;
        Ok(Self {
            conn,
            prefix: format!("dks:{kernel_rev}:"),
            ttl,
        })
    }

    pub fn key(&self, parts: &[&str]) -> String {
        format!("{}{}", self.prefix, parts.join(":"))
    }

    /// `latest:{table}` for path-based latest, `latest:{table}:{tail_id}` per catalog tail.
    fn latest_key(&self, table: &str, tail_id: &str) -> String {
        if tail_id.is_empty() {
            self.key(&["latest", table])
        } else {
            self.key(&["latest", table, tail_id])
        }
    }

    fn ttl_ms(&self) -> i64 {
        self.ttl.as_millis() as i64
    }

    async fn read_snap(&self, key: &str) -> StoreResult<Option<StoredSnapshot<Vec<u8>>>> {
        let mut conn = self.conn.clone();
        let fields: HashMap<String, Vec<u8>> = conn.hgetall(key).await.map_err(rerr)?;
        if fields.is_empty() {
            return Ok(None);
        }
        let num = |name: &str| -> StoreResult<i64> {
            let v = fields
                .get(name)
                .ok_or_else(|| format!("snap entry without {name}"))?;
            String::from_utf8_lossy(v)
                .parse()
                .map_err(|e| format!("{name}: {e}"))
        };
        let info = proto::SnapshotInfo::decode(
            fields
                .get("info")
                .ok_or("snap entry without info")?
                .as_slice(),
        )
        .map_err(|e| e.to_string())?;
        let seg_id = key.rsplit(':').next().unwrap_or_default().to_string();
        Ok(Some(StoredSnapshot {
            seg_id,
            version: num("version")?,
            info,
            payload: fields.get("hint").filter(|h| !h.is_empty()).cloned(),
            built_at_ms: num("built_at")?,
            confirmed_at_ms: num("confirmed_at")?,
        }))
    }
}

#[async_trait]
impl SnapshotStore for RedisStore {
    type Payload = Vec<u8>;

    fn tier(&self) -> &'static str {
        "l2-redis"
    }

    async fn get_snapshot(
        &self,
        table: &str,
        seg_id: &str,
    ) -> StoreResult<Option<StoredSnapshot<Vec<u8>>>> {
        self.read_snap(&self.key(&["snap", table, seg_id])).await
    }

    async fn snapshot_at_version(
        &self,
        table: &str,
        version: i64,
    ) -> StoreResult<Option<StoredSnapshot<Vec<u8>>>> {
        let mut conn = self.conn.clone();
        let ver = self.key(&["ver", table]);
        let segs: Vec<String> = conn
            .zrangebyscore(&ver, version, version)
            .await
            .map_err(rerr)?;
        let mut best: Option<StoredSnapshot<Vec<u8>>> = None;
        for seg in segs {
            match self.read_snap(&self.key(&["snap", table, &seg])).await? {
                Some(s)
                    if best
                        .as_ref()
                        .is_none_or(|b| s.confirmed_at_ms > b.confirmed_at_ms) =>
                {
                    best = Some(s)
                }
                Some(_) => {}
                // The snap key expired or was evicted: drop the dangling zset member.
                None => {
                    let _: i64 = conn.zrem(&ver, &seg).await.map_err(rerr)?;
                }
            }
        }
        Ok(best)
    }

    async fn put_snapshot(&self, table: &str, snap: &StoredSnapshot<Vec<u8>>) -> StoreResult<()> {
        let key = self.key(&["snap", table, &snap.seg_id]);
        let ver = self.key(&["ver", table]);
        let mut pipe = redis::pipe();
        pipe.atomic()
            .del(&key)
            .hset(&key, "info", snap.info.encode_to_vec())
            .hset(
                &key,
                "seg",
                snap.info
                    .log_segment
                    .as_ref()
                    .map(|s| s.encode_to_vec())
                    .unwrap_or_default(),
            )
            .hset(&key, "version", snap.version)
            .hset(&key, "built_at", snap.built_at_ms)
            .hset(&key, "confirmed_at", snap.confirmed_at_ms);
        if let Some(hint) = &snap.payload {
            pipe.hset(&key, "hint", hint.as_slice());
        }
        pipe.pexpire(&key, self.ttl_ms())
            .zadd(&ver, &snap.seg_id, snap.version)
            .pexpire(&ver, self.ttl_ms());
        let mut conn = self.conn.clone();
        pipe.query_async::<()>(&mut conn).await.map_err(rerr)
    }

    async fn confirm_snapshot(&self, table: &str, seg_id: &str, at_ms: i64) -> StoreResult<()> {
        // Only an existing entry is re-confirmed; never resurrect an expired one without data.
        let script = redis::Script::new(
            r"if redis.call('EXISTS', KEYS[1]) == 1 then
                local cur = tonumber(redis.call('HGET', KEYS[1], 'confirmed_at') or '0')
                if tonumber(ARGV[1]) > cur then redis.call('HSET', KEYS[1], 'confirmed_at', ARGV[1]) end
                redis.call('PEXPIRE', KEYS[1], ARGV[2])
              end
              return 1",
        );
        let mut conn = self.conn.clone();
        script
            .key(self.key(&["snap", table, seg_id]))
            .arg(at_ms)
            .arg(self.ttl_ms())
            .invoke_async::<i64>(&mut conn)
            .await
            .map(|_| ())
            .map_err(rerr)
    }

    async fn get_latest(&self, table: &str, tail_id: &str) -> StoreResult<Option<LatestRecord>> {
        let mut conn = self.conn.clone();
        let fields: HashMap<String, String> = conn
            .hgetall(self.latest_key(table, tail_id))
            .await
            .map_err(rerr)?;
        if fields.is_empty() {
            return Ok(None);
        }
        let get = |k: &str| fields.get(k).cloned().unwrap_or_default();
        Ok(Some(LatestRecord {
            seg_id: get("seg_id"),
            version: get("version")
                .parse()
                .map_err(|e| format!("version: {e}"))?,
            confirmed_at_ms: get("confirmed_at_ms")
                .parse()
                .map_err(|e| format!("confirmed_at_ms: {e}"))?,
            tail_id: get("tail_id"),
        }))
    }

    async fn put_latest(
        &self,
        table: &str,
        rec: &LatestRecord,
        staleness: Duration,
    ) -> StoreResult<()> {
        // Compare-and-set in one script (atomic in Redis), with the rule of `latest_replaces`.
        let script = redis::Script::new(
            r"local v = redis.call('HGET', KEYS[1], 'version')
              local replace = true
              if v then
                local cur_v = tonumber(v)
                local new_v = tonumber(ARGV[2])
                local cur_c = tonumber(redis.call('HGET', KEYS[1], 'confirmed_at_ms') or '-1')
                local cur_t = redis.call('HGET', KEYS[1], 'tail_id') or ''
                replace = new_v > cur_v
                  or (new_v == cur_v and ARGV[4] == cur_t and tonumber(ARGV[3]) > cur_c)
              end
              if replace then
                redis.call('HSET', KEYS[1], 'seg_id', ARGV[1], 'version', ARGV[2],
                           'confirmed_at_ms', ARGV[3], 'tail_id', ARGV[4])
                redis.call('PEXPIRE', KEYS[1], ARGV[5])
                return 1
              end
              return 0",
        );
        let mut conn = self.conn.clone();
        script
            .key(self.latest_key(table, &rec.tail_id))
            .arg(&rec.seg_id)
            .arg(rec.version)
            .arg(rec.confirmed_at_ms)
            .arg(&rec.tail_id)
            .arg(latest_ttl(staleness).as_millis() as i64)
            .invoke_async::<i64>(&mut conn)
            .await
            .map(|_| ())
            .map_err(rerr)
    }

    async fn get_plan(
        &self,
        seg_id: &str,
        spec_hash: &str,
    ) -> StoreResult<Option<proto::ScanPlanResult>> {
        let mut conn = self.conn.clone();
        let v: Option<Vec<u8>> = conn
            .get(self.key(&["plan", seg_id, spec_hash]))
            .await
            .map_err(rerr)?;
        v.map(|v| decode_plan(&v)).transpose()
    }

    async fn put_plan(
        &self,
        seg_id: &str,
        spec_hash: &str,
        plan: &proto::ScanPlanResult,
    ) -> StoreResult<PlanPut> {
        match encode_plan(plan)? {
            Err(len) => Ok(PlanPut::TooLarge(len)),
            Ok(v) => {
                let mut conn = self.conn.clone();
                let _: () = redis::cmd("SET")
                    .arg(self.key(&["plan", seg_id, spec_hash]))
                    .arg(v)
                    .arg("PX")
                    .arg(self.ttl_ms())
                    .query_async(&mut conn)
                    .await
                    .map_err(rerr)?;
                Ok(PlanPut::Stored)
            }
        }
    }

    async fn try_lock(
        &self,
        table: &str,
        target: &str,
        owner: &str,
        ttl: Duration,
    ) -> StoreResult<bool> {
        let mut conn = self.conn.clone();
        let r: Option<String> = redis::cmd("SET")
            .arg(self.key(&["lock", table, target]))
            .arg(owner)
            .arg("NX")
            .arg("PX")
            .arg(ttl.as_millis().max(1) as i64)
            .query_async(&mut conn)
            .await
            .map_err(rerr)?;
        Ok(r.is_some())
    }

    async fn unlock(&self, table: &str, target: &str, owner: &str) -> StoreResult<()> {
        let script = redis::Script::new(
            r"if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
              return 0",
        );
        let mut conn = self.conn.clone();
        script
            .key(self.key(&["lock", table, target]))
            .arg(owner)
            .invoke_async::<i64>(&mut conn)
            .await
            .map(|_| ())
            .map_err(rerr)
    }

    async fn get_authz(&self, token_hash: &str, table: &str) -> StoreResult<Option<i64>> {
        let mut conn = self.conn.clone();
        let v: Option<i64> = conn
            .get(self.key(&["authz", token_hash, table]))
            .await
            .map_err(rerr)?;
        Ok(v.filter(|until| *until > now_ms()))
    }

    async fn put_authz(&self, token_hash: &str, table: &str, until_ms: i64) -> StoreResult<()> {
        let ttl_ms = until_ms - now_ms();
        if ttl_ms <= 0 {
            return Ok(());
        }
        let mut conn = self.conn.clone();
        redis::cmd("SET")
            .arg(self.key(&["authz", token_hash, table]))
            .arg(until_ms)
            .arg("PX")
            .arg(ttl_ms)
            .query_async::<()>(&mut conn)
            .await
            .map_err(rerr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(bytes: Vec<u8>) -> proto::ScanPlanResult {
        proto::ScanPlanResult {
            result: Some(scan_plan_result::Result::Plan(bytes)),
            cache_hit: false,
        }
    }

    #[test]
    fn plan_values_round_trip() {
        for r in [
            plan(b"plan bytes".repeat(100)),
            proto::ScanPlanResult {
                result: Some(scan_plan_result::Result::Empty(proto::StaticallyEmpty {})),
                cache_hit: false,
            },
            proto::ScanPlanResult {
                result: Some(scan_plan_result::Result::Unsupported(proto::Error {
                    code: proto::error::Code::Unsupported as i32,
                    message: "TIMESTAMP partition with a timezone".into(),
                    retryable: false,
                })),
                cache_hit: false,
            },
        ] {
            let v = encode_plan(&r).unwrap().unwrap();
            assert_eq!(decode_plan(&v).unwrap(), r);
        }
    }

    /// Plans whose zstd form exceeds 4 MiB are never written (CRC allFiles inlining).
    #[tokio::test]
    async fn plans_over_four_mib_compressed_are_not_stored() {
        let mut big = vec![0u8; MAX_L2_PLAN_BYTES + 4096];
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        for b in big.iter_mut() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = x as u8;
        }
        let store = MemoryStore::new(DEFAULT_L2_TTL);
        assert!(matches!(
            store.put_plan("s", "big", &plan(big)).await.unwrap(),
            PlanPut::TooLarge(n) if n > MAX_L2_PLAN_BYTES
        ));
        assert!(store.get_plan("s", "big").await.unwrap().is_none());
        // Compressible plans of the same raw size are stored.
        let compressible = plan(vec![b'a'; MAX_L2_PLAN_BYTES + 4096]);
        assert_eq!(
            store.put_plan("s", "ok", &compressible).await.unwrap(),
            PlanPut::Stored
        );
    }

    #[test]
    fn latest_ttl_is_twice_staleness_at_least_a_minute() {
        assert_eq!(latest_ttl(Duration::ZERO), MIN_LATEST_TTL);
        assert_eq!(latest_ttl(Duration::from_secs(10)), MIN_LATEST_TTL);
        assert_eq!(
            latest_ttl(Duration::from_secs(300)),
            Duration::from_secs(600)
        );
    }
}
