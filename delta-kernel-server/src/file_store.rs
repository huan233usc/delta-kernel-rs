//! File-backed L2 snapshot store (SPEC v12, `--l2-dir <dir>`).
//!
//! Same key semantics and never-store rules as [`crate::store::RedisStore`]: the server writes
//! through it exactly as it writes through Redis, and the snapshot, plan and latest values are
//! the same. Layout under `<dir>/<kernel_rev>/`:
//!
//! | path | Redis key | content |
//! |---|---|---|
//! | `snap/<table_key>/<seg_id>.bin` | `snap:{table_key}:{seg_id}` | envelope: `info`, `seg`, `hint`, `version`, `built_at`, `confirmed_at`, `table_id`, `expires_at` |
//! | `ver/<table_key>.json` | `ver:{table_key}` | `members`: `seg_id` → `{version, confirmed_at_ms}` |
//! | `latest/<table_key>.json`, `latest/<table_key>__<tail_id>.json` | `latest:{table_key}[:{tail_id}]` | `seg_id`, `version`, `confirmed_at_ms`, `tail_id`; monotonic ([`latest_replaces`]) |
//! | `plan/<seg_id>/<spec_hash>.bin` | `plan:{seg_id}:{spec_hash}` | envelope: `value` = zstd(plan) \| `EMPTY` \| `UNSUPPORTED:<reason>` |
//!
//! - **Not persisted**: `lock:*` and `authz:*` live in process memory only. Every process checks
//!   authz with UC itself, and there is no cross-process single flight: each process builds on its
//!   own miss and writes the result through.
//! - **Atomic writes**: every write goes to a temporary file (`.<name>.tmp-<pid>-<n>`) in the
//!   target directory and is renamed over the target, so a reader sees the old or the new file,
//!   never a partial one. Files are not fsynced; a file torn by a host crash fails its checksum.
//! - **Read-compare-rename** (`ver`, `latest`): serialized in process and, where the filesystem
//!   supports `flock`, across processes. Without `flock`, concurrent processes can lose at most one
//!   update (accepted by SPEC v12).
//! - **Validation**: every file names its kernel revision and its own key and is checksummed:
//!   `.bin` files end with a SHA-256 of their content; `.json` files are sealed as `{"body": {...},
//!   "sha256": ...}` with the digest over the whole body (unknown fields rejected). A corrupt file
//!   (including a valid-JSON change to any field), or one whose kernel revision or key does not
//!   match, is a miss logged at WARN (and counted in [`FileStore::rejected`]); preload skips it;
//!   the next write of that key replaces it.
//! - **Read-only** (`--l2-read-only`, SPEC v12.1, [`FileStore::open_read_only`]): reads and preload
//!   work as above; every write is skipped before touching the filesystem (no entry, no temporary
//!   file, no directory, no `flock`), so the directory stays byte-identical.
//! - **Expiry**: with `--l2-ttl-secs`, `snap` and `plan` entries expire like the Redis keys
//!   (refreshed on put and confirm). Without it they never expire: they are content-addressed (an
//!   old entry is never wrong) and a restarted server must still find them. `latest` pointers never
//!   expire. Freshness is decided by `confirmed_at_ms` against the request's staleness, so an old
//!   pointer is only a base for one re-listing. Redis drops them after 2 × staleness instead.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{ErrorKind, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use prost::Message as _;
use serde::{Deserialize, Serialize};
use sha2::Digest as _;

use crate::proto;
use crate::store::{
    LatestRecord, PlanPut, SnapshotStore, StoreResult, StoredSnapshot, decode_plan, encode_plan,
    latest_replaces, now_ms,
};

/// Envelope magic of `.bin` entries: `DKSL2` plus the envelope format.
const MAGIC: &[u8; 6] = b"DKSL2\x01";
const KIND_SNAP: u8 = b'S';
const KIND_PLAN: u8 = b'P';
/// `format` of the JSON entries (2: sealed with a digest; unsealed format-1 files are rejected).
const JSON_FORMAT: u32 = 2;
const DIGEST_LEN: usize = 32;
const MAX_PART_LEN: usize = 128;

/// Temporary-file sequence, process-wide so two stores on one directory never collide.
static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Everything a preload needs from the store: every valid entry under `<dir>/<kernel_rev>/`.
#[derive(Debug, Default)]
pub struct L2Scan {
    /// `(table_key, entry)`; `confirmed_at_ms` includes re-confirmations in the version index.
    pub snapshots: Vec<(String, StoredSnapshot<Vec<u8>>)>,
    /// `(table_key, pointer)`, path-based and catalog-tail.
    pub latest: Vec<(String, LatestRecord)>,
    /// `(seg_id, spec_hash)` of every plan file (read on demand with [`SnapshotStore::get_plan`]).
    pub plans: Vec<(String, String)>,
    /// Expired `snap` entries (with `--l2-ttl-secs`).
    pub expired: u64,
    /// Sibling directories of other kernel revisions (ignored).
    pub other_revisions: Vec<String>,
}

/// A JSON entry: `{"body": {...}, "sha256": hex(SHA-256(canonical body))}`. The body carries every
/// field, including `format`, `kernel_rev` and the entry's own key, so any change to any of them
/// (a bit flip that still parses, a rewritten field) fails the digest. Unknown fields are rejected.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Sealed<T> {
    body: T,
    sha256: String,
}

/// Canonical bytes of a JSON body: serde_json over the struct (field order fixed, maps sorted).
fn json_digest<T: Serialize>(body: &T) -> Result<String, String> {
    let bytes = serde_json::to_vec(body).map_err(|e| e.to_string())?;
    Ok(hex::encode(sha2::Sha256::digest(&bytes)))
}

fn seal<T: Serialize>(body: &T) -> StoreResult<Vec<u8>> {
    let sealed = Sealed {
        body,
        sha256: json_digest(body)?,
    };
    serde_json::to_vec(&sealed).map_err(|e| e.to_string())
}

/// Parses a sealed JSON entry and checks its digest over the re-serialized body.
fn unseal<T: Serialize + serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    let sealed: Sealed<T> =
        serde_json::from_slice(bytes).map_err(|e| format!("not a sealed entry: {e}"))?;
    if json_digest(&sealed.body)? != sealed.sha256 {
        return Err("checksum mismatch".into());
    }
    Ok(sealed.body)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
struct LatestFile {
    format: u32,
    kernel_rev: String,
    table_key: String,
    tail_id: String,
    seg_id: String,
    version: i64,
    confirmed_at_ms: i64,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
#[serde(deny_unknown_fields)]
struct VerMember {
    version: i64,
    confirmed_at_ms: i64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct VerFile {
    format: u32,
    kernel_rev: String,
    table_key: String,
    members: BTreeMap<String, VerMember>,
}

/// One file read.
enum Entry<T> {
    Absent,
    Valid(T),
    /// Past its `expires_at` (with `--l2-ttl-secs`).
    Expired,
    /// Corrupt, another kernel revision, or another key; logged and counted.
    Rejected,
}

/// File-backed L2 (`--l2-dir`).
pub struct FileStore {
    inner: Arc<Inner>,
    /// Process-local build locks (never persisted).
    locks: Mutex<HashMap<(String, String), (String, Instant)>>,
    /// Process-local authz entries (never persisted).
    authz: Mutex<HashMap<(String, String), i64>>,
}

struct Inner {
    /// `<dir>/<kernel_rev>`.
    root: PathBuf,
    kernel_rev: String,
    ttl: Option<Duration>,
    /// Serializes this process's read-compare-rename updates.
    rmw: Mutex<()>,
    rejected: AtomicU64,
    /// `--l2-read-only`: never write under the directory.
    read_only: bool,
    /// Writes skipped because the store is read-only.
    skipped_writes: AtomicU64,
    /// Entry files written (renamed into place).
    writes: AtomicU64,
    /// Test hook: delay before each file write.
    write_delay_ms: AtomicU64,
    /// Entry files read (opened), and a test-hook delay before each read.
    reads: AtomicU64,
    read_delay_ms: AtomicU64,
}

/// Holds the in-process RMW mutex and, where supported, an exclusive `flock` on the directory of
/// the file being updated. The flock is released first (field order).
struct RmwGuard<'a> {
    _flock: Option<File>,
    _guard: MutexGuard<'a, ()>,
}

/// Validates one key component as a file name: `[A-Za-z0-9_-]`, no leading or trailing `_` and
/// no `__` (the `latest` file separator), at most 128 bytes.
fn part(s: &str) -> StoreResult<&str> {
    let ok = !s.is_empty()
        && s.len() <= MAX_PART_LEN
        && !s.starts_with('_')
        && !s.ends_with('_')
        && !s.contains("__")
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if ok {
        Ok(s)
    } else {
        Err(format!("file L2: key part {s:?} is not a safe file name"))
    }
}

fn read_file(path: &Path) -> StoreResult<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(b) => Ok(Some(b)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("file L2: read {}: {e}", path.display())),
    }
}

/// Directory entries `(name, is_dir)`, skipping hidden names (temporary files); a missing
/// directory is empty.
fn list_dir(dir: &Path) -> StoreResult<Vec<(String, bool)>> {
    let rd = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(format!("file L2: list {}: {e}", dir.display())),
    };
    let mut out = vec![];
    for entry in rd {
        let entry = entry.map_err(|e| format!("file L2: list {}: {e}", dir.display()))?;
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if name.starts_with('.') {
            continue;
        }
        let is_dir = entry.file_type().is_ok_and(|t| t.is_dir());
        out.push((name, is_dir));
    }
    out.sort();
    Ok(out)
}

fn expired(expires_at_ms: i64) -> bool {
    expires_at_ms > 0 && expires_at_ms <= now_ms()
}

// ---------- `.bin` envelope ----------

/// `MAGIC, kind, u16 count, {u8 name_len, name, u32 value_len, value}*, sha256(all before)`.
fn encode_envelope(kind: u8, fields: &[(&str, &[u8])]) -> Vec<u8> {
    let size: usize = fields.iter().map(|(n, v)| 5 + n.len() + v.len()).sum();
    let mut out = Vec::with_capacity(MAGIC.len() + 3 + size + DIGEST_LEN);
    out.extend_from_slice(MAGIC);
    out.push(kind);
    out.extend_from_slice(&(fields.len() as u16).to_le_bytes());
    for (name, value) in fields {
        out.push(name.len() as u8);
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value);
    }
    let digest = sha2::Sha256::digest(&out);
    out.extend_from_slice(&digest);
    out
}

struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.0.len() < n {
            return Err("truncated field".into());
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }
}

fn decode_envelope(kind: u8, bytes: &[u8]) -> Result<HashMap<String, Vec<u8>>, String> {
    if bytes.len() < MAGIC.len() + 3 + DIGEST_LEN {
        return Err(format!("truncated ({} bytes)", bytes.len()));
    }
    let (body, digest) = bytes.split_at(bytes.len() - DIGEST_LEN);
    if sha2::Sha256::digest(body).as_slice() != digest {
        return Err("checksum mismatch".into());
    }
    let mut c = Cursor(body);
    if c.take(MAGIC.len())? != MAGIC {
        return Err("unknown envelope format".into());
    }
    let k = c.take(1)?[0];
    if k != kind {
        return Err(format!(
            "entry kind {:?}, expected {:?}",
            k as char, kind as char
        ));
    }
    let count = u16::from_le_bytes(c.take(2)?.try_into().expect("2 bytes"));
    let mut fields = HashMap::with_capacity(count as usize);
    for _ in 0..count {
        let n = c.take(1)?[0] as usize;
        let name = String::from_utf8(c.take(n)?.to_vec()).map_err(|_| "field name not UTF-8")?;
        let len = u32::from_le_bytes(c.take(4)?.try_into().expect("4 bytes")) as usize;
        fields.insert(name, c.take(len)?.to_vec());
    }
    if !c.0.is_empty() {
        return Err("trailing bytes".into());
    }
    Ok(fields)
}

fn field<'a>(f: &'a HashMap<String, Vec<u8>>, name: &str) -> Result<&'a [u8], String> {
    f.get(name)
        .map(Vec::as_slice)
        .ok_or_else(|| format!("no {name} field"))
}

fn text(f: &HashMap<String, Vec<u8>>, name: &str) -> Result<String, String> {
    String::from_utf8(field(f, name)?.to_vec()).map_err(|_| format!("{name} is not UTF-8"))
}

fn num(f: &HashMap<String, Vec<u8>>, name: &str) -> Result<i64, String> {
    text(f, name)?.parse().map_err(|e| format!("{name}: {e}"))
}

impl Inner {
    fn snap_path(&self, table: &str, seg: &str) -> StoreResult<PathBuf> {
        Ok(self
            .root
            .join("snap")
            .join(part(table)?)
            .join(format!("{}.bin", part(seg)?)))
    }

    fn ver_path(&self, table: &str) -> StoreResult<PathBuf> {
        Ok(self.root.join("ver").join(format!("{}.json", part(table)?)))
    }

    fn latest_path(&self, table: &str, tail_id: &str) -> StoreResult<PathBuf> {
        let name = if tail_id.is_empty() {
            format!("{}.json", part(table)?)
        } else {
            format!("{}__{}.json", part(table)?, part(tail_id)?)
        };
        Ok(self.root.join("latest").join(name))
    }

    fn plan_path(&self, seg: &str, spec: &str) -> StoreResult<PathBuf> {
        Ok(self
            .root
            .join("plan")
            .join(part(seg)?)
            .join(format!("{}.bin", part(spec)?)))
    }

    fn expiry(&self) -> i64 {
        self.ttl
            .map_or(0, |ttl| now_ms() + ttl.as_millis().max(1) as i64)
    }

    /// Reads an entry file (`None` if absent), counting the read.
    fn read_opt(&self, path: &Path) -> StoreResult<Option<Vec<u8>>> {
        let delay = self.read_delay_ms.load(Ordering::Relaxed);
        if delay > 0 {
            std::thread::sleep(Duration::from_millis(delay));
        }
        self.reads.fetch_add(1, Ordering::Relaxed);
        read_file(path)
    }

    fn check_rev(&self, rev: &str) -> Result<(), String> {
        if rev == self.kernel_rev {
            Ok(())
        } else {
            Err(format!(
                "written by kernel_rev {rev}, server is {}",
                self.kernel_rev
            ))
        }
    }

    fn reject<T>(&self, path: &Path, why: impl std::fmt::Display) -> Entry<T> {
        self.rejected.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            path = %path.display(),
            reason = %why,
            "L2 file entry rejected; treated as a miss"
        );
        Entry::Rejected
    }

    fn write_atomic(&self, path: &Path, bytes: &[u8]) -> StoreResult<()> {
        // Unreachable: every write method returns before this on a read-only store.
        if self.read_only {
            return Err("file L2: read-only store".into());
        }
        let delay = self.write_delay_ms.load(Ordering::Relaxed);
        if delay > 0 {
            std::thread::sleep(Duration::from_millis(delay));
        }
        let dir = path.parent().expect("entry paths have a parent");
        fs::create_dir_all(dir).map_err(|e| format!("file L2: create {}: {e}", dir.display()))?;
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("entry");
        let tmp = dir.join(format!(
            ".{name}.tmp-{}-{}",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let written = File::create(&tmp)
            .and_then(|mut f| f.write_all(bytes))
            .and_then(|()| fs::rename(&tmp, path));
        if let Err(e) = written {
            let _ = fs::remove_file(&tmp);
            return Err(format!("file L2: write {}: {e}", path.display()));
        }
        self.writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn rmw_lock(&self, dir: &Path) -> StoreResult<RmwGuard<'_>> {
        // Unreachable on a read-only store, like `write_atomic`.
        if self.read_only {
            return Err("file L2: read-only store".into());
        }
        let guard = self.rmw.lock().unwrap_or_else(|e| e.into_inner());
        fs::create_dir_all(dir).map_err(|e| format!("file L2: create {}: {e}", dir.display()))?;
        // Advisory and best effort: a filesystem without flock degrades to SPEC v12's "at most
        // one lost update" across processes.
        let flock = File::open(dir).ok().filter(|f| f.lock().is_ok());
        Ok(RmwGuard {
            _flock: flock,
            _guard: guard,
        })
    }

    // ---------- snap ----------

    fn encode_snap(&self, table: &str, snap: &StoredSnapshot<Vec<u8>>, expires_at: i64) -> Vec<u8> {
        let info = snap.info.encode_to_vec();
        let seg = snap
            .info
            .log_segment
            .as_ref()
            .map(|s| s.encode_to_vec())
            .unwrap_or_default();
        let (version, built, confirmed, expires) = (
            snap.version.to_string(),
            snap.built_at_ms.to_string(),
            snap.confirmed_at_ms.to_string(),
            expires_at.to_string(),
        );
        let mut fields: Vec<(&str, &[u8])> = vec![
            ("kernel_rev", self.kernel_rev.as_bytes()),
            ("table_key", table.as_bytes()),
            ("seg_id", snap.seg_id.as_bytes()),
            ("table_id", snap.table_id.as_bytes()),
            ("version", version.as_bytes()),
            ("built_at", built.as_bytes()),
            ("confirmed_at", confirmed.as_bytes()),
            ("expires_at", expires.as_bytes()),
            ("info", &info),
            ("seg", &seg),
        ];
        if let Some(hint) = &snap.payload {
            fields.push(("hint", hint));
        }
        encode_envelope(KIND_SNAP, &fields)
    }

    fn decode_snap(
        &self,
        bytes: &[u8],
        table: &str,
        seg: &str,
    ) -> Result<Option<StoredSnapshot<Vec<u8>>>, String> {
        let f = decode_envelope(KIND_SNAP, bytes)?;
        self.check_rev(&text(&f, "kernel_rev")?)?;
        let (tk, sid) = (text(&f, "table_key")?, text(&f, "seg_id")?);
        if tk != table || sid != seg {
            return Err(format!("file holds {tk}/{sid}"));
        }
        if expired(num(&f, "expires_at")?) {
            return Ok(None);
        }
        let info =
            proto::SnapshotInfo::decode(field(&f, "info")?).map_err(|e| format!("info: {e}"))?;
        Ok(Some(StoredSnapshot {
            seg_id: sid,
            table_id: text(&f, "table_id")?,
            version: num(&f, "version")?,
            info,
            payload: f.get("hint").filter(|h| !h.is_empty()).cloned(),
            built_at_ms: num(&f, "built_at")?,
            confirmed_at_ms: num(&f, "confirmed_at")?,
        }))
    }

    fn read_snap(&self, table: &str, seg: &str) -> StoreResult<Entry<StoredSnapshot<Vec<u8>>>> {
        let path = self.snap_path(table, seg)?;
        let Some(bytes) = self.read_opt(&path)? else {
            return Ok(Entry::Absent);
        };
        Ok(match self.decode_snap(&bytes, table, seg) {
            Ok(Some(s)) => Entry::Valid(s),
            Ok(None) => Entry::Expired,
            Err(why) => self.reject(&path, why),
        })
    }

    fn get_snapshot(&self, table: &str, seg: &str) -> StoreResult<Option<StoredSnapshot<Vec<u8>>>> {
        Ok(match self.read_snap(table, seg)? {
            Entry::Valid(s) => Some(s),
            _ => None,
        })
    }

    fn put_snapshot(&self, table: &str, snap: &StoredSnapshot<Vec<u8>>) -> StoreResult<()> {
        let path = self.snap_path(table, &snap.seg_id)?;
        self.write_atomic(&path, &self.encode_snap(table, snap, self.expiry()))?;
        let (seg, member) = (
            snap.seg_id.clone(),
            VerMember {
                version: snap.version,
                confirmed_at_ms: snap.confirmed_at_ms,
            },
        );
        self.update_ver(table, |members| {
            members.insert(seg, member);
            true
        })
    }

    fn confirm_snapshot(&self, table: &str, seg: &str, at_ms: i64) -> StoreResult<()> {
        // With a TTL, re-confirming extends the entry's expiry (Redis PEXPIRE): rewrite it.
        if self.ttl.is_some()
            && let Entry::Valid(mut s) = self.read_snap(table, seg)?
        {
            s.confirmed_at_ms = s.confirmed_at_ms.max(at_ms);
            let path = self.snap_path(table, seg)?;
            self.write_atomic(&path, &self.encode_snap(table, &s, self.expiry()))?;
        }
        let ttl = self.ttl.is_some();
        self.update_ver(table, |members| {
            let Some(version) = members.get(seg).map(|m| m.version) else {
                return false;
            };
            // The confirmation only orders entries of one version (`snapshot_at_version`): a sole
            // entry at its version gains nothing from the write.
            let shared = members.values().filter(|m| m.version == version).count() > 1;
            let m = members.get_mut(seg).expect("checked");
            if (shared || ttl) && at_ms > m.confirmed_at_ms {
                m.confirmed_at_ms = at_ms;
                true
            } else {
                false
            }
        })
    }

    // ---------- ver ----------

    fn read_ver(&self, table: &str) -> StoreResult<Entry<VerFile>> {
        let path = self.ver_path(table)?;
        let Some(bytes) = self.read_opt(&path)? else {
            return Ok(Entry::Absent);
        };
        let ver = match unseal::<VerFile>(&bytes) {
            Ok(v) => v,
            Err(e) => return Ok(self.reject(&path, format!("version index: {e}"))),
        };
        if ver.format != JSON_FORMAT {
            return Ok(self.reject(&path, format!("format {}", ver.format)));
        }
        if let Err(why) = self.check_rev(&ver.kernel_rev) {
            return Ok(self.reject(&path, why));
        }
        if ver.table_key != table {
            return Ok(self.reject(&path, format!("file holds {}", ver.table_key)));
        }
        Ok(Entry::Valid(ver))
    }

    /// Read-compare-rename of a table's version index; `update` returns whether it changed.
    fn update_ver(
        &self,
        table: &str,
        update: impl FnOnce(&mut BTreeMap<String, VerMember>) -> bool,
    ) -> StoreResult<()> {
        let path = self.ver_path(table)?;
        let _lock = self.rmw_lock(path.parent().expect("has a parent"))?;
        let mut ver = match self.read_ver(table)? {
            Entry::Valid(v) => v,
            _ => VerFile {
                format: JSON_FORMAT,
                kernel_rev: self.kernel_rev.clone(),
                table_key: table.to_string(),
                members: BTreeMap::new(),
            },
        };
        if update(&mut ver.members) {
            let bytes = seal(&ver)?;
            self.write_atomic(&path, &bytes)?;
        }
        Ok(())
    }

    fn snapshot_at_version(
        &self,
        table: &str,
        version: i64,
    ) -> StoreResult<Option<StoredSnapshot<Vec<u8>>>> {
        let Entry::Valid(ver) = self.read_ver(table)? else {
            return Ok(None);
        };
        let mut best: Option<StoredSnapshot<Vec<u8>>> = None;
        let mut dangling = vec![];
        for (seg, m) in ver.members.iter().filter(|(_, m)| m.version == version) {
            match self.read_snap(table, seg)? {
                Entry::Valid(mut s) if s.version == version => {
                    s.confirmed_at_ms = s.confirmed_at_ms.max(m.confirmed_at_ms);
                    if best
                        .as_ref()
                        .is_none_or(|b| s.confirmed_at_ms > b.confirmed_at_ms)
                    {
                        best = Some(s);
                    }
                }
                Entry::Absent | Entry::Expired => dangling.push(seg.clone()),
                Entry::Valid(_) | Entry::Rejected => {}
            }
        }
        // The snap entry expired or was deleted: drop the dangling index member (Redis ZREM),
        // unless a concurrent put just wrote it again. A read-only store leaves it.
        if !dangling.is_empty() && !self.read_only {
            self.update_ver(table, |members| {
                let mut changed = false;
                for seg in &dangling {
                    let gone = !matches!(self.read_snap(table, seg), Ok(Entry::Valid(_)));
                    changed |= gone && members.remove(seg).is_some();
                }
                changed
            })?;
        }
        Ok(best)
    }

    // ---------- latest ----------

    fn read_latest_file(&self, path: &Path) -> StoreResult<Entry<LatestFile>> {
        let Some(bytes) = self.read_opt(path)? else {
            return Ok(Entry::Absent);
        };
        let rec = match unseal::<LatestFile>(&bytes) {
            Ok(r) => r,
            Err(e) => return Ok(self.reject(path, format!("latest pointer: {e}"))),
        };
        if rec.format != JSON_FORMAT {
            return Ok(self.reject(path, format!("format {}", rec.format)));
        }
        if let Err(why) = self.check_rev(&rec.kernel_rev) {
            return Ok(self.reject(path, why));
        }
        Ok(Entry::Valid(rec))
    }

    fn read_latest(&self, table: &str, tail_id: &str) -> StoreResult<Entry<LatestRecord>> {
        let path = self.latest_path(table, tail_id)?;
        Ok(match self.read_latest_file(&path)? {
            Entry::Valid(f) if f.table_key == table && f.tail_id == tail_id => {
                Entry::Valid(LatestRecord {
                    seg_id: f.seg_id,
                    version: f.version,
                    confirmed_at_ms: f.confirmed_at_ms,
                    tail_id: f.tail_id,
                })
            }
            Entry::Valid(f) => self.reject(
                &path,
                format!("file holds {} tail {:?}", f.table_key, f.tail_id),
            ),
            Entry::Absent => Entry::Absent,
            Entry::Expired => Entry::Expired,
            Entry::Rejected => Entry::Rejected,
        })
    }

    fn put_latest(&self, table: &str, rec: &LatestRecord) -> StoreResult<()> {
        let path = self.latest_path(table, &rec.tail_id)?;
        part(&rec.seg_id)?;
        let _lock = self.rmw_lock(path.parent().expect("has a parent"))?;
        let replace = match self.read_latest(table, &rec.tail_id)? {
            Entry::Valid(cur) => latest_replaces(&cur, rec),
            _ => true,
        };
        if replace {
            let file = LatestFile {
                format: JSON_FORMAT,
                kernel_rev: self.kernel_rev.clone(),
                table_key: table.to_string(),
                tail_id: rec.tail_id.clone(),
                seg_id: rec.seg_id.clone(),
                version: rec.version,
                confirmed_at_ms: rec.confirmed_at_ms,
            };
            let bytes = seal(&file)?;
            self.write_atomic(&path, &bytes)?;
        }
        Ok(())
    }

    // ---------- plan ----------

    fn get_plan(&self, seg: &str, spec: &str) -> StoreResult<Option<proto::ScanPlanResult>> {
        let path = self.plan_path(seg, spec)?;
        let Some(bytes) = self.read_opt(&path)? else {
            return Ok(None);
        };
        let decoded = (|| -> Result<Option<proto::ScanPlanResult>, String> {
            let f = decode_envelope(KIND_PLAN, &bytes)?;
            self.check_rev(&text(&f, "kernel_rev")?)?;
            let (sid, sh) = (text(&f, "seg_id")?, text(&f, "spec_hash")?);
            if sid != seg || sh != spec {
                return Err(format!("file holds {sid}/{sh}"));
            }
            if expired(num(&f, "expires_at")?) {
                return Ok(None);
            }
            decode_plan(field(&f, "value")?).map(Some)
        })();
        Ok(match decoded {
            Ok(p) => p,
            Err(why) => {
                self.reject::<()>(&path, why);
                None
            }
        })
    }

    fn put_plan(
        &self,
        seg: &str,
        spec: &str,
        plan: &proto::ScanPlanResult,
    ) -> StoreResult<PlanPut> {
        let path = self.plan_path(seg, spec)?;
        match encode_plan(plan)? {
            Err(len) => Ok(PlanPut::TooLarge(len)),
            Ok(value) => {
                let expires = self.expiry().to_string();
                let bytes = encode_envelope(
                    KIND_PLAN,
                    &[
                        ("kernel_rev", self.kernel_rev.as_bytes()),
                        ("seg_id", seg.as_bytes()),
                        ("spec_hash", spec.as_bytes()),
                        ("expires_at", expires.as_bytes()),
                        ("value", &value),
                    ],
                );
                self.write_atomic(&path, &bytes)?;
                Ok(PlanPut::Stored)
            }
        }
    }

    // ---------- scan ----------

    fn scan(&self) -> StoreResult<L2Scan> {
        let mut scan = L2Scan::default();
        if let Some(parent) = self.root.parent() {
            scan.other_revisions = list_dir(parent)?
                .into_iter()
                .filter(|(name, is_dir)| *is_dir && *name != self.kernel_rev)
                .map(|(name, _)| name)
                .collect();
        }
        for (table, is_dir) in list_dir(&self.root.join("snap"))? {
            if !is_dir || part(&table).is_err() {
                continue;
            }
            let confirmations = match self.read_ver(&table)? {
                Entry::Valid(v) => v.members,
                _ => BTreeMap::new(),
            };
            for (name, _) in list_dir(&self.root.join("snap").join(&table))? {
                let Some(seg) = name.strip_suffix(".bin").filter(|s| part(s).is_ok()) else {
                    continue;
                };
                match self.read_snap(&table, seg)? {
                    Entry::Valid(mut s) => {
                        if let Some(m) = confirmations.get(seg) {
                            s.confirmed_at_ms = s.confirmed_at_ms.max(m.confirmed_at_ms);
                        }
                        scan.snapshots.push((table.clone(), s));
                    }
                    Entry::Expired => scan.expired += 1,
                    Entry::Absent | Entry::Rejected => {}
                }
            }
        }
        let latest_dir = self.root.join("latest");
        for (name, _) in list_dir(&latest_dir)? {
            if !name.ends_with(".json") {
                continue;
            }
            let path = latest_dir.join(&name);
            if let Entry::Valid(f) = self.read_latest_file(&path)? {
                // The file must sit at its own key's path.
                if self.latest_path(&f.table_key, &f.tail_id).ok().as_ref() != Some(&path) {
                    self.reject::<()>(
                        &path,
                        format!("file holds {} tail {:?}", f.table_key, f.tail_id),
                    );
                    continue;
                }
                scan.latest.push((
                    f.table_key,
                    LatestRecord {
                        seg_id: f.seg_id,
                        version: f.version,
                        confirmed_at_ms: f.confirmed_at_ms,
                        tail_id: f.tail_id,
                    },
                ));
            }
        }
        for (seg, is_dir) in list_dir(&self.root.join("plan"))? {
            if !is_dir || part(&seg).is_err() {
                continue;
            }
            for (name, _) in list_dir(&self.root.join("plan").join(&seg))? {
                if let Some(spec) = name.strip_suffix(".bin").filter(|s| part(s).is_ok()) {
                    scan.plans.push((seg.clone(), spec.to_string()));
                }
            }
        }
        Ok(scan)
    }
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> StoreResult<T> + Send + 'static,
) -> StoreResult<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| format!("file L2 task failed: {e}"))?
}

impl FileStore {
    /// Opens (creating) `<dir>/<kernel_rev>/`. `ttl`: expiry of `snap` and `plan` entries
    /// (`None`: never).
    pub fn open(dir: &Path, kernel_rev: &str, ttl: Option<Duration>) -> StoreResult<Self> {
        let root = dir.join(part(kernel_rev)?);
        fs::create_dir_all(&root)
            .map_err(|e| format!("file L2: create {}: {e}", root.display()))?;
        Ok(Self::with_root(root, kernel_rev, ttl, false))
    }

    /// `--l2-read-only` (SPEC v12.1): reads `<dir>/<kernel_rev>/` and never writes under `dir`,
    /// not even to create it (a missing directory is an empty store).
    pub fn open_read_only(
        dir: &Path,
        kernel_rev: &str,
        ttl: Option<Duration>,
    ) -> StoreResult<Self> {
        let root = dir.join(part(kernel_rev)?);
        if !root.is_dir() {
            tracing::warn!(dir = %root.display(), "read-only L2 directory does not exist; no entries");
        }
        Ok(Self::with_root(root, kernel_rev, ttl, true))
    }

    fn with_root(root: PathBuf, kernel_rev: &str, ttl: Option<Duration>, read_only: bool) -> Self {
        Self {
            inner: Arc::new(Inner {
                root,
                kernel_rev: kernel_rev.to_string(),
                ttl,
                rmw: Mutex::new(()),
                rejected: AtomicU64::new(0),
                read_only,
                skipped_writes: AtomicU64::new(0),
                writes: AtomicU64::new(0),
                write_delay_ms: AtomicU64::new(0),
                reads: AtomicU64::new(0),
                read_delay_ms: AtomicU64::new(0),
            }),
            locks: Mutex::default(),
            authz: Mutex::default(),
        }
    }

    /// Writes skipped because the store is read-only.
    pub fn skipped_writes(&self) -> u64 {
        self.inner.skipped_writes.load(Ordering::Relaxed)
    }

    /// Entry files written so far by this store.
    pub fn writes(&self) -> u64 {
        self.inner.writes.load(Ordering::Relaxed)
    }

    /// Entry files read so far by this store.
    pub fn reads(&self) -> u64 {
        self.inner.reads.load(Ordering::Relaxed)
    }

    /// Test hook: sleep this long before every entry-file read (`Duration::ZERO` disables).
    pub fn set_read_delay(&self, delay: Duration) {
        self.inner
            .read_delay_ms
            .store(delay.as_millis() as u64, Ordering::Relaxed);
    }

    /// Test hook: sleep this long before every file write (`Duration::ZERO` disables).
    pub fn set_write_delay(&self, delay: Duration) {
        self.inner
            .write_delay_ms
            .store(delay.as_millis() as u64, Ordering::Relaxed);
    }

    /// `true` (and counted) if a write must be skipped.
    fn skip_write(&self) -> bool {
        if self.inner.read_only {
            self.inner.skipped_writes.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.read_only
    }

    /// `<dir>/<kernel_rev>`.
    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    /// Entries read and rejected so far (corrupt, other kernel revision, wrong key).
    pub fn rejected(&self) -> u64 {
        self.inner.rejected.load(Ordering::Relaxed)
    }

    /// Every valid entry under `<dir>/<kernel_rev>/` (`--l2-preload`).
    pub async fn scan(&self) -> StoreResult<L2Scan> {
        let inner = self.inner.clone();
        blocking(move || inner.scan()).await
    }
}

#[async_trait]
impl SnapshotStore for FileStore {
    type Payload = Vec<u8>;

    fn tier(&self) -> &'static str {
        "l2-file"
    }

    fn read_only(&self) -> bool {
        self.inner.read_only
    }

    async fn get_snapshot(
        &self,
        table: &str,
        seg_id: &str,
    ) -> StoreResult<Option<StoredSnapshot<Vec<u8>>>> {
        let (inner, table, seg) = (self.inner.clone(), table.to_string(), seg_id.to_string());
        blocking(move || inner.get_snapshot(&table, &seg)).await
    }

    async fn snapshot_at_version(
        &self,
        table: &str,
        version: i64,
    ) -> StoreResult<Option<StoredSnapshot<Vec<u8>>>> {
        let (inner, table) = (self.inner.clone(), table.to_string());
        blocking(move || inner.snapshot_at_version(&table, version)).await
    }

    async fn put_snapshot(&self, table: &str, snap: &StoredSnapshot<Vec<u8>>) -> StoreResult<()> {
        if self.skip_write() {
            return Ok(());
        }
        let (inner, table, snap) = (self.inner.clone(), table.to_string(), snap.clone());
        blocking(move || inner.put_snapshot(&table, &snap)).await
    }

    async fn confirm_snapshot(&self, table: &str, seg_id: &str, at_ms: i64) -> StoreResult<()> {
        if self.skip_write() {
            return Ok(());
        }
        let (inner, table, seg) = (self.inner.clone(), table.to_string(), seg_id.to_string());
        blocking(move || inner.confirm_snapshot(&table, &seg, at_ms)).await
    }

    async fn get_latest(&self, table: &str, tail_id: &str) -> StoreResult<Option<LatestRecord>> {
        let (inner, table, tail) = (self.inner.clone(), table.to_string(), tail_id.to_string());
        blocking(move || {
            Ok(match inner.read_latest(&table, &tail)? {
                Entry::Valid(r) => Some(r),
                _ => None,
            })
        })
        .await
    }

    /// Monotonic ([`latest_replaces`]); the pointer never expires (module docs).
    async fn put_latest(
        &self,
        table: &str,
        rec: &LatestRecord,
        _staleness: Duration,
    ) -> StoreResult<()> {
        if self.skip_write() {
            return Ok(());
        }
        let (inner, table, rec) = (self.inner.clone(), table.to_string(), rec.clone());
        blocking(move || inner.put_latest(&table, &rec)).await
    }

    async fn get_plan(
        &self,
        seg_id: &str,
        spec_hash: &str,
    ) -> StoreResult<Option<proto::ScanPlanResult>> {
        let (inner, seg, spec) = (
            self.inner.clone(),
            seg_id.to_string(),
            spec_hash.to_string(),
        );
        blocking(move || inner.get_plan(&seg, &spec)).await
    }

    async fn put_plan(
        &self,
        seg_id: &str,
        spec_hash: &str,
        plan: &proto::ScanPlanResult,
    ) -> StoreResult<PlanPut> {
        if self.skip_write() {
            return Ok(PlanPut::ReadOnly);
        }
        let (inner, seg, spec, plan) = (
            self.inner.clone(),
            seg_id.to_string(),
            spec_hash.to_string(),
            plan.clone(),
        );
        blocking(move || inner.put_plan(&seg, &spec, &plan)).await
    }

    /// Process-local: the file store has no cross-process single flight.
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

    /// Process-local: every process checks authz with UC itself.
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
        if until_ms > now_ms() {
            self.authz
                .lock()
                .unwrap()
                .insert((token_hash.to_string(), table.to_string()), until_ms);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_round_trips_and_detects_corruption() {
        let fields: Vec<(&str, &[u8])> = vec![("a", b"1"), ("empty", b""), ("big", &[7u8; 300])];
        let bytes = encode_envelope(KIND_SNAP, &fields);
        let f = decode_envelope(KIND_SNAP, &bytes).unwrap();
        assert_eq!(f.len(), 3);
        assert_eq!(f["big"], vec![7u8; 300]);
        assert!(
            decode_envelope(KIND_PLAN, &bytes)
                .unwrap_err()
                .contains("kind")
        );
        for cut in [0, 10, bytes.len() / 2, bytes.len() - 1] {
            assert!(
                decode_envelope(KIND_SNAP, &bytes[..cut]).is_err(),
                "cut {cut}"
            );
        }
        let mut flipped = bytes.clone();
        flipped[MAGIC.len() + 5] ^= 1;
        assert_eq!(
            decode_envelope(KIND_SNAP, &flipped).unwrap_err(),
            "checksum mismatch"
        );
    }

    #[test]
    fn key_parts_are_safe_file_names() {
        for ok in [
            "0123abcd",
            "a-b",
            "a_b",
            "1b4e28ba-2fa1-11d2-883f-0016d3cca427",
        ] {
            assert!(part(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "..",
            "a/b",
            "a__b",
            "_a",
            "a_",
            ".hidden",
            "a b",
            &"x".repeat(129),
        ] {
            assert!(part(bad).is_err(), "{bad}");
        }
    }
}
