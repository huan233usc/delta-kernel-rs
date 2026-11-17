//! Server-side execution of simple IO ops (SPEC v9 hybrid IO).
//!
//! `FileListing`, `ReadBytes`, `HeadFile` and `ParquetFooter` run here through the default
//! engine's object_store storage / parquet handlers. `QueryPlan` ops are still forwarded to the
//! client (see [`crate::executor::RemotePlanExecutor`]). All engines share one multi-thread tokio
//! runtime instead of each owning a background executor thread.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, sync_channel};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use delta_kernel::object_store::DynObjectStore;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::{
    Engine as _, FileMeta, FileSlice, KernelResult, ParquetFooter, ParquetHandler, StorageHandler,
};
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use url::Url;

use crate::proto;

/// One multi-thread runtime shared by every local-IO engine in the process.
static IO_EXECUTOR: LazyLock<Arc<TokioMultiThreadExecutor>> = LazyLock::new(|| {
    Arc::new(
        TokioMultiThreadExecutor::new_owned_runtime(None, None)
            .expect("create the shared local-IO tokio runtime"),
    )
});

/// Store for `file://` tables, shared by all of them.
static LOCAL_FS: LazyLock<Arc<DynObjectStore>> = LazyLock::new(|| Arc::new(LocalFileSystem::new()));

/// Storage and parquet handlers over one object store.
pub struct LocalIo {
    storage: Arc<dyn StorageHandler>,
    parquet: Arc<dyn ParquetHandler>,
}

impl LocalIo {
    pub fn new(store: Arc<DynObjectStore>) -> Self {
        let engine = DefaultEngineBuilder::new(store)
            .with_task_executor(IO_EXECUTOR.clone())
            .build();
        Self {
            storage: engine.storage_handler(),
            parquet: engine.parquet_handler(),
        }
    }

    /// Local IO for `file://` tables (no credentials).
    pub fn local_fs() -> Self {
        Self::new(LOCAL_FS.clone())
    }

    /// Lists from `url`; per-file errors are kept as items. `Err` only for whole-op failures.
    pub fn list(&self, url: &Url) -> KernelResult<Vec<KernelResult<FileMeta>>> {
        Ok(self.storage.list_from(url)?.collect())
    }

    /// Reads every slice; one item per slice, in order, errors kept per item.
    pub fn read(&self, files: Vec<FileSlice>) -> KernelResult<Vec<KernelResult<bytes::Bytes>>> {
        let n = files.len();
        let mut items: Vec<KernelResult<bytes::Bytes>> = self.storage.read_files(files)?.collect();
        // A stream that stopped early must not silently shorten the result.
        while items.len() < n {
            items.push(Err(delta_kernel::KernelError::generic(
                "read_files returned fewer items than requested slices",
            )));
        }
        Ok(items)
    }

    pub fn head(&self, url: &Url) -> KernelResult<FileMeta> {
        self.storage.head(url)
    }

    pub fn footer(&self, file: &FileMeta) -> KernelResult<ParquetFooter> {
        self.parquet.read_parquet_footer(file)
    }
}

/// Outcome of [`LocalIoPool::run`].
pub enum PoolRun<T> {
    Done(T),
    /// The op did not finish within the deadline. It keeps its worker and permit until it
    /// actually returns; its result is then dropped.
    TimedOut,
    /// Every worker is busy (possibly with abandoned ops): rejected without queueing.
    Saturated,
    /// The op panicked.
    Panicked,
}

type Task = Box<dyn FnOnce() + Send>;

/// A fixed-size pool for blocking server-side IO (`--local-io-threads`).
///
/// The default-engine handlers are synchronous (they `block_on` their object_store futures), so a
/// timed-out op cannot be dropped from here; it can only be abandoned. To keep abandoned ops from
/// piling up, each op holds one of `size` permits from submission until it really returns, and
/// there are never more than `size` worker threads. With every permit taken, new ops fail fast
/// instead of queueing or spawning. Abandoned ops only drop their result: nothing they produce is
/// written to shared state.
pub struct LocalIoPool {
    size: usize,
    tx: Mutex<mpsc::Sender<Task>>,
    rx: Arc<Mutex<mpsc::Receiver<Task>>>,
    in_flight: Arc<AtomicUsize>,
    workers: AtomicUsize,
}

/// Releases one pool permit when the op it guards ends (also on panic).
struct Permit(Arc<AtomicUsize>);

impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Name of pool worker threads (visible in `/proc/self/task/*/comm`).
pub const WORKER_THREAD_NAME: &str = "dks-local-io";

impl LocalIoPool {
    pub fn new(size: usize) -> Arc<Self> {
        let (tx, rx) = mpsc::channel::<Task>();
        Arc::new(Self {
            size: size.max(1),
            tx: Mutex::new(tx),
            rx: Arc::new(Mutex::new(rx)),
            in_flight: Arc::new(AtomicUsize::new(0)),
            workers: AtomicUsize::new(0),
        })
    }

    pub fn size(&self) -> usize {
        self.size
    }

    /// Ops currently holding a permit (running, including abandoned ones).
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    /// Worker threads started so far (never more than `size`).
    pub fn workers(&self) -> usize {
        self.workers.load(Ordering::Acquire)
    }

    fn try_acquire(&self) -> Option<Permit> {
        let mut cur = self.in_flight.load(Ordering::Acquire);
        loop {
            if cur >= self.size {
                return None;
            }
            match self.in_flight.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(Permit(self.in_flight.clone())),
                Err(actual) => cur = actual,
            }
        }
    }

    /// Starts workers lazily so that there is one per permit in use, up to `size`.
    fn ensure_workers(&self) -> std::io::Result<()> {
        loop {
            let have = self.workers.load(Ordering::Acquire);
            if have >= self.size || have >= self.in_flight() {
                return Ok(());
            }
            if self
                .workers
                .compare_exchange(have, have + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let rx = self.rx.clone();
            let spawned = std::thread::Builder::new()
                .name(WORKER_THREAD_NAME.into())
                .spawn(move || {
                    loop {
                        let task = match rx.lock().unwrap().recv() {
                            Ok(task) => task,
                            Err(_) => return, // pool dropped
                        };
                        // A panicking op must not take the worker down; its permit is released
                        // by unwinding.
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(task));
                    }
                });
            if let Err(e) = spawned {
                self.workers.fetch_sub(1, Ordering::AcqRel);
                return Err(e);
            }
        }
    }

    /// Runs `f` on a pool worker and waits at most `timeout` for it.
    pub fn run<T: Send + 'static>(
        &self,
        timeout: Duration,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> PoolRun<T> {
        let Some(permit) = self.try_acquire() else {
            return PoolRun::Saturated;
        };
        if self.ensure_workers().is_err() && self.workers() == 0 {
            return PoolRun::Saturated;
        }
        let (done_tx, done_rx) = sync_channel::<T>(1);
        let task: Task = Box::new(move || {
            let out = f();
            drop(permit);
            // The waiter may be gone (timed out): then the result is simply dropped.
            let _ = done_tx.send(out);
        });
        if self.tx.lock().unwrap().send(task).is_err() {
            return PoolRun::Saturated;
        }
        match done_rx.recv_timeout(timeout) {
            Ok(out) => PoolRun::Done(out),
            Err(RecvTimeoutError::Timeout) => PoolRun::TimedOut,
            // The op panicked (sender dropped without a result).
            Err(RecvTimeoutError::Disconnected) => PoolRun::Panicked,
        }
    }
}

/// Delta log version of `location` if it is a versioned file (`<20 digits>.<...>`) directly in
/// `dir` (a directory URL path ending in `/`).
fn versioned_in(location: &Url, dir: &str) -> Option<i64> {
    let rest = location.path().strip_prefix(dir)?;
    if rest.contains('/') {
        return None;
    }
    let (digits, tail) = (rest.get(..20)?, rest.get(20..)?);
    (digits.bytes().all(|b| b.is_ascii_digit()) && tail.starts_with('.'))
        .then(|| digits.parse().ok())
        .flatten()
}

/// Applies a `ListingHint` (SPEC §5) to a sorted listing of `url`'s directory: stop at the first
/// entry that is not a versioned file directly in that directory (when asked), and stop once
/// versions pass `end_version`. Per-item errors are kept.
pub fn apply_listing_hint(
    url: &Url,
    items: Vec<KernelResult<FileMeta>>,
    hint: &proto::ListingHint,
) -> Vec<KernelResult<FileMeta>> {
    let path = url.path();
    let dir = match path.rfind('/') {
        Some(i) => &path[..=i],
        None => return items,
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        if let Ok(f) = &item {
            match versioned_in(&f.location, dir) {
                None if hint.stop_at_first_non_versioned => break,
                Some(v) if hint.end_version.is_some_and(|end| v > end) => break,
                _ => {}
            }
        }
        out.push(item);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fm(u: &str) -> KernelResult<FileMeta> {
        Ok(FileMeta {
            location: Url::parse(u).unwrap(),
            last_modified: 0,
            size: 1,
        })
    }

    #[test]
    fn listing_hint_truncates_by_version_and_non_versioned() {
        let url = Url::parse("file:///t/_delta_log/00000000000000000001").unwrap();
        let items = || {
            vec![
                fm("file:///t/_delta_log/00000000000000000001.json"),
                fm("file:///t/_delta_log/00000000000000000002.checkpoint.parquet"),
                fm("file:///t/_delta_log/00000000000000000002.json"),
                fm("file:///t/_delta_log/00000000000000000003.json"),
                fm("file:///t/_delta_log/_last_checkpoint"),
                fm("file:///t/_delta_log/_staged_commits/00000000000000000009.u.json"),
            ]
        };
        let names = |v: Vec<KernelResult<FileMeta>>| -> Vec<String> {
            v.into_iter()
                .map(|r| {
                    r.unwrap()
                        .location
                        .path()
                        .rsplit('/')
                        .next()
                        .unwrap()
                        .to_string()
                })
                .collect()
        };
        let both = proto::ListingHint {
            end_version: Some(2),
            stop_at_first_non_versioned: true,
        };
        assert_eq!(
            names(apply_listing_hint(&url, items(), &both)),
            [
                "00000000000000000001.json",
                "00000000000000000002.checkpoint.parquet",
                "00000000000000000002.json"
            ]
        );
        let stop_only = proto::ListingHint {
            end_version: None,
            stop_at_first_non_versioned: true,
        };
        assert_eq!(
            names(apply_listing_hint(&url, items(), &stop_only)).len(),
            4
        );
        // A versioned name in a subdirectory is not versioned for this listing.
        let sub_first = vec![
            fm("file:///t/_delta_log/00000000000000000001.json"),
            fm("file:///t/_delta_log/_staged_commits/00000000000000000002.u.json"),
        ];
        assert_eq!(
            names(apply_listing_hint(&url, sub_first, &stop_only)).len(),
            1
        );
        let none = proto::ListingHint::default();
        assert_eq!(apply_listing_hint(&url, items(), &none).len(), 6);
    }

    #[test]
    fn pool_bounds_threads_and_fails_fast_when_saturated() {
        let pool = LocalIoPool::new(2);
        let gate = Arc::new(std::sync::Barrier::new(3));
        // Two ops block until released; both time out and stay in flight.
        for _ in 0..2 {
            let g = gate.clone();
            assert!(matches!(
                pool.run(Duration::from_millis(20), move || {
                    g.wait();
                }),
                PoolRun::TimedOut
            ));
        }
        assert_eq!((pool.in_flight(), pool.workers()), (2, 2));
        let t = std::time::Instant::now();
        assert!(matches!(
            pool.run(Duration::from_secs(5), || 1),
            PoolRun::Saturated
        ));
        assert!(
            t.elapsed() < Duration::from_millis(100),
            "saturation fails fast"
        );
        assert_eq!(pool.workers(), 2, "no thread spawned on saturation");
        gate.wait(); // release the abandoned ops
        for _ in 0..200 {
            if pool.in_flight() == 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(pool.in_flight(), 0, "abandoned ops returned their permits");
        assert!(matches!(
            pool.run(Duration::from_secs(5), || 7),
            PoolRun::Done(7)
        ));
        // A panicking op frees its permit and keeps its worker.
        assert!(matches!(
            pool.run(Duration::from_secs(5), || -> i32 { panic!("boom") }),
            PoolRun::Panicked
        ));
        for _ in 0..200 {
            if pool.in_flight() == 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(matches!(
            pool.run(Duration::from_secs(5), || 8),
            PoolRun::Done(8)
        ));
        assert_eq!(pool.workers(), 2);
    }

    #[test]
    fn listing_hint_keeps_per_item_errors() {
        let url = Url::parse("file:///t/_delta_log/00000000000000000000").unwrap();
        let items = vec![
            fm("file:///t/_delta_log/00000000000000000000.json"),
            Err(delta_kernel::KernelError::generic("boom")),
            fm("file:///t/_delta_log/00000000000000000001.json"),
        ];
        let hint = proto::ListingHint {
            end_version: Some(5),
            stop_at_first_non_versioned: true,
        };
        let out = apply_listing_hint(&url, items, &hint);
        assert_eq!(out.len(), 3);
        assert!(out[1].is_err());
    }
}
