//! Server-side execution of simple IO ops (SPEC v9 hybrid IO).
//!
//! `FileListing`, `ReadBytes`, `HeadFile` and `ParquetFooter` run here through the default
//! engine's object_store storage / parquet handlers. `QueryPlan` ops are still forwarded to the
//! client (see [`crate::executor::RemotePlanExecutor`]). All engines share one multi-thread tokio
//! runtime instead of each owning a background executor thread.

use std::sync::{Arc, LazyLock};

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
