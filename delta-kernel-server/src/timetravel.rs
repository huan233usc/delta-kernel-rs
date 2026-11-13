//! `ResolveTimeTravel` (SPEC v10.2): the client's JNR history queries, run on a server snapshot.
//!
//! - `timestamp_ms` mirrors `JnrHistory.getActiveCommitAtTimestamp`: a timestamp after the
//!   snapshot's own timestamp returns the snapshot version if `can_return_last_commit`, else fails;
//!   otherwise kernel `latest_version_as_of`; if that fails with a log-history error and
//!   `can_return_earliest_commit`, kernel `first_version_after`, accepted only when its timestamp
//!   is strictly after the requested one (the timestamp precedes every commit).
//! - `check_version` mirrors `PathBasedSnapshotManager` / `UCManagedTableSnapshotManager`
//!   `checkVersionExists`: the earliest version (`get_earliest_commit`, recreatable or published
//!   per `must_be_recreatable`, bounded by the earliest catalog commit for catalog-managed tables)
//!   and the latest version. The client applies `version < earliest || (version > latest &&
//!   !allowOutOfRange)` and raises `VersionNotFoundException(version, earliest, latest)`; the
//!   bounds travel in the response so it can.
//!
//! Commit types follow `must_be_recreatable` (`Recreatable`) or not (`Published`), as in JNR.

use delta_kernel::history_manager::{
    HistoryCommitType, first_version_after, get_earliest_commit, latest_version_as_of,
};
use delta_kernel::{Engine, KernelError, Snapshot};

use crate::errors::{ServerError, root_kernel_error};
use crate::proto::{self, resolve_time_travel_request};

/// Prefix of the error message of a history query that has no answer (timestamp out of range,
/// no recreatable commit, ...). Code `UNSUPPORTED`: the client falls back to its JNR path, which
/// raises the same user-facing exception.
pub const LOG_HISTORY_PREFIX: &str = "LOG_HISTORY: ";

#[derive(Clone, Debug)]
pub enum TimeTravelQuery {
    Timestamp {
        timestamp_ms: i64,
        can_return_last_commit: bool,
        must_be_recreatable: bool,
        can_return_earliest_commit: bool,
    },
    CheckVersion {
        version: i64,
        must_be_recreatable: bool,
    },
}

impl TimeTravelQuery {
    pub fn from_request(req: &proto::ResolveTimeTravelRequest) -> Result<Self, ServerError> {
        match req.target {
            Some(resolve_time_travel_request::Target::TimestampMs(timestamp_ms)) => {
                Ok(Self::Timestamp {
                    timestamp_ms,
                    can_return_last_commit: req.can_return_last_commit,
                    must_be_recreatable: req.must_be_recreatable,
                    can_return_earliest_commit: req.can_return_earliest_commit,
                })
            }
            Some(resolve_time_travel_request::Target::CheckVersion(version)) => {
                Ok(Self::CheckVersion {
                    version,
                    must_be_recreatable: req.must_be_recreatable,
                })
            }
            None => Err(ServerError::internal(
                "ResolveTimeTravelRequest.target is required",
            )),
        }
    }
}

/// A resolved query. `-1` marks a field the query does not compute: `earliest_version` for
/// timestamp queries, `commit_timestamp_ms` for version checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resolved {
    pub version: i64,
    pub commit_timestamp_ms: i64,
    pub earliest_version: i64,
}

pub enum TimeTravelError {
    /// A kernel error (op failures, history errors); classified by the caller.
    Kernel(KernelError),
    /// A failure JNR raises itself (timestamp after the latest commit).
    Api(ServerError),
}

impl From<KernelError> for TimeTravelError {
    fn from(e: KernelError) -> Self {
        Self::Kernel(e)
    }
}

fn commit_type(must_be_recreatable: bool) -> HistoryCommitType {
    if must_be_recreatable {
        HistoryCommitType::Recreatable
    } else {
        HistoryCommitType::Published
    }
}

fn is_log_history(e: &KernelError) -> bool {
    matches!(root_kernel_error(e), KernelError::LogHistory(_))
}

/// The wire error of a kernel history error, if `e` is one.
pub fn history_error(e: &KernelError) -> Option<ServerError> {
    is_log_history(e).then(|| {
        ServerError::new(
            proto::error::Code::Unsupported,
            format!("{LOG_HISTORY_PREFIX}{e}"),
        )
    })
}

/// Runs `query` on `snapshot`, the latest (or `catalog_latest`) snapshot of the table.
/// `earliest_ratified` is the earliest catalog commit version of a catalog-managed table.
pub fn resolve(
    snapshot: &Snapshot,
    engine: &dyn Engine,
    query: &TimeTravelQuery,
    earliest_ratified: Option<u64>,
) -> Result<Resolved, TimeTravelError> {
    match *query {
        TimeTravelQuery::Timestamp {
            timestamp_ms,
            can_return_last_commit,
            must_be_recreatable,
            can_return_earliest_commit,
        } => {
            let latest_ts = snapshot.reader().get_timestamp(engine)?;
            let latest = snapshot.version() as i64;
            if latest_ts < timestamp_ms {
                if can_return_last_commit {
                    return Ok(Resolved {
                        version: latest,
                        commit_timestamp_ms: latest_ts,
                        earliest_version: -1,
                    });
                }
                return Err(TimeTravelError::Api(ServerError::new(
                    proto::error::Code::Unsupported,
                    format!(
                        "{LOG_HISTORY_PREFIX}The provided timestamp ({timestamp_ms} ms) is after \
                         the latest available version {latest} (timestamp {latest_ts} ms)."
                    ),
                )));
            }
            let ty = commit_type(must_be_recreatable);
            let commit = match latest_version_as_of(snapshot, engine, timestamp_ms, ty) {
                Ok(c) => c,
                Err(e) if is_log_history(&e) && can_return_earliest_commit => {
                    match first_version_after(snapshot, engine, timestamp_ms, ty) {
                        Ok(c) if c.timestamp > timestamp_ms => c,
                        _ => return Err(e.into()),
                    }
                }
                Err(e) => return Err(e.into()),
            };
            Ok(Resolved {
                version: commit.version as i64,
                commit_timestamp_ms: commit.timestamp,
                earliest_version: -1,
            })
        }
        TimeTravelQuery::CheckVersion {
            version,
            must_be_recreatable,
        } => {
            let log_root = snapshot.log_segment().log_root.clone();
            let earliest = get_earliest_commit(
                engine,
                &log_root,
                earliest_ratified,
                commit_type(must_be_recreatable),
            )?;
            Ok(Resolved {
                version,
                commit_timestamp_ms: -1,
                earliest_version: earliest as i64,
            })
        }
    }
}
