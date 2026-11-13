//! Serialized kernel [`SnapshotHint`] for the L2 store (SPEC v10.3).
//!
//! Kernel at `4633926` exposes `SnapshotHint` only under `internal-api` and without serde, but
//! every part of it is reachable and rebuildable through public (internal-api) accessors:
//!
//! - version, freshness and publication watermark;
//! - the retained log files (`LogSegmentFiles`), re-grouped by `SnapshotHint::try_new`;
//! - `Protocol`, `Metadata`, `LastCheckpointHint` (serde);
//! - the at-version CRC, rebuilt with `Crc::try_from_parts` from its accessors. Its inline
//!   `allFiles` adds have crate-private fields and no `Serialize`; they are exported through the
//!   public `From<Add> for Scalar` conversion (field names = Delta protocol names) and read back
//!   with `Add`'s own `Deserialize`.
//!
//! [`encode`] never trusts the format: it decodes its own output and compares every component with
//! the original (`PartialEq` on `LogSegmentFiles`, `Protocol`, `Metadata`, `Crc`, ...). A hint that
//! does not round-trip exactly is not stored, and L2 then holds `SnapshotInfo` and plans only.
//!
//! Rebuilding from a hint ([`rebuild`]) goes through `SnapshotBuilder::with_snapshot_hint`, which
//! performs no engine IO; it runs with an engine whose every op fails, so a kernel change that
//! starts reading storage there surfaces as an L2 miss instead of silent IO.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::actions::{
    Add, DomainMetadata, LastManifestCommit, Metadata, Protocol, SetTransaction,
};
use delta_kernel::crc::{
    Crc, DeletedRecordCountsHistogram, DomainMetadataState, FileSizeHistogram, FileStats,
    FileStatsState, SetTransactionState,
};
use delta_kernel::engine::plans::PlanBasedEngine;
use delta_kernel::expressions::Scalar;
use delta_kernel::last_checkpoint_hint::LastCheckpointHint;
use delta_kernel::snapshot::{PublicationWatermark, SnapshotHint, SnapshotHintFreshness};
use delta_kernel::{FileMeta, LogPath, PlanExecutor, PlanResult, Snapshot, SnapshotRef};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Bumped on any change to the encoding; a different format is treated as absent.
const FORMAT: u32 = 1;

#[derive(Serialize, Deserialize)]
struct HintWire {
    format: u32,
    version: u64,
    latest: bool,
    published_through: Option<u64>,
    paths: Vec<PathWire>,
    protocol: Protocol,
    metadata: Metadata,
    last_checkpoint_hint: Option<LastCheckpointHint>,
    crc: Option<CrcWire>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
struct PathWire {
    location: String,
    last_modified: i64,
    size: u64,
}

#[derive(Serialize, Deserialize)]
enum FileStatsWire {
    Complete {
        num_files: i64,
        table_size_bytes: i64,
        histogram: Option<FileSizeHistogram>,
    },
    Indeterminate,
}

#[derive(Serialize, Deserialize)]
struct CrcWire {
    version: u64,
    metadata: Metadata,
    protocol: Protocol,
    file_stats: FileStatsWire,
    in_commit_timestamp: Option<i64>,
    set_transactions_complete: bool,
    set_transactions: Vec<(String, SetTransaction)>,
    domain_metadata_complete: bool,
    domain_metadata: Vec<(String, DomainMetadata)>,
    txn_id: Option<String>,
    all_files: Option<Vec<Value>>,
    num_deleted_records: Option<i64>,
    num_deletion_vectors: Option<i64>,
    deleted_record_counts: Option<Vec<i64>>,
    last_manifest_commit: Option<LastManifestCommit>,
}

/// Encodes `snapshot`'s hint, or explains why it cannot be stored losslessly.
pub fn encode(snapshot: &Snapshot, table_root: &str) -> Result<Vec<u8>, String> {
    let hint = snapshot
        .to_snapshot_hint()
        .map_err(|e| format!("to_snapshot_hint: {e}"))?;
    let bytes = encode_hint(&hint)?;
    let decoded = decode(&bytes, table_root)?;
    let mismatch = first_mismatch(&hint, &decoded);
    match mismatch {
        None => Ok(bytes),
        Some(what) => Err(format!("hint does not round-trip: {what} differs")),
    }
}

fn encode_hint(hint: &SnapshotHint) -> Result<Vec<u8>, String> {
    let files = hint.log_segment_files();
    if !files.ascending_compaction_files.is_empty() {
        return Err("log compaction files are not supported by snapshot hints".into());
    }
    let mut seen = std::collections::HashSet::new();
    let paths: Vec<PathWire> = files
        .ascending_commit_files
        .iter()
        .chain(&files.checkpoint_parts)
        .chain(&files.latest_crc_file)
        .chain(&files.latest_commit_file)
        .map(|p| PathWire {
            location: p.location.location.to_string(),
            last_modified: p.location.last_modified,
            size: p.location.size,
        })
        .filter(|p| seen.insert(p.location.clone()))
        .collect();
    let wire = HintWire {
        format: FORMAT,
        version: hint.version(),
        latest: hint.freshness() == SnapshotHintFreshness::Latest,
        published_through: match hint.publication_watermark() {
            PublicationWatermark::PublishedThrough(v) => Some(v),
            _ => None,
        },
        paths,
        protocol: hint.protocol().clone(),
        metadata: hint.metadata().clone(),
        last_checkpoint_hint: hint.last_checkpoint_hint().cloned(),
        crc: hint.crc().map(crc_wire).transpose()?,
    };
    let json = serde_json::to_vec(&wire).map_err(|e| format!("encode hint: {e}"))?;
    zstd::encode_all(json.as_slice(), 3).map_err(|e| format!("compress hint: {e}"))
}

fn crc_wire(crc: &Crc) -> Result<CrcWire, String> {
    let (set_transactions_complete, set_transactions) = match &crc.set_transaction_state {
        SetTransactionState::Complete(m) => (true, sorted(m)),
        SetTransactionState::Partial(m) => (false, sorted(m)),
    };
    let (domain_metadata_complete, domain_metadata) = match &crc.domain_metadata_state {
        DomainMetadataState::Complete(m) => (true, sorted(m)),
        DomainMetadataState::Partial(m) => (false, sorted(m)),
    };
    let file_stats = match crc.file_stats_state() {
        FileStatsState::Complete(s) => FileStatsWire::Complete {
            num_files: s.num_files(),
            table_size_bytes: s.table_size_bytes(),
            histogram: s.file_size_histogram().cloned(),
        },
        FileStatsState::Indeterminate => FileStatsWire::Indeterminate,
    };
    let all_files = crc
        .all_files()
        .map(|adds| {
            adds.iter()
                .map(|a| scalar_to_json(&Scalar::from(a.clone())))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    Ok(CrcWire {
        version: crc.version,
        metadata: crc.metadata.clone(),
        protocol: crc.protocol.clone(),
        file_stats,
        in_commit_timestamp: crc.in_commit_timestamp_opt,
        set_transactions_complete,
        set_transactions,
        domain_metadata_complete,
        domain_metadata,
        txn_id: crc.txn_id().map(str::to_string),
        all_files,
        num_deleted_records: crc.num_deleted_records(),
        num_deletion_vectors: crc.num_deletion_vectors(),
        deleted_record_counts: crc
            .deleted_record_counts_histogram()
            .map(|h| h.deleted_record_counts().to_vec()),
        last_manifest_commit: crc.last_manifest_commit().cloned(),
    })
}

fn sorted<V: Clone>(m: &HashMap<String, V>) -> Vec<(String, V)> {
    let mut v: Vec<(String, V)> = m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    v.sort_by(|a, b| a.0.cmp(&b.0));
    v
}

/// JSON form of a kernel scalar produced by `From<Add> for Scalar`: structs and maps become
/// objects keyed by field name / map key, so `Add`'s camelCase `Deserialize` reads it back.
fn scalar_to_json(s: &Scalar) -> Result<Value, String> {
    Ok(match s {
        Scalar::Null(_) => Value::Null,
        Scalar::Boolean(b) => Value::Bool(*b),
        Scalar::Integer(i) => Value::from(*i),
        Scalar::Long(i) => Value::from(*i),
        Scalar::Short(i) => Value::from(*i),
        Scalar::Byte(i) => Value::from(*i),
        Scalar::String(s) => Value::String(s.clone()),
        Scalar::Struct(data) => Value::Object(
            data.fields()
                .iter()
                .zip(data.values())
                .map(|(f, v)| Ok((f.name().to_string(), scalar_to_json(v)?)))
                .collect::<Result<_, String>>()?,
        ),
        Scalar::Map(data) => Value::Object(
            data.pairs()
                .iter()
                .map(|(k, v)| match k {
                    Scalar::String(k) => Ok((k.clone(), scalar_to_json(v)?)),
                    other => Err(format!("non-string map key {other:?}")),
                })
                .collect::<Result<_, String>>()?,
        ),
        Scalar::Array(data) => Value::Array(
            data.array_elements()
                .iter()
                .map(scalar_to_json)
                .collect::<Result<_, _>>()?,
        ),
        other => return Err(format!("unsupported scalar in add action: {other:?}")),
    })
}

/// Decodes a hint for the table at `table_root`.
pub fn decode(bytes: &[u8], table_root: &str) -> Result<SnapshotHint, String> {
    let json = zstd::decode_all(bytes).map_err(|e| format!("decompress hint: {e}"))?;
    let wire: HintWire = serde_json::from_slice(&json).map_err(|e| format!("decode hint: {e}"))?;
    if wire.format != FORMAT {
        return Err(format!("hint format {} (expected {FORMAT})", wire.format));
    }
    let log_paths = wire
        .paths
        .into_iter()
        .map(|p| {
            let location = url::Url::parse(&p.location).map_err(|e| e.to_string())?;
            LogPath::try_new(FileMeta::new(location, p.last_modified, p.size))
                .map_err(|e| e.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let crc = wire.crc.map(crc_from_wire).transpose()?.map(Arc::new);
    SnapshotHint::try_new(
        table_root,
        wire.version,
        match wire.published_through {
            Some(v) => PublicationWatermark::PublishedThrough(v),
            None => PublicationWatermark::NoPublishedCommits,
        },
        log_paths,
        wire.protocol,
        wire.metadata,
        wire.last_checkpoint_hint,
        crc,
        if wire.latest {
            SnapshotHintFreshness::Latest
        } else {
            SnapshotHintFreshness::Unverified
        },
    )
    .map_err(|e| format!("SnapshotHint::try_new: {e}"))
}

fn crc_from_wire(w: CrcWire) -> Result<Crc, String> {
    let e = |e: delta_kernel::KernelError| e.to_string();
    let file_stats_state = match w.file_stats {
        FileStatsWire::Complete {
            num_files,
            table_size_bytes,
            histogram,
        } => FileStatsState::Complete(
            FileStats::try_new(num_files, table_size_bytes, histogram).map_err(e)?,
        ),
        FileStatsWire::Indeterminate => FileStatsState::Indeterminate,
    };
    let set_txns: HashMap<String, SetTransaction> = w.set_transactions.into_iter().collect();
    let dms: HashMap<String, DomainMetadata> = w.domain_metadata.into_iter().collect();
    let all_files = w
        .all_files
        .map(|adds| {
            adds.into_iter()
                .map(|v| serde_json::from_value::<Add>(v).map_err(|e| format!("add: {e}")))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    Crc::try_from_parts(
        w.version,
        w.metadata,
        w.protocol,
        file_stats_state,
        w.in_commit_timestamp,
        if w.set_transactions_complete {
            SetTransactionState::Complete(set_txns)
        } else {
            SetTransactionState::Partial(set_txns)
        },
        if w.domain_metadata_complete {
            DomainMetadataState::Complete(dms)
        } else {
            DomainMetadataState::Partial(dms)
        },
        w.txn_id,
        all_files,
        w.num_deleted_records,
        w.num_deletion_vectors,
        w.deleted_record_counts
            .map(DeletedRecordCountsHistogram::try_new)
            .transpose()
            .map_err(e)?,
        w.last_manifest_commit,
    )
    .map_err(e)
}

/// The first component in which two hints differ, if any.
fn first_mismatch(a: &SnapshotHint, b: &SnapshotHint) -> Option<&'static str> {
    if a.version() != b.version() {
        return Some("version");
    }
    if a.log_segment_files() != b.log_segment_files() {
        return Some("log segment files");
    }
    if a.protocol() != b.protocol() {
        return Some("protocol");
    }
    if a.metadata() != b.metadata() {
        return Some("metadata");
    }
    if a.last_checkpoint_hint() != b.last_checkpoint_hint() {
        return Some("last checkpoint hint");
    }
    if a.crc() != b.crc() {
        return Some("crc");
    }
    if a.freshness() != b.freshness() {
        return Some("freshness");
    }
    None
}

/// An executor that fails every op: a hint rebuild must not touch storage.
#[derive(Debug)]
struct NoIo;

impl PlanExecutor for NoIo {
    fn execute_op(&self, _op: delta_kernel::Operation) -> delta_kernel::KernelResult<PlanResult> {
        Err(delta_kernel::KernelError::internal_error(
            "snapshot hint rebuild attempted engine IO",
        ))
    }
}

/// Rebuilds the native snapshot at `version` from an encoded hint, with zero engine IO.
pub fn rebuild(bytes: &[u8], table_root: &str, version: i64) -> Result<SnapshotRef, String> {
    let hint = decode(bytes, table_root)?;
    let engine = PlanBasedEngine::new(None, Arc::new(NoIo));
    Snapshot::builder_for(table_root)
        .at_version(version as u64)
        .with_snapshot_hint(hint)
        .build(&engine)
        .map_err(|e| format!("rebuild from hint: {e}"))
}
