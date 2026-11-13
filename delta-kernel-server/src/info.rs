//! Computes [`proto::SnapshotInfo`] from a kernel snapshot.
//!
//! Called exactly once per build, with the build's engine (and therefore its driving stream).
//! Several getters (ICT timestamp, clustering columns, domain metadata, row-tracking high-water
//! mark) issue IO and kernel does not cache them, so a cache hit must serve the stored copy rather
//! than recomputing (SPEC §7).

use std::collections::{BTreeMap, HashMap};

use delta_kernel::actions::{DomainMetadata, Metadata, Protocol};
use delta_kernel::crc::{Crc, DomainMetadataState, SetTransactionState};
use delta_kernel::log_segment::LogSegment;
use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use delta_kernel::{Engine, KernelError, KernelResult, Snapshot};
use sha2::Digest as _;

use crate::errors::root_kernel_error;
use crate::proto;

pub fn compute_snapshot_info(
    snapshot: &Snapshot,
    engine: &dyn Engine,
) -> KernelResult<proto::SnapshotInfo> {
    let reader = snapshot.reader();
    let log_segment = snapshot.log_segment();
    let tc = snapshot.table_configuration();

    let timestamp_ms = reader.get_timestamp(engine)?;
    // Kernel fails this getter when the clustering domain does not resolve against the schema.
    // The in-process path only fails when the getter is called, so report it per-getter.
    let (clustering_columns, clustering_error) = match clustering(&reader, engine) {
        Ok(cols) => (cols, None),
        Err(e) if matches!(root_kernel_error(&e), KernelError::Cancelled) => return Err(e),
        Err(e) => (
            Vec::new(),
            Some(proto::Error {
                code: proto::error::Code::Internal as i32,
                message: e.to_string(),
                retryable: false,
            }),
        ),
    };
    let row_tracking_high_water_mark = reader.get_row_tracking_high_water_mark(engine)?;
    // Mirrors the JNR `getActiveDomainMetadataMap` getter, which is ffi `visit_domain_metadata`
    // over `get_all_domain_metadata`: active user-controlled domains only. System `delta.*`
    // domains (clustering, row tracking) are exposed through their own SnapshotInfo fields.
    let active_domain_metadata = reader
        .get_all_domain_metadata(engine)?
        .iter()
        .map(|d| (d.domain().to_string(), domain_metadata(d)))
        .collect();

    let crc = snapshot.crc_at_version().map(|crc| {
        let stats = snapshot.log_segment_stats().ok();
        crc_info(crc, stats)
    });

    Ok(proto::SnapshotInfo {
        table_root: snapshot.table_root().to_string(),
        log_path: log_segment.log_root.to_string(),
        timestamp_ms,
        metadata: Some(metadata(tc.metadata())),
        protocol: Some(protocol(tc.protocol())),
        crc,
        clustering_columns,
        clustering_error,
        log_segment: Some(log_segment_info(log_segment)),
        row_tracking_high_water_mark,
        active_domain_metadata,
        latest_txn_version_placeholder: None,
    })
}

fn clustering(
    reader: &delta_kernel::SnapshotReader<'_>,
    engine: &dyn Engine,
) -> KernelResult<Vec<proto::ClusteringColumn>> {
    reader
        .get_clustering_column_infos(engine)?
        .unwrap_or_default()
        .into_iter()
        .map(|c| {
            Ok(proto::ClusteringColumn {
                physical_path: Some(column_path(c.physical_column.path())),
                logical_path: Some(column_path(c.logical_column.path())),
                data_type_json: serde_json::to_string(&c.data_type)
                    .map_err(KernelError::generic_err)?,
            })
        })
        .collect()
}

/// Whether the segment contains staged (unpublished catalog) commits; such instances are not
/// cached (SPEC §7).
pub fn has_staged_commits(snapshot: &Snapshot) -> bool {
    snapshot
        .log_segment()
        .listed
        .ascending_commit_files
        .iter()
        .any(|p| p.file_type == LogPathFileType::StagedCommit)
}

pub fn log_segment_info(seg: &LogSegment) -> proto::LogSegmentInfo {
    let files = &seg.listed;
    proto::LogSegmentInfo {
        version: seg.end_version as i64,
        checkpoint_version: seg.checkpoint_version.map(|v| v as i64),
        checkpoint_parts: files.checkpoint_parts.iter().map(file_status).collect(),
        deltas: files
            .ascending_commit_files
            .iter()
            .map(file_status)
            .collect(),
        delta_is_staged: files
            .ascending_commit_files
            .iter()
            .map(|p| p.file_type == LogPathFileType::StagedCommit)
            .collect(),
    }
}

/// Canonical identity of a log segment for cache-hit comparison (SPEC v7 §7).
///
/// Clients do not echo URLs byte-for-byte: DBR re-qualifies every location through Hadoop
/// `Path.makeQualified`, which renders `file:///tmp/t` as `file:/tmp/t` and leaves spaces
/// unescaped. Locations are therefore compared after parsing as URLs. `last_modified` is ignored:
/// Delta log files are immutable and named by version, so (location, size) identifies a file, and
/// listing sources disagree on mtime precision. Any difference in version, checkpoint version, the
/// set of checkpoint parts or deltas, a size, or a staged flag is still a different segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentId {
    version: i64,
    checkpoint_version: Option<i64>,
    checkpoint_parts: Vec<(String, i64)>,
    deltas: Vec<(String, i64, bool)>,
}

/// Canonical form of a location: the parsed URL, or the raw string if it does not parse.
pub fn canonical_location(location: &str) -> String {
    url::Url::parse(location).map_or_else(|_| location.to_string(), |u| u.to_string())
}

impl SegmentId {
    /// `SnapshotRef.segment_id` and the L2 `seg_id` (SPEC v10.1): hex SHA-256 of the canonical
    /// identity, serialized as JSON `[version, checkpoint_version, [[location, size]...],
    /// [[location, size, staged]...]]` with both lists sorted.
    pub fn digest(&self) -> String {
        let bytes = serde_json::to_vec(&(
            self.version,
            self.checkpoint_version,
            &self.checkpoint_parts,
            &self.deltas,
        ))
        .expect("tuples of strings and integers always serialize");
        hex::encode(sha2::Sha256::digest(&bytes))
    }

    pub fn version(&self) -> i64 {
        self.version
    }
}

pub fn segment_id(seg: &proto::LogSegmentInfo) -> SegmentId {
    let mut checkpoint_parts: Vec<(String, i64)> = seg
        .checkpoint_parts
        .iter()
        .map(|f| (canonical_location(&f.location), f.size))
        .collect();
    checkpoint_parts.sort();
    let mut deltas: Vec<(String, i64, bool)> = seg
        .deltas
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let staged = seg.delta_is_staged.get(i).copied().unwrap_or(false);
            (canonical_location(&f.location), f.size, staged)
        })
        .collect();
    deltas.sort();
    SegmentId {
        version: seg.version,
        checkpoint_version: seg.checkpoint_version,
        checkpoint_parts,
        deltas,
    }
}

fn file_status(p: &ParsedLogPath) -> proto::FileStatus {
    proto::FileStatus {
        location: p.location.location.to_string(),
        size: p.location.size as i64,
        last_modified: p.location.last_modified,
    }
}

fn column_path(parts: &[String]) -> proto::ColumnPath {
    proto::ColumnPath {
        parts: parts.to_vec(),
    }
}

fn clone_map(map: &HashMap<String, String>) -> HashMap<String, String> {
    map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

fn metadata(m: &Metadata) -> proto::Metadata {
    proto::Metadata {
        id: m.id().to_string(),
        name: m.name().map(str::to_string),
        description: m.description().map(str::to_string),
        format: Some(proto::Format {
            provider: m.format_provider().to_string(),
            options: clone_map(m.format_options()),
        }),
        schema_string: m.schema_string().to_string(),
        partition_columns: m.partition_columns().to_vec(),
        configuration: clone_map(m.configuration()),
        created_time: m.created_time(),
    }
}

fn protocol(p: &Protocol) -> proto::Protocol {
    let list = |f: Option<&[delta_kernel::table_features::TableFeature]>| {
        f.map(|f| proto::FeatureList {
            features: f.iter().map(|x| x.to_string()).collect(),
        })
    };
    proto::Protocol {
        min_reader_version: p.min_reader_version(),
        min_writer_version: p.min_writer_version(),
        reader_features: list(p.reader_features()),
        writer_features: list(p.writer_features()),
    }
}

fn domain_metadata(d: &DomainMetadata) -> proto::DomainMetadata {
    proto::DomainMetadata {
        domain: d.domain().to_string(),
        configuration: d.configuration().to_string(),
        removed: d.is_removed(),
    }
}

fn crc_info(
    crc: &Crc,
    stats: Option<delta_kernel::log_segment::LogSegmentStats>,
) -> proto::CrcInfo {
    let file_stats = crc.file_stats();
    let set_transactions = match &crc.set_transaction_state {
        SetTransactionState::Complete(m) | SetTransactionState::Partial(m) => m
            .iter()
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .map(|t| proto::SetTransaction {
                app_id: t.app_id().to_string(),
                version: t.version(),
                last_updated: t.last_updated(),
            })
            .collect(),
    };
    let domain_metadata = match &crc.domain_metadata_state {
        DomainMetadataState::Complete(m) | DomainMetadataState::Partial(m) => m
            .iter()
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .map(domain_metadata)
            .collect(),
    };
    proto::CrcInfo {
        num_files: file_stats.map_or(0, |s| s.num_files()),
        table_size_bytes: file_stats.map_or(0, |s| s.table_size_bytes()),
        num_delta_files: stats.as_ref().map(|s| s.num_commit_files as i64),
        total_delta_files_byte_size: stats.as_ref().map(|s| s.total_commit_file_bytes as i64),
        file_size_histogram: file_stats.and_then(|s| s.file_size_histogram()).map(|h| {
            proto::FileSizeHistogram {
                sorted_bin_boundaries: h.sorted_bin_boundaries().to_vec(),
                file_counts: h.file_counts().to_vec(),
                total_bytes: h.total_bytes().to_vec(),
            }
        }),
        set_transactions,
        domain_metadata,
        num_deleted_records: crc.num_deleted_records(),
        txn_id: crc.txn_id().map(str::to_string),
    }
}
