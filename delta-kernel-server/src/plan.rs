//! Metadata scan plan generation for one [`proto::ScanPlanSpec`].

use delta_kernel::expressions::ColumnName;
use delta_kernel::plans::proto::plan as proto_plan;
use delta_kernel::scan::{PartitionValuesOptions, StatsOptions};
use delta_kernel::{Engine, KernelError, SnapshotRef};
use prost::Message as _;

use crate::errors::root_kernel_error;
use crate::proto::{self, scan_plan_result, stats_options};

/// Canonical cache key of a spec. The message has no map fields, so prost's encoding is stable.
pub fn spec_key(spec: &proto::ScanPlanSpec) -> Vec<u8> {
    spec.encode_to_vec()
}

/// Kernel scan options for a spec.
pub fn scan_options(
    spec: &proto::ScanPlanSpec,
) -> Result<(StatsOptions, PartitionValuesOptions), String> {
    let stats = spec.stats.clone().unwrap_or_default();
    let stats = match stats_options::Kind::try_from(stats.kind) {
        Ok(stats_options::Kind::JsonOnly) => StatsOptions::json_only(),
        Ok(stats_options::Kind::AllStructWithExtraIndexed) => {
            StatsOptions::all_struct_with_extra_indexed(
                stats
                    .extra_indexed_columns
                    .into_iter()
                    .map(|c| ColumnName::new(c.parts))
                    .collect(),
            )
        }
        Err(_) => return Err(format!("unknown StatsOptions.Kind {}", stats.kind)),
    };
    let partition = match proto::PartitionValuesMode::try_from(spec.partition_values) {
        Ok(proto::PartitionValuesMode::WithStruct) => PartitionValuesOptions::with_struct(),
        Err(_) => {
            return Err(format!(
                "unknown PartitionValuesMode {}",
                spec.partition_values
            ));
        }
    };
    let partition = match &spec.timestamp_timezone {
        Some(tz) => partition.with_timestamp_timezone(tz.clone()),
        None => partition,
    };
    Ok((stats, partition))
}

/// Result of generating one plan: a per-plan outcome, or a kernel error that fails the request.
pub enum PlanOutcome {
    Result(proto::ScanPlanResult),
    Failed(KernelError),
}

/// Builds the scan for `spec` and returns its declarative metadata plan, encoded.
///
/// Kernel `Unsupported` errors (TIMESTAMP partitions with a timezone, adaptive-metadata checkpoint
/// actions) become a per-plan `unsupported` result so the client can fall back for that plan only.
pub fn generate_plan(
    snapshot: &SnapshotRef,
    spec: &proto::ScanPlanSpec,
    engine: &dyn Engine,
) -> PlanOutcome {
    let unsupported = |message: String| {
        PlanOutcome::Result(proto::ScanPlanResult {
            result: Some(scan_plan_result::Result::Unsupported(proto::Error {
                code: proto::error::Code::Unsupported as i32,
                message,
                retryable: false,
            })),
            cache_hit: false,
        })
    };
    let (stats, partition) = match scan_options(spec) {
        Ok(opts) => opts,
        Err(message) => return unsupported(message),
    };
    let result = snapshot
        .clone()
        .scan_builder()
        .with_stats(stats)
        .with_partition_values(partition)
        .build()
        .and_then(|scan| scan.declarative_metadata_scan_plan(engine));
    match result {
        Ok(Some(plan)) => PlanOutcome::Result(proto::ScanPlanResult {
            result: Some(scan_plan_result::Result::Plan(
                proto_plan::Plan::from(&plan).encode_to_vec(),
            )),
            cache_hit: false,
        }),
        Ok(None) => PlanOutcome::Result(proto::ScanPlanResult {
            result: Some(scan_plan_result::Result::Empty(proto::StaticallyEmpty {})),
            cache_hit: false,
        }),
        Err(e) if matches!(root_kernel_error(&e), KernelError::Unsupported(_)) => {
            unsupported(e.to_string())
        }
        Err(e) => PlanOutcome::Failed(e),
    }
}
