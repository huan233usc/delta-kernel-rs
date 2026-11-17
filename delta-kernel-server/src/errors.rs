//! Error plumbing: ffi-compatible numeric `KernelError` codes and the server-level [`ServerError`].
//!
//! The numeric codes are the `FFIKernelError` discriminants from `ffi/src/error.rs`, so the DBR
//! client can reuse the mapping it already has for the JNR path.

use delta_kernel::KernelError;

use crate::proto;

/// ffi `FFIKernelError` discriminants used on the wire.
pub mod codes {
    pub const UNKNOWN: i32 = 0;
    pub const ARROW: i32 = 2;
    pub const ENGINE_DATA_TYPE: i32 = 3;
    pub const EXTRACT: i32 = 4;
    pub const GENERIC: i32 = 5;
    pub const IO: i32 = 6;
    pub const PARQUET: i32 = 7;
    pub const OBJECT_STORE: i32 = 8;
    pub const OBJECT_STORE_PATH: i32 = 9;
    pub const REQWEST: i32 = 10;
    pub const FILE_NOT_FOUND: i32 = 11;
    pub const MISSING_COLUMN: i32 = 12;
    pub const UNEXPECTED_COLUMN_TYPE: i32 = 13;
    pub const MISSING_DATA: i32 = 14;
    pub const MISSING_VERSION: i32 = 15;
    pub const DELETION_VECTOR: i32 = 16;
    pub const INVALID_URL: i32 = 17;
    pub const MALFORMED_JSON: i32 = 18;
    pub const MISSING_METADATA: i32 = 19;
    pub const MISSING_PROTOCOL: i32 = 20;
    pub const INVALID_PROTOCOL: i32 = 21;
    pub const MISSING_METADATA_AND_PROTOCOL: i32 = 22;
    pub const PARSE: i32 = 23;
    pub const JOIN_FAILURE: i32 = 24;
    pub const UTF8: i32 = 25;
    pub const PARSE_INT: i32 = 26;
    pub const INVALID_COLUMN_MAPPING_MODE: i32 = 27;
    pub const INVALID_TABLE_LOCATION: i32 = 28;
    pub const INVALID_DECIMAL: i32 = 29;
    pub const INVALID_STRUCT_DATA: i32 = 30;
    pub const INTERNAL: i32 = 31;
    pub const INVALID_EXPRESSION: i32 = 32;
    pub const INVALID_LOG_PATH: i32 = 33;
    pub const FILE_ALREADY_EXISTS: i32 = 34;
    pub const UNSUPPORTED: i32 = 35;
    pub const PARSE_INTERVAL: i32 = 36;
    pub const CHANGE_DATA_FEED_UNSUPPORTED: i32 = 37;
    pub const CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA: i32 = 38;
    pub const INVALID_CHECKPOINT: i32 = 39;
    pub const CHECKPOINT_WRITE: i32 = 41;
    pub const SCHEMA: i32 = 42;
    pub const LOG_HISTORY: i32 = 43;
    pub const ROW_TRACKING_CHANGE_FEED_UNSUPPORTED: i32 = 44;
    pub const CANCELLED: i32 = 45;
    pub const INVALID_TRANSACTION_STATE: i32 = 46;
    pub const INVALID_LOG_SEGMENT: i32 = 47;
    pub const UNPUBLISHED_VERSION: i32 = 48;
    pub const EMPTY_LOG: i32 = 49;
    pub const INVALID_SNAPSHOT_HINT: i32 = 50;
    pub const START_VERSION_NOT_FOUND: i32 = 51;
    pub const INVALID_GEO_PARAMS: i32 = 52;
    pub const MAX_CATALOG_VERSION: i32 = 53;
    pub const TABLE_ALREADY_EXISTS_AT_PATH: i32 = 54;
    pub const COMMIT_RANGE: i32 = 55;
    pub const INVALID_INTERVAL: i32 = 56;
}

/// Maps a kernel error to its ffi numeric code (the inverse of [`kernel_error_from_code`]).
pub fn kernel_error_code(e: &KernelError) -> i32 {
    use codes::*;
    match e {
        KernelError::Arrow(_) => ARROW,
        KernelError::CheckpointWrite(_) => CHECKPOINT_WRITE,
        KernelError::EngineDataType(_) => ENGINE_DATA_TYPE,
        KernelError::Extract(..) => EXTRACT,
        KernelError::ScalarConversion(_) => UNKNOWN,
        KernelError::Generic(_) | KernelError::GenericError { .. } => GENERIC,
        KernelError::CommitRange(_) => COMMIT_RANGE,
        KernelError::MaxCatalogVersion(_) => MAX_CATALOG_VERSION,
        KernelError::LogTailVersionsNotContiguous { .. } => INVALID_LOG_SEGMENT,
        KernelError::IOError(_) => IO,
        KernelError::Parquet(_) => PARQUET,
        KernelError::ObjectStore(object_store_err) => {
            // Mirror what a JVM storage client reports for a missing object.
            if matches!(
                object_store_err,
                delta_kernel::object_store::Error::NotFound { .. }
            ) {
                FILE_NOT_FOUND
            } else {
                OBJECT_STORE
            }
        }
        KernelError::ObjectStorePath(_) => OBJECT_STORE_PATH,
        KernelError::Reqwest(_) => REQWEST,
        KernelError::FileNotFound(_) => FILE_NOT_FOUND,
        KernelError::MissingColumn(_) => MISSING_COLUMN,
        KernelError::InvalidPartitionValues(_) => UNKNOWN,
        KernelError::UnexpectedColumnType(_) => UNEXPECTED_COLUMN_TYPE,
        KernelError::MissingData(_) => MISSING_DATA,
        KernelError::EmptyLog => EMPTY_LOG,
        KernelError::MissingVersion(_) => MISSING_VERSION,
        KernelError::StartVersionNotFound { .. } => START_VERSION_NOT_FOUND,
        KernelError::UnpublishedVersion(_) => UNPUBLISHED_VERSION,
        KernelError::DeletionVector(_) => DELETION_VECTOR,
        KernelError::InvalidSelectionVector(_) => UNKNOWN,
        KernelError::InvalidTransactionState(_) => INVALID_TRANSACTION_STATE,
        KernelError::InvalidUrl(_) => INVALID_URL,
        KernelError::MalformedJson(_) => MALFORMED_JSON,
        KernelError::MissingMetadata => MISSING_METADATA,
        KernelError::MissingProtocol => MISSING_PROTOCOL,
        KernelError::InvalidProtocol(_) => INVALID_PROTOCOL,
        KernelError::MissingMetadataAndProtocol => MISSING_METADATA_AND_PROTOCOL,
        KernelError::ParseError(..) => PARSE,
        KernelError::JoinFailure(_) => JOIN_FAILURE,
        KernelError::Utf8Error(_) => UTF8,
        KernelError::ParseIntError(_) => PARSE_INT,
        KernelError::InvalidColumnMappingMode(_) => INVALID_COLUMN_MAPPING_MODE,
        KernelError::InvalidTableLocation(_) => INVALID_TABLE_LOCATION,
        KernelError::InvalidDecimal(_) => INVALID_DECIMAL,
        KernelError::InvalidInterval(_) => INVALID_INTERVAL,
        KernelError::InvalidGeoParams(_) => INVALID_GEO_PARAMS,
        KernelError::InvalidStructData(_) => INVALID_STRUCT_DATA,
        KernelError::InternalError(_) => INTERNAL,
        KernelError::InvalidExpressionEvaluation(_) => INVALID_EXPRESSION,
        KernelError::InvalidLogPath(_) => INVALID_LOG_PATH,
        KernelError::InvalidLogSegment(_) => INVALID_LOG_SEGMENT,
        KernelError::SnapshotHint(_) => INVALID_SNAPSHOT_HINT,
        KernelError::FileAlreadyExists(_) => FILE_ALREADY_EXISTS,
        KernelError::TableAlreadyExistsAtPath(_) => TABLE_ALREADY_EXISTS_AT_PATH,
        KernelError::Unsupported(_) => UNSUPPORTED,
        KernelError::ChecksumWriteUnsupported(_) => UNKNOWN,
        KernelError::ParseIntervalError(_) => PARSE_INTERVAL,
        KernelError::ChangeDataFeedUnsupported(_) => CHANGE_DATA_FEED_UNSUPPORTED,
        KernelError::RowTrackingChangeFeedUnsupported(_) => ROW_TRACKING_CHANGE_FEED_UNSUPPORTED,
        KernelError::ChangeDataFeedIncompatibleSchema(_, _) => CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA,
        KernelError::InvalidCheckpoint(_) => INVALID_CHECKPOINT,
        KernelError::Schema(_) => SCHEMA,
        KernelError::StatsValidation(_) => UNKNOWN,
        KernelError::LogHistory(_) => LOG_HISTORY,
        KernelError::PlanResultTypeMismatch { .. } => UNKNOWN,
        KernelError::Cancelled => CANCELLED,
        KernelError::Backtraced { source, .. } => kernel_error_code(source),
        _ => UNKNOWN,
    }
}

/// The name of an ffi `KernelError` code, for messages. Error text never shows the bare number:
/// it is easily mistaken for a `ServerMsg.error` code (ffi `OBJECT_STORE` = 8 vs
/// `PERMISSION_DENIED` = 8).
pub fn kernel_code_name(code: i32) -> &'static str {
    use codes::*;
    match code {
        ARROW => "Arrow",
        ENGINE_DATA_TYPE => "EngineDataType",
        EXTRACT => "Extract",
        GENERIC => "Generic",
        IO => "IO",
        PARQUET => "Parquet",
        OBJECT_STORE => "ObjectStore",
        OBJECT_STORE_PATH => "ObjectStorePath",
        REQWEST => "Reqwest",
        FILE_NOT_FOUND => "FileNotFound",
        MISSING_COLUMN => "MissingColumn",
        UNEXPECTED_COLUMN_TYPE => "UnexpectedColumnType",
        MISSING_DATA => "MissingData",
        MISSING_VERSION => "MissingVersion",
        DELETION_VECTOR => "DeletionVector",
        INVALID_URL => "InvalidUrl",
        MALFORMED_JSON => "MalformedJson",
        MISSING_METADATA => "MissingMetadata",
        MISSING_PROTOCOL => "MissingProtocol",
        INVALID_PROTOCOL => "InvalidProtocol",
        MISSING_METADATA_AND_PROTOCOL => "MissingMetadataAndProtocol",
        PARSE => "Parse",
        JOIN_FAILURE => "JoinFailure",
        UTF8 => "Utf8",
        PARSE_INT => "ParseInt",
        INVALID_COLUMN_MAPPING_MODE => "InvalidColumnMappingMode",
        INVALID_TABLE_LOCATION => "InvalidTableLocation",
        INVALID_DECIMAL => "InvalidDecimal",
        INVALID_STRUCT_DATA => "InvalidStructData",
        INTERNAL => "Internal",
        INVALID_EXPRESSION => "InvalidExpression",
        INVALID_LOG_PATH => "InvalidLogPath",
        FILE_ALREADY_EXISTS => "FileAlreadyExists",
        UNSUPPORTED => "Unsupported",
        PARSE_INTERVAL => "ParseInterval",
        CHANGE_DATA_FEED_UNSUPPORTED => "ChangeDataFeedUnsupported",
        CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA => "ChangeDataFeedIncompatibleSchema",
        INVALID_CHECKPOINT => "InvalidCheckpoint",
        CHECKPOINT_WRITE => "CheckpointWrite",
        SCHEMA => "Schema",
        LOG_HISTORY => "LogHistory",
        ROW_TRACKING_CHANGE_FEED_UNSUPPORTED => "RowTrackingChangeFeedUnsupported",
        CANCELLED => "Cancelled",
        INVALID_TRANSACTION_STATE => "InvalidTransactionState",
        INVALID_LOG_SEGMENT => "InvalidLogSegment",
        UNPUBLISHED_VERSION => "UnpublishedVersion",
        EMPTY_LOG => "EmptyLog",
        INVALID_SNAPSHOT_HINT => "InvalidSnapshotHint",
        START_VERSION_NOT_FOUND => "StartVersionNotFound",
        INVALID_GEO_PARAMS => "InvalidGeoParams",
        MAX_CATALOG_VERSION => "MaxCatalogVersion",
        TABLE_ALREADY_EXISTS_AT_PATH => "TableAlreadyExistsAtPath",
        COMMIT_RANGE => "CommitRange",
        INVALID_INTERVAL => "InvalidInterval",
        _ => "Unknown",
    }
}

/// Message prefix of the `UNSUPPORTED` error for a catalog-managed table resolved without catalog
/// state (kernel `MaxCatalogVersion`).
pub const CATALOG_STATE_REQUIRED_PREFIX: &str = "CATALOG_STATE_REQUIRED: ";

/// The kernel condition behind [`CATALOG_STATE_REQUIRED_PREFIX`]: loading the latest snapshot of a
/// catalog-managed table without a max catalog version. Other `MaxCatalogVersion` errors (a tail
/// inconsistent with its bound, a version above the bound) are request bugs and stay `INTERNAL`.
const CATALOG_STATE_REQUIRED_KERNEL_TEXT: &str =
    "required when loading the latest catalog-managed snapshot";

/// `UNSUPPORTED` with [`CATALOG_STATE_REQUIRED_PREFIX`] when a build that had no catalog state
/// (path-based latest; the caller decides) failed only because the table is catalog-managed.
pub fn catalog_state_required(e: &delta_kernel::Error) -> Option<ServerError> {
    let delta_kernel::Error::Kernel(k) = e else {
        return None;
    };
    match root_kernel_error(k) {
        KernelError::MaxCatalogVersion(m) if m.contains(CATALOG_STATE_REQUIRED_KERNEL_TEXT) => {
            Some(
                ServerError::new(
                    proto::error::Code::Unsupported,
                    format!("{CATALOG_STATE_REQUIRED_PREFIX}{e}"),
                )
                .with_kernel_error(k),
            )
        }
        _ => None,
    }
}

/// Message prefix of an `OP_FAILED` error (client-reported op failure).
pub const OP_FAILED_PREFIX: &str = "OP_FAILED: client op failed";
/// Message prefix of the `UNAVAILABLE` error for a failed server-side op (SPEC v9.1).
pub const LOCAL_IO_FAILED_PREFIX: &str = "UNAVAILABLE: server-side IO failed";

/// Converts a wire error from the client back into a kernel error, mirroring ffi's
/// `From<EngineExecError> for KernelError`.
pub fn kernel_error_from_code(code: i32, message: String) -> KernelError {
    use codes::*;
    match code {
        CHECKPOINT_WRITE => KernelError::CheckpointWrite(message),
        ENGINE_DATA_TYPE => KernelError::EngineDataType(message),
        GENERIC => KernelError::Generic(message),
        COMMIT_RANGE => KernelError::CommitRange(message),
        MAX_CATALOG_VERSION => KernelError::MaxCatalogVersion(message),
        INTERNAL => KernelError::InternalError(message),
        FILE_NOT_FOUND => KernelError::FileNotFound(message),
        MISSING_COLUMN => KernelError::MissingColumn(message),
        UNEXPECTED_COLUMN_TYPE => KernelError::UnexpectedColumnType(message),
        MISSING_DATA => KernelError::MissingData(message),
        DELETION_VECTOR => KernelError::DeletionVector(message),
        INVALID_PROTOCOL => KernelError::InvalidProtocol(message),
        JOIN_FAILURE => KernelError::JoinFailure(message),
        INVALID_COLUMN_MAPPING_MODE => KernelError::InvalidColumnMappingMode(message),
        INVALID_TABLE_LOCATION => KernelError::InvalidTableLocation(message),
        INVALID_DECIMAL => KernelError::InvalidDecimal(message),
        INVALID_INTERVAL => KernelError::InvalidInterval(message),
        INVALID_GEO_PARAMS => KernelError::InvalidGeoParams(message),
        INVALID_STRUCT_DATA => KernelError::InvalidStructData(message),
        INVALID_EXPRESSION => KernelError::InvalidExpressionEvaluation(message),
        INVALID_LOG_PATH => KernelError::InvalidLogPath(message),
        INVALID_LOG_SEGMENT => KernelError::InvalidLogSegment(message),
        INVALID_SNAPSHOT_HINT => delta_kernel::snapshot::SnapshotHintError::Connector {
            message,
            source: None,
        }
        .into(),
        FILE_ALREADY_EXISTS => KernelError::FileAlreadyExists(message),
        TABLE_ALREADY_EXISTS_AT_PATH => KernelError::TableAlreadyExistsAtPath(message),
        UNSUPPORTED => KernelError::Unsupported(message),
        INVALID_CHECKPOINT => KernelError::InvalidCheckpoint(message),
        SCHEMA => KernelError::Schema(message),
        INVALID_TRANSACTION_STATE => KernelError::InvalidTransactionState(message),
        EMPTY_LOG => KernelError::EmptyLog,
        MISSING_METADATA => KernelError::MissingMetadata,
        MISSING_PROTOCOL => KernelError::MissingProtocol,
        MISSING_METADATA_AND_PROTOCOL => KernelError::MissingMetadataAndProtocol,
        CANCELLED => KernelError::Cancelled,
        other => KernelError::generic(format!(
            "engine execution error ({}): {message}",
            ffi_kernel_error_debug_name(other)
        )),
    }
}

fn ffi_kernel_error_debug_name(code: i32) -> &'static str {
    use codes::*;
    match code {
        UNKNOWN => "UnknownError",
        1 => "FFIError",
        ARROW => "ArrowError",
        EXTRACT => "ExtractError",
        IO => "IOErrorError",
        PARQUET => "ParquetError",
        OBJECT_STORE => "ObjectStoreError",
        OBJECT_STORE_PATH => "ObjectStorePathError",
        REQWEST => "ReqwestError",
        INVALID_URL => "InvalidUrlError",
        MALFORMED_JSON => "MalformedJsonError",
        PARSE => "ParseError",
        UTF8 => "Utf8Error",
        PARSE_INT => "ParseIntError",
        PARSE_INTERVAL => "ParseIntervalError",
        CHANGE_DATA_FEED_UNSUPPORTED => "ChangeDataFeedUnsupported",
        CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA => "ChangeDataFeedIncompatibleSchema",
        ROW_TRACKING_CHANGE_FEED_UNSUPPORTED => "RowTrackingChangeFeedUnsupported",
        LOG_HISTORY => "LogHistoryError",
        MISSING_VERSION => "MissingVersionError",
        UNPUBLISHED_VERSION => "UnpublishedVersionError",
        START_VERSION_NOT_FOUND => "StartVersionNotFound",
        _ => "UnknownError",
    }
}

/// Builds the wire form of a kernel error.
pub fn to_wire_kernel_error(e: &KernelError) -> proto::KernelError {
    proto::KernelError {
        code: kernel_error_code(e),
        message: e.to_string(),
    }
}

/// The root cause of a kernel error, looking through `Backtraced` wrappers.
pub fn root_kernel_error(e: &KernelError) -> &KernelError {
    match e {
        KernelError::Backtraced { source, .. } => root_kernel_error(source),
        other => other,
    }
}

/// A request-level failure reported to the client as `ServerMsg.error`.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerError {
    pub code: proto::error::Code,
    pub message: String,
    pub retryable: bool,
    pub kernel_error: Option<proto::KernelError>,
}

impl ServerError {
    pub fn new(code: proto::error::Code, message: impl Into<String>) -> Self {
        let retryable = matches!(
            code,
            proto::error::Code::Cancelled
                | proto::error::Code::Deadline
                | proto::error::Code::Unavailable
        );
        Self {
            code,
            message: message.into(),
            retryable,
            kernel_error: None,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(proto::error::Code::Internal, message)
    }

    pub fn with_kernel_error(mut self, e: &KernelError) -> Self {
        self.kernel_error = Some(to_wire_kernel_error(root_kernel_error(e)));
        self
    }

    pub fn to_proto(&self) -> proto::Error {
        proto::Error {
            code: self.code as i32,
            message: self.message.clone(),
            retryable: self.retryable,
            kernel_error: self.kernel_error.clone(),
        }
    }
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

/// Why a job's op channel stopped working; decides the error code once kernel unwinds with
/// `KernelError::Cancelled`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReason {
    /// Every candidate stream closed before the op completed.
    StreamsGone,
    /// The driving stream did not answer within its op deadline (or a server-side op ran past it).
    Deadline,
    /// The bounded server-side IO pool had no free worker (SPEC v9.1: UNAVAILABLE).
    Saturated,
}

/// Classifies a snapshot build failure (kernel's top-level [`delta_kernel::Error`]).
pub fn classify_build_error(
    e: &delta_kernel::Error,
    cancel: Option<CancelReason>,
    op_failure: Option<&proto::KernelError>,
) -> ServerError {
    match e {
        delta_kernel::Error::Kernel(k) => classify_kernel_error(k, cancel, op_failure),
        delta_kernel::Error::Delta(d) if cancel.is_none() && op_failure.is_none() => {
            let is_missing_path = d.name().contains("PATH_DOES_NOT_EXIST");
            let code = if is_missing_path {
                proto::error::Code::TableNotFound
            } else {
                proto::error::Code::Internal
            };
            let err = ServerError::new(code, format!("{}: {d}", d.name()));
            if is_missing_path {
                let message = err.message.clone();
                err.with_kernel_error(&KernelError::FileNotFound(message))
            } else {
                err
            }
        }
        other => {
            classify_kernel_error(&KernelError::generic(other.to_string()), cancel, op_failure)
        }
    }
}

/// Classifies a kernel error raised by a snapshot build or plan generation.
///
/// `op_failure` is the first op-level error the client reported during the job, if any: kernel
/// errors caused by client-reported storage failures surface as `OP_FAILED`.
pub fn classify_kernel_error(
    e: &KernelError,
    cancel: Option<CancelReason>,
    op_failure: Option<&proto::KernelError>,
) -> ServerError {
    use proto::error::Code;
    let root = root_kernel_error(e);
    if let Some(reason) = cancel {
        return match reason {
            CancelReason::Deadline => ServerError::new(Code::Deadline, format!("op deadline: {e}")),
            CancelReason::Saturated => ServerError::new(
                Code::Unavailable,
                format!("server-side IO pool saturated; retry later: {e}"),
            ),
            CancelReason::StreamsGone => ServerError::new(
                Code::Cancelled,
                format!("all client streams closed during the job: {e}"),
            ),
        };
    }
    if let Some(op_err) = op_failure {
        return ServerError::new(
            Code::OpFailed,
            format!(
                "{OP_FAILED_PREFIX} ({} error): {}; kernel error: {e}",
                kernel_code_name(op_err.code),
                op_err.message
            ),
        )
        .with_kernel_error(root);
    }
    match root {
        KernelError::Unsupported(_) => {
            ServerError::new(Code::Unsupported, e.to_string()).with_kernel_error(root)
        }
        KernelError::InvalidProtocol(_) => {
            ServerError::new(Code::ProtocolUnsupported, e.to_string()).with_kernel_error(root)
        }
        KernelError::FileNotFound(_)
        | KernelError::EmptyLog
        | KernelError::MissingVersion(_)
        | KernelError::InvalidTableLocation(_) => {
            ServerError::new(Code::TableNotFound, e.to_string()).with_kernel_error(root)
        }
        _ => ServerError::internal(e.to_string()).with_kernel_error(root),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    /// Only kernel's "latest of a catalog-managed table without a bound" condition maps to
    /// CATALOG_STATE_REQUIRED; every other MaxCatalogVersion stays INTERNAL without the prefix.
    #[test]
    fn only_the_missing_catalog_bound_is_catalog_state_required() {
        let mcv = |m: &str| delta_kernel::Error::Kernel(KernelError::MaxCatalogVersion(m.into()));
        let required = mcv(
            "Max catalog version is required when loading the latest catalog-managed snapshot. \
             Use with_max_catalog_version().",
        );
        let e = catalog_state_required(&required).expect("maps");
        assert_eq!(e.code, proto::error::Code::Unsupported);
        assert!(e.message.starts_with(CATALOG_STATE_REQUIRED_PREFIX));
        for other in [
            "Requested version 5 exceeds max catalog version 3",
            "Latest commit version 2 does not match max catalog version 3",
            "Latest commit version 1 is less than requested version 2 for max catalog version 3",
            "Max catalog version is required when providing staged commits. Use \
             with_max_catalog_version().",
        ] {
            assert!(catalog_state_required(&mcv(other)).is_none(), "{other}");
            let delta_kernel::Error::Kernel(k) = mcv(other) else {
                unreachable!()
            };
            let c = classify_kernel_error(&k, None, None);
            assert_eq!(c.code, proto::error::Code::Internal, "{other}");
            assert!(
                !c.message.contains(CATALOG_STATE_REQUIRED_PREFIX),
                "{other}"
            );
        }
        assert!(
            catalog_state_required(&delta_kernel::Error::Kernel(KernelError::generic("x")))
                .is_none()
        );
    }

    #[test]
    fn op_failure_text_names_the_kernel_error_not_a_number() {
        let op_err = proto::KernelError {
            code: codes::OBJECT_STORE,
            message: "Received redirect without LOCATION".into(),
        };
        let e = classify_kernel_error(&KernelError::generic("x"), None, Some(&op_err));
        assert_eq!(e.code, proto::error::Code::OpFailed);
        assert!(e.message.starts_with(OP_FAILED_PREFIX), "{}", e.message);
        assert!(e.message.contains("(ObjectStore error)"), "{}", e.message);
        assert!(!e.message.contains("code 8"), "{}", e.message);
        assert_eq!(kernel_code_name(8), "ObjectStore");
    }

    #[test]
    fn synthetic_cancel_errors_do_not_attach_kernel_error() {
        for reason in [
            CancelReason::Deadline,
            CancelReason::Saturated,
            CancelReason::StreamsGone,
        ] {
            let e = classify_kernel_error(&KernelError::Cancelled, Some(reason), None);
            assert_eq!(e.kernel_error, None, "{reason:?}: {e:?}");
        }
    }

    #[test]
    fn op_failed_invalid_column_mapping_mode_preserves_ffi_code() {
        let op_err = proto::KernelError {
            code: codes::INVALID_COLUMN_MAPPING_MODE,
            message: "unsupported".into(),
        };
        let kernel_error = kernel_error_from_code(op_err.code, op_err.message.clone());
        let e = classify_kernel_error(&kernel_error, None, Some(&op_err));
        assert_eq!(e.code, proto::error::Code::OpFailed);
        assert_eq!(
            e.kernel_error,
            Some(proto::KernelError {
                code: codes::INVALID_COLUMN_MAPPING_MODE,
                message: "Invalid column mapping mode: unsupported".into(),
            })
        );
    }

    #[derive(Debug)]
    struct FfiErrorMappings {
        discriminants: BTreeMap<String, i32>,
        kernel_to_ffi: BTreeMap<String, String>,
        kernel_fallback: String,
        engine_direct: BTreeMap<String, String>,
        engine_fallback: BTreeSet<String>,
    }

    fn ffi_error_mappings() -> FfiErrorMappings {
        const SOURCE: &str = include_str!("../../ffi/src/error.rs");
        let discriminants = ffi_kernel_error_discriminants(SOURCE);
        let kernel_to_ffi = ffi_kernel_error_from_kernel_arms(SOURCE);
        let kernel_fallback = ffi_kernel_error_from_kernel_fallback(SOURCE);
        let (engine_direct, engine_fallback) = ffi_engine_exec_arms(SOURCE);
        FfiErrorMappings {
            discriminants,
            kernel_to_ffi,
            kernel_fallback,
            engine_direct,
            engine_fallback,
        }
    }

    fn ffi_kernel_error_discriminants(source: &str) -> BTreeMap<String, i32> {
        let enum_body = source
            .split_once("pub enum FFIKernelError {")
            .expect("FFIKernelError enum")
            .1
            .split_once("\n}")
            .expect("FFIKernelError enum end")
            .0;
        enum_body
            .lines()
            .filter_map(|line| {
                let line = line
                    .split_once("//")
                    .map_or(line, |(before, _)| before)
                    .trim();
                let (name, value) = line.trim_end_matches(',').split_once(" = ")?;
                let name = name.trim();
                if !name.chars().next().is_some_and(char::is_uppercase) {
                    return None;
                }
                Some((
                    name.to_string(),
                    value.trim().parse::<i32>().expect("ffi discriminant"),
                ))
            })
            .collect()
    }

    fn ffi_kernel_error_from_kernel_arms(source: &str) -> BTreeMap<String, String> {
        let match_body = source
            .split_once("impl From<&KernelError> for FFIKernelError")
            .expect("From<&KernelError>")
            .1
            .split_once("match e {")
            .expect("From<&KernelError> match")
            .1
            .split_once("\n        }\n    }\n}")
            .expect("From<&KernelError> match end")
            .0;
        let mut mappings = BTreeMap::new();
        let mut pending_kernel = None;
        for line in match_body.lines() {
            if let Some(kernel) = extract_prefixed_ident(line, "KernelError::") {
                pending_kernel = Some(kernel);
            }
            if let Some(ffi) = extract_prefixed_ident(line, "FFIKernelError::")
                && let Some(kernel) = pending_kernel.take()
            {
                mappings.insert(kernel, ffi);
            }
            if line.contains("Self::from(source.as_ref())") {
                pending_kernel = None;
            }
        }
        mappings
    }

    fn ffi_kernel_error_from_kernel_fallback(source: &str) -> String {
        let match_body = source
            .split_once("impl From<&KernelError> for FFIKernelError")
            .expect("From<&KernelError>")
            .1
            .split_once("match e {")
            .expect("From<&KernelError> match")
            .1
            .split_once("\n        }\n    }\n}")
            .expect("From<&KernelError> match end")
            .0;
        match_body
            .lines()
            .find(|line| line.trim_start().starts_with("_ => FFIKernelError::"))
            .and_then(|line| extract_prefixed_ident(line, "FFIKernelError::"))
            .expect("KernelError fallback ffi mapping")
    }

    fn ffi_engine_exec_arms(source: &str) -> (BTreeMap<String, String>, BTreeSet<String>) {
        let match_body = source
            .split_once("impl From<EngineExecError> for KernelError")
            .expect("From<EngineExecError>")
            .1
            .split_once("match etype {")
            .expect("From<EngineExecError> match")
            .1
            .split_once("\n        }\n    }\n}")
            .expect("From<EngineExecError> match end")
            .0;

        let mut arms = Vec::new();
        let mut current = String::new();
        for line in match_body.lines() {
            let trimmed = line.trim_start();
            let starts_arm =
                trimmed.starts_with("FFIKernelError::") || trimmed.starts_with("code @");
            if starts_arm && !current.is_empty() {
                arms.push(std::mem::take(&mut current));
            }
            if starts_arm || !current.is_empty() {
                current.push_str(line);
                current.push('\n');
            }
        }
        if !current.is_empty() {
            arms.push(current);
        }

        let mut direct = BTreeMap::new();
        let mut fallback = BTreeSet::new();
        for arm in arms {
            let ffi_variants = extract_all_prefixed_idents(&arm, "FFIKernelError::");
            if arm.contains("KernelError::generic") {
                fallback.extend(ffi_variants);
                continue;
            }
            let kernel = if arm.contains("SnapshotHintError::") {
                Some("SnapshotHint".to_string())
            } else {
                extract_prefixed_ident(&arm, "KernelError::")
            };
            if let Some(kernel) = kernel {
                assert_eq!(
                    ffi_variants.len(),
                    1,
                    "direct EngineExecError arm should name one ffi variant: {arm}"
                );
                direct.insert(ffi_variants[0].clone(), kernel);
            }
        }
        (direct, fallback)
    }

    fn extract_all_prefixed_idents(text: &str, prefix: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut start = 0;
        while let Some(relative) = text[start..].find(prefix) {
            let index = start + relative;
            if index > 0 {
                let previous = text[..index].chars().next_back().expect("previous char");
                if previous.is_ascii_alphanumeric() || previous == '_' {
                    start = index + prefix.len();
                    continue;
                }
            }
            let rest = &text[index + prefix.len()..];
            let ident_len = rest
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(rest.len());
            out.push(rest[..ident_len].to_string());
            start = index + prefix.len() + ident_len;
        }
        out
    }

    fn extract_prefixed_ident(text: &str, prefix: &str) -> Option<String> {
        extract_all_prefixed_idents(text, prefix).into_iter().next()
    }

    fn debug_variant_name(e: &KernelError) -> String {
        let debug = format!("{e:?}");
        debug
            .split(['(', '{'])
            .next()
            .expect("debug variant")
            .trim()
            .to_string()
    }

    #[test]
    fn kernel_error_from_code_matches_ffi_engine_exec_mapping() {
        let ffi = ffi_error_mappings();
        let covered: BTreeSet<_> = ffi
            .engine_direct
            .keys()
            .chain(ffi.engine_fallback.iter())
            .cloned()
            .collect();
        assert_eq!(
            covered,
            ffi.discriminants.keys().cloned().collect::<BTreeSet<_>>()
        );

        for (ffi_variant, code) in &ffi.discriminants {
            let actual = kernel_error_from_code(*code, "boom".into());
            if let Some(expected_variant) = ffi.engine_direct.get(ffi_variant) {
                assert_eq!(
                    debug_variant_name(&actual),
                    *expected_variant,
                    "{ffi_variant} ({code}): {actual:?}"
                );
            } else {
                assert!(
                    ffi.engine_fallback.contains(ffi_variant),
                    "{ffi_variant} has no direct or fallback EngineExecError mapping"
                );
                assert!(
                    matches!(
                        actual,
                        KernelError::Generic(ref message)
                            if message == &format!(
                                "engine execution error ({ffi_variant}): boom"
                            )
                    ),
                    "{ffi_variant} ({code}): {actual:?}"
                );
            }
        }
    }

    #[test]
    fn kernel_error_code_matches_ffi_discriminants_for_current_variants() {
        use delta_kernel::expressions::Scalar;
        use delta_kernel::history_manager::error::LogHistoryError;
        use delta_kernel::object_store;
        use delta_kernel::schema::DataType;
        use delta_kernel::snapshot::SnapshotHintError;
        use delta_kernel::table_properties::ParseIntervalError;

        fn scalar_conversion_error() -> KernelError {
            bool::try_from(Scalar::Integer(7)).expect_err("expected scalar conversion error")
        }

        fn malformed_json_error() -> serde_json::Error {
            serde_json::from_str::<serde_json::Value>("{").expect_err("expected json error")
        }

        fn object_store_error() -> object_store::Error {
            object_store::Error::Generic {
                store: "test",
                source: std::io::Error::other("boom").into(),
            }
        }

        fn object_store_not_found() -> object_store::Error {
            object_store::Error::NotFound {
                path: "missing".to_string(),
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "boom").into(),
            }
        }

        fn object_store_path_error() -> object_store::path::Error {
            object_store::path::Path::parse("//").expect_err("expected object-store path error")
        }

        fn reqwest_error() -> reqwest::Error {
            reqwest::Client::new()
                .get("not a url")
                .build()
                .expect_err("expected reqwest error")
        }

        fn invalid_url_error() -> url::ParseError {
            url::Url::parse("not a url").expect_err("expected url parse error")
        }

        fn utf8_error() -> std::str::Utf8Error {
            let bytes = vec![0xff];
            std::str::from_utf8(&bytes).expect_err("expected utf8 error")
        }

        fn parse_int_error() -> std::num::ParseIntError {
            "x".parse::<i64>().expect_err("expected parse-int error")
        }

        fn log_history_error() -> Box<LogHistoryError> {
            Box::new(LogHistoryError::Internal {
                context: "test",
                source: None,
            })
        }

        let cases: Vec<(&str, KernelError)> = vec![
            (
                "Arrow",
                delta_kernel::arrow::error::ArrowError::ComputeError("boom".into()).into(),
            ),
            (
                "CheckpointWrite",
                KernelError::CheckpointWrite("boom".into()),
            ),
            ("EngineDataType", KernelError::EngineDataType("boom".into())),
            ("Extract", KernelError::Extract("u8", "i32")),
            ("ScalarConversion", scalar_conversion_error()),
            ("Generic", KernelError::Generic("boom".into())),
            (
                "GenericError",
                KernelError::GenericError {
                    source: std::io::Error::other("boom").into(),
                },
            ),
            ("CommitRange", KernelError::CommitRange("boom".into())),
            (
                "MaxCatalogVersion",
                KernelError::MaxCatalogVersion("boom".into()),
            ),
            (
                "LogTailVersionsNotContiguous",
                KernelError::LogTailVersionsNotContiguous {
                    first_version: 1,
                    second_version: 3,
                },
            ),
            (
                "IOError",
                KernelError::IOError(std::io::Error::other("boom")),
            ),
            (
                "Parquet",
                delta_kernel::parquet::errors::ParquetError::General("boom".into()).into(),
            ),
            (
                "ObjectStore",
                KernelError::ObjectStore(object_store_error()),
            ),
            (
                "ObjectStoreNotFound",
                KernelError::ObjectStore(object_store_not_found()),
            ),
            (
                "ObjectStorePath",
                KernelError::ObjectStorePath(object_store_path_error()),
            ),
            ("Reqwest", KernelError::Reqwest(reqwest_error())),
            ("FileNotFound", KernelError::FileNotFound("missing".into())),
            ("MissingColumn", KernelError::MissingColumn("col".into())),
            (
                "InvalidPartitionValues",
                KernelError::InvalidPartitionValues("boom".into()),
            ),
            (
                "UnexpectedColumnType",
                KernelError::UnexpectedColumnType("col".into()),
            ),
            ("MissingData", KernelError::MissingData("col".into())),
            ("EmptyLog", KernelError::EmptyLog),
            ("MissingVersion", KernelError::MissingVersion(7)),
            (
                "StartVersionNotFound",
                KernelError::StartVersionNotFound {
                    requested: 1,
                    earliest: 3,
                },
            ),
            ("UnpublishedVersion", KernelError::UnpublishedVersion(7)),
            ("DeletionVector", KernelError::DeletionVector("boom".into())),
            (
                "InvalidSelectionVector",
                KernelError::InvalidSelectionVector("boom".into()),
            ),
            (
                "InvalidTransactionState",
                KernelError::InvalidTransactionState("boom".into()),
            ),
            ("InvalidUrl", KernelError::InvalidUrl(invalid_url_error())),
            (
                "MalformedJson",
                KernelError::MalformedJson(malformed_json_error()),
            ),
            ("MissingMetadata", KernelError::MissingMetadata),
            ("MissingProtocol", KernelError::MissingProtocol),
            (
                "InvalidProtocol",
                KernelError::InvalidProtocol("boom".into()),
            ),
            (
                "MissingMetadataAndProtocol",
                KernelError::MissingMetadataAndProtocol,
            ),
            (
                "ParseError",
                KernelError::ParseError("boom".into(), DataType::STRING),
            ),
            ("JoinFailure", KernelError::JoinFailure("boom".into())),
            ("Utf8Error", KernelError::Utf8Error(utf8_error())),
            (
                "ParseIntError",
                KernelError::ParseIntError(parse_int_error()),
            ),
            (
                "InvalidColumnMappingMode",
                KernelError::InvalidColumnMappingMode("boom".into()),
            ),
            (
                "InvalidTableLocation",
                KernelError::InvalidTableLocation("boom".into()),
            ),
            ("InvalidDecimal", KernelError::InvalidDecimal("boom".into())),
            (
                "InvalidInterval",
                KernelError::InvalidInterval("boom".into()),
            ),
            (
                "InvalidGeoParams",
                KernelError::InvalidGeoParams("boom".into()),
            ),
            (
                "InvalidStructData",
                KernelError::InvalidStructData("boom".into()),
            ),
            ("InternalError", KernelError::InternalError("boom".into())),
            (
                "Backtraced",
                KernelError::Backtraced {
                    source: Box::new(KernelError::InternalError("boom".into())),
                    backtrace: Box::new(std::backtrace::Backtrace::force_capture()),
                },
            ),
            (
                "InvalidExpressionEvaluation",
                KernelError::InvalidExpressionEvaluation("boom".into()),
            ),
            ("InvalidLogPath", KernelError::InvalidLogPath("boom".into())),
            (
                "InvalidLogSegment",
                KernelError::InvalidLogSegment("boom".into()),
            ),
            (
                "SnapshotHint",
                SnapshotHintError::Connector {
                    message: "boom".into(),
                    source: None,
                }
                .into(),
            ),
            (
                "FileAlreadyExists",
                KernelError::FileAlreadyExists("boom".into()),
            ),
            (
                "TableAlreadyExistsAtPath",
                KernelError::TableAlreadyExistsAtPath("boom".into()),
            ),
            ("Unsupported", KernelError::Unsupported("boom".into())),
            (
                "ChecksumWriteUnsupported",
                KernelError::ChecksumWriteUnsupported("boom".into()),
            ),
            (
                "ParseIntervalError",
                KernelError::ParseIntervalError(ParseIntervalError::NotAnInterval("boom".into())),
            ),
            (
                "ChangeDataFeedUnsupported",
                KernelError::ChangeDataFeedUnsupported(7),
            ),
            (
                "RowTrackingChangeFeedUnsupported",
                KernelError::RowTrackingChangeFeedUnsupported(7),
            ),
            (
                "ChangeDataFeedIncompatibleSchema",
                KernelError::ChangeDataFeedIncompatibleSchema("expected".into(), "actual".into()),
            ),
            (
                "InvalidCheckpoint",
                KernelError::InvalidCheckpoint("boom".into()),
            ),
            ("Schema", KernelError::Schema("boom".into())),
            (
                "StatsValidation",
                KernelError::StatsValidation("boom".into()),
            ),
            ("LogHistory", KernelError::LogHistory(log_history_error())),
            (
                "PlanResultTypeMismatch",
                KernelError::PlanResultTypeMismatch {
                    expected: "Unit",
                    actual: "Data",
                },
            ),
            ("Cancelled", KernelError::Cancelled),
        ];

        let ffi = ffi_error_mappings();
        assert_eq!(
            kernel_error_case_names(&cases),
            current_kernel_error_variant_names()
        );
        for (name, error) in &cases {
            let expected_debug = if *name == "ObjectStoreNotFound" {
                "ObjectStore"
            } else {
                *name
            };
            assert_eq!(
                debug_variant_name(error),
                expected_debug,
                "{name}: {error:?}"
            );

            let ffi_variant = expected_ffi_variant_for_case(name, error, &ffi);
            let expected = ffi
                .discriminants
                .get(&ffi_variant)
                .unwrap_or_else(|| panic!("missing ffi discriminant for {ffi_variant}"));
            assert_eq!(kernel_error_code(error), *expected, "{name}: {error}");
        }
    }

    fn expected_ffi_variant_for_case(
        name: &str,
        error: &KernelError,
        ffi: &FfiErrorMappings,
    ) -> String {
        match name {
            // The wire protocol deliberately reports storage NotFound the way a JVM storage client
            // does, so table-missing errors line up with the existing DBR mapping.
            "ObjectStoreNotFound" => "FileNotFoundError".to_string(),
            "Backtraced" => {
                let KernelError::Backtraced { source, .. } = error else {
                    panic!("Backtraced row was not Backtraced: {error:?}");
                };
                expected_ffi_variant_for_case(&debug_variant_name(source), source, ffi)
            }
            _ => ffi
                .kernel_to_ffi
                .get(name)
                .cloned()
                .unwrap_or_else(|| ffi.kernel_fallback.clone()),
        }
    }

    fn kernel_error_case_names(cases: &[(&'static str, KernelError)]) -> BTreeSet<&'static str> {
        cases
            .iter()
            .map(|(name, _)| *name)
            .filter(|name| *name != "ObjectStoreNotFound")
            .collect()
    }

    fn current_kernel_error_variant_names() -> BTreeSet<&'static str> {
        const SOURCE: &str = include_str!("../../kernel/src/error.rs");
        let enum_body = SOURCE
            .split_once("pub enum KernelError {")
            .expect("KernelError enum")
            .1
            .split_once("\n}")
            .expect("KernelError enum end")
            .0;
        enum_body
            .lines()
            .filter_map(|line| {
                let line = line.trim_start();
                let first = line.split([' ', '(', '{', ',']).next()?;
                (line.starts_with(first) && first.chars().next().is_some_and(char::is_uppercase))
                    .then_some(first)
            })
            .collect()
    }
}
