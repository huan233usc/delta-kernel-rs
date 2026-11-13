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
    pub const INVALID_TABLE_LOCATION: i32 = 28;
    pub const INVALID_STRUCT_DATA: i32 = 30;
    pub const INTERNAL: i32 = 31;
    pub const INVALID_EXPRESSION: i32 = 32;
    pub const INVALID_LOG_PATH: i32 = 33;
    pub const FILE_ALREADY_EXISTS: i32 = 34;
    pub const UNSUPPORTED: i32 = 35;
    pub const INVALID_CHECKPOINT: i32 = 39;
    pub const SCHEMA: i32 = 42;
    pub const CANCELLED: i32 = 45;
    pub const INVALID_LOG_SEGMENT: i32 = 47;
    pub const EMPTY_LOG: i32 = 49;
}

/// Maps a kernel error to its ffi numeric code (the inverse of [`kernel_error_from_code`]).
pub fn kernel_error_code(e: &KernelError) -> i32 {
    use codes::*;
    match e {
        KernelError::Arrow(_) => ARROW,
        KernelError::EngineDataType(_) => ENGINE_DATA_TYPE,
        KernelError::Extract(..) => EXTRACT,
        KernelError::Generic(_) | KernelError::GenericError { .. } => GENERIC,
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
        KernelError::FileNotFound(_) => FILE_NOT_FOUND,
        KernelError::MissingColumn(_) => MISSING_COLUMN,
        KernelError::UnexpectedColumnType(_) => UNEXPECTED_COLUMN_TYPE,
        KernelError::MissingData(_) => MISSING_DATA,
        KernelError::MissingVersion(_) => MISSING_VERSION,
        KernelError::DeletionVector(_) => DELETION_VECTOR,
        KernelError::InvalidUrl(_) => INVALID_URL,
        KernelError::MalformedJson(_) => MALFORMED_JSON,
        KernelError::MissingMetadata => MISSING_METADATA,
        KernelError::MissingProtocol => MISSING_PROTOCOL,
        KernelError::InvalidProtocol(_) => INVALID_PROTOCOL,
        KernelError::MissingMetadataAndProtocol => MISSING_METADATA_AND_PROTOCOL,
        KernelError::ParseError(..) => PARSE,
        KernelError::JoinFailure(_) => JOIN_FAILURE,
        KernelError::InvalidTableLocation(_) => INVALID_TABLE_LOCATION,
        KernelError::InvalidStructData(_) => INVALID_STRUCT_DATA,
        KernelError::InternalError(_) => INTERNAL,
        KernelError::InvalidExpressionEvaluation(_) => INVALID_EXPRESSION,
        KernelError::InvalidLogPath(_) => INVALID_LOG_PATH,
        KernelError::FileAlreadyExists(_) => FILE_ALREADY_EXISTS,
        KernelError::Unsupported(_) => UNSUPPORTED,
        KernelError::InvalidCheckpoint(_) => INVALID_CHECKPOINT,
        KernelError::Schema(_) => SCHEMA,
        KernelError::Cancelled => CANCELLED,
        KernelError::InvalidLogSegment(_) => INVALID_LOG_SEGMENT,
        KernelError::EmptyLog => EMPTY_LOG,
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
        INVALID_TABLE_LOCATION => "InvalidTableLocation",
        INVALID_STRUCT_DATA => "InvalidStructData",
        INTERNAL => "Internal",
        INVALID_EXPRESSION => "InvalidExpression",
        INVALID_LOG_PATH => "InvalidLogPath",
        FILE_ALREADY_EXISTS => "FileAlreadyExists",
        UNSUPPORTED => "Unsupported",
        INVALID_CHECKPOINT => "InvalidCheckpoint",
        SCHEMA => "Schema",
        CANCELLED => "Cancelled",
        INVALID_LOG_SEGMENT => "InvalidLogSegment",
        EMPTY_LOG => "EmptyLog",
        _ => "Unknown",
    }
}

/// Message prefix of the `UNSUPPORTED` error for a catalog-managed table resolved without catalog
/// state (kernel `MaxCatalogVersion`).
pub const CATALOG_STATE_REQUIRED_PREFIX: &str = "CATALOG_STATE_REQUIRED: ";

/// Message prefix of an `OP_FAILED` error (client-reported op failure).
pub const OP_FAILED_PREFIX: &str = "OP_FAILED: client op failed";
/// Message prefix of the `UNAVAILABLE` error for a failed server-side op (SPEC v9.1).
pub const LOCAL_IO_FAILED_PREFIX: &str = "UNAVAILABLE: server-side IO failed";

/// Converts a wire error from the client back into a kernel error, mirroring ffi's
/// `From<EngineExecError> for KernelError`.
pub fn kernel_error_from_code(code: i32, message: String) -> KernelError {
    use codes::*;
    match code {
        ENGINE_DATA_TYPE => KernelError::EngineDataType(message),
        GENERIC => KernelError::Generic(message),
        INTERNAL => KernelError::InternalError(message),
        FILE_NOT_FOUND => KernelError::FileNotFound(message),
        MISSING_COLUMN => KernelError::MissingColumn(message),
        UNEXPECTED_COLUMN_TYPE => KernelError::UnexpectedColumnType(message),
        MISSING_DATA => KernelError::MissingData(message),
        DELETION_VECTOR => KernelError::DeletionVector(message),
        INVALID_PROTOCOL => KernelError::InvalidProtocol(message),
        JOIN_FAILURE => KernelError::JoinFailure(message),
        INVALID_TABLE_LOCATION => KernelError::InvalidTableLocation(message),
        INVALID_STRUCT_DATA => KernelError::InvalidStructData(message),
        INVALID_EXPRESSION => KernelError::InvalidExpressionEvaluation(message),
        INVALID_LOG_PATH => KernelError::InvalidLogPath(message),
        INVALID_LOG_SEGMENT => KernelError::InvalidLogSegment(message),
        FILE_ALREADY_EXISTS => KernelError::FileAlreadyExists(message),
        UNSUPPORTED => KernelError::Unsupported(message),
        INVALID_CHECKPOINT => KernelError::InvalidCheckpoint(message),
        SCHEMA => KernelError::Schema(message),
        EMPTY_LOG => KernelError::EmptyLog,
        MISSING_METADATA => KernelError::MissingMetadata,
        MISSING_PROTOCOL => KernelError::MissingProtocol,
        MISSING_METADATA_AND_PROTOCOL => KernelError::MissingMetadataAndProtocol,
        CANCELLED => KernelError::Cancelled,
        other => KernelError::generic(format!(
            "engine execution error ({} error): {message}",
            kernel_code_name(other)
        )),
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
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(proto::error::Code::Internal, message)
    }

    pub fn to_proto(&self) -> proto::Error {
        proto::Error {
            code: self.code as i32,
            message: self.message.clone(),
            retryable: self.retryable,
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
            let code = if d.name().contains("PATH_DOES_NOT_EXIST") {
                proto::error::Code::TableNotFound
            } else {
                proto::error::Code::Internal
            };
            ServerError::new(code, format!("{}: {d}", d.name()))
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
        );
    }
    match root {
        KernelError::Unsupported(_) => ServerError::new(Code::Unsupported, e.to_string()),
        // A catalog-managed table resolved without catalog state (no `catalog_latest`): the
        // request cannot be answered by the server; the client must use its catalog path.
        KernelError::MaxCatalogVersion(_) => ServerError::new(
            Code::Unsupported,
            format!("{CATALOG_STATE_REQUIRED_PREFIX}{e}"),
        ),
        KernelError::InvalidProtocol(_) => {
            ServerError::new(Code::ProtocolUnsupported, e.to_string())
        }
        KernelError::FileNotFound(_)
        | KernelError::EmptyLog
        | KernelError::MissingVersion(_)
        | KernelError::InvalidTableLocation(_) => {
            ServerError::new(Code::TableNotFound, e.to_string())
        }
        _ => ServerError::internal(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
