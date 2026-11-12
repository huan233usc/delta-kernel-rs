//! delta-kernel-server (POC, not for merge).
//!
//! A long-running gRPC service that builds delta_kernel snapshots, `SnapshotInfo`, and declarative
//! metadata scan plans, and caches them across queries. The service performs no storage IO: every
//! kernel IO or query op is sent to the requesting client over the bidi stream (see
//! [`executor::RemotePlanExecutor`]). The wire contract is `proto/delta_kernel_server.proto`.

pub mod cache;
pub mod decode;
pub mod errors;
pub mod executor;
pub mod info;
pub mod normalize;
pub mod plan;
pub mod service;
pub mod stats;

/// Generated messages and gRPC stubs for `delta.kernel.server.v1`.
#[allow(clippy::all, missing_docs)]
pub mod proto {
    tonic::include_proto!("delta.kernel.server.v1");
}
