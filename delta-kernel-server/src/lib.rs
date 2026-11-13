//! delta-kernel-server (POC, not for merge).
//!
//! A long-running gRPC service that builds delta_kernel snapshots, `SnapshotInfo`, and declarative
//! metadata scan plans, and caches them across queries. Simple IO ops run on the server
//! ([`local_io`], with UC-vended credentials from [`creds`]); query-plan ops are sent to the
//! requesting client over the bidi stream (see [`executor::RemotePlanExecutor`]). The wire
//! contract is `proto/delta_kernel_server.proto`.

pub mod cache;
pub mod creds;
pub mod decode;
pub mod errors;
pub mod executor;
pub mod hint;
pub mod info;
pub mod local_io;
pub mod normalize;
pub mod plan;
pub mod service;
pub mod stats;
pub mod store;
pub mod timetravel;

/// Generated messages and gRPC stubs for `delta.kernel.server.v1`.
#[allow(clippy::all, missing_docs)]
pub mod proto {
    tonic::include_proto!("delta.kernel.server.v1");
}
