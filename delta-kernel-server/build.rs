//! Generates the gRPC service stubs and a deterministic (BTreeMap-backed) copy of kernel's plan
//! protos used to normalize plan bytes before comparing them.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let server_proto_dir = manifest_dir.join("proto");
    let kernel_proto_dir = manifest_dir.join("../kernel/proto");
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);

    println!("cargo:rerun-if-changed=proto/delta_kernel_server.proto");
    for f in ["expressions", "operation", "plan", "schema"] {
        println!("cargo:rerun-if-changed=../kernel/proto/{f}.proto");
    }

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(
            &[server_proto_dir.join("delta_kernel_server.proto")],
            std::slice::from_ref(&server_proto_dir),
        )?;

    // Kernel's own prost types use `HashMap` for proto `map<>` fields, so re-encoding them is
    // not byte-stable. This second copy uses `BTreeMap` everywhere; it is wire-compatible and is
    // only used to produce a canonical encoding of a plan.
    let normalized_out = out_dir.join("normalized");
    std::fs::create_dir_all(&normalized_out)?;
    prost_build::Config::new()
        .btree_map(["."])
        .out_dir(&normalized_out)
        .compile_protos(
            &[
                kernel_proto_dir.join("schema.proto"),
                kernel_proto_dir.join("expressions.proto"),
                kernel_proto_dir.join("plan.proto"),
                kernel_proto_dir.join("operation.proto"),
            ],
            &[kernel_proto_dir],
        )?;
    Ok(())
}
