# delta-kernel-server (POC, do not merge)

A long-running gRPC service that runs the driver-side delta_kernel work of the DBR DSv2 read
path: snapshot build (full or incremental), `SnapshotInfo`, and
`Scan::declarative_metadata_scan_plan`. It caches all three across queries.

The server does no storage IO and holds no credentials. Every kernel IO or query op is sent to the
requesting client as an `OpRequest` on the same bidi stream. The client executes it (in DBR, with
the existing `DeltaV2KernelPlanExecutor`) and sends back an `OpResult`.

- Wire contract: [`proto/delta_kernel_server.proto`](proto/delta_kernel_server.proto) (SPEC v5 §5,
  plus two additive fields marked `DEVIATION`).
- Kernel: the in-tree `kernel` crate at `4633926ff40ff39e5ba8bc690a965b4fb793c073`.

## Build

Requires `protoc` on `PATH`, or set `PROTOC`. Kernel's `declarative-plans` and this crate both
generate code from protos. Keep cargo output off `/home`:

```bash
export CARGO_TARGET_DIR=/ephemeral/cargo-out/dks-server
cargo +1.95.0 build -p delta-kernel-server --release
# binary: $CARGO_TARGET_DIR/release/delta-kernel-server
```

### Kernel features

The server builds kernel with the same feature set as DBR's `delta_kernel_ffi` build:
`declarative-plans`, `default-engine-base`, `adaptive-metadata-in-dev`, `geo-type-in-dev`,
`internal-api` (plus `arrow-60`).

`delta-kernel-unity-catalog` is an ffi-crate feature, not a kernel feature. Its only effect on
kernel is `internal-api`, so the server adds no dependency for it. It still appears in the
advertised `kernel_features` list so the client's list compares equal.

`ClientInfo.kernel_rev` must equal `4633926ff40ff39e5ba8bc690a965b4fb793c073`, and
`ClientInfo.kernel_features` must equal the list above (order-insensitive). Otherwise the request
fails with `VERSION_MISMATCH` before any op is issued. To change the expected rev, set
`DELTA_KERNEL_SERVER_KERNEL_REV` at build time.

## Run

```bash
delta-kernel-server \
  --listen 127.0.0.1:0 \
  --endpoint-file /tmp/dks.endpoint \
  --instance-id my-run-1 \
  --enable-test-hooks \
  --max-instances-per-table 2
```

| flag | default | meaning |
|---|---|---|
| `--listen` | `127.0.0.1:50051` | bind address; port `0` picks a free port |
| `--endpoint-file` | none | after bind, write `host:port\n` here atomically (temp file + rename) |
| `--instance-id` | `dks-<pid>-<nanos>` | returned by `Ping` and in every `ServerStats` |
| `--enable-test-hooks` | off | enables `Ping`, `GetDebugStats`, `ResetCache`, `InjectFault`; without it they return `PERMISSION_DENIED` |
| `--max-instances-per-table` | `2` | cached snapshot instances kept per `(table_id, location)` |
| `--max-cache-bytes` | `4 GiB` | global LRU bound over estimated memory (snapshot heap + plan bytes) |

SIGTERM or Ctrl-C shuts the server down gracefully. Logs go to stderr; `RUST_LOG` controls the
level (default `info`). Max gRPC message size is 256 MiB in both directions.

## Server semantics (SPEC §7)

- **Cache entry**: `{Arc<Snapshot>, SnapshotInfo, plans: ScanPlanSpec -> ScanPlanResult}`.
  `SnapshotInfo` is computed once, during the build, through the build's op channel.
  `active_domain_metadata` mirrors JNR `getActiveDomainMetadataMap` (kernel
  `get_all_domain_metadata`): active user domains only, no `delta.*` system domains. A hit
  returns the stored copy. Clustering, ICT timestamp, domain metadata, and row-tracking getters do
  IO and kernel does not cache them, so they cannot be recomputed on a hit.
- **Hit (exact version)**: same version, and `expected_log_segment` is present and names the same
  files as the cached segment. A request without `expected_log_segment` is always a miss
  (SPEC v6). A hit issues 0 ops when every requested plan is cached.
  - Segments are compared by canonical identity: version, checkpoint version, and the sorted
    `(parsed URL, size)` of checkpoint parts plus `(parsed URL, size, staged)` of deltas.
    `last_modified` is ignored. DBR re-qualifies locations through Hadoop (`file:///p` becomes
    `file:/p`), so raw string comparison never matched a real client.
- **Path-based `latest {max_staleness_ms}`** (SPEC v7): the server remembers, per table, the last
  confirmed latest instance and when its listing started.
  - Confirmed within `max_staleness_ms` → `CACHE_HIT`, 0 ops.
  - Otherwise one re-listing via `Snapshot::builder_from(confirmed instance)`. No new commit →
    renew, `CACHE_HIT` with `ops_issued = 1`, same SnapshotInfo and plan cache. New commits →
    `INCREMENTAL`. No confirmed instance (or it was evicted) → `FULL_BUILD`.
  - Concurrent latest requests for a table share one re-listing (`COALESCED`).
  - Tainted results do not renew or replace the confirmation.
- **Hit / coalescing (`catalog_latest`)**: keyed on `max_catalog_version` plus the canonical log
  tail (`(version, location, size)` sorted by version). A tail containing staged commits is never
  served from cache, never coalesced, and its result is never cached.
- **ListingHint**: sent only for listings of `_delta_log/` itself (`end_version` = build target,
  `stop_at_first_non_versioned = true`). `_delta_log/_sidecars/` listings get no hint.
- **Miss**: incremental `Snapshot::builder_from(newest cached older instance)` if there is one,
  otherwise a full build.
- **Coalescing**: builds coalesce on `(table_id, location, target version)`; plan generation
  coalesces on `(instance, ScanPlanSpec)`. Each waiting stream joins the job's op channel. If the
  driving stream dies, the in-flight op is re-sent to the next waiting stream. The job continues
  and does not restart. With no waiter left, the executor returns `KernelError::Cancelled`.
- **Errors**: a stream loss or a missed op deadline makes `RemotePlanExecutor` return
  `KernelError::Cancelled`, which surfaces as `CANCELLED` / `DEADLINE`. Only a stream loss hands
  the op to another waiter; a missed deadline fails the build for the driver and every waiter
  (SPEC v6). A client-reported op error
  surfaces as `OP_FAILED`. Kernel `Unsupported` from plan generation (TIMESTAMP partitions with a
  timezone, adaptive-metadata checkpoint actions) is a per-plan `unsupported` result.
- **Tainted builds**: any op error other than `FileNotFound` (whole-op or per-item) taints the job.
  The result still goes to the waiting requests, but it is not cached. Staged-commit instances are
  never cached.
- Kernel runs on `spawn_blocking` threads with
  `PlanBasedEngine::new(None, RemotePlanExecutor)`. `Snapshot` and `Scan` have a compile-time
  `Send + Sync` assertion.

## Tests (L0)

```bash
export CARGO_TARGET_DIR=/ephemeral/cargo-out/dks-server
cargo +1.95.0 test -p delta-kernel-server
cargo +1.95.0 clippy -p delta-kernel-server --all-targets -- -D warnings
cargo +nightly fmt -- --check   # the repo's rustfmt.toml uses nightly-only options
```

`tests/loopback.rs` (L0a) starts the server in-process on `127.0.0.1:0`. Its test client decodes
each forwarded `Operation` (`src/decode.rs`) and executes it with kernel's `SyncPlanExecutor`
(dev-only `test-utils`) over copies of `kernel/tests/data` fixtures. Every parity test checks:

1. The remote plan and the in-process `declarative_metadata_scan_plan` on the same snapshot
   execute to the same live add rows, sorted by path.
2. Both plans' bytes are equal after normalization (`src/normalize.rs`).
3. `SnapshotInfo` equals an in-process computation.

## How the runtime L1 suite consumes the binary

`DeltaKernelServerParitySuite` (runtime draft worktree, SPEC §8 L1) does not build this crate. It
runs a prebuilt binary:

```bash
bazel test //.../tahoe/v2/remote:DeltaKernelServerParitySuite_ffi \
  --test_env=DELTA_KERNEL_SERVER_BIN=$CARGO_TARGET_DIR/release/delta-kernel-server
```

The fixture (`KernelServerFixture`, modeled on `LiteRuntimeTestEnvFixture`) does the following:

1. If `DELTA_KERNEL_SERVER_ENDPOINT=host:port` is set, it warm-attaches to that server instead of
   spawning one.
2. Otherwise it spawns `$DELTA_KERNEL_SERVER_BIN --listen 127.0.0.1:0 --endpoint-file <tmp>
   --instance-id <uuid> --enable-test-hooks`, with stderr sent to `$TEST_UNDECLARED_OUTPUTS_DIR`.
   If `DELTA_KERNEL_SERVER_BIN` is unset, it cancels the suite in `beforeAll`.
3. It polls until the endpoint file exists. The file is renamed into place, so a partial read is
   impossible.
4. It calls `Ping` and checks that the returned `instance_id` matches the one it passed.
5. It uses `GetDebugStats` (`builds_started`, `builds_coalesced`, `cached_instances_per_table`),
   `ResetCache`, and `InjectFault` for the concurrency and fault cases.
6. On shutdown, or from `kill()` / `restart()`, it sends SIGTERM, then SIGKILL after a grace
   period.

The client sends `ClientInfo{kernel_rev, kernel_features, op_deadline_ms}` on every request. It
must set the gRPC max message size to 256 MiB on its side too.
