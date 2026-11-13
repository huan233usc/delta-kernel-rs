# delta-kernel-server (POC, do not merge)

A long-running gRPC service that runs the driver-side delta_kernel work of the DBR DSv2 read
path: snapshot build (full or incremental), `SnapshotInfo`, and
`Scan::declarative_metadata_scan_plan`. It caches all three across queries.

IO is hybrid (SPEC v9):

- **On the server**: `FileListing`, `ReadBytes`, `HeadFile`, `ParquetFooter`, through the default
  engine's object_store handlers on one shared multi-thread tokio runtime. Cloud tables use
  storage credentials the server gets from UC with the caller's forwarded token.
- **On the client**: `QueryPlan` ops (P&M replay, sidecar / checkpoint scans) are sent to the
  requesting client as an `OpRequest` on the same bidi stream. The client executes them (in DBR,
  with the existing `DeltaV2KernelPlanExecutor`) and sends back an `OpResult`.
- `--io-mode forward` restores the pre-v9 behavior (every op forwarded) for A/B runs.

- Wire contract: [`proto/delta_kernel_server.proto`](proto/delta_kernel_server.proto) (SPEC v9 §5
  plus the additive fields of the v6-v9 revisions).
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
| `--io-mode` | `local` | `local`: simple IO ops run on the server; `forward`: every op goes to the client |
| `--local-io-threads` | `8` | worker threads and in-flight cap for server-side IO; when all are busy, requests fail with `UNAVAILABLE` |
| `--uc-endpoint-override` | none | UC base URL instead of `https://{auth.workspace_host}` (tests, proxies, HiFi) |
| `--storage-option KEY=VALUE` | none, repeatable | object_store option for every cloud store the server builds, e.g. `aws_endpoint`, `aws_allow_http`, `aws_region`, `aws_virtual_hosted_style_request`; unknown keys fail at startup |
| `--s3-endpoint URL` | none | same as `--storage-option aws_endpoint=URL`; requests are path-style (`{endpoint}/{bucket}/{key}`) unless `aws_virtual_hosted_style_request=true` |
| `--s3-allow-http` | off | same as `--storage-option aws_allow_http=true` |
| `--s3-region REGION` | none | same as `--storage-option aws_region=REGION` |
| `--test-uc-credentials-for-file` | off, hidden | test only, needs `--enable-test-hooks`: run the UC credential check for `file://` tables too |

Storage options are merged per store in this order, later wins: default `timeout=60s`, the
`--s3-*` flags, `--storage-option`, then the UC-vended credential (`aws_access_key_id`,
`aws_secret_access_key`, `aws_session_token`, `azure_storage_sas_token`, `google_bearer_token`). A
server flag therefore cannot replace the caller's credential. Keys are compared after object_store
canonicalization (`endpoint` = `aws_endpoint`), and a key only reaches the stores that take it (an
`aws_*` key never reaches an Azure store). UC responses carry no endpoint or region, so without
these flags an `s3://` table goes to real AWS.

TLS roots: the binary bundles Mozilla's root CAs (`webpki-root-certs`) and trusts them in addition
to the system store, for UC and for every object store. It runs in an image without
`/etc/ssl/certs` or `ca-certificates`. If no HTTPS client can be built at all, the server still
starts and UC calls fail with `UNAVAILABLE`.

SIGTERM or Ctrl-C shuts the server down gracefully. Logs go to stderr; `RUST_LOG` controls the
level (default `info`). Max gRPC message size is 256 MiB in both directions.

## Running in a separate container

The release binary links glibc dynamically, so use a glibc base image of the same or newer
generation as the build host (e.g. `debian:bookworm-slim`). No `ca-certificates` package is
needed.

```bash
mkdir -p /tmp/dks && cp $CARGO_TARGET_DIR/release/delta-kernel-server /tmp/dks/
docker network create dks || true     # or reuse the network LocalStack runs on

docker run --rm --name dks --network dks -p 50051:50051 \
  -v /tmp/dks:/dks \
  debian:bookworm-slim \
  /dks/delta-kernel-server \
    --listen 0.0.0.0:50051 \
    --endpoint-file /dks/endpoint \
    --instance-id hifi-1 \
    --enable-test-hooks \
    --uc-endpoint-override http://<uc-host>:<uc-port> \
    --s3-endpoint http://<s3-host>:<s3-port> --s3-allow-http --s3-region us-east-1
```

- **Listen on `0.0.0.0`** inside the container. `127.0.0.1` is the container's own loopback.
- **The endpoint file holds the bind address** (`0.0.0.0:50051`). A client on the host connects to
  `127.0.0.1:50051` through `-p`, so do not hand the file to it verbatim.
- **Addresses as seen from the container**: `<uc-host>` and `<s3-host>` are how the container
  reaches UC and S3. A service on the host is at the bridge gateway (`docker network inspect dks
  -f '{{(index .IPAM.Config 0).Gateway}}'`), or `host.docker.internal` with `--add-host
  host.docker.internal:host-gateway`. A container on the same network is reachable by its name.
  A host service bound to `127.0.0.1` only cannot be reached from a container; bind it to
  `0.0.0.0` or use `--network host` (then drop `-p` and the client uses the server's port
  directly).
- **`file://` tables** need the table directory mounted at the same path in the container
  (`-v /path:/path`), because table locations come from the client.

### HiFi run (UC control plane + LocalStack)

1. **Control plane**: in a universe worktree, run
   `bazel run //managed-catalog/src/test/hifi:control-plane-server`, with LocalStack running as
   the HiFi setup provides it.
2. **Read the dynamic endpoints from the test JVM**. Both ports change every run:
   - UC API URL: `CredentialManager.getApiUrlOpt` → `--uc-endpoint-override`;
   - LocalStack: Hadoop conf `fs.s3a.endpoint` → `--s3-endpoint`.
   Tables live in bucket `test` in `us-east-1` and need path-style requests, which is the default
   once `--s3-endpoint` is set. Rewrite `localhost` to the address the container reaches (see
   above).
3. **Start the server** as in the recipe above, with `--uc-endpoint-override http://<gw>:<uc-port>
   --s3-endpoint http://<gw>:<localstack-port> --s3-allow-http --s3-region us-east-1`. Without
   `--s3-endpoint`, the server's `_last_checkpoint` read goes to
   `https://s3.us-east-1.amazonaws.com/test/...` and fails (`Received redirect without
   LOCATION`).
4. **Point the client** at the published port (`127.0.0.1:50051`). The client forwards its UC
   token in `GetSnapshotRequest.auth`; the server exchanges it at the overridden UC endpoint for
   the LocalStack credential.

## Server semantics (SPEC §7)

- **Local IO** (SPEC v9) keeps the forwarded-op rules: per-item results for listings, reads and
  HEAD; `FileNotFound` is benign (a missing `_last_checkpoint`); any other error taints the job,
  so its result is returned but not cached. The server computes and applies the `ListingHint`
  itself. Handoff to another waiter only applies to forwarded ops.
- **Local op deadline** (SPEC v9.1): server-side ops run on a fixed-size pool
  (`--local-io-threads`, default 8) and the job waits at most the driver's `op_deadline_ms`. On
  timeout the job fails with `DEADLINE` for the driver and every waiter, is tainted and not
  cached. The abandoned op keeps its worker and permit until it really returns, then its result is
  dropped. With every permit taken, new server-side ops fail at once with `UNAVAILABLE`
  (retryable): no queueing, no extra threads. The default-engine handlers are synchronous, so a
  timed-out op cannot be dropped; cloud stores' 60 s object_store client timeout bounds how long a
  stuck call can hold a permit.
- **Server-side IO failure** that fails a job → `UNAVAILABLE` (retryable); a client-reported op
  failure stays `OP_FAILED`. The message starts with the wire code (`UNAVAILABLE: server-side IO
  failed (ObjectStore error): …`, `OP_FAILED: client op failed (IO error): …`) and names the
  kernel error kind, never its ffi number, which collided with `Error.Code` values (ffi
  `ObjectStore` = 8, `PERMISSION_DENIED` = 8).
- **Counters**: `ServerStats.ops_issued` = ops forwarded to this request's stream;
  `ServerStats.ops_local` = server-side ops run by jobs this request started (a coalesced waiter
  that only waited reports 0). `DebugStats.ops_local_total` sums all server-side ops.
- **Credentials** (SPEC v9): `file://` tables need none. For s3/abfss/gs tables:
  - `table_id` is a UC UUID → `POST /api/2.1/unity-catalog/temporary-table-credentials
    {table_id, operation: READ}`; otherwise `temporary-path-credentials {url, operation: PATH_READ}`.
  - The response maps to object_store options: `aws_temp_credentials` → `aws_access_key_id`,
    `aws_secret_access_key`, `aws_session_token`; `azure_user_delegation_sas` →
    `azure_storage_sas_token`; `gcp_oauth_token` → `google_bearer_token`.
  - Cached per `(sha256(token), table key)` until `expiration_time - 60 s`. The token is never
    logged or stored in clear.
  - Checked on every request before the snapshot cache, in both `--io-mode`s (SPEC v9.1), so a
    cache hit still needs a valid credential for the caller's token (POC authz).
  - UC 401/403 → `PERMISSION_DENIED`; 404 → `TABLE_NOT_FOUND`; 5xx or network errors →
    `UNAVAILABLE` (retryable); missing `auth` → `PERMISSION_DENIED`.
  - UC error text is untrusted: before it enters an error or a log it is sanitized (the caller's
    token, `Bearer …`, `dapi…`, JWT-like and long opaque words → `[REDACTED]`, control characters
    dropped) and cut to 256 characters.

- **Cache entry**: `{Arc<Snapshot>, SnapshotInfo, plans: ScanPlanSpec -> ScanPlanResult}`.
  `SnapshotInfo` is computed once, during the build, through the build's op channel.
  `active_domain_metadata` mirrors JNR `getActiveDomainMetadataMap` (kernel
  `get_all_domain_metadata`): active user domains only, no `delta.*` system domains. A hit
  returns the stored copy. Clustering, ICT timestamp, domain metadata, and row-tracking getters do
  IO and kernel does not cache them, so they cannot be recomputed on a hit.
- **Hit (exact version)**: same version, and either
  - `expected_log_segment` is present and names the same files as the cached segment (a mismatch
    is a miss), or
  - `expected_log_segment` is absent and some instance of that version is cached: the most
    recently built/confirmed one is returned (SPEC v8). Its `log_segment` is in the response.
  A hit issues 0 ops when every requested plan is cached.
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

`tests/local_io.rs` covers SPEC v9: cold-build op split (forwarded = QueryPlan ops only), local
FileNotFound / taint / ListingHint, a forward-mode smoke run, and the UC credential provider
against a mock UC HTTP server (endpoint choice, caching and expiry, 401/403/5xx, authz on cache
hit, token absent from trace logs of the real binary), the binary running with `SSL_CERT_FILE` /
`SSL_CERT_DIR` pointed at nothing (no system roots), and storage-option flag validation. Unit
tests in `src/creds.rs` cover the option merge (vended credentials win) and an S3 store built
with an endpoint sending a path-style, vended-key-signed request to a local mock.

`tests/loopback.rs` (L0a) starts the server in-process on `127.0.0.1:0` (default `local` IO mode;
forward-specific mechanics such as handoff of a listing op run in `forward` mode). Its test client decodes
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
