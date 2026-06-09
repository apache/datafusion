# Arrow Flight Examples

Three runnable subcommands of a single `flight` example:

| Subcommand   | What it does                                                              |
|--------------|---------------------------------------------------------------------------|
| `server`     | Minimal Arrow Flight (not Flight SQL) server, backed by DataFusion        |
| `client`     | Connects to `server`, walks a schema, runs a query                        |
| `sql_server` | Arrow Flight **SQL** server wrapping DataFusion, with `OomGuard` wired in |

## Building

```bash
cargo build --example flight
```

## Running a query against `sql_server`

Start the server in one terminal:

```bash
cargo run --example flight -- sql_server
```

Binds `0.0.0.0:50051`. Speaks the Arrow Flight SQL protocol — any Flight SQL
client works (JDBC driver, the `arrow-rs` CLI, etc.). The example accepts any
username/password during handshake and mints a Bearer token for the session.

The simplest off-the-shelf client is `arrow-rs`'s `flight_sql_client`:

```bash
git clone https://github.com/apache/arrow-rs.git
cd arrow-rs
cargo build --bin flight_sql_client --features cli,flight-sql,tls-ring
```

Then with `sql_server` running, send a query from another terminal:

```bash
./target/debug/flight_sql_client \
  --host localhost --port 50051 \
  --username admin --password admin \
  prepared-statement-query "SELECT 42 AS answer"
```

The example only implements `get_flight_info_prepared_statement`, so use
`prepared-statement-query` (not `statement-query`).

## OomGuard

`sql_server` wraps the global allocator with `OomGuard`, a `GlobalAlloc`
wrapper that converts a process-wide OOM into a per-query panic *before* the
kernel SIGKILLs the process.

### Why it exists

DataFusion's `MemoryPool` is voluntary. Operators that don't call `try_grow`
— in-flight batches, `arrow-row` decode buffers, Arrow kernel scratch space —
allocate freely past the declared budget. On a process with a hard memory
ceiling (k8s pod cgroup, ulimit), the result is an OS-level kill that takes
down every concurrent query, not just the offender.

`OomGuard` is the backstop for that case.

### How it works

1. Wraps the global allocator (`OomGuard<Jemalloc>` here, but any
   `GlobalAlloc` works).
2. A dedicated OS thread polls jemalloc's `stats.resident` every 10 ms and
   writes `BALANCE = threshold - resident` to a global atomic.
3. Each Rust allocation also debits `BALANCE` locally (per-thread drift,
   amortized into the global atomic every 64 KiB) to detect overdraft
   between poll ticks.
4. When `BALANCE` goes negative on a stamped thread (every tokio worker, via
   `on_thread_start`), the wrapper's `alloc` returns `NULL`.
5. Rust's runtime invokes the `alloc_error_hook` installed in `main.rs`. The
   hook converts the `NULL` into `panic_any(OomGuardPanic)` from a clean
   safe-Rust frame.
6. A global panic hook logs `OomGuardPanic` distinctly.
7. The panic unwinds through `df.collect()`, gets caught by the
   `catch_unwind` wrapper in `sql_server.rs`, and is converted to
   `Status::resource_exhausted` for the client. Drop chains release all
   the memory the query was holding.
8. The server keeps listening.

When `OomGuard` is disabled or no cgroup memory limit is detected, the
wrapper stays **disarmed**. Each allocation then costs one relaxed atomic
load — effectively free.

### Knobs (environment variables)

| Variable                                  | Default     | Effect                                                                                          |
|-------------------------------------------|-------------|-------------------------------------------------------------------------------------------------|
| `OOM_GUARD`                               | unset (on)  | Set to `off` to skip arming the guard. Useful for A/B comparison on the same cgroup.            |
| `OOM_GUARD_HEADROOM`                      | `0.05`      | Fraction of cgroup memory limit reserved as headroom. Clamped to `[0.0, 0.95]`. Tune as needed. |
| `DATAFUSION_EXECUTION_TARGET_PARTITIONS`  | `num_cpus`  | Standard DataFusion env var. Lower values reduce per-query peak memory on high-core hosts.      |

### Seeing it in action — A/B comparison

Run `sql_server` under a memory cap so the guard actually has something to
guard against. Unprivileged via `systemd-run`:

```bash
systemd-run --user --scope -p MemoryMax=1G -- \
  cargo run --example flight -- sql_server
```

In Docker / k8s, use `--memory=1g` / `resources.limits.memory: 1Gi`.

Pick a query that overflows the cgroup. The `GROUP BY` below on 8 million
wide-key strings allocates a multi-GiB hash table:

```sql
SELECT k, count(*) FROM (
  SELECT cast(v AS varchar) || repeat('x', 200) AS k
  FROM generate_series(1, 8000000) AS t(v)
) GROUP BY k LIMIT 5
```

**Without OomGuard** — the kernel kills the process:

```bash
OOM_GUARD=off \
DATAFUSION_EXECUTION_TARGET_PARTITIONS=1 \
systemd-run --user --scope -p MemoryMax=1G -- \
  cargo run --example flight -- sql_server &

flight_sql_client --host localhost --port 50051 \
  --username admin --password admin \
  prepared-statement-query "SELECT k, count(*) FROM (SELECT cast(v AS varchar) || repeat('x', 200) AS k FROM generate_series(1, 8000000) AS t(v)) GROUP BY k LIMIT 5"
```

Expected: client sees `transport error: broken pipe`. Server process is
gone (SIGKILL, `Failed with result 'oom-kill'`).

**With OomGuard** (default, just don't set `OOM_GUARD=off`):

```bash
DATAFUSION_EXECUTION_TARGET_PARTITIONS=1 \
systemd-run --user --scope -p MemoryMax=1G -- \
  cargo run --example flight -- sql_server &

flight_sql_client --host localhost --port 50051 \
  --username admin --password admin \
  prepared-statement-query "SELECT k, count(*) FROM (SELECT cast(v AS varchar) || repeat('x', 200) AS k FROM generate_series(1, 8000000) AS t(v)) GROUP BY k LIMIT 5"
```

Expected: client sees
```
Status: ResourceExhausted, "OomGuard: query killed before process OOM (balance=...)"
```
Server logs an `OomGuard panic on thread "tokio-rt-worker": balance=...`
line, then keeps listening. A follow-up query works:

```bash
flight_sql_client --host localhost --port 50051 \
  --username admin --password admin \
  prepared-statement-query "SELECT 'still alive' AS status"
```

returns `still alive`.

### Relationship to `MemoryPool`

`OomGuard` is complementary to `MemoryPool`, not a replacement:

- `MemoryPool` is voluntary, fine-grained, and reports `ResourcesExhausted`
  cleanly when operators that *do* call `try_grow` exceed their budget.
  Use it for graceful, expected memory pressure.
- `OomGuard` is mandatory, coarse-grained, and converts the *unexpected* —
  allocations that bypass `MemoryPool` — into a per-query panic instead of
  process death. Use it as a last line of defence.

### Caveats

The `alloc_error_hook` is currently an unstable Rust API
(`#![feature(alloc_error_hook)]`, tracking issue rust-lang/rust#51245). The
workspace `.cargo/config.toml` sets `RUSTC_BOOTSTRAP=1` to compile it on a
stable toolchain.

## Running `server` + `client`

The pair predates `sql_server` and uses the lower-level Flight protocol (not
Flight SQL).

In one terminal:

```bash
cargo run --example flight -- server
```

In another:

```bash
cargo run --example flight -- client
```
