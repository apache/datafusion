<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Crate Configuration

This section contains information on how to configure builds of DataFusion in
your Rust project. The [Configuration Settings] section lists options that
control additional aspects DataFusion's runtime behavior.

[configuration settings]: configs.md

## Using the nightly DataFusion builds

DataFusion changes are published to `crates.io` according to the [release schedule](https://github.com/apache/datafusion/blob/main/dev/release/README.md#release-process)

If you would like to use or test versions of the DataFusion code which are
merged but not yet published, you can use Cargo's [support for adding
dependencies] directly to a GitHub branch:

```toml
datafusion = { git = "https://github.com/apache/datafusion", branch = "main"}
```

Also it works on the package level

```toml
datafusion-common = { git = "https://github.com/apache/datafusion", branch = "main", package = "datafusion-common"}
```

And with features

```toml
datafusion = { git = "https://github.com/apache/datafusion", branch = "main", default-features = false, features = ["unicode_expressions"] }
```

More on [Cargo dependencies](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#specifying-dependencies)

## Optimizing Builds

Here are several suggestions to get the Rust compler to produce faster code when
compiling DataFusion. Note that these changes may increase compile time and
binary size.

### Generate Code with CPU Specific Instructions

By default, the Rust compiler produces code that runs on a wide range of CPUs,
but may not take advantage of all the features of your specific CPU (such as
certain [SIMD instructions]). This is especially true for x86_64 CPUs, where the
default target is `x86_64-unknown-linux-gnu`, which only guarantees support for
the `SSE2` instruction set. DataFusion can benefit from the more advanced
instructions in the `AVX2` and `AVX512` to speed up operations like filtering,
aggregation, and joins. To tell the Rust compiler to use these instructions, set
the `RUSTFLAGS` environment variable to specify a more specific target CPU.

We recommend setting `target-cpu` or at least `avx2`, or preferably at least
`native` (whatever the current CPU is). For example, to build and run DataFusion
with optimizations for your current CPU:

```shell
RUSTFLAGS='-C target-cpu=native' cargo run --release
```

[simd instructions]: https://en.wikipedia.org/wiki/SIMD

### Enable Link Time Optimization / Single Codegen Unit

You can potentially improve your performance by compiling DataFusion into a
single codegen unit which gives the Rust compiler more opportunity to optimize
across crate boundaries. To do so, modify your projects' `Cargo.toml` to include
`lto = true` and `codegen-units = 1` as shown below. Beware that using a single
codegen unit _significantly_ increases `--release` build times.

```toml
[profile.release]
lto = true
codegen-units = 1
```

### Profile Guided Optimization (PGO)

Profile Guided Optimization can improve DataFusion performance by up to 25%. It works by compiling with instrumentation, running representative workloads to collect profile data, then recompiling with optimizations based on that data.

Build with instrumentation:

```shell
RUSTFLAGS="-C profile-generate=/tmp/pgo-data" cargo build --release
```

Run your workloads to collect profile data. Use benchmarks like TPCH or Clickbench, or your actual production queries:

```shell
./target/release/your-datafusion-app --benchmark
```

Rebuild using the collected profile:

```shell
RUSTFLAGS="-C profile-use=/tmp/pgo-data" cargo build --release
```

Tips:

- Use workloads that match your production patterns
- Run multiple iterations during profiling for better coverage
- Combine with LTO and CPU-specific optimizations for best results

See the [Rust compiler guide](https://rustc-dev-guide.rust-lang.org/building/optimized-build.html#profile-guided-optimization) for more details. Discussion and results in [issue #9507](https://github.com/apache/datafusion/issues/9507).

### Alternate Allocator: `snmalloc`

You can also use [snmalloc-rs](https://crates.io/crates/snmalloc-rs) crate as
the memory allocator for DataFusion to improve performance. To do so, add the
dependency to your `Cargo.toml` as shown below.

```toml
[dependencies]
snmalloc-rs = "0.3"
```

Then, in `main.rs.` update the memory allocator with the below after your imports:

<!-- Note can't include snmalloc-rs in a runnable example, because it takes over the global allocator -->

```no-run
use datafusion::prelude::*;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  Ok(())
}
```

## Enable Backtraces

By default, Datafusion returns errors as a plain text message. You can enable more verbose details about the error,
such as backtraces by enabling the `backtrace` feature to your `Cargo.toml` file like this:

```toml
datafusion = { version = "31.0.0", features = ["backtrace"]}
```

Set environment [variables](https://doc.rust-lang.org/std/backtrace/index.html#environment-variables)

```bash
RUST_BACKTRACE=1 ./target/debug/datafusion-cli
DataFusion CLI v31.0.0
> select row_numer() over (partition by a order by a) from (select 1 a);
Error during planning: Invalid function 'row_numer'.
Did you mean 'ROW_NUMBER'?

backtrace:    0: std::backtrace_rs::backtrace::libunwind::trace
             at /rustc/5680fa18feaa87f3ff04063800aec256c3d4b4be/library/std/src/../../backtrace/src/backtrace/libunwind.rs:93:5
   1: std::backtrace_rs::backtrace::trace_unsynchronized
             at /rustc/5680fa18feaa87f3ff04063800aec256c3d4b4be/library/std/src/../../backtrace/src/backtrace/mod.rs:66:5
   2: std::backtrace::Backtrace::create
             at /rustc/5680fa18feaa87f3ff04063800aec256c3d4b4be/library/std/src/backtrace.rs:332:13
   3: std::backtrace::Backtrace::capture
             at /rustc/5680fa18feaa87f3ff04063800aec256c3d4b4be/library/std/src/backtrace.rs:298:9
   4: datafusion_common::error::DataFusionError::get_back_trace
             at /datafusion/datafusion/common/src/error.rs:436:30
   5: datafusion_sql::expr::function::<impl datafusion_sql::planner::SqlToRel<S>>::sql_function_to_expr
   ............
```

The backtraces are useful when debugging code. If there is a test in `datafusion/core/src/physical_planner.rs`

```rust
#[tokio::test]
async fn test_get_backtrace_for_failed_code() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "
    select row_numer() over (partition by a order by a) from (select 1 a);
    ";

    let _ = ctx.sql(sql).await?.collect().await?;

    Ok(())
}
```

To obtain a backtrace:

```bash
cargo build --features=backtrace
RUST_BACKTRACE=1 cargo test --features=backtrace --package datafusion --lib -- physical_planner::tests::test_get_backtrace_for_failed_code --exact --nocapture

running 1 test
Error: Plan("Invalid function 'row_numer'.\nDid you mean 'ROW_NUMBER'?\n\nbacktrace:    0: std::backtrace_rs::backtrace::libunwind::trace\n             at /rustc/129f3b9964af4d4a709d1383930ade12dfe7c081/library/std/src/../../backtrace/src/backtrace/libunwind.rs:105:5\n   1: std::backtrace_rs::backtrace::trace_unsynchronized\n...
```

Note: The backtrace wrapped into systems calls, so some steps on top of the backtrace can be ignored

To show the backtrace in a pretty-printed format use `eprintln!("{e}");`.

```rust
#[tokio::test]
async fn test_get_backtrace_for_failed_code() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select row_numer() over (partition by a order by a) from (select 1 a);";

    let _ = match ctx.sql(sql).await {
        Ok(result) => result.show().await?,
        Err(e) => {
            eprintln!("{e}");
        }
    };

    Ok(())
}
```

Then run the test:

```bash
$ RUST_BACKTRACE=1 cargo test --features=backtrace --package datafusion --lib -- physical_planner::tests::test_get_backtrace_for_failed_code --exact --nocapture

running 1 test
Error during planning: Invalid function 'row_numer'.
Did you mean 'ROW_NUMBER'?

backtrace:    0: std::backtrace_rs::backtrace::libunwind::trace
             at /rustc/129f3b9964af4d4a709d1383930ade12dfe7c081/library/std/src/../../backtrace/src/backtrace/libunwind.rs:105:5
   1: std::backtrace_rs::backtrace::trace_unsynchronized
             at /rustc/129f3b9964af4d4a709d1383930ade12dfe7c081/library/std/src/../../backtrace/src/backtrace/mod.rs:66:5
   2: std::backtrace::Backtrace::create
             at /rustc/129f3b9964af4d4a709d1383930ade12dfe7c081/library/std/src/backtrace.rs:331:13
   3: std::backtrace::Backtrace::capture
   ...
```
