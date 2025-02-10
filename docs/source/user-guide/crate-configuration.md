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

This section contains information on how to configure DataFusion in your Rust
project. See the [Configuration Settings] section for a list of options that
control DataFusion's behavior.

[configuration settings]: configs.md

## Add latest non published DataFusion dependency

DataFusion changes are published to `crates.io` according to the [release schedule](https://github.com/apache/datafusion/blob/main/dev/release/README.md#release-process)

If you would like to test out DataFusion changes which are merged but not yet
published, Cargo supports adding dependency directly to GitHub branch:

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

## Optimized Configuration

For an optimized build several steps are required. First, use the below in your `Cargo.toml`. It is
worth noting that using the settings in the `[profile.release]` section will significantly increase the build time.

```toml
[dependencies]
datafusion = { version = "22.0" }
tokio = { version = "^1.0", features = ["rt-multi-thread"] }
snmalloc-rs = "0.3"

[profile.release]
lto = true
codegen-units = 1
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

Based on the instruction set architecture you are building on you will want to configure the `target-cpu` as well, ideally
with `native` or at least `avx2`.

```shell
RUSTFLAGS='-C target-cpu=native' cargo run --release
```

## Enable backtraces

By default Datafusion returns errors as a plain message. There is option to enable more verbose details about the error,
like error backtrace. To enable a backtrace you need to add Datafusion `backtrace` feature to your `Cargo.toml` file:

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
