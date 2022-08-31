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

# Using DataFusion as a library

## Create a new project

```shell
cargo new hello_datafusion
```

```shell
$ cd hello_datafusion
$ tree .
.
├── Cargo.toml
└── src
    └── main.rs

1 directory, 2 files
```

## Default Configuration

DataFusion is [published on crates.io](https://crates.io/crates/datafusion), and is [well documented on docs.rs](https://docs.rs/datafusion/).

To get started, add the following to your `Cargo.toml` file:

```toml
[dependencies]
datafusion = "11.0"
```

## Create a main function

Update the main.rs file with your first datafusion application based on [Example usage](https://arrow.apache.org/datafusion/user-guide/example-usage.html)

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  // register the table
  let ctx = SessionContext::new();
  ctx.register_csv("test", "<PATH_TO_YOUR_CSV_FILE>", CsvReadOptions::new()).await?;

  // create a plan to run a SQL query
  let df = ctx.sql("SELECT * FROM test").await?;

  // execute and print results
  df.show().await?;
  Ok(())
}
```

## Extensibility

DataFusion is designed to be extensible at all points. To that end, you can provide your own custom:

- [x] User Defined Functions (UDFs)
- [x] User Defined Aggregate Functions (UDAFs)
- [x] User Defined Table Source (`TableProvider`) for tables
- [x] User Defined `Optimizer` passes (plan rewrites)
- [x] User Defined `LogicalPlan` nodes
- [x] User Defined `ExecutionPlan` nodes

## Rust Version Compatibility

This crate is tested with the latest stable version of Rust. We do not currently test against other, older versions of the Rust compiler.

## Optimized Configuration

For an optimized build several steps are required. First, use the below in your `Cargo.toml`. It is
worth noting that using the settings in the `[profile.release]` section will significantly increase the build time.

```toml
[dependencies]
datafusion = { version = "11.0" , features = ["simd"]}
tokio = { version = "^1.0", features = ["rt-multi-thread"] }
snmalloc-rs = "0.2"

[profile.release]
lto = true
codegen-units = 1
```

Then, in `main.rs.` update the memory allocator with the below after your imports:

```rust
use datafusion::prelude::*;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

async fn main() -> datafusion::error::Result<()> {
  Ok(())
}
```

Finally, in order to build with the `simd` optimization `cargo nightly` is required.

```shell
rustup toolchain install nightly
```

Based on the instruction set architecture you are building on you will want to configure the `target-cpu` as well, ideally
with `native` or at least `avx2`.

```
RUSTFLAGS='-C target-cpu=native' cargo +nightly run --release
```
