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

## Rust Version Compatibility

This crate is tested with the latest stable version of Rust. We do not currently test against other, older versions of the Rust compiler.

## Documentation

DataFusion is [published on crates.io](https://crates.io/crates/datafusion), and is [well documented on docs.rs](https://docs.rs/datafusion/).
You can also reference the simple examples in [Example Usage](./example-usage.md).

## Dependencies
To get started, add the following to your `Cargo.toml` file:

```toml
[dependencies]
datafusion = "10"
tokio = "1"
```

## Features
TODO: some documentation of the features that can be enabled.

## Optimized Builds

For an optimized build several steps are required. First, use the below in your `Cargo.toml`. It is
worth noting that using the settings in the `[profile.release]` section will significantly increase the build time.

```toml
[dependencies]
datafusion = { version = "10" , features = ["simd"]}
tokio = { version = "^1.0", features = ["rt-multi-thread"] }
snmalloc-rs = "0.3"

[profile.release]
lto = true
codegen-units = 1
```

Then, in `main.rs` update the memory allocator with the below after your imports:

```shell
use datafusion::prelude::*;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
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

```shell
RUSTFLAGS='-C target-cpu=native' cargo +nightly run --release
```
