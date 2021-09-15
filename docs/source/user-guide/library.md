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

## Default Configuration

DataFusion is [published on crates.io](https://crates.io/crates/datafusion), and is [well documented on docs.rs](https://docs.rs/datafusion/).

To get started, add the following to your `Cargo.toml` file:

```toml
[dependencies]
datafusion = "5.1.0"
```

## Optimized Configuration

For an optimized build several steps are required. First, use the below in your `Cargo.toml`. It is
worth noting that using the settings in the `[profile.release]` section will significantly increase the build time.

```toml
[dependencies]
datafusion = { version = "5.0" , features = ["simd"]}
tokio = { version = "^1.0", features = ["macros", "rt", "rt-multi-thread"] }
snmalloc-rs = {version = "0.2", features= ["cache-friendly"]}
num_cpus = "1.0"

[profile.release]
lto = true
codegen-units = 1
```

Then, in `main.rs.` update the memory allocator with the below after your imports:

```rust
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;
```

Finally, in order to build with the `simd` optimization `cargo nightly` is required. Based on the instruction
set architecture you are building on you will want to configure the `target-cpu` as well, ideally
with `native` or at least `avx2`.

```
RUSTFLAGS='-C target-cpu=native' cargo +nightly run --release
```
