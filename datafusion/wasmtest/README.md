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

# DataFusion wasmtest

[DataFusion][df] is an extensible query execution framework, written in Rust, that uses Apache Arrow as its in-memory format.

This crate is a submodule of DataFusion used to verify that various DataFusion crates compile successfully to the
`wasm32-unknown-unknown` target with wasm-pack.

[df]: https://crates.io/crates/datafusion

## wasmtest

Some of DataFusion's downstream projects compile to WASM to run in the browser. Doing so requires special care that certain library dependencies are not included in DataFusion.

## Setup

First, [install wasm-pack](https://rustwasm.github.io/wasm-pack/installer/)

Then use wasm-pack to compile the crate from within this directory

```shell
wasm-pack build
```

## Try it out

The `datafusion-wasm-app` directory contains a simple app (created with [`create-wasm-app`](https://github.com/rustwasm/create-wasm-app) and then manually updated to WebPack 5) that invokes DataFusion and writes results to the browser console.

From within the `datafusion/wasmtest/datafusion-wasm-app` directory:

```shell
npm install
npm run start
```

Then open http://localhost:8080/ in a web browser and check the console to see the results of using various DataFusion crates.

## Test

This crate uses `wasm-pack test` to run tests. Try it out with one of the following commands depending on your browser of choice:

```shell
wasm-pack test --firefox
wasm-pack test --chrome
wasm-pack test --safari
```

To run the tests in headless mode, add the `--headless` flag:

```shell
wasm-pack test --headless --firefox
wasm-pack test --headless --chrome
wasm-pack test --headless --safari
```

**Note:** In GitHub Actions we test the compilation with `wasm-build`, but we don't currently invoke `wasm-pack test`. This is because the headless mode is not yet working. Document of adding a GitHub Action job: https://rustwasm.github.io/docs/wasm-bindgen/wasm-bindgen-test/continuous-integration.html#github-actions.

To tweak timeout setting, use `WASM_BINDGEN_TEST_TIMEOUT` environment variable. E.g., `WASM_BINDGEN_TEST_TIMEOUT=300 wasm-pack test --firefox --headless`.

## Compatibility

The following DataFusion crates are verified to work in a wasm-pack environment using the default `wasm32-unknown-unknown` target:

- `datafusion` (datafusion-core) with default-features disabled to remove `bzip2-sys` from `async-compression`
- `datafusion-common` with default-features disabled to remove the `parquet` dependency (see below)
- `datafusion-expr`
- `datafusion-execution`
- `datafusion-optimizer`
- `datafusion-physical-expr`
- `datafusion-physical-plan`
- `datafusion-sql`

The difficulty with getting the remaining DataFusion crates compiled to WASM is that they have non-optional dependencies on the [`parquet`](https://docs.rs/crate/parquet/) crate with its default features enabled. Several of the default parquet crate features require native dependencies that are not compatible with WASM, in particular the `lz4` and `zstd` features. If we can arrange our feature flags to make it possible to depend on parquet with these features disabled, then it should be possible to compile the core `datafusion` crate to WASM as well.
