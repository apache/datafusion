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

# DataFusion xtask

This directory contains DataFusion's project automation implemented using the
[`xtask` pattern](https://github.com/matklad/cargo-xtask).

## Usage

Run the following command from the repository root for the available commands,
arguments, and examples:

```shell
cargo xtask help
```

## CI runner

`cargo xtask ci [args]` domain is used for organizing commands used in GitHub CI.

For example, `cargo xtask ci step test workspace` runs DataFusion's default CI test suite. Developers can run the same command to reproduce that GitHub Actions step locally. Append `--explain` to display the underlying command without running it.

The goal is to keep local development and GitHub CI runs in sync. Each CI step command is defined once in this in-repository Rust binary and is invoked the same way locally and in GitHub Actions.

Tracking issue: https://github.com/apache/datafusion/issues/24487

### Extended test suites

The [extended tests] are long-running suites that run in the merge queue, on
pushes to release branches, and on manual dispatch. They do not run on ordinary
pull request updates. Each job in [`extended.yml`] runs one of these commands:

| GitHub Actions job                             | Command                                    |
| ---------------------------------------------- | ------------------------------------------ |
| `cargo test 'extended_tests' (amd64)`          | `cargo xtask ci step test extended`        |
| `cargo test hash collisions (amd64)`           | `cargo xtask ci step test hash-collisions` |
| `Run sqllogictests with the sqlite test suite` | `cargo xtask ci step test sqlite`          |

Append `--explain` to print the `cargo test` invocation, its working directory,
and the environment variables the command sets, without running the suite:

```shell
cargo xtask ci step test extended --explain
cargo xtask ci step test hash-collisions --explain
cargo xtask ci step test sqlite --explain
```

Each command sets only the environment variables that its test suite needs.
Everything else is inherited from the calling shell:

- `extended` sets `RUST_BACKTRACE=1` and `DATAFUSION_SPILL_POOL_FUZZ_ITERATIONS=1000`.
  The second variable runs more random spill pool fuzzer scenarios than the
  default test suite does.
- `hash-collisions` runs from the `datafusion` directory and sets
  `RUST_BACKTRACE=1`, which matches the CI builder setup for that job.
- `sqlite` sets no variables. Its CI job deliberately skips the builder setup
  because backtraces make this suite much slower. A `RUST_BACKTRACE` value
  inherited from your shell therefore makes a local run differ from CI.

#### Prerequisites

- The Rust toolchain from `rust-toolchain.toml` and the Protobuf compiler
  (`protoc`). See the [development environment] guide.
- The test data submodules. The `sqlite` suite reads
  `datafusion-testing/data/sqlite`:

  ```shell
  git submodule update --init --recursive
  ```

- Time and disk space. These suites build most of the workspace with test
  features enabled, and the `sqlite` suite runs several million queries.

#### What a test command does not do

A test command reproduces one `cargo test` invocation from a CI job. It is not
the complete job. The GitHub Actions workflow still installs the toolchain,
configures build flags, caching, and network settings, verifies that the
working tree is clean, and runs `cargo clean`. The commands never run cleanup
and never call the GitHub API, so they are safe to run in a local checkout.

[extended tests]: https://github.com/apache/datafusion/blob/main/docs/source/contributor-guide/testing.md#extended-tests
[`extended.yml`]: https://github.com/apache/datafusion/blob/main/.github/workflows/extended.yml
[development environment]: https://github.com/apache/datafusion/blob/main/docs/source/contributor-guide/development_environment.md
