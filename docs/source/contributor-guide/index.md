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

# Introduction

We welcome and encourage contributions of all kinds, such as:

1. Tickets with issue reports of feature requests
2. Documentation improvements
3. Code, both PR and (especially) PR Review.

In addition to submitting new PRs, we have a healthy tradition of community members reviewing each other's PRs. Doing so is a great way to help the community as well as get more familiar with Rust and the relevant codebases.

You can find a curated
[good-first-issue](https://github.com/apache/arrow-datafusion/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
list to help you get started.

# Developer's guide

## Pull Request Overview

We welcome pull requests (PRs) from anyone from the community.

DataFusion is a very active fast-moving project and we try to review and merge PRs quickly to keep the review backlog down and the pace up. After review and approval, one of the [many people with commit access](https://arrow.apache.org/committers/) will merge your PR.

Review bandwidth is currently our most limited resource, and we highly encourage reviews by the broader community. If you are waiting for your PR to be reviewed, consider helping review other PRs that are waiting. Such review both helps the reviewer to learn the codebase and become more expert, as well as helps identify issues in the PR (such as lack of test coverage), that can be addressed and make future reviews faster and more efficient.

## Creating Pull Requests

We recommend splitting your contributions into smaller PRs rather than large PRs (500+ lines) because:

1. The PR is more likely to be reviewed quickly -- our reviewers struggle to find the contiguous time needed to review large PRs.
2. The PR discussions tend to be more focused and less likely to get lost among several different threads.
3. It is often easier to accept and act on feedback when it comes early on in a small change, before a particular approach has been polished too much.

If you are concerned that a larger design will be lost in a string of small PRs, creating a large draft PR that shows how they all work together can help.

# Reviewing Pull Requests

When reviewing PRs, please remember our primary goal is to improve DataFusion and its community together. PR feedback should be constructive with the aim to help improve the code as well as the understanding of the contributor.

Please ensure any issues you raise contains a rationale and suggested alternative -- it is frustrating to be told "don't do it this way" without any clear reason or alternate provided.

Some things to specifically check:

1. Is the feature or fix covered sufficiently with tests (see `Test Organization` below)?
2. Is the code clear, and fits the style of the existing codebase?

## "Major" and "Minor" PRs

Since we are a worldwide community, we have contributors in many timezones who review and comment. To ensure anyone who wishes has an opportunity to review a PR, our committers try to ensure that at least 24 hours passes between when a "major" PR is approved and when it is merged.

A "major" PR means there is a substantial change in design or a change in the API. Committers apply their best judgment to determine what constitutes a substantial change. A "minor" PR might be merged without a 24 hour delay, again subject to the judgment of the committer. Examples of potential "minor" PRs are:

1. Documentation improvements/additions
2. Small bug fixes
3. Non-controversial build-related changes (clippy, version upgrades etc.)
4. Smaller non-controversial feature additions

The good thing about open code and open development is that any issues in one change can almost always be fixed with a follow on PR.

## Getting Started

This section describes how you can get started at developing DataFusion.

### Windows setup

```shell
wget https://az792536.vo.msecnd.net/vms/VMBuild_20190311/VirtualBox/MSEdge/MSEdge.Win10.VirtualBox.zip
choco install -y git rustup.install visualcpp-build-tools
git-bash.exe
cargo build
```

### Protoc Installation

Compiling DataFusion from sources requires an installed version of the protobuf compiler, `protoc`.

On most platforms this can be installed from your system's package manager

```
$ apt install -y protobuf-compiler
$ dnf install -y protobuf-devel
$ pacman -S protobuf
$ brew install protobuf
```

You will want to verify the version installed is `3.12` or greater, which introduced support for explicit [field presence](https://github.com/protocolbuffers/protobuf/blob/v3.12.0/docs/field_presence.md). Older versions may fail to compile.

```shell
$ protoc --version
libprotoc 3.12.4
```

Alternatively a binary release can be downloaded from the [Release Page](https://github.com/protocolbuffers/protobuf/releases) or [built from source](https://github.com/protocolbuffers/protobuf/blob/main/src/README.md).

### Bootstrap environment

DataFusion is written in Rust and it uses a standard rust toolkit:

- `cargo build`
- `cargo fmt` to format the code
- `cargo test` to test
- etc.

Testing setup:

- `rustup update stable` DataFusion uses the latest stable release of rust
- `git submodule init`
- `git submodule update`

Formatting instructions:

- [ci/scripts/rust_fmt.sh](../../../ci/scripts/rust_fmt.sh)
- [ci/scripts/rust_clippy.sh](../../../ci/scripts/rust_clippy.sh)
- [ci/scripts/rust_toml_fmt.sh](../../../ci/scripts/rust_toml_fmt.sh)

or run them all at once:

- [dev/rust_lint.sh](../../../dev/rust_lint.sh)

## Testing

Tests are critical to ensure that DataFusion is working properly and
is not accidentally broken during refactorings. All new features
should have test coverage.

DataFusion has several levels of tests in its [Test
Pyramid](https://martinfowler.com/articles/practical-test-pyramid.html)
and tries to follow the Rust standard [Testing Organization](https://doc.rust-lang.org/book/ch11-03-test-organization.html) in the The Book.

### Unit tests

Tests for code in an individual module are defined in the same source file with a `test` module, following Rust convention.

### sqllogictests Tests

DataFusion's SQL implementation is tested using [sqllogictest](https://github.com/apache/arrow-datafusion/tree/main/datafusion/core/tests/sqllogictests) which are run like any other Rust test using `cargo test --test sqllogictests`.

`sqllogictests` tests may be less convenient for new contributors who are familiar with writing `.rs` tests as they require learning another tool. However, `sqllogictest` based tests are much easier to develop and maintain as they 1) do not require a slow recompile/link cycle and 2) can be automatically updated via `cargo test --test sqllogictests -- --complete`.

Like similar systems such as [DuckDB](https://duckdb.org/dev/testing), DataFusion has chosen to trade off a slightly higher barrier to contribution for longer term maintainability. While we are still in the process of [migrating some old sql_integration tests](https://github.com/apache/arrow-datafusion/issues/6195), all new tests should be written using sqllogictests if possible.

### Rust Integration Tests

There are several tests of the public interface of the DataFusion library in the [tests](https://github.com/apache/arrow-datafusion/tree/main/datafusion/core/tests) directory.

You can run these tests individually using `cargo` as normal command such as

```shell
cargo test -p datafusion --test dataframe
```

## Benchmarks

### Criterion Benchmarks

[Criterion](https://docs.rs/criterion/latest/criterion/index.html) is a statistics-driven micro-benchmarking framework used by DataFusion for evaluating the performance of specific code-paths. In particular, the criterion benchmarks help to both guide optimisation efforts, and prevent performance regressions within DataFusion.

Criterion integrates with Cargo's built-in [benchmark support](https://doc.rust-lang.org/cargo/commands/cargo-bench.html) and a given benchmark can be run with

```
cargo bench --bench BENCHMARK_NAME
```

A full list of benchmarks can be found [here](https://github.com/apache/arrow-datafusion/tree/main/datafusion/core/benches).

_[cargo-criterion](https://github.com/bheisler/cargo-criterion) may also be used for more advanced reporting._

### Parquet SQL Benchmarks

The parquet SQL benchmarks can be run with

```
 cargo bench --bench parquet_query_sql
```

These randomly generate a parquet file, and then benchmark queries sourced from [parquet_query_sql.sql](../../../datafusion/core/benches/parquet_query_sql.sql) against it. This can therefore be a quick way to add coverage of particular query and/or data paths.

If the environment variable `PARQUET_FILE` is set, the benchmark will run queries against this file instead of a randomly generated one. This can be useful for performing multiple runs, potentially with different code, against the same source data, or for testing against a custom dataset.

The benchmark will automatically remove any generated parquet file on exit, however, if interrupted (e.g. by CTRL+C) it will not. This can be useful for analysing the particular file after the fact, or preserving it to use with `PARQUET_FILE` in subsequent runs.

### Upstream Benchmark Suites

Instructions and tooling for running upstream benchmark suites against DataFusion can be found in [benchmarks](https://github.com/apache/arrow-datafusion/tree/main/benchmarks).

These are valuable for comparative evaluation against alternative Arrow implementations and query engines.

## HOWTOs

### How to add a new scalar function

Below is a checklist of what you need to do to add a new scalar function to DataFusion:

- Add the actual implementation of the function:
  - [here](../../../datafusion/physical-expr/src/string_expressions.rs) for string functions
  - [here](../../../datafusion/physical-expr/src/math_expressions.rs) for math functions
  - [here](../../../datafusion/physical-expr/src/datetime_expressions.rs) for datetime functions
  - create a new module [here](../../../datafusion/physical-expr/src) for other functions
- In [physical-expr/src](../../../datafusion/physical-expr/src/functions.rs), add:
  - a new variant to `BuiltinScalarFunction`
  - a new entry to `FromStr` with the name of the function as called by SQL
  - a new line in `return_type` with the expected return type of the function, given an incoming type
  - a new line in `signature` with the signature of the function (number and types of its arguments)
  - a new line in `create_physical_expr`/`create_physical_fun` mapping the built-in to the implementation
  - tests to the function.
- In [core/tests/sqllogictests/test_files](../../../datafusion/core/tests/sqllogictests/test_files), add new `sqllogictest` integration tests where the function is called through SQL against well known data and returns the expected result.
  - Documentation for `sqllogictest` [here](../../../datafusion/sqllogictest/README.md)
- In [expr/src/expr_fn.rs](../../../datafusion/expr/src/expr_fn.rs), add:
  - a new entry of the `unary_scalar_expr!` macro for the new function.
- Add SQL reference documentation [here](../../../docs/source/user-guide/sql/scalar_functions.md)

### How to add a new aggregate function

Below is a checklist of what you need to do to add a new aggregate function to DataFusion:

- Add the actual implementation of an `Accumulator` and `AggregateExpr`:
  - [here](../../../datafusion/physical-expr/src/string_expressions.rs) for string functions
  - [here](../../../datafusion/physical-expr/src/math_expressions.rs) for math functions
  - [here](../../../datafusion/physical-expr/src/datetime_expressions.rs) for datetime functions
  - create a new module [here](../../../datafusion/physical-expr/src) for other functions
- In [datafusion/expr/src](../../../datafusion/expr/src/aggregate_function.rs), add:
  - a new variant to `AggregateFunction`
  - a new entry to `FromStr` with the name of the function as called by SQL
  - a new line in `return_type` with the expected return type of the function, given an incoming type
  - a new line in `signature` with the signature of the function (number and types of its arguments)
  - a new line in `create_aggregate_expr` mapping the built-in to the implementation
  - tests to the function.
- In [core/tests/sqllogictests/test_files](../../../datafusion/core/tests/sqllogictests/test_files), add new `sqllogictest` integration tests where the function is called through SQL against well known data and returns the expected result.
  - Documentation for `sqllogictest` [here](../../../datafusion/core/tests/sqllogictests/README.md)
- Add SQL reference documentation [here](../../../docs/source/user-guide/sql/aggregate_functions.md)

### How to display plans graphically

The query plans represented by `LogicalPlan` nodes can be graphically
rendered using [Graphviz](https://www.graphviz.org/).

To do so, save the output of the `display_graphviz` function to a file.:

```rust
// Create plan somehow...
let mut output = File::create("/tmp/plan.dot")?;
write!(output, "{}", plan.display_graphviz());
```

Then, use the `dot` command line tool to render it into a file that
can be displayed. For example, the following command creates a
`/tmp/plan.pdf` file:

```bash
dot -Tpdf < /tmp/plan.dot > /tmp/plan.pdf
```

## Specifications

We formalize some DataFusion semantics and behaviors through specification
documents. These specifications are useful to be used as references to help
resolve ambiguities during development or code reviews.

You are also welcome to propose changes to existing specifications or create
new specifications as you see fit.

Here is the list current active specifications:

- [Output field name semantic](https://arrow.apache.org/datafusion/contributor-guide/specification/output-field-name-semantic.html)
- [Invariants](https://arrow.apache.org/datafusion/contributor-guide/specification/invariants.html)

All specifications are stored in the `docs/source/specification` folder.

## How to format `.md` document

We are using `prettier` to format `.md` files.

You can either use `npm i -g prettier` to install it globally or use `npx` to run it as a standalone binary. Using `npx` required a working node environment. Upgrading to the latest prettier is recommended (by adding `--upgrade` to the `npm` command).

```bash
$ prettier --version
2.3.0
```

After you've confirmed your prettier version, you can format all the `.md` files:

```bash
prettier -w {datafusion,datafusion-cli,datafusion-examples,dev,docs}/**/*.md
```
