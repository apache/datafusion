<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Testing

Tests are critical to ensure that DataFusion is working properly and
is not accidentally broken during refactorings. All new features
should have test coverage and the entire test suite is run as part of CI.

For CI workflows, documentation checks, and benchmarks, see [Non-Functional Tests](testing_non_functional.md).

## Testing Quick Start

While developing a feature or bug fix, best practice is to run the smallest set
of tests that gives confidence for your change, then expand as needed.

Initially, run the tests in the crates you changed. For example, if you made changes
to files in `datafusion-optimizer/src`, run the corresponding crate tests:

```shell
cargo test -p datafusion-optimizer
```

Then, run the `sqllogictest` suite, which provides a strong speed–coverage tradeoff for development: it runs quickly while offering broad regression coverage across most SQL behavior in DataFusion.

```shell
cargo test --profile=ci --test sqllogictests
```

Finally, before submitting a PR, run the tests for the core `datafusion` and
`datafusion-cli` crates:

```shell
cargo test -p datafusion
cargo test -p datafusion-cli
```

Some integration tests require optional external services such as Docker-backed
containers and may skip when unavailable.

## Testing Overview

DataFusion has several levels of tests in its [Test Pyramid] and tries to follow
the Rust standard [Testing Organization] described in [The Book].

Run tests using `cargo`:

```shell
cargo test
```

You can also use other runners such as [cargo-nextest].

```shell
cargo nextest run
```

[test pyramid]: https://martinfowler.com/articles/practical-test-pyramid.html
[testing organization]: https://doc.rust-lang.org/book/ch11-03-test-organization.html
[the book]: https://doc.rust-lang.org/book/
[cargo-nextest]: https://nexte.st/

### Code coverage

The [Codecov] bot posts a coverage report on GitHub pull requests with:

- `all-tests`: coverage from the workspace test suite.
- `sqllogictests`: coverage from SQLLogicTests alone.

We [prefer end-to-end tests](#choosing-what-kind-of-test-to-write) because they are
easier to maintain and verify final results through the full query pipeline,
including optimizer passes. The SLT report helps ensure good coverage from these
tests alone.

Coverage helps find missing tests; high coverage does not guarantee good tests:

- **Do:** Enumerate edge cases to build comprehensive tests, then check the
  coverage report for gaps.
- **Don't:** Use Codecov to choose a minimal set of tests that covers a new feature.

[codecov]: https://app.codecov.io/gh/apache/datafusion

## Choosing What Kind of Test to Write

When adding tests, prefer tests that exercise **user-visible behavior** over
tests of internal implementation details. Doing so means your tests are more
likely to keep working (and keep catching regressions) while the code is
refactored. In rough order of preference:

1. Use [sqllogictest](#sqllogictests-tests) (`.slt`) tests for any behavior that can
   be expressed in SQL.

2. End-to-end tests that use public APIs such as the DataFrame API, for behavior
   that cannot be expressed in SQL. For example, DataFrame tests live in
   [`datafusion/core/tests/dataframe`] and run as part of the `core_integration`
   test binary:

   ```shell
   cargo test -p datafusion --test core_integration -- dataframe
   ```

3. Rust unit tests for logic that cannot practically be reached through
   SQL or DataFrame public APIs.

[`datafusion/core/tests/dataframe`]: https://github.com/apache/datafusion/tree/main/datafusion/core/tests/dataframe

## Unit tests

Tests for code in an individual module are defined in the same source file with a `test` module, following Rust convention.

For example, to run tests in the `datafusion` crate:

```shell
cargo test -p datafusion
```

The [test_util] module provides useful macros to write unit tests effectively, such as [`assert_batches_sorted_eq`] and [`assert_batches_eq`] for RecordBatches and [`assert_contains`] / [`assert_not_contains`] which are used extensively in the codebase.

[test_util]: https://github.com/apache/datafusion/tree/main/datafusion/common/src/test_util.rs
[`assert_batches_sorted_eq`]: https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_sorted_eq.html
[`assert_batches_eq`]: https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_eq.html
[`assert_contains`]: https://docs.rs/datafusion/latest/datafusion/common/macro.assert_contains.html
[`assert_not_contains`]: https://docs.rs/datafusion/latest/datafusion/common/macro.assert_not_contains.html

## sqllogictests Tests

DataFusion's SQL implementation is tested using [sqllogictest](https://github.com/apache/datafusion/tree/main/datafusion/sqllogictest). You can run these tests with commands like:

```shell
# Run all tests
cargo test --profile=ci --test sqllogictests
# Run a specific test file
cargo test --profile=ci --test sqllogictests -- aggregate.slt
# Run a specific test file and update expected outputs
cargo test --profile=ci --test sqllogictests -- aggregate.slt --complete
# Run and update expected outputs for all test files
cargo test --profile=ci --test sqllogictests -- --complete
```

`sqllogictests` may be less convenient for new contributors who are familiar with writing `.rs` tests as they require learning another tool. However, `sqllogictest` based tests are much easier to develop and maintain as they 1) do not require a slow recompile/link cycle and 2) can be automatically updated.

Like similar systems such as [DuckDB](https://duckdb.org/dev/testing), DataFusion has chosen to trade off a slightly higher barrier to contribution for longer term maintainability.

DataFusion runs [sqlite's test suite](https://sqlite.org/sqllogictest/doc/trunk/about.wiki) in the merge queue before merging PRs into `main`. For local instructions, see [Running Tests: sqlite](https://github.com/apache/datafusion/blob/main/datafusion/sqllogictest/README.md#running-tests-sqlite).

## Snapshot testing (`cargo insta`)

[Insta](https://github.com/mitsuhiko/insta) is used for snapshot testing. Snapshots are generated
and compared on each test run. If the output changes, tests will fail.

To review the changes, you can use Insta CLI:

```shell
cargo install cargo-insta
cargo insta review
```

## Extended Tests

DataFusion has extended tests (defined in [extended.yml]) that take significantly
longer to run than the standard suite. They provide additional correctness
coverage and must pass in the merge queue before a PR merges into `main`.

To conserve CI resources, these tests do not run on ordinary PR updates. They
also run on pushes to release branches (`branch-*`). You can run them manually.

For local SQLite test instructions, see the [instructions in the documentation].

[instructions in the documentation]: https://github.com/apache/datafusion/tree/main/datafusion/sqllogictest#running-tests-sqlite
[extended.yml]: https://github.com/apache/datafusion/blob/main/.github/workflows/extended.yml

## Rust Integration Tests

There are several public interface tests for the DataFusion library in the [tests](https://github.com/apache/datafusion/tree/main/datafusion/core/tests) directory.

You can run these tests individually using `cargo` as normal command such as

```shell
cargo test -p datafusion --test parquet_integration
```

## Fuzz testing

Fuzz testing generates random inputs and configurations, then checks that a property holds for every generated case. For example, an optimized operator should produce the same results as a reference implementation.

### Fuzzing unit tests

For fuzzing unit tests, such as tests for a single operator or an internal utility:

- If the test is small (easy to set up and takes a few seconds), keep it as a unit test.
- If the test is large (requires a fuzzing driver and takes more than 10 seconds), it must implement the [`FuzzTest` trait][fuzz framework] and run through the [central fuzz-test entry point]. See the [spill-pool fuzzer] for a reference implementation.

[fuzz framework]: https://github.com/apache/datafusion/blob/main/test-utils/src/fuzz.rs
[central fuzz-test entry point]: https://github.com/apache/datafusion/blob/main/datafusion/fuzz/tests/fuzz.rs
[spill-pool fuzzer]: https://github.com/apache/datafusion/blob/main/datafusion/fuzz/tests/suites/spill_pool.rs

### SQL fuzz testing

For SQL fuzzing of more complex queries, use [datafusion-fuzzer], which is currently maintained outside the core DataFusion repository. See [its documentation][datafusion-fuzzer-docs] for details.

[datafusion-fuzzer]: https://github.com/datafusion-contrib/datafusion-fuzzer
[datafusion-fuzzer-docs]: https://github.com/datafusion-contrib/datafusion-fuzzer/blob/main/README.md

```{toctree}
:hidden:
:maxdepth: 1

testing_non_functional
```
