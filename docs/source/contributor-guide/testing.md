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

# Testing

Tests are critical to ensure that DataFusion is working properly and
is not accidentally broken during refactorings. All new features
should have test coverage and the entire test suite is run as part of CI.

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

## Unit tests

Tests for code in an individual module are defined in the same source file with a `test` module, following Rust convention.
The [test_util](https://github.com/apache/datafusion/tree/main/datafusion/common/src/test_util.rs) module provides useful macros to write unit tests effectively, such as `assert_batches_sorted_eq` and `assert_batches_eq` for RecordBatches and `assert_contains` / `assert_not_contains` which are used extensively in the codebase.

## sqllogictests Tests

DataFusion's SQL implementation is tested using [sqllogictest](https://github.com/apache/datafusion/tree/main/datafusion/sqllogictest) which are run like other tests using `cargo test --test sqllogictests`.

`sqllogictests` tests may be less convenient for new contributors who are familiar with writing `.rs` tests as they require learning another tool. However, `sqllogictest` based tests are much easier to develop and maintain as they 1) do not require a slow recompile/link cycle and 2) can be automatically updated via `cargo test --test sqllogictests -- --complete`.

Like similar systems such as [DuckDB](https://duckdb.org/dev/testing), DataFusion has chosen to trade off a slightly higher barrier to contribution for longer term maintainability.

DataFusion has integrated [sqlite's test suite](https://sqlite.org/sqllogictest/doc/trunk/about.wiki) as a supplemental test suite that is run whenever a PR is merged into DataFusion. To run it manually please refer to the [README](https://github.com/apache/datafusion/blob/main/datafusion/sqllogictest/README.md#running-tests-sqlite) file for instructions.

## Snapshot testing

[Insta](https://github.com/mitsuhiko/insta) is used for snapshot testing. Snapshots are generated
and compared on each test run. If the output changes, tests will fail.

To review the changes, you can use Insta CLI:

```shell
cargo install cargo-insta
cargo insta review
```

## Extended Tests

In addition to the standard CI test suite that is run on all PRs prior to merge,
DataFusion has "extended" tests (defined in [extended.yml]) that are run on each
commit to `main`. These tests rarely fail but take significantly longer to run
than the standard test suite and add important test coverage such as that the
code works when there are hash collisions as well as running the relevant
portions of the entire [sqlite test suite].

You can run the extended tests on any PR by leaving the following comment (see [example here]):

```
Run extended tests
```

[extended.yml]: https://github.com/apache/datafusion/blob/main/.github/workflows/extended.yml
[sqlite test suite]: https://www.sqlite.org/sqllogictest/dir?ci=tip
[example here]: https://github.com/apache/datafusion/pull/15427#issuecomment-2759160812

## Rust Integration Tests

There are several tests of the public interface of the DataFusion library in the [tests](https://github.com/apache/datafusion/tree/main/datafusion/core/tests) directory.

You can run these tests individually using `cargo` as normal command such as

```shell
cargo test -p datafusion --test parquet_exec
```

## SQL "Fuzz" testing

DataFusion uses the [SQLancer] for "fuzz" testing: it generates random SQL
queries and execute them against DataFusion to find bugs.

The code is in the [datafusion-sqllancer] repository, and we welcome further
contributions. Kudos to [@2010YOUY01] for the initial implementation.

[sqlancer]: https://github.com/sqlancer/sqlancer
[datafusion-sqllancer]: https://github.com/datafusion-contrib/datafusion-sqllancer
[@2010youy01]: https://github.com/2010YOUY01

## Documentation Examples

We use Rust [doctest] to verify examples from the documentation are correct and
up-to-date. These tests are run as part of our CI and you can run them them
locally with the following command:

```shell
cargo test --doc
```

### API Documentation Examples

As with other Rust projects, examples in doc comments in `.rs` files are
automatically checked to ensure they work and evolve along with the code.

### User Guide Documentation

Rust example code from the user guide (anything marked with \`\`\`rust) is also
tested in the same way using the [doc_comment] crate. See the end of
[core/src/lib.rs] for more details.

[doctest]: https://doc.rust-lang.org/rust-by-example/testing/doc_testing.html
[doc_comment]: https://docs.rs/doc-comment/latest/doc_comment
[core/src/lib.rs]: https://github.com/apache/datafusion/blob/main/datafusion/core/src/lib.rs#L583

## Benchmarks

### Criterion Benchmarks

[Criterion](https://docs.rs/criterion/latest/criterion/index.html) is a statistics-driven micro-benchmarking framework used by DataFusion for evaluating the performance of specific code-paths. In particular, the criterion benchmarks help to both guide optimisation efforts, and prevent performance regressions within DataFusion.

Criterion integrates with Cargo's built-in [benchmark support](https://doc.rust-lang.org/cargo/commands/cargo-bench.html) and a given benchmark can be run with

```
cargo bench --bench BENCHMARK_NAME
```

A full list of benchmarks can be found [here](https://github.com/apache/datafusion/tree/main/datafusion/core/benches).

_[cargo-criterion](https://github.com/bheisler/cargo-criterion) may also be used for more advanced reporting._

### Parquet SQL Benchmarks

The parquet SQL benchmarks can be run with

```
 cargo bench --bench parquet_query_sql
```

These randomly generate a parquet file, and then benchmark queries sourced from [parquet_query_sql.sql](../../../datafusion/core/benches/parquet_query_sql.sql) against it. This can therefore be a quick way to add coverage of particular query and/or data paths.

If the environment variable `PARQUET_FILE` is set, the benchmark will run queries against this file instead of a randomly generated one. This can be useful for performing multiple runs, potentially with different code, against the same source data, or for testing against a custom dataset.

The benchmark will automatically remove any generated parquet file on exit, however, if interrupted (e.g. by CTRL+C) it will not. This can be useful for analysing the particular file after the fact, or preserving it to use with `PARQUET_FILE` in subsequent runs.

### Comparing Baselines

By default, Criterion.rs will compare the measurements against the previous run (if any). Sometimes it's useful to keep a set of measurements around for several runs. For example, you might want to make multiple changes to the code while comparing against the master branch. For this situation, Criterion.rs supports custom baselines.

```
 git checkout main
 cargo bench --bench sql_planner -- --save-baseline main
 git checkout YOUR_BRANCH
 cargo bench --bench sql_planner --  --baseline main
```

Note: For MacOS it may be required to run `cargo bench` with `sudo`

```
sudo cargo bench ...
```

More information on [Baselines](https://bheisler.github.io/criterion.rs/book/user_guide/command_line_options.html#baselines)

### Upstream Benchmark Suites

Instructions and tooling for running upstream benchmark suites against DataFusion can be found in [benchmarks](https://github.com/apache/datafusion/tree/main/benchmarks).

These are valuable for comparative evaluation against alternative Arrow implementations and query engines.
