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
3. Code (PR or PR Review)

In addition to submitting new PRs, we have a healthy tradition of community members helping review each other's PRs. Doing so is a great way to help the community as well as get more familiar with Rust and the relevant codebases.

You can find a curated
[good-first-issue](https://github.com/apache/arrow-datafusion/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
list to help you get started.

# Developer's guide

This section describes how you can get started at developing DataFusion.

## Windows setup

```shell
wget https://az792536.vo.msecnd.net/vms/VMBuild_20190311/VirtualBox/MSEdge/MSEdge.Win10.VirtualBox.zip
choco install -y git rustup.install visualcpp-build-tools
git-bash.exe
cargo build
```

## Bootstrap environment

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

## Test Organization

DataFusion has several levels of tests in its [Test
Pyramid](https://martinfowler.com/articles/practical-test-pyramid.html)
and tries to follow [Testing Organization](https://doc.rust-lang.org/book/ch11-03-test-organization.html) in the The Book.

This section highlights the most important test modules that exist

### Unit tests

Tests for the code in an individual module are defined in the same source file with a `test` module, following Rust convention

### Rust Integration Tests

There are several tests of the public interface of the DataFusion library in the [tests](https://github.com/apache/arrow-datafusion/tree/master/datafusion/core/tests) directory.

You can run these tests individually using a command such as

```shell
cargo test -p datafusion --tests sql_integration
```

One very important test is the [sql_integration](https://github.com/apache/arrow-datafusion/blob/master/datafusion/core/tests/sql_integration.rs) test which validates DataFusion's ability to run a large assortment of SQL queries against an assortment of data setups.

### SQL / Postgres Integration Tests

The [integration-tests](https://github.com/apache/arrow-datafusion/blob/master/datafusion/integration-tests) directory contains a harness that runs certain queries against both postgres and datafusion and compares results

#### setup environment

```shell
export POSTGRES_DB=postgres
export POSTGRES_USER=postgres
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
```

#### Install dependencies

```shell
# Install dependencies
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r integration-tests/requirements.txt

# setup environment
POSTGRES_DB=postgres POSTGRES_USER=postgres POSTGRES_HOST=localhost POSTGRES_PORT=5432 python -m pytest -v integration-tests/test_psql_parity.py

# Create
psql -d "$POSTGRES_DB" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -c 'CREATE TABLE IF NOT EXISTS test (
  c1 character varying NOT NULL,
  c2 integer NOT NULL,
  c3 smallint NOT NULL,
  c4 smallint NOT NULL,
  c5 integer NOT NULL,
  c6 bigint NOT NULL,
  c7 smallint NOT NULL,
  c8 integer NOT NULL,
  c9 bigint NOT NULL,
  c10 character varying NOT NULL,
  c11 double precision NOT NULL,
  c12 double precision NOT NULL,
  c13 character varying NOT NULL
);'

psql -d "$POSTGRES_DB" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -c "\copy test FROM '$(pwd)/testing/data/csv/aggregate_test_100.csv' WITH (FORMAT csv, HEADER true);"
```

#### Invoke the test runner

```shell
python -m pytest -v integration-tests/test_psql_parity.py
```

## Benchmarks

### Criterion Benchmarks

[Criterion](https://docs.rs/criterion/latest/criterion/index.html) is a statistics-driven micro-benchmarking framework used by DataFusion for evaluating the performance of specific code-paths. In particular, the criterion benchmarks help to both guide optimisation efforts, and prevent performance regressions within DataFusion.

Criterion integrates with Cargo's built-in [benchmark support](https://doc.rust-lang.org/cargo/commands/cargo-bench.html) and a given benchmark can be run with

```
cargo bench --bench BENCHMARK_NAME
```

A full list of benchmarks can be found [here](../../../datafusion/benches).

_[cargo-criterion](https://github.com/bheisler/cargo-criterion) may also be used for more advanced reporting._

#### Parquet SQL Benchmarks

The parquet SQL benchmarks can be run with

```
 cargo bench --bench parquet_query_sql
```

These randomly generate a parquet file, and then benchmark queries sourced from [parquet_query_sql.sql](../../../datafusion/core/benches/parquet_query_sql.sql) against it. This can therefore be a quick way to add coverage of particular query and/or data paths.

If the environment variable `PARQUET_FILE` is set, the benchmark will run queries against this file instead of a randomly generated one. This can be useful for performing multiple runs, potentially with different code, against the same source data, or for testing against a custom dataset.

The benchmark will automatically remove any generated parquet file on exit, however, if interrupted (e.g. by CTRL+C) it will not. This can be useful for analysing the particular file after the fact, or preserving it to use with `PARQUET_FILE` in subsequent runs.

### Upstream Benchmark Suites

Instructions and tooling for running upstream benchmark suites against DataFusion can be found in [benchmarks](../../../benchmarks).

These are valuable for comparative evaluation against alternative Arrow implementations and query engines.

## How to add a new scalar function

Below is a checklist of what you need to do to add a new scalar function to DataFusion:

- Add the actual implementation of the function:
  - [here](../../../datafusion/physical-expr/src/string_expressions.rs) for string functions
  - [here](../../../datafusion/physical-expr/src/math_expressions.rs) for math functions
  - [here](../../../datafusion/physical-expr/src/datetime_expressions.rs) for datetime functions
  - create a new module [here](../../../datafusion/physical-expr/src) for other functions
- In [core/src/physical_plan](../../../datafusion/core/src/physical_plan/functions.rs), add:
  - a new variant to `BuiltinScalarFunction`
  - a new entry to `FromStr` with the name of the function as called by SQL
  - a new line in `return_type` with the expected return type of the function, given an incoming type
  - a new line in `signature` with the signature of the function (number and types of its arguments)
  - a new line in `create_physical_expr`/`create_physical_fun` mapping the built-in to the implementation
  - tests to the function.
- In [core/tests/sql](../../../datafusion/core/tests/sql), add a new test where the function is called through SQL against well known data and returns the expected result.
- In [expr/src/expr_fn.rs](../../../datafusion/expr/src/expr_fn.rs), add:
  - a new entry of the `unary_scalar_expr!` macro for the new function.
- In [core/src/logical_plan/mod](../../../datafusion/core/src/logical_plan/mod.rs), add:
  - a new entry in the `pub use expr::{}` set.

## How to add a new aggregate function

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
- In [tests/sql](../../../datafusion/core/tests/sql), add a new test where the function is called through SQL against well known data and returns the expected result.

## How to display plans graphically

The query plans represented by `LogicalPlan` nodes can be graphically
rendered using [Graphviz](http://www.graphviz.org/).

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

## Specification

We formalize DataFusion semantics and behaviors through specification
documents. These specifications are useful to be used as references to help
resolve ambiguities during development or code reviews.

You are also welcome to propose changes to existing specifications or create
new specifications as you see fit.

Here is the list current active specifications:

- [Output field name semantic](https://arrow.apache.org/datafusion/specification/output-field-name-semantic.html)
- [Invariants](https://arrow.apache.org/datafusion/specification/invariants.html)

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
