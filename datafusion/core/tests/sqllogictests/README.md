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

#### Overview

This is the Datafusion implementation of [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki). We
use [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) as a parser/runner of `.slt` files
in [`test_files`](test_files).

#### Running tests: TLDR Examples

```shell
# Run all tests
cargo test -p datafusion --test sqllogictests
```

```shell
# Run all tests, with debug logging enabled
RUST_LOG=debug cargo test -p datafusion --test sqllogictests
```

```shell
# Run only the tests in `information_schema.slt`
cargo test -p datafusion --test sqllogictests -- information_schema
```

```shell
# Automatically update ddl.slt with expected output
cargo test -p datafusion --test sqllogictests -- ddl --complete
```

```shell
# Run ddl.slt, printing debug logging to stdout
RUST_LOG=debug cargo test -p datafusion --test sqllogictests -- ddl
```

#### Cookbook: Adding Tests

1. Add queries

Add the setup and queries you want to run to a `.slt` file
(`my_awesome_test.slt` in this example) using the following format:

```text
query
CREATE TABLE foo AS VALUES (1);

query
SELECT * from foo;
```

2. Fill in expected answers with `--complete` mode

Running the following command will update `my_awesome_test.slt` with the expected output:

```shell
cargo test -p datafusion --test sqllogictests -- my_awesome_test --complete
```

3. Verify the content

In the case above, `my_awesome_test.slt` will look something like

```
statement ok
CREATE TABLE foo AS VALUES (1);

query I
SELECT * from foo;
----
1
```

Assuming it looks good, check it in!

#### Reference

#### Running tests: Validation Mode

In this model, `sqllogictests` runs the statements and queries in a `.slt` file, comparing the expected output in the
file to the output produced by that run.

For example, to run all tests suites in validation mode

```shell
cargo test -p datafusion --test sqllogictests
```

sqllogictests also supports `cargo test` style substring matches on file names to restrict which tests to run

```shell
# information_schema.slt matches due to substring matching `information`
cargo test -p datafusion --test sqllogictests -- information
```

#### Running tests: Postgres compatibility

Test files that start with prefix `pg_compat_` verify compatibility
with Postgres by running the same script files both with DataFusion and with Posgres

In order to run the sqllogictests running against a previously running Postgres instance, do:

```shell
PG_COMPAT=true PG_URI="postgresql://postgres@127.0.0.1/postgres" cargo test -p datafusion --test sqllogictests
```

The environemnt variables:

1. `PG_COMPAT` instructs sqllogictest to run against Postgres (not DataFusion)
2. `PG_URI` contains a `libpq` style connection string, whose format is described in
   [the docs](https://docs.rs/tokio-postgres/latest/tokio_postgres/config/struct.Config.html#url)

One way to create a suitable a posgres container in docker is to use
the [Official Image](https://hub.docker.com/_/postgres) with a command
such as the following. Note the collation **must** be set to `C` otherwise
`ORDER BY` will not match DataFusion and the tests will diff.

```shell
docker run \
  -p5432:5432 \
  -e POSTGRES_INITDB_ARGS="--encoding=UTF-8 --lc-collate=C --lc-ctype=C" \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  postgres
```

#### Running Tests: `tpch`

Test files in `tpch` directory runs against the `TPCH` data set (SF =
0.1), which must be generated before running. You can use following
command to generate tpch data, assuming you are in the repository
root:

```shell
docker run -it \
  -v "$(realpath datafusion/core/tests/sqllogictests/test_files/tpch/data)":/data \
  ghcr.io/databloom-ai/tpch-docker:main -vf -s 0.1
```

Then you need to add `INCLUDE_TPCH=true` to run tpch tests:

```shell
INCLUDE_TPCH=true cargo test -p datafusion --test sqllogictests
```

#### Updating tests: Completion Mode

In test script completion mode, `sqllogictests` reads a prototype script and runs the statements and queries against the
database engine. The output is a full script that is a copy of the prototype script with result inserted.

You can update the tests / generate expected output by passing the `--complete` argument.

```shell
# Update ddl.slt with output from running
cargo test -p datafusion --test sqllogictests -- ddl --complete
```

#### sqllogictests

sqllogictest is a program originally written for SQLite to verify the correctness of SQL queries against the SQLite
engine. The program is engine-agnostic and can parse sqllogictest files (`.slt`), runs queries against an SQL engine and
compare the output to the expected output.

Tests in the `.slt` file are a sequence of query record generally starting with `CREATE` statements to populate tables
and then further queries to test the populated data (arrow-datafusion exception).

Each `.slt` file runs in its own, isolated `SessionContext`, to make the test setup explicit and so they can run in
parallel. Thus it important to keep the tests from having externally visible side effects (like writing to a global
location such as `/tmp/`)

Query records follow the format:

```sql
# <test_name>
query <type_string> <sort_mode>
<sql_query>
----
<expected_result>
```

- `test_name`: Uniquely identify the test name (arrow-datafusion only)
- `type_string`: A short string that specifies the number of result columns and the expected datatype of each result
  column. There is one character in the <type_string> for each result column. The characters codes are:
  - 'B' - **B**oolean,
  - 'D' - **D**atetime,
  - 'I' - **I**nteger,
  - 'P' - timestam**P**,
  - 'R' - floating-point results,
  - 'T' - **T**ext,
  - "?" - any other types
- `expected_result`: In the results section, some values are converted according to some rules:
  - floating point values are rounded to the scale of "12",
  - NULL values are rendered as `NULL`,
  - empty strings are rendered as `(empty)`,
  - boolean values are rendered as `true`/`false`,
  - this list can be not exhaustive, check the `datafusion/core/tests/sqllogictests/src/engines/conversion.rs` for
    details.
- `sort_mode`: If included, it must be one of `nosort` (**default**), `rowsort`, or `valuesort`. In `nosort` mode, the
  results appear in exactly the order in which they were received from the database engine. The `nosort` mode should
  only be used on queries that have an `ORDER BY` clause or which only have a single row of result, since otherwise the
  order of results is undefined and might vary from one database engine to another. The `rowsort` mode gathers all
  output from the database engine then sorts it by rows on the client side. Sort comparisons
  use [sort_unstable](https://doc.rust-lang.org/std/primitive.slice.html#method.sort_unstable) on the rendered text
  representation of the values. Hence, "9" sorts after "10", not before. The `valuesort` mode works like `rowsort`
  except that it does not honor row groupings. Each individual result value is sorted on its own.

> :warning: It is encouraged to either apply `order by`, or use `rowsort` for queries without explicit `order by`
> clauses.

##### Example

```sql
# group_by_distinct
query TTI
SELECT a, b, COUNT(DISTINCT c) FROM my_table GROUP BY a, b ORDER BY a, b
----
foo bar 10
foo baz 5
foo     4
        3
```
