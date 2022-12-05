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

This is the Datafusion implementation of [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki). We use [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) as a parser/runner of `.slt` files in `test_files`.

#### Running tests

```shell
cargo test -p datafusion --test sqllogictests
```

Run tests with debug logging enabled:

```shell
RUST_LOG=debug cargo test -p datafusion --test sqllogictests
```

Run only the tests in `information_schema.slt`:

```shell
# information_schema.slt matches due to substring matching `information`
cargo test -p datafusion --test sqllogictests -- information
```

#### sqllogictests

> :warning: **Warning**:Datafusion's sqllogictest implementation and migration is still in progress. Definitions taken from https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

sqllogictest is a program originally written for SQLite to verify the correctness of SQL queries against the SQLite engine. The program is engine-agnostic and can parse sqllogictest files (`.slt`), runs queries against an SQL engine and compare the output to the expected output.

Tests in the `.slt` file are a sequence of query record generally starting with `CREATE` statements to populate tables and then further queries to test the populated data (arrow-datafusion exception).

Query records follow the format:

```sql
# <test_name>
query <type_string> <sort_mode> <label>
<sql_query>
----
<expected_result>
```

- `test_name`: Uniquely identify the test name (arrow-datafusion only)
- `type_string`: A short string that specifies the number of result columns and the expected datatype of each result column. There is one character in the <type_string> for each result column. The characters codes are "T" for a text result, "I" for an integer result, and "R" for a floating-point result.
- (Optional) `label`: sqllogictest stores a hash of the results of this query under the given label. If the label is reused, then sqllogictest verifies that the results are the same. This can be used to verify that two or more queries in the same test script that are logically equivalent always generate the same output.
- `expected_result`: In the results section, integer values are rendered as if by printf("%d"). Floating point values are rendered as if by printf("%.3f"). NULL values are rendered as "NULL". Empty strings are rendered as "(empty)". Within non-empty strings, all control characters and unprintable characters are rendered as "@".
- `sort_mode`: If included, it must be one of "nosort", "rowsort", or "valuesort". The default is "nosort". In nosort mode, the results appear in exactly the order in which they were received from the database engine. The nosort mode should only be used on queries that have an ORDER BY clause or which only have a single row of result, since otherwise the order of results is undefined and might vary from one database engine to another. The "rowsort" mode gathers all output from the database engine then sorts it by rows on the client side. Sort comparisons use strcmp() on the rendered ASCII text representation of the values. Hence, "9" sorts after "10", not before. The "valuesort" mode works like rowsort except that it does not honor row groupings. Each individual result value is sorted on its own.

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
