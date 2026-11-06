<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Running the Spark sqllogictests on Apache Spark

`pyspark_slt_runner.py` runs the `.slt` files in
[`datafusion/sqllogictest/test_files/spark`](../../sqllogictest/test_files/spark)
on Apache Spark 4.2 and reports every query where Spark's result differs from
the expected result in the file.

The expected results in those files come from DataFusion, and nothing else
checks them against Spark. A difference means that DataFusion and Spark
disagree, or that the test expects the wrong value. This is a manual tool. It
does not run in CI.

## Setup

You need Python 3.10 or later and Java 17 or 21. Install PySpark 4.2.0 into a
virtual environment:

```shell
python3 -m venv /tmp/pyspark-4.2
/tmp/pyspark-4.2/bin/pip install -r datafusion/spark/scripts/requirements.txt
```

The runner ignores `SPARK_HOME`, so it always uses the Spark bundled with this
PySpark.

## Usage

From the repository root:

```shell
# All files
/tmp/pyspark-4.2/bin/python datafusion/spark/scripts/pyspark_slt_runner.py

# One file or directory
/tmp/pyspark-4.2/bin/python datafusion/spark/scripts/pyspark_slt_runner.py \
    datafusion/sqllogictest/test_files/spark/math/abs.slt

# Also list skipped and passing records
/tmp/pyspark-4.2/bin/python datafusion/spark/scripts/pyspark_slt_runner.py --verbose
```

Each difference is printed with its `file:line`, the query, the expected result
and Spark's result. A summary of every outcome and the most common reasons for
skipping comes at the end. The runner exits with status 1 if it found any
differences.

| Outcome                                             | Meaning                                                                  |
| --------------------------------------------------- | ------------------------------------------------------------------------ |
| results differ                                      | Spark returns different rows                                             |
| Spark raises an error                               | The file expects a result, but Spark raises an error                     |
| DataFusion expects an error, Spark returns a result | The file expects an error, but Spark does not raise one                  |
| results match, column types differ                  | The rows match, but a column's type letter (`I`, `R`, `T`, ...) does not |
| results differ only in floating-point digits        | The values differ by a relative error below 1e-12                        |
| same rows in a different order                      | Usually a query with no `ORDER BY`                                       |
| the runner failed                                   | A bug in this runner, not Spark behavior                                 |

## How queries are run

Each file starts with ANSI mode off and the session time zone set to UTC, which
are DataFusion's defaults. These settings in the files are passed on to Spark:

- `datafusion.execution.enable_ansi_mode` sets `spark.sql.ansi.enabled`
- `datafusion.execution.time_zone` sets `spark.sql.session.timeZone`
- `datafusion.spark.map_key_dedup_policy` sets `spark.sql.mapKeyDedupPolicy`

Spark 4.2 parses most of the SQL as written, including `::` casts. The runner
makes only these changes:

- `CREATE TABLE` becomes `CREATE TEMPORARY VIEW`, and `DROP TABLE` becomes
  `DROP VIEW`
- PostgreSQL type names that Spark does not accept are renamed, for example
  `FLOAT8` to `DOUBLE` and `TEXT` to `STRING`
- `column1`, `column2`, ... become `col1`, `col2`, ..., which is how Spark names
  `VALUES` columns

These records are skipped and counted, with the reason, in the summary:

- queries Spark cannot parse, such as `[1, 2]` array literals
- queries that call a function Spark does not have, such as `arrow_cast` and
  `arrow_typeof`
- queries whose string literals contain a backslash. Spark reads the backslash
  as an escape character and DataFusion does not, so the two engines would see
  different strings.
- `DECIMAL` types with a precision above 38
- queries that use a table whose `CREATE TABLE` Spark could not run
- records marked `skipif spark`, or `onlyif` another engine

## Values

Spark's results are printed the way DataFusion's sqllogictest engine prints
them, so they can be compared as text. For example, a `DOUBLE` keeps 15 decimal
places, a `DECIMAL` keeps its scale and an empty string is `(empty)`. A Spark
`TIMESTAMP` is printed in the session time zone. An expected timestamp with an
offset, such as `2024-07-15T00:00:00Z`, is converted to that time zone before
the comparison.

## Tests

The runner's parsing, translation and formatting have unit tests that need
neither Spark nor Java:

```shell
python3 -m unittest discover -s datafusion/spark/scripts
```
