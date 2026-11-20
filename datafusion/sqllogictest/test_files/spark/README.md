<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Spark Test Files

This directory contains test files for the `spark` test suite.

## RoadMap

Implementing the `datafusion-spark` compatible functions project is still a work in progress.
Many of the tests in this directory are commented out and are waiting for help with implementation.

For more information please see:

- [The `datafusion-spark` Epic](https://github.com/apache/datafusion/issues/15914)
- [Spark Test Generation Script] (https://github.com/apache/datafusion/pull/16409#issuecomment-2972618052)

## Testing Guide

When testing Spark functions:

- Functions must be tested on both `Scalar` and `Array` inputs
- Test cases should only contain `SELECT` statements with the function being tested
- Add explicit casts to input values to ensure the correct data type is used (e.g., `0::INT`)
  - Explicit casting is necessary because DataFusion and Spark do not infer data types in the same way
- If the Spark built-in function under test behaves differently in ANSI SQL mode, please wrap your test cases like this example:

```sql
statement ok
set datafusion.execution.enable_ansi_mode = true;

# Functions under test
select abs((-128)::TINYINT)

statement ok
set datafusion.execution.enable_ansi_mode = false;
```

Run `datafusion-cli --spark` to explore Spark-compatible functions
interactively. This enables the Spark SQL dialect and registers the
Spark-compatible function library for the CLI session.

### Finding Test Cases

To verify and compare function behavior at a minimum, you can refer to the following documentation sources:

1. Databricks SQL Function Reference:
   https://docs.databricks.com/aws/en/sql/language-manual/functions/NAME
2. Apache Spark SQL Function Reference:
   https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.NAME.html
3. PySpark SQL Function Reference:
   https://spark.apache.org/docs/latest/api/sql/#NAME

**Note:** Replace `NAME` in each URL with the actual function name (e.g., for the `ASCII` function, use `ascii` instead
of `NAME`).

### Scalar Example:

```sql
SELECT expm1(0::INT);
```

### Array Example:

```sql
SELECT expm1(a) FROM (VALUES (0::INT), (1::INT)) AS t(a);
```
