<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Overview

DataFusion CLI (`datafusion-cli`) is an interactive command-line utility for executing
SQL queries against any supported data files.

While intended as an example of how to use DataFusion, `datafusion-cli` offers a
full range of SQL and support reading and writing CSV, Parquet, JSON, Arrow and
Avro, from local files, directories, or remote locations such as S3.

Here is an example of how to run a SQL query against a local file, `hits.parquet`:

```shell
$ datafusion-cli
DataFusion CLI v37.0.0
> select count(distinct "URL") from 'hits.parquet';
+----------------------------------+
| COUNT(DISTINCT hits.parquet.URL) |
+----------------------------------+
| 18342019                         |
+----------------------------------+
1 row(s) fetched.
Elapsed 1.969 seconds.
```

For more information, see the [Installation](installation.md), [Usage Guide](usage.md)
and [Data Sources](datasources.md) sections.
