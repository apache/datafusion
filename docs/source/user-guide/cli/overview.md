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

For more information, see the [Installation](installation), [Usage Guide](usage)
and [Data Sources](datasources) sections.
