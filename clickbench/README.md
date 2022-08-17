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

# Clickbench Benchmark

## Introduction

<https://benchmark.clickhouse.com/> is a new benchmark set provided by ClickHouse. The current result for DataFusion is from Azure's `v16s_v2` VM


## The way to reproduce

1. `git clone git@github.com:ClickHouse/ClickBench.git`
2. cd ClickBench
3. `bash run.sh`

Note that this will output the needed result like the `result` field in <https://github.com/ClickHouse/ClickBench/blob/main/datafusion/results/f16s_v2.json>. We need to manually provide `system`, `date`, ... for now, and save a new json into `results` folder. After merged by ClickBench and new html files regenerated, it'll be shown in <https://benchmark.clickhouse.com/> .

There're 43 queries to compute, each will be executed 3 times. each rows contains the execution time for these 3 quries. The overall output will be a 43 by 3 matrix.

## To generate human readable results

your can do

```bash
bash run2.sh
```

Each query will be only exeute only once, for each query it'll print the SQL expression first then output the result. (Note that this index begins with 1, the ClickBench begins with 0)

## Known Issues


1. importing parquet by `datafusion-cli` make column name case-sensitive (i.e. i need to query thoses column by double quoted string). The original `queries.sql`'s column name <https://github.com/ClickHouse/ClickBench/blob/main/clickhouse/queries.sql> contains no double quotes, I manually added the double quotes for all the column name for now <https://github.com/ClickHouse/ClickBench/blob/main/datafusion/queries.sql>

2. since our parquet importer doesn't support schema, I use some functions to convert the data type in queries (i.e. "EventDate" becomes "EventDate::INT::DATE"). Note that other plaform support this, so it could be added while creating table i.e. <https://github.com/ClickHouse/ClickBench/blob/main/clickhouse/create.sql>