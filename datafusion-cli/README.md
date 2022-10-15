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

# DataFusion Command-line Interface

[DataFusion](df) is an extensible query execution framework, written in Rust, that uses Apache Arrow as its in-memory format.

The DataFusion CLI allows SQL queries to be executed by an in-process DataFusion context.

```ignore
USAGE:
    datafusion-cli [OPTIONS]

OPTIONS:
    -c, --batch-size <BATCH_SIZE>    The batch size of each query, or use DataFusion default
    -f, --file <FILE>...             Execute commands from file(s), then exit
        --format <FORMAT>            [default: table] [possible values: csv, tsv, table, json,
                                     nd-json]
    -h, --help                       Print help information
    -p, --data-path <DATA_PATH>      Path to your data, default to current directory
    -q, --quiet                      Reduce printing other than the results and work quietly
    -r, --rc <RC>...                 Run the provided files on startup instead of ~/.datafusionrc
    -V, --version                    Print version information

```

## Example

Create a CSV file to query.

```bash,ignore
$ echo "1,2" > data.csv
```

```sql,ignore
$ datafusion-cli

DataFusion CLI v12.0.0

> CREATE EXTERNAL TABLE foo (a INT, b INT) STORED AS CSV LOCATION 'data.csv';
0 rows in set. Query took 0.001 seconds.

> SELECT * FROM foo;
+---+---+
| a | b |
+---+---+
| 1 | 2 |
+---+---+
1 row in set. Query took 0.017 seconds.
```

## Querying S3 Data Sources

The CLI can query data in S3 if the following environment variables are defined:

- `AWS_REGION`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

Note that the region must be set to the region where the bucket exists until the following issue is resolved:

- https://github.com/apache/arrow-rs/issues/2795

Example:

```bash
$ aws s3 cp test.csv s3://my-bucket/
upload: ./test.csv to s3://my-bucket/test.csv

$ export AWS_REGION=us-east-1
$ export AWS_SECRET_ACCESS_KEY=***************************
$ export AWS_ACCESS_KEY_ID=**************

$ ./target/release/datafusion-cli
DataFusion CLI v12.0.0
❯ create external table test stored as csv location 's3://my-bucket/test.csv';
0 rows in set. Query took 0.374 seconds.
❯ select * from test;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
1 row in set. Query took 0.171 seconds.
```

## DataFusion-Cli

Build the `datafusion-cli` by `cd` into the sub-directory:

```bash
cd datafusion-cli
cargo build
```

[df]: https://crates.io/crates/datafusion
