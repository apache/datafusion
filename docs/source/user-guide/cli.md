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

# DataFusion Command-line SQL Utility

The DataFusion CLI is a command-line interactive SQL utility for executing
queries against any supported data files. It is a convenient way to
try DataFusion out with your own data sources, and test out its SQL support.

## Example

Create a CSV file to query.

```shell
$ echo "a,b" > data.csv
$ echo "1,2" >> data.csv
```

Query that single file (the CLI also supports parquet, compressed csv, avro, json and more)

```shell
$ datafusion-cli
DataFusion CLI v17.0.0
❯ select * from 'data.csv';
+---+---+
| a | b |
+---+---+
| 1 | 2 |
+---+---+
1 row in set. Query took 0.007 seconds.
```

You can also query directories of files with compatible schemas:

```shell
$ ls data_dir/
data.csv   data2.csv
```

```shell
$ datafusion-cli
DataFusion CLI v16.0.0
❯ select * from 'data_dir';
+---+---+
| a | b |
+---+---+
| 3 | 4 |
| 1 | 2 |
+---+---+
2 rows in set. Query took 0.007 seconds.
```

## Installation

### Install and run using Cargo

The easiest way to install DataFusion CLI a spin is via `cargo install datafusion-cli`.

### Install and run using Homebrew (on MacOS)

DataFusion CLI can also be installed via Homebrew (on MacOS). Install it as any other pre-built software like this:

```bash
brew install datafusion
# ==> Downloading https://ghcr.io/v2/homebrew/core/datafusion/manifests/12.0.0
# ######################################################################## 100.0%
# ==> Downloading https://ghcr.io/v2/homebrew/core/datafusion/blobs/sha256:9ecc8a01be47ceb9a53b39976696afa87c0a8
# ==> Downloading from https://pkg-containers.githubusercontent.com/ghcr1/blobs/sha256:9ecc8a01be47ceb9a53b39976
# ######################################################################## 100.0%
# ==> Pouring datafusion--12.0.0.big_sur.bottle.tar.gz
# 🍺  /usr/local/Cellar/datafusion/12.0.0: 9 files, 17.4MB

datafusion-cli
```

### Run using Docker

There is no officially published Docker image for the DataFusion CLI, so it is necessary to build from source
instead.

Use the following commands to clone this repository and build a Docker image containing the CLI tool. Note
that there is `.dockerignore` file in the root of the repository that may need to be deleted in order for
this to work.

```bash
git clone https://github.com/apache/arrow-datafusion
git checkout 12.0.0
cd arrow-datafusion
docker build -f datafusion-cli/Dockerfile . --tag datafusion-cli
docker run -it -v $(your_data_location):/data datafusion-cli
```

## Usage

See the current usage using `datafusion-cli --help`:

```bash
Apache Arrow <dev@arrow.apache.org>
Command Line Client for DataFusion query engine.

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

## Selecting files directly

Files can be queried directly by enclosing the file or
directory name in single `'` quotes as shown in the example.

It is also possible to create a table backed by files by explicitly
via `CREATE EXTERNAL TABLE` as shown below.

## Registering Parquet Data Sources

Parquet data sources can be registered by executing a `CREATE EXTERNAL TABLE` SQL statement. It is not necessary to provide schema information for Parquet files.

```sql
CREATE EXTERNAL TABLE taxi
STORED AS PARQUET
LOCATION '/mnt/nyctaxi/tripdata.parquet';
```

## Registering CSV Data Sources

CSV data sources can be registered by executing a `CREATE EXTERNAL TABLE` SQL statement.

```sql
CREATE EXTERNAL TABLE test
STORED AS CSV
WITH HEADER ROW
LOCATION '/path/to/aggregate_test_100.csv';
```

It is also possible to provide schema information.

```sql
CREATE EXTERNAL TABLE test (
    c1  VARCHAR NOT NULL,
    c2  INT NOT NULL,
    c3  SMALLINT NOT NULL,
    c4  SMALLINT NOT NULL,
    c5  INT NOT NULL,
    c6  BIGINT NOT NULL,
    c7  SMALLINT NOT NULL,
    c8  INT NOT NULL,
    c9  BIGINT NOT NULL,
    c10 VARCHAR NOT NULL,
    c11 FLOAT NOT NULL,
    c12 DOUBLE NOT NULL,
    c13 VARCHAR NOT NULL
)
STORED AS CSV
LOCATION '/path/to/aggregate_test_100.csv';
```

## Querying S3 Data Sources

The CLI can query data in S3 if the following environment variables are defined:

- `AWS_DEFAULT_REGION`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

Details of the environment variables that can be used are

- AWS_ACCESS_KEY_ID -> access_key_id
- AWS_SECRET_ACCESS_KEY -> secret_access_key
- AWS_DEFAULT_REGION -> region
- AWS_ENDPOINT -> endpoint
- AWS_SESSION_TOKEN -> token
- AWS_CONTAINER_CREDENTIALS_RELATIVE_URI -> <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html>
- AWS_ALLOW_HTTP -> set to "true" to permit HTTP connections without TLS

Example:

```bash
$ aws s3 cp test.csv s3://my-bucket/
upload: ./test.csv to s3://my-bucket/test.csv

$ export AWS_DEFAULT_REGION=us-east-2
$ export AWS_SECRET_ACCESS_KEY=***************************
$ export AWS_ACCESS_KEY_ID=**************

$ datafusion-cli
DataFusion CLI v14.0.0
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

## Commands

Available commands inside DataFusion CLI are:

- Quit

```bash
> \q
```

- Help

```bash
> \?
```

- ListTables

```bash
> \d
```

- DescribeTable

```bash
> \d table_name
```

- QuietMode

```bash
> \quiet [true|false]
```

- list function

```bash
> \h
```

- Search and describe function

```bash
> \h function
```

- Show configuration options

```SQL
> show all;

+-------------------------------------------------+---------+
| name                                            | setting |
+-------------------------------------------------+---------+
| datafusion.execution.batch_size                 | 8192    |
| datafusion.execution.coalesce_batches           | true    |
| datafusion.execution.coalesce_target_batch_size | 4096    |
| datafusion.execution.time_zone                  | UTC     |
| datafusion.explain.logical_plan_only            | false   |
| datafusion.explain.physical_plan_only           | false   |
| datafusion.optimizer.filter_null_join_keys      | false   |
| datafusion.optimizer.skip_failed_rules          | true    |
+-------------------------------------------------+---------+

```

- Set configuration options

```SQL
> SET datafusion.execution.batch_size to 1024;
```

## Changing Configuration Options

All available configuration options can be seen using `SHOW ALL` as described above.

You can change the configuration options using environment
variables. `datafusion-cli` looks in the corresponding environment
variable with an upper case name and all `.` converted to `_`.

For example, to set `datafusion.execution.batch_size` to `1024` you
would set the `DATAFUSION_EXECUTION_BATCH_SIZE` environment variable
appropriately:

```shell
$ DATAFUSION_EXECUTION_BATCH_SIZE=1024 datafusion-cli
DataFusion CLI v12.0.0
❯ show all;
+-------------------------------------------------+---------+
| name                                            | setting |
+-------------------------------------------------+---------+
| datafusion.execution.batch_size                 | 1024    |
| datafusion.execution.coalesce_batches           | true    |
| datafusion.execution.coalesce_target_batch_size | 4096    |
| datafusion.execution.time_zone                  | UTC     |
| datafusion.explain.logical_plan_only            | false   |
| datafusion.explain.physical_plan_only           | false   |
| datafusion.optimizer.filter_null_join_keys      | false   |
| datafusion.optimizer.skip_failed_rules          | true    |
+-------------------------------------------------+---------+
8 rows in set. Query took 0.002 seconds.
```

You can change the configuration options using `SET` statement as well

```shell
$ datafusion-cli
DataFusion CLI v13.0.0

❯ show datafusion.execution.batch_size;
+---------------------------------+---------+
| name                            | setting |
+---------------------------------+---------+
| datafusion.execution.batch_size | 8192    |
+---------------------------------+---------+
1 row in set. Query took 0.011 seconds.

❯ set datafusion.execution.batch_size to 1024;
0 rows in set. Query took 0.000 seconds.

❯ show datafusion.execution.batch_size;
+---------------------------------+---------+
| name                            | setting |
+---------------------------------+---------+
| datafusion.execution.batch_size | 1024    |
+---------------------------------+---------+
1 row in set. Query took 0.005 seconds.
```
