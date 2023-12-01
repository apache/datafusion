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

# Command line SQL console

The DataFusion CLI is a command-line interactive SQL utility for executing
queries against any supported data files. It is a convenient way to
try DataFusion's SQL support with your own data.

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
cd arrow-datafusion
git checkout 12.0.0
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
    -b, --batch-size <BATCH_SIZE>
            The batch size of each query, or use DataFusion default

    -c, --command <COMMAND>...
            Execute the given command string(s), then exit

    -f, --file <FILE>...
            Execute commands from file(s), then exit

        --format <FORMAT>
            [default: table] [possible values: csv, tsv, table, json, nd-json]

    -h, --help
            Print help information

    -m, --memory-limit <MEMORY_LIMIT>
            The memory pool limitation (e.g. '10g'), default to None (no limit)

        --maxrows <MAXROWS>
            The max number of rows to display for 'Table' format
            [default: 40] [possible values: numbers(0/10/...), inf(no limit)]

        --mem-pool-type <MEM_POOL_TYPE>
            Specify the memory pool type 'greedy' or 'fair', default to 'greedy'

    -p, --data-path <DATA_PATH>
            Path to your data, default to current directory

    -q, --quiet
            Reduce printing other than the results and work quietly

    -r, --rc <RC>...
            Run the provided files on startup instead of ~/.datafusionrc

    -V, --version
            Print version information
```

## Querying data from the files directly

Files can be queried directly by enclosing the file or
directory name in single `'` quotes as shown in the example.

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

## Creating external tables

It is also possible to create a table backed by files by explicitly
via `CREATE EXTERNAL TABLE` as shown below. Filemask wildcards supported

## Registering Parquet Data Sources

Parquet data sources can be registered by executing a `CREATE EXTERNAL TABLE` SQL statement. The schema information will be derived automatically.

Register a single file parquet datasource

```sql
CREATE EXTERNAL TABLE taxi
STORED AS PARQUET
LOCATION '/mnt/nyctaxi/tripdata.parquet';
```

Register a single folder parquet datasource. All files inside must be valid parquet files!

```sql
CREATE EXTERNAL TABLE taxi
STORED AS PARQUET
LOCATION '/mnt/nyctaxi/';
```

Register a single folder parquet datasource by specifying a wildcard for files to read

```sql
CREATE EXTERNAL TABLE taxi
STORED AS PARQUET
LOCATION '/mnt/nyctaxi/*.parquet';
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

## Registering S3 Data Sources

[AWS S3](https://aws.amazon.com/s3/) data sources can be registered by executing a `CREATE EXTERNAL TABLE` SQL statement.

```sql
CREATE EXTERNAL TABLE test
STORED AS PARQUET
OPTIONS(
    'access_key_id' '******',
    'secret_access_key' '******',
    'region' 'us-east-2'
)
LOCATION 's3://bucket/path/file.parquet';
```

The supported OPTIONS are:

- access_key_id
- secret_access_key
- session_token
- region

It is also possible to simplify sql statements by environment variables.

```bash
$ export AWS_DEFAULT_REGION=us-east-2
$ export AWS_SECRET_ACCESS_KEY=******
$ export AWS_ACCESS_KEY_ID=******

$ datafusion-cli
DataFusion CLI v21.0.0
❯ create external table test stored as parquet location 's3://bucket/path/file.parquet';
0 rows in set. Query took 0.374 seconds.
❯ select * from test;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
1 row in set. Query took 0.171 seconds.
```

Details of the environment variables that can be used are:

- AWS_ACCESS_KEY_ID -> access_key_id
- AWS_SECRET_ACCESS_KEY -> secret_access_key
- AWS_DEFAULT_REGION -> region
- AWS_ENDPOINT -> endpoint
- AWS_SESSION_TOKEN -> token
- AWS_CONTAINER_CREDENTIALS_RELATIVE_URI -> <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html>
- AWS_ALLOW_HTTP -> set to "true" to permit HTTP connections without TLS
- AWS_PROFILE -> Support for using a [named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) to supply credentials

## Registering OSS Data Sources

[Alibaba cloud OSS](https://www.alibabacloud.com/product/object-storage-service) data sources can be registered by executing a `CREATE EXTERNAL TABLE` SQL statement.

```sql
CREATE EXTERNAL TABLE test
STORED AS PARQUET
OPTIONS(
    'access_key_id' '******',
    'secret_access_key' '******',
    'endpoint' 'https://bucket.oss-cn-hangzhou.aliyuncs.com'
)
LOCATION 'oss://bucket/path/file.parquet';
```

The supported OPTIONS are:

- access_key_id
- secret_access_key
- endpoint

Note that the `endpoint` format of oss needs to be: `https://{bucket}.{oss-region-endpoint}`

## Registering GCS Data Sources

[Google Cloud Storage](https://cloud.google.com/storage) data sources can be registered by executing a `CREATE EXTERNAL TABLE` SQL statement.

```sql
CREATE EXTERNAL TABLE test
STORED AS PARQUET
OPTIONS(
    'service_account_path' '/tmp/gcs.json',
)
LOCATION 'gs://bucket/path/file.parquet';
```

The supported OPTIONS are:

- service_account_path -> location of service account file
- service_account_key -> JSON serialized service account key
- application_credentials_path -> location of application credentials file

It is also possible to simplify sql statements by environment variables.

```bash
$ export GOOGLE_SERVICE_ACCOUNT=/tmp/gcs.json

$ datafusion-cli
DataFusion CLI v21.0.0
❯ create external table test stored as parquet location 'gs://bucket/path/file.parquet';
0 rows in set. Query took 0.374 seconds.
❯ select * from test;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
1 row in set. Query took 0.171 seconds.
```

Details of the environment variables that can be used are:

- GOOGLE_SERVICE_ACCOUNT: location of service account file
- GOOGLE_SERVICE_ACCOUNT_PATH: (alias) location of service account file
- SERVICE_ACCOUNT: (alias) location of service account file
- GOOGLE_SERVICE_ACCOUNT_KEY: JSON serialized service account key
- GOOGLE_BUCKET: bucket name
- GOOGLE_BUCKET_NAME: (alias) bucket name

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

`SHOW ALL [VERBOSE]`

```SQL
> show all;

+-------------------------------------------------+---------+
| name                                            | value   |
+-------------------------------------------------+---------+
| datafusion.execution.batch_size                 | 32768    |
| datafusion.execution.coalesce_batches           | true    |
| datafusion.execution.time_zone                  | UTC     |
| datafusion.explain.logical_plan_only            | false   |
| datafusion.explain.physical_plan_only           | false   |
| datafusion.optimizer.filter_null_join_keys      | false   |
| datafusion.optimizer.skip_failed_rules          | true    |
+-------------------------------------------------+---------+

```

- Show specific configuration option

`SHOW xyz.abc.qwe [VERBOSE]`

```SQL
> show datafusion.execution.batch_size;

+-------------------------------------------------+---------+
| name                                            | value   |
+-------------------------------------------------+---------+
| datafusion.execution.batch_size                 | 32768    |
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

```SQL
$ DATAFUSION_EXECUTION_BATCH_SIZE=1024 datafusion-cli
DataFusion CLI v12.0.0
❯ show all;
+-------------------------------------------------+---------+
| name                                            | value   |
+-------------------------------------------------+---------+
| datafusion.execution.batch_size                 | 1024    |
| datafusion.execution.coalesce_batches           | true    |
| datafusion.execution.time_zone                  | UTC     |
| datafusion.explain.logical_plan_only            | false   |
| datafusion.explain.physical_plan_only           | false   |
| datafusion.optimizer.filter_null_join_keys      | false   |
| datafusion.optimizer.skip_failed_rules          | true    |
+-------------------------------------------------+---------+
8 rows in set. Query took 0.002 seconds.
```

You can change the configuration options using `SET` statement as well

```SQL
$ datafusion-cli
DataFusion CLI v13.0.0

❯ show datafusion.execution.batch_size;
+---------------------------------+---------+
| name                            | value   |
+---------------------------------+---------+
| datafusion.execution.batch_size | 32768    |
+---------------------------------+---------+
1 row in set. Query took 0.011 seconds.

❯ set datafusion.execution.batch_size to 1024;
0 rows in set. Query took 0.000 seconds.

❯ show datafusion.execution.batch_size;
+---------------------------------+---------+
| name                            | value   |
+---------------------------------+---------+
| datafusion.execution.batch_size | 1024    |
+---------------------------------+---------+
1 row in set. Query took 0.005 seconds.
```
