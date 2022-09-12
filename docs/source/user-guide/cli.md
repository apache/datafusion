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

The DataFusion CLI is a command-line interactive SQL utility that allows
queries to be executed against any supported data files. It is a convenient way to
try DataFusion out with your own data sources.

## Example

Create a CSV file to query.

```bash
$ echo "1,2" > data.csv
```

```bash
$ datafusion-cli
DataFusion CLI v12.0.0
❯ CREATE EXTERNAL TABLE foo STORED AS CSV LOCATION 'data.csv';
0 rows in set. Query took 0.017 seconds.
❯ select * from foo;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
1 row in set. Query took 0.012 seconds.
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

Type `exit` or `quit` to exit the CLI.
```

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
> \h function_table
```
