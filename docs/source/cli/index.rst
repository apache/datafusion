.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

=======================
DataFusion Command-line
=======================

The Arrow DataFusion CLI is a command-line interactive SQL utility that allows
queries to be executed against CSV and Parquet files. It is a convenient way to
try DataFusion out with your own data sources.

Install and run using Cargo
===========================

The easiest way to install DataFusion CLI a spin is via `cargo install datafusion-cli`.

Install and run using Homebrew (on MacOS)
=========================================

DataFusion CLI can also be installed via Homebrew (on MacOS). Install it as any other pre-built software like this:

.. code-block:: bash

    brew install datafusion
    # ==> Downloading https://ghcr.io/v2/homebrew/core/datafusion/manifests/5.0.0
    # ######################################################################## 100.0%
    # ==> Downloading https://ghcr.io/v2/homebrew/core/datafusion/blobs/sha256:9ecc8a01be47ceb9a53b39976696afa87c0a8
    # ==> Downloading from https://pkg-containers.githubusercontent.com/ghcr1/blobs/sha256:9ecc8a01be47ceb9a53b39976
    # ######################################################################## 100.0%
    # ==> Pouring datafusion--5.0.0.big_sur.bottle.tar.gz
    # 🍺  /usr/local/Cellar/datafusion/5.0.0: 9 files, 17.4MB

    datafusion-cli


Run using Docker
================

There is no officially published Docker image for the DataFusion CLI, so it is necessary to build from source
instead.

Use the following commands to clone this repository and build a Docker image containing the CLI tool. Note that there is :code:`.dockerignore` file in the root of the repository that may need to be deleted in order for this to work.

.. code-block:: bash

    git clone https://github.com/apache/arrow-datafusion
    git checkout 8.0.0
    cd arrow-datafusion
    docker build -f datafusion-cli/Dockerfile . --tag datafusion-cli
    docker run -it -v $(your_data_location):/data datafusion-cli


Usage
=====

.. code-block:: bash

    Apache Arrow <dev@arrow.apache.org>
    Command Line Client for DataFusion query engine and Ballista distributed computation engine.

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


Registering Parquet Data Sources
================================

Parquet data sources can be registered by executing a :code:`CREATE EXTERNAL TABLE` SQL statement. It is not necessary to provide schema information for Parquet files.

.. code-block:: sql

    CREATE EXTERNAL TABLE taxi
    STORED AS PARQUET
    LOCATION '/mnt/nyctaxi/tripdata.parquet';


Registering CSV Data Sources
============================

CSV data sources can be registered by executing a :code:`CREATE EXTERNAL TABLE` SQL statement. It is necessary to provide schema information for CSV files since DataFusion does not automatically infer the schema when using SQL to query CSV files.

.. code-block:: sql

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
    WITH HEADER ROW
    LOCATION '/path/to/aggregate_test_100.csv';
