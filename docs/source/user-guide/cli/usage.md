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

# Usage

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

        --color
            Enables console syntax highlighting

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
            [possible values: numbers(0/10/...), inf(no limit)] [default: 40]

        --mem-pool-type <MEM_POOL_TYPE>
            Specify the memory pool type 'greedy' or 'fair', default to 'greedy'

        --top-memory-consumers <TOP_MEMORY_CONSUMERS>
            The number of top memory consumers to display when query fails due to memory exhaustion. To disable memory consumer tracking, set this value to 0 [default: 3]

    -d, --disk-limit <DISK_LIMIT>
            Available disk space for spilling queries (e.g. '10g'), default to None (uses DataFusion's default value of '100g')

      --object-store-profiling <OBJECT_STORE_PROFILING>
          Specify the default object_store_profiling mode, defaults to 'disabled'.
          [possible values: disabled, summary, trace] [default: Disabled]

    -p, --data-path <DATA_PATH>
            Path to your data, default to current directory

    -q, --quiet
            Reduce printing other than the results and work quietly

    -r, --rc <RC>...
            Run the provided files on startup instead of ~/.datafusionrc

    -V, --version
            Print version information
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

- Object Store Profiling Mode

```bash
> \object_store_profiling [disabled|summary|trace]
```

When enabled, prints detailed information about object store (I/O) operations
performed during query execution to STDOUT.

```sql
> \object_store_profiling trace
ObjectStore Profile mode set to Trace
> select count(*) from 'https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_1.parquet';
+----------+
| count(*) |
+----------+
| 1000000  |
+----------+
1 row(s) fetched.
Elapsed 0.552 seconds.

Object Store Profiling
Instrumented Object Store: instrument_mode: Trace, inner: HttpStore
2025-10-17T18:08:48.457992+00:00 operation=Get duration=0.043592s size=8 range: bytes=174965036-174965043 path=hits_compatible/athena_partitioned/hits_1.parquet
2025-10-17T18:08:48.501878+00:00 operation=Get duration=0.031542s size=34322 range: bytes=174930714-174965035 path=hits_compatible/athena_partitioned/hits_1.parquet

Summaries:
+-----------+----------+-----------+-----------+-----------+-----------+-------+
| Operation | Metric   | min       | max       | avg       | sum       | count |
+-----------+----------+-----------+-----------+-----------+-----------+-------+
| Get       | duration | 0.031542s | 0.043592s | 0.037567s | 0.075133s | 2     |
| Get       | size     | 8 B       | 34322 B   | 17165 B   | 34330 B   | 2     |
+-----------+----------+-----------+-----------+-----------+-----------+-------+
```

## Supported SQL

In addition to the normal [SQL supported in DataFusion], `datafusion-cli` also
supports additional statements and commands:

[sql supported in datafusion]: ../sql/index.rst

### `SHOW ALL [VERBOSE]`

Show configuration options

```sql
> show all;

+-------------------------------------------------+---------+
| name                                            | value   |
+-------------------------------------------------+---------+
| datafusion.execution.batch_size                 | 8192    |
| datafusion.execution.coalesce_batches           | true    |
| datafusion.execution.time_zone                  | UTC     |
| datafusion.explain.logical_plan_only            | false   |
| datafusion.explain.physical_plan_only           | false   |
| datafusion.optimizer.filter_null_join_keys      | false   |
| datafusion.optimizer.skip_failed_rules          | true    |
+-------------------------------------------------+---------+

```

### `SHOW <OPTION>>`

Show specific configuration option

```SQL
> show datafusion.execution.batch_size;

+-------------------------------------------------+---------+
| name                                            | value   |
+-------------------------------------------------+---------+
| datafusion.execution.batch_size                 | 8192    |
+-------------------------------------------------+---------+

```

### `SET <OPTION> TO <VALUE>`

- Set configuration options

```sql
> SET datafusion.execution.batch_size to 1024;
```

## Configuration Options

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
> show all;
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

```shell
$ datafusion-cli
DataFusion CLI v13.0.0
> show datafusion.execution.batch_size;
+---------------------------------+---------+
| name                            | value   |
+---------------------------------+---------+
| datafusion.execution.batch_size | 8192    |
+---------------------------------+---------+
1 row in set. Query took 0.011 seconds.

> set datafusion.execution.batch_size to 1024;
0 rows in set. Query took 0.000 seconds.

> show datafusion.execution.batch_size;
+---------------------------------+---------+
| name                            | value   |
+---------------------------------+---------+
| datafusion.execution.batch_size | 1024    |
+---------------------------------+---------+
1 row in set. Query took 0.005 seconds.
```

## Functions

`datafusion-cli` comes with build-in functions that are not included in the
DataFusion SQL engine, see [DataFusion CLI specific functions](functions.md) section
for details.
