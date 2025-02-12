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
DataFusion SQL engine. These functions are:

### `parquet_metadata`

The `parquet_metadata` table function can be used to inspect detailed metadata
about a parquet file such as statistics, sizes, and other information. This can
be helpful to understand how parquet files are structured.

For example, to see information about the `"WatchID"` column in the
`hits.parquet` file, you can use:

```sql
SELECT path_in_schema, row_group_id, row_group_num_rows, stats_min, stats_max, total_compressed_size
FROM parquet_metadata('hits.parquet')
WHERE path_in_schema = '"WatchID"'
LIMIT 3;

+----------------+--------------+--------------------+---------------------+---------------------+-----------------------+
| path_in_schema | row_group_id | row_group_num_rows | stats_min           | stats_max           | total_compressed_size |
+----------------+--------------+--------------------+---------------------+---------------------+-----------------------+
| "WatchID"      | 0            | 450560             | 4611687214012840539 | 9223369186199968220 | 3883759               |
| "WatchID"      | 1            | 612174             | 4611689135232456464 | 9223371478009085789 | 5176803               |
| "WatchID"      | 2            | 344064             | 4611692774829951781 | 9223363791697310021 | 3031680               |
+----------------+--------------+--------------------+---------------------+---------------------+-----------------------+
3 rows in set. Query took 0.053 seconds.
```

The returned table has the following columns for each row for each column chunk
in the file. Please refer to the [Parquet Documentation] for more information.

[parquet documentation]: https://parquet.apache.org/

| column_name             | data_type | Description                                                                                         |
| ----------------------- | --------- | --------------------------------------------------------------------------------------------------- |
| filename                | Utf8      | Name of the file                                                                                    |
| row_group_id            | Int64     | Row group index the column chunk belongs to                                                         |
| row_group_num_rows      | Int64     | Count of rows stored in the row group                                                               |
| row_group_num_columns   | Int64     | Total number of columns in the row group (same for all row groups)                                  |
| row_group_bytes         | Int64     | Number of bytes used to store the row group (not including metadata)                                |
| column_id               | Int64     | ID of the column                                                                                    |
| file_offset             | Int64     | Offset within the file that this column chunk's data begins                                         |
| num_values              | Int64     | Total number of values in this column chunk                                                         |
| path_in_schema          | Utf8      | "Path" (column name) of the column chunk in the schema                                              |
| type                    | Utf8      | Parquet data type of the column chunk                                                               |
| stats_min               | Utf8      | The minimum value for this column chunk, if stored in the statistics, cast to a string              |
| stats_max               | Utf8      | The maximum value for this column chunk, if stored in the statistics, cast to a string              |
| stats_null_count        | Int64     | Number of null values in this column chunk, if stored in the statistics                             |
| stats_distinct_count    | Int64     | Number of distinct values in this column chunk, if stored in the statistics                         |
| stats_min_value         | Utf8      | Same as `stats_min`                                                                                 |
| stats_max_value         | Utf8      | Same as `stats_max`                                                                                 |
| compression             | Utf8      | Block level compression (e.g. `SNAPPY`) used for this column chunk                                  |
| encodings               | Utf8      | All block level encodings (e.g. `[PLAIN_DICTIONARY, PLAIN, RLE]`) used for this column chunk        |
| index_page_offset       | Int64     | Offset in the file of the [`page index`], if any                                                    |
| dictionary_page_offset  | Int64     | Offset in the file of the dictionary page, if any                                                   |
| data_page_offset        | Int64     | Offset in the file of the first data page, if any                                                   |
| total_compressed_size   | Int64     | Number of bytes the column chunk's data after encoding and compression (what is stored in the file) |
| total_uncompressed_size | Int64     | Number of bytes the column chunk's data after encoding                                              |

+-------------------------+-----------+-------------+

[`page index`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
