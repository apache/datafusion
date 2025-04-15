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

# Format Options

DataFusion supports customizing how data is read from or written to disk as a result of a `COPY`, `INSERT INTO`, or `CREATE EXTERNAL TABLE` statements. There are a few special options, file format (e.g., CSV or Parquet) specific options, and Parquet column-specific options. In some cases, Options can be specified in multiple ways with a set order of precedence.

## Specifying Options and Order of Precedence

Format-related options can be specified in three ways, in decreasing order of precedence:

- `CREATE EXTERNAL TABLE` syntax
- `COPY` option tuples
- Session-level config defaults

For a list of supported session-level config defaults, see [Configuration Settings](../configs). These defaults apply to all operations but have the lowest level of precedence.

If creating an external table, table-specific format options can be specified when the table is created using the `OPTIONS` clause:

```sql
CREATE EXTERNAL TABLE
  my_table(a bigint, b bigint)
  STORED AS csv
  LOCATION '/test/location/my_csv_table/'
  OPTIONS(
    NULL_VALUE 'NAN',
    'has_header' 'true',
    'format.delimiter' ';'
  )
```

When running `INSERT INTO my_table ...`, the options from the `CREATE TABLE` will be respected (e.g., gzip compression, special delimiter, and header row included). Note that compression, header, and delimiter settings can also be specified within the `OPTIONS` tuple list. Dedicated syntax within the SQL statement always takes precedence over arbitrary option tuples, so if both are specified, the `OPTIONS` setting will be ignored.

Finally, options can be passed when running a `COPY` command.

```sql
COPY source_table
  TO 'test/table_with_options'
  PARTITIONED BY (column3, column4)
  OPTIONS (
    format parquet,
    compression snappy,
    'compression::column1' 'zstd(5)',
  )
```

In this example, we write the entirety of `source_table` out to a folder of Parquet files. One Parquet file will be written in parallel to the folder for each partition in the query. The next option `compression` set to `snappy` indicates that unless otherwise specified, all columns should use the snappy compression codec. The option `compression::col1` sets an override, so that the column `col1` in the Parquet file will use the ZSTD compression codec with compression level `5`. In general, Parquet options that support column-specific settings can be specified with the syntax `OPTION::COLUMN.NESTED.PATH`.

# Available Options

## Execution-Specific Options

The following options are available when executing a `COPY` query.

| Option                    | Description                                                                        | Default Value |
| ------------------------- | ---------------------------------------------------------------------------------- | ------------- |
| KEEP_PARTITION_BY_COLUMNS | Flag to retain the columns in the output data when using `PARTITIONED BY` queries. | false         |

Note: `execution.keep_partition_by_columns` flag can also be enabled through `ExecutionOptions` within `SessionConfig`.

## JSON Format Options

The following options are available when reading or writing JSON files. Note: If any unsupported option is specified, an error will be raised and the query will fail.

| Option      | Description                                                                                                                        | Default Value |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| COMPRESSION | Sets the compression that should be applied to the entire JSON file. Supported values are GZIP, BZIP2, XZ, ZSTD, and UNCOMPRESSED. | UNCOMPRESSED  |

**Example:**

```sql
CREATE EXTERNAL TABLE t(a int)
STORED AS JSON
LOCATION '/tmp/foo/'
OPTIONS('COMPRESSION' 'gzip');
-- Inserting arow creates a new file in /tmp/foo
INSERT INTO t VALUES(1);
```

## CSV Format Options

The following options are available when reading or writing CSV files. Note: If any unsupported option is specified, an error will be raised and the query will fail.

| Option             | Description                                                                                                                       | Default Value    |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| COMPRESSION        | Sets the compression that should be applied to the entire CSV file. Supported values are GZIP, BZIP2, XZ, ZSTD, and UNCOMPRESSED. | UNCOMPRESSED     |
| HAS_HEADER         | Sets if the CSV file should include column headers                                                                                | false            |
| NEWLINES_IN_VALUES | Sets if newlines in quoted values are supported                                                                                   | false            |
| DATE_FORMAT        | Sets the format that dates should be encoded in within the CSV file                                                               | arrow-rs default |
| DATETIME_FORMAT    | Sets the format that datetimes should be encoded in within the CSV file                                                           | arrow-rs default |
| TIME_FORMAT        | Sets the format that times should be encoded in within the CSV file                                                               | arrow-rs default |
| RFC3339            | If true, uses RFC3339 format for date and time encodings                                                                          | arrow-rs default |
| NULL_VALUE         | Sets the string which should be used to indicate null values within the CSV file.                                                 | arrow-rs default |
| DELIMITER          | Sets the character which should be used as the column delimiter within the CSV file.                                              | arrow-rs default |

**Example:**

```sql
CREATE EXTERNAL TABLE t (col1 varchar, col2 int, col3 boolean)
STORED AS CSV
LOCATION '/tmp/foo/'
OPTIONS('DELIMITER' '|', 'HAS_HEADER' 'true', 'NEWLINES_IN_VALUES' 'true');
```

## Parquet Format Options

The following options are available when reading or writing Parquet files. If any unsupported option is specified, an error will be raised and the query will fail. If a column-specific option is specified for a column that does not exist, the option will be ignored without error.

| Option               | Can be Column Specific? | Description                                                                                                                                                                                                                                                                                                                                  | OPTIONS Key                             |
| -------------------- | ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------- |
| COMPRESSION          | Yes                     | Sets the internal Parquet **compression codec** for data pages, optionally including the compression level. Applies globally if set without `::col`, or specifically to a column if set using `'compression::column_name'`. Valid values : `uncompressed`, `snappy`, `gzip(level)`, `lzo`, `brotli(level)`, `lz4`, `zstd(level)`, `lz4_raw`. | `'compression'` or `'compression::col'` |
| ENCODING             | Yes                     | Sets the **encoding** scheme for data pages. Valid values (lowercase recommended): `plain`, `plain_dictionary`, `rle`, `bit_packed`, `delta_binary_packed`, `delta_length_byte_array`, `delta_byte_array`, `rle_dictionary`, `byte_stream_split`. Use key `'encoding'` or `'encoding::col'` in OPTIONS.                                      | `'encoding'` or `'encoding::col'`       |
| MAX_ROW_GROUP_SIZE   | No                      | Sets the maximum number of rows per row group. Larger groups require more memory but can improve compression and scan efficiency.                                                                                                                                                                                                            | `'max_row_group_size'`                  |
| BLOOM_FILTER_ENABLED | Yes (Only via `::col`)  | Sets whether a bloom filter should be written for a specific column.                                                                                                                                                                                                                                                                         | `'bloom_filter_enabled::column_name'`   |

**Example:**

```sql
CREATE EXTERNAL TABLE t (id bigint, value double, category varchar)
STORED AS PARQUET
LOCATION '/tmp/parquet_data/'
OPTIONS(
  'COMPRESSION::user_id' 'snappy',
  'ENCODING::col_a' 'delta_binary_packed',
  'MAX_ROW_GROUP_SIZE' '1000000',
  'BLOOM_FILTER_ENABLED::id' 'true'
);
```
