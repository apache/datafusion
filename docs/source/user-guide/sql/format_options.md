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
  );
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

| Option Key                                                  | Description                                                                                                  | Default Value |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ------------- |
| execution.batch_size                                        | Default batch size while creating new batches.                                                               | 8192          |
| execution.coalesce_batches                                  | Whether to coalesce small batches into larger ones between operators.                                        | true          |
| execution.collect_statistics                                | Should DataFusion collect statistics after listing files.                                                    | false         |
| execution.target_partitions                                 | Number of partitions for query execution. Defaults to number of CPU cores.                                   | (num_cpus)    |
| execution.time_zone                                         | The default time zone (e.g., "+00:00").                                                                      | "+00:00"      |
| execution.planning_concurrency                              | Fan-out during initial physical planning. Defaults to number of CPU cores.                                   | (num_cpus)    |
| execution.skip_physical_aggregate_schema_check              | Skip verifying that the schema produced by planning the input of `Aggregate` matches the input plan.         | false         |
| execution.sort_spill_reservation_bytes                      | Reserved memory for each spillable sort operation (bytes).                                                   | 10485760      |
| execution.sort_in_place_threshold_bytes                     | Below what size should data be concatenated and sorted in a single RecordBatch (bytes).                      | 1048576       |
| execution.meta_fetch_concurrency                            | Number of files to read in parallel when inferring schema and statistics.                                    | 32            |
| execution.minimum_parallel_output_files                     | Guarantees a minimum level of output files running in parallel.                                              | 4             |
| execution.soft_max_rows_per_output_file                     | Target number of rows in output files when writing multiple (soft max).                                      | 50000000      |
| execution.max_buffered_batches_per_output_file              | Maximum number of RecordBatches buffered for each output file.                                               | 2             |
| execution.listing_table_ignore_subdirectory                 | Should subdirectories be ignored when scanning directories for data files.                                   | true          |
| execution.enable_recursive_ctes                             | Should DataFusion support recursive CTEs.                                                                    | true          |
| execution.split_file_groups_by_statistics                   | Attempt to eliminate sorts by packing & sorting files with non-overlapping statistics into same file groups. | false         |
| execution.keep_partition_by_columns                         | Should DataFusion keep the columns used for partition_by in the output RecordBatches.                        | false         |
| execution.skip_partial_aggregation_probe_ratio_threshold    | Aggregation ratio threshold for skipping partial aggregation.                                                | 0.8           |
| execution.skip_partial_aggregation_probe_rows_threshold     | Number of input rows partial aggregation partition should process before switching mode.                     | 100000        |
| execution.use_row_number_estimates_to_optimize_partitioning | Use row number estimates to optimize partitioning.                                                           | false         |
| execution.enforce_batch_size_in_joins                       | Should DataFusion enforce batch size in joins.                                                               | false         |

**Usage Example:**

```sql
COPY my_table TO '/tmp/output.csv'
OPTIONS (
  'execution.batch_size' '4096',
  'execution.target_partitions' '8',
  'execution.keep_partition_by_columns' 'true'
);
```

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

| Option               | Description                                                                                                                       | Default Value      |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| COMPRESSION          | Sets the compression that should be applied to the entire CSV file. Supported values are GZIP, BZIP2, XZ, ZSTD, and UNCOMPRESSED. | UNCOMPRESSED       |
| HAS_HEADER           | Sets if the CSV file should include column headers. If not set, uses session or system default.                                   | None               |
| DELIMITER            | Sets the character which should be used as the column delimiter within the CSV file.                                              | `,` (comma)        |
| QUOTE                | Sets the character which should be used for quoting values within the CSV file.                                                   | `"` (double quote) |
| TERMINATOR           | Sets the character which should be used as the line terminator within the CSV file.                                               | None               |
| ESCAPE               | Sets the character which should be used for escaping special characters within the CSV file.                                      | None               |
| DOUBLE_QUOTE         | Sets if quotes within quoted fields should be escaped by doubling them (e.g., `"aaa""bbb"`).                                      | None               |
| NEWLINES_IN_VALUES   | Sets if newlines in quoted values are supported. If not set, uses session or system default.                                      | None               |
| DATE_FORMAT          | Sets the format that dates should be encoded in within the CSV file.                                                              | None               |
| DATETIME_FORMAT      | Sets the format that datetimes should be encoded in within the CSV file.                                                          | None               |
| TIMESTAMP_FORMAT     | Sets the format that timestamps should be encoded in within the CSV file.                                                         | None               |
| TIMESTAMP_TZ_FORMAT  | Sets the format that timestamps with timezone should be encoded in within the CSV file.                                           | None               |
| TIME_FORMAT          | Sets the format that times should be encoded in within the CSV file.                                                              | None               |
| NULL_VALUE           | Sets the string which should be used to indicate null values within the CSV file.                                                 | None               |
| NULL_REGEX           | Sets the regex pattern to match null values when loading CSVs.                                                                    | None               |
| SCHEMA_INFER_MAX_REC | Sets the maximum number of records to scan to infer the schema.                                                                   | None               |
| COMMENT              | Sets the character which should be used to indicate comment lines in the CSV file.                                                | None               |

**Example:**

```sql
CREATE EXTERNAL TABLE t (col1 varchar, col2 int, col3 boolean)
STORED AS CSV
LOCATION '/tmp/foo/'
OPTIONS('DELIMITER' '|', 'HAS_HEADER' 'true', 'NEWLINES_IN_VALUES' 'true');
```

## Parquet Format Options

The following options are available when reading or writing Parquet files. If any unsupported option is specified, an error will be raised and the query will fail. If a column-specific option is specified for a column that does not exist, the option will be ignored without error.

| Option                                     | Can be Column Specific? | Description                                                                                                                                                                                                                                                                                                                                 | OPTIONS Key                                           | Default Value            |
| ------------------------------------------ | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------- | ------------------------ |
| COMPRESSION                                | Yes                     | Sets the internal Parquet **compression codec** for data pages, optionally including the compression level. Applies globally if set without `::col`, or specifically to a column if set using `'compression::column_name'`. Valid values: `uncompressed`, `snappy`, `gzip(level)`, `lzo`, `brotli(level)`, `lz4`, `zstd(level)`, `lz4_raw`. | `'compression'` or `'compression::col'`               | zstd(3)                  |
| ENCODING                                   | Yes                     | Sets the **encoding** scheme for data pages. Valid values: `plain`, `plain_dictionary`, `rle`, `bit_packed`, `delta_binary_packed`, `delta_length_byte_array`, `delta_byte_array`, `rle_dictionary`, `byte_stream_split`. Use key `'encoding'` or `'encoding::col'` in OPTIONS.                                                             | `'encoding'` or `'encoding::col'`                     | None                     |
| DICTIONARY_ENABLED                         | Yes                     | Sets whether dictionary encoding should be enabled globally or for a specific column.                                                                                                                                                                                                                                                       | `'dictionary_enabled'` or `'dictionary_enabled::col'` | true                     |
| STATISTICS_ENABLED                         | Yes                     | Sets the level of statistics to write (`none`, `chunk`, `page`).                                                                                                                                                                                                                                                                            | `'statistics_enabled'` or `'statistics_enabled::col'` | page                     |
| BLOOM_FILTER_ENABLED                       | Yes                     | Sets whether a bloom filter should be written for a specific column.                                                                                                                                                                                                                                                                        | `'bloom_filter_enabled::column_name'`                 | None                     |
| BLOOM_FILTER_FPP                           | Yes                     | Sets bloom filter false positive probability (global or per column).                                                                                                                                                                                                                                                                        | `'bloom_filter_fpp'` or `'bloom_filter_fpp::col'`     | None                     |
| BLOOM_FILTER_NDV                           | Yes                     | Sets bloom filter number of distinct values (global or per column).                                                                                                                                                                                                                                                                         | `'bloom_filter_ndv'` or `'bloom_filter_ndv::col'`     | None                     |
| MAX_ROW_GROUP_SIZE                         | No                      | Sets the maximum number of rows per row group. Larger groups require more memory but can improve compression and scan efficiency.                                                                                                                                                                                                           | `'max_row_group_size'`                                | 1048576                  |
| ENABLE_PAGE_INDEX                          | No                      | If true, reads the Parquet data page level metadata (the Page Index), if present, to reduce I/O and decoding.                                                                                                                                                                                                                               | `'enable_page_index'`                                 | true                     |
| PRUNING                                    | No                      | If true, enables row group pruning based on min/max statistics.                                                                                                                                                                                                                                                                             | `'pruning'`                                           | true                     |
| SKIP_METADATA                              | No                      | If true, skips optional embedded metadata in the file schema.                                                                                                                                                                                                                                                                               | `'skip_metadata'`                                     | true                     |
| METADATA_SIZE_HINT                         | No                      | Sets the size hint (in bytes) for fetching Parquet file metadata.                                                                                                                                                                                                                                                                           | `'metadata_size_hint'`                                | None                     |
| PUSHDOWN_FILTERS                           | No                      | If true, enables filter pushdown during Parquet decoding.                                                                                                                                                                                                                                                                                   | `'pushdown_filters'`                                  | false                    |
| REORDER_FILTERS                            | No                      | If true, enables heuristic reordering of filters during Parquet decoding.                                                                                                                                                                                                                                                                   | `'reorder_filters'`                                   | false                    |
| SCHEMA_FORCE_VIEW_TYPES                    | No                      | If true, reads Utf8/Binary columns as view types.                                                                                                                                                                                                                                                                                           | `'schema_force_view_types'`                           | true                     |
| BINARY_AS_STRING                           | No                      | If true, reads Binary columns as strings.                                                                                                                                                                                                                                                                                                   | `'binary_as_string'`                                  | false                    |
| DATA_PAGESIZE_LIMIT                        | No                      | Sets best effort maximum size of data page in bytes.                                                                                                                                                                                                                                                                                        | `'data_pagesize_limit'`                               | 1048576                  |
| DATA_PAGE_ROW_COUNT_LIMIT                  | No                      | Sets best effort maximum number of rows in data page.                                                                                                                                                                                                                                                                                       | `'data_page_row_count_limit'`                         | 20000                    |
| DICTIONARY_PAGE_SIZE_LIMIT                 | No                      | Sets best effort maximum dictionary page size, in bytes.                                                                                                                                                                                                                                                                                    | `'dictionary_page_size_limit'`                        | 1048576                  |
| WRITE_BATCH_SIZE                           | No                      | Sets write_batch_size in bytes.                                                                                                                                                                                                                                                                                                             | `'write_batch_size'`                                  | 1024                     |
| WRITER_VERSION                             | No                      | Sets the Parquet writer version (`1.0` or `2.0`).                                                                                                                                                                                                                                                                                           | `'writer_version'`                                    | 1.0                      |
| SKIP_ARROW_METADATA                        | No                      | If true, skips writing Arrow schema information into the Parquet file metadata.                                                                                                                                                                                                                                                             | `'skip_arrow_metadata'`                               | false                    |
| CREATED_BY                                 | No                      | Sets the "created by" string in the Parquet file metadata.                                                                                                                                                                                                                                                                                  | `'created_by'`                                        | datafusion version X.Y.Z |
| COLUMN_INDEX_TRUNCATE_LENGTH               | No                      | Sets the length (in bytes) to truncate min/max values in column indexes.                                                                                                                                                                                                                                                                    | `'column_index_truncate_length'`                      | 64                       |
| STATISTICS_TRUNCATE_LENGTH                 | No                      | Sets statistics truncate length.                                                                                                                                                                                                                                                                                                            | `'statistics_truncate_length'`                        | None                     |
| BLOOM_FILTER_ON_WRITE                      | No                      | Sets whether bloom filters should be written for all columns by default (can be overridden per column).                                                                                                                                                                                                                                     | `'bloom_filter_on_write'`                             | false                    |
| ALLOW_SINGLE_FILE_PARALLELISM              | No                      | Enables parallel serialization of columns in a single file.                                                                                                                                                                                                                                                                                 | `'allow_single_file_parallelism'`                     | true                     |
| MAXIMUM_PARALLEL_ROW_GROUP_WRITERS         | No                      | Maximum number of parallel row group writers.                                                                                                                                                                                                                                                                                               | `'maximum_parallel_row_group_writers'`                | 1                        |
| MAXIMUM_BUFFERED_RECORD_BATCHES_PER_STREAM | No                      | Maximum number of buffered record batches per stream.                                                                                                                                                                                                                                                                                       | `'maximum_buffered_record_batches_per_stream'`        | 2                        |
| KEY_VALUE_METADATA                         | No (Key is specific)    | Adds custom key-value pairs to the file metadata. Use the format `'metadata::your_key_name' 'your_value'`. Multiple entries allowed.                                                                                                                                                                                                        | `'metadata::key_name'`                                | None                     |

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
