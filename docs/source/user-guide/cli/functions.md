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

# CLI Specific Functions

`datafusion-cli` comes with build-in functions that are not included in the
DataFusion SQL engine by default. These functions are:

## `parquet_metadata`

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
in the file. Please refer to the [Parquet Documentation] for more information in
the meaning of these fields.

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

[`page index`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md

## `metadata_cache`

The `metadata_cache` function shows information about the default File Metadata Cache that is used by the
[`ListingTable`] implementation in DataFusion. This cache is used to speed up
reading metadata from files when scanning directories with many files.

For example, after creating a table with the [CREATE EXTERNAL TABLE](../sql/ddl.md#create-external-table)
command:

```sql
> create external table hits
  stored as parquet
  location 's3://clickhouse-public-datasets/hits_compatible/athena_partitioned/';
```

You can inspect the metadata cache by querying the `metadata_cache` function:

```sql
> select * from metadata_cache();
+----------------------------------------------------+---------------------+-----------------+---------------------------------------+---------+---------------------+------+------------------+
| path                                               | file_modified       | file_size_bytes | e_tag                                 | version | metadata_size_bytes | hits | extra            |
+----------------------------------------------------+---------------------+-----------------+---------------------------------------+---------+---------------------+------+------------------+
| hits_compatible/athena_partitioned/hits_61.parquet | 2022-07-03T15:40:34 | 117270944       | "5db11cad1ca0d80d748fc92c914b010a-6"  | NULL    | 212949              | 0    | page_index=false |
| hits_compatible/athena_partitioned/hits_32.parquet | 2022-07-03T15:37:17 | 94506004        | "2f7db49a9fe242179590b615b94a39d2-5"  | NULL    | 278157              | 0    | page_index=false |
| hits_compatible/athena_partitioned/hits_40.parquet | 2022-07-03T15:38:07 | 142508647       | "9e5852b45a469d5a05bf270a286eab8a-8"  | NULL    | 212917              | 0    | page_index=false |
| hits_compatible/athena_partitioned/hits_93.parquet | 2022-07-03T15:44:07 | 127987774       | "751100bf0dac7d489b9836abf3108b99-7"  | NULL    | 278318              | 0    | page_index=false |
| .                                                                                                                                                                                            |
+----------------------------------------------------+---------------------+-----------------+---------------------------------------+---------+---------------------+------+------------------+
```

Since `metadata_cache` is a normal table function, you can use it in most places you can use
a table reference.

For example, to get the total size consumed by the cached entries:

```sql
> select sum(metadata_size_bytes) from metadata_cache();
+-------------------------------------------+
| sum(metadata_cache().metadata_size_bytes) |
+-------------------------------------------+
| 22972345                                  |
+-------------------------------------------+
```

The columns of the returned table are:

| column_name         | data_type | Description                                                                               |
| ------------------- | --------- | ----------------------------------------------------------------------------------------- |
| path                | Utf8      | File path relative to the object store / filesystem root                                  |
| file_modified       | Timestamp | Last modified time of the file                                                            |
| file_size_bytes     | UInt64    | Size of the file in bytes                                                                 |
| e_tag               | Utf8      | [Entity Tag] (ETag) of the file if available                                              |
| version             | Utf8      | Version of the file if available (for object stores that support versioning)              |
| metadata_size_bytes | UInt64    | Size of the cached metadata in memory (not its thrift encoded form)                       |
| hits                | UInt64    | Number of times the cached metadata has been accessed                                     |
| extra               | Utf8      | Extra information about the cached metadata (e.g., if page index information is included) |

[`listingtable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[entity tag]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag
