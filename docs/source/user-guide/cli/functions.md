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

## `statistics_cache`

Similarly to the `metadata_cache`, the `statistics_cache` function can be used to show information
about the File Statistics Cache that is used by the [`ListingTable`] implementation in DataFusion.
For the statistics to be collected, the config `datafusion.execution.collect_statistics` must be
enabled.

You can inspect the statistics cache by querying the `statistics_cache` function. For example:

```sql
> select * from statistics_cache();
+------------------+---------------------+-----------------+------------------------+---------+-----------------+-------------+--------------------+-----------------------+
| path             | file_modified       | file_size_bytes | e_tag                  | version | num_rows        | num_columns | table_size_bytes   | statistics_size_bytes |
+------------------+---------------------+-----------------+------------------------+---------+-----------------+-------------+--------------------+-----------------------+
| .../hits.parquet | 2022-06-25T22:22:22 | 14779976446     | 0-5e24d1ee16380-370f48 | NULL    | Exact(99997497) | 105         | Exact(36445943240) | 0                     |
+------------------+---------------------+-----------------+------------------------+---------+-----------------+-------------+--------------------+-----------------------+
```

The columns of the returned table are:

| column_name           | data_type | Description                                                                  |
| --------------------- | --------- | ---------------------------------------------------------------------------- |
| path                  | Utf8      | File path relative to the object store / filesystem root                     |
| file_modified         | Timestamp | Last modified time of the file                                               |
| file_size_bytes       | UInt64    | Size of the file in bytes                                                    |
| e_tag                 | Utf8      | [Entity Tag] (ETag) of the file if available                                 |
| version               | Utf8      | Version of the file if available (for object stores that support versioning) |
| num_rows              | Utf8      | Number of rows in the table                                                  |
| num_columns           | UInt64    | Number of columns in the table                                               |
| table_size_bytes      | Utf8      | Size of the table, in bytes                                                  |
| statistics_size_bytes | UInt64    | Size of the cached statistics in memory                                      |

## `list_files_cache`

The `list_files_cache` function shows information about the `ListFilesCache` that is used by the [`ListingTable`] implementation in DataFusion. When creating a [`ListingTable`], DataFusion lists the files in the table's location and caches results in the `ListFilesCache`. Subsequent queries against the same table can reuse this cached information instead of re-listing the files. Cache entries are scoped to tables.

You can inspect the cache by querying the `list_files_cache` function. For example,

```sql
> set datafusion.runtime.list_files_cache_ttl = "30s";
> create external table overturemaps
stored as parquet
location 's3://overturemaps-us-west-2/release/2025-12-17.0/theme=base/type=infrastructure';
0 row(s) fetched.
> select table, path, metadata_size_bytes, expires_in, unnest(metadata_list)['file_size_bytes'] as file_size_bytes, unnest(metadata_list)['e_tag'] as e_tag from list_files_cache() limit 10;
+--------------+-----------------------------------------------------+---------------------+-----------------------------------+-----------------+---------------------------------------+
| table        | path                                                | metadata_size_bytes | expires_in                        | file_size_bytes | e_tag                                 |
+--------------+-----------------------------------------------------+---------------------+-----------------------------------+-----------------+---------------------------------------+
| overturemaps | release/2025-12-17.0/theme=base/type=infrastructure | 2750                | 0 days 0 hours 0 mins 25.264 secs | 999055952       | "35fc8fbe8400960b54c66fbb408c48e8-60" |
| overturemaps | release/2025-12-17.0/theme=base/type=infrastructure | 2750                | 0 days 0 hours 0 mins 25.264 secs | 975592768       | "8a16e10b722681cdc00242564b502965-59" |
| overturemaps | release/2025-12-17.0/theme=base/type=infrastructure | 2750                | 0 days 0 hours 0 mins 25.264 secs | 1082925747      | "24cd13ddb5e0e438952d2499f5dabe06-65" |
| overturemaps | release/2025-12-17.0/theme=base/type=infrastructure | 2750                | 0 days 0 hours 0 mins 25.264 secs | 1008425557      | "37663e31c7c64d4ef355882bcd47e361-61" |
| overturemaps | release/2025-12-17.0/theme=base/type=infrastructure | 2750                | 0 days 0 hours 0 mins 25.264 secs | 1065561905      | "4e7c50d2d1b3c5ed7b82b4898f5ac332-64" |
| overturemaps | release/2025-12-17.0/theme=base/type=infrastructure | 2750                | 0 days 0 hours 0 mins 25.264 secs | 1045655427      | "8fff7e6a72d375eba668727c55d4f103-63" |
| overturemaps | release/2025-12-17.0/theme=base/type=infrastructure | 2750                | 0 days 0 hours 0 mins 25.264 secs | 1086822683      | "b67167d8022d778936c330a52a5f1922-65" |
| overturemaps | release/2025-12-17.0/theme=base/type=infrastructure | 2750                | 0 days 0 hours 0 mins 25.264 secs | 1016732378      | "6d70857a0473ed9ed3fc6e149814168b-61" |
| overturemaps | release/2025-12-17.0/theme=base/type=infrastructure | 2750                | 0 days 0 hours 0 mins 25.264 secs | 991363784       | "c9cafb42fcbb413f851691c895dd7c2b-60" |
| overturemaps | release/2025-12-17.0/theme=base/type=infrastructure | 2750                | 0 days 0 hours 0 mins 25.264 secs | 1032469715      | "7540252d0d67158297a67038a3365e0f-62" |
+--------------+-----------------------------------------------------+---------------------+-----------------------------------+-----------------+---------------------------------------+
```

The columns of the returned table are:
| column_name | data_type | Description |
| ------------------- | ------------ | ----------------------------------------------------------------------------------------- |
| table | Utf8 | Name of the table |
| path | Utf8 | File path relative to the object store / filesystem root |
| metadata_size_bytes | UInt64 | Size of the cached metadata in memory (not its thrift encoded form) |
| expires_in | Duration(ms) | Last modified time of the file |
| metadata_list | List(Struct) | List of metadatas, one for each file under the path. |

A metadata struct in the metadata_list contains the following fields:

```text
{
  "file_path": "release/2025-12-17.0/theme=base/type=infrastructure/part-00000-d556e455-e0c5-4940-b367-daff3287a952-c000.zstd.parquet",
  "file_modified": "2025-12-17T22:20:29",
  "file_size_bytes": 999055952,
  "e_tag": "35fc8fbe8400960b54c66fbb408c48e8-60",
  "version": null
}
```

[`listingtable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[entity tag]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag
