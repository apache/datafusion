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

# Write Options

DataFusion supports customizing how data is written out to disk as a result of a ```COPY``` or ```INSERT INTO``` query. There are a few special options, file format (e.g. CSV or parquet) specific options, and parquet column specific options. Options can also in some cases be specified in multiple ways with a set order of precedence. 

## Specifying Options and Order of Precedence

Write related options can be specified in the following ways:

* Session level config defaults
* ```CREATE EXTERNAL TABLE``` options
* ```COPY``` option tuples

For a list of supported session level config defaults see [Configuration Settings](https://arrow.apache.org/datafusion/user-guide/configs.md). These defaults apply to all write operations but have the lowest level of precedence.

If inserting to an external table, table specific write options can be specified when the table is created:

```sql
CREATE EXTERNAL TABLE
my_table(a bigint, b bigint)
STORED AS csv
COMPRESSION TYPE gzip
WITH HEADER ROW
DELIMETER ';'
LOCATION '/test/location/my_csv_table/'
OPTIONS(
CREATE_LOCAL_PATH 'true',
NULL_VALUE 'NAN'
);
```

When running ```INSERT INTO my_table ...```, the above specified options will be respected (gzip compression, special delimiter, and header row included). Note that compression, header, and delimeter settings can also be specified within the ```OPTIONS``` tuple list. Dedicated syntax within the SQL statement always takes precedence over arbitrary option tuples, so if both are specified the ```OPTIONS``` setting will be ignored. CREATE_LOCAL_PATH is a special option that indicates if DataFusion should create local file paths when writing new files if they do not already exist. This option is useful if you wish to create an external table from scratch, using only DataFusion SQL statements. Finally, NULL_VALUE is a CSV format specific option that determines how null values should be encoded within the CSV file.

Finally, options can be passed when running a ```COPY``` command.

```sql
COPY source_table 
TO 'test/table_with_options' 
(format parquet,
single_file_output false,
compression snappy,
'compression::col1' 'zstd(5)',
)
```

In this example, we write the entirety of ```source_table``` out to a folder of parquet files. The option ```single_file_output``` set to false, indicates that the destination path should be interpreted as a folder to which the query will output multiple files. One parquet file will be written in parallel to the folder for each partition in the query. The next option ```compression``` set to ```snappy``` indicates that unless otherwise specified all columns should use the snappy compression codec. The option ```compression::col1``` sets an override, so that the column ```col1``` in the parquet file will use ```ZSTD``` compression codec with compression level ```5```. In general, parquet option which support column specific settings can be specified with the syntax ```OPTION::COLUMN.NESTED.PATH```.

## Available Options


### COPY Specific Options

The following special options are specific to the ```COPY``` query.

| Option             | Description                                                                                                                                                          | Default Value |
|--------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| SINGLE_FILE_OUTPUT | If true, COPY query will  write output to a single file.                                                                                                             | true          |
| FORMAT             | Specifies the file format COPY query will write out. If single_file_output is false or format cannot be inferred from file extension, then FORMAT must be specified. | N/A           |

### CREATE EXTERNAL TABLE Specific Options

The following special options are specific to creating an external table.

| Option            | Description                                                                                                                                                                                                                                | Default Value                                                                |
|-------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|
| SINGLE_FILE       | If true, indicates that this external table is backed by a single file. INSERT INTO queries will append to this file.                                                                                                                      | false                                                                        |
| CREATE_LOCAL_PATH | If true, the folder or file backing this table will be created on the local file system if it does not already exist when running INSERT INTO queries.                                                                                     | false                                                                        |
| INSERT_MODE       | Determines if INSERT INTO queries should append to existing files or append new files to an existing directory. Valid values are append_to_file, append_new_files, and error. Note that "error" will block inserting data into this table. | CSV and JSON default to append_to_file. Parquet defaults to append_new_files |