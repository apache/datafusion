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

# DDL

DDL stands for "Data Definition Language" and relates to creating and
modifying catalog objects such as Tables.

## CREATE DATABASE

Create catalog with specified name.

<pre>
CREATE DATABASE [ IF NOT EXISTS ] <i><b>catalog</i></b>
</pre>

```sql
-- create catalog cat
CREATE DATABASE cat;
```

## CREATE SCHEMA

Create schema under specified catalog, or the default DataFusion catalog if not specified.

<pre>
CREATE SCHEMA [ IF NOT EXISTS ] [ <i><b>catalog.</i></b> ] <b><i>schema_name</i></b>
</pre>

```sql
-- create schema emu under catalog cat
CREATE SCHEMA cat.emu;
```

## CREATE EXTERNAL TABLE

`CREATE EXTERNAL TABLE` SQL statement registers a location on a local
file system or remote object store as a named table which can be queried.

The supported syntax is:

```sql
CREATE [UNBOUNDED] EXTERNAL TABLE
[ IF NOT EXISTS ]
<TABLE_NAME>[ (<column_definition>) ]
STORED AS <file_type>
[ PARTITIONED BY (<column list>) ]
[ WITH ORDER (<ordered column list>) ]
[ OPTIONS (<key_value_list>) ]
LOCATION <literal>

<column_definition> := (<column_name> <data_type>, ...)

<column_list> := (<column_name>, ...)

<ordered_column_list> := (<column_name> <sort_clause>, ...)

<key_value_list> := (<literal> <literal, <literal> <literal>, ...)
```

For a detailed list of write related options which can be passed in the OPTIONS key_value_list, see [Write Options](write_options).

`file_type` is one of `CSV`, `ARROW`, `PARQUET`, `AVRO` or `JSON`

`LOCATION <literal>` specifies the location to find the data. It can be
a path to a file or directory of partitioned files locally or on an
object store.

Parquet data sources can be registered by executing a `CREATE EXTERNAL TABLE` SQL statement such as the following. It is not necessary to
provide schema information for Parquet files.

```sql
CREATE EXTERNAL TABLE taxi
STORED AS PARQUET
LOCATION '/mnt/nyctaxi/tripdata.parquet';
```

CSV data sources can also be registered by executing a `CREATE EXTERNAL TABLE` SQL statement. The schema will be inferred based on
scanning a subset of the file.

```sql
CREATE EXTERNAL TABLE test
STORED AS CSV
LOCATION '/path/to/aggregate_simple.csv'
OPTIONS ('has_header' 'true');
```

It is also possible to use compressed files, such as `.csv.gz`:

```sql
CREATE EXTERNAL TABLE test
STORED AS CSV
COMPRESSION TYPE GZIP
LOCATION '/path/to/aggregate_simple.csv.gz'
OPTIONS ('has_header' 'true');
```

It is also possible to specify the schema manually.

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
LOCATION '/path/to/aggregate_test_100.csv'
OPTIONS ('has_header' 'true');
```

It is also possible to specify a directory that contains a partitioned
table (multiple files with the same schema)

```sql
CREATE EXTERNAL TABLE test
STORED AS CSV
LOCATION '/path/to/directory/of/files'
OPTIONS ('has_header' 'true');
```

With `CREATE UNBOUNDED EXTERNAL TABLE` SQL statement. We can create unbounded data sources such as following:

```sql
CREATE UNBOUNDED EXTERNAL TABLE taxi
STORED AS PARQUET
LOCATION '/mnt/nyctaxi/tripdata.parquet';
```

Note that this statement actually reads data from a fixed-size file, so a better example would involve reading from a FIFO file. Nevertheless, once Datafusion sees the `UNBOUNDED` keyword in a data source, it tries to execute queries that refer to this unbounded source in streaming fashion. If this is not possible according to query specifications, plan generation fails stating it is not possible to execute given query in streaming fashion. Note that queries that can run with unbounded sources (i.e. in streaming mode) are a subset of those that can with bounded sources. A query that fails with unbounded source(s) may work with bounded source(s).

When creating an output from a data source that is already ordered by
an expression, you can pre-specify the order of the data using the
`WITH ORDER` clause. This applies even if the expression used for
sorting is complex, allowing for greater flexibility.

Here's an example of how to use `WITH ORDER` clause.

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
WITH ORDER (c2 ASC, c5 + c8 DESC NULL FIRST)
LOCATION '/path/to/aggregate_test_100.csv'
OPTIONS ('has_header' 'true');
```

Where `WITH ORDER` clause specifies the sort order:

```sql
WITH ORDER (sort_expression1 [ASC | DESC] [NULLS { FIRST | LAST }]
         [, sort_expression2 [ASC | DESC] [NULLS { FIRST | LAST }] ...])
```

### Cautions when using the WITH ORDER Clause

- It's important to understand that using the `WITH ORDER` clause in the `CREATE EXTERNAL TABLE` statement only specifies the order in which the data should be read from the external file. If the data in the file is not already sorted according to the specified order, then the results may not be correct.

- It's also important to note that the `WITH ORDER` clause does not affect the ordering of the data in the original external file.

If data sources are already partitioned in Hive style, `PARTITIONED BY` can be used for partition pruning.

```text
/mnt/nyctaxi/year=2022/month=01/tripdata.parquet
/mnt/nyctaxi/year=2021/month=12/tripdata.parquet
/mnt/nyctaxi/year=2021/month=11/tripdata.parquet
```

```sql
CREATE EXTERNAL TABLE taxi
STORED AS PARQUET
PARTITIONED BY (year, month)
LOCATION '/mnt/nyctaxi';
```

## CREATE TABLE

An in-memory table can be created with a query or values list.

<pre>
CREATE [OR REPLACE] TABLE [IF NOT EXISTS] <b><i>table_name</i></b> AS [SELECT | VALUES LIST];
</pre>

```sql
CREATE TABLE IF NOT EXISTS valuetable AS VALUES(1,'HELLO'),(12,'DATAFUSION');

CREATE TABLE IF NOT EXISTS valuetable(c1 INT, c2 VARCHAR) AS VALUES(1,'HELLO'),(12,'DATAFUSION');

CREATE TABLE memtable as select * from valuetable;
```

## DROP TABLE

Removes the table from DataFusion's catalog.

<pre>
DROP TABLE [ IF EXISTS ] <b><i>table_name</i></b>;
</pre>

```sql
CREATE TABLE users AS VALUES(1,2),(2,3);
DROP TABLE users;
-- or use 'if exists' to silently ignore if the table doesn't exist
DROP TABLE IF EXISTS nonexistent_table;
```

## CREATE VIEW

View is a virtual table based on the result of a SQL query. It can be created from an existing table or values list.

<pre>
CREATE [ OR REPLACE ] VIEW <i><b>view_name</b></i> AS statement;
</pre>

```sql
CREATE TABLE users AS VALUES(1,2),(2,3),(3,4),(4,5);
CREATE VIEW test AS SELECT column1 FROM users;
SELECT * FROM test;
+---------+
| column1 |
+---------+
| 1       |
| 2       |
| 3       |
| 4       |
+---------+
```

```sql
CREATE VIEW test AS VALUES(1,2),(5,6);
SELECT * FROM test;
+---------+---------+
| column1 | column2 |
+---------+---------+
| 1       | 2       |
| 5       | 6       |
+---------+---------+
```

## DROP VIEW

Removes the view from DataFusion's catalog.

<pre>
DROP VIEW [ IF EXISTS ] <b><i>view_name</i></b>;
</pre>

```sql
-- drop users_v view from the customer_a schema
DROP VIEW IF EXISTS customer_a.users_v;
```
