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

## CREATE EXTERNAL TABLE

Parquet data sources can be registered by executing a `CREATE EXTERNAL TABLE` SQL statement. It is not necessary
to provide schema information for Parquet files.

```sql
CREATE EXTERNAL TABLE taxi
STORED AS PARQUET
LOCATION '/mnt/nyctaxi/tripdata.parquet';
```

CSV data sources can also be registered by executing a `CREATE EXTERNAL TABLE` SQL statement. The schema will be
inferred based on scanning a subset of the file.

```sql
CREATE EXTERNAL TABLE test
STORED AS CSV
WITH HEADER ROW
LOCATION '/path/to/aggregate_simple.csv';
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
WITH HEADER ROW
LOCATION '/path/to/aggregate_test_100.csv';
```

If data sources are already partitioned in Hive style, `PARTITIONED BY` can be used for partition pruning.

```
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
CREATE TABLE valuetable IF NOT EXISTS AS VALUES(1,'HELLO'),(12,'DATAFUSION');

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

## DROP VIEW

Removes the view from DataFusion's catalog.

<pre>
DROP VIEW [ IF EXISTS ] <b><i>view_name</i></b>;
</pre>

```sql
-- drop users_v view from the customer_a schema
DROP VIEW IF EXISTS customer_a.users_v;
```
