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

CSV data sources can also be registered by executing a `CREATE EXTERNAL TABLE` SQL statement. It is necessary to
provide schema information for CSV files since DataFusion does not automatically infer the schema when using SQL
to query CSV files.

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
