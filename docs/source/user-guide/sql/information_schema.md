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

# Information Schema

DataFusion supports showing metadata about the tables available. This information can be accessed using the views
of the ISO SQL `information_schema` schema or the DataFusion specific `SHOW TABLES` and `SHOW COLUMNS` commands.

More information can be found in the [Postgres docs](https://www.postgresql.org/docs/13/infoschema-schema.html)).

To show tables available for use in DataFusion, use the `SHOW TABLES` command or the `information_schema.tables` view:

```sql
> show tables;
+---------------+--------------------+------------+------------+
| table_catalog | table_schema       | table_name | table_type |
+---------------+--------------------+------------+------------+
| datafusion    | public             | t          | BASE TABLE |
| datafusion    | information_schema | tables     | VIEW       |
+---------------+--------------------+------------+------------+

> select * from information_schema.tables;

+---------------+--------------------+------------+--------------+
| table_catalog | table_schema       | table_name | table_type   |
+---------------+--------------------+------------+--------------+
| datafusion    | public             | t          | BASE TABLE   |
| datafusion    | information_schema | TABLES     | SYSTEM TABLE |
+---------------+--------------------+------------+--------------+
```

To show the schema of a table in DataFusion, use the `SHOW COLUMNS` command or the or `information_schema.columns` view:

```sql
> show columns from t;
+---------------+--------------+------------+-------------+-----------+-------------+
| table_catalog | table_schema | table_name | column_name | data_type | is_nullable |
+---------------+--------------+------------+-------------+-----------+-------------+
| datafusion    | public       | t          | a           | Int32     | NO          |
| datafusion    | public       | t          | b           | Utf8      | NO          |
| datafusion    | public       | t          | c           | Float32   | NO          |
+---------------+--------------+------------+-------------+-----------+-------------+

> select table_name, column_name, ordinal_position, is_nullable, data_type from information_schema.columns;
+------------+-------------+------------------+-------------+-----------+
| table_name | column_name | ordinal_position | is_nullable | data_type |
+------------+-------------+------------------+-------------+-----------+
| t          | a           | 0                | NO          | Int32     |
| t          | b           | 1                | NO          | Utf8      |
| t          | c           | 2                | NO          | Float32   |
+------------+-------------+------------------+-------------+-----------+
```
