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

DataFusion supports showing metadata about the tables and views available. This information can be accessed using the
views of the ISO SQL `information_schema` schema or the DataFusion specific `SHOW TABLES` and `SHOW COLUMNS` commands.

To show tables in the DataFusion catalog, use the `SHOW TABLES` command or the `information_schema.tables` view:

```text
> show tables;
or
> select * from information_schema.tables;
+---------------+--------------------+------------+------------+
| table_catalog | table_schema       | table_name | table_type |
+---------------+--------------------+------------+------------+
| datafusion    | public             | t          | BASE TABLE |
| datafusion    | information_schema | tables     | VIEW       |
| datafusion    | information_schema | views      | VIEW       |
| datafusion    | information_schema | columns    | VIEW       |
+---------------+--------------------+------------+------------+

```

To show the schema of a table in DataFusion, use the `SHOW COLUMNS` command or the `information_schema.columns` view:

```text
> show columns from t;
or
> select table_catalog, table_schema, table_name, column_name, data_type, is_nullable from information_schema.columns;
+---------------+--------------+------------+-------------+-----------+-------------+
| table_catalog | table_schema | table_name | column_name | data_type | is_nullable |
+---------------+--------------+------------+-------------+-----------+-------------+
| datafusion    | public       | t1         | Int64(1)    | Int64     | NO          |
+---------------+--------------+------------+-------------+-----------+-------------+
```
