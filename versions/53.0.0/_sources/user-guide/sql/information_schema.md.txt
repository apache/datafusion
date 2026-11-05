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

## `SHOW TABLES`

To show tables in the DataFusion catalog, use the `SHOW TABLES` command or the
`information_schema.tables` view:

```sql
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

## `SHOW COLUMNS`

To show the schema of a table in DataFusion, use the `SHOW COLUMNS` command or
the `information_schema.columns` view.

```sql
> show columns from t;
or
> select table_catalog, table_schema, table_name, column_name, data_type, is_nullable from information_schema.columns;
+---------------+--------------+------------+-------------+-----------+-------------+
| table_catalog | table_schema | table_name | column_name | data_type | is_nullable |
+---------------+--------------+------------+-------------+-----------+-------------+
| datafusion    | public       | t          | Int64(1)    | Int64     | NO          |
+---------------+--------------+------------+-------------+-----------+-------------+
```

## `SHOW ALL` (configuration options)

To show the current session configuration options, use the `SHOW ALL` command or
the `information_schema.df_settings` view:

```sql
select * from information_schema.df_settings;

+-------------------------------------------------+---------+
| name                                            | setting |
+-------------------------------------------------+---------+
| datafusion.execution.batch_size                 | 8192    |
| datafusion.execution.coalesce_batches           | true    |
| datafusion.execution.time_zone                  | UTC     |
| datafusion.explain.logical_plan_only            | false   |
| datafusion.explain.physical_plan_only           | false   |
...
| datafusion.optimizer.filter_null_join_keys      | false   |
| datafusion.optimizer.skip_failed_rules          | true    |
+-------------------------------------------------+---------+
```

## `SHOW FUNCTIONS`

To show the list of functions available, use the `SHOW FUNCTIONS` command or the

- `information_schema.information_schema.routines` view: functions and descriptions
- `information_schema.information_schema.parameters` view: parameters and descriptions

Syntax:

```sql
SHOW FUNCTIONS [ LIKE <pattern> ];
```

Example output

```sql
> show functions like '%datetrunc';
+---------------+-------------------------------------+-------------------------+-------------------------------------------------+---------------+-------------------------------------------------------+-----------------------------------+
| function_name | return_type                         | parameters              | parameter_types                                 | function_type | description                                           | syntax_example                    |
+---------------+-------------------------------------+-------------------------+-------------------------------------------------+---------------+-------------------------------------------------------+-----------------------------------+
| datetrunc     | Timestamp(Microsecond, Some("+TZ")) | [precision, expression] | [Utf8, Timestamp(Microsecond, Some("+TZ"))]     | SCALAR        | Truncates a timestamp value to a specified precision. | date_trunc(precision, expression) |
| datetrunc     | Timestamp(Nanosecond, None)         | [precision, expression] | [Utf8View, Timestamp(Nanosecond, None)]         | SCALAR        | Truncates a timestamp value to a specified precision. | date_trunc(precision, expression) |
| datetrunc     | Timestamp(Second, Some("+TZ"))      | [precision, expression] | [Utf8View, Timestamp(Second, Some("+TZ"))]      | SCALAR        | Truncates a timestamp value to a specified precision. | date_trunc(precision, expression) |
| datetrunc     | Timestamp(Microsecond, None)        | [precision, expression] | [Utf8View, Timestamp(Microsecond, None)]        | SCALAR        | Truncates a timestamp value to a specified precision. | date_trunc(precision, expression) |
| datetrunc     | Timestamp(Second, None)             | [precision, expression] | [Utf8View, Timestamp(Second, None)]             | SCALAR        | Truncates a timestamp value to a specified precision. | date_trunc(precision, expression) |
| datetrunc     | Timestamp(Microsecond, None)        | [precision, expression] | [Utf8, Timestamp(Microsecond, None)]            | SCALAR        | Truncates a timestamp value to a specified precision. | date_trunc(precision, expression) |
| datetrunc     | Timestamp(Second, None)             | [precision, expression] | [Utf8, Timestamp(Second, None)]                 | SCALAR        | Truncates a timestamp value to a specified precision. | date_trunc(precision, expression) |
| datetrunc     | Timestamp(Microsecond, Some("+TZ")) | [precision, expression] | [Utf8View, Timestamp(Microsecond, Some("+TZ"))] | SCALAR        | Truncates a timestamp value to a specified precision. | date_trunc(precision, expression) |
| datetrunc     | Timestamp(Nanosecond, Some("+TZ"))  | [precision, expression] | [Utf8, Timestamp(Nanosecond, Some("+TZ"))]      | SCALAR        | Truncates a timestamp value to a specified precision. | date_trunc(precision, expression) |
| datetrunc     | Timestamp(Millisecond, None)        | [precision, expression] | [Utf8, Timestamp(Millisecond, None)]            | SCALAR        | Truncates a timestamp value to a specified precision. | date_trunc(precision, expression) |
| datetrunc     | Timestamp(Millisecond, Some("+TZ")) | [precision, expression] | [Utf8, Timestamp(Millisecond, Some("+TZ"))]     | SCALAR        | Truncates a timestamp value to a specified precision. | date_trunc(precision, expression) |
| datetrunc     | Timestamp(Second, Some("+TZ"))      | [precision, expression] | [Utf8, Timestamp(Second, Some("+TZ"))]          | SCALAR        | Truncates a timestamp value to a specified precision. | date_trunc(precision, expression) |
| datetrunc     | Timestamp(Nanosecond, None)         | [precision, expression] | [Utf8, Timestamp(Nanosecond, None)]             | SCALAR        | Truncates a timestamp value to a specified precision. | date_trunc(precision, expression) |
| datetrunc     | Timestamp(Millisecond, None)        | [precision, expression] | [Utf8View, Timestamp(Millisecond, None)]        | SCALAR        | Truncates a timestamp value to a specified precision. | date_trunc(precision, expression) |
| datetrunc     | Timestamp(Millisecond, Some("+TZ")) | [precision, expression] | [Utf8View, Timestamp(Millisecond, Some("+TZ"))] | SCALAR        | Truncates a timestamp value to a specified precision. | date_trunc(precision, expression) |
| datetrunc     | Timestamp(Nanosecond, Some("+TZ"))  | [precision, expression] | [Utf8View, Timestamp(Nanosecond, Some("+TZ"))]  | SCALAR        | Truncates a timestamp value to a specified precision. | date_trunc(precision, expression) |
+---------------+-------------------------------------+-------------------------+-------------------------------------------------+---------------+-------------------------------------------------------+-----------------------------------+
16 row(s) fetched.
```
