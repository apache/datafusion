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

# DML

DML stands for "Data Manipulation Language" and relates to inserting
and modifying data in tables.

## COPY

Copies the contents of a table or query to file(s). Supported file
formats are `parquet`, `csv`, `json`, and `arrow`.

<pre>
COPY { <i><b>table_name</i></b> | <i><b>query</i></b> }
TO '<i><b>file_name</i></b>'
[ STORED AS <i><b>format</i></b> ]
[ PARTITIONED BY <i><b>column_name</i></b> [, ...] ]
[ OPTIONS( <i><b>option</i></b> [, ... ] ) ]
</pre>

`STORED AS` specifies the file format the `COPY` command will write. If this
clause is not specified, it will be inferred from the file extension if possible.

`PARTITIONED BY` specifies the columns to use for partitioning the output files into
separate hive-style directories. By default, columns used in `PARTITIONED BY` will be removed
from the output format. If you want to keep the columns, you should provide the option
`execution.keep_partition_by_columns true`. `execution.keep_partition_by_columns` flag can also
be enabled through `ExecutionOptions` within `SessionConfig`.

The output format is determined by the first match of the following rules:

1. Value of `STORED AS`
2. Filename extension (e.g. `foo.parquet` implies `PARQUET` format)

For a detailed list of valid OPTIONS, see [Format Options](format_options.md).

### Examples

Copy the contents of `source_table` to `file_name.json` in JSON format:

```sql
> COPY source_table TO 'file_name.json';
+-------+
| count |
+-------+
| 2     |
+-------+
```

Copy the contents of `source_table` to one or more Parquet formatted
files in the `dir_name` directory:

```sql
> COPY source_table TO 'dir_name' STORED AS PARQUET;
+-------+
| count |
+-------+
| 2     |
+-------+
```

Copy the contents of `source_table` to multiple directories
of hive-style partitioned parquet files:

```sql
> COPY source_table TO 'dir_name' STORED AS parquet, PARTITIONED BY (column1, column2);
+-------+
| count |
+-------+
| 2     |
+-------+
```

If the data contains values of `x` and `y` in column1 and only `a` in
column2, output files will appear in the following directory structure:

```text
dir_name/
  column1=x/
    column2=a/
      <file>.parquet
      <file>.parquet
      ...
  column1=y/
    column2=a/
      <file>.parquet
      <file>.parquet
      ...
```

Run the query `SELECT * from source ORDER BY time` and write the
results (maintaining the order) to a parquet file named
`output.parquet` with a maximum parquet row group size of 10MB:

```sql
> COPY (SELECT * from source ORDER BY time) TO 'output.parquet' OPTIONS (MAX_ROW_GROUP_SIZE 10000000);
+-------+
| count |
+-------+
| 2     |
+-------+
```

## INSERT

### Examples

Insert values into a table.

<pre>
INSERT INTO <i><b>table_name</i></b> { VALUES ( <i><b>expression</i></b> [, ...] ) [, ...] | <i><b>query</i></b> }
</pre>

```sql
> INSERT INTO target_table VALUES (1, 'Foo'), (2, 'Bar');
+-------+
| count |
+-------+
| 2     |
+-------+
```

## DELETE

Removes rows from a table.

<pre>
DELETE FROM <i><b>table_name</b></i> [ WHERE <i><b>condition</b></i> ]
</pre>

`DELETE` returns the number of removed rows in a column named `count`.

If you omit the `WHERE` clause, DataFusion removes all rows.

DataFusion removes a row only if the condition is true for that row. SQL three-valued logic applies: if the condition evaluates to `NULL`, the row remains. For example, `WHERE value > 15` keeps a row with a `NULL` value, because `NULL > 15` is `NULL`.

Not all tables support `DELETE`. See [Table support for DELETE and UPDATE](#table-support-for-delete-and-update).

### Examples

Remove the rows that match a condition:

```sql
> DELETE FROM target_table WHERE id > 1;
+-------+
| count |
+-------+
| 2     |
+-------+
```

Remove all rows:

```sql
> DELETE FROM target_table;
+-------+
| count |
+-------+
| 3     |
+-------+
```

## UPDATE

Changes the values of existing rows.

<pre>
UPDATE <i><b>table_name</b></i> SET <i><b>column</b></i> = <i><b>expression</b></i> [, ...] [ WHERE <i><b>condition</b></i> ]
</pre>

`UPDATE` returns the number of affected rows in a column named `count`.

If you omit the `WHERE` clause, DataFusion changes all rows. The three-valued logic of `DELETE` also applies here.

Each assignment expression reads the row values from before the statement. `SET a = b, b = a` therefore exchanges the two values.

Not all tables support `UPDATE`. See [Table support for DELETE and UPDATE](#table-support-for-delete-and-update).

### Examples

Set one column in the rows that match a condition:

```sql
> UPDATE target_table SET name = 'Baz' WHERE id = 2;
+-------+
| count |
+-------+
| 1     |
+-------+
```

Set two columns, one from an expression:

```sql
> UPDATE target_table SET value = value * 2, name = 'Doubled' WHERE id < 3;
+-------+
| count |
+-------+
| 2     |
+-------+
```

## Table support for DELETE and UPDATE

Not all table providers support `DELETE` and `UPDATE`. In-memory tables created with `CREATE TABLE` support both statements. File-based tables created with `CREATE EXTERNAL TABLE` and views do not. Support for custom table providers depends on the provider.

### Known limitations

- Subqueries in `DELETE` and `UPDATE` conditions can affect unintended rows: [#24654](https://github.com/apache/datafusion/issues/24654).
- `EXPLAIN DELETE` and `EXPLAIN UPDATE` can modify in-memory tables: [#24656](https://github.com/apache/datafusion/issues/24656).
- `DELETE` ignores `LIMIT`: [#24998](https://github.com/apache/datafusion/issues/24998).
- `UPDATE ... FROM` is not supported: [#19950](https://github.com/apache/datafusion/issues/19950).

## MERGE INTO

Merges rows from a source relation into a target table. Each target row may
match at most one source row. DataFusion applies only the first matching
`WHEN` clause for each row and returns the number of rows inserted, updated, or
deleted.

<pre>
MERGE INTO <i><b>target_table</i></b> [ AS <i><b>target_alias</i></b> ]
USING { <i><b>source_table</i></b> | ( <i><b>query</i></b> ) } [ AS <i><b>source_alias</i></b> ]
ON <i><b>condition</i></b>
<i><b>merge_clause</i></b> [ ... ]
</pre>

`merge_clause` can be:

```sql
WHEN MATCHED [ AND condition ] THEN UPDATE SET column = expression [, ...]
WHEN MATCHED [ AND condition ] THEN DELETE
WHEN NOT MATCHED [ BY TARGET ] [ AND condition ] THEN INSERT [(column [, ...])] VALUES (expression [, ...])
WHEN NOT MATCHED BY SOURCE [ AND condition ] THEN UPDATE SET column = expression [, ...]
WHEN NOT MATCHED BY SOURCE [ AND condition ] THEN DELETE
```

### Examples

```sql
> MERGE INTO inventory AS t
  USING updates AS s
  ON t.id = s.id
  WHEN MATCHED AND s.deleted THEN DELETE
  WHEN MATCHED THEN UPDATE SET qty = s.qty
  WHEN NOT MATCHED THEN INSERT (id, qty) VALUES (s.id, s.qty);
+-------+
| count |
+-------+
| 3     |
+-------+
```

### Provider support

`MERGE INTO` execution depends on the target table provider. DataFusion's
in-memory `MemTable` supports basic `MERGE INTO` execution. Other providers
must implement `TableProvider::merge_into`; otherwise planning the statement
returns an unsupported-operation error.

The SQL planner currently rejects these `MERGE INTO` forms:

- target table modifiers, such as `MERGE INTO target PARTITION (...)`
- target alias column lists, such as `MERGE INTO target AS t(a, b)`
- `UPDATE ... WHERE` and `UPDATE ... DELETE WHERE` predicates inside a merge
  action
- `INSERT ... WHERE` predicates inside a merge action
- `INSERT ROW`
