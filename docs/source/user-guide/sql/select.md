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

# SELECT syntax

The queries in DataFusion scan data from tables and return 0 or more rows.
Please be aware that column names in queries are made lower-case, but not on the inferred schema. Accordingly, if you
want to query against a capitalized field, make sure to use double quotes. Please see this
[example](https://datafusion.apache.org/user-guide/example-usage.html) for clarification.
In this documentation we describe the SQL syntax in DataFusion.

DataFusion supports the following syntax for queries:
<code class="language-sql hljs">

[ [WITH](#with-clause) with_query [, ...] ] <br/>
[SELECT](#select-clause) [ ALL | DISTINCT ] select_expr [, ...] <br/>
[ [FROM](#from-clause) from_item [ [TABLESAMPLE](#tablesample-clause) ... ] [, ...] ] <br/>
[ [JOIN](#join-clause) join_item [, ...] ] <br/>
[ [WHERE](#where-clause) condition ] <br/>
[ [GROUP BY](#group-by-clause) grouping_element [, ...] ] <br/>
[ [HAVING](#having-clause) condition] <br/>
[ [QUALIFY](#qualify-clause) condition] <br/>
[ [UNION](#union-clause) [ ALL | select ] <br/>
[ [ORDER BY](#order-by-clause) expression [ ASC | DESC ][, ...] ] <br/>
[ [LIMIT](#limit-clause) count ] <br/>
[ [EXCLUDE | EXCEPT](#exclude-and-except-clause) ] <br/>
[Pipe operators](#pipe-operators) <br/>

</code>

## WITH clause

A with clause allows to give names for queries and reference them by name.

```sql
WITH x AS (SELECT a, MAX(b) AS b FROM t GROUP BY a)
SELECT a, b FROM x;
```

## SELECT clause

Example:

```sql
SELECT a, b, a + b FROM table
```

The `DISTINCT` quantifier can be added to make the query return all distinct rows.
By default `ALL` will be used, which returns all the rows.

```sql
SELECT DISTINCT person, age FROM employees
```

## FROM clause

Example:

```sql
SELECT t.a FROM table AS t
```

## TABLESAMPLE clause

`TABLESAMPLE` returns a random subset of rows from a table. It's
useful for ad-hoc data exploration ("give me roughly 1% of this
table"), bounded `EXPLAIN ANALYZE` runs against representative data,
and any analytics workload where an approximate answer is acceptable
in exchange for reading less data.

```sql
SELECT * FROM table TABLESAMPLE SYSTEM (10);             -- ~10% of the table
SELECT * FROM table TABLESAMPLE SYSTEM (5) REPEATABLE (42);  -- deterministic
```

The percentage is in the range `(0, 100]`. `REPEATABLE(seed)` makes
the sample deterministic — the same seed against the same data always
returns the same rows.

### What `SYSTEM` means

`SYSTEM` is **block-level** sampling: instead of evaluating a
per-row coin flip, the scan keeps or drops whole blocks of rows
chosen at random. This is the same behaviour PostgreSQL documents
for `TABLESAMPLE SYSTEM` and what Hive calls `BLOCK` sampling
(DataFusion accepts `BLOCK` as an alias for `SYSTEM`).

The trade-off vs. row-level sampling (`BERNOULLI`):

- **`SYSTEM`** is fast — the scan can skip blocks entirely, so it
  reads less I/O proportional to the requested fraction. Rows
  inside each kept block are correlated, so it's statistically
  lossier than per-row sampling.
- **`BERNOULLI`** evaluates `random() < p` per row, so every row is
  read but only some are kept. Statistically tighter, but no I/O
  saving.

DataFusion only ships `SYSTEM` out of the box. To add `BERNOULLI` or
other forms, register a [`RelationPlanner`] extension; see the
[extending SQL] guide and the [`relation_planner` example].

[`relationplanner`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/trait.RelationPlanner.html
[extending sql]: ../../library-user-guide/extending-sql.md
[`relation_planner` example]: https://github.com/apache/datafusion/tree/main/datafusion-examples/examples/relation_planner

### How `SYSTEM` is implemented for Parquet

For a `ParquetSource`, `TABLESAMPLE SYSTEM(p%)` is pushed all the way
into the scan rather than evaluated as a post-scan filter. The plan
contains no `SampleExec` — instead, `ParquetSource` itself drops
files, row groups, and row-clusters in proportion to `p`.

The selection uses an **adaptive cube-root hybrid** that splits the
budget across three independent levels — file, row-group, and row
— and collapses the split when an axis can't reduce:

1. **File level** (in the `SamplePushdown` rule, where the file count
   is known): with `n_files ≥ 2`, keep `⌈n_files * cbrt(p)⌉` files
   chosen by a seeded shuffle. With `n_files = 1`, the file axis
   can't reduce, so the full budget flows to the opener.
2. **Row-group level** (in the parquet opener, after the footer is
   loaded so the row-group count is known): with multiple row groups
   in a file, the residual fraction is split as `sqrt(remaining)` at
   the row-group and row axes. With a single row group, the row-group
   axis is skipped and the full residual is applied at the row level.
3. **Row level** — within each kept row group, the kept fraction is
   materialised as a small number of contiguous `RowSelection`
   windows so the parquet reader can use the page index to skip data
   pages entirely.

The product across all axes is always `p`, so the expected result
size is `p × N` rows regardless of how many files or row groups
the scan happens to have. Spreading the reduction across all three
axes means the I/O win at small fractions does not concentrate at
a single granularity: dropping 90% of files (1/0.1 ≈ 10× fewer files)
produces a coarser sample than dropping 90% across all axes evenly.
Small inputs degenerate gracefully — a single-file scan still hits
the requested `p`; a single-file / single-row-group scan reduces to
pure row-level sampling.

`REPEATABLE(seed)` mixes the seed into every random draw, so all
levels produce the same selection across runs. The selection also
depends on the execution `partition_index` of each file (a stable
per-file id assigned by the scan, independent of the on-disk path),
the row-group index within the file, and the cluster size, so
different files don't accidentally see correlated samples and the
sample is reproducible across environments.

The sampling is visible in `EXPLAIN`:

```text
DataSourceExec: file_groups={...}, projection=[...],
  file_type=parquet,
  sample_system_target_remaining=0.5000
```

`sample_system_target_remaining` is the residual fraction handed off
to the opener after the file axis has been applied. The opener then
splits adaptively across the row-group / row axes based on what it
sees in each file's footer — that decision isn't visible in the plan
text since it varies per file at scan time.

There is no `SampleExec` in the physical plan — the `SamplePushdown`
optimizer rule absorbed the sample into the source. If pushdown is
not possible (for example, against a non-Parquet source that does
not implement `try_push_sample`), the rule errors at planning time
with `TABLESAMPLE is not supported for this source`.

### Limitations

The built-in planner accepts only `TABLESAMPLE SYSTEM(p%)` with an
optional `REPEATABLE(seed)`. The following forms error at planning
time:

- `TABLESAMPLE BERNOULLI(...)` — register a custom `RelationPlanner`.
- `TABLESAMPLE (N ROWS)` — use `LIMIT N` instead, or a custom planner.
- `TABLESAMPLE BUCKET m OUT OF n` — Hive bucket sampling is not
  supported.
- `TABLESAMPLE ... OFFSET ...` — ClickHouse-style offset sampling is
  not supported.
- Fractions outside `(0, 100]`.

## WHERE clause

Example:

```sql
SELECT a FROM table WHERE a > 10
```

## JOIN clause

DataFusion supports `INNER JOIN`, `LEFT OUTER JOIN`, `RIGHT OUTER JOIN`, `FULL OUTER JOIN`, `NATURAL JOIN`, `CROSS JOIN`, `LEFT SEMI JOIN`, `RIGHT SEMI JOIN`, `LEFT ANTI JOIN`, `RIGHT ANTI JOIN`, `LATERAL JOIN`, and `LEFT JOIN LATERAL`.

The following examples are based on this table:

```sql
select * from x;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
```

### INNER JOIN

The keywords `JOIN` or `INNER JOIN` define a join that only shows rows where there is a match in both tables.

```sql
SELECT * FROM x INNER JOIN x y ON x.column_1 = y.column_1;
+----------+----------+----------+----------+
| column_1 | column_2 | column_1 | column_2 |
+----------+----------+----------+----------+
| 1        | 2        | 1        | 2        |
+----------+----------+----------+----------+
```

### LEFT OUTER JOIN

The keywords `LEFT JOIN` or `LEFT OUTER JOIN` define a join that includes all rows from the left table even if there
is not a match in the right table. When there is no match, null values are produced for the right side of the join.

```sql
SELECT * FROM x LEFT JOIN x y ON x.column_1 = y.column_2;
+----------+----------+----------+----------+
| column_1 | column_2 | column_1 | column_2 |
+----------+----------+----------+----------+
| 1        | 2        |          |          |
+----------+----------+----------+----------+
```

### RIGHT OUTER JOIN

The keywords `RIGHT JOIN` or `RIGHT OUTER JOIN` define a join that includes all rows from the right table even if there
is not a match in the left table. When there is no match, null values are produced for the left side of the join.

```sql
SELECT * FROM x RIGHT JOIN x y ON x.column_1 = y.column_2;
+----------+----------+----------+----------+
| column_1 | column_2 | column_1 | column_2 |
+----------+----------+----------+----------+
|          |          | 1        | 2        |
+----------+----------+----------+----------+
```

### FULL OUTER JOIN

The keywords `FULL JOIN` or `FULL OUTER JOIN` define a join that is effectively a union of a `LEFT OUTER JOIN` and
`RIGHT OUTER JOIN`. It will show all rows from the left and right side of the join and will produce null values on
either side of the join where there is not a match.

```sql
SELECT * FROM x FULL OUTER JOIN x y ON x.column_1 = y.column_2;
+----------+----------+----------+----------+
| column_1 | column_2 | column_1 | column_2 |
+----------+----------+----------+----------+
| 1        | 2        |          |          |
|          |          | 1        | 2        |
+----------+----------+----------+----------+
```

### NATURAL JOIN

A `NATURAL JOIN` defines an inner join based on common column names found between the input tables. When no common
column names are found, it behaves like a `CROSS JOIN`.

```sql
SELECT * FROM x NATURAL JOIN x y;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
```

### CROSS JOIN

A `CROSS JOIN` produces a cartesian product that matches every row in the left side of the join with every row in the
right side of the join.

```sql
SELECT * FROM x CROSS JOIN x y;
+----------+----------+----------+----------+
| column_1 | column_2 | column_1 | column_2 |
+----------+----------+----------+----------+
| 1        | 2        | 1        | 2        |
+----------+----------+----------+----------+
```

### LEFT SEMI JOIN

The `LEFT SEMI JOIN` returns all rows from the left table that have at least one matching row in the right table, and
projects only the columns from the left table.

```sql
SELECT * FROM x LEFT SEMI JOIN x y ON x.column_1 = y.column_1;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
```

### RIGHT SEMI JOIN

The `RIGHT SEMI JOIN` returns all rows from the right table that have at least one matching row in the left table, and
only projects the columns from the right table.

```sql
SELECT * FROM x RIGHT SEMI JOIN x y ON x.column_1 = y.column_1;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
```

### LEFT ANTI JOIN

The `LEFT ANTI JOIN` returns all rows from the left table that do not have any matching row in the right table, projecting
only the left table’s columns.

```sql
SELECT * FROM x LEFT ANTI JOIN x y ON x.column_1 = y.column_1;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
+----------+----------+
```

### RIGHT ANTI JOIN

The `RIGHT ANTI JOIN` returns all rows from the right table that do not have any matching row in the left table, projecting
only the right table’s columns.

```sql
SELECT * FROM x RIGHT ANTI JOIN x y ON x.column_1 = y.column_1;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
+----------+----------+
```

### LATERAL JOIN

A `LATERAL JOIN` allows the right-hand side of a join to reference columns from
the left-hand side. Conceptually, the subquery on the right is evaluated once
for each row of the left-hand table, which makes it possible to "parameterize" a
subquery with values from preceding tables.

The `LATERAL` keyword is required; DataFusion does not implicitly detect
correlation in `FROM` clause subqueries.

The following examples use these tables:

```sql
CREATE TABLE departments(id INT, name TEXT) AS VALUES (1, 'HR'), (2, 'Eng'), (3, 'Sales');
CREATE TABLE employees(id INT, dept_id INT, name TEXT) AS VALUES
  (10, 1, 'Alice'), (20, 1, 'Bob'), (30, 2, 'Carol');
```

#### Comma syntax

The most concise form places `LATERAL` after a comma in the `FROM` clause.
Rows from the left table that have no matching rows in the subquery are excluded
(inner join semantics).

```sql
SELECT d.name AS dept, e.name AS emp
FROM departments d, LATERAL (
    SELECT employees.name FROM employees WHERE employees.dept_id = d.id
) AS e
ORDER BY dept, emp;
+------+-------+
| dept | emp   |
+------+-------+
| Eng  | Carol |
| HR   | Alice |
| HR   | Bob   |
+------+-------+
```

#### CROSS JOIN LATERAL

Equivalent to the comma syntax above.

```sql
SELECT d.name AS dept, e.name AS emp
FROM departments d
CROSS JOIN LATERAL (
    SELECT employees.name FROM employees WHERE employees.dept_id = d.id
) AS e
ORDER BY dept, emp;
+------+-------+
| dept | emp   |
+------+-------+
| Eng  | Carol |
| HR   | Alice |
| HR   | Bob   |
+------+-------+
```

#### JOIN LATERAL ... ON

`JOIN LATERAL` with an `ON` clause applies the `ON` condition as an additional
filter after the lateral subquery is evaluated.

```sql
SELECT d.name AS dept, sub.emp, sub.cnt
FROM departments d
JOIN LATERAL (
    SELECT count(*) AS cnt, min(employees.name) AS emp
    FROM employees WHERE employees.dept_id = d.id
) AS sub ON sub.cnt > 0
ORDER BY dept;
+------+-------+-----+
| dept | emp   | cnt |
+------+-------+-----+
| Eng  | Carol | 1   |
| HR   | Alice | 2   |
+------+-------+-----+
```

#### LEFT JOIN LATERAL

`LEFT JOIN LATERAL` preserves all rows from the left table. When the lateral
subquery produces no matching rows, the right-side columns are filled with
NULLs.

```sql
SELECT d.name AS dept, e.name AS emp
FROM departments d
LEFT JOIN LATERAL (
    SELECT employees.name FROM employees WHERE employees.dept_id = d.id
) AS e ON true
ORDER BY dept, emp;
+-------+-------+
| dept  | emp   |
+-------+-------+
| Eng   | Carol |
| HR    | Alice |
| HR    | Bob   |
| Sales | NULL  |
+-------+-------+
```

The `ON` clause can also filter results. Rows that do not satisfy the `ON`
condition are preserved with NULLs, just like a regular `LEFT JOIN`:

```sql
SELECT d.name AS dept, sub.cnt
FROM departments d
LEFT JOIN LATERAL (
    SELECT count(*) AS cnt
    FROM employees WHERE employees.dept_id = d.id
) AS sub ON sub.cnt > 0
ORDER BY dept;
+-------+------+
| dept  | cnt  |
+-------+------+
| Eng   | 1    |
| HR    | 2    |
| Sales | NULL |
+-------+------+
```

#### Limitations

The following patterns are not yet supported:

- Outer references in the `SELECT` list of the lateral subquery (e.g., `LATERAL (SELECT outer.col + 1)`).
- `HAVING` in lateral subqueries.

## GROUP BY clause

Example:

```sql
SELECT a, b, MAX(c) FROM table GROUP BY a, b
```

Some aggregation functions accept optional ordering requirement, such as `ARRAY_AGG`. If a requirement is given,
aggregation is calculated in the order of the requirement.

Example:

```sql
SELECT a, b, ARRAY_AGG(c, ORDER BY d) FROM table GROUP BY a, b
```

## HAVING clause

Example:

```sql
SELECT a, b, MAX(c) FROM table GROUP BY a, b HAVING MAX(c) > 10
```

## QUALIFY clause

Example:

```sql
SELECT ROW_NUMBER() OVER (PARTITION BY region) AS rk FROM table QUALIFY rk > 1;
```

## UNION clause

Example:

```sql
SELECT
    a,
    b,
    c
FROM table1
UNION ALL
SELECT
    a,
    b,
    c
FROM table2
```

## ORDER BY clause

Orders the results by the referenced expression. By default it uses ascending order (`ASC`).
This order can be changed to descending by adding `DESC` after the order-by expressions.

Examples:

```sql
SELECT age, person FROM table ORDER BY age;
SELECT age, person FROM table ORDER BY age DESC;
SELECT age, person FROM table ORDER BY age, person DESC;
```

## LIMIT clause

Limits the number of rows to be a maximum of `count` rows. `count` should be a non-negative integer.

Example:

```sql
SELECT age, person FROM table
LIMIT 10
```

## EXCLUDE and EXCEPT clause

Excluded named columns from query results.

Example selecting all columns except for `age` and `person`:

```sql
SELECT * EXCEPT(age, person)
FROM table;
```

```sql
SELECT * EXCLUDE(age, person)
FROM table;
```

## Pipe operators

Some SQL dialects (e.g. BigQuery) support the pipe operator `|>`.
The SQL dialect can be set like this:

```sql
set datafusion.sql_parser.dialect = 'BigQuery';
```

DataFusion currently supports the following pipe operators:

- [WHERE](#pipe_where)
- [ORDER BY](#pipe_order_by)
- [LIMIT](#pipe_limit)
- [SELECT](#pipe_select)
- [EXTEND](#pipe_extend)
- [AS](#pipe_as)
- [UNION](#pipe_union)
- [INTERSECT](#pipe_intersect)
- [EXCEPT](#pipe_except)
- [AGGREGATE](#pipe_aggregate)
- [JOIN](#pipe_join)

(pipe_where)=

### WHERE

```sql
select * from range(0,10)
|> where value < 2;
+-------+
| value |
+-------+
| 0     |
| 1     |
+-------+
```

(pipe_order_by)=

### ORDER BY

```sql
select * from range(0,3)
|> order by value desc;
+-------+
| value |
+-------+
| 2     |
| 1     |
| 0     |
+-------+
```

(pipe_limit)=

### LIMIT

```sql
select * from range(0,3)
|> order by value desc
|> limit 1;
+-------+
| value |
+-------+
| 2     |
+-------+
```

(pipe_select)=

### SELECT

```sql
select * from range(0,3)
|> select value + 10;
+---------------------------+
| range().value + Int64(10) |
+---------------------------+
| 10                        |
| 11                        |
| 12                        |
+---------------------------+
```

(pipe_extend)=

### EXTEND

```sql
select * from range(0,3)
|> extend -value AS minus_value;
+-------+-------------+
| value | minus_value |
+-------+-------------+
| 0     | 0           |
| 1     | -1          |
| 2     | -2          |
+-------+-------------+
```

(pipe_as)=

### AS

```sql
select * from range(0,3)
|> as my_range
|> SELECT my_range.value;
+-------+
| value |
+-------+
| 0     |
| 1     |
| 2     |
+-------+
```

(pipe_union)=

### UNION

```sql
select * from range(0,3)
|> union all (
  select * from range(3,6)
);
+-------+
| value |
+-------+
| 0     |
| 1     |
| 2     |
| 3     |
| 4     |
| 5     |
+-------+
```

(pipe_intersect)=

### INTERSECT

```sql
select * from range(0,100)
|> INTERSECT DISTINCT (
  select 3
);
+-------+
| value |
+-------+
| 3     |
+-------+
```

(pipe_except)=

### EXCEPT

```sql
select * from range(0,10)
|> EXCEPT DISTINCT (select * from range(5,10));
+-------+
| value |
+-------+
| 0     |
| 1     |
| 2     |
| 3     |
| 4     |
+-------+
```

(pipe_aggregate)=

### AGGREGATE

```sql
select * from range(0,3)
|> aggregate sum(value) AS total;
+-------+
| total |
+-------+
| 3     |
+-------+
```

(pipe_join)=

### JOIN

```sql
(
  SELECT 'apples' AS item, 2 AS sales
  UNION ALL
  SELECT 'bananas' AS item, 5 AS sales
)
|> AS produce_sales
|> LEFT JOIN
     (
       SELECT 'apples' AS item, 123 AS id
     ) AS produce_data
   ON produce_sales.item = produce_data.item
|> SELECT produce_sales.item, sales, id;
+--------+-------+------+
| item   | sales | id   |
+--------+-------+------+
| apples | 2     | 123  |
| bananas| 5     | NULL |
+--------+-------+------+
```
