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

Queries in DataFusion scan data from tables, subqueries, table functions, or
literal values and return zero or more rows. DataFusion supports the following
general form for `SELECT` queries. Optional clauses can be omitted. The linked
sections describe each clause in more detail.

<pre><code>[ <a href="#with-clause">WITH</a> cte [, ...] ]
<a href="#select-clause">SELECT</a> select_item [, ...]
[ <a href="#select-into">INTO</a> table_name ]
[ <a href="#from-clause">FROM</a> from_item [, ...] ]
[ <a href="#join-clause">JOIN</a> join_item ... ]
[ <a href="#where-clause">WHERE</a> condition ]
[ <a href="#group-by-clause">GROUP BY</a> grouping_element [, ...] | GROUP BY ALL ]
[ <a href="#having-clause">HAVING</a> condition ]
[ <a href="#window-clause">WINDOW</a> window_name AS (window_definition) [, ...] ]
[ <a href="#qualify-clause">QUALIFY</a> condition ]
[ { <a href="#set-operations">UNION</a> | <a href="#set-operations">INTERSECT</a> | <a href="#set-operations">EXCEPT</a> } query ] [...]
[ <a href="#order-by-clause">ORDER BY</a> order_expression [, ...] ]
[ <a href="#limit-and-offset-clauses">LIMIT</a> count ] [ <a href="#limit-and-offset-clauses">OFFSET</a> count ]
[ <a href="#pipe-operators">|&gt;</a> pipe_operator ... ]</code></pre>

Unquoted identifiers are normalized to lower case in SQL queries, but inferred
schema field names are not changed. If a field name contains capital letters or
other characters that require quoting, reference it with double quotes. See this
[example](https://datafusion.apache.org/user-guide/example-usage.html) for
clarification.

## WITH clause

```text
WITH [RECURSIVE] cte_name [(column_name [, ...])] AS (query) [, ...]
```

A `WITH` clause defines common table expressions (CTEs) that can be referenced
by name in the rest of the query.

Examples:

```sql
WITH x AS (SELECT a, MAX(b) AS b FROM t GROUP BY a)
SELECT a, b FROM x;
```

CTEs can also rename their output columns:

```sql
WITH x(key, total) AS (
  SELECT a, SUM(b) FROM t GROUP BY a
)
SELECT key, total FROM x;
```

DataFusion supports `WITH RECURSIVE` for recursive CTEs. Recursive CTE support
is controlled by the `datafusion.execution.enable_recursive_ctes` configuration
setting, which is enabled by default.

```sql
WITH RECURSIVE numbers AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM numbers WHERE n < 3
)
SELECT n FROM numbers;
```

## SELECT clause

```text
SELECT [ALL | DISTINCT | DISTINCT ON (expression [, ...])]
       select_item [, ...]
       [INTO table_name]
```

The `SELECT` list can contain column references, arbitrary expressions, scalar
functions, aggregate functions, window functions, scalar subqueries, and
wildcards.

Examples:

```sql
SELECT a, b, a + b AS sum_ab FROM table_name;
```

Aliases can be written with or without `AS`:

```sql
SELECT a AS key, b value FROM table_name;
```

`SELECT` can be used without a `FROM` clause when the selected expressions do
not need input rows:

```sql
SELECT 1 + 2 AS three;
```

`SELECT *` requires a `FROM` clause.

### DISTINCT

```text
SELECT DISTINCT select_item [, ...]
SELECT DISTINCT ON (expression [, ...]) select_item [, ...]
```

By default, `SELECT` uses `ALL` semantics and returns every row. The `DISTINCT`
quantifier removes duplicate rows from the query result.

Examples:

```sql
SELECT DISTINCT person, age FROM employees;
```

DataFusion also supports PostgreSQL-style `DISTINCT ON`, which keeps one row for
each distinct value of the listed expressions. Use `ORDER BY` to choose which
row is kept for each group. When `ORDER BY` is present, the initial `ORDER BY`
expressions must match the `DISTINCT ON` expressions.

If multiple rows have the same `DISTINCT ON` values and the `ORDER BY` clause
does not fully order those rows, the row that is kept is not specified. Add
additional `ORDER BY` expressions to make the choice deterministic.

```sql
SELECT DISTINCT ON (customer_id) customer_id, order_id, order_date
FROM orders
ORDER BY customer_id, order_date DESC;
```

### Wildcards

```text
*
table_alias.*
* EXCLUDE column_name
* EXCLUDE (column_name [, ...])
* EXCEPT column_name
* EXCEPT (column_name [, ...])
* REPLACE (expression AS column_name [, ...])
```

Use `*` to select all columns, or `table_alias.*` to select all columns from a
specific input.

Examples:

```sql
SELECT * FROM orders;
SELECT o.* FROM orders AS o;
```

Wildcard projections support `EXCLUDE` and `EXCEPT` to omit columns. Both
accept either a single column name or a parenthesized list of column names.

```sql
SELECT * EXCLUDE customer_id FROM orders;
SELECT * EXCLUDE (customer_id, internal_note) FROM orders;
SELECT * EXCEPT customer_id FROM orders;
SELECT * EXCEPT (customer_id, internal_note) FROM orders;
SELECT o.* EXCLUDE (internal_note) FROM orders AS o;
```

Every name in an `EXCLUDE` or `EXCEPT` list must refer to an existing column.
The list must not name the same column more than once, and the wildcard must
not expand to zero columns.

Wildcard projections also support `REPLACE`, which keeps the original column
name but substitutes a new expression for that column.

```sql
SELECT * REPLACE (price * 2 AS price) FROM products;
SELECT p.* REPLACE (price * 2 AS price, product_id + 1000 AS product_id)
FROM products AS p;
```

`RENAME` and wildcard aliases such as `* AS alias` are not supported.

### SELECT INTO

```text
SELECT select_item [, ...] INTO table_name FROM ...
```

`SELECT ... INTO table_name` creates an in-memory table from the query result.
It is similar to [`CREATE TABLE ... AS SELECT`](ddl.md#create-table).

```sql
SELECT customer_id, SUM(amount) AS total
INTO customer_totals
FROM orders
GROUP BY customer_id;
```

## FROM clause

```text
FROM from_item [, ...]

from_item:
  table_name [[AS] alias [(column_alias [, ...])]]
| (query) [[AS] alias [(column_alias [, ...])]]
| VALUES (expression [, ...]) [, ...] [[AS] alias [(column_alias [, ...])]]
| table_function(argument [, ...]) [[AS] alias [(column_alias [, ...])]]
| UNNEST(expression) [[AS] alias [(column_alias [, ...])]]
```

The `FROM` clause specifies the input relations for the query. Supported inputs
include tables, CTEs, derived tables, `VALUES`, table functions, and `UNNEST`.

Examples:

```sql
SELECT t.a FROM table_name AS t;
```

Table aliases can include column aliases:

```sql
SELECT x, y
FROM some_table AS t(x, y);
```

Subqueries can be used in the `FROM` clause:

```sql
SELECT q.a
FROM (SELECT a FROM table_name WHERE a > 10) AS q;
```

`VALUES` can be used as a table expression:

```sql
SELECT *
FROM VALUES (1, 'a'), (2, 'b') AS t(id, label);
```

Table functions such as `range` and `generate_series` can be used in `FROM`:

```sql
SELECT value FROM range(0, 3);
```

`UNNEST` expands a list, array, or similar nested value into one row for each
element. It can be used in the `SELECT` list to expand a value in each input
row, or as an input relation in `FROM`. When used in `FROM`, it can be given a
table alias and column alias.

```sql
SELECT * FROM UNNEST([1, 2, 3]) AS u(value);
```

To expand a column for each input row, use `UNNEST` in the `SELECT` list:

```sql
SELECT id, UNNEST(items) FROM orders;
```

`UNNEST` in the `FROM` clause cannot yet reference columns from preceding `FROM`
items (implicit lateral references such as `FROM orders AS t, UNNEST(t.items)`
are not currently supported).

## WHERE clause

```text
WHERE condition
```

The `WHERE` clause filters input rows before grouping, aggregation, and window
processing.

```sql
SELECT a FROM table_name WHERE a > 10;
```

## JOIN clause

```text
from_item [join_type] JOIN from_item [join_condition]
from_item CROSS JOIN from_item
from_item NATURAL JOIN from_item
from_item [join_type] JOIN LATERAL (query) AS alias [join_condition]
from_item, LATERAL (query) AS alias

join_type:
  INNER
| LEFT [OUTER]
| RIGHT [OUTER]
| FULL [OUTER]
| LEFT SEMI
| RIGHT SEMI
| LEFT ANTI
| RIGHT ANTI

join_condition:
  ON condition
| USING (column_name [, ...])
```

Joins are written inside the `FROM` clause between input relations.

Join conditions can use `ON` or `USING`.

Examples:

```sql
SELECT *
FROM orders AS o
JOIN customers AS c ON o.customer_id = c.id;

SELECT *
FROM orders
JOIN customers USING (customer_id);
```

The join examples below use this table:

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
SELECT * FROM x INNER JOIN x AS y ON x.column_1 = y.column_1;
+----------+----------+----------+----------+
| column_1 | column_2 | column_1 | column_2 |
+----------+----------+----------+----------+
| 1        | 2        | 1        | 2        |
+----------+----------+----------+----------+
```

The same behavior can also be written by listing both inputs in the `FROM`
clause and putting the join condition in the `WHERE` clause:

```sql
SELECT * FROM x, x AS y WHERE x.column_1 = y.column_1;
```

### LEFT OUTER JOIN

The keywords `LEFT JOIN` or `LEFT OUTER JOIN` define a join that includes all rows from the left table even if there
is not a match in the right table. When there is no match, null values are produced for the right side of the join.

```sql
SELECT * FROM x LEFT JOIN x AS y ON x.column_1 = y.column_2;
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
SELECT * FROM x RIGHT JOIN x AS y ON x.column_1 = y.column_2;
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
SELECT * FROM x FULL OUTER JOIN x AS y ON x.column_1 = y.column_2;
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
SELECT * FROM x NATURAL JOIN x AS y;
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
SELECT * FROM x CROSS JOIN x AS y;
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
SELECT * FROM x LEFT SEMI JOIN x AS y ON x.column_1 = y.column_1;
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
SELECT * FROM x RIGHT SEMI JOIN x AS y ON x.column_1 = y.column_1;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
```

### LEFT ANTI JOIN

The `LEFT ANTI JOIN` returns all rows from the left table that do not have any matching row in the right table, projecting
only the left table's columns.

```sql
SELECT * FROM x LEFT ANTI JOIN x AS y ON x.column_1 = y.column_1;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
+----------+----------+
```

### RIGHT ANTI JOIN

The `RIGHT ANTI JOIN` returns all rows from the right table that do not have any matching row in the left table, projecting
only the right table's columns.

```sql
SELECT * FROM x RIGHT ANTI JOIN x AS y ON x.column_1 = y.column_1;
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
- `FULL OUTER JOIN LATERAL`, `RIGHT JOIN LATERAL`, `RIGHT SEMI JOIN LATERAL`, and `RIGHT ANTI JOIN LATERAL`.

## GROUP BY clause

```text
GROUP BY ALL
GROUP BY grouping_element [, ...]

grouping_element:
  expression
  ordinal_position
  ROLLUP(expression [, ...])
  CUBE(expression [, ...])
  GROUPING SETS ((grouping_element [, ...]) [, ...])
```

The `GROUP BY` clause groups rows before aggregate expressions are evaluated.
Grouping elements can be expressions, output aliases, or ordinal positions in
the `SELECT` list.

Examples:

```sql
SELECT a, b, MAX(c) FROM table_name GROUP BY a, b;
SELECT a AS key, COUNT(*) FROM table_name GROUP BY key;
SELECT a, b, COUNT(*) FROM table_name GROUP BY 1, 2;
```

`GROUP BY ALL` groups by every non-aggregate expression in the `SELECT` list.

```sql
SELECT a, b, SUM(c) FROM table_name GROUP BY ALL;
```

Grouping sets allow a single query to compute aggregates for multiple grouping
levels. `ROLLUP(a, b)` computes aggregate rows grouped by `(a, b)`, then by
`a`, then over all input rows. `CUBE(a, b)` computes aggregate rows for all
combinations of `a` and `b`. `GROUPING SETS` lets you list the grouping levels
explicitly.

```sql
SELECT a, b, SUM(c) FROM table_name GROUP BY ROLLUP(a, b);
SELECT a, b, SUM(c) FROM table_name GROUP BY CUBE(a, b);
SELECT a, b, SUM(c)
FROM table_name
GROUP BY GROUPING SETS ((a), (a, b), ());
```

Some aggregate functions accept an optional ordering requirement, such as
`ARRAY_AGG`. If an ordering requirement is given, aggregation is calculated in
that order.

```sql
SELECT a, b, ARRAY_AGG(c ORDER BY d) FROM table_name GROUP BY a, b;
```

## HAVING clause

```text
HAVING condition
```

The `HAVING` clause filters groups after aggregation. It can reference grouping
expressions, aggregate expressions, and aliases from the `SELECT` list.

```sql
SELECT a, b, MAX(c) AS max_c
FROM table_name
GROUP BY a, b
HAVING max_c > 10;
```

## WINDOW clause

```text
WINDOW window_name AS (window_definition) [, ...]
```

The `WINDOW` clause defines named window specifications that can be referenced
from window functions. See [Window Functions](window_functions.md) for the full
window-function reference.

```sql
SELECT
  depname,
  empno,
  salary,
  AVG(salary) OVER w AS avg_salary
FROM empsalary
WINDOW w AS (PARTITION BY depname ORDER BY salary DESC);
```

## QUALIFY clause

```text
QUALIFY condition
```

The `QUALIFY` clause filters rows after window functions are evaluated. A query
with `QUALIFY` must contain a window function in either the `SELECT` list or the
`QUALIFY` expression. `QUALIFY` can reference aliases from the `SELECT` list.

```sql
SELECT ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales DESC) AS rk
FROM table_name
QUALIFY rk <= 3;
```

## Set operations

```text
query UNION [ALL | DISTINCT] [BY NAME] query
query INTERSECT [ALL | DISTINCT] query
query EXCEPT [ALL | DISTINCT] query
```

Set operations combine the results of two queries into a single result. They
operate on whole rows rather than on individual columns, and the input queries
must produce compatible columns. Except for `UNION ... BY NAME` variants,
inputs must have the same number of output columns.

`UNION` returns rows from both inputs and removes duplicates by default.
`UNION DISTINCT` is equivalent to `UNION`; `UNION ALL` preserves duplicates.

Examples:

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

`INTERSECT` returns rows that appear in both inputs. `EXCEPT` returns rows from
the left input that do not appear in the right input. Both support `ALL` and
`DISTINCT`.

```sql
SELECT a FROM table1
INTERSECT
SELECT a FROM table2;

SELECT a FROM table1
EXCEPT ALL
SELECT a FROM table2;
```

`UNION BY NAME` matches columns by name instead of by position. `UNION ALL BY NAME` preserves duplicates, and `UNION DISTINCT BY NAME` removes duplicates.

```sql
SELECT a, b FROM table1
UNION BY NAME
SELECT b, a FROM table2;
```

Set operations can be followed by `ORDER BY`, `LIMIT`, and `OFFSET` clauses,
which apply to the combined result.

## ORDER BY clause

```text
ORDER BY order_expression [ASC | DESC] [NULLS FIRST | NULLS LAST] [, ...]
```

`ORDER BY` sorts query results. Each `order_expression` can be an expression, a
`SELECT` alias, or an ordinal position. The default direction is ascending
(`ASC`).

If multiple rows have the same values for every `ORDER BY` expression, their
relative order is not specified. Add additional `ORDER BY` expressions to break
ties when the exact row order matters.

Examples:

```sql
SELECT age, person FROM table_name ORDER BY age;
SELECT age, person FROM table_name ORDER BY age DESC;
SELECT age AS years, person FROM table_name ORDER BY years;
SELECT age, person FROM table_name ORDER BY 1, person DESC;
```

Use `NULLS FIRST` or `NULLS LAST` to control where null values sort:

```sql
SELECT age, person FROM table_name ORDER BY age DESC NULLS LAST;
```

With the DuckDB dialect, DataFusion supports `ORDER BY ALL`, which orders by
every column in the `SELECT` list from left to right. All selected items must
be column references; ordering by computed expressions such as `a + b` is not
supported:

```sql
SET datafusion.sql_parser.dialect = 'DuckDB';
SELECT address, zip FROM addresses ORDER BY ALL DESC;
```

## LIMIT and OFFSET clauses

```text
[LIMIT count]
[OFFSET count]
```

`LIMIT` restricts the number of rows returned. `OFFSET` skips rows before
returning results. The count expressions must be constant expressions that
evaluate to non-negative integers or `NULL`; column references are not allowed.
`NULL` has no effect.

Without an `ORDER BY` clause, `LIMIT` and `OFFSET` operate on an unspecified row
order, so the returned rows are not guaranteed to be deterministic.

Examples:

```sql
SELECT age, person FROM table_name LIMIT 10;
SELECT age, person FROM table_name OFFSET 20;
SELECT age, person FROM table_name LIMIT 10 OFFSET 20;
SELECT age, person FROM table_name OFFSET 20 LIMIT 10;
```

DataFusion also accepts MySQL-style `LIMIT offset, count`:

```sql
SELECT age, person FROM table_name LIMIT 20, 10;
```

## Pipe operators

```text
query |> pipe_operator [|> pipe_operator ...]
```

DataFusion supports BigQuery-style pipe operators (`|>`).

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
