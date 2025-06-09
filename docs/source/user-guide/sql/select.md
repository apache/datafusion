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
[ [FROM](#from-clause) from_item [, ...] ] <br/>
[ [JOIN](#join-clause) join_item [, ...] ] <br/>
[ [WHERE](#where-clause) condition ] <br/>
[ [GROUP BY](#group-by-clause) grouping_element [, ...] ] <br/>
[ [HAVING](#having-clause) condition] <br/>
[ [UNION](#union-clause) [ ALL | select ] <br/>
[ [ORDER BY](#order-by-clause) expression [ ASC | DESC ][, ...] ] <br/>
[ [LIMIT](#limit-clause) count ] <br/>
[ [EXCLUDE | EXCEPT](#exclude-and-except-clause) ] <br/>

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

## WHERE clause

Example:

```sql
SELECT a FROM table WHERE a > 10
```

## JOIN clause

DataFusion supports `INNER JOIN`, `LEFT OUTER JOIN`, `RIGHT OUTER JOIN`, `FULL OUTER JOIN`, `NATURAL JOIN`, `CROSS JOIN`, `LEFT SEMI JOIN`, `RIGHT SEMI JOIN`, `LEFT ANTI JOIN`, and `RIGHT ANTI JOIN`.

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
