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

# Subqueries

Subqueries (also known as inner queries or nested queries) are queries within
a query.
Subqueries can be used in `SELECT`, `FROM`, `WHERE`, and `HAVING` clauses.

The examples below are based on the following tables.

```sql
SELECT * FROM x;

+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
| 2        | 4        |
+----------+----------+
```

```sql
SELECT * FROM y;

+--------+--------+
| number | string |
+--------+--------+
| 1      | one    |
+--------+--------+
| 2      | two    |
+--------+--------+
| 3      | three  |
+--------+--------+
| 4      | four   |
+--------+--------+
```

## Subquery operators

- [[ NOT ] EXISTS](#-not--exists)
- [[ NOT ] IN](#-not--in)

### [ NOT ] EXISTS

The `EXISTS` operator returns all rows where a
_[correlated subquery](#correlated-subqueries)_ produces one or more matches for
that row. `NOT EXISTS` returns all rows where a _correlated subquery_ produces
zero matches for that row. Only _correlated subqueries_ are supported.

```sql
[NOT] EXISTS (subquery)
```

### [ NOT ] IN

The `IN` operator returns all rows where a given expression’s value can be found
in the results of a _[correlated subquery](#correlated-subqueries)_.
`NOT IN` returns all rows where a given expression’s value cannot be found in
the results of a subquery or list of values.

```sql
expression [NOT] IN (subquery|list-literal)
```

#### Examples

```sql
SELECT * FROM x WHERE column_1 IN (1,3);

+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
```

```sql
SELECT * FROM x WHERE column_1 NOT IN (1,3);

+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 2        | 4        |
+----------+----------+
```

## SELECT clause subqueries

`SELECT` clause subqueries use values returned from the inner query as part
of the outer query's `SELECT` list.
The `SELECT` clause only supports [scalar subqueries](#scalar-subqueries) that
return a single value per execution of the inner query.
The returned value can be unique per row.

```sql
SELECT [expression1[, expression2, ..., expressionN],] (<subquery>)
```

**Note**: `SELECT` clause subqueries can be used as an alternative to `JOIN`
operations.

### Example

```sql
SELECT
  column_1,
  (
    SELECT
      first_value(string)
    FROM
      y
    WHERE
      number = x.column_1
  ) AS "numeric string"
FROM
  x;

+----------+----------------+
| column_1 | numeric string |
+----------+----------------+
|        1 | one            |
|        2 | two            |
+----------+----------------+
```

## FROM clause subqueries

`FROM` clause subqueries return a set of results that is then queried and
operated on by the outer query.

```sql
SELECT expression1[, expression2, ..., expressionN] FROM (<subquery>)
```

### Example

The following query returns the average of maximum values per room.
The inner query returns the maximum value for each field from each room.
The outer query uses the results of the inner query and returns the average
maximum value for each field.

```sql
SELECT
  column_2
FROM
  (
    SELECT
      *
    FROM
      x
    WHERE
      column_1 > 1
  );

+----------+
| column_2 |
+----------+
|        4 |
+----------+
```

## WHERE clause subqueries

`WHERE` clause subqueries compare an expression to the result of the subquery
and return _true_ or _false_.
Rows that evaluate to _false_ or NULL are filtered from results.
The `WHERE` clause supports correlated and non-correlated subqueries
as well as scalar and non-scalar subqueries (depending on the the operator used
in the predicate expression).

```sql
SELECT
  expression1[, expression2, ..., expressionN]
FROM
  <measurement>
WHERE
  expression operator (<subquery>)
```

**Note:** `WHERE` clause subqueries can be used as an alternative to `JOIN`
operations.

### Examples

#### `WHERE` clause with scalar subquery

The following query returns all rows with `column_2` values above the average
of all `number` values in `y`.

```sql
SELECT
  *
FROM
  x
WHERE
  column_2 > (
    SELECT
      AVG(number)
    FROM
      y
  );

+----------+----------+
| column_1 | column_2 |
+----------+----------+
|        2 |        4 |
+----------+----------+
```

#### `WHERE` clause with non-scalar subquery

Non-scalar subqueries must use the `[NOT] IN` or `[NOT] EXISTS` operators and
can only return a single column.
The values in the returned column are evaluated as a list.

The following query returns all rows with `column_2` values in table `x` that
are in the list of numbers with string lengths greater than three from table
`y`.

```sql
SELECT
  *
FROM
  x
WHERE
  column_2 IN (
    SELECT
      number
    FROM
      y
    WHERE
      length(string) > 3
  );

+----------+----------+
| column_1 | column_2 |
+----------+----------+
|        2 |        4 |
+----------+----------+
```

### `WHERE` clause with correlated subquery

The following query returns rows with `column_2` values from table `x` greater
than the average `string` value length from table `y`.
The subquery in the `WHERE` clause uses the `column_1` value from the outer
query to return the average `string` value length for that specific value.

```sql
SELECT
  *
FROM
  x
WHERE
  column_2 > (
    SELECT
      AVG(length(string))
    FROM
      y
    WHERE
      number = x.column_1
  );

+----------+----------+
| column_1 | column_2 |
+----------+----------+
|        2 |        4 |
+----------+----------+
```

## HAVING clause subqueries

`HAVING` clause subqueries compare an expression that uses aggregate values
returned by aggregate functions in the `SELECT` clause to the result of the
subquery and return _true_ or _false_.
Rows that evaluate to _false_ are filtered from results.
The `HAVING` clause supports correlated and non-correlated subqueries
as well as scalar and non-scalar subqueries (depending on the the operator used
in the predicate expression).

```sql
SELECT
  aggregate_expression1[, aggregate_expression2, ..., aggregate_expressionN]
FROM
  <measurement>
WHERE
  <conditional_expression>
GROUP BY
  column_expression1[, column_expression2, ..., column_expressionN]
HAVING
  expression operator (<subquery>)
```

### Examples

The following query calculates the averages of even and odd numbers in table `y`
and returns the averages that are equal to the maximum value of `column_1`
in table `x`.

#### `HAVING` clause with a scalar subquery

```sql
SELECT
  AVG(number) AS avg,
  (number % 2 = 0) AS even
FROM
  y
GROUP BY
  even
HAVING
  avg = (
    SELECT
      MAX(column_1)
    FROM
      x
  );

+-------+--------+
|   avg | even   |
+-------+--------+
|     2 | false  |
+-------+--------+
```

#### `HAVING` clause with a non-scalar subquery

Non-scalar subqueries must use the `[NOT] IN` or `[NOT] EXISTS` operators and
can only return a single column.
The values in the returned column are evaluated as a list.

The following query calculates the averages of even and odd numbers in table `y`
and returns the averages that are in `column_1` of table `x`.

```sql
SELECT
  AVG(number) AS avg,
  (number % 2 = 0) AS even
FROM
  y
GROUP BY
  even
HAVING
  avg IN (
    SELECT
      column_1
    FROM
      x
  );

+-------+--------+
|   avg | even   |
+-------+--------+
|     2 | false  |
+-------+--------+
```

## Subquery categories

Subqueries can be categorized as one or more of the following based on the
behavior of the subquery:

- [correlated](#correlated-subqueries) or
  [non-correlated](#non-correlated-subqueries)
- [scalar](#scalar-subqueries) or [non-scalar](#non-scalar-subqueries)

### Correlated subqueries

In a **correlated** subquery, the inner query depends on the values of the
current row being processed.

**Note:** DataFusion internally rewrites correlated subqueries into JOINs to
improve performance. In general correlated subqueries are **less performant**
than non-correlated subqueries.

### Non-correlated subqueries

In a **non-correlated** subquery, the inner query _doesn't_ depend on the outer
query and executes independently.
The inner query executes first, and then passes the results to the outer query.

### Scalar subqueries

A **scalar** subquery returns a single value (one column of one row).
If no rows are returned, the subquery returns NULL.

### Non-scalar subqueries

A **non-scalar** subquery returns 0, 1, or multiple rows, each of which may
contain 1 or multiple columns. For each column, if there is no value to return,
the subquery returns NULL. If no rows qualify to be returned, the subquery
returns 0 rows.
