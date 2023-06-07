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
  KIND, either expressioness or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Window Functions

A _window function_ performs a calculation across a set of table rows that are somehow related to the current row. This is comparable to the type of calculation that can be done with an aggregate function. However, window functions do not cause rows to become grouped into a single output row like non-window aggregate calls would. Instead, the rows retain their separate identities. Behind the scenes, the window function is able to access more than just the current row of the query result

Here is an example that shows how to compare each employee's salary with the average salary in his or her department:

```sql
SELECT depname, empno, salary, avg(salary) OVER (PARTITION BY depname) FROM empsalary;

+-----------+-------+--------+-------------------+
| depname   | empno | salary | avg               |
+-----------+-------+--------+-------------------+
| personnel | 2     | 3900   | 3700.0            |
| personnel | 5     | 3500   | 3700.0            |
| develop   | 8     | 6000   | 5020.0            |
| develop   | 10    | 5200   | 5020.0            |
| develop   | 11    | 5200   | 5020.0            |
| develop   | 9     | 4500   | 5020.0            |
| develop   | 7     | 4200   | 5020.0            |
| sales     | 1     | 5000   | 4866.666666666667 |
| sales     | 4     | 4800   | 4866.666666666667 |
| sales     | 3     | 4800   | 4866.666666666667 |
+-----------+-------+--------+-------------------+
```

A window function call always contains an OVER clause directly following the window function's name and argument(s). This is what syntactically distinguishes it from a normal function or non-window aggregate. The OVER clause determines exactly how the rows of the query are split up for processing by the window function. The PARTITION BY clause within OVER divides the rows into groups, or partitions, that share the same values of the PARTITION BY expression(s). For each row, the window function is computed across the rows that fall into the same partition as the current row. The previous example showed how to count the average of a column per partition.

You can also control the order in which rows are processed by window functions using ORDER BY within OVER. (The window ORDER BY does not even have to match the order in which the rows are output.) Here is an example:

```sql
SELECT depname, empno, salary,
       rank() OVER (PARTITION BY depname ORDER BY salary DESC)
FROM empsalary;

+-----------+-------+--------+--------+
| depname   | empno | salary | rank   |
+-----------+-------+--------+--------+
| personnel | 2     | 3900   | 1      |
| develop   | 8     | 6000   | 1      |
| develop   | 10    | 5200   | 2      |
| develop   | 11    | 5200   | 2      |
| develop   | 9     | 4500   | 4      |
| develop   | 7     | 4200   | 5      |
| sales     | 1     | 5000   | 1      |
| sales     | 4     | 4800   | 2      |
| personnel | 5     | 3500   | 2      |
| sales     | 3     | 4800   | 2      |
+-----------+-------+--------+--------+
```

There is another important concept associated with window functions: for each row, there is a set of rows within its partition called its window frame. Some window functions act only on the rows of the window frame, rather than of the whole partition. Here is an example of using window frames in queries:

```sql
SELECT depname, empno, salary,
    avg(salary) OVER(ORDER BY salary ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS avg,
    min(salary) OVER(ORDER BY empno ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_min
FROM empsalary
ORDER BY empno ASC;

+-----------+-------+--------+--------------------+---------+
| depname   | empno | salary | avg                | cum_min |
+-----------+-------+--------+--------------------+---------+
| sales     | 1     | 5000   | 5000.0             | 5000    |
| personnel | 2     | 3900   | 3866.6666666666665 | 3900    |
| sales     | 3     | 4800   | 4700.0             | 3900    |
| sales     | 4     | 4800   | 4866.666666666667  | 3900    |
| personnel | 5     | 3500   | 3700.0             | 3500    |
| develop   | 7     | 4200   | 4200.0             | 3500    |
| develop   | 8     | 6000   | 5600.0             | 3500    |
| develop   | 9     | 4500   | 4500.0             | 3500    |
| develop   | 10    | 5200   | 5133.333333333333  | 3500    |
| develop   | 11    | 5200   | 5466.666666666667  | 3500    |
+-----------+-------+--------+--------------------+---------+
```

When a query involves multiple window functions, it is possible to write out each one with a separate OVER clause, but this is duplicative and error-prone if the same windowing behavior is wanted for several functions. Instead, each windowing behavior can be named in a WINDOW clause and then referenced in OVER. For example:

```sql
SELECT sum(salary) OVER w, avg(salary) OVER w
FROM empsalary
WINDOW w AS (PARTITION BY depname ORDER BY salary DESC);
```

## Syntax

The syntax for the OVER-clause is

```sql
function([expr])
  OVER(
    [PARTITION BY expr[, …]]
    [ORDER BY expr [ ASC | DESC ][, …]]
    [ frame_clause ]
    )
```

where **frame_clause** is one of:

```sql
  { RANGE | ROWS | GROUPS } frame_start
  { RANGE | ROWS | GROUPS } BETWEEN frame_start AND frame_end
```

and **frame_start** and **frame_end** can be one of

```sql
UNBOUNDED PRECEDING
offset PRECEDING
CURRENT ROW
offset FOLLOWING
UNBOUNDED FOLLOWING
```

where **offset** is an non-negative integer.

RANGE and GROUPS modes require an ORDER BY clause (with RANGE the ORDER BY must specify exactly one column).

## Aggregate functions

All [aggregate functions](aggregate_functions.md) can be used as window functions.

## Ranking functions

- [row_number](#row_number)
- [rank](#rank)
- [dense_rank](#dense_rank)
- [ntile](#ntile)

### `row_number`

Number of the current row within its partition, counting from 1.

```sql
row_number()
```

### `rank`

Rank of the current row with gaps; same as row_number of its first peer.

```sql
rank()
```

### `dense_rank`

Rank of the current row without gaps; this function counts peer groups.

```sql
dense_rank()
```

### `ntile`

Integer ranging from 1 to the argument value, dividing the partition as equally as possible.

```sql
ntile(expression)
```

#### Arguments

- **expression**: An integer describing the number groups the partition should be split into

## Analytical functions

- [cume_dist](#cume_dist)
- [percent_rank](#percent_rank)
- [lag](#lag)
- [lead](#lead)
- [first_value](#first_value)
- [last_value](#last_value)
- [nth_value](#nth_value)

### `cume_dist`

Relative rank of the current row: (number of rows preceding or peer with current row) / (total rows).

```sql
cume_dist()
```

### `percent_rank`

Relative rank of the current row: (rank - 1) / (total rows - 1).

```sql
percent_rank()
```

### `lag`

Returns value evaluated at the row that is offset rows before the current row within the partition; if there is no such row, instead return default (which must be of the same type as value). Both offset and default are evaluated with respect to the current row. If omitted, offset defaults to 1 and default to null.

```sql
lag(expression, offset, default)
```

#### Arguments

- **expression**: Expression to operate on
- **offset**: Integer. Specifies how many rows back the value of _expression_ should be retrieved. Defaults to 1.
- **default**: The default value if the offset is not within the partition. Must be of the same type as _expression_.

### `lead`

Returns value evaluated at the row that is offset rows after the current row within the partition; if there is no such row, instead return default (which must be of the same type as value). Both offset and default are evaluated with respect to the current row. If omitted, offset defaults to 1 and default to null.

```sql
lead(expression, offset, default)
```

#### Arguments

- **expression**: Expression to operate on
- **offset**: Integer. Specifies how many rows forward the value of _expression_ should be retrieved. Defaults to 1.
- **default**: The default value if the offset is not within the partition. Must be of the same type as _expression_.

### `first_value`

Returns value evaluated at the row that is the first row of the window frame.

```sql
first_value(expression)
```

#### Arguments

- **expression**: Expression to operate on

### `last_value`

Returns value evaluated at the row that is the last row of the window frame.

```sql
last_value(expression)
```

#### Arguments

- **expression**: Expression to operate on

### `nth_value`

Returns value evaluated at the row that is the nth row of the window frame (counting from 1); null if no such row.

```sql
nth_value(expression, n)
```

#### Arguments

- **expression**: The name the column of which nth value to retrieve
- **n**: Integer. Specifies the _n_ in nth
