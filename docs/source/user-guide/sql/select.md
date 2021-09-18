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
In this documentation we describe the SQL syntax in DataFusion.

DataFusion supports the following syntax for queries:
<code class="language-sql hljs">

[ [WITH](#with-clause) with_query [, ...] ] <br/>
[SELECT](#select-clause) [ ALL | DISTINCT ] select_expr [, ...] <br/>
[ [FROM](#from-clause) from_item [, ...] ] <br/>
[ [WHERE](#where-clause) condition ] <br/>
[ [GROUP BY](#group-by-clause) grouping_element [, ...] ] <br/>
[ [HAVING](#having-clause) condition] <br/>
[ [UNION](#union-clause) [ ALL | select ] <br/>
[ [ORDER BY](#order-by-clause) expression [ ASC | DESC ][, ...] ] <br/>
[ [LIMIT](#limit-clause) count ] <br/>

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

## GROUP BY clause

Example:

```sql
SELECT a, b, MAX(c) FROM table GROUP BY a, b
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
