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

DataFusion supports `EXISTS`, `NOT EXISTS`, `IN`, `NOT IN` and Scalar Subqueries.

The examples below are based on the following table.

```sql
❯ select * from x;
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
```

## EXISTS

The `EXISTS` syntax can be used to find all rows in a relation where a correlated subquery produces one or more matches
for that row. Only correlated subqueries are supported.

```sql
❯ select * from x y where exists (select * from x where x.column_1 = y.column_1);
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
1 row in set.
```

## NOT EXISTS

The `NOT EXISTS` syntax can be used to find all rows in a relation where a correlated subquery produces zero matches
for that row. Only correlated subqueries are supported.

```sql
❯ select * from x y where not exists (select * from x where x.column_1 = y.column_1);
0 rows in set.
```

## IN

The `IN` syntax can be used to find all rows in a relation where a given expression's value can be found in the
results of a correlated subquery.

```sql
❯ select * from x where column_1 in (select column_1 from x);
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
1 row in set.
```

## NOT IN

The `NOT IN` syntax can be used to find all rows in a relation where a given expression's value can not be found in the
results of a correlated subquery.

```sql
❯ select * from x where column_1 not in (select column_1 from x);
0 rows in set.
```

## Scalar Subquery

A scalar subquery can be used to produce a single value that can be used in many different contexts in a query. Here
is an example of a filter using a scalar subquery. Only correlated subqueries are supported.

```sql
❯ select * from x y where column_1 < (select sum(column_2) from x where x.column_1 = y.column_1);
+----------+----------+
| column_1 | column_2 |
+----------+----------+
| 1        | 2        |
+----------+----------+
1 row in set.
```
