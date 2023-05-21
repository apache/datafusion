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

Window functions calculate a value for a set of rows within a result set.

## Aggregate functions

All [aggregate functions](aggregate_functions.md) can be used as window functions.

Examples:
```sql
    select min(x) over(partition by col1 order by col2) from table
    select avg(x) over(partition by col1 order by col2) from table
```

## Ranking functions

- [row_number](#row_number)
- [rank](#rank)
- [dense_rank](#dense_rank)
- [ntile](#ntile)

### `row_number`

Number of the current row within its partition, counting from 1.
```sql
row_number() over([partition by expression1] [order by expression2])
```

#### Arguments

- **expression1**: The expression by which to split the result set into partitions
- **expression2**: The expression by which the rows are sorted within partitions

### `rank`

Rank of the current row with gaps; same as row_number of its first peer.
```sql
rank() over([partition by expression1] [order by expression2])
```

#### Arguments

- **expression1**: The expression by which to split the result set into partitions
- **expression2**: The expression by which the rows are sorted within partitions

### `dense_rank`

Rank of the current row without gaps; this function counts peer groups.

```sql
dense_rank() over([partition by expression1] [order by expression2])
```

#### Arguments

- **expression1**: The expression by which to split the result set into partitions
- **expression2**: The expression by which the rows are sorted within partitions


### `ntile`

Integer ranging from 1 to the argument value, dividing the partition as equally as possible.

```sql
ntile(expression1) over([partition by expression2] [order by expression3])
```

#### Arguments

- **expression1**: An integer describing the number groups the partition should be split into
- **expression2**: The expression by which to split the result set into partitions
- **expression3**: The expression by which the rows are sorted within partitions



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
cume_dist() over([partition by expression1] [order by expression2])
```

#### Arguments

- **expression1**: The expression by which to split the result set into partitions
- **expression2**: The expression by which the rows are sorted within partitions


### `percent_rank`

Relative rank of the current row: (rank - 1) / (total rows - 1).


```sql
percent_rank() over([partition by expression1] [order by expression2])
```

#### Arguments

- **expression1**: The expression by which to split the result set into partitions
- **expression2**: The expression by which the rows are sorted within partitions


### `lag`

Returns value evaluated at the row that is offset rows before the current row within the partition; if there is no such row, instead return default (which must be of the same type as value). Both offset and default are evaluated with respect to the current row. If omitted, offset defaults to 1 and default to null.

```sql
lag(expression1, offset, default) over([partition by expression3] [order by expression4])
```

#### Arguments

- **expression1**: Expression to operate on
- **offset**: Integer. Specifies how many rows back the value of *expression1* should be retrieved. Defaults to 1.
- **default**: The default value if the offset is not within the partition. Must be of the same type as *expression1*.
- **expression3**: The expression by which to split the result set into partitions
- **expression4**: The expression by which the rows are sorted within partitions


### `lead`

Returns value evaluated at the row that is offset rows after the current row within the partition; if there is no such row, instead return default (which must be of the same type as value). Both offset and default are evaluated with respect to the current row. If omitted, offset defaults to 1 and default to null.

```sql
lead(expression1, offset default) over([partition by expression2] [order by expression3])
```

#### Arguments

- **expression1**: Expression to operate on
- **offset**: Integer. Specifies how many rows forward the value of *expression1* should be retrieved. Defaults to 1.
- **default**: The default value if the offset is not within the partition. Must be of the same type as *expression1*.
- **expression3**: The expression by which to split the result set into partitions
- **expression4**: The expression by which the rows are sorted within partitions

### `first_value`

Returns value evaluated at the row that is the first row of the window frame.

```sql
first_value(expression1) over([partition by expression2] [order by expression3])
```

#### Arguments

- **expression1**: Expression to operate on
- **expression2**: The expression by which to split the result set into partitions
- **expression3**: The expression by which the rows are sorted within partitions


### `last_value`

Returns value evaluated at the row that is the last row of the window frame.


```sql
last_value(expression1) over([partition by expression2] [order by expression3])
```

#### Arguments

- **expression1**: Expression to operate on
- **expression2**: The expression by which to split the result set into partitions
- **expression3**: The expression by which the rows are sorted within partitions


### `nth_value`

Returns value evaluated at the row that is the nth row of the window frame (counting from 1); null if no such row.


```sql
nth_value(expression1, n) over([partition by expression2] [order by expression3])
```

#### Arguments

- **expression1**: The name the column of which nth value to retrieve
- **n**: Integer. Specifies the *n* in nth
- **expression2**: The expression by which to split the result set into partitions
- **expression3**: The expression by which the rows are sorted within partitions



