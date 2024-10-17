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

# Scalar Functions

Scalar functions operate on a single row at a time and return a single value.

Note: this documentation is in the process of being migrated to be [automatically created from the codebase].
Please see the [Scalar Functions (new)](scalar_functions_new.md) page for
the rest of the documentation.

[automatically created from the codebase]: https://github.com/apache/datafusion/issues/12740

## Conditional Functions

See the new documentation [`here`](https://datafusion.apache.org/user-guide/sql/scalar_functions_new.html)

## String Functions

See the new documentation [`here`](https://datafusion.apache.org/user-guide/sql/scalar_functions_new.html)

### `position`

Returns the position of `substr` in `origstr` (counting from 1). If `substr` does
not appear in `origstr`, return 0.

```
position(substr in origstr)
```

#### Arguments

- **substr**: The pattern string.
- **origstr**: The model string.

## Time and Date Functions

- [extract](#extract)

### `extract`

Returns a sub-field from a time value as an integer.

```
extract(field FROM source)
```

Equivalent to calling `date_part('field', source)`. For example, these are equivalent:

```sql
extract(day FROM '2024-04-13'::date)
date_part('day', '2024-04-13'::date)
```

See [date_part](#date_part).

## Array Functions

- [unnest](#unnest)
- [range](#range)

### `unnest`

Transforms an array into rows.

#### Arguments

- **array**: Array expression to unnest.
  Can be a constant, column, or function, and any combination of array operators.

#### Examples

```
> select unnest(make_array(1, 2, 3, 4, 5));
+------------------------------------------------------------------+
| unnest(make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5))) |
+------------------------------------------------------------------+
| 1                                                                |
| 2                                                                |
| 3                                                                |
| 4                                                                |
| 5                                                                |
+------------------------------------------------------------------+
```

```
> select unnest(range(0, 10));
+-----------------------------------+
| unnest(range(Int64(0),Int64(10))) |
+-----------------------------------+
| 0                                 |
| 1                                 |
| 2                                 |
| 3                                 |
| 4                                 |
| 5                                 |
| 6                                 |
| 7                                 |
| 8                                 |
| 9                                 |
+-----------------------------------+
```

### `range`

Returns an Arrow array between start and stop with step. `SELECT range(2, 10, 3) -> [2, 5, 8]` or
`SELECT range(DATE '1992-09-01', DATE '1993-03-01', INTERVAL '1' MONTH);`

The range start..end contains all values with start <= x < end. It is empty if start >= end.

Step can not be 0 (then the range will be nonsense.).

Note that when the required range is a number, it accepts (stop), (start, stop), and (start, stop, step) as parameters,
but when the required range is a date or timestamp, it must be 3 non-NULL parameters.
For example,

```
SELECT range(3);
SELECT range(1,5);
SELECT range(1,5,1);
```

are allowed in number ranges

but in date and timestamp ranges, only

```
SELECT range(DATE '1992-09-01', DATE '1993-03-01', INTERVAL '1' MONTH);
SELECT range(TIMESTAMP '1992-09-01', TIMESTAMP '1993-03-01', INTERVAL '1' MONTH);
```

is allowed, and

```
SELECT range(DATE '1992-09-01', DATE '1993-03-01', NULL);
SELECT range(NULL, DATE '1993-03-01', INTERVAL '1' MONTH);
SELECT range(DATE '1992-09-01', NULL, INTERVAL '1' MONTH);
```

are not allowed

#### Arguments

- **start**: start of the range. Ints, timestamps, dates or string types that can be coerced to Date32 are supported.
- **end**: end of the range (not included). Type must be the same as start.
- **step**: increase by step (can not be 0). Steps less than a day are supported only for timestamp ranges.

#### Aliases

- generate_series

## Struct Functions

- [unnest](#unnest-struct)

For more struct functions see the new documentation [
`here`](https://datafusion.apache.org/user-guide/sql/scalar_functions_new.html)

### `unnest (struct)`

Unwraps struct fields into columns.

#### Arguments

- **struct**: Object expression to unnest.
  Can be a constant, column, or function, and any combination of object operators.

#### Examples

```
> select * from foo;
+---------------------+
| column1             |
+---------------------+
| {a: 5, b: a string} |
+---------------------+

> select unnest(column1) from foo;
+-----------------------+-----------------------+
| unnest(foo.column1).a | unnest(foo.column1).b |
+-----------------------+-----------------------+
| 5                     | a string              |
+-----------------------+-----------------------+
```

## Map Functions

- [map](#map)
- [make_map](#make_map)
- [map_extract](#map_extract)
- [map_keys](#map_keys)
- [map_values](#map_values)

### `map`

Returns an Arrow map with the specified key-value pairs.

```
map(key, value)
map(key: value)
```

#### Arguments

- **key**: Expression to be used for key.
  Can be a constant, column, or function, any combination of arithmetic or
  string operators, or a named expression of previous listed.
- **value**: Expression to be used for value.
  Can be a constant, column, or function, any combination of arithmetic or
  string operators, or a named expression of previous listed.

#### Example

```
SELECT MAP(['POST', 'HEAD', 'PATCH'], [41, 33, null]);
----
{POST: 41, HEAD: 33, PATCH: }

SELECT MAP([[1,2], [3,4]], ['a', 'b']);
----
{[1, 2]: a, [3, 4]: b}

SELECT MAP { 'a': 1, 'b': 2 };
----
{a: 1, b: 2}
```

### `make_map`

Returns an Arrow map with the specified key-value pairs.

```
make_map(key_1, value_1, ..., key_n, value_n)
```

#### Arguments

- **key_n**: Expression to be used for key.
  Can be a constant, column, or function, any combination of arithmetic or
  string operators, or a named expression of previous listed.
- **value_n**: Expression to be used for value.
  Can be a constant, column, or function, any combination of arithmetic or
  string operators, or a named expression of previous listed.

#### Example

```
SELECT MAKE_MAP('POST', 41, 'HEAD', 33, 'PATCH', null);
----
{POST: 41, HEAD: 33, PATCH: }
```

### `map_extract`

Return a list containing the value for a given key or an empty list if the key is not contained in the map.

```
map_extract(map, key)
```

#### Arguments

- `map`: Map expression.
  Can be a constant, column, or function, and any combination of map operators.
- `key`: Key to extract from the map.
  Can be a constant, column, or function, any combination of arithmetic or
  string operators, or a named expression of previous listed.

#### Example

```
SELECT map_extract(MAP {'a': 1, 'b': NULL, 'c': 3}, 'a');
----
[1]
```

#### Aliases

- element_at

### `map_keys`

Return a list of all keys in the map.

```
map_keys(map)
```

#### Arguments

- `map`: Map expression.
  Can be a constant, column, or function, and any combination of map operators.

#### Example

```
SELECT map_keys(MAP {'a': 1, 'b': NULL, 'c': 3});
----
[a, b, c]

select map_keys(map([100, 5], [42,43]));
----
[100, 5]
```

### `map_values`

Return a list of all values in the map.

```
map_values(map)
```

#### Arguments

- `map`: Map expression.
  Can be a constant, column, or function, and any combination of map operators.

#### Example

```
SELECT map_values(MAP {'a': 1, 'b': NULL, 'c': 3});
----
[1, , 3]

select map_values(map([100, 5], [42,43]));
----
[42, 43]
```

## Other Functions

See the new documentation [`here`](https://datafusion.apache.org/user-guide/sql/scalar_functions_new.html)
