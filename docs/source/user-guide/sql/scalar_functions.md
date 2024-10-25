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

- [range](#range)

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

## Other Functions

See the new documentation [`here`](https://datafusion.apache.org/user-guide/sql/scalar_functions_new.html)
