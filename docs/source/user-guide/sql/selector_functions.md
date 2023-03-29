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

# Selector Functions

Selector functions are designed to work with time series data.
They behave similarly to aggregate functions in that they take a collection of
data and return a single value.
However, selectors are unique in that they return an
[Arrow _struct_](https://arrow.apache.org/docs/format/Columnar.html#struct-layout)
that contains `time` and `value` fields.

## Selector struct schema

The struct returned from a selector function has two fields:

- **time**: `time` value in the selected row
- **value**: value of the specified column in the selected row

```rst
{time: 2023-01-01T00:00:00Z, value: 72.1}
```

## Selector functions in use

Use _bracket notation_ to reference fields of the
[returned struct](#selector-struct-schema) to populate the column value:

```sql
SELECT
  selector_first(temp, time)['time'] AS time,
  selector_first(temp, time)['value'] AS temp,
  room
FROM home
GROUP BY room
```

## Available selector functions

- [selector_min](#selector_min)
- [selector_max](#selector_max)
- [selector_first](#selector_first)
- [selector_last](#selector_last)

### `selector_min`

Returns the smallest value of a selected column and a timestamp.

```sql
selector_min(expression, timestamp)
```

#### Arguments

- **expression**: Column or literal value to operate on.
- **timestamp**: Time column or timestamp literal.

### `selector_max`

Returns the largest value of a selected column and a timestamp.

```sql
selector_max(expression, timestamp)
```

#### Arguments

- **expression**: Column or literal value to operate on.
- **timestamp**: Time column or timestamp literal.

### `selector_first`

Returns the first value ordered by time ascending.

```sql
selector_first(expression, timestamp)
```

#### Arguments

- **expression**: Column or literal value to operate on.
- **timestamp**: Time column or timestamp literal.

### `selector_last`

Returns the last value ordered by time ascending.

```sql
selector_last(expression, timestamp)
```

#### Arguments

- **expression**: Column or literal value to operate on.
- **timestamp**: Time column or timestamp literal.
