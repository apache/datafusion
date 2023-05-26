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

# Aggregate Functions

Aggregate functions operate on a set of values to compute a single result.

## General

- [avg](#avg)
- [bit_and](#bit_and)
- [bit_or](#bit_or)
- [bit_xor](#bit_xor)
- [bool_and](#bool_and)
- [bool_or](#bool_or)
- [count](#count)
- [max](#max)
- [mean](#mean)
- [median](#median)
- [min](#min)
- [sum](#sum)
- [array_agg](#array_agg)
- [first_value](#first_value)
- [last_value](#last_value)

### `avg`

Returns the average of numeric values in the specified column.

```
avg(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

#### Aliases

- `mean`

### `bit_and`

Computes the bitwise AND of all non-null input values.

```
bit_and(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `bit_or`

Computes the bitwise OR of all non-null input values.

```
bit_or(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `bit_xor`

Computes the bitwise exclusive OR of all non-null input values.

```
bit_xor(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `bool_and`

Returns true if all non-null input values are true, otherwise false.

```
bool_and(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `bool_or`

Returns true if any non-null input value is true, otherwise false.

```
bool_or(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `count`

Returns the number of rows in the specified column.

Count includes _null_ values in the total count.
To exclude _null_ values from the total count, include `<column> IS NOT NULL`
in the `WHERE` clause.

```
count(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `max`

Returns the maximum value in the specified column.

```
max(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `mean`

_Alias of [avg](#avg)._

### `median`

Returns the median value in the specified column.

```
median(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `min`

Returns the minimum value in the specified column.

```
min(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `sum`

Returns the sum of all values in the specified column.

```
sum(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `array_agg`

Returns an array created from the expression elements. If ordering requirement is given, elements are inserted in the order of required ordering.

```
array_agg(expression [ORDER BY expression])
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `first_value`

Returns the first element in an aggregation group according to the requested ordering. If no ordering is given, returns an arbitrary element from the group.

```
first_value(expression [ORDER BY expression])
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `last_value`

Returns the last element in an aggregation group according to the requested ordering. If no ordering is given, returns an arbitrary element from the group.

```
last_value(expression [ORDER BY expression])
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

## Statistical

- [corr](#corr)
- [covar](#covar)
- [covar_pop](#covar_pop)
- [covar_samp](#covar_samp)
- [stddev](#stddev)
- [stddev_pop](#stddev_pop)
- [stddev_samp](#stddev_samp)
- [var](#var)
- [var_pop](#var_pop)
- [var_samp](#var_samp)

### `corr`

Returns the coefficient of correlation between two numeric values.

```
corr(expression1, expression2)
```

#### Arguments

- **expression1**: First expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression2**: Second expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `covar`

Returns the covariance of a set of number pairs.

```
covar(expression1, expression2)
```

#### Arguments

- **expression1**: First expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression2**: Second expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `covar_pop`

Returns the population covariance of a set of number pairs.

```
covar_pop(expression1, expression2)
```

#### Arguments

- **expression1**: First expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression2**: Second expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `covar_samp`

Returns the sample covariance of a set of number pairs.

```
covar_samp(expression1, expression2)
```

#### Arguments

- **expression1**: First expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression2**: Second expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `stddev`

Returns the standard deviation of a set of numbers.

```
stddev(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `stddev_pop`

Returns the population standard deviation of a set of numbers.

```
stddev_pop(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `stddev_samp`

Returns the sample standard deviation of a set of numbers.

```
stddev_samp(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `var`

Returns the statistical variance of a set of numbers.

```
var(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `var_pop`

Returns the statistical population variance of a set of numbers.

```
var_pop(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `var_samp`

Returns the statistical sample variance of a set of numbers.

```
var_samp(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

## Approximate

- [approx_distinct](#approx_distinct)
- [approx_median](#approx_median)
- [approx_percentile_cont](#approx_percentile_cont)
- [approx_percentile_cont_with_weight](#approx_percentile_cont_with_weight)

### `approx_distinct`

Returns the approximate number of distinct input values calculated using the
HyperLogLog algorithm.

```
approx_distinct(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `approx_median`

Returns the approximate median (50th percentile) of input values.
It is an alias of `approx_percentile_cont(x, 0.5)`.

```
approx_median(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `approx_percentile_cont`

Returns the approximate percentile of input values using the t-digest algorithm.

```
approx_percentile_cont(expression, percentile, centroids)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **percentile**: Percentile to compute. Must be a float value between 0 and 1 (inclusive).
- **centroids**: Number of centroids to use in the t-digest algorithm. _Default is 100_.

  If there are this number or fewer unique values, you can expect an exact result.
  A higher number of centroids results in a more accurate approximation, but
  requires more memory to compute.

### `approx_percentile_cont_with_weight`

Returns the weighted approximate percentile of input values using the
t-digest algorithm.

```
approx_percentile_cont_with_weight(expression, weight, percentile)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **weight**: Expression to use as weight.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **percentile**: Percentile to compute. Must be a float value between 0 and 1 (inclusive).
