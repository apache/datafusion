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
- [count](#count)
- [max](#max)
- [mean](#mean)
- [min](#min)
- [sum](#sum)
- [array_agg](#array_agg)

### `avg`

Returns the average of numeric values in the specified column.

```
avg(expression)
```

#### Arguments

- **expression**: Column to operate on.

#### Aliases

- `mean`

### `count`

Returns the number of rows in the specified column.

Count includes _null_ values in the total count.
To exclude _null_ values from the total count, include `<column> IS NOT NULL`
in the `WHERE` clause.

```
count(expression)
```

#### Arguments

- **expression**: Column to operate on.

### `max`

Returns the maximum value in the specified column.

```
max(expression)
```

#### Arguments

- **expression**: Column to operate on.

### `mean`

_Alias of [avg](#avg)._

### `min`

Returns the minimum value in the specified column.

```
min(expression)
```

#### Arguments

- **expression**: Column to operate on.

### `sum`

Returns the sum of all values in the specified column.

```
sum(expression)
```

#### Arguments

- **expression**: Column to operate on.

### `array_agg`

<!-- TODO: Add array_agg documentation -->

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

- **expression1**: First column or literal value to operate on.
- **expression2**: Second column or literal value to operate on.

### `covar`

Returns the covariance of a set of number pairs.

```
covar(expression1, expression2)
```

#### Arguments

- **expression1**: First column or literal value to operate on.
- **expression2**: Second column or literal value to operate on.

### `covar_pop`

Returns the population covariance of a set of number pairs.

```
covar_pop(expression1, expression2)
```

#### Arguments

- **expression1**: First column or literal value to operate on.
- **expression2**: Second column or literal value to operate on.

### `covar_samp`

Returns the sample covariance of a set of number pairs.

```
covar_samp(expression1, expression2)
```

#### Arguments

- **expression1**: First column or literal value to operate on.
- **expression2**: Second column or literal value to operate on.

### `stddev`

Returns the standard deviation of a set of numbers.

```
stddev(expression)
```

#### Arguments

- **expression**: Column or literal value to operate on.

### `stddev_pop`

Returns the population standard deviation of a set of numbers.

```
stddev_pop(expression)
```

#### Arguments

- **expression**: Column or literal value to operate on.

### `stddev_samp`

Returns the sample standard deviation of a set of numbers.

```
stddev_samp(expression)
```

#### Arguments

- **expression**: Column or literal value to operate on.

### `var`

Returns the statistical variance of a set of numbers.

```
var(expression)
```

#### Arguments

- **expression**: Column or literal value to operate on.

### `var_pop`

Returns the statistical population variance of a set of numbers.

```
var_pop(expression)
```

#### Arguments

- **expression**: Column or literal value to operate on.

### `var_samp`

Returns the statistical sample variance of a set of numbers.

```
var_samp(expression)
```

#### Arguments

- **expression**: Column or literal value to operate on.

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

- **expression**: Column or literal value to operate on.

### `approx_median`

Returns the approximate median (50th percentile) of input values.
It is an alias of `approx_percentile_cont(x, 0.5)`.

```
approx_median(expression)
```

#### Arguments

- **expression**: Column or literal value to operate on.

### `approx_percentile_cont`

Returns the approximate percentile of input values using the t-digest algorithm.

```
approx_percentile_cont(expression, percentile, centroids)
```

#### Arguments

- **expression**: Column or literal value to operate on.
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

- **expression**: Column or literal value to operate on.
- **weight**: Column or literal value to use as weight.
- **percentile**: Percentile to compute. Must be a float value between 0 and 1 (inclusive).
