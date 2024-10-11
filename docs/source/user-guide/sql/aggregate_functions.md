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

Note: this documentation is in the process of being migrated to be [automatically created from the codebase].
Please see the [Aggregate Functions (new)](aggregate_functions_new.md) page for
the rest of the documentation.

[automatically created from the codebase]: https://github.com/apache/datafusion/issues/12740

## Statistical

- [covar](#covar)
- [regr_avgx](#regr_avgx)
- [regr_avgy](#regr_avgy)
- [regr_count](#regr_count)
- [regr_intercept](#regr_intercept)
- [regr_r2](#regr_r2)
- [regr_slope](#regr_slope)
- [regr_sxx](#regr_sxx)
- [regr_syy](#regr_syy)
- [regr_sxy](#regr_sxy)

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

### `regr_slope`

Returns the slope of the linear regression line for non-null pairs in aggregate columns.
Given input column Y and X: regr_slope(Y, X) returns the slope (k in Y = k\*X + b) using minimal RSS fitting.

```
regr_slope(expression1, expression2)
```

#### Arguments

- **expression_y**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression_x**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `regr_avgx`

Computes the average of the independent variable (input) `expression_x` for the non-null paired data points.

```
regr_avgx(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression_x**: Independent variable.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `regr_avgy`

Computes the average of the dependent variable (output) `expression_y` for the non-null paired data points.

```
regr_avgy(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression_x**: Independent variable.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `regr_count`

Counts the number of non-null paired data points.

```
regr_count(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression_x**: Independent variable.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `regr_intercept`

Computes the y-intercept of the linear regression line. For the equation \(y = kx + b\), this function returns `b`.

```
regr_intercept(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression_x**: Independent variable.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `regr_r2`

Computes the square of the correlation coefficient between the independent and dependent variables.

```
regr_r2(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression_x**: Independent variable.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `regr_sxx`

Computes the sum of squares of the independent variable.

```
regr_sxx(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression_x**: Independent variable.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `regr_syy`

Computes the sum of squares of the dependent variable.

```
regr_syy(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression_x**: Independent variable.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `regr_sxy`

Computes the sum of products of paired data points.

```
regr_sxy(expression_y, expression_x)
```

#### Arguments

- **expression_y**: Dependent variable.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression_x**: Independent variable.
  Can be a constant, column, or function, and any combination of arithmetic operators.
