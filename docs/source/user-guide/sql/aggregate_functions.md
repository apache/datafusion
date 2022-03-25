# Aggregate Functions

Aggregate functions operate on a set of values to compute a single result.

## General

### min
`min(x) -> x` returns the minimum value of all input values.

### max
`max(x) -> x` returns the maximum value of all input values.

### count
`count(1) -> uint64` returns the number of input values.

`count(*) -> uint64` returns the number of input values. it is an alias of `count(1)`.

`count(x) -> uint64` returns the number of non-null input values.

`count(distinct x) -> uint64` returns the number of non-null distinct input values.

### avg
`avg(x) -> float64` returns the average (arithmetic mean) of input values.

### sum
`sum(x) -> x` returns the sum of all input values.

### array_agg
`array_agg(x) -> array<x>` returns an array created from the input values.

## Approximate

### approx_distinct
`approx_distinct(x) -> uint64` returns the approximate number (HyperLogLog) of distinct input values

### approx_median
`approx_median(x) -> x` returns the approximate median of input values. it is an alias of `approx_percentile_cont(x, 0.5)`.

### approx_percentile_cont
`approx_percentile_cont(x, p) -> x` return the approximate percentile (TDigest) of input values, where `p` is a float64 between 0 and 1 (inclusive).

it supports raw data as input and build Tdigest sketches during query time.

it is approximately equal to `approx_percentile_cont_with_weight(x, 1, p)`.

### approx_percentile_cont_with_weight
`approx_percentile_cont_with_weight(x, w, p) -> x` return the approximate percentile (TDigest) of input values with weight, where `w` is weight column expression and `p` is a float64 between 0 and 1 (inclusive).

it supports raw data as input or pre-aggregated TDigest sketches, then build or merge Tdigest sketches during query time. TDigest sketches are a list of centroid `(x, w)`, where `x` stands for mean and `w` stands for weight.

it is suitable for low latency OLAP system where streaming compute engine (e.g. Spark Streaming/Flink) pre-aggregate data to a data store, then query by Datafusion.

## Statistical

### var
`var(x) -> float64` returns the sample variance of all input values. it is an alias of `var_samp(x)`.

`var_samp(x) -> float64` returns the sample variance of all input values.

`var_pop(x) -> float64` returns the population variance of all input values.

### stddev
`stddev(x) -> float64` returns the sample standard deviation of all input values it is an alias of `stddev_samp(x)`.

`stddev_samp(x) -> float64` returns the sample standard deviation of all input values

`stddev_pop(x) -> float64` returns the population standard deviation of all input values

### covar
`covar(x, y) -> float64` returns the sample covariance of input values. it is an alias of `covar_samp(x)`.

`covar_samp(x, y) -> float64` returns the sample covariance of input values.

`covar_pop(x, y) -> float64` returns the population covariance of input values.

### corr
`corr(x, y) -> float64` returns correlation coefficient of input values
