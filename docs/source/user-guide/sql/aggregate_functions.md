# Aggregate Functions

Aggregate functions operate on a set of values to compute a single result.

## General

### min
`min(x) -> x` returns the minimum value of all input values.

### max
`max(x) -> x` returns the maximum value of all input values.

### count
`count(1) -> uint64` returns the number of input values.

`count(*) -> uint64` returns the number of input values.

`count(x) -> uint64` returns the number of non-null input values.

`count(distinct x) -> uint64` returns the number of non-null distinct input values.

### avg
`avg(x) -> float64` returns the average (arithmetic mean) of input values.

### sum
`sum(x) -> same as x` returns the sum of all input values.

### array_agg
`array_agg(x) -> array<x>` returns an array created from the input values.

## Approximate

### approx_distinct
`approx_distinct(x) -> uint64` returns the approximate number (HyperLogLog) of distinct input values

### approx_median
`approx_median(x) -> x` returns the approximate median of input values.

it is same as `approx_percentile_cont(x, 0.5)`.

### approx_percentile_cont
`approx_percentile_cont(x, p) -> x` return the approximate percentile (TDigest) of input values, where `p` is a float64 between 0 and 1 (inclusive).

it supports raw data as input and build Tdigest sketches during query time.

### approx_percentile_cont_with_weight
`approx_percentile_cont_with_weight(x, w, p) -> x` return the approximate percentile (TDigest) of input values with weight, where `w` is weight column expression and `p` is a float64 between 0 and 1 (inclusive).

it supports raw data as input or pre-aggregated TDigest sketches (mean and weight), then build and merge Tdigest sketches during query time.
it is suitable for low latency query OLAP system where `Spark Streaming/Flink` pre-aggregate data to a data store, then query by Datafusion.

## Statistical

### var
var
var_samp
var_pop

### stddev
stddev
stddev_samp
stddev_pop

### covar
covar
covar_samp
covar_pop

### corr
