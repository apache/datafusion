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

Aggregate functions operate on a set of values to compute a single result. Please refer to [PostgreSQL](https://www.postgresql.org/docs/current/functions-aggregate.html) for usage of standard SQL functions.

## General

- min
- max
- count
- avg
- sum
- array_agg

## Statistical

- var / var_samp / var_pop
- stddev / stddev_samp / stddev_pop
- covar / covar_samp / covar_pop
- corr

## Approximate

### approx_distinct

`approx_distinct(x) -> uint64` returns the approximate number (HyperLogLog) of distinct input values

### approx_median

`approx_median(x) -> x` returns the approximate median of input values. it is an alias of `approx_percentile_cont(x, 0.5)`.

### approx_percentile_cont

`approx_percentile_cont(x, p) -> x` return the approximate percentile (TDigest) of input values, where `p` is a float64 between 0 and 1 (inclusive).

It supports raw data as input and build Tdigest sketches during query time, and is approximately equal to `approx_percentile_cont_with_weight(x, 1, p)`.

`approx_percentile_cont(x, p, n) -> x` return the approximate percentile (TDigest) of input values, where `p` is a float64 between 0 and 1 (inclusive),

and `n` (default 100) is the number of centroids in Tdigest which means that if there are `n` or fewer unique values in `x`, you can expect an exact result.

A higher value of `n` results in a more accurate approximation and the cost of higher memory usage.

### approx_percentile_cont_with_weight

`approx_percentile_cont_with_weight(x, w, p) -> x` returns the approximate percentile (TDigest) of input values with weight, where `w` is weight column expression and `p` is a float64 between 0 and 1 (inclusive).

It supports raw data as input or pre-aggregated TDigest sketches, then builds or merges Tdigest sketches during query time. TDigest sketches are a list of centroid `(x, w)`, where `x` stands for mean and `w` stands for weight.

It is suitable for low latency OLAP system where a streaming compute engine (e.g. Spark Streaming/Flink) pre-aggregates data to a data store, then queries using Datafusion.
