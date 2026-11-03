---
name: add-benchmarks
description: Guidelines for designing or extending DataFusion benchmarks.
---

# Benchmark Design

Follow these guidelines when designing or adding a new benchmark.

## Design principles

### Use the highest-level interface

Use the highest-level interface possible, preferably SQL, while keeping the work
around the operator being measured as cheap as possible. When benchmarking a
function, exercise its evaluation path instead of benchmarking internal utility
functions in isolation.

This makes benchmarks easier to maintain and helps assess how much an optimization
matters to end-to-end runtime. It also helps avoid spending time optimizing code
that accounts for only a small fraction of the total runtime.

Practical criteria for choosing SQL or Rust benchmarks: Try implementing the benchmark in SQL first. If a Rust microbenchmark still seems like a better fit, use Criterion.

### Vary the key workload axes

First identify the key axes to vary. For example, for a join benchmark:

- Input size on each side.
- Join-filter selectivity.
- ...

Then choose benchmark cases that exercise representative variations. Full
combinatorial coverage is unnecessary; focus on typical workloads that reflect
real use cases.

When adding benchmark queries, simply tag each query with the decision made
for every axis. For example, for a join benchmark with input sizes/filter selectivity
to tune:

```sql
-- Q1: Small left input, large right input; 0.1% of pairs match.
SELECT *
FROM generate_series(1, 100) AS l
JOIN generate_series(1, 100000) AS r
  ON (l.value + r.value) % 1000 = 0;

-- Q2: Medium inputs on both sides; no filter.
SELECT *
FROM generate_series(1, 1000) AS l
CROSS JOIN generate_series(1, 1000) AS r;
```

## SQL benchmarks

For implementation details, see the
[SQL benchmark README](../../../benchmarks/sql_benchmarks/README.md).

1. **Keep other operators cheap.**

   When a SQL benchmark targets a specific operator, keep the work done by
   other operators as lightweight as possible. For example, use a data source
   such as `generate_series()` instead of a Parquet scan so scan overhead does
   not dominate the measurement. See the [`nlj` benchmark](../../../benchmarks/sql_benchmarks/nlj/) for examples.

2. **Integrate with the top-level benchmark script.**

   Ensure the benchmark can be prepared and run through `bench.sh`:

   ```bash
   # Run from the benchmarks directory.

   # Generate any required dataset.
   ./bench.sh data new_bench

   # Run the benchmark.
   ./bench.sh run new_bench
   ```

3. **Keep query runtimes practical.**

   Tune the workload so each query takes roughly a few seconds per execution.
   This helps reduce the relative impact of timing noise while keeping the
   suite practical to run.