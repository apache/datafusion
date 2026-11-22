-- Materializes a pre-sorted copy of the h2o window table that *declares* its
-- ordering, for the `window_sorted` subgroup.
--
-- Why this exists: the `window` subgroup partitions by an expression
-- (`id3 % N`) and, in its heavy-ties queries, orders by another (`v2 % 10`).
-- `WITH ORDER` can only name columns, so both are materialized here as `pk`
-- and `ob`. The queries then partition and order by plain columns, and the
-- declared ordering can satisfy the window's requirement.
--
-- Parameters, supplied by window_sorted.benchmark.template so each benchmark
-- writes only the shape it measures:
--   PK_MOD   partition count            (100, 1000, 10000, 100000)
--   OB_EXPR  ORDER BY value expression  (`v2` distinct, integer-cast for ties)
--
-- Note `v2` is Float64 here, so a plain `v2 % 10` is float modulo and stays
-- near-unique. Producing genuine ties needs an integer cast first; the template
-- asserts ob's resulting cardinality so a query cannot get this wrong silently.
--
-- The ORDER BY is load-bearing: the file has to be written in (pk, ob DESC)
-- order for the WITH ORDER declaration below to be true. A declaration that
-- does not match the file is not a planning error, it silently produces wrong
-- results, so this ORDER BY and that WITH ORDER must be edited together.
--
-- `load` runs before `init` and is not timed, so this sort never enters the
-- measurement.
DROP TABLE IF EXISTS x_sorted;

COPY (
  SELECT id3 % ${PK_MOD:-1000} AS pk, ${OB_EXPR:-v2} AS ob
  FROM x
  WHERE v2 IS NOT NULL
  ORDER BY pk ASC, ob DESC
)
TO 'sql_benchmarks/h2o/scratch/window_sorted.parquet'
STORED AS PARQUET;

CREATE EXTERNAL TABLE x_sorted
STORED AS PARQUET
WITH ORDER (pk ASC, ob DESC)
LOCATION 'sql_benchmarks/h2o/scratch/window_sorted.parquet';