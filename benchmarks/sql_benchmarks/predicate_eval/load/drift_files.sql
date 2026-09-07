-- Per-partition drift dataset: 16 Parquet files, each with a fixed selectivity
-- profile, handed to the scan one file per partition. Which conjunct is the
-- cheaper filter therefore differs from partition to partition instead of
-- drifting over time within one stream, and one global ordering decision is
-- backwards for half of the partitions no matter which way it goes.
--
--   odd-numbered files  (f01, f03, ... f15): `a_sel = 0` is selective (~0.1%)
--                                            and `b_sel = 0` unselective (~50%)
--   even-numbered files (f00, f02, ... f14): the mirror
--
-- The two rates are the ones `drift.sql` and `drift_half.sql` use (`% 1000` for
-- ~0.1%, `% 2` for ~50%), so all four drift queries stay comparable.
--
-- Why Parquet files and not `CREATE TABLE ... AS SELECT`. A MemTable built by
-- CTAS cannot express this shape. The plan for a `generate_series` scan is a
-- single partition fanned out by a `RepartitionExec` with
-- `RoundRobinBatch(target_partitions)`, which deals *whole batches*
-- round-robin: partition `p` ends up holding batches `p, p+P, p+2P, ...`. Any
-- contiguous block of rows is therefore sprayed across every partition, and the
-- block index -- not the partition -- decides which conjunct wins. Writing one
-- file per profile and letting the listing table hand whole files out is what
-- actually pins a profile to a stream.

-- f00: `b_sel = 0` selective, `a_sel = 0` unselective
COPY (
  SELECT
    value AS seq,
    value % 2    AS a_sel,
    value % 1000 AS b_sel
  FROM generate_series(1, ${PRED_ROWS:-1000000} / 16)
  ORDER BY value
)
TO 'sql_benchmarks/predicate_eval/scratch/drift_files/f00.parquet'
STORED AS PARQUET;

-- f01: `a_sel = 0` selective, `b_sel = 0` unselective
COPY (
  SELECT
    value AS seq,
    value % 1000 AS a_sel,
    value % 2    AS b_sel
  FROM generate_series(1, ${PRED_ROWS:-1000000} / 16)
  ORDER BY value
)
TO 'sql_benchmarks/predicate_eval/scratch/drift_files/f01.parquet'
STORED AS PARQUET;

-- f02: `b_sel = 0` selective, `a_sel = 0` unselective
COPY (
  SELECT
    value AS seq,
    value % 2    AS a_sel,
    value % 1000 AS b_sel
  FROM generate_series(1, ${PRED_ROWS:-1000000} / 16)
  ORDER BY value
)
TO 'sql_benchmarks/predicate_eval/scratch/drift_files/f02.parquet'
STORED AS PARQUET;

-- f03: `a_sel = 0` selective, `b_sel = 0` unselective
COPY (
  SELECT
    value AS seq,
    value % 1000 AS a_sel,
    value % 2    AS b_sel
  FROM generate_series(1, ${PRED_ROWS:-1000000} / 16)
  ORDER BY value
)
TO 'sql_benchmarks/predicate_eval/scratch/drift_files/f03.parquet'
STORED AS PARQUET;

-- f04: `b_sel = 0` selective, `a_sel = 0` unselective
COPY (
  SELECT
    value AS seq,
    value % 2    AS a_sel,
    value % 1000 AS b_sel
  FROM generate_series(1, ${PRED_ROWS:-1000000} / 16)
  ORDER BY value
)
TO 'sql_benchmarks/predicate_eval/scratch/drift_files/f04.parquet'
STORED AS PARQUET;

-- f05: `a_sel = 0` selective, `b_sel = 0` unselective
COPY (
  SELECT
    value AS seq,
    value % 1000 AS a_sel,
    value % 2    AS b_sel
  FROM generate_series(1, ${PRED_ROWS:-1000000} / 16)
  ORDER BY value
)
TO 'sql_benchmarks/predicate_eval/scratch/drift_files/f05.parquet'
STORED AS PARQUET;

-- f06: `b_sel = 0` selective, `a_sel = 0` unselective
COPY (
  SELECT
    value AS seq,
    value % 2    AS a_sel,
    value % 1000 AS b_sel
  FROM generate_series(1, ${PRED_ROWS:-1000000} / 16)
  ORDER BY value
)
TO 'sql_benchmarks/predicate_eval/scratch/drift_files/f06.parquet'
STORED AS PARQUET;

-- f07: `a_sel = 0` selective, `b_sel = 0` unselective
COPY (
  SELECT
    value AS seq,
    value % 1000 AS a_sel,
    value % 2    AS b_sel
  FROM generate_series(1, ${PRED_ROWS:-1000000} / 16)
  ORDER BY value
)
TO 'sql_benchmarks/predicate_eval/scratch/drift_files/f07.parquet'
STORED AS PARQUET;

-- f08: `b_sel = 0` selective, `a_sel = 0` unselective
COPY (
  SELECT
    value AS seq,
    value % 2    AS a_sel,
    value % 1000 AS b_sel
  FROM generate_series(1, ${PRED_ROWS:-1000000} / 16)
  ORDER BY value
)
TO 'sql_benchmarks/predicate_eval/scratch/drift_files/f08.parquet'
STORED AS PARQUET;

-- f09: `a_sel = 0` selective, `b_sel = 0` unselective
COPY (
  SELECT
    value AS seq,
    value % 1000 AS a_sel,
    value % 2    AS b_sel
  FROM generate_series(1, ${PRED_ROWS:-1000000} / 16)
  ORDER BY value
)
TO 'sql_benchmarks/predicate_eval/scratch/drift_files/f09.parquet'
STORED AS PARQUET;

-- f10: `b_sel = 0` selective, `a_sel = 0` unselective
COPY (
  SELECT
    value AS seq,
    value % 2    AS a_sel,
    value % 1000 AS b_sel
  FROM generate_series(1, ${PRED_ROWS:-1000000} / 16)
  ORDER BY value
)
TO 'sql_benchmarks/predicate_eval/scratch/drift_files/f10.parquet'
STORED AS PARQUET;

-- f11: `a_sel = 0` selective, `b_sel = 0` unselective
COPY (
  SELECT
    value AS seq,
    value % 1000 AS a_sel,
    value % 2    AS b_sel
  FROM generate_series(1, ${PRED_ROWS:-1000000} / 16)
  ORDER BY value
)
TO 'sql_benchmarks/predicate_eval/scratch/drift_files/f11.parquet'
STORED AS PARQUET;

-- f12: `b_sel = 0` selective, `a_sel = 0` unselective
COPY (
  SELECT
    value AS seq,
    value % 2    AS a_sel,
    value % 1000 AS b_sel
  FROM generate_series(1, ${PRED_ROWS:-1000000} / 16)
  ORDER BY value
)
TO 'sql_benchmarks/predicate_eval/scratch/drift_files/f12.parquet'
STORED AS PARQUET;

-- f13: `a_sel = 0` selective, `b_sel = 0` unselective
COPY (
  SELECT
    value AS seq,
    value % 1000 AS a_sel,
    value % 2    AS b_sel
  FROM generate_series(1, ${PRED_ROWS:-1000000} / 16)
  ORDER BY value
)
TO 'sql_benchmarks/predicate_eval/scratch/drift_files/f13.parquet'
STORED AS PARQUET;

-- f14: `b_sel = 0` selective, `a_sel = 0` unselective
COPY (
  SELECT
    value AS seq,
    value % 2    AS a_sel,
    value % 1000 AS b_sel
  FROM generate_series(1, ${PRED_ROWS:-1000000} / 16)
  ORDER BY value
)
TO 'sql_benchmarks/predicate_eval/scratch/drift_files/f14.parquet'
STORED AS PARQUET;

-- f15: `a_sel = 0` selective, `b_sel = 0` unselective
COPY (
  SELECT
    value AS seq,
    value % 1000 AS a_sel,
    value % 2    AS b_sel
  FROM generate_series(1, ${PRED_ROWS:-1000000} / 16)
  ORDER BY value
)
TO 'sql_benchmarks/predicate_eval/scratch/drift_files/f15.parquet'
STORED AS PARQUET;

-- Read-side settings. `load`, the asserts and the benchmarked `run` all share
-- one `SessionContext` (see `SqlBenchmark::initialize`), so these reach the
-- measured query. They are set after the writes above so the `COPY`s keep their
-- own trivial single-partition plans.
--
-- Both are load-bearing, and neither alone is enough:
--
--   * `target_partitions = 16` matches the file count, so
--     `FileGroup::split_files` (chunks of `files.div_ceil(target_partitions)`)
--     puts exactly one file in each group and `EnforceDistribution` adds no
--     `RoundRobinBatch` on top -- the `FilterExec` sits directly on the scan and
--     each of its streams sees one profile. At the machine default it would not:
--     on a 12-core box 16 files chunk into 8 groups of 2 (one file of each
--     profile per group) and a `RoundRobinBatch(12)` above the scan then deals
--     those batches across every partition. Pinning it also makes the shape the
--     same on every machine, at the cost of over- or under-subscribing cores
--     relative to the other drift queries.
--
--   * `repartition_file_scans = false` stops `FileGroupPartitioner` from
--     re-deriving the groups as byte ranges over the total file bytes, which
--     ignores file identity: at the machine default it produces groups like
--     `[f00:0..224901, f01:0..74967]`, i.e. both profiles in one stream again.
--     With 16 equal files and 16 partitions its boundaries would happen to land
--     on file edges, but that is coincidence, not structure.
--
-- With both set the scan reports whole files and no byte ranges:
--
--   FilterExec: a_sel@0 = 0 AND b_sel@1 = 0, projection=[]
--     DataSourceExec: file_groups={16 groups: [[.../f00.parquet],
--       [.../f01.parquet], [.../f02.parquet], ...]}, ...
--
-- PRED_ROWS sizes the table; each file gets PRED_ROWS / 16 rows, and `seq`
-- restarts at 1 in every file (nothing reads it across files).
set datafusion.execution.target_partitions = 16;
set datafusion.optimizer.repartition_file_scans = false;

CREATE EXTERNAL TABLE t
STORED AS PARQUET
LOCATION 'sql_benchmarks/predicate_eval/scratch/drift_files/';
