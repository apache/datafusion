-- Registers the events table, picking the dataset based on BENCH_SUBGROUP:
--
--   BENCH_SUBGROUP=wide   → 1024-col synthetic dataset (the actual benchmark)
--   BENCH_SUBGROUP=narrow → 8-col baseline (companion only — meaningful
--                           only when compared to the wide numbers)
CREATE EXTERNAL TABLE events STORED AS PARQUET LOCATION 'data/wide_schema/${BENCH_SUBGROUP:-wide}/';
