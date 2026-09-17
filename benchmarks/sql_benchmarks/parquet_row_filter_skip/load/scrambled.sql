-- Scrambled control dataset for the fully-matched RowFilter-skip benchmark.
--
-- Same schema, key domain, and payload as clustered.sql, but `skey` is a
-- pseudo-random permutation of 1..PRED_ROWS (999983 is prime, so the map
-- `v -> (v * 999983) % PRED_ROWS + 1` is a bijection whenever PRED_ROWS is
-- not a multiple of 999983 -- true for the default and any power-of-ten
-- size). Every row group therefore spans nearly the whole key range and no
-- row group is ever fully matched by min/max statistics: the per-row
-- RowFilter must run everywhere. Queries over this dataset measure the
-- overhead of the fully-matched check when it can never fire.
--
-- Keeping the domain exactly 1..PRED_ROWS lets the template's row-count
-- asserts hold for both datasets.
--
-- Knobs: PRED_ROWS (row count, must exceed the 100_000 predicate cutoff),
-- RG_SIZE (parquet row-group size).
COPY (
  SELECT
    lpad(CAST((value * 999983) % ${PRED_ROWS:-10000000} + 1 AS VARCHAR), 10, '0') AS skey,
    (value * 7) % 1000000 AS p0,
    (value * 13) % 1000000 AS p1,
    (value * 17) % 1000000 AS p2,
    (value * 19) % 1000000 AS p3,
    (value * 23) % 1000000 AS p4,
    (value * 29) % 1000000 AS p5,
    (value * 31) % 1000000 AS p6,
    (value * 37) % 1000000 AS p7,
    (value * 41) % 1000000 AS p8,
    (value * 43) % 1000000 AS p9,
    (value * 47) % 1000000 AS p10,
    (value * 53) % 1000000 AS p11,
    (value * 59) % 1000000 AS p12,
    (value * 61) % 1000000 AS p13
  FROM generate_series(1, ${PRED_ROWS:-10000000})
  ORDER BY value
)
TO 'sql_benchmarks/parquet_row_filter_skip/scratch/scrambled.parquet'
STORED AS PARQUET
OPTIONS ('format.max_row_group_size' '${RG_SIZE:-1000000}');

CREATE EXTERNAL TABLE t
STORED AS PARQUET
LOCATION 'sql_benchmarks/parquet_row_filter_skip/scratch/scrambled.parquet';
