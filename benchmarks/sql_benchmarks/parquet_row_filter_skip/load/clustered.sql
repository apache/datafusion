-- Clustered Parquet dataset for the fully-matched RowFilter-skip benchmark.
--
-- `skey` is a fixed-width, zero-padded, monotonically increasing string, so
-- each row group holds a disjoint, sorted range of keys. With
-- `pushdown_filters=true`, a low-selectivity range predicate (see the query)
-- leaves the first row group straddling and every later row group fully
-- matched by min/max statistics, which is exactly what the per-RG RowFilter
-- skip targets. `skey` is not projected by the query, so the skip also avoids
-- decoding it on the fully-matched run.
--
-- Knobs: PRED_ROWS (row count), RG_SIZE (parquet row-group size).
set datafusion.execution.parquet.pushdown_filters = true;

COPY (
  SELECT
    lpad(CAST(value AS VARCHAR), 10, '0') AS skey,
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
)
TO 'sql_benchmarks/parquet_row_filter_skip/scratch/clustered.parquet'
STORED AS PARQUET
OPTIONS ('format.max_row_group_size' '${RG_SIZE:-1000000}');

CREATE EXTERNAL TABLE t
STORED AS PARQUET
LOCATION 'sql_benchmarks/parquet_row_filter_skip/scratch/clustered.parquet';
