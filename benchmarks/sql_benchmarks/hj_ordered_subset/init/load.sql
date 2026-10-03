-- Data for the hj_ordered_subset suite.
--
-- `events` is the probe (fact) table: HJOS_DAYS days x HJOS_ROWS_PER_DAY rows.
-- `event_id` increases with time, and the file is written in `event_id`
-- order, so each row group holds a disjoint, sorted range of `event_id` and
-- of `day`.
--
-- The two build tables hold one row for every event:
--   - `labels_by_day` is sorted by `event_id` (thus by `day`). A filter on
--     `day` selects a contiguous, ordered range of `event_id`.
--   - `labels_by_bucket` is sorted by a pseudo-random `bucket` (0..99). A
--     filter on `bucket` selects every 100th `event_id`, spread over the whole
--     range.
-- Both build filters are pruned by the static row-group statistics of the
-- build table, so the build scan is cheap and the same for every query shape.
--
-- The ORDER BY clauses are load-bearing: the benchmark measures row-group
-- pruning of `events` by the join's dynamic filter, which needs disjoint
-- row-group ranges.
--
-- Knobs: HJOS_DAYS (must be at least 50), HJOS_ROWS_PER_DAY, HJOS_RG_SIZE
-- (parquet max row-group size).
COPY (
  SELECT
    value AS event_id,
    CAST(value / ${HJOS_ROWS_PER_DAY:-200000} AS INT) AS day,
    (value * 7919) % 1000003 AS user_id,
    (value * 37) % 10000 AS amount_cents,
    (value * 13) % 1000000 AS v1,
    concat('note-', CAST(value % 997 AS VARCHAR)) AS note
  FROM generate_series(0, ${HJOS_DAYS:-100} * ${HJOS_ROWS_PER_DAY:-200000} - 1)
  ORDER BY value
)
TO 'sql_benchmarks/hj_ordered_subset/scratch/events.parquet'
STORED AS PARQUET
OPTIONS ('format.max_row_group_size' '${HJOS_RG_SIZE:-100000}');

COPY (
  SELECT
    value AS event_id,
    CAST(value / ${HJOS_ROWS_PER_DAY:-200000} AS INT) AS day
  FROM generate_series(0, ${HJOS_DAYS:-100} * ${HJOS_ROWS_PER_DAY:-200000} - 1)
  ORDER BY value
)
TO 'sql_benchmarks/hj_ordered_subset/scratch/labels_by_day.parquet'
STORED AS PARQUET
OPTIONS ('format.max_row_group_size' '${HJOS_RG_SIZE:-100000}');

-- 7919 is coprime to 100, so `bucket` is a bijection of `value % 100`.
COPY (
  SELECT
    value AS event_id,
    CAST((value * 7919) % 100 AS INT) AS bucket
  FROM generate_series(0, ${HJOS_DAYS:-100} * ${HJOS_ROWS_PER_DAY:-200000} - 1)
  ORDER BY bucket, value
)
TO 'sql_benchmarks/hj_ordered_subset/scratch/labels_by_bucket.parquet'
STORED AS PARQUET
OPTIONS ('format.max_row_group_size' '${HJOS_RG_SIZE:-100000}');

CREATE EXTERNAL TABLE events
STORED AS PARQUET
LOCATION 'sql_benchmarks/hj_ordered_subset/scratch/events.parquet';

CREATE EXTERNAL TABLE labels_by_day
STORED AS PARQUET
LOCATION 'sql_benchmarks/hj_ordered_subset/scratch/labels_by_day.parquet';

CREATE EXTERNAL TABLE labels_by_bucket
STORED AS PARQUET
LOCATION 'sql_benchmarks/hj_ordered_subset/scratch/labels_by_bucket.parquet';
