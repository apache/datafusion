-- One distinct string value. Parquet dictionary-encodes it, so all views
-- point into one small shared buffer.
--
-- 1M rows. `id` holds distinct values in shuffled order (7919 is
-- invertible modulo the prime 1000003), so ORDER BY id must really sort.
-- `s` is a 64-byte payload.
COPY (
  SELECT
    (value * 7919) % 1000003 AS id,
    'payload-' || lpad('x', 56, '0') AS s
  FROM generate_series(1, 1000000)
)
TO 'sql_benchmarks/spill_views/scratch/low_card_1.parquet'
STORED AS PARQUET;

CREATE EXTERNAL TABLE t
STORED AS PARQUET
LOCATION 'sql_benchmarks/spill_views/scratch/low_card_1.parquet';
