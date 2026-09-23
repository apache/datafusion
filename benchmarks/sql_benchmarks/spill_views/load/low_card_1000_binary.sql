-- Same as low_card_1000, but stored as binary, so it is read back as
-- BinaryView.
--
-- 1M rows. `id` holds distinct values in shuffled order (7919 is
-- invertible modulo the prime 1000003), so ORDER BY id must really sort.
-- `s` is a 64-byte payload.
COPY (
  SELECT
    (value * 7919) % 1000003 AS id,
    CAST('payload-' || lpad(CAST(value % 1000 AS VARCHAR), 56, '0') AS BYTEA) AS s
  FROM generate_series(1, 1000000)
)
TO 'sql_benchmarks/spill_views/scratch/low_card_1000_binary.parquet'
STORED AS PARQUET;

CREATE EXTERNAL TABLE t
STORED AS PARQUET
LOCATION 'sql_benchmarks/spill_views/scratch/low_card_1000_binary.parquet';
