CREATE TABLE aggregate_data AS
SELECT
  -- Keep this key high-cardinality and stable when AGGREGATE_ROWS changes.
  CASE
    WHEN value % 97 = 0 THEN NULL
    ELSE (value * 8191 + 104729) % 2147483647
  END AS i64_high_cardinality,
  -- Derive the low-cardinality key from each lane's ordinal so every key
  -- occurs in all four UNION ALL partitions.
  'key-' || LPAD(
    CAST(
      ((((value - 1) / 4) * 13 + (value - 1) % 4) % 32) AS VARCHAR
    ),
    2,
    '0'
  ) AS utf8_low_cardinality,
  'employee-' || CAST(value AS VARCHAR) AS utf8_value
FROM (
  SELECT value FROM generate_series(1, ${AGGREGATE_ROWS:-1000000}, 4)
  UNION ALL
  SELECT value FROM generate_series(2, ${AGGREGATE_ROWS:-1000000}, 4)
  UNION ALL
  SELECT value FROM generate_series(3, ${AGGREGATE_ROWS:-1000000}, 4)
  UNION ALL
  SELECT value FROM generate_series(4, ${AGGREGATE_ROWS:-1000000}, 4)
) partitioned_data;
