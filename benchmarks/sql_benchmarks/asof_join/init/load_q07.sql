COPY (
    SELECT value / 10 AS group_key,
           value % 10 + 1 AS ts,
           value AS payload
    FROM range(100000)
    ORDER BY group_key, ts
)
TO 'sql_benchmarks/asof_join/scratch/q07_left.parquet'
STORED AS PARQUET;

COPY (
    SELECT value / 10 AS group_key,
           value % 10 AS ts,
           value AS payload
    FROM range(100000)
    ORDER BY group_key, ts
)
TO 'sql_benchmarks/asof_join/scratch/q07_right.parquet'
STORED AS PARQUET;

CREATE EXTERNAL TABLE asof_left_sorted (
    group_key BIGINT,
    ts BIGINT,
    payload BIGINT
)
STORED AS PARQUET
LOCATION 'sql_benchmarks/asof_join/scratch/q07_left.parquet'
WITH ORDER (group_key ASC NULLS FIRST, ts ASC NULLS FIRST);

CREATE EXTERNAL TABLE asof_right_sorted (
    group_key BIGINT,
    ts BIGINT,
    payload BIGINT
)
STORED AS PARQUET
LOCATION 'sql_benchmarks/asof_join/scratch/q07_right.parquet'
WITH ORDER (group_key ASC NULLS FIRST, ts ASC NULLS FIRST);
