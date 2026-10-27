CREATE EXTERNAL TABLE asof_left_sorted (
    group_key BIGINT,
    ts BIGINT,
    payload BIGINT
)
STORED AS PARQUET
LOCATION '${DATA_DIR:-data}/asof_join/q07_left.parquet'
WITH ORDER (group_key ASC NULLS FIRST, ts ASC NULLS FIRST);

CREATE EXTERNAL TABLE asof_right_sorted (
    group_key BIGINT,
    ts BIGINT,
    payload BIGINT
)
STORED AS PARQUET
LOCATION '${DATA_DIR:-data}/asof_join/q07_right.parquet'
WITH ORDER (group_key ASC NULLS FIRST, ts ASC NULLS FIRST);
