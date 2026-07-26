CREATE EXTERNAL TABLE lineitem_raw STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/lineitem/lineitem.1.parquet';

CREATE TABLE lineitem as (SELECT * FROM lineitem_raw${BENCH_SORTED:-false| order by l_orderkey asc| });