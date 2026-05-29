CREATE EXTERNAL TABLE nation STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/nation/nation.1.parquet';

CREATE EXTERNAL TABLE supplier STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/supplier/supplier.1.parquet';

CREATE EXTERNAL TABLE customer STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/customer/customer.1.parquet';

CREATE EXTERNAL TABLE lineitem STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/lineitem/lineitem.1.parquet';