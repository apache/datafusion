CREATE EXTERNAL TABLE customer STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/customer/customer.1.parquet';

CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/orders/orders.1.parquet';

CREATE EXTERNAL TABLE nation STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpch_sf${BENCH_SIZE:-1}/nation/nation.1.parquet';
