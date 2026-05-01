CREATE EXTERNAL TABLE nation_raw STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/nation/nation.1.parquet';

CREATE EXTERNAL TABLE region_raw STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/region/region.1.parquet';

CREATE EXTERNAL TABLE supplier_raw STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/supplier/supplier.1.parquet';

CREATE EXTERNAL TABLE customer_raw STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/customer/customer.1.parquet';

CREATE EXTERNAL TABLE part_raw STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/part/part.1.parquet';

CREATE EXTERNAL TABLE partsupp_raw STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/partsupp/partsupp.1.parquet';

CREATE EXTERNAL TABLE orders_raw STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/orders/orders.1.parquet';

CREATE EXTERNAL TABLE lineitem_raw STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/lineitem/lineitem.1.parquet';

CREATE TABLE nation as SELECT * FROM nation_raw;

CREATE TABLE region as SELECT * FROM region_raw;

CREATE TABLE supplier as SELECT * FROM supplier_raw;

CREATE TABLE customer as SELECT * FROM customer_raw;

CREATE TABLE part as SELECT * FROM part_raw;

CREATE TABLE partsupp as SELECT * FROM partsupp_raw;

CREATE TABLE orders as SELECT * FROM orders_raw;

CREATE TABLE lineitem as SELECT * FROM lineitem_raw;
