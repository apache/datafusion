CREATE EXTERNAL TABLE nation STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/nation/nation.1.parquet';

CREATE EXTERNAL TABLE region STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/region/region.1.parquet';

CREATE EXTERNAL TABLE supplier STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/supplier/supplier.1.parquet';

CREATE EXTERNAL TABLE customer STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/customer/customer.1.parquet';

CREATE EXTERNAL TABLE part STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/part/part.1.parquet';

CREATE EXTERNAL TABLE partsupp STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/partsupp/partsupp.1.parquet';

CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/orders/orders.1.parquet';

CREATE EXTERNAL TABLE lineitem STORED AS PARQUET LOCATION 'data/tpch_sf${BENCH_SIZE:-1}/lineitem/lineitem.1.parquet';