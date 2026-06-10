CREATE EXTERNAL TABLE call_center STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/call_center.parquet';

CREATE EXTERNAL TABLE catalog_page STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/catalog_page.parquet';

CREATE EXTERNAL TABLE catalog_returns STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/catalog_returns.parquet';

CREATE EXTERNAL TABLE catalog_sales STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/catalog_sales.parquet';

CREATE EXTERNAL TABLE customer STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/customer.parquet';

CREATE EXTERNAL TABLE customer_address STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/customer_address.parquet';

CREATE EXTERNAL TABLE customer_demographics STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/customer_demographics.parquet';

CREATE EXTERNAL TABLE date_dim STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/date_dim.parquet';

CREATE EXTERNAL TABLE household_demographics STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/household_demographics.parquet';

CREATE EXTERNAL TABLE income_band STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/income_band.parquet';

CREATE EXTERNAL TABLE inventory STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/inventory.parquet';

CREATE EXTERNAL TABLE item STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/item.parquet';

CREATE EXTERNAL TABLE promotion STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/promotion.parquet';

CREATE EXTERNAL TABLE reason STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/reason.parquet';

CREATE EXTERNAL TABLE ship_mode STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/ship_mode.parquet';

CREATE EXTERNAL TABLE store STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/store.parquet';

CREATE EXTERNAL TABLE store_returns STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/store_returns.parquet';

CREATE EXTERNAL TABLE store_sales STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/store_sales.parquet';

CREATE EXTERNAL TABLE time_dim STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/time_dim.parquet';

CREATE EXTERNAL TABLE warehouse STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/warehouse.parquet';

CREATE EXTERNAL TABLE web_page STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/web_page.parquet';

CREATE EXTERNAL TABLE web_returns STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/web_returns.parquet';

CREATE EXTERNAL TABLE web_sales STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/web_sales.parquet';

CREATE EXTERNAL TABLE web_site STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/web_site.parquet';
