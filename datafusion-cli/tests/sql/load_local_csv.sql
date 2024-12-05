CREATE EXTERNAL TABLE cars
STORED AS CSV
LOCATION '../datafusion/core/tests/data/cars.csv'
OPTIONS ('has_header' 'TRUE');

select * from cars limit 1;