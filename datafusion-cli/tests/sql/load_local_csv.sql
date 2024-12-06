CREATE EXTERNAL TABLE CARS
STORED AS CSV
LOCATION '../datafusion/core/tests/data/cars.csv'
OPTIONS ('has_header' 'TRUE');

SELECT * FROM CARS limit 1;