CREATE EXTERNAL TABLE CARS
STORED AS CSV
OPTIONS(
    'aws.access_key_id' 'DataFusionLogin',
    'aws.secret_access_key' 'DataFusionPassword',
    'aws.endpoint' 'http://localhost:9000',
    'aws.allow_http' 'true'
)
LOCATION 's3://datafusion/cars.csv';

SELECT * FROM CARS limit 1;