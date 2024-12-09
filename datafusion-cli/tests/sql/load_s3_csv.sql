CREATE EXTERNAL TABLE CARS
STORED AS CSV
OPTIONS(
    'aws.access_key_id' 'test',
    'aws.secret_access_key' 'test',
    'aws.region' 'us-east-1',
    'aws.endpoint' 'http://localhost:4566',
    'aws.allow_http' 'true'
)
LOCATION 's3://test-bucket/cars.csv';

SELECT * FROM CARS limit 1;