CREATE EXTERNAL TABLE cars
STORED AS CSV
OPTIONS(
    'aws.access_key_id' 'test',
    'aws.secret_access_key' 'test',
    'aws.region' 'us-east-1',
    'aws.endpoint' 'http://localhost:4566',
    'aws.allow_http' 'true'
)
LOCATION 's3://test-bucket/cars.csv';

select * from cars limit 1;