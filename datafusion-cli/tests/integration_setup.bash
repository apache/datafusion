# you should have localstack up, e.g by
#$ LOCALSTACK_VERSION=sha256:a0b79cb2430f1818de2c66ce89d41bba40f5a1823410f5a7eaf3494b692eed97
#$ podman run -d -p 4566:4566 localstack/localstack@$LOCALSTACK_VERSION
#$ podman run -d -p 1338:1338 amazon/amazon-ec2-metadata-mock:v1.9.2 --imdsv2

export TEST_INTEGRATION=1
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_ENDPOINT=http://localhost:4566
export AWS_ALLOW_HTTP=true
export AWS_BUCKET_NAME=test-bucket


aws s3 mb s3://test-bucket --endpoint-url=$AWS_ENDPOINT
aws s3 cp ../datafusion/core/tests/data/cars.csv s3://test-bucket/cars.csv --endpoint-url=$AWS_ENDPOINT
