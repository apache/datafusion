<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Development instructions

## Running Tests

Tests can be run using `cargo`

```shell
cargo test
```

## Running Storage Integration Tests

By default, storage integration tests are not run. To run them you will need to set `TEST_STORAGE_INTEGRATION=1` and 
then provide the necessary configuration for that object store.

### AWS

To test the S3 integration against [Minio](https://github.com/minio/minio)

First start up a container with Minio

```
$ LOCALSTACK_VERSION=sha256:a0b79cb2430f1818de2c66ce89d41bba40f5a1823410f5a7eaf3494b692eed97
$ podman run -d -p 4566:4566 localstack/localstack@$LOCALSTACK_VERSION
$ podman run -d -p 1338:1338 amazon/amazon-ec2-metadata-mock:v1.9.2 --imdsv2
```

Setup environment

```
export TEST_INTEGRATION=1
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_ENDPOINT=http://localhost:4566
export AWS_ALLOW_HTTP=true
export AWS_BUCKET_NAME=test-bucket
```

Create a bucket using the AWS CLI

```
podman run --net=host --env-host amazon/aws-cli --endpoint-url=http://localhost:4566 s3 mb s3://test-bucket
```

Run tests

```
$ cargo test --features aws
```

