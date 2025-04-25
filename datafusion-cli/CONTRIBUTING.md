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

For some of the tests, [snapshots](https://datafusion.apache.org/contributor-guide/testing.html#snapshot-testing) are used.

### AWS

To test the S3 integration against [Minio](https://github.com/minio/minio)

First start up a container with Minio and load test files.

```shell
docker run -d \
  --name datafusion-test-minio \
  -p 9000:9000 \
  -e MINIO_ROOT_USER=TEST-DataFusionLogin \
  -e MINIO_ROOT_PASSWORD=TEST-DataFusionPassword \
  -v $(pwd)/../datafusion/core/tests/data:/source \
  quay.io/minio/minio server /data

docker exec datafusion-test-minio /bin/sh -c "\
  mc ready local
  mc alias set localminio http://localhost:9000 TEST-DataFusionLogin TEST-DataFusionPassword && \
  mc mb localminio/data && \
  mc cp -r /source/* localminio/data"
```

Setup environment

```shell
export TEST_STORAGE_INTEGRATION=1
export AWS_ACCESS_KEY_ID=TEST-DataFusionLogin
export AWS_SECRET_ACCESS_KEY=TEST-DataFusionPassword
export AWS_ENDPOINT=http://127.0.0.1:9000
export AWS_ALLOW_HTTP=true
```

Note that `AWS_ENDPOINT` is set without slash at the end.

Run tests

```shell
cargo test
```
