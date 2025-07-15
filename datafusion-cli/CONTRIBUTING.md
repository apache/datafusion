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

By default, storage integration tests are not run. These test use the `testcontainers` crate to start up a local MinIO server using docker on port 9000.

To run them you will need to set `TEST_STORAGE_INTEGRATION`:

```shell
TEST_STORAGE_INTEGRATION=1 cargo test
```

For some of the tests, [snapshots](https://datafusion.apache.org/contributor-guide/testing.html#snapshot-testing) are used.

### AWS

S3 integration is tested against [Minio](https://github.com/minio/minio) with [TestContainers](https://github.com/testcontainers/testcontainers-rs)
This requires Docker to be running on your machine and port 9000 to be free.

If you see an error mentioning "failed to load IMDS session token" such as

> ---- object_storage::tests::s3_object_store_builder_resolves_region_when_none_provided stdout ----
> Error: ObjectStore(Generic { store: "S3", source: "Error getting credentials from provider: an error occurred while loading credentials: failed to load IMDS session token" })

You my need to disable trying to fetch S3 credentials from the environment using the `AWS_EC2_METADATA_DISABLED`, for example:

> $ AWS_EC2_METADATA_DISABLED=true TEST_STORAGE_INTEGRATION=1 cargo test
