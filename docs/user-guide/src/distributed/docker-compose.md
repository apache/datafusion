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

# Starting a Ballista cluster using Docker Compose

Docker Compose is a convenient way to launch a cluster when testing locally.

## Build Docker image

There is no officially published Docker image so it is currently necessary to build the image from source instead.

Run the following commands to clone the source repository and build the Docker image.

```bash
git clone git@github.com:apache/arrow-datafusion.git -b 5.1.0
cd arrow-datafusion
./dev/build-ballista-docker.sh
```

This will create an image with the tag `ballista:0.6.0`.

## Start a cluster

The following Docker Compose example demonstrates how to start a cluster using one scheduler process and one
executor process, with the scheduler using etcd as a backing store. A data volume is mounted into each container
so that Ballista can access the host file system.

```yaml
version: "2.2"
services:
  etcd:
    image: quay.io/coreos/etcd:v3.4.9
    command: "etcd -advertise-client-urls http://etcd:2379 -listen-client-urls http://0.0.0.0:2379"
  ballista-scheduler:
    image: ballista:0.6.0
    command: "/scheduler --config-backend etcd --etcd-urls etcd:2379 --bind-host 0.0.0.0 --bind-port 50050"
    ports:
      - "50050:50050"
    environment:
      - RUST_LOG=info
    volumes:
      - ./data:/data
    depends_on:
      - etcd
  ballista-executor:
    image: ballista:0.6.0
    command: "/executor --bind-host 0.0.0.0 --bind-port 50051 --scheduler-host ballista-scheduler"
    ports:
      - "50051:50051"
    environment:
      - RUST_LOG=info
    volumes:
      - ./data:/data
    depends_on:
      - ballista-scheduler
```

With the above content saved to a `docker-compose.yaml` file, the following command can be used to start the single
node cluster.

```bash
docker-compose up
```

This should show output similar to the following:

```bash
$ docker-compose up
Creating network "ballista-benchmarks_default" with the default driver
Creating ballista-benchmarks_etcd_1 ... done
Creating ballista-benchmarks_ballista-scheduler_1 ... done
Creating ballista-benchmarks_ballista-executor_1  ... done
Attaching to ballista-benchmarks_etcd_1, ballista-benchmarks_ballista-scheduler_1, ballista-benchmarks_ballista-executor_1
ballista-executor_1   | [2021-08-28T15:55:22Z INFO  ballista_executor] Running with config:
ballista-executor_1   | [2021-08-28T15:55:22Z INFO  ballista_executor] work_dir: /tmp/.tmpLVx39c
ballista-executor_1   | [2021-08-28T15:55:22Z INFO  ballista_executor] concurrent_tasks: 4
ballista-scheduler_1  | [2021-08-28T15:55:22Z INFO  ballista_scheduler] Ballista v0.6.0 Scheduler listening on 0.0.0.0:50050
ballista-executor_1   | [2021-08-28T15:55:22Z INFO  ballista_executor] Ballista v0.6.0 Rust Executor listening on 0.0.0.0:50051
```

The scheduler listens on port 50050 and this is the port that clients will need to connect to.
