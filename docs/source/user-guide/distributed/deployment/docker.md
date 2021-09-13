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

# Starting a Ballista cluster using Docker

## Build Docker image

There is no officially published Docker image so it is currently necessary to build the image from source instead.

Run the following commands to clone the source repository and build the Docker image.

```bash
git clone git@github.com:apache/arrow-datafusion.git -b 5.1.0
cd arrow-datafusion
./dev/build-ballista-docker.sh
```

This will create an image with the tag `ballista:0.6.0`.

### Start a Scheduler

Start a scheduler using the following syntax:

```bash
docker run --network=host \
  -d ballista:0.6.0 \
  /scheduler --bind-port 50050
```

Run `docker ps` to check that the process is running:

```
$ docker ps
CONTAINER ID   IMAGE            COMMAND                  CREATED         STATUS        PORTS     NAMES
1f3f8b5ed93a   ballista:0.6.0   "/scheduler --bind-p…"   2 seconds ago   Up 1 second             tender_archimedes
```

Run `docker logs CONTAINER_ID` to check the output from the process:

```
$ docker logs 1f3f8b5ed93a
[2021-08-28T15:45:11Z INFO  ballista_scheduler] Ballista v0.6.0 Scheduler listening on 0.0.0.0:50050
```

### Start executors

Start one or more executor processes. Each executor process will need to listen on a different port.

```bash
docker run --network=host \
  -d ballista:0.6.0 \
  /executor --external-host localhost --bind-port 50051
```

Use `docker ps` to check that both the scheduer and executor(s) are now running:

```
$ docker ps
CONTAINER ID   IMAGE            COMMAND                  CREATED          STATUS          PORTS     NAMES
7c6941bb8dc0   ballista:0.6.0   "/executor --externa…"   3 seconds ago    Up 2 seconds              tender_goldberg
1f3f8b5ed93a   ballista:0.6.0   "/scheduler --bind-p…"   50 seconds ago   Up 49 seconds             tender_archimedes
```

Use `docker logs CONTAINER_ID` to check the output from the executor(s):

```
$ docker logs 7c6941bb8dc0
[2021-08-28T15:45:58Z INFO  ballista_executor] Running with config:
[2021-08-28T15:45:58Z INFO  ballista_executor] work_dir: /tmp/.tmpeyEM76
[2021-08-28T15:45:58Z INFO  ballista_executor] concurrent_tasks: 4
[2021-08-28T15:45:58Z INFO  ballista_executor] Ballista v0.6.0 Rust Executor listening on 0.0.0.0:50051
```

### Using etcd as backing store

_NOTE: This functionality is currently experimental_

Ballista can optionally use [etcd](https://etcd.io/) as a backing store for the scheduler. Use the following commands
to launch the scheduler with this option enabled.

```bash
docker run --network=host \
  -d ballista:0.6.0 \
  /scheduler --bind-port 50050 \
  --config-backend etcd \
  --etcd-urls etcd:2379
```

Please refer to the [etcd](https://etcd.io/) web site for installation instructions. Etcd version 3.4.9 or later is
recommended.
