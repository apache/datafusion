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

# Running Ballista on Raspberry Pi

The Raspberry Pi single-board computer provides a fun and relatively inexpensive way to get started with distributed
computing.

These instructions have been tested using an Ubuntu Linux desktop as the host, and a
[Raspberry Pi 4 Model B](https://www.raspberrypi.org/products/raspberry-pi-4-model-b/) with 4 GB RAM as the target.

## Preparing the Raspberry Pi

We recommend installing the 64-bit version of [Ubuntu for Raspberry Pi](https://ubuntu.com/raspberry-pi).

The Rust implementation of Arrow does not work correctly on 32-bit ARM architectures
([issue](https://github.com/apache/arrow-rs/issues/109)).

## Cross Compiling DataFusion for the Raspberry Pi

We do not yet publish official Docker images as part of the release process, although we do plan to do this in the
future ([issue #228](https://github.com/apache/arrow-datafusion/issues/228)).

Although it is technically possible to build DataFusion directly on a Raspberry Pi, it really isn't very practical.
It is much faster to use [cross](https://github.com/rust-embedded/cross) to cross-compile from a more powerful
desktop computer.

Docker must be installed and the Docker daemon must be running before cross-compiling with cross. See the
[cross](https://github.com/rust-embedded/cross) project for more detailed instructions.

Run the following command to install cross.

```bash
cargo install cross
```

From the root of the DataFusion project, run the following command to cross-compile for ARM 64 architecture.

```bash
cross build --release --target aarch64-unknown-linux-gnu
```

It is even possible to cross-test from your desktop computer:

```bash
cross test --target aarch64-unknown-linux-gnu
```

## Deploying the binaries to Raspberry Pi

You should now be able to copy the executable to the Raspberry Pi using scp on Linux. You will need to change the IP
address in these commands to be the IP address for your Raspberry Pi. The easiest way to find this is to connect a
keyboard and monitor to the Pi and run `ifconfig`.

```bash
scp ./target/aarch64-unknown-linux-gnu/release/ballista-scheduler ubuntu@10.0.0.186:
scp ./target/aarch64-unknown-linux-gnu/release/ballista-executor ubuntu@10.0.0.186:
```

Finally, ssh into the Pi and make the binaries executable:

```bash
ssh ubuntu@10.0.0.186
chmod +x ballista-scheduler ballista-executor
```

It is now possible to run the Ballista scheduler and executor natively on the Pi.

## Docker

Using Docker's `buildx` cross-platform functionality, we can also build a docker image targeting ARM64
from any desktop environment. This will require write access to a Docker repository
on [Docker Hub](https://hub.docker.com/) because the resulting Docker image will be pushed directly
to the repo.

```bash
DOCKER_REPO=myrepo ./dev/build-ballista-docker-arm64.sh
```

On the Raspberry Pi:

```bash
docker pull myrepo/ballista-arm64
```

Run a scheduler:

```bash
docker run -it myrepo/ballista-arm64 /ballista-scheduler
```

Run an executor:

```bash
docker run -it myrepo/ballista-arm64 /ballista-executor
```

Run the benchmarks:

```bash
docker run -it myrepo/ballista-arm64 \
  /tpch benchmark datafusion --query=1 --path=/path/to/data --format=parquet \
  --concurrency=24 --iterations=1 --debug --host=ballista-scheduler --bind-port=50050
```

Note that it will be necessary to mount appropriate volumes into the containers and also configure networking
so that the Docker containers can communicate with each other. This can be achieved using Docker compose or Kubernetes.

## Kubernetes

With Docker images built using the instructions above, it is now possible to deploy Ballista to a Kubernetes cluster
running on one of more Raspberry Pi computers. Refer to the instructions in the [Kubernetes](kubernetes.md) chapter
for more information, and remember to change the Docker image name to `myrepo/ballista-arm64`.
