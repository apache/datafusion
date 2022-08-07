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

# Demo

For demonstration purposes, we will use the DataFusion CLI. The DataFusion CLI is a command-line interactive SQL utility that allows
queries to be executed against an in-process DataFusion query engine. It embeds the DataFusion library
in a command line utility and is a convenient way to try DataFusion with your own data sources.


## Try it out

Datafusion-CLI provides a fast way to get started testing the capabilities of the DataFusion library.


### Installation

#### Via Cargo

If you already have the Rust toolchain installed, the easiest way to install DataFusion CLI is to install it via cargo:

```
cargo install datafusion-cli
```

Or you can build from source, taking the latest code from master:

```
git clone https://github.com/apache/arrow-datafusion && cd arrow-datafusion\datafusion-cli
cargo install --path .
```

#### Via Docker

If you don't have the Rust toolchain installed, but you do have Docker, you can build DataFusion CLI inside Docker.
There is no officially published Docker image for the DataFusion CLI, so it is necessary to build from source
instead.

Use the following commands to clone this repository and build a Docker image containing the CLI tool.

```
    git clone https://github.com/apache/arrow-datafusion && cd arrow-datafusion
    docker build -f datafusion-cli/Dockerfile . --tag datafusion-cli
    docker run -it -v $(your_data_location):/data datafusion-cli
```

#### Via Homebrew (on MacOS)

DataFusion CLI can also be installed via Homebrew (on MacOS). Install it as any other pre-built software like this:

```
    brew install datafusion
    # ==> Downloading https://ghcr.io/v2/homebrew/core/datafusion/manifests/5.0.0
    # ######################################################################## 100.0%
    # ==> Downloading https://ghcr.io/v2/homebrew/core/datafusion/blobs/sha256:9ecc8a01be47ceb9a53b39976696afa87c0a8
    # ==> Downloading from https://pkg-containers.githubusercontent.com/ghcr1/blobs/sha256:9ecc8a01be47ceb9a53b39976
    # ######################################################################## 100.0%
    # ==> Pouring datafusion--5.0.0.big_sur.bottle.tar.gz
    # 🍺  /usr/local/Cellar/datafusion/5.0.0: 9 files, 17.4MB

    datafusion-cli
```


register some tables

run some queries

run some explain plans

