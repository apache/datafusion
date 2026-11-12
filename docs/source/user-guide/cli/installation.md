<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Installation

## Install and run using Cargo

To build and install the latest release of `datafusion-cli` from source, do:

```shell
cargo install datafusion-cli
#    Updating crates.io index
#  Installing datafusion-cli v37.0.0
#    Updating crates.io index
# ...
```

## Install and run using Homebrew (on MacOS)

`datafusion-cli` can also be installed via [Homebrew] (on MacOS) like this:

[homebrew]: https://docs.brew.sh/Installation

```bash
brew install datafusion
# ...
# ==> Pouring datafusion--37.0.0.arm64_sonoma.bottle.tar.gz
# 🍺  /opt/homebrew/Cellar/datafusion/37.0.0: 9 files, 63.0MB
# ==> Running `brew cleanup datafusion`...
```

## Run using Docker

There is no officially published Docker image for the DataFusion CLI, so it is necessary to build from source
instead.

Use the following commands to clone this repository and build a Docker image containing the CLI tool. Note
that there is `.dockerignore` file in the root of the repository that may need to be deleted in order for
this to work.

```bash
git clone https://github.com/apache/datafusion
cd datafusion
# Note: the build can take a while
docker build -f datafusion-cli/Dockerfile . --tag datafusion-cli
# You can also bind persistent storage with `-v /path/to/data:/data`
docker run --rm -it datafusion-cli
```
