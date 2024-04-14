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

## Install and run using PyPI

`datafusion-cli` can also be installed via PyPI like this:

[pypi]: https://pip.pypa.io/en/latest/installation/

```bash
pip3 install datafusion
# Defaulting to user installation because normal site-packages is not writeable
# Collecting datafusion
#   Downloading datafusion-33.0.0-cp38-abi3-macosx_11_0_arm64.whl.metadata (9.6 kB)
# Collecting pyarrow>=11.0.0 (from datafusion)
#   Downloading pyarrow-14.0.1-cp39-cp39-macosx_11_0_arm64.whl.metadata (3.0 kB)
# Requirement already satisfied: numpy>=1.16.6 in /Users/Library/Python/3.9/lib/python/site-packages (from pyarrow>=11.0.0->datafusion) (1.23.4)
# Downloading datafusion-33.0.0-cp38-abi3-macosx_11_0_arm64.whl (13.5 MB)
#    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 13.5/13.5 MB 3.6 MB/s eta 0:00:00
# Downloading pyarrow-14.0.1-cp39-cp39-macosx_11_0_arm64.whl (24.0 MB)
#    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 24.0/24.0 MB 36.4 MB/s eta 0:00:00
# Installing collected packages: pyarrow, datafusion
#   Attempting uninstall: pyarrow
#     Found existing installation: pyarrow 10.0.1
#     Uninstalling pyarrow-10.0.1:
#       Successfully uninstalled pyarrow-10.0.1
# Successfully installed datafusion-33.0.0 pyarrow-14.0.1

datafusion-cli
```

## Run using Docker

There is no officially published Docker image for the DataFusion CLI, so it is necessary to build from source
instead.

Use the following commands to clone this repository and build a Docker image containing the CLI tool. Note
that there is `.dockerignore` file in the root of the repository that may need to be deleted in order for
this to work.

```bash
git clone https://github.com/apache/arrow-datafusion
cd arrow-datafusion
git checkout 12.0.0
docker build -f datafusion-cli/Dockerfile . --tag datafusion-cli
docker run -it -v $(your_data_location):/data datafusion-cli
```
