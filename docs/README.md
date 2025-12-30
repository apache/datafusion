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

# DataFusion Documentation

This folder contains the source content of the [User Guide](./source/user-guide)
and [Contributor Guide](./source/contributor-guide). These are both published to
https://datafusion.apache.org/ as part of the release process.

## Dependencies

It's recommended to install build dependencies and build the documentation
inside a Python virtualenv.

```sh
python3 -m venv venv
pip install -r requirements.txt
```

If using [uv](https://docs.astral.sh/uv/) the script can be run like so without
needing to create a virtual environment:

```sh
uv run --with-requirements requirements.txt bash build.sh
```

The docs build regenerates the workspace dependency graph via
`docs/scripts/generate_dependency_graph.sh`, so ensure `cargo`, `cargo-depgraph`
(`cargo install cargo-depgraph --version ^1.6 --locked`), and Graphviz `dot`
(`brew install graphviz` or `sudo apt-get install -y graphviz`) are available.

## Build & Preview

Run the provided script to build the HTML pages.

```bash
# If using venv, ensure you have activated it
./build.sh
```

The HTML will be generated into a `build` directory. Open `build/html/index.html`
in your preferred browser, e.g.

Preview the site on Linux by running this command.

```bash
# On macOS
open build/html/index.html
# On Linux with Firefox
firefox build/html/index.html
```

## Making Changes

To make changes to the docs, simply make a Pull Request with your
proposed changes as normal. When the PR is merged the docs will be
automatically updated.

## Release Process

This documentation is hosted at https://datafusion.apache.org/

When the PR is merged to the `main` branch of the DataFusion
repository, a [github workflow](https://github.com/apache/datafusion/blob/main/.github/workflows/docs.yaml) which:

1. Builds the html content
2. Pushes the html content to the [`asf-site`](https://github.com/apache/datafusion/tree/asf-site) branch in this repository.

The Apache Software Foundation provides https://datafusion.apache.org/,
which serves content based on the configuration in
[.asf.yaml](https://github.com/apache/datafusion/blob/main/.asf.yaml),
which specifies the target as https://datafusion.apache.org/.
