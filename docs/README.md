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
and [Contributor Guide](./source/contributor-guide). The site root shows the
development documentation built from `main`. Released versions of the complete
site are available under `/versions/<version>/`.

## Dependencies

Install build dependencies and build the documentation using
[uv](https://docs.astral.sh/uv/):

```sh
uv sync
uv run bash build.sh
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

The HTML will be generated into a `build` directory. Serve the site over HTTP
to test the version switcher (it cannot fetch JSON from a `file:` URL):

```bash
python3 -m http.server --directory build/html 8000
```

The version switcher reads `source/_static/versions.json` from the site root.
Add an entry only when that release's documentation is published. For the manual
release build and publication procedure, see
[the release guide](../dev/release/README.md#publish-the-versioned-documentation).

For a local or fork preview, set `DATAFUSION_DOCS_BASE_URL` to the preview's root
URL when building both development and released docs, and change the URLs in
the preview's `_static/versions.json` to that same root. For example, use
`http://localhost:8000/` locally or `https://<username>.github.io/datafusion/`
on GitHub Pages. Serve the combined site, with released docs under `versions/`,
and check the picker in both directions, a page missing from a release, search,
and static assets. Only the preview manifest should contain preview URLs.

## Making Changes

To make changes to the docs, simply make a Pull Request with your
proposed changes as normal. When the PR is merged the docs will be
automatically updated.

## Release Process

This documentation is hosted at https://datafusion.apache.org/

When a PR is merged to the `main` branch of the DataFusion
repository, a [github workflow](https://github.com/apache/datafusion/blob/main/.github/workflows/docs.yaml) which:

1. Builds the html content
2. Pushes the html content to the [`asf-site`](https://github.com/apache/datafusion/tree/asf-site) branch in this repository.

The Apache Software Foundation provides https://datafusion.apache.org/,
which serves content based on the configuration in
[.asf.yaml](https://github.com/apache/datafusion/blob/main/.asf.yaml),
which specifies the target as https://datafusion.apache.org/.
