<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
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

The HTML will be generated into a `build` directory. Open `build/html/index.html`
in your preferred browser, for example on Linux via

```bash
# On macOS
open build/html/index.html
# On Linux with Firefox
firefox build/html/index.html
```

Note that some features of the site, such as the version picker, do not work
when the pages are opened directly as files. To preview such features, serve
the site over HTTP with sphinx-autobuild:

```bash
uv run --with sphinx-autobuild sphinx-autobuild source build/html --port 8000
```

And then open http://localhost:8000/ in your browser

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

The documentation for previously released versions is served under
`/versions/<version>/`. The releases in the version picker are listed in
`source/_static/versions.json`, and older releases that are still published,
but not shown in the picker, are listed in `source/_static/old_versions.json`.
See the
[release instructions](../dev/release/README.md#publish-the-versioned-documentation)
for how it is published.
