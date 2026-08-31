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

This folder contains the sources for https://datafusion.apache.org/. The root
site is built continuously from `main`. Complete, immutable release sites are
published under `/versions/<version>/`.

## Dependencies

From the repository root, install the documentation dependencies using
[uv](https://docs.astral.sh/uv/):

```sh
uv sync --package datafusion-docs
```

The docs build regenerates the workspace dependency graph via
`docs/scripts/generate_dependency_graph.sh`, so ensure `cargo`, `cargo-depgraph`
(`cargo install cargo-depgraph --version ^1.6 --locked`), and Graphviz `dot`
(`brew install graphviz` or `sudo apt-get install -y graphviz`) are available.
`.gitattributes` keeps documentation shell scripts LF-terminated so the same
scripts run from Linux and WSL checkouts.

## Build and Preview

Build the current complete site from the repository root:

```bash
uv run --package datafusion-docs docs/build.sh
```

The HTML is generated in `docs/build/html`. Serve it over HTTP because browsers
do not load the version manifest from `file:` URLs:

```bash
python3 -m http.server --directory docs/build/html 8000
```

Then open http://localhost:8000/.

The public and assembled layouts are:

```text
https://datafusion.apache.org/
|-- user-guide/
|-- library-user-guide/
|-- contributor-guide/
|-- _static/versions.json
`-- versions/55.0.0/
    |-- user-guide/
    |-- library-user-guide/
    |-- contributor-guide/
    |-- download.html
    |-- search.html
    |-- _sources/
    |-- _static/
    `-- sitemap.xml
```

## Release Snapshots

`docs/source/_static/versions.json` is both the PyData version-picker manifest
and the release catalog. It contains `Development` at the site root and records
each release's semantic version, exact tag, and exact 40-character commit.

The first snapshot is the lightweight tag `55.0.0`, which peels to
`d5552342012888b7d1a3ab88d92e3d292fc0cde0`. Create its publication package
from the repository root with the tag available locally:

```bash
uv run --package datafusion-docs python docs/scripts/snapshot_site.py \
  55.0.0 55.0.0 /tmp/datafusion-release-site
```

The command creates only
`/tmp/datafusion-release-site/versions/55.0.0/`. It refuses to overwrite that
directory, rejects output overlapping the repository, builds in an isolated
detached worktree, and does not commit or push.

The snapshot uses the tag's complete documentation tree, templates, static
files, helper extension, build script dependency graph, and locked documentation
dependencies. `release_conf.py` supplies only the publication prefix, canonical
base URL, exactly pinned `sphinx-sitemap` extension, version picker, exact GitHub
tag, corrected repository name, and a release-local redirect. After Sphinx
builds, `snapshot_site.py` rewrites only published links that would otherwise
escape to current DataFusion docs or mutable DataFusion `main` and `latest`
targets, including links in the agent-facing `llms.txt`. This includes one narrow
fix for the tagged broken `/contributor-guide/gsoc_application_guidelines.html`
link, which is redirected to the tagged
`contributor-guide/gsoc/gsoc_application_guidelines_2025.html` page. Tagged
source files and generated `_sources` files are never modified. In particular,
the tagged statement that 55.0.0 has not been released yet remains unchanged.

Validate a complete assembled site with:

```bash
uv run --package datafusion-docs python docs/scripts/assemble_site.py \
  --current-site docs/build/html \
  --published-site /tmp/datafusion-release-site \
  --output-site /tmp/datafusion-site
uv run --package datafusion-docs python docs/scripts/validate_site.py \
  --site-root /tmp/datafusion-site --require-snapshots
```

The assembler starts with a fresh current build, copies the existing complete
`asf-site/versions/` archive unchanged, and creates a root sitemap index. It does
not use picker entries as a deletion list, so removing an old release from the
picker cannot erase its archive.

## Publication Bootstrap

The old deployment uses unrestricted `rsync --delete`, so publishing a snapshot
before snapshot retention reaches `main` is not race-free. Keep the feature PR
in draft until maintainers agree on one of these bootstrap procedures:

1. Merge a preliminary retention-only workflow change, wait for it to deploy,
   publish `versions/55.0.0/` manually to `asf-site`, then merge the picker and
   validation changes.
2. Use a coordinated window: confirm no old documentation deployment is running
   or queued, publish the snapshot, merge this change immediately, wait for the
   new serialized deployment, and verify `asf-site/versions/55.0.0/` afterward.

For manual publication, copy the single generated `versions/55.0.0/` directory
to the same location in an `asf-site` worktree. Review, commit, and push that
branch manually. Never replace an existing release directory. The snapshot
tooling never commits, pushes, or publishes.

## Site Deployment

When a documentation change reaches `main`, the deployment workflow:

1. Builds the current complete site and replaces all current root files.
2. Copies the entire pre-existing `asf-site/versions/` archive without using the
   picker as a retention registry.
3. Builds `sitemap.xml` as an index over `sitemap-main.xml` and every retained
   complete release sitemap.
4. Deliberately installs `.asf.yaml` and `.nojekyll`, then uses `rsync --delete`
   while excluding `.git` to remove stale current output.
5. Serializes deployments and pushes a normal, non-force commit.

The Apache Software Foundation serves the branch according to
[`.asf.yaml`](https://github.com/apache/datafusion/blob/main/.asf.yaml).
