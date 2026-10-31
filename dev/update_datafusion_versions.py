#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

# Script that updates versions for datafusion crates, locally
#
# dependencies:
# uv sync

import re
import argparse
from pathlib import Path
import tomlkit


def update_workspace_version(new_version: str):
    cargo_toml = 'Cargo.toml'
    print(f'updating {cargo_toml}')
    with open(cargo_toml) as f:
        data = f.read()

    doc = tomlkit.parse(data)
    pkg = doc.get('workspace').get('package')

    print('workspace package', pkg)
    pkg['version'] = new_version

    doc = tomlkit.parse(data)

    for crate, df_dep in doc['workspace']['dependencies'].items():
        if not crate.startswith('datafusion'):
            continue
        # skip crates that pin datafusion using git hash
        if df_dep is not None and df_dep.get('version') is not None:
            print(f'updating {crate} dependency in {cargo_toml}')
            df_dep['version'] = new_version

    doc['workspace']['package']['version'] = new_version

    with open(cargo_toml, 'w') as f:
        f.write(tomlkit.dumps(doc))


def update_docs(path: str, new_version: str):
    print(f"updating docs in {path}")
    with open(path, 'r+') as fd:
        content = fd.read()
        fd.seek(0)
        content = re.sub(r'datafusion\s*=\s*"(.+?)"', f'datafusion = "{new_version}"', content)
        content = re.sub(r'datafusion\s*=\s*\{\s*version\s*=\s*"(.+?)"', f'datafusion = {{ version = "{new_version}"', content)
        content = re.sub(r'datafusion version \d+\.\d+\.\d+(-\S+)?', f'datafusion version {new_version}', content)
        fd.truncate()
        fd.write(content)


def is_prerelease(version: str) -> bool:
    """Returns True for development versions such as `56.0.0-dev`"""
    return '-' in version


def release_version(version: str) -> str:
    """Strips any pre-release suffix: `56.0.0-dev` --> `56.0.0`"""
    return version.split('-')[0]


def update_ci(new_version: str):
    # The release version labeler adds a label to merged PRs with the
    # version they will be released in. On `main` the version is
    # `N.0.0-dev` so the label is `vN.0.0`
    print("updating ci/scripts/release_version_labeler.js")
    with open("ci/scripts/release_version_labeler.js", 'r+') as fd:
        target_version = f"v{release_version(new_version)}"
        content = fd.read()
        fd.seek(0)
        content = re.sub(
            r"const target_version = '.*?'", f"const target_version = '{target_version}'", content
        )
        fd.truncate()
        fd.write(content)


def main():
    parser = argparse.ArgumentParser(
        description=(
            'Update datafusion crate version and corresponding version pins '
            'in downstream crates.'
        ))
    parser.add_argument('new_version', type=str, help='new datafusion version (e.g. `56.0.0` or `56.0.0-dev`)')
    args = parser.parse_args()

    new_version = args.new_version
    repo_root = Path(__file__).parent.parent.absolute()

    print(f'Updating workspace in {repo_root} to {new_version}')
    update_workspace_version(new_version)

    # The generated config docs embed the full crate version (for example
    # `datafusion version 56.0.0-dev`) so they are always updated
    update_docs("docs/source/user-guide/configs.md", new_version)

    if is_prerelease(new_version):
        # Development versions (e.g. `56.0.0-dev`) are never published to
        # crates.io, so leave the user facing examples pointing at the
        # latest released version
        print(f'{new_version} is a development version, not updating docs examples')
    else:
        update_docs("README.md", new_version)
        update_docs("docs/source/download.md", new_version)
        update_docs("docs/source/user-guide/example-usage.md", new_version)
        update_docs("docs/source/user-guide/crate-configuration.md", new_version)

    update_ci(new_version)


if __name__ == "__main__":
    main()
