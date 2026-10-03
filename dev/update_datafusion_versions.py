#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
        content = re.sub(r'datafusion version \d+\.\d+\.\d+', f'datafusion version {new_version}', content)
        fd.truncate()
        fd.write(content)


def update_ci(new_version: str):
    # release version script
    print("updating ci/scripts/release_version_labeler.js")
    with open("ci/scripts/release_version_labeler.js", 'r+') as fd:
        new_major_version = int(new_version.split(".")[0])
        next_version = f"v{new_major_version + 1}.0.0"
        content = fd.read()
        fd.seek(0)
        content = re.sub(
            r"const target_version = '.*?'", f"const target_version = '{next_version}'", content
        )
        fd.truncate()
        fd.write(content)


def main():
    parser = argparse.ArgumentParser(
        description=(
            'Update datafusion crate version and corresponding version pins '
            'in downstream crates.'
        ))
    parser.add_argument('new_version', type=str, help='new datafusion version')
    args = parser.parse_args()

    new_version = args.new_version
    repo_root = Path(__file__).parent.parent.absolute()

    print(f'Updating workspace in {repo_root} to {new_version}')
    update_workspace_version(new_version)

    update_docs("README.md", new_version)
    update_docs("docs/source/download.md", new_version)
    update_docs("docs/source/user-guide/example-usage.md", new_version)
    update_docs("docs/source/user-guide/crate-configuration.md", new_version)
    update_docs("docs/source/user-guide/configs.md", new_version)
    
    update_ci(new_version)


if __name__ == "__main__":
    main()
