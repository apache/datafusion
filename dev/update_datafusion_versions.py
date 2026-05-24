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

crates = {
    'datafusion-common': 'datafusion/common/Cargo.toml',
    'datafusion-common-runtime': 'datafusion/common-runtime/Cargo.toml',
    'datafusion': 'datafusion/core/Cargo.toml',
    'datafusion-execution': 'datafusion/execution/Cargo.toml',
    'datafusion-expr': 'datafusion/expr/Cargo.toml',
    'datafusion-ffi': 'datafusion/ffi/Cargo.toml',
    'datafusion-functions': 'datafusion/functions/Cargo.toml',
    'datafusion-functions-aggregate': 'datafusion/functions-aggregate/Cargo.toml',
    'datafusion-functions-nested': 'datafusion/functions-nested/Cargo.toml',
    'datafusion-optimizer': 'datafusion/optimizer/Cargo.toml',
    'datafusion-physical-expr': 'datafusion/physical-expr/Cargo.toml',
    'datafusion-physical-expr-common': 'datafusion/physical-expr-common/Cargo.toml',
    'datafusion-physical-plan': 'datafusion/physical-plan/Cargo.toml',
    'datafusion-proto': 'datafusion/proto/Cargo.toml',
    'datafusion-sql': 'datafusion/sql/Cargo.toml',
    'datafusion-sqllogictest': 'datafusion/sqllogictest/Cargo.toml',
    'datafusion-substrait': 'datafusion/substrait/Cargo.toml',
    'datafusion-wasmtest': 'datafusion/wasmtest/Cargo.toml',
    'datafusion-benchmarks': 'benchmarks/Cargo.toml',
    'datafusion-cli': 'datafusion-cli/Cargo.toml',
    'datafusion-examples': 'datafusion-examples/Cargo.toml',
}

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

    for crate in crates.keys():
        df_dep = doc.get('workspace').get('dependencies', {}).get(crate)
        # skip crates that pin datafusion using git hash
        if df_dep is not None and df_dep.get('version') is not None:
            print(f'updating {crate} dependency in {cargo_toml}')
            df_dep['version'] = new_version

    with open(cargo_toml, 'w') as f:
        f.write(tomlkit.dumps(doc))


def update_datafusion_version(cargo_toml: str, new_version: str):
    print(f'updating {cargo_toml}')
    with open(cargo_toml) as f:
        data = f.read()

    doc = tomlkit.parse(data)
    pkg = doc.get('package')
    if 'workspace' not in pkg['version']:
        pkg['version'] = new_version

    with open(cargo_toml, 'w') as f:
        f.write(tomlkit.dumps(doc))

def update_downstream_versions(cargo_toml: str, new_version: str):
    with open(cargo_toml) as f:
        data = f.read()

    doc = tomlkit.parse(data)

    for crate in crates.keys():
        df_dep = doc.get('dependencies', {}).get(crate)
        # skip crates that pin datafusion using git hash
        if df_dep is not None and df_dep.get('version') is not None:
            print(f'updating {crate} dependency in {cargo_toml}')
            df_dep['version'] = new_version

        df_dep = doc.get('dev-dependencies', {}).get(crate)
        if df_dep is not None and df_dep.get('version') is not None:
            print(f'updating {crate} dev-dependency in {cargo_toml}')
            df_dep['version'] = new_version

    with open(cargo_toml, 'w') as f:
        f.write(tomlkit.dumps(doc))


def update_docs(path: str, new_version: str):
    print(f"updating docs in {path}")
    with open(path, 'r+') as fd:
        content = fd.read()
        fd.seek(0)
        content = re.sub(r'datafusion\s*=\s*"(.+?)"', f'datafusion = "{new_version}"', content)
        content = re.sub(r'datafusion\s*=\s*\{\s*version\s*=\s*"(.+?)"', f'datafusion = {{ version = "{new_version}"', content)
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

    print(f'Updating datafusion crate versions in {repo_root} to {new_version}')
    for cargo_toml in crates.values():
        update_datafusion_version(cargo_toml, new_version)

    print(f'Updating datafusion dependency versions in {repo_root} to {new_version}')
    for cargo_toml in crates.values():
        update_downstream_versions(cargo_toml, new_version)

    update_docs("README.md", new_version)
    update_docs("docs/source/download.md", new_version)
    update_docs("docs/source/user-guide/example-usage.md", new_version)
    update_docs("docs/source/user-guide/crate-configuration.md", new_version)


if __name__ == "__main__":
    main()
