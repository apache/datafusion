#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Script that updates verions for datafusion crates, locally
#
# dependencies:
# pip install tomlkit

import re
import os
import argparse
from pathlib import Path
import tomlkit


def update_datafusion_version(cargo_toml: str, new_version: str):
    print(f'updating {cargo_toml}')
    with open(cargo_toml) as f:
        data = f.read()

    doc = tomlkit.parse(data)
    doc.get('package')['version'] = new_version

    with open(cargo_toml, 'w') as f:
        f.write(tomlkit.dumps(doc))


def update_downstream_versions(cargo_toml: str, new_version: str):
    with open(cargo_toml) as f:
        data = f.read()

    doc = tomlkit.parse(data)

    df_dep = doc.get('dependencies', {}).get('datafusion')
    # skip crates that pin datafusion using git hash
    if df_dep is not None and df_dep.get('version') is not None:
        print(f'updating datafusion dependency in {cargo_toml}')
        df_dep['version'] = new_version

    df_dep = doc.get('dev-dependencies', {}).get('datafusion')
    if df_dep is not None and df_dep.get('version') is not None:
        print(f'updating datafusion dev-dependency in {cargo_toml}')
        df_dep['version'] = new_version

    with open(cargo_toml, 'w') as f:
        f.write(tomlkit.dumps(doc))


def update_docs(path: str, new_version: str):
    print(f"updating docs in {path}")
    with open(path, 'r+') as fd:
        content = fd.read()
        fd.seek(0)
        content = re.sub(r'datafusion = "(.+)"', f'datafusion = "{new_version}"', content)
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

    print(f'Updating datafusion versions in {repo_root} to {new_version}')

    update_datafusion_version("datafusion/Cargo.toml", new_version)
    for cargo_toml in repo_root.rglob('Cargo.toml'):
        update_downstream_versions(cargo_toml, new_version)

    update_docs("README.md", new_version)


if __name__ == "__main__":
    main()
