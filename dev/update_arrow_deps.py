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

# Script that updates the arrow dependencies in datafusion and ballista, locally
#
# installation:
# pip install tomlkit requests
#
# pin all arrow crates deps to a specific version:
#
# python update_arrow_deps.py version '^5.2'
#
# update all arrow git deps to latest commit:
#
# python update_arrow_deps.py commit
#
# help:
#
# python update_arrow_deps.py --help

import argparse
from pathlib import Path

# use tomlkit as it preserves comments and other formatting
import tomlkit
import requests


# find latest arrow-rs sha
def get_arrow_sha():
    url = 'https://api.github.com/repos/apache/arrow-rs/branches/master'
    response = requests.get(url)
    return response.json()['commit']['sha']


# Update all entries that look like
# {
#   'git': 'https://github.com/apache/arrow-rs',
#   'rev': 'c3fe3bab9905739fdda75301dab07a18c91731bd'
# }
# to point at a new SHA
def update_commit_dependencies(dependencies, new_sha):
    if dependencies is None:
        return
    for dep_name in dependencies:
        dep = dependencies[dep_name]
        if hasattr(dep, 'get'):
            if dep.get('git') == 'https://github.com/apache/arrow-rs':
                dep['rev'] = new_sha


def update_commit_cargo_toml(cargo_toml, new_sha):
    print('updating {}'.format(cargo_toml.absolute()))
    with open(cargo_toml) as f:
        data = f.read()

    doc = tomlkit.parse(data)

    update_commit_dependencies(doc.get('dependencies'), new_sha)
    update_commit_dependencies(doc.get('dev-dependencies'), new_sha)

    with open(cargo_toml, 'w') as f:
        f.write(tomlkit.dumps(doc))


def update_version_cargo_toml(cargo_toml, new_version):
    print('updating {}'.format(cargo_toml.absolute()))
    with open(cargo_toml) as f:
        data = f.read()

    doc = tomlkit.parse(data)

    for section in ("dependencies", "dev-dependencies"):
        for (dep_name, constraint) in doc.get(section, {}).items():
            if dep_name in ("arrow", "parquet", "arrow-flight") and constraint.get("version") is not None:
                doc[section][dep_name]["version"] = new_version

    with open(cargo_toml, 'w') as f:
        f.write(tomlkit.dumps(doc))


def main():
    parser = argparse.ArgumentParser(description='Update arrow dep versions.')
    sub_parsers = parser.add_subparsers(help='sub-command help')

    parser_version = sub_parsers.add_parser('version', help='update arrow version')
    parser_version.add_argument('new_version', type=str, help='new arrow version')
    parser_version.set_defaults(which='version')

    parser_commit = sub_parsers.add_parser('commit', help='update arrow commit')
    parser_commit.set_defaults(which='commit')

    args = parser.parse_args()

    repo_root = Path(__file__).parent.parent.absolute()

    if args.which == "version":
        for cargo_toml in repo_root.rglob('Cargo.toml'):
            update_version_cargo_toml(cargo_toml, args.new_version)
    elif args.which == "commit":
        new_sha = get_arrow_sha()
        print('Updating files in {} to use sha {}'.format(repo_root, new_sha))
        for cargo_toml in repo_root.rglob('Cargo.toml'):
            update_commit_cargo_toml(cargo_toml, new_sha)



if __name__ == "__main__":
    main()
