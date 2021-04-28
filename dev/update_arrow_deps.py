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

# Script that updates the arrow dependencies in datafusion and ballista, locall
#
# installation:
# pip install tomlkit requests
#
# usage:
# python update_arrow_deps.py

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
def update_dependencies(dependencies, new_sha):
    if dependencies is None:
        return
    for dep_name in dependencies:
        dep = dependencies[dep_name]
        if hasattr(dep, 'get'):
            if dep.get('git') == 'https://github.com/apache/arrow-rs':
                dep['rev'] = new_sha


def update_cargo_toml(cargo_toml, new_sha):
    print('updating {}'.format(cargo_toml.absolute()))
    with open(cargo_toml) as f:
        data = f.read()

    doc = tomlkit.parse(data)

    update_dependencies(doc.get('dependencies'), new_sha)
    update_dependencies(doc.get('dev-dependencies'), new_sha)

    with open(cargo_toml, 'w') as f:
        f.write(tomlkit.dumps(doc))


# Begin main script

repo_root = Path(__file__).parent.parent.absolute()


new_sha = get_arrow_sha()

print('Updating files in {} to use sha {}'.format(repo_root, new_sha))


for cargo_toml in repo_root.rglob('Cargo.toml'):
    update_cargo_toml(cargo_toml, new_sha)
