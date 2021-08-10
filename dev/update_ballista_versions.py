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

# Script that updates verions for ballista crates, locally
#
# dependencies:
# pip install tomlkit

import os
import argparse
from pathlib import Path
import tomlkit


def update_cargo_toml(cargo_toml: str, new_version: str):
    print(f'updating {cargo_toml}')
    with open(cargo_toml) as f:
        data = f.read()

    doc = tomlkit.parse(data)
    doc.get('package')['version'] = new_version

    with open(cargo_toml, 'w') as f:
        f.write(tomlkit.dumps(doc))


def main():
    parser = argparse.ArgumentParser(description='Update ballista crate versions.')
    parser.add_argument('new_version', type=str, help='new ballista version')
    args = parser.parse_args()

    repo_root = Path(__file__).parent.parent.absolute()
    ballista_crates = set([
        os.path.join(repo_root, rel_path, "Cargo.toml")
        for rel_path in [
            'ballista-examples',
            'ballista/rust/core',
            'ballista/rust/scheduler',
            'ballista/rust/executor',
            'ballista/rust/client',
        ]
    ])
    new_version = args.new_version

    print(f'Updating ballista versions in {repo_root} to {new_version}')

    for cargo_toml in ballista_crates:
        update_cargo_toml(cargo_toml, new_version)


if __name__ == "__main__":
    main()
