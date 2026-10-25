#!/usr/bin/env python3
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

# Checks if datafusion-examples uses `datafusion-...` subcrates unnecessarily, as they are already
# exported by the `datafusion` crate.

import tomllib
import sys

allowed_datafusion_crates = {
    "datafusion",
    "datafusion-proto",
    "datafusion-substrait",
}

crates = set()
with open("datafusion-examples/Cargo.toml", "rb") as f:
    cargo = tomllib.load(f)
    for section in ("dependencies", "dev-dependencies", "build-dependencies"):
        crates.update(
            value.get("package", name) if isinstance(value, dict) else name
            for name, value in cargo.get(section, {}).items()
        )

crates = [
    crate for crate in crates if "datafusion" in crate and crate not in allowed_datafusion_crates
]

if len(crates) > 0:
    print(
        "datafusion-examples should only use the main datafusion crate and not its subcrates.",
        file=sys.stderr,
    )
    print(
        f"Please remove the following crates and use `datafusion::...` instead: {crates}.",
        file=sys.stderr,
    )

    sys.exit(1)
