#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
