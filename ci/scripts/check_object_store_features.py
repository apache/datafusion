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

"""Check storage opt-out outside the workspace's feature unification.

Each child crate must keep storage enabled by default and allow opting out.
Run a custom file reader through SQL with storage disabled, explicitly restored,
and enabled by default. The temporary manifest is a test consumer, not a new
DataFusion package. Cargo's workspace dev-dependencies must not mask failures.
"""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[2]
PACKAGES = {
    "datafusion": "core",
    **{
        f"datafusion-{name}": name
        for name in (
            "catalog",
            "datasource",
            "execution",
            "functions",
            "functions-aggregate",
            "functions-nested",
            "functions-table",
            "physical-expr-adapter",
            "physical-optimizer",
            "physical-plan",
            "pruning",
            "session",
            "spark",
            "sql",
        )
    },
}


def main():
    fixture = ROOT / "ci/fixtures/object_store_features.rs"
    subprocess.run(
        ["rustfmt", "--check", "--edition", "2024", str(fixture)], check=True
    )
    with tempfile.TemporaryDirectory(prefix="datafusion-features-") as temp:
        consumer = Path(temp)
        manifest = consumer / "Cargo.toml"
        # Reuse the workspace resolution and build cache, but not its features.
        if (ROOT / "Cargo.lock").exists():
            shutil.copyfile(ROOT / "Cargo.lock", consumer / "Cargo.lock")
        env = dict(os.environ)
        env.setdefault("CARGO_TARGET_DIR", str(ROOT / "target"))
        declarations = "\n".join(
            f"{name} = {{ path = {json.dumps(str(ROOT / 'datafusion' / directory))}, "
            "default-features = false, optional = true }"
            for name, directory in PACKAGES.items()
        )
        manifest.write_text(
            '[package]\nname = "datafusion-feature-test"\nversion = "0.0.0"\n'
            'edition = "2024"\npublish = false\n[workspace]\n'
            f'[[bin]]\nname = "feature-test"\npath = {json.dumps(str(fixture))}\n'
            '[features]\nstorage = ["datafusion/object_store"]\n'
            '[dependencies]\nasync-trait = "0.1"\ntempfile = "3"\n'
            'tokio = { version = "1", features = ["macros", "rt-multi-thread"] }\n'
            f"{declarations}\n"
            '[profile.dev]\ndebug = 0\n'
        )

        def cargo(command, features, *args, capture=False):
            return subprocess.run(
                [
                    "cargo", command, "--manifest-path", str(manifest),
                    "--features", ",".join(features), *args,
                ],
                cwd=consumer,
                env=env,
                check=True,
                text=True,
                stdout=subprocess.PIPE if capture else None,
            ).stdout

        def check_tree(features, expected):
            tree = cargo(
                "tree", features, "--target", "all", "--edges", "all",
                "--prefix", "none", "--format", "{p}", capture=True,
            )
            found = any(line.startswith("object_store v") for line in tree.splitlines())
            if found != expected:
                raise RuntimeError(
                    f"{features}: expected object_store presence={expected}\n{tree}"
                )

        for package in PACKAGES:
            check_tree([package], False)
            check_tree([f"{package}/default"], True)
            print(f"{package}: default storage and opt-out passed", flush=True)

        # Import traits directly from child crates as a real downstream does.
        base = [
            "datafusion/sql", "datafusion-catalog",
            "datafusion-execution", "datafusion-physical-plan",
        ]
        cases = (
            ([], False),
            (["storage"], True),
            (["storage", "datafusion/default"], True),
        )
        for additional, expected in cases:
            features = base + additional
            check_tree(features, expected)
            cargo("run", features)
        print("Downstream dependency and execution contracts passed", flush=True)


if __name__ == "__main__":
    main()
