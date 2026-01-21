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

"""Compile profile benchmark runner for DataFusion.

Builds the `tpch` benchmark binary with several Cargo profiles (e.g. `--release` or `--profile ci`), runs the full TPC-H suite against the Parquet data under `benchmarks/data/tpch_sf1`, and reports compile time, execution time, and resulting 
binary size.

See `benchmarks/README.md` for usages.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable, NamedTuple

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = REPO_ROOT / "benchmarks" / "data" / "tpch_sf1"
DEFAULT_ITERATIONS = 1
DEFAULT_FORMAT = "parquet"
DEFAULT_PARTITIONS: int | None = None
TPCH_BINARY = "tpch.exe" if os.name == "nt" else "tpch"
PROFILE_TARGET_DIR = {
    "dev": "debug",
    "release": "release",
    "ci": "ci",
    "release-nonlto": "release-nonlto",
}


class ProfileResult(NamedTuple):
    profile: str
    compile_seconds: float
    run_seconds: float
    binary_bytes: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profiles",
        nargs="+",
        default=list(PROFILE_TARGET_DIR.keys()),
        help="Cargo profiles to test (default: dev release ci release-nonlto)",
    )
    parser.add_argument(
        "--data",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Path to TPCH dataset (default: benchmarks/data/tpch_sf1)",
    )
    return parser.parse_args()


def timed_run(command: Iterable[str]) -> float:
    start = time.perf_counter()
    try:
        subprocess.run(command, cwd=REPO_ROOT, check=True)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"command failed: {' '.join(command)}") from exc
    return time.perf_counter() - start


def cargo_build(profile: str) -> float:
    if profile == "dev":
        command = ["cargo", "build", "--bin", "tpch"]
    else:
        command = ["cargo", "build", "--profile", profile, "--bin", "tpch"]
    return timed_run(command)


def cargo_clean(profile: str) -> None:
    command = ["cargo", "clean", "--profile", profile]
    try:
        subprocess.run(command, cwd=REPO_ROOT, check=True)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"failed to clean cargo artifacts for profile '{profile}'") from exc


def run_benchmark(profile: str, data_path: Path) -> float:
    binary_dir = PROFILE_TARGET_DIR.get(profile)
    if not binary_dir:
        raise ValueError(f"unknown profile '{profile}'")
    binary_path = REPO_ROOT / "target" / binary_dir / TPCH_BINARY
    if not binary_path.exists():
        raise FileNotFoundError(f"compiled binary not found at {binary_path}")

    command = [
        str(binary_path),
        "benchmark",
        "datafusion",
        "--iterations",
        str(DEFAULT_ITERATIONS),
        "--path",
        str(data_path),
        "--format",
        DEFAULT_FORMAT,
    ]
    if DEFAULT_PARTITIONS is not None:
        command.extend(["--partitions", str(DEFAULT_PARTITIONS)])
    env = os.environ.copy()
    env.setdefault("RUST_LOG", "warn")

    start = time.perf_counter()
    try:
        subprocess.run(command, cwd=REPO_ROOT, env=env, check=True)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"benchmark failed for profile '{profile}'") from exc
    return time.perf_counter() - start


def binary_size(profile: str) -> int:
    binary_dir = PROFILE_TARGET_DIR[profile]
    binary_path = REPO_ROOT / "target" / binary_dir / TPCH_BINARY
    return binary_path.stat().st_size


def human_time(seconds: float) -> str:
    return f"{seconds:6.2f}s"


def human_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:6.1f}{unit}"
        value /= 1024
    return f"{value:6.1f}TB"


def main() -> None:
    args = parse_args()
    data_path = args.data.resolve()
    if not data_path.exists():
        print(f"Data directory not found: {data_path}", file=sys.stderr)
        sys.exit(1)

    results: list[ProfileResult] = []
    for profile in args.profiles:
        print(f"\n=== Profile: {profile} ===")
        print("Cleaning previous build artifacts...")
        cargo_clean(profile)
        compile_seconds = cargo_build(profile)
        run_seconds = run_benchmark(profile, data_path)
        size_bytes = binary_size(profile)
        results.append(ProfileResult(profile, compile_seconds, run_seconds, size_bytes))

    print("\nSummary")
    header = f"{'Profile':<15}{'Compile':>12}{'Run':>12}{'Size':>12}"
    print(header)
    print("-" * len(header))
    for result in results:
        print(
            f"{result.profile:<15}{human_time(result.compile_seconds):>12}"
            f"{human_time(result.run_seconds):>12}{human_size(result.binary_bytes):>12}"
        )

if __name__ == "__main__":
    main()
