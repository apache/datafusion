#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

"""Measure coverage compilation on fresh runners with isolated remote caches."""

import json
import os
from pathlib import Path
import shlex
import subprocess
import time


def main():
    stage = os.environ["GITHUB_JOB"]
    backend = os.environ["CACHE_STUDY_BACKEND"]
    if stage not in {"cold", "warm", "changed"}:
        raise ValueError(f"Unknown stage: {stage}")
    if backend not in {"rust-cache", "sccache", "hybrid"}:
        raise ValueError(f"Unknown backend: {backend}")
    wrapper = os.environ.get("RUSTC_WRAPPER", "")
    if (backend == "rust-cache" and wrapper) or (
        backend != "rust-cache" and Path(wrapper).name != "sccache"
    ):
        raise ValueError(f"Unexpected compiler wrapper for {backend}: {wrapper}")

    output = Path(os.environ["RUNNER_TEMP"]) / "cache-study"
    output.mkdir(parents=True, exist_ok=True)
    source = Path("datafusion/functions/src/math/abs.rs")
    original = source.read_text()
    old = "Unsupported data type {other:?} for function abs"
    new = "Unsupported input data type {other:?} for function abs"
    if stage == "changed":
        if original.count(old) != 1:
            raise ValueError("The benchmark edit must match exactly once")
        source.write_text(original.replace(old, new))
        (output / "source.patch").write_bytes(subprocess.check_output(["git", "diff"]))

    # Match the workspace coverage selection, but measure compilation without executing tests.
    command = [
        "cargo", "test", "--no-run", "--locked", "--profile", "ci", "--workspace",
        "--exclude", "datafusion-examples",
        "--exclude", "ffi_example_table_provider",
        "--exclude", "datafusion-cli",
        "--features", "serde,avro,json,backtrace,integration-tests,parquet_encryption,substrait",
        "--lib", "--bins", "--tests", "--message-format=json-render-diagnostics",
    ]
    print(shlex.join(command), flush=True)
    started = time.monotonic()
    try:
        with (output / "cargo.jsonl").open("w") as log:
            result = subprocess.run(command, stdout=log, check=False)
    finally:
        elapsed = time.monotonic() - started
        if stage == "changed":
            source.write_text(original)

    artifacts = []
    for line in (output / "cargo.jsonl").read_text().splitlines():
        item = json.loads(line)
        if item.get("reason") == "compiler-artifact":
            artifacts.append(item)
    measurement = {
        "stage": stage,
        "backend": backend,
        "trial": os.environ.get("CACHE_STUDY_TRIAL"),
        "seconds": round(elapsed, 3),
        "exit_code": result.returncode,
        "command": command,
        "rustflags": os.environ.get("RUSTFLAGS"),
        "rustc_wrapper": wrapper,
        "sccache_prefix": os.environ.get("SCCACHE_S3_KEY_PREFIX"),
        "target_dir": os.environ.get("CARGO_TARGET_DIR"),
        "fresh_units": sum(item["fresh"] for item in artifacts),
        "compiled_units": sum(not item["fresh"] for item in artifacts),
        "test_executables": sum(item["executable"] is not None for item in artifacts),
    }
    (output / "measurement.json").write_text(json.dumps(measurement, indent=2) + "\n")
    print(json.dumps(measurement, indent=2), flush=True)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
        summary.write("| Stage | Cache | Compile seconds | Fresh units | Compiled units |\n")
        summary.write("| --- | --- | ---: | ---: | ---: |\n")
        summary.write(
            f"| {stage} | {backend} | {elapsed:.3f} | "
            f"{measurement['fresh_units']} | {measurement['compiled_units']} |\n"
        )
    if result.returncode:
        raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
