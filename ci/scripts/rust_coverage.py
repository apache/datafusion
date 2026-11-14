#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

"""Run each workspace test once and report SLT-only and combined coverage."""

import argparse
import json
import subprocess
import sys


def run(*args):
    subprocess.run(args, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--features", required=True)
    parser.add_argument("--exclude", action="append", default=[])
    options = parser.parse_args()

    compiler = subprocess.check_output(["rustc", "-vV"], text=True)
    host = next(
        line.removeprefix("host: ")
        for line in compiler.splitlines()
        if line.startswith("host: ")
    )
    metadata = json.loads(
        subprocess.check_output(
            [
                "cargo",
                "metadata",
                "--format-version",
                "1",
                "--locked",
                "--filter-platform",
                host,
                "--features",
                options.features,
            ],
            text=True,
        )
    )
    members = set(metadata["workspace_members"])
    packages = [
        package
        for package in metadata["packages"]
        if package["id"] in members and package["name"] not in options.exclude
    ]
    features = {
        node["id"]: set(node["features"]) for node in metadata["resolve"]["nodes"]
    }
    tests = sorted(
        {
            target["name"]
            for package in packages
            for target in package["targets"]
            if "test" in target["kind"]
            and set(target.get("required-features", [])).issubset(
                features[package["id"]]
            )
        }
    )
    if "sqllogictests" not in tests:
        parser.error("the workspace must contain an enabled sqllogictests target")

    common = ["--profile", options.profile, "--workspace"]
    for package in options.exclude:
        common.extend(["--exclude", package])
    common.extend(["--features", options.features])
    report = ["--profile", options.profile]
    for package in packages:
        report.extend(["--package", package["name"]])

    run("cargo", "llvm-cov", "clean", "--workspace")
    run("cargo", "llvm-cov", *common, "--no-report", "--test", "sqllogictests")
    run(
        "cargo",
        "llvm-cov",
        "report",
        *report,
        "--codecov",
        "--output-path",
        "target/codecov-slt.json",
    )

    # Explicit integration targets skip SLT while retaining every crate's unit tests.
    targets = ["--lib", "--bins"]
    for test in tests:
        if test != "sqllogictests":
            targets.extend(["--test", test])
    run("cargo", "llvm-cov", *common, "--no-report", *targets)
    run(
        "cargo",
        "llvm-cov",
        "report",
        *report,
        "--codecov",
        "--output-path",
        "target/codecov.json",
    )


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        sys.exit(error.returncode)
