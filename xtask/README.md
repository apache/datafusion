<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# DataFusion xtask

This directory contains DataFusion's project automation implemented using the
[`xtask` pattern](https://github.com/matklad/cargo-xtask).

## Usage

Run the following command from the repository root for the available commands,
arguments, and examples:

```shell
cargo xtask help
```

## CI runner

`cargo xtask ci [args]` domain is used for organizing commands used in GitHub CI.

For example, `cargo xtask ci step test workspace` runs DataFusion's default CI test suite. Developers can run the same command to reproduce that GitHub Actions step locally. Append `--explain` to display the underlying command without running it.

The goal is to keep local development and GitHub CI runs in sync. Each CI step command is defined once in this in-repository Rust binary and is invoked the same way locally and in GitHub Actions.

Tracking issue: https://github.com/apache/datafusion/issues/24487
