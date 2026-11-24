#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

# Note: cargo doc does not support an auto-fix mode; this script runs the check-only build.
set -ex
export RUSTDOCFLAGS="-D warnings"
cargo doc --document-private-items --no-deps --workspace
