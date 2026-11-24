#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root" && cargo run --manifest-path datafusion/proto-models/gen/Cargo.toml
