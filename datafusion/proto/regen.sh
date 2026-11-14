#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

# The proto schema and code generation now live in `datafusion-proto-models`.
# This script is kept as a convenience wrapper.
repo_root=$(git rev-parse --show-toplevel)
exec "$repo_root/datafusion/proto-models/regen.sh"
