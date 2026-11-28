// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::env;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct ReplOptions {
    pub history_file: PathBuf,
}

impl Default for ReplOptions {
    fn default() -> Self {
        let history_file = env::var_os("DATAFUSION_HISTORY_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(".history"));
        Self { history_file }
    }
}
