// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
