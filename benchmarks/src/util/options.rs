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

use datafusion::prelude::SessionConfig;
use structopt::StructOpt;

// Common benchmark options (don't use doc comments otherwise this doc
// shows up in help files)
#[derive(Debug, StructOpt, Clone)]
pub struct CommonOpt {
    /// Number of iterations of each test run
    #[structopt(short = "i", long = "iterations", default_value = "3")]
    pub iterations: usize,

    /// Number of partitions to process in parallel. Defaults to number of available cores.
    #[structopt(short = "n", long = "partitions")]
    pub partitions: Option<usize>,

    /// Batch size when reading CSV or Parquet files
    #[structopt(short = "s", long = "batch-size", default_value = "8192")]
    pub batch_size: usize,

    /// Activate debug mode to see more details
    #[structopt(short, long)]
    pub debug: bool,

    /// If true, will use StringView/BinaryViewArray instead of String/BinaryArray
    /// when reading ParquetFiles
    #[structopt(long)]
    pub force_view_types: bool,
}

impl CommonOpt {
    /// Return an appropriately configured `SessionConfig`
    pub fn config(&self) -> SessionConfig {
        self.update_config(SessionConfig::new())
    }

    /// Modify the existing config appropriately
    pub fn update_config(&self, config: SessionConfig) -> SessionConfig {
        config
            .with_target_partitions(self.partitions.unwrap_or(num_cpus::get()))
            .with_batch_size(self.batch_size)
    }
}
