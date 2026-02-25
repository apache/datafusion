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

use std::{num::NonZeroUsize, sync::Arc};

use clap::Args;
use datafusion::{
    execution::{
        disk_manager::DiskManagerBuilder,
        memory_pool::{FairSpillPool, GreedyMemoryPool, MemoryPool, TrackConsumersPool},
        runtime_env::RuntimeEnvBuilder,
    },
    prelude::SessionConfig,
};
use datafusion_common::{DataFusionError, Result};

// Common benchmark options (don't use doc comments otherwise this doc
// shows up in help files)
#[derive(Debug, Args, Clone)]
pub struct CommonOpt {
    /// Number of iterations of each test run
    #[arg(short = 'i', long = "iterations", default_value = "3")]
    pub iterations: usize,

    /// Number of partitions to process in parallel. Defaults to number of available cores.
    #[arg(short = 'n', long = "partitions")]
    pub partitions: Option<usize>,

    /// Batch size when reading CSV or Parquet files
    #[arg(short = 's', long = "batch-size")]
    pub batch_size: Option<usize>,

    /// The memory pool type to use, should be one of "fair" or "greedy"
    #[arg(long = "mem-pool-type", default_value = "fair")]
    pub mem_pool_type: String,

    /// Memory limit (e.g. '100M', '1.5G'). If not specified, run all pre-defined memory limits for given query
    /// if there's any, otherwise run with no memory limit.
    #[arg(long = "memory-limit", value_parser = parse_memory_limit)]
    pub memory_limit: Option<usize>,

    /// The amount of memory to reserve for sort spill operations. DataFusion's default value will be used
    /// if not specified.
    #[arg(long = "sort-spill-reservation-bytes", value_parser = parse_memory_limit)]
    pub sort_spill_reservation_bytes: Option<usize>,

    /// Activate debug mode to see more details
    #[arg(short, long)]
    pub debug: bool,
}

impl CommonOpt {
    /// Return an appropriately configured `SessionConfig`
    pub fn config(&self) -> Result<SessionConfig> {
        SessionConfig::from_env().map(|config| self.update_config(config))
    }

    /// Modify the existing config appropriately
    pub fn update_config(&self, mut config: SessionConfig) -> SessionConfig {
        if let Some(batch_size) = self.batch_size {
            config = config.with_batch_size(batch_size);
        }

        if let Some(partitions) = self.partitions {
            config = config.with_target_partitions(partitions);
        }

        if let Some(sort_spill_reservation_bytes) = self.sort_spill_reservation_bytes {
            config =
                config.with_sort_spill_reservation_bytes(sort_spill_reservation_bytes);
        }

        config
    }

    /// Return an appropriately configured `RuntimeEnvBuilder`
    pub fn runtime_env_builder(&self) -> Result<RuntimeEnvBuilder> {
        let mut rt_builder = RuntimeEnvBuilder::new();
        const NUM_TRACKED_CONSUMERS: usize = 5;
        if let Some(memory_limit) = self.memory_limit {
            let pool: Arc<dyn MemoryPool> = match self.mem_pool_type.as_str() {
                "fair" => Arc::new(TrackConsumersPool::new(
                    FairSpillPool::new(memory_limit),
                    NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                )),
                "greedy" => Arc::new(TrackConsumersPool::new(
                    GreedyMemoryPool::new(memory_limit),
                    NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                )),
                _ => {
                    return Err(DataFusionError::Configuration(format!(
                        "Invalid memory pool type: {}",
                        self.mem_pool_type
                    )));
                }
            };
            rt_builder = rt_builder
                .with_memory_pool(pool)
                .with_disk_manager_builder(DiskManagerBuilder::default());
        }
        Ok(rt_builder)
    }
}

/// Parse memory limit from string to number of bytes
/// e.g. '1.5G', '100M' -> 1572864
fn parse_memory_limit(limit: &str) -> Result<usize, String> {
    let (number, unit) = limit.split_at(limit.len() - 1);
    let number: f64 = number
        .parse()
        .map_err(|_| format!("Failed to parse number from memory limit '{limit}'"))?;

    match unit {
        "K" => Ok((number * 1024.0) as usize),
        "M" => Ok((number * 1024.0 * 1024.0) as usize),
        "G" => Ok((number * 1024.0 * 1024.0 * 1024.0) as usize),
        _ => Err(format!(
            "Unsupported unit '{unit}' in memory limit '{limit}'"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_memory_limit_all() {
        // Test valid inputs
        assert_eq!(parse_memory_limit("100K").unwrap(), 102400);
        assert_eq!(parse_memory_limit("1.5M").unwrap(), 1572864);
        assert_eq!(parse_memory_limit("2G").unwrap(), 2147483648);

        // Test invalid unit
        assert!(parse_memory_limit("500X").is_err());

        // Test invalid number
        assert!(parse_memory_limit("abcM").is_err());
    }
}
