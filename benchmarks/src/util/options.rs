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
        object_store::ObjectStoreUrl,
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    },
    prelude::SessionConfig,
};
use datafusion_common::{DataFusionError, Result};
use object_store::local::LocalFileSystem;

use super::latency_object_store::LatencyObjectStore;

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
    #[arg(long = "memory-limit", value_parser = parse_capacity_limit)]
    pub memory_limit: Option<usize>,

    /// The amount of memory to reserve for sort spill operations. DataFusion's default value will be used
    /// if not specified.
    #[arg(long = "sort-spill-reservation-bytes", value_parser = parse_capacity_limit)]
    pub sort_spill_reservation_bytes: Option<usize>,

    /// Activate debug mode to see more details
    #[arg(short, long)]
    pub debug: bool,

    /// Simulate object store latency to mimic remote storage (e.g. S3).
    /// Adds random latency in the range 20-200ms to each object store operation.
    #[arg(long = "simulate-latency")]
    pub simulate_latency: bool,
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
        // Use CLI --memory-limit if provided, otherwise fall back to
        // DATAFUSION_RUNTIME_MEMORY_LIMIT env var
        let memory_limit = self.memory_limit.or_else(|| {
            std::env::var("DATAFUSION_RUNTIME_MEMORY_LIMIT")
                .ok()
                .and_then(|val| parse_capacity_limit(&val).ok())
        });

        if let Some(memory_limit) = memory_limit {
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

    /// Build the runtime environment, optionally wrapping the local filesystem
    /// with a throttled object store to simulate remote storage latency.
    pub fn build_runtime(&self) -> Result<Arc<RuntimeEnv>> {
        let rt = self.runtime_env_builder()?.build_arc()?;
        if self.simulate_latency {
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(LatencyObjectStore::new(LocalFileSystem::new()));
            let url = ObjectStoreUrl::parse("file:///")?;
            rt.register_object_store(url.as_ref(), store);
            println!(
                "Simulating S3-like object store latency (get: 25-200ms, list: 40-400ms)"
            );
        }
        Ok(rt)
    }
}

/// Parse capacity limit from string to number of bytes by allowing units: K, M and G.
/// Supports formats like '1.5G' -> 1610612736, '100M' -> 104857600
fn parse_capacity_limit(limit: &str) -> Result<usize, String> {
    if limit.trim().is_empty() {
        return Err("Capacity limit cannot be empty".to_string());
    }
    let (number, unit) = limit.split_at(limit.len() - 1);
    let number: f64 = number
        .parse()
        .map_err(|_| format!("Failed to parse number from capacity limit '{limit}'"))?;
    if number.is_sign_negative() || number.is_infinite() {
        return Err("Limit value should be positive finite number".to_string());
    }

    match unit {
        "K" => Ok((number * 1024.0) as usize),
        "M" => Ok((number * 1024.0 * 1024.0) as usize),
        "G" => Ok((number * 1024.0 * 1024.0 * 1024.0) as usize),
        _ => Err(format!(
            "Unsupported unit '{unit}' in capacity limit '{limit}'. Unit must be one of: 'K', 'M', 'G'"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_env_builder_reads_env_var() {
        // Set the env var and verify runtime_env_builder picks it up
        // when no CLI --memory-limit is provided
        let opt = CommonOpt {
            iterations: 3,
            partitions: None,
            batch_size: None,
            mem_pool_type: "fair".to_string(),
            memory_limit: None,
            sort_spill_reservation_bytes: None,
            debug: false,
            simulate_latency: false,
        };

        // With env var set, builder should succeed and have a memory pool
        // SAFETY: This test is single-threaded and the env var is restored after use
        unsafe {
            std::env::set_var("DATAFUSION_RUNTIME_MEMORY_LIMIT", "2G");
        }
        let builder = opt.runtime_env_builder().unwrap();
        let runtime = builder.build().unwrap();
        unsafe {
            std::env::remove_var("DATAFUSION_RUNTIME_MEMORY_LIMIT");
        }
        // A 2G memory pool should be present — verify it reports the correct limit
        match runtime.memory_pool.memory_limit() {
            datafusion::execution::memory_pool::MemoryLimit::Finite(limit) => {
                assert_eq!(limit, 2 * 1024 * 1024 * 1024);
            }
            _ => panic!("Expected Finite memory limit"),
        }
    }

    #[test]
    fn test_parse_capacity_limit_all() {
        // Test valid inputs
        assert_eq!(parse_capacity_limit("100K").unwrap(), 102400);
        assert_eq!(parse_capacity_limit("1.5M").unwrap(), 1572864);
        assert_eq!(parse_capacity_limit("2G").unwrap(), 2147483648);

        // Test invalid unit
        assert!(parse_capacity_limit("500X").is_err());

        // Test invalid number
        assert!(parse_capacity_limit("abcM").is_err());

        // Test negative number
        assert!(parse_capacity_limit("-1M").is_err());

        // Test infinite number
        assert!(parse_capacity_limit("infM").is_err());

        // Test negative infinite number
        assert!(parse_capacity_limit("-infM").is_err());
    }
}
