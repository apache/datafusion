// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::{num::NonZeroUsize, sync::Arc};

use clap::Args;
use datafusion::{
    execution::{
        disk_manager::DiskManagerBuilder,
        memory_pool::{
            FairSpillPool, GreedyMemoryPool, MemoryPool, PeakRecordingPool,
            TrackConsumersPool,
        },
        object_store::ObjectStoreUrl,
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    },
    prelude::SessionConfig,
};
use datafusion_common::{DataFusionError, Result};
use object_store::local::LocalFileSystem;

use super::simulated_object_store::{
    BYTES_PER_SECOND, MAX_CONNECTIONS, SimulatedObjectStore,
};

// Common benchmark options (don't use doc comments otherwise this doc
// shows up in help files)
#[derive(Debug, Args, Clone)]
pub struct CommonOpt {
    /// Number of iterations of each test run
    #[arg(short = 'i', long = "iterations", default_value = "3", env)]
    pub iterations: usize,

    /// Number of partitions to process in parallel. Defaults to number of available cores.
    #[arg(short = 'n', long = "partitions")]
    pub partitions: Option<usize>,

    /// Batch size when reading CSV or Parquet files
    #[arg(short = 's', long = "batch-size")]
    pub batch_size: Option<usize>,

    /// The memory pool type to use, should be one of "fair" or "greedy"
    #[arg(long = "mem-pool-type", default_value = "fair", env)]
    pub mem_pool_type: String,

    /// Memory limit (e.g. '100M', '1.5G'). If not specified, run all pre-defined memory limits for given query
    /// if there's any, otherwise run with no memory limit.
    #[arg(long = "memory-limit", value_parser = parse_capacity_limit, env)]
    pub memory_limit: Option<usize>,

    /// The amount of memory to reserve for sort spill operations. DataFusion's default value will be used
    /// if not specified.
    #[arg(long = "sort-spill-reservation-bytes", value_parser = parse_capacity_limit)]
    pub sort_spill_reservation_bytes: Option<usize>,

    /// Activate debug mode to see more details
    #[arg(short, long, env)]
    pub debug: bool,

    /// Serve the local filesystem as if it were remote object storage (e.g. S3
    /// or GCS): per-request latency, per-connection bandwidth, a bounded
    /// connection pool, and paginated LIST. See
    /// [`crate::util::simulated_object_store`].
    #[arg(long = "simulate-latency", env)]
    pub simulate_latency: bool,
}

/// Session configuration overrides shared by query benchmarks.
///
/// Only explicitly supplied CLI arguments override `SessionConfig::from_env()`.
#[derive(Debug, Args, Clone, Default)]
pub struct QueryOpt {
    /// Disable collection of statistics (and cost based optimizations).
    #[arg(short = 'S', long = "disable-statistics")]
    disable_statistics: bool,

    /// Prefer hash joins over sort merge joins.
    #[arg(short = 'j', long = "prefer_hash_join")]
    prefer_hash_join: Option<bool>,

    /// Allow Piecewise Merge Join instead of Nested Loop Join.
    #[arg(short = 'w', long = "enable_piecewise_merge_join")]
    enable_piecewise_merge_join: Option<bool>,

    /// How many bytes to buffer on the probe side of hash joins.
    #[arg(long)]
    hash_join_buffering_capacity: Option<usize>,
}

impl QueryOpt {
    /// Read standard environment settings, then apply explicit CLI overrides.
    pub fn config(&self, common: &CommonOpt) -> Result<SessionConfig> {
        let mut config = common.config()?;
        if self.disable_statistics {
            config = config.with_collect_statistics(false);
        }
        if let Some(value) = self.prefer_hash_join {
            config.options_mut().optimizer.prefer_hash_join = value;
        }
        if let Some(value) = self.enable_piecewise_merge_join {
            config.options_mut().optimizer.enable_piecewise_merge_join = value;
        }
        if let Some(value) = self.hash_join_buffering_capacity {
            config.options_mut().execution.hash_join_buffering_capacity = value;
        }
        Ok(config)
    }
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
            // Record the peak reservation so benchmarks can report it next to
            // peak RSS. Purely observational: every call is delegated.
            let pool: Arc<dyn MemoryPool> = Arc::new(PeakRecordingPool::new(pool));
            rt_builder = rt_builder
                .with_memory_pool(pool)
                .with_disk_manager_builder(DiskManagerBuilder::default());
        }
        Ok(rt_builder)
    }

    /// Build the runtime environment, optionally presenting the local
    /// filesystem as remote object storage.
    pub fn build_runtime(&self) -> Result<Arc<RuntimeEnv>> {
        let rt = self.runtime_env_builder()?.build_arc()?;
        if self.simulate_latency {
            let store = SimulatedObjectStore::new(LocalFileSystem::new());
            let url = ObjectStoreUrl::parse("file:///")?;
            rt.register_object_store(url.as_ref(), Arc::new(store));
            println!(
                "Simulating remote object storage (get TTFB: 19-230ms, head TTFB: \
                 8-28ms, list TTFB: 40-200ms/page, {} MB/s per connection, {MAX_CONNECTIONS} connections)",
                BYTES_PER_SECOND / (1024 * 1024),
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

    #[derive(clap::Parser)]
    struct TestOptions {
        #[command(flatten)]
        common: CommonOpt,
        #[command(flatten)]
        query: QueryOpt,
    }

    #[test]
    fn session_settings_from_env_and_cli() {
        use clap::Parser;

        const CHILD: &str = "DATAFUSION_BENCHMARK_SESSION_SETTINGS_TEST";
        if let Ok(mode) = std::env::var(CHILD) {
            let mut args = vec!["benchmark"];
            if mode == "cli" {
                args.extend([
                    "--partitions",
                    "3",
                    "--batch-size",
                    "512",
                    "--sort-spill-reservation-bytes",
                    "2M",
                    "--prefer_hash_join",
                    "true",
                    "--enable_piecewise_merge_join",
                    "false",
                    "--hash-join-buffering-capacity",
                    "0",
                    "--disable-statistics",
                ]);
            }
            let options = TestOptions::try_parse_from(args).unwrap();
            let config = options.query.config(&options.common).unwrap();
            let settings = config.options();
            let cli = mode == "cli";
            assert_eq!(
                settings.execution.target_partitions,
                if cli { 3 } else { 2 }
            );
            assert_eq!(
                settings.execution.batch_size.get(),
                if cli { 512 } else { 256 }
            );
            assert_eq!(
                settings.execution.sort_spill_reservation_bytes,
                if cli { 2 * 1024 * 1024 } else { 1048576 }
            );
            assert_eq!(settings.optimizer.prefer_hash_join, cli);
            assert_eq!(settings.optimizer.enable_piecewise_merge_join, !cli);
            assert_eq!(
                settings.execution.hash_join_buffering_capacity,
                if cli { 0 } else { 4096 }
            );
            assert!(!settings.execution.collect_statistics);

            // The SQL runner uses the same standard environment settings.
            if !cli {
                let ctx = crate::sql_benchmark_runner::make_ctx(&options.common).unwrap();
                assert_eq!(ctx.copied_config().to_props(), config.to_props());
            }
            return;
        }

        // Isolate environment changes from parallel tests and inherited settings.
        for mode in ["env", "cli"] {
            let mut command =
                std::process::Command::new(std::env::current_exe().unwrap());
            for (key, _) in std::env::vars_os() {
                if key.to_string_lossy().starts_with("DATAFUSION_") {
                    command.env_remove(key);
                }
            }
            for key in [
                "ITERATIONS",
                "MEM_POOL_TYPE",
                "MEMORY_LIMIT",
                "DEBUG",
                "SIMULATE_LATENCY",
            ] {
                command.env_remove(key);
            }
            let output = command
                .args([
                    "--exact",
                    "util::options::tests::session_settings_from_env_and_cli",
                    "--nocapture",
                ])
                .env(CHILD, mode)
                .env("DATAFUSION_EXECUTION_TARGET_PARTITIONS", "2")
                .env("DATAFUSION_EXECUTION_BATCH_SIZE", "256")
                .env(
                    "DATAFUSION_EXECUTION_SORT_SPILL_RESERVATION_BYTES",
                    "1048576",
                )
                .env("DATAFUSION_OPTIMIZER_PREFER_HASH_JOIN", "false")
                .env("DATAFUSION_OPTIMIZER_ENABLE_PIECEWISE_MERGE_JOIN", "true")
                .env("DATAFUSION_EXECUTION_HASH_JOIN_BUFFERING_CAPACITY", "4096")
                .env(
                    "DATAFUSION_EXECUTION_COLLECT_STATISTICS",
                    if mode == "cli" { "true" } else { "false" },
                )
                // Obsolete aliases must not override the standard settings.
                .env("PARTITIONS", "8")
                .env("BATCH_SIZE", "1024")
                .env("SORT_SPILL_RESERVATION_BYTES", "8M")
                .env("PREFER_HASH_JOIN", "true")
                .env("HASH_JOIN_BUFFERING_CAPACITY", "0")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{mode} child failed:\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

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
