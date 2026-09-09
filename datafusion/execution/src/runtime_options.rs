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

//! Typed schema for the `datafusion.runtime.*` configuration namespace.
//!
//! [`RuntimeOptions`] is the single source of truth for the runtime
//! configuration keys, their descriptions, their defaults and how their string
//! values are parsed. It is built with the same [`ConfigField`] machinery that
//! backs [`ConfigOptions`], so `SET`/`RESET` handling no longer needs a
//! hand-written `match` arm per key.
//!
//! [`RuntimeOptions`] deliberately does *not* live inside [`ConfigOptions`]:
//! `datafusion-execution` depends on `datafusion-common`, never the reverse.
//!
//! [`ConfigOptions`]: datafusion_common::config::ConfigOptions

use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use crate::cache::cache_manager::{
    DEFAULT_FILE_STATISTICS_MEMORY_LIMIT, DEFAULT_LIST_FILES_CACHE_MEMORY_LIMIT,
    DEFAULT_LIST_FILES_CACHE_TTL, DEFAULT_METADATA_CACHE_LIMIT,
};
use crate::disk_manager::{
    DEFAULT_MAX_SPILL_MERGE_FAN_IN, DEFAULT_MAX_TEMP_DIRECTORY_SIZE, DiskManagerBuilder,
};
use crate::memory_pool::MemoryLimit;
use crate::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};

use datafusion_common::config::{ConfigEntry, ConfigField, Visit};
use datafusion_common::{
    DataFusionError, Result, config_field, config_namespace, plan_datafusion_err,
    plan_err,
};

/// Prefix shared by every key in this namespace.
pub const RUNTIME_CONFIG_PREFIX: &str = "datafusion.runtime";

/// A byte capacity written as a plain number of bytes (`0`) or a number with a
/// `K`, `M` or `G` suffix (`512K`, `100M`, `1.5G`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CapacityLimit(pub usize);

impl Display for CapacityLimit {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        const GB: usize = 1024 * 1024 * 1024;
        const MB: usize = 1024 * 1024;
        const KB: usize = 1024;

        match self.0 {
            s if s >= GB => write!(f, "{}G", s / GB),
            s if s >= MB => write!(f, "{}M", s / MB),
            s if s >= KB => write!(f, "{}K", s / KB),
            s => write!(f, "{s}"),
        }
    }
}

config_field!(CapacityLimit, value => CapacityLimit(parse_capacity(value)?));

/// A cache time-to-live written as minutes and/or seconds (`90s`, `2m`, `1m30s`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CacheTtl(pub Duration);

impl Display for CacheTtl {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let total = self.0.as_secs();
        write!(f, "{}m{}s", total / 60, total % 60)
    }
}

config_field!(CacheTtl, value => CacheTtl(parse_ttl(value)?));

/// A count that must not be negative.
///
/// A plain `usize` would parse through [`default_config_transform`], whose
/// message names the Rust type rather than the constraint.
///
/// [`default_config_transform`]: datafusion_common::config::default_config_transform
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NonNegativeCount(pub usize);

impl Display for NonNegativeCount {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

config_field!(NonNegativeCount, value => NonNegativeCount(
    value.parse::<usize>().map_err(|e| plan_datafusion_err!(
        "Failed to parse non-negative integer from value '{value}': {e}"
    ))?
));

config_namespace! {
    /// Options that configure the [`RuntimeEnv`] shared by a session.
    ///
    /// Field order below is the order entries are reported in.
    pub struct RuntimeOptions {
        /// Maximum memory limit for query execution. Supports suffixes K (kilobytes), M (megabytes), and G (gigabytes) or '0' for 0. Example: '2G' for 2 gigabytes.
        pub memory_limit: Option<CapacityLimit>, default = None

        /// Maximum temporary file directory size. Supports suffixes K (kilobytes), M (megabytes), and G (gigabytes) or '0' for 0. Example: '2G' for 2 gigabytes.
        pub max_temp_directory_size: CapacityLimit, default = CapacityLimit(DEFAULT_MAX_TEMP_DIRECTORY_SIZE as usize)

        /// Maximum number of spill files opened by one external merge pass. Use 0 for unlimited. Values below 2 still use 2 so a merge can make progress.
        pub max_spill_merge_fan_in: NonNegativeCount, default = NonNegativeCount(DEFAULT_MAX_SPILL_MERGE_FAN_IN)

        /// The path to the temporary file directory.
        pub temp_directory: Option<String>, default = None

        /// Maximum memory to use for file metadata cache such as Parquet metadata. Supports suffixes K (kilobytes), M (megabytes), and G (gigabytes) or '0' for 0. Example: '2G' for 2 gigabytes.
        pub metadata_cache_limit: CapacityLimit, default = CapacityLimit(DEFAULT_METADATA_CACHE_LIMIT)

        /// Maximum memory to use for list files cache. Supports suffixes K (kilobytes), M (megabytes), and G (gigabytes) or '0' for 0. Example: '2G' for 2 gigabytes.
        pub list_files_cache_limit: CapacityLimit, default = CapacityLimit(DEFAULT_LIST_FILES_CACHE_MEMORY_LIMIT)

        /// TTL (time-to-live) of the entries in the list file cache. Supports units m (minutes), and s (seconds). Example: '2m' for 2 minutes.
        pub list_files_cache_ttl: Option<CacheTtl>, default = DEFAULT_LIST_FILES_CACHE_TTL.map(CacheTtl)

        /// Maximum memory to use for file statistics cache. Supports suffixes K (kilobytes), M (megabytes), and G (gigabytes) or '0' for 0. Example: '2G' for 2 gigabytes.
        pub file_statistics_cache_limit: CapacityLimit, default = CapacityLimit(DEFAULT_FILE_STATISTICS_MEMORY_LIMIT)
    }
}

impl RuntimeOptions {
    /// Set `key` (without the `datafusion.runtime.` prefix) from its string form.
    pub fn set_entry(&mut self, key: &str, value: &str) -> Result<()> {
        ConfigField::set(self, key, value).map_err(|e| qualify(e, key))
    }

    /// Restore `key` (without the `datafusion.runtime.` prefix) to its default.
    pub fn reset_entry(&mut self, key: &str) -> Result<()> {
        ConfigField::reset(self, key).map_err(|e| qualify(e, key))
    }

    /// Every key in this namespace with its current value and description.
    pub fn entries(&self) -> Vec<ConfigEntry> {
        struct Visitor(Vec<ConfigEntry>);

        impl Visit for Visitor {
            fn some<V: Display>(
                &mut self,
                key: &str,
                value: V,
                description: &'static str,
            ) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: Some(value.to_string()),
                    description,
                })
            }

            fn none(&mut self, key: &str, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: None,
                    description,
                })
            }
        }

        let mut v = Visitor(vec![]);
        self.visit(&mut v, RUNTIME_CONFIG_PREFIX, "");
        v.0
    }

    /// Read the values currently in effect from the live resource objects.
    ///
    /// A [`MemoryLimit::Infinite`] pool has no representation here; callers that
    /// report values handle it separately.
    pub fn from_runtime_env(env: &RuntimeEnv) -> Self {
        let memory_limit = match env.memory_pool.memory_limit() {
            MemoryLimit::Finite(size) => Some(CapacityLimit(size)),
            MemoryLimit::Infinite | MemoryLimit::Unknown => None,
        };

        let temp_paths = env.disk_manager.temp_dir_paths();
        let temp_directory = (!temp_paths.is_empty()).then(|| {
            temp_paths
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(",")
        });

        Self {
            memory_limit,
            max_temp_directory_size: CapacityLimit(
                env.disk_manager.max_temp_directory_size() as usize,
            ),
            max_spill_merge_fan_in: NonNegativeCount(
                env.disk_manager.max_spill_merge_fan_in(),
            ),
            temp_directory,
            metadata_cache_limit: CapacityLimit(
                env.cache_manager.get_metadata_cache_limit(),
            ),
            list_files_cache_limit: CapacityLimit(
                env.cache_manager.get_list_files_cache_limit(),
            ),
            list_files_cache_ttl: env
                .cache_manager
                .get_list_files_cache_ttl()
                .map(CacheTtl),
            file_statistics_cache_limit: CapacityLimit(
                env.cache_manager.get_file_statistic_cache_limit(),
            ),
        }
    }

    /// Write the value of a single `key` onto `builder`.
    ///
    /// Only the named key is applied: rebuilding every resource on each `SET`
    /// would discard the live memory pool and its outstanding reservations.
    pub fn apply_key(
        &self,
        key: &str,
        mut builder: RuntimeEnvBuilder,
    ) -> Result<RuntimeEnvBuilder> {
        builder = match key {
            "memory_limit" => match self.memory_limit {
                Some(limit) => builder.with_memory_limit(limit.0, 1.0),
                None => {
                    builder.memory_pool = None;
                    builder
                }
            },
            "max_temp_directory_size" => builder
                .with_max_temp_directory_size(self.max_temp_directory_size.0 as u64),
            "max_spill_merge_fan_in" => {
                builder.with_max_spill_merge_fan_in(self.max_spill_merge_fan_in.0)
            }
            "temp_directory" => match &self.temp_directory {
                Some(path) => builder.with_temp_file_path(path),
                None => {
                    builder.disk_manager_builder = Some(DiskManagerBuilder::default());
                    builder
                }
            },
            "metadata_cache_limit" => {
                builder.with_metadata_cache_limit(self.metadata_cache_limit.0)
            }
            "list_files_cache_limit" => {
                builder.with_object_list_cache_limit(self.list_files_cache_limit.0)
            }
            "list_files_cache_ttl" => builder
                .with_object_list_cache_ttl(self.list_files_cache_ttl.map(|ttl| ttl.0)),
            "file_statistics_cache_limit" => builder
                .with_file_statistics_cache_limit(self.file_statistics_cache_limit.0),
            _ => return Err(unknown_key(key)),
        };
        Ok(builder)
    }
}

/// `config_namespace!` reports an unknown key as a [`DataFusionError::Configuration`];
/// parse failures below are [`DataFusionError::Plan`]. Restate the former in this
/// namespace's wording, and name the key on the latter.
fn qualify(e: DataFusionError, key: &str) -> DataFusionError {
    match e {
        DataFusionError::Configuration(_) => unknown_key(key),
        other => name_config(other, &format!("{RUNTIME_CONFIG_PREFIX}.{key}")),
    }
}

/// The parsers below take only the value, because [`ConfigField::set`] is not
/// given the key it is setting. Callers that know the key append it here.
fn name_config(e: DataFusionError, config_name: &str) -> DataFusionError {
    match e {
        DataFusionError::Plan(msg) => {
            plan_datafusion_err!("{msg} when setting '{config_name}'")
        }
        other => other,
    }
}

fn unknown_key(key: &str) -> DataFusionError {
    plan_datafusion_err!("Unknown runtime configuration: {RUNTIME_CONFIG_PREFIX}.{key}")
}

/// [`parse_capacity`], with `config_name` named in any error.
pub fn parse_capacity_limit(config_name: &str, limit: &str) -> Result<usize> {
    parse_capacity(limit).map_err(|e| name_config(e, config_name))
}

/// [`parse_ttl`], with `config_name` named in any error.
pub fn parse_duration(config_name: &str, duration: &str) -> Result<Duration> {
    parse_ttl(duration).map_err(|e| name_config(e, config_name))
}

/// Parse a byte capacity: `0`, or a number with a `K`, `M` or `G` suffix.
pub fn parse_capacity(limit: &str) -> Result<usize> {
    if limit.trim().is_empty() {
        return plan_err!("Empty limit value found");
    }
    if limit == "0" {
        return Ok(0);
    }
    let (unit_start, unit) = limit
        .char_indices()
        .next_back()
        .ok_or_else(|| plan_datafusion_err!("Empty limit value found"))?;
    let number: f64 = limit[..unit_start].parse().map_err(|_| {
        plan_datafusion_err!("Failed to parse number from limit '{limit}'")
    })?;
    if number.is_sign_negative() || number.is_infinite() {
        return plan_err!("Limit value should be positive finite number");
    }

    match unit {
        'K' => Ok((number * 1024.0) as usize),
        'M' => Ok((number * 1024.0 * 1024.0) as usize),
        'G' => Ok((number * 1024.0 * 1024.0 * 1024.0) as usize),
        _ => plan_err!(
            "Unsupported unit '{unit}' in limit '{limit}'. \
             Unit must be one of: 'K', 'M', 'G'"
        ),
    }
}

/// Parse a cache TTL: minutes and/or seconds, in that order (`90s`, `2m`, `1m30s`).
pub fn parse_ttl(ttl: &str) -> Result<Duration> {
    if ttl.trim().is_empty() {
        return plan_err!("Duration should not be empty or blank");
    }

    let mut minutes = None;
    let mut seconds = None;

    for part in ttl.split_inclusive(&['m', 's']) {
        let (unit_start, unit) = part.char_indices().next_back().ok_or_else(|| {
            plan_datafusion_err!("Duration should not be empty or blank")
        })?;
        let number: u64 = part[..unit_start].parse().map_err(|_| {
            plan_datafusion_err!("Failed to parse number from duration '{part}'")
        })?;

        match unit {
            'm' if minutes.is_none() && seconds.is_none() => minutes = Some(number),
            's' if seconds.is_none() => seconds = Some(number),
            other => plan_err!(
                "Invalid duration unit: '{other}'. The unit must be either 'm' (minutes), \
                 or 's' (seconds), and be in the correct order"
            )?,
        }
    }

    let secs = checked_secs(minutes, seconds)?;
    let duration = Duration::from_secs(secs);
    if duration.is_zero() {
        return plan_err!("Duration must be greater than 0 seconds");
    }
    Ok(duration)
}

fn checked_secs(mins: Option<u64>, secs: Option<u64>) -> Result<u64> {
    mins.unwrap_or_default()
        .checked_mul(60)
        .ok_or_else(|| {
            plan_datafusion_err!(
                "Duration has overflowed allowed maximum limit due to 'mins * 60'"
            )
        })?
        .checked_add(secs.unwrap_or_default())
        .ok_or_else(|| {
            plan_datafusion_err!(
                "Duration has overflowed allowed maximum limit due to 'mins * 60 + secs'"
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_runtime_constants() {
        let entries = RuntimeOptions::default().entries();
        let value = |key: &str| {
            entries
                .iter()
                .find(|e| e.key == format!("{RUNTIME_CONFIG_PREFIX}.{key}"))
                .unwrap_or_else(|| panic!("missing {key}"))
                .value
                .clone()
        };

        assert_eq!(value("memory_limit"), None);
        assert_eq!(value("max_temp_directory_size"), Some("100G".to_string()));
        assert_eq!(value("max_spill_merge_fan_in"), Some("0".to_string()));
        assert_eq!(value("temp_directory"), None);
        assert_eq!(value("metadata_cache_limit"), Some("50M".to_string()));
        assert_eq!(value("list_files_cache_limit"), Some("1M".to_string()));
        assert_eq!(value("list_files_cache_ttl"), None);
        assert_eq!(
            value("file_statistics_cache_limit"),
            Some("20M".to_string())
        );
    }

    /// `config_entries` reports what is in effect, which is not the same as
    /// `RuntimeOptions::default()`: an unbounded pool reports `unlimited`, and a
    /// bounded one reports its size.
    #[test]
    fn config_entries_report_the_live_memory_pool() {
        let memory_limit = |env: &RuntimeEnv| {
            env.config_entries()
                .into_iter()
                .find(|e| e.key == format!("{RUNTIME_CONFIG_PREFIX}.memory_limit"))
                .expect("missing memory_limit")
                .value
        };

        assert_eq!(
            memory_limit(&RuntimeEnv::default()),
            Some("unlimited".to_string())
        );

        let bounded = RuntimeEnvBuilder::new()
            .with_memory_limit(2 * 1024 * 1024 * 1024, 1.0)
            .build()
            .unwrap();
        assert_eq!(memory_limit(&bounded), Some("2G".to_string()));
    }

    #[test]
    fn set_then_reset_round_trips() {
        let mut options = RuntimeOptions::default();
        options.set_entry("memory_limit", "1.5G").unwrap();
        assert_eq!(
            options.memory_limit,
            Some(CapacityLimit((1.5 * 1024.0 * 1024.0 * 1024.0) as usize))
        );
        options.reset_entry("memory_limit").unwrap();
        assert_eq!(options.memory_limit, None);
    }

    #[test]
    fn unknown_key_is_a_plan_error() {
        let err = RuntimeOptions::default()
            .set_entry("nope", "1")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Unknown runtime configuration: datafusion.runtime.nope"),
            "{err}"
        );
    }

    #[test]
    fn parse_errors_name_the_key() {
        let err = RuntimeOptions::default()
            .set_entry("memory_limit", "10X")
            .unwrap_err()
            .to_string();
        assert!(err.contains("Unsupported unit 'X' in limit '10X'"), "{err}");
        assert!(
            err.contains("when setting 'datafusion.runtime.memory_limit'"),
            "{err}"
        );
    }

    #[test]
    fn non_ascii_value_does_not_panic() {
        // Regression guard for the panic fixed in apache/datafusion#23316.
        assert!(
            RuntimeOptions::default()
                .set_entry("memory_limit", "1️⃣")
                .is_err()
        );
    }

    #[test]
    fn ttl_round_trips_through_display() {
        let mut options = RuntimeOptions::default();
        options.set_entry("list_files_cache_ttl", "1m30s").unwrap();
        assert_eq!(
            options.list_files_cache_ttl,
            Some(CacheTtl(Duration::from_secs(90)))
        );
        assert_eq!(options.list_files_cache_ttl.unwrap().to_string(), "1m30s");
    }
}
