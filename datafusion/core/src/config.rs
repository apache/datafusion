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

//! DataFusion Configuration Options

use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use itertools::Itertools;
use log::warn;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Configuration option "datafusion.execution.target_partitions"
pub const OPT_TARGET_PARTITIONS: &str = "datafusion.execution.target_partitions";

/// Configuration option "datafusion.catalog.create_default_catalog_and_schema"
pub const OPT_CREATE_DEFAULT_CATALOG_AND_SCHEMA: &str =
    "datafusion.catalog.create_default_catalog_and_schema";
/// Configuration option "datafusion.catalog.information_schema"
pub const OPT_INFORMATION_SCHEMA: &str = "datafusion.catalog.information_schema";

/// Configuration option "datafusion.optimizer.repartition_joins"
pub const OPT_REPARTITION_JOINS: &str = "datafusion.optimizer.repartition_joins";

/// Configuration option "datafusion.optimizer.repartition_aggregations"
pub const OPT_REPARTITION_AGGREGATIONS: &str =
    "datafusion.optimizer.repartition_aggregations";

/// Configuration option "datafusion.optimizer.repartition_windows"
pub const OPT_REPARTITION_WINDOWS: &str = "datafusion.optimizer.repartition_windows";

/// Configuration option "datafusion.execuction_collect_statistics"
pub const OPT_COLLECT_STATISTICS: &str = "datafusion.execuction_collect_statistics";

/// Configuration option "datafusion.optimizer.filter_null_join_keys"
pub const OPT_FILTER_NULL_JOIN_KEYS: &str = "datafusion.optimizer.filter_null_join_keys";

/// Configuration option "datafusion.explain.logical_plan_only"
pub const OPT_EXPLAIN_LOGICAL_PLAN_ONLY: &str = "datafusion.explain.logical_plan_only";

/// Configuration option "datafusion.explain.physical_plan_only"
pub const OPT_EXPLAIN_PHYSICAL_PLAN_ONLY: &str = "datafusion.explain.physical_plan_only";

/// Configuration option "datafusion.execution.batch_size"
pub const OPT_BATCH_SIZE: &str = "datafusion.execution.batch_size";

/// Configuration option "datafusion.execution.coalesce_batches"
pub const OPT_COALESCE_BATCHES: &str = "datafusion.execution.coalesce_batches";

/// Configuration option "datafusion.execution.coalesce_target_batch_size"
pub const OPT_COALESCE_TARGET_BATCH_SIZE: &str =
    "datafusion.execution.coalesce_target_batch_size";

/// Configuration option "datafusion.execution.time_zone"
pub const OPT_TIME_ZONE: &str = "datafusion.execution.time_zone";

/// Configuration option "datafusion.execution.parquet.pushdown_filters"
pub const OPT_PARQUET_PUSHDOWN_FILTERS: &str =
    "datafusion.execution.parquet.pushdown_filters";

/// Configuration option "datafusion.execution.parquet.reorder_filters"
pub const OPT_PARQUET_REORDER_FILTERS: &str =
    "datafusion.execution.parquet.reorder_filters";

/// Configuration option "datafusion.execution.parquet.enable_page_index"
pub const OPT_PARQUET_ENABLE_PAGE_INDEX: &str =
    "datafusion.execution.parquet.enable_page_index";

/// Configuration option "datafusion.execution.parquet.pruning"
pub const OPT_PARQUET_ENABLE_PRUNING: &str = "datafusion.execution.parquet.pruning";

/// Configuration option "datafusion.execution.parquet.skip_metadata"
pub const OPT_PARQUET_SKIP_METADATA: &str = "datafusion.execution.parquet.skip_metadata";

/// Configuration option "datafusion.execution.parquet.metadata_size_hint"
pub const OPT_PARQUET_METADATA_SIZE_HINT: &str =
    "datafusion.execution.parquet.metadata_size_hint";

/// Configuration option "datafusion.optimizer.skip_failed_rules"
pub const OPT_OPTIMIZER_SKIP_FAILED_RULES: &str =
    "datafusion.optimizer.skip_failed_rules";

/// Configuration option "datafusion.optimizer.max_passes"
pub const OPT_OPTIMIZER_MAX_PASSES: &str = "datafusion.optimizer.max_passes";

/// Location scanned to load tables for `default` schema
pub const OPT_CATALOG_LOCATION: &str = "datafusion.catalog.location";

/// Type of `TableProvider` to use when loading `default` schema
pub const OPT_CATALOG_TYPE: &str = "datafusion.catalog.type";

/// Configuration option "datafusion.optimizer.top_down_join_key_reordering"
pub const OPT_TOP_DOWN_JOIN_KEY_REORDERING: &str =
    "datafusion.optimizer.top_down_join_key_reordering";

/// Configuration option "datafusion.optimizer.prefer_hash_join"
pub const OPT_PREFER_HASH_JOIN: &str = "datafusion.optimizer.prefer_hash_join";

/// Configuration option "atafusion.optimizer.hash_join_single_partition_threshold"
pub const OPT_HASH_JOIN_SINGLE_PARTITION_THRESHOLD: &str =
    "datafusion.optimizer.hash_join_single_partition_threshold";

/// Definition of a configuration option
pub struct ConfigDefinition {
    /// key used to identifier this configuration option
    key: String,
    /// Description to be used in generated documentation
    description: String,
    /// Data type of this option
    data_type: DataType,
    /// Default value
    default_value: ScalarValue,
}

macro_rules! get_conf_value {
    ($SELF: expr, $TPE: ident, $KEY: expr, $TPE_NAME: expr) => {
        match $SELF.get($KEY) {
            Some(ScalarValue::$TPE(v)) => v,
            Some(v) => {
                warn!(
                    "Config type mismatch for {}. Expected: {}, got: {:?}",
                    $KEY, $TPE_NAME, &v
                );
                None
            }
            None => None,
        }
    };
}

impl ConfigDefinition {
    /// Create a configuration option definition
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        data_type: DataType,
        default_value: ScalarValue,
    ) -> Self {
        Self {
            key: name.into(),
            description: description.into(),
            data_type,
            default_value,
        }
    }

    /// Create a configuration option definition with a boolean value
    pub fn new_bool(
        key: impl Into<String>,
        description: impl Into<String>,
        default_value: bool,
    ) -> Self {
        Self::new(
            key,
            description,
            DataType::Boolean,
            ScalarValue::Boolean(Some(default_value)),
        )
    }

    /// Create a configuration option definition with a u64 value
    pub fn new_u64(
        key: impl Into<String>,
        description: impl Into<String>,
        default_value: u64,
    ) -> Self {
        Self::new(
            key,
            description,
            DataType::UInt64,
            ScalarValue::UInt64(Some(default_value)),
        )
    }

    /// Create a configuration option definition with a string value
    pub fn new_string(
        key: impl Into<String>,
        description: impl Into<String>,
        default_value: Option<String>,
    ) -> Self {
        Self::new(
            key,
            description,
            DataType::Utf8,
            ScalarValue::Utf8(default_value),
        )
    }
}

/// Contains definitions for all built-in configuration options
pub struct BuiltInConfigs {
    /// Configuration option definitions
    config_definitions: Vec<ConfigDefinition>,
}

impl Default for BuiltInConfigs {
    fn default() -> Self {
        Self::new()
    }
}

impl BuiltInConfigs {
    /// Create a new BuiltInConfigs struct containing definitions for all built-in
    /// configuration options
    pub fn new() -> Self {
        Self {
            config_definitions: vec![ConfigDefinition::new_u64(
                OPT_TARGET_PARTITIONS,
                "Number of partitions for query execution. Increasing partitions can increase \
                 concurrency. Defaults to the number of cpu cores on the system.",
                num_cpus::get() as u64,
            ),

            ConfigDefinition::new_bool(
                OPT_CREATE_DEFAULT_CATALOG_AND_SCHEMA,
                "Whether the default catalog and schema should be created automatically.",
                true
            ),

            ConfigDefinition::new_bool(
                OPT_INFORMATION_SCHEMA,
                "Should DataFusion provide access to `information_schema` \
                 virtual tables for displaying schema information",
                false
            ),

            ConfigDefinition::new_bool(
                OPT_REPARTITION_JOINS,
                "Should DataFusion repartition data using the join keys to execute joins in parallel \
                 using the provided `target_partitions` level",
                true
            ),

            ConfigDefinition::new_bool(
                OPT_REPARTITION_AGGREGATIONS,
                "Should DataFusion repartition data using the aggregate keys to execute aggregates \
                 in parallel using the provided `target_partitions` level",
                true
            ),

            ConfigDefinition::new_bool(
                OPT_REPARTITION_WINDOWS,
                "Should DataFusion collect statistics after listing files",
                true
            ),

            ConfigDefinition::new_bool(
                OPT_COLLECT_STATISTICS,
                "Should DataFusion repartition data using the partitions keys to execute window \
                 functions in parallel using the provided `target_partitions` level",
                false
            ),

            ConfigDefinition::new_bool(
                OPT_FILTER_NULL_JOIN_KEYS,
                "When set to true, the optimizer will insert filters before a join between \
                a nullable and non-nullable column to filter out nulls on the nullable side. This \
                filter can add additional overhead when the file format does not fully support \
                predicate push down.",
                false,
            ),
            ConfigDefinition::new_bool(
                OPT_EXPLAIN_LOGICAL_PLAN_ONLY,
                "When set to true, the explain statement will only print logical plans.",
                false,
            ),
            ConfigDefinition::new_bool(
                OPT_EXPLAIN_PHYSICAL_PLAN_ONLY,
                "When set to true, the explain statement will only print physical plans.",
                false,
            ),
            ConfigDefinition::new_u64(
                OPT_BATCH_SIZE,
                "Default batch size while creating new batches, it's especially useful for \
                 buffer-in-memory batches since creating tiny batches would results in too much metadata \
                 memory consumption.",
                8192,
            ),
            ConfigDefinition::new_bool(
                OPT_COALESCE_BATCHES,
                format!("When set to true, record batches will be examined between each operator and \
                small batches will be coalesced into larger batches. This is helpful when there \
                are highly selective filters or joins that could produce tiny output batches. The \
                target batch size is determined by the configuration setting \
                '{}'.", OPT_COALESCE_TARGET_BATCH_SIZE),
                true,
            ),
             ConfigDefinition::new_u64(
                 OPT_COALESCE_TARGET_BATCH_SIZE,
                 format!("Target batch size when coalescing batches. Uses in conjunction with the \
                          configuration setting '{}'.", OPT_COALESCE_BATCHES),
                 4096,
            ),
            ConfigDefinition::new_string(
                OPT_TIME_ZONE,
                "The session time zone which some function require \
                e.g. EXTRACT(HOUR from SOME_TIME) shift the underline datetime according to the time zone,
                then extract the hour.",
                Some("+00:00".into()),
            ),
            ConfigDefinition::new_bool(
                OPT_PARQUET_PUSHDOWN_FILTERS,
                "If true, filter expressions are be applied during the parquet decoding operation to \
                 reduce the number of rows decoded.",
                false,
            ),
            ConfigDefinition::new_bool(
                OPT_PARQUET_REORDER_FILTERS,
                "If true, filter expressions evaluated during the parquet decoding opearation \
                 will be reordered heuristically to minimize the cost of evaluation. If false, \
                 the filters are applied in the same order as written in the query.",
                false,
            ),
            ConfigDefinition::new_bool(
                OPT_PARQUET_ENABLE_PAGE_INDEX,
                "If true, uses parquet data page level metadata (Page Index) statistics \
                 to reduce the number of rows decoded.",
                false,
            ),
            ConfigDefinition::new_bool(
                OPT_PARQUET_ENABLE_PRUNING,
                "If true, the parquet reader attempts to skip entire row groups based \
                 on the predicate in the query and the metadata (min/max values) stored in \
                 the parquet file.",
                true,
            ),
            ConfigDefinition::new_bool(
                OPT_PARQUET_SKIP_METADATA,
                "If true, the parquet reader skip the optional embedded metadata that may be in \
                the file Schema. This setting can help avoid schema conflicts when querying \
                multiple parquet files with schemas containing compatible types but different metadata.",
                true,
            ),
            ConfigDefinition::new(
                OPT_PARQUET_METADATA_SIZE_HINT,
                "If specified, the parquet reader will try and fetch the last `size_hint` \
                 bytes of the parquet file optimistically. If not specified, two read are required: \
                 One read to fetch the 8-byte parquet footer and  \
                 another to fetch the metadata length encoded in the footer.",
                DataType::UInt64,
                ScalarValue::UInt64(None),
            ),
            ConfigDefinition::new_bool(
                OPT_OPTIMIZER_SKIP_FAILED_RULES,
                "When set to true, the logical plan optimizer will produce warning \
                messages if any optimization rules produce errors and then proceed to the next \
                rule. When set to false, any rules that produce errors will cause the query to fail.",
                true
            ),
            ConfigDefinition::new_u64(
                OPT_OPTIMIZER_MAX_PASSES,
                "Number of times that the optimizer will attempt to optimize the plan",
                3
            ),
            ConfigDefinition::new_string(
                OPT_CATALOG_LOCATION,
                "Location scanned to load tables for `default` schema, defaults to None",
                None,
            ),
            ConfigDefinition::new_string(
                OPT_CATALOG_TYPE,
                "Type of `TableProvider` to use when loading `default` schema. Defaults to None",
                None,
            ),
             ConfigDefinition::new_bool(
                 OPT_TOP_DOWN_JOIN_KEY_REORDERING,
                 "When set to true, the physical plan optimizer will run a top down process to reorder the join keys. Defaults to true",
                 true,
             ),
             ConfigDefinition::new_bool(
                 OPT_PREFER_HASH_JOIN,
                 "When set to true, the physical plan optimizer will prefer HashJoin over SortMergeJoin. HashJoin can work more efficiently\
                 than SortMergeJoin but consumes more memory. Defaults to true",
                 true,
             ),
             ConfigDefinition::new_u64(
                 OPT_HASH_JOIN_SINGLE_PARTITION_THRESHOLD,
                 "The maximum estimated size in bytes for one input side of a HashJoin will be collected into a single partition",
                 1024 * 1024,
             ),
            ]
        }
    }

    /// Generate documentation that can be included in the user guide
    pub fn generate_config_markdown() -> String {
        use std::fmt::Write as _;
        let configs = Self::new();
        let mut docs = "| key | type | default | description |\n".to_string();
        docs += "|-----|------|---------|-------------|\n";

        let config_definitions: Vec<_> = configs
            .config_definitions
            .into_iter()
            .map(normalize_for_display)
            .collect();

        for config in config_definitions.iter().sorted_by_key(|c| c.key.as_str()) {
            let _ = writeln!(
                &mut docs,
                "| {} | {} | {} | {} |",
                config.key, config.data_type, config.default_value, config.description
            );
        }
        docs
    }
}

/// Normalizes a config definition prior to markdown display
fn normalize_for_display(mut v: ConfigDefinition) -> ConfigDefinition {
    // Since the default value of target_partitions depends on the number of cores,
    // set the default value to 0 in the docs.
    if v.key == OPT_TARGET_PARTITIONS {
        v.default_value = ScalarValue::UInt64(Some(0))
    }
    v
}

/// Configuration options struct. This can contain values for built-in and custom options
#[derive(Clone)]
pub struct ConfigOptions {
    options: HashMap<String, ScalarValue>,
}

/// Print the configurations in an ordered way so that we can directly compare the equality of two ConfigOptions by their debug strings
impl Debug for ConfigOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigOptions")
            .field(
                "options",
                &format!("{:?}", BTreeMap::from_iter(self.options.iter())),
            )
            .finish()
    }
}

impl Default for ConfigOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigOptions {
    /// Create new ConfigOptions struct
    pub fn new() -> Self {
        let built_in = BuiltInConfigs::new();
        let mut options = HashMap::with_capacity(built_in.config_definitions.len());
        for config_def in &built_in.config_definitions {
            options.insert(config_def.key.clone(), config_def.default_value.clone());
        }
        Self { options }
    }

    /// Create a new [`ConfigOptions`] wrapped in an RwLock and Arc
    pub fn into_shareable(self) -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(self))
    }

    /// Create new ConfigOptions struct, taking values from
    /// environment variables where possible.
    ///
    /// For example, setting `DATAFUSION_EXECUTION_BATCH_SIZE` will
    /// control `datafusion.execution.batch_size`.
    pub fn from_env() -> Self {
        let built_in = BuiltInConfigs::new();
        let mut options = HashMap::with_capacity(built_in.config_definitions.len());
        for config_def in &built_in.config_definitions {
            let config_value = {
                let mut env_key = config_def.key.replace('.', "_");
                env_key.make_ascii_uppercase();
                match env::var(&env_key) {
                    Ok(value) => match ScalarValue::try_from_string(
                        value.clone(),
                        &config_def.data_type,
                    ) {
                        Ok(parsed) => parsed,
                        Err(_) => {
                            warn!("Warning: could not parse environment variable {}={} to type {}.", env_key, value, config_def.data_type);
                            config_def.default_value.clone()
                        }
                    },
                    Err(_) => config_def.default_value.clone(),
                }
            };
            options.insert(config_def.key.clone(), config_value);
        }
        Self { options }
    }

    /// set a configuration option
    pub fn set(&mut self, key: &str, value: ScalarValue) {
        self.options.insert(key.to_string(), value);
    }

    /// set a boolean configuration option
    pub fn set_bool(&mut self, key: &str, value: bool) {
        self.set(key, ScalarValue::Boolean(Some(value)))
    }

    /// set a `u64` configuration option
    pub fn set_u64(&mut self, key: &str, value: u64) {
        self.set(key, ScalarValue::UInt64(Some(value)))
    }

    /// set a `usize` configuration option
    pub fn set_usize(&mut self, key: &str, value: usize) {
        let value: u64 = value.try_into().expect("convert u64 to usize");
        self.set(key, ScalarValue::UInt64(Some(value)))
    }

    /// set a `String` configuration option
    pub fn set_string(&mut self, key: &str, value: impl Into<String>) {
        self.set(key, ScalarValue::Utf8(Some(value.into())))
    }

    /// get a configuration option
    pub fn get(&self, key: &str) -> Option<ScalarValue> {
        self.options.get(key).cloned()
    }

    /// get a boolean configuration option
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        get_conf_value!(self, Boolean, key, "bool")
    }

    /// get a u64 configuration option
    pub fn get_u64(&self, key: &str) -> Option<u64> {
        get_conf_value!(self, UInt64, key, "u64")
    }

    /// get a u64 configuration option as a usize
    pub fn get_usize(&self, key: &str) -> Option<usize> {
        let v = get_conf_value!(self, UInt64, key, "usize");
        v.and_then(|v| v.try_into().ok())
    }

    /// get a string configuration option
    pub fn get_string(&self, key: &str) -> Option<String> {
        get_conf_value!(self, Utf8, key, "string")
    }

    /// Access the underlying hashmap
    pub fn options(&self) -> &HashMap<String, ScalarValue> {
        &self.options
    }

    /// Tests if the key exists in the configuration
    pub fn exists(&self, key: &str) -> bool {
        self.options().contains_key(key)
    }
}

#[cfg(test)]
mod test {
    use crate::config::{BuiltInConfigs, ConfigOptions};

    #[test]
    fn docs() {
        let docs = BuiltInConfigs::generate_config_markdown();
        let mut lines = docs.lines();
        assert_eq!(
            lines.next().unwrap(),
            "| key | type | default | description |"
        );
        let configs = BuiltInConfigs::default();
        for config in configs.config_definitions {
            assert!(docs.contains(&config.key));
        }
    }

    #[test]
    fn get_then_set() {
        let mut config = ConfigOptions::new();
        let config_key = "datafusion.optimizer.filter_null_join_keys";
        assert!(!config.get_bool(config_key).unwrap_or_default());
        config.set_bool(config_key, true);
        assert!(config.get_bool(config_key).unwrap_or_default());
    }

    #[test]
    fn get_invalid_config() {
        let config = ConfigOptions::new();
        let invalid_key = "not.valid";
        assert!(!config.exists(invalid_key));
        assert!(!config.get_bool(invalid_key).unwrap_or_default());
    }

    #[test]
    fn get_config_in_invalid_format() {
        let config = ConfigOptions::new();
        let key = "datafusion.execution.batch_size";

        assert!(config.exists(key));
        assert_eq!(None, config.get_string(key));
        assert!(!config.get_bool(key).unwrap_or_default());
    }
}
