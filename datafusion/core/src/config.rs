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
use std::collections::HashMap;

/// Configuration option "datafusion.optimizer.filter_null_join_keys"
pub const OPT_FILTER_NULL_JOIN_KEYS: &str = "datafusion.optimizer.filter_null_join_keys";

/// Configuration option "datafusion.execution.batch_size"
pub const OPT_BATCH_SIZE: &str = "datafusion.execution.batch_size";

/// Configuration option "datafusion.execution.coalesce_batches"
pub const OPT_COALESCE_BATCHES: &str = "datafusion.execution.coalesce_batches";

/// Configuration option "datafusion.execution.coalesce_target_batch_size"
pub const OPT_COALESCE_TARGET_BATCH_SIZE: &str =
    "datafusion.execution.coalesce_target_batch_size";

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
            config_definitions: vec![ConfigDefinition::new_bool(
                OPT_FILTER_NULL_JOIN_KEYS,
                "When set to true, the optimizer will insert filters before a join between \
                a nullable and non-nullable column to filter out nulls on the nullable side. This \
                filter can add additional overhead when the file format does not fully support \
                predicate push down.",
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
            )],
        }
    }

    /// Generate documentation that can be included int he user guide
    pub fn generate_config_markdown() -> String {
        use std::fmt::Write as _;
        let configs = Self::new();
        let mut docs = "| key | type | default | description |\n".to_string();
        docs += "|-----|------|---------|-------------|\n";
        for config in configs
            .config_definitions
            .iter()
            .sorted_by_key(|c| c.key.as_str())
        {
            let _ = writeln!(
                &mut docs,
                "| {} | {} | {} | {} |",
                config.key, config.data_type, config.default_value, config.description
            );
        }
        docs
    }
}

/// Configuration options struct. This can contain values for built-in and custom options
#[derive(Debug, Clone)]
pub struct ConfigOptions {
    options: HashMap<String, ScalarValue>,
}

impl Default for ConfigOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigOptions {
    /// Create new ConfigOptions struct
    pub fn new() -> Self {
        let mut options = HashMap::new();
        let built_in = BuiltInConfigs::new();
        for config_def in &built_in.config_definitions {
            options.insert(config_def.key.clone(), config_def.default_value.clone());
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

    /// get a configuration option
    pub fn get(&self, key: &str) -> Option<ScalarValue> {
        self.options.get(key).cloned()
    }

    /// get a boolean configuration option
    pub fn get_bool(&self, key: &str) -> bool {
        match self.get(key) {
            Some(ScalarValue::Boolean(Some(b))) => b,
            _ => false,
        }
    }

    /// get a u64 configuration option
    pub fn get_u64(&self, key: &str) -> u64 {
        match self.get(key) {
            Some(ScalarValue::UInt64(Some(n))) => n,
            _ => 0,
        }
    }

    /// Access the underlying hashmap
    pub fn options(&self) -> &HashMap<String, ScalarValue> {
        &self.options
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
        assert!(!config.get_bool(config_key));
        config.set_bool(config_key, true);
        assert!(config.get_bool(config_key));
    }

    #[test]
    fn get_invalid_config() {
        let config = ConfigOptions::new();
        let invalid_key = "not.valid";
        assert!(config.get(invalid_key).is_none());
        assert!(!config.get_bool(invalid_key));
    }
}
