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
use std::collections::HashMap;

/// Configuration option "datafusion.optimizer.filterNullJoinKeys"
pub const OPT_FILTER_NULL_JOIN_KEYS: &str = "datafusion.optimizer.filterNullJoinKeys";

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
    pub fn new_bool(name: &str, description: &str, default_value: bool) -> Self {
        Self {
            key: name.to_string(),
            description: description.to_string(),
            data_type: DataType::Boolean,
            default_value: ScalarValue::Boolean(Some(default_value)),
        }
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
            )],
        }
    }

    /// Generate documentation that can be included int he user guide
    pub fn generate_config_markdown() -> String {
        let configs = Self::new();
        let mut docs = "| key | type | default | description |\n".to_string();
        docs += "|-----|------|---------|-------------|\n";
        for config in configs.config_definitions {
            docs += &format!(
                "| {} | {} | {} | {} |\n",
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
}

#[cfg(test)]
mod test {
    use crate::config::{BuiltInConfigs, ConfigOptions};

    #[test]
    fn docs() {
        let docs = BuiltInConfigs::generate_config_markdown();
        assert_eq!("| key | type | default | description |\
        \n|-----|------|---------|-------------|\
        \n| datafusion.optimizer.filterNullJoinKeys | Boolean | false | When set to true, the optimizer \
        will insert filters before a join between a nullable and non-nullable column to filter out \
        nulls on the nullable side. This filter can add additional overhead when the file format does \
        not fully support predicate push down. |\n", docs);
    }

    #[test]
    fn get_then_set() {
        let mut config = ConfigOptions::new();
        let config_key = "datafusion.optimizer.filterNullJoinKeys";
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
