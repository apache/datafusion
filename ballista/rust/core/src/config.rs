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
//

//! Ballista configuration

use std::collections::HashMap;

use crate::error::{BallistaError, Result};

use datafusion::arrow::datatypes::DataType;
use log::warn;

pub const BALLISTA_DEFAULT_SHUFFLE_PARTITIONS: &str = "ballista.shuffle.partitions";

/// Configuration option meta-data
#[derive(Debug, Clone)]
pub struct ConfigEntry {
    name: String,
    _description: String,
    _data_type: DataType,
    default_value: Option<String>,
}

impl ConfigEntry {
    fn new(
        name: String,
        _description: String,
        _data_type: DataType,
        default_value: Option<String>,
    ) -> Self {
        Self {
            name,
            _description,
            _data_type,
            default_value,
        }
    }
}

/// Ballista configuration builder
pub struct BallistaConfigBuilder {
    settings: HashMap<String, String>,
}

impl Default for BallistaConfigBuilder {
    /// Create a new config builder
    fn default() -> Self {
        Self {
            settings: HashMap::new(),
        }
    }
}

impl BallistaConfigBuilder {
    /// Create a new config with an additional setting
    pub fn set(&self, k: &str, v: &str) -> Self {
        let mut settings = self.settings.clone();
        settings.insert(k.to_owned(), v.to_owned());
        Self { settings }
    }

    pub fn build(&self) -> Result<BallistaConfig> {
        BallistaConfig::with_settings(self.settings.clone())
    }
}

/// Ballista configuration
#[derive(Debug, Clone)]
pub struct BallistaConfig {
    /// Settings stored in map for easy serde
    settings: HashMap<String, String>,
}

impl BallistaConfig {
    /// Create a default configuration
    pub fn new() -> Result<Self> {
        Self::with_settings(HashMap::new())
    }

    /// Create a configuration builder
    pub fn builder() -> BallistaConfigBuilder {
        BallistaConfigBuilder::default()
    }

    /// Create a new configuration based on key-value pairs
    pub fn with_settings(settings: HashMap<String, String>) -> Result<Self> {
        let supported_entries = BallistaConfig::valid_entries();
        for (name, entry) in &supported_entries {
            if let Some(v) = settings.get(name) {
                // validate that we can parse the user-supplied value
                let _ = v.parse::<usize>().map_err(|e| BallistaError::General(format!("Failed to parse user-supplied value '{}' for configuration setting '{}': {:?}", name, v, e)))?;
            } else if let Some(v) = entry.default_value.clone() {
                let _ = v.parse::<usize>().map_err(|e| BallistaError::General(format!("Failed to parse default value '{}' for configuration setting '{}': {:?}", name, v, e)))?;
            } else {
                return Err(BallistaError::General(format!(
                    "No value specified for mandatory configuration setting '{}'",
                    name
                )));
            }
        }

        Ok(Self { settings })
    }

    /// All available configuration options
    pub fn valid_entries() -> HashMap<String, ConfigEntry> {
        let entries = vec![
            ConfigEntry::new(BALLISTA_DEFAULT_SHUFFLE_PARTITIONS.to_string(),
                "Sets the default number of partitions to create when repartitioning query stages".to_string(),
                DataType::UInt16, Some("2".to_string())),
        ];
        entries
            .iter()
            .map(|e| (e.name.clone(), e.clone()))
            .collect::<HashMap<_, _>>()
    }

    pub fn settings(&self) -> &HashMap<String, String> {
        &self.settings
    }

    pub fn default_shuffle_partitions(&self) -> usize {
        self.get_usize_setting(BALLISTA_DEFAULT_SHUFFLE_PARTITIONS)
    }

    fn get_usize_setting(&self, key: &str) -> usize {
        if let Some(v) = self.settings.get(key) {
            // infallible because we validate all configs in the constructor
            v.parse().unwrap()
        } else {
            let entries = Self::valid_entries();
            // infallible because we validate all configs in the constructor
            let v = entries.get(key).unwrap().default_value.as_ref().unwrap();
            v.parse().unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() -> Result<()> {
        let config = BallistaConfig::new()?;
        assert_eq!(2, config.default_shuffle_partitions());
        Ok(())
    }

    #[test]
    fn custom_config() -> Result<()> {
        let config = BallistaConfig::builder()
            .set(BALLISTA_DEFAULT_SHUFFLE_PARTITIONS, "123")
            .build()?;
        assert_eq!(123, config.default_shuffle_partitions());
        Ok(())
    }

    #[test]
    fn custom_config_invalid() -> Result<()> {
        let config = BallistaConfig::builder()
            .set(BALLISTA_DEFAULT_SHUFFLE_PARTITIONS, "true")
            .build();
        assert!(config.is_err());
        assert_eq!("General(\"Failed to parse user-supplied value 'ballista.shuffle.partitions' for configuration setting 'true': ParseIntError { kind: InvalidDigit }\")", format!("{:?}", config.unwrap_err()));
        Ok(())
    }
}
