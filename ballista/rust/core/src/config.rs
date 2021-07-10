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

use log::warn;

pub const BALLISTA_DEFAULT_SHUFFLE_PARTITIONS: &str = "ballista.shuffle.partitions";

/// Ballista configuration
#[derive(Debug, Clone)]
pub struct BallistaConfig {
    /// Settings stored in map for easy serde
    settings: HashMap<String, String>,
}

impl BallistaConfig {
    /// Create a new configuration based on key-value pairs
    pub fn new(settings: HashMap<String, String>) -> Self {
        Self { settings }
    }

    pub fn settings(&self) -> &HashMap<String, String> {
        &self.settings
    }

    pub fn default_shuffle_partitions(&self) -> usize {
        self.get_usize_setting(BALLISTA_DEFAULT_SHUFFLE_PARTITIONS, 2)
    }

    fn get_usize_setting(&self, key: &str, default_value: usize) -> usize {
        if let Some(v) = self.settings.get(key) {
            //TODO error handling
            v.parse().unwrap()
        } else {
            default_value
        }
    }
}
