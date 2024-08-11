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

//! Only meant for registering the `flight` namespace with `datafusion-cli`

use datafusion_common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use std::any::Any;
use std::collections::HashMap;

/// Collects and reports back config entries. Only used to persuade `datafusion-cli`
/// to accept the `flight.` prefix for `CREATE EXTERNAL TABLE` options.
#[derive(Default, Debug, Clone)]
pub struct FlightOptions {
    inner: HashMap<String, String>,
}

impl ConfigExtension for FlightOptions {
    const PREFIX: &'static str = "flight";
}

impl ExtensionOptions for FlightOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> datafusion_common::Result<()> {
        self.inner.insert(key.into(), value.into());
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        self.inner
            .iter()
            .map(|(key, value)| ConfigEntry {
                key: key.to_owned(),
                value: Some(value.to_owned()).filter(|s| !s.is_empty()),
                description: "",
            })
            .collect()
    }
}
