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

pub mod extension_options;

use abi_stable::StableAbi;
use abi_stable::std_types::{RHashMap, RString};
use datafusion_common::config::{ConfigOptions, ExtensionOptions};
use datafusion_common::{DataFusionError, Result};

use crate::config::extension_options::FFI_ExtensionOptions;

// TODO(tsaucer) add text about how extension options will require user to convert to concrete type.
#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
pub struct FFI_ConfigOptions {
    base_options: RHashMap<RString, RString>,

    extensions: FFI_ExtensionOptions,
}

impl From<&ConfigOptions> for FFI_ConfigOptions {
    fn from(options: &ConfigOptions) -> Self {
        let base_options: RHashMap<RString, RString> = options
            .entries()
            .into_iter()
            .filter_map(|entry| entry.value.map(|value| (entry.key, value)))
            .map(|(key, value)| (key.into(), value.into()))
            .collect();

        let mut extensions = FFI_ExtensionOptions::default();
        for (extension_name, extension) in options.extensions.iter() {
            for entry in extension.entries().iter() {
                if let Some(value) = entry.value.as_ref() {
                    extensions
                        .set(format!("{extension_name}.{}", entry.key).as_str(), value)
                        .expect("FFI_ExtensionOptions set should always return Ok");
                }
            }
        }

        Self {
            base_options,
            extensions,
        }
    }
}

impl TryFrom<FFI_ConfigOptions> for ConfigOptions {
    type Error = DataFusionError;
    fn try_from(ffi_options: FFI_ConfigOptions) -> Result<Self, Self::Error> {
        let mut options = ConfigOptions::default();
        options.extensions.insert(ffi_options.extensions);

        for kv_tuple in ffi_options.base_options.iter() {
            options.set(kv_tuple.0.as_str(), kv_tuple.1.as_str())?;
        }

        Ok(options)
    }
}
