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

use datafusion_common::config::ConfigExtension;
use datafusion_common::extensions_options;

use crate::config::extension_options::FFI_ExtensionOptions;

extensions_options! {
   pub struct ExternalConfig {
       /// Should "foo" be replaced by "bar"?
       pub is_enabled: bool, default = true

       /// Some value to be extracted
       pub base_number: usize, default = 1000
   }
}

impl PartialEq for ExternalConfig {
    fn eq(&self, other: &Self) -> bool {
        self.base_number == other.base_number && self.is_enabled == other.is_enabled
    }
}
impl Eq for ExternalConfig {}

impl ConfigExtension for ExternalConfig {
    const PREFIX: &'static str = "external_config";
}

pub(crate) extern "C" fn create_extension_options() -> FFI_ExtensionOptions {
    let mut extensions = FFI_ExtensionOptions::default();
    extensions
        .add_config(&ExternalConfig::default())
        .expect("add_config should be infallible for ExternalConfig");

    extensions
}
