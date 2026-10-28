// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
