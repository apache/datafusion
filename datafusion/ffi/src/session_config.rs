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

use std::{
    collections::HashMap,
    ffi::{c_char, c_void, CString},
};

use abi_stable::{
    std_types::{RHashMap, RString},
    StableAbi,
};
use datafusion::prelude::SessionConfig;
use datafusion::{config::ConfigOptions, error::Result};

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_SessionConfig {
    pub config_options: unsafe extern "C" fn(config: &Self) -> RHashMap<RString, RString>,

    pub private_data: *mut c_void,
    pub clone: unsafe extern "C" fn(&Self) -> Self,
    pub release: unsafe extern "C" fn(config: &mut Self),
}

unsafe impl Send for FFI_SessionConfig {}
unsafe impl Sync for FFI_SessionConfig {}

unsafe extern "C" fn config_options_fn_wrapper(
    config: &FFI_SessionConfig,
) -> RHashMap<RString, RString> {
    let private_data = config.private_data as *mut SessionConfigPrivateData;
    let config_options = &(*private_data).config;

    let mut options = RHashMap::default();
    for config_entry in config_options.entries() {
        if let Some(value) = config_entry.value {
            options.insert(config_entry.key.into(), value.into());
        }
    }

    options
}

unsafe extern "C" fn release_fn_wrapper(config: &mut FFI_SessionConfig) {
    let private_data =
        Box::from_raw(config.private_data as *mut SessionConfigPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(config: &FFI_SessionConfig) -> FFI_SessionConfig {
    let old_private_data = config.private_data as *mut SessionConfigPrivateData;
    let old_config = &(*old_private_data).config;

    let private_data = Box::new(SessionConfigPrivateData {
        config: old_config.clone(),
    });

    FFI_SessionConfig {
        config_options: config_options_fn_wrapper,
        private_data: Box::into_raw(private_data) as *mut c_void,
        clone: clone_fn_wrapper,
        release: release_fn_wrapper,
    }
}

struct SessionConfigPrivateData {
    pub config: ConfigOptions,
}

impl FFI_SessionConfig {
    /// Creates a new [`FFI_SessionConfig`].
    pub fn new(session: &SessionConfig) -> Self {
        let mut config_keys = Vec::new();
        let mut config_values = Vec::new();
        for config_entry in session.options().entries() {
            if let Some(value) = config_entry.value {
                let key_cstr = CString::new(config_entry.key).unwrap_or_default();
                let key_ptr = key_cstr.into_raw() as *const c_char;
                config_keys.push(key_ptr);

                config_values
                    .push(CString::new(value).unwrap_or_default().into_raw()
                        as *const c_char);
            }
        }

        let private_data = Box::new(SessionConfigPrivateData {
            config: session.options().clone(),
        });

        Self {
            config_options: config_options_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
        }
    }
}

impl Clone for FFI_SessionConfig {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl Drop for FFI_SessionConfig {
    fn drop(&mut self) {
        unsafe { (self.release)(self) };
    }
}

pub struct ForeignSessionConfig(pub SessionConfig);

impl ForeignSessionConfig {
    /// Create a session config object from a foreign provider.
    ///
    /// # Safety
    ///
    /// This function will dereference the provided config pointer and will
    /// access it's unsafe methods. It is the provider's responsibility that
    /// this pointer and it's internal functions remain valid for the lifetime
    /// of the returned struct.
    pub unsafe fn new(config: &FFI_SessionConfig) -> Result<Self> {
        let config_options = (config.config_options)(config);

        let mut options_map = HashMap::new();
        config_options.iter().for_each(|kv_pair| {
            options_map.insert(kv_pair.0.to_string(), kv_pair.1.to_string());
        });

        Ok(Self(SessionConfig::from_string_hash_map(&options_map)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip_ffi_session_config() -> Result<()> {
        let session_config = SessionConfig::new();
        let original_options = session_config.options().entries();

        let ffi_config = FFI_SessionConfig::new(&session_config);

        let foreign_config = unsafe { ForeignSessionConfig::new(&ffi_config)? };

        let returned_options = foreign_config.0.options().entries();

        assert!(original_options.len() == returned_options.len());

        Ok(())
    }
}
