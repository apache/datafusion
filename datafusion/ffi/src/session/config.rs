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

use std::collections::HashMap;
use std::ffi::c_void;

use abi_stable::StableAbi;
use abi_stable::std_types::{RHashMap, RString};
use datafusion_common::error::{DataFusionError, Result};
use datafusion_execution::config::SessionConfig;

/// A stable struct for sharing [`SessionConfig`] across FFI boundaries.
/// Instead of attempting to expose the entire SessionConfig interface, we
/// convert the config options into a map from a string to string and pass
/// those values across the FFI boundary. On the receiver side, we
/// reconstruct a SessionConfig from those values.
///
/// It is possible that using different versions of DataFusion across the
/// FFI boundary could have differing expectations of the config options.
/// This is a limitation of this approach, but exposing the entire
/// SessionConfig via a FFI interface would be extensive and provide limited
/// value over this version.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_SessionConfig {
    /// Return a hash map from key to value of the config options represented
    /// by string values.
    pub config_options: unsafe extern "C" fn(config: &Self) -> RHashMap<RString, RString>,

    /// Used to create a clone on the provider of the execution plan. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the plan.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_SessionConfig {}
unsafe impl Sync for FFI_SessionConfig {}

impl FFI_SessionConfig {
    fn inner(&self) -> &SessionConfig {
        let private_data = self.private_data as *mut SessionConfigPrivateData;
        unsafe { &(*private_data).config }
    }
}

unsafe extern "C" fn config_options_fn_wrapper(
    config: &FFI_SessionConfig,
) -> RHashMap<RString, RString> {
    let config_options = config.inner().options();

    let mut options = RHashMap::default();
    for config_entry in config_options.entries() {
        if let Some(value) = config_entry.value {
            options.insert(config_entry.key.into(), value.into());
        }
    }

    options
}

unsafe extern "C" fn release_fn_wrapper(config: &mut FFI_SessionConfig) {
    unsafe {
        debug_assert!(!config.private_data.is_null());
        let private_data =
            Box::from_raw(config.private_data as *mut SessionConfigPrivateData);
        drop(private_data);
        config.private_data = std::ptr::null_mut();
    }
}

unsafe extern "C" fn clone_fn_wrapper(config: &FFI_SessionConfig) -> FFI_SessionConfig {
    unsafe {
        let old_private_data = config.private_data as *mut SessionConfigPrivateData;
        let old_config = (*old_private_data).config.clone();

        let private_data = Box::new(SessionConfigPrivateData { config: old_config });

        FFI_SessionConfig {
            config_options: config_options_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

struct SessionConfigPrivateData {
    pub config: SessionConfig,
}

impl From<&SessionConfig> for FFI_SessionConfig {
    fn from(session: &SessionConfig) -> Self {
        let private_data = Box::new(SessionConfigPrivateData {
            config: session.clone(),
        });

        Self {
            config_options: config_options_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            library_marker_id: crate::get_library_marker_id,
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

impl TryFrom<&FFI_SessionConfig> for SessionConfig {
    type Error = DataFusionError;

    fn try_from(config: &FFI_SessionConfig) -> Result<Self, Self::Error> {
        if (config.library_marker_id)() == crate::get_library_marker_id() {
            return Ok(config.inner().clone());
        }

        let config_options = unsafe { (config.config_options)(config) };

        let mut options_map = HashMap::new();
        config_options.iter().for_each(|kv_pair| {
            options_map.insert(kv_pair.0.to_string(), kv_pair.1.to_string());
        });

        SessionConfig::from_string_hash_map(&options_map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip_ffi_session_config() -> Result<()> {
        let session_config = SessionConfig::new();
        let original_options = session_config.options().entries();

        let mut ffi_config: FFI_SessionConfig = (&session_config).into();
        let _ = ffi_config.clone();
        ffi_config.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_config: SessionConfig = (&ffi_config).try_into()?;

        let returned_options = foreign_config.options().entries();

        assert_eq!(original_options.len(), returned_options.len());

        Ok(())
    }
}
