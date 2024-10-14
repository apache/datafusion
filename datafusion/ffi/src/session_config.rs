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
    ffi::{c_char, c_uint, c_void, CStr, CString},
    ptr::null_mut,
    slice,
};

use datafusion::error::Result;
use datafusion::{error::DataFusionError, prelude::SessionConfig};

#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_SessionConfig {
    pub config_options: Option<
        unsafe extern "C" fn(
            config: *const FFI_SessionConfig,
            num_options: &mut c_uint,
            keys: &mut *const *const c_char,
            values: &mut *const *const c_char,
        ) -> (),
    >,

    pub private_data: *mut c_void,
    pub release: Option<unsafe extern "C" fn(arg: *mut Self)>,
}

unsafe impl Send for FFI_SessionConfig {}

unsafe extern "C" fn config_options_fn_wrapper(
    config: *const FFI_SessionConfig,
    num_options: &mut c_uint,
    keys: &mut *const *const c_char,
    values: &mut *const *const c_char,
) {
    let private_data = (*config).private_data as *mut SessionConfigPrivateData;

    *num_options = (*private_data).config_keys.len() as c_uint;
    *keys = (*private_data).config_keys.as_ptr();
    *values = (*private_data).config_values.as_ptr();
}

unsafe extern "C" fn release_fn_wrapper(config: *mut FFI_SessionConfig) {
    if config.is_null() {
        return;
    }
    let config = &mut *config;

    let mut private_data =
        Box::from_raw(config.private_data as *mut SessionConfigPrivateData);
    let _removed_keys: Vec<_> = private_data
        .config_keys
        .drain(..)
        .map(|key| CString::from_raw(key as *mut c_char))
        .collect();
    let _removed_values: Vec<_> = private_data
        .config_values
        .drain(..)
        .map(|key| CString::from_raw(key as *mut c_char))
        .collect();

    drop(private_data);

    config.release = None;
}

struct SessionConfigPrivateData {
    pub config_keys: Vec<*const c_char>,
    pub config_values: Vec<*const c_char>,
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
            config_keys,
            config_values,
        });

        Self {
            config_options: Some(config_options_fn_wrapper),
            private_data: Box::into_raw(private_data) as *mut c_void,
            release: Some(release_fn_wrapper),
        }
    }
}

impl Drop for FFI_SessionConfig {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        };
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
    pub unsafe fn new(config: *const FFI_SessionConfig) -> Result<Self> {
        let (keys, values) = unsafe {
            let config_options =
                (*config)
                    .config_options
                    .ok_or(DataFusionError::NotImplemented(
                        "config_options not implemented on FFI_SessionConfig".to_string(),
                    ))?;
            let mut num_keys = 0;
            let mut keys: *const *const c_char = null_mut();
            let mut values: *const *const c_char = null_mut();
            config_options(config, &mut num_keys, &mut keys, &mut values);
            let num_keys = num_keys as usize;
            println!("Received {} key value pairs", num_keys);

            let keys: Vec<String> = slice::from_raw_parts(keys, num_keys)
                .iter()
                .map(|key| CStr::from_ptr(*key))
                .map(|key| key.to_str().unwrap_or_default().to_string())
                .collect();
            let values: Vec<String> = slice::from_raw_parts(values, num_keys)
                .iter()
                .map(|value| CStr::from_ptr(*value))
                .map(|val| val.to_str().unwrap_or_default().to_string())
                .collect();

            (keys, values)
        };

        let mut options_map = HashMap::new();
        keys.into_iter().zip(values).for_each(|(key, value)| {
            options_map.insert(key, value);
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
        let ffi_config_ptr = Box::into_raw(Box::new(ffi_config));

        let foreign_config = unsafe { ForeignSessionConfig::new(ffi_config_ptr)? };

        let returned_options = foreign_config.0.options().entries();

        println!(
            "Length of original options: {} returned {}",
            original_options.len(),
            returned_options.len()
        );

        let _ = unsafe { Box::from_raw(ffi_config_ptr) };

        Ok(())
    }
}
