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

use std::any::Any;
use std::collections::HashMap;
use std::ffi::c_void;

use abi_stable::StableAbi;
use abi_stable::std_types::{RResult, RStr, RString, RVec, Tuple2};
use datafusion_common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion_common::{Result, exec_err};

use crate::df_result;

/// A stable struct for sharing [`ExtensionOptions`] across FFI boundaries.
///
/// Unlike other FFI structs in this crate, we do not construct a foreign
/// variant of this object. This is due to the typical method for interacting
/// with extension options is by creating a local struct of your concrete type.
/// To support this methodology use the `to_extension` method instead.
///
/// When using [`FFI_ExtensionOptions`] with multiple extensions, all extension
/// values are stored on a single [`FFI_ExtensionOptions`] object. The keys
/// are stored with the full path prefix to avoid overwriting values when using
/// multiple extensions.
#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_ExtensionOptions {
    /// Return a deep clone of this [`ExtensionOptions`]
    pub cloned: unsafe extern "C" fn(&Self) -> FFI_ExtensionOptions,

    /// Set the given `key`, `value` pair
    pub set:
        unsafe extern "C" fn(&mut Self, key: RStr, value: RStr) -> RResult<(), RString>,

    /// Returns the [`ConfigEntry`] stored in this [`ExtensionOptions`]
    pub entries: unsafe extern "C" fn(&Self) -> RVec<Tuple2<RString, RString>>,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(&mut Self),

    /// Internal data. This is only to be accessed by the provider of the options.
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_ExtensionOptions {}
unsafe impl Sync for FFI_ExtensionOptions {}

pub struct ExtensionOptionsPrivateData {
    pub options: HashMap<String, String>,
}

impl FFI_ExtensionOptions {
    #[inline]
    fn inner_mut(&mut self) -> &mut HashMap<String, String> {
        let private_data = self.private_data as *mut ExtensionOptionsPrivateData;
        unsafe { &mut (*private_data).options }
    }

    #[inline]
    fn inner(&self) -> &HashMap<String, String> {
        let private_data = self.private_data as *const ExtensionOptionsPrivateData;
        unsafe { &(*private_data).options }
    }
}

unsafe extern "C" fn cloned_fn_wrapper(
    options: &FFI_ExtensionOptions,
) -> FFI_ExtensionOptions {
    options
        .inner()
        .iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect::<HashMap<String, String>>()
        .into()
}

unsafe extern "C" fn set_fn_wrapper(
    options: &mut FFI_ExtensionOptions,
    key: RStr,
    value: RStr,
) -> RResult<(), RString> {
    let _ = options.inner_mut().insert(key.into(), value.into());
    RResult::ROk(())
}

unsafe extern "C" fn entries_fn_wrapper(
    options: &FFI_ExtensionOptions,
) -> RVec<Tuple2<RString, RString>> {
    options
        .inner()
        .iter()
        .map(|(key, value)| (key.to_owned().into(), value.to_owned().into()).into())
        .collect()
}

unsafe extern "C" fn release_fn_wrapper(options: &mut FFI_ExtensionOptions) {
    unsafe {
        debug_assert!(!options.private_data.is_null());
        let private_data =
            Box::from_raw(options.private_data as *mut ExtensionOptionsPrivateData);
        drop(private_data);
        options.private_data = std::ptr::null_mut();
    }
}

impl Default for FFI_ExtensionOptions {
    fn default() -> Self {
        HashMap::new().into()
    }
}

impl From<HashMap<String, String>> for FFI_ExtensionOptions {
    fn from(options: HashMap<String, String>) -> Self {
        let private_data = ExtensionOptionsPrivateData { options };

        Self {
            cloned: cloned_fn_wrapper,
            set: set_fn_wrapper,
            entries: entries_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(Box::new(private_data)) as *mut c_void,
        }
    }
}

impl Drop for FFI_ExtensionOptions {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl Clone for FFI_ExtensionOptions {
    fn clone(&self) -> Self {
        unsafe { (self.cloned)(self) }
    }
}

impl ConfigExtension for FFI_ExtensionOptions {
    const PREFIX: &'static str =
        datafusion_common::config::DATAFUSION_FFI_CONFIG_NAMESPACE;
}

impl ExtensionOptions for FFI_ExtensionOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        let ffi_options = unsafe { (self.cloned)(self) };
        Box::new(ffi_options)
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        if key.split_once('.').is_none() {
            return exec_err!("Unable to set FFI config value without namespace set");
        };

        df_result!(unsafe { (self.set)(self, key.into(), value.into()) })
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        unsafe {
            (self.entries)(self)
                .into_iter()
                .map(|entry_tuple| ConfigEntry {
                    key: entry_tuple.0.into(),
                    value: Some(entry_tuple.1.into()),
                    description: "ffi_config_options",
                })
                .collect()
        }
    }
}

impl FFI_ExtensionOptions {
    /// Add all of the values in a concrete configuration extension to the
    /// FFI variant. This is safe to call on either side of the FFI
    /// boundary.
    pub fn add_config<C: ConfigExtension>(&mut self, config: &C) -> Result<()> {
        for entry in config.entries() {
            if let Some(value) = entry.value {
                let key = format!("{}.{}", C::PREFIX, entry.key);
                self.set(key.as_str(), value.as_str())?;
            }
        }

        Ok(())
    }

    /// Merge another `FFI_ExtensionOptions` configurations into this one.
    /// This is safe to call on either side of the FFI boundary.
    pub fn merge(&mut self, other: &FFI_ExtensionOptions) -> Result<()> {
        for entry in other.entries() {
            if let Some(value) = entry.value {
                self.set(entry.key.as_str(), value.as_str())?;
            }
        }
        Ok(())
    }

    /// Create a concrete extension type from the FFI variant.
    /// This is safe to call on either side of the FFI boundary.
    pub fn to_extension<C: ConfigExtension + Default>(&self) -> Result<C> {
        let mut result = C::default();

        unsafe {
            for entry in (self.entries)(self) {
                let key = entry.0.as_str();
                let value = entry.1.as_str();

                if let Some((prefix, inner_key)) = key.split_once('.')
                    && prefix == C::PREFIX
                {
                    result.set(inner_key, value)?;
                }
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::config::{ConfigExtension, ConfigOptions};
    use datafusion_common::extensions_options;

    use crate::config::extension_options::FFI_ExtensionOptions;

    // Define a new configuration struct using the `extensions_options` macro
    extensions_options! {
       /// My own config options.
       pub struct MyConfig {
           /// Should "foo" be replaced by "bar"?
           pub foo_to_bar: bool, default = true

           /// How many "baz" should be created?
           pub baz_count: usize, default = 1337
       }
    }

    impl ConfigExtension for MyConfig {
        const PREFIX: &'static str = "my_config";
    }

    #[test]
    fn round_trip_ffi_extension_options() {
        // set up config struct and register extension
        let mut config = ConfigOptions::default();
        let mut ffi_options = FFI_ExtensionOptions::default();
        ffi_options.add_config(&MyConfig::default()).unwrap();

        config.extensions.insert(ffi_options);

        // overwrite config default
        config.set("my_config.baz_count", "42").unwrap();

        // check config state
        let returned_ffi_config =
            config.extensions.get::<FFI_ExtensionOptions>().unwrap();
        let my_config: MyConfig = returned_ffi_config.to_extension().unwrap();

        // check default value
        assert!(my_config.foo_to_bar);

        // check overwritten value
        assert_eq!(my_config.baz_count, 42);
    }
}
