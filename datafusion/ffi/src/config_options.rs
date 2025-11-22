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

use std::{any::Any, ffi::c_void, ops::Deref};

use abi_stable::{std_types::{RHashMap, RResult, RStr, RString, RVec}, RTuple, StableAbi};
use abi_stable::std_types::{ROption, Tuple3};
use arrow::{array::ArrayRef, error::ArrowError};
use datafusion::{
    error::{DataFusionError, Result},
    scalar::ScalarValue,
};
use datafusion_common::config::{ConfigEntry, ExtensionOptions};
use prost::Message;

use crate::{arrow_wrappers::WrappedArray, df_result, rresult, rresult_return};

/// A stable struct for sharing [`ExtensionOptions`] across FFI boundaries.
/// For an explanation of each field, see the corresponding function
/// defined in [`ExtensionOptions`].
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_ExtensionOptions {
    pub cloned: unsafe extern "C" fn(&Self) -> FFI_ExtensionOptions,

    pub set: unsafe extern "C" fn(&mut Self, key: RStr, value: RStr) -> RResult<(), RString>,

    pub entries:
        unsafe extern "C" fn(&Self) -> RVec<Tuple3<RString, ROption<RString>, RStr>>,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(&mut Self),

    /// Internal data. This is only to be accessed by the provider of the options.
    /// A [`ForeignExtensionOptions`] should never attempt to access this data.
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_ExtensionOptions {}
unsafe impl Sync for FFI_ExtensionOptions {}

pub struct ExtensionOptionsPrivateData {
    pub options: Box<dyn ExtensionOptions>,
}

impl FFI_ExtensionOptions {
    #[inline]
    unsafe fn inner_mut(&mut self) -> &mut Box<dyn ExtensionOptions> {
        let private_data = self.private_data as *mut ExtensionOptionsPrivateData;
        &mut (*private_data).options
    }

    #[inline]
    unsafe fn inner(&self) -> &dyn ExtensionOptions {
        let private_data = self.private_data as *const ExtensionOptionsPrivateData;
        (*private_data).options.deref()
    }
}

unsafe extern "C" fn cloned_fn_wrapper(options: &FFI_ExtensionOptions) -> FFI_ExtensionOptions {
    options.inner().cloned().into()
}

unsafe extern "C" fn set_fn_wrapper(
    options: &mut FFI_ExtensionOptions,
    key: RStr,
    value: RStr,
) -> RResult<(), RString> {
    let key = key.as_str();
    let value = value.as_str();

    rresult!(options.inner_mut().set(key, value))
}

unsafe extern "C" fn entries_fn_wrapper(
    options: &FFI_ExtensionOptions,
) -> RVec<Tuple3<RString, ROption<RString>, RStr>> {
    options.inner()
        .entries()
        .into_iter()
        .map(|entry| (entry.key.into(), entry.value.map(Into::into).into(), entry.description.into()).into())
        .collect()
}

unsafe extern "C" fn release_fn_wrapper(options: &mut FFI_ExtensionOptions) {
    let private_data =
        Box::from_raw(options.private_data as *mut ExtensionOptionsPrivateData);
    drop(private_data);
}

impl From<Box<dyn ExtensionOptions>> for FFI_ExtensionOptions {
    fn from(options: Box<dyn ExtensionOptions>) -> Self {
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

/// This struct is used to access an UDF provided by a foreign
/// library across a FFI boundary.
///
/// The ForeignExtensionOptions is to be used by the caller of the UDF, so it has
/// no knowledge or access to the private data. All interaction with the UDF
/// must occur through the functions defined in FFI_ExtensionOptions.
#[derive(Debug)]
pub struct ForeignExtensionOptions(FFI_ExtensionOptions);

unsafe impl Send for ForeignExtensionOptions {}
unsafe impl Sync for ForeignExtensionOptions {}

impl From<FFI_ExtensionOptions> for ForeignExtensionOptions {
    fn from(options: FFI_ExtensionOptions) -> Self {
        Self(options)
    }
}

impl ExtensionOptions for ForeignExtensionOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        (self.0.cloned)(&self.0).into()
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        df_result!({
            (self.0.set)(&mut self.0, key.into(), value.into())
        })
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        (self.0.entries)(&self.0).into_iter()
            .map(|entry_tuple| ConfigEntry {
                key: entry_tuple.0.into(),
                value: entry_tuple.1.into(),
                description: entry_tuple.2.into(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::{
        config::ConfigExtension, config::ConfigOptions, extensions_options,
    };
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
        config.extensions.insert(MyConfig::default());

        // overwrite config default
        config.set("my_config.baz_count", "42").unwrap();

        // check config state
        let my_config = config.extensions.get::<MyConfig>().unwrap();
        assert!(my_config.foo_to_bar,);
        assert_eq!(my_config.baz_count, 42,);

    }
}
