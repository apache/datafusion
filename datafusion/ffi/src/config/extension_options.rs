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
use datafusion::error::Result;
use datafusion_common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion_common::{DataFusionError, exec_err};

use crate::df_result;

/// A stable struct for sharing [`ExtensionOptions`] across FFI boundaries.
/// For an explanation of each field, see the corresponding function
/// defined in [`ExtensionOptions`].
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_ExtensionOptions {
    pub cloned: unsafe extern "C" fn(&Self) -> FFI_ExtensionOptions,

    pub set:
        unsafe extern "C" fn(&mut Self, key: RStr, value: RStr) -> RResult<(), RString>,

    pub entries: unsafe extern "C" fn(&Self) -> RVec<Tuple2<RString, RString>>,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(&mut Self),

    /// Internal data. This is only to be accessed by the provider of the options.
    /// A [`ForeignExtensionOptions`] should never attempt to access this data.
    pub private_data: *mut c_void,
}

// TODO(tsaucer) We have a problem in datafusion_common::config::Extension::get
// which relies on knowing the concrete types of the extensions so that we can
// use their PREFIX for insertion of configs. We cannot work around this using
// things like `fn namespace() -> &'static str` because we must be able to do
// this without having an instance. Instead we will go to an approach of having
// a concrete FFI_ForeignConfigExtension and add a check into all of the methods
// in the above `get` (and similar) methods to check to see if we have an FFI
// configs. If so we get the concrete FFI config and then have a method that will
// convert from FFI_ForeignExtensionConfig into the concrete type. Somehow our
// FFI library will need to make this as easy an experience as they are used to
// so maybe we need to implement something at the `Extensions` level in addition
// to the ConfigExtension.

unsafe impl Send for FFI_ExtensionOptions {}
unsafe impl Sync for FFI_ExtensionOptions {}

pub struct ExtensionOptionsPrivateData {
    pub options: HashMap<String, String>,
}

impl FFI_ExtensionOptions {
    #[inline]
    unsafe fn inner_mut(&mut self) -> &mut HashMap<String, String> {
        let private_data = self.private_data as *mut ExtensionOptionsPrivateData;
        &mut (*private_data).options
    }

    #[inline]
    unsafe fn inner(&self) -> &HashMap<String, String> {
        let private_data = self.private_data as *const ExtensionOptionsPrivateData;
        &(*private_data).options
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
    let private_data =
        Box::from_raw(options.private_data as *mut ExtensionOptionsPrivateData);
    drop(private_data);
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

// impl<T : ConfigExtension + Default> TryFrom<FFI_ExtensionOptions> for T {
//     type Error = DataFusionError;
//
//     fn try_from(options: &FFI_ExtensionOptions) -> Result<Self, Self::Error> {
//         let mut config = T::default();
//
//         let mut found = false;
//         unsafe {
//             for entry_tuple in (options.entries)(&options).into_iter() {
//                 if let ROption::RSome(value) = entry_tuple.1 {
//                     if let Some((namespace, key)) = entry_tuple.0.as_str().split_once('.')
//                     {
//                         if namespace == T::PREFIX {
//                             found = true;
//                             config.set(key, value.as_str())?;
//                         }
//                     }
//                 }
//             }
//         }
//
//         Ok(config)
//     }
// }

impl ForeignExtensionOptions {
    pub fn add_config<C: ConfigExtension>(&mut self, config: &C) -> Result<()> {
        for entry in config.entries() {
            if let Some(value) = entry.value {
                let key = format!("{}.{}", C::PREFIX, entry.key);
                self.set(key.as_str(), value.as_str())?;
            }
        }

        Ok(())
    }
}

impl ConfigExtension for ForeignExtensionOptions {
    const PREFIX: &'static str = "datafusion_ffi";
}

impl ExtensionOptions for ForeignExtensionOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        let ffi_options = unsafe { (self.0.cloned)(&self.0) };
        let foreign_options = ForeignExtensionOptions(ffi_options);
        Box::new(foreign_options)
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        println!("Setting {key} = {value}");
        if key.split_once('.').is_none() {
            return exec_err!("Unable to set FFI config value without namespace set");
        };

        df_result!(unsafe { (self.0.set)(&mut self.0, key.into(), value.into()) })
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        unsafe {
            (self.0.entries)(&self.0)
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

// TODO: Maybe get rid of ForeignExtensionOptions?
impl<C: ConfigExtension + Default> TryFrom<&ForeignExtensionOptions> for C {
    type Error = DataFusionError;
    fn try_from(options: &ForeignExtensionOptions) -> Result<Self> {
        let mut result = C::default();
        for entry in options.entries() {
            if let Some(value) = entry.value {
                result.set(entry.key.as_str(), value.as_str())?;
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::config::{ConfigExtension, ConfigOptions};
    use datafusion_common::extensions_options;

    use crate::config::extension_options::{
        FFI_ExtensionOptions, ForeignExtensionOptions,
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
        let mut foreign_options =
            ForeignExtensionOptions(FFI_ExtensionOptions::default());
        foreign_options.add_config(&MyConfig::default()).unwrap();

        config.extensions.insert(foreign_options);
        // config.extensions.insert(MyConfig::default());

        // overwrite config default
        config.set("my_config.baz_count", "42").unwrap();

        // check config state
        let my_config = config.extensions.get::<MyConfig>().unwrap();
        assert!(my_config.foo_to_bar,);
        assert_eq!(my_config.baz_count, 42,);

        // let boxed_config = Box::new(MyConfig::default()) as Box<dyn ExtensionOptions>;
        // let mut ffi_config = FFI_ExtensionOptions::from(boxed_config);
        // ffi_config.library_marker_id = crate::mock_foreign_marker_id;
        // let foreign_config: Box<dyn ExtensionOptions> = ffi_config.into();
        //
        // config.extensions.insert(foreign_config);
    }
}
