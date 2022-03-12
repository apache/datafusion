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
use crate::error::{BallistaError, Result};
use crate::plugin::plugin_manager::global_plugin_manager;
use crate::plugin::{Plugin, PluginEnum, PluginRegistrar};
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use libloading::{Library, Symbol};
use std::any::Any;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

/// UDF plugin trait
pub trait UDFPlugin: Plugin {
    /// get a ScalarUDF by name
    fn get_scalar_udf_by_name(&self, fun_name: &str) -> Result<ScalarUDF>;

    /// return all udf names in the plugin
    fn udf_names(&self) -> Result<Vec<String>>;

    /// get a aggregate udf by name
    fn get_aggregate_udf_by_name(&self, fun_name: &str) -> Result<AggregateUDF>;

    /// return all udaf names
    fn udaf_names(&self) -> Result<Vec<String>>;
}

/// UDFPluginManager
#[derive(Default, Clone)]
pub struct UDFPluginManager {
    /// scalar udfs
    pub scalar_udfs: HashMap<String, Arc<ScalarUDF>>,

    /// aggregate udfs
    pub aggregate_udfs: HashMap<String, Arc<AggregateUDF>>,

    /// All libraries load from the plugin dir.
    pub libraries: Vec<Arc<Library>>,
}

impl PluginRegistrar for UDFPluginManager {
    unsafe fn load(&mut self, library: Arc<Library>) -> Result<()> {
        type PluginRegister = unsafe fn() -> Box<dyn UDFPlugin>;
        let register_fun: Symbol<PluginRegister> =
            library.get(b"registrar_udf_plugin\0").map_err(|e| {
                BallistaError::IoError(io::Error::new(
                    io::ErrorKind::Other,
                    format!("not found fn registrar_udf_plugin in the library: {}", e),
                ))
            })?;

        let udf_plugin: Box<dyn UDFPlugin> = register_fun();
        udf_plugin
            .udf_names()
            .unwrap()
            .iter()
            .try_for_each(|udf_name| {
                if self.scalar_udfs.contains_key(udf_name) {
                    Err(BallistaError::IoError(io::Error::new(
                        io::ErrorKind::Other,
                        format!("udf name: {} already exists", udf_name),
                    )))
                } else {
                    let scalar_udf = udf_plugin.get_scalar_udf_by_name(udf_name)?;
                    self.scalar_udfs
                        .insert(udf_name.to_string(), Arc::new(scalar_udf));
                    Ok(())
                }
            })?;

        udf_plugin
            .udaf_names()
            .unwrap()
            .iter()
            .try_for_each(|udaf_name| {
                if self.aggregate_udfs.contains_key(udaf_name) {
                    Err(BallistaError::IoError(io::Error::new(
                        io::ErrorKind::Other,
                        format!("udaf name: {} already exists", udaf_name),
                    )))
                } else {
                    let aggregate_udf =
                        udf_plugin.get_aggregate_udf_by_name(udaf_name)?;
                    self.aggregate_udfs
                        .insert(udaf_name.to_string(), Arc::new(aggregate_udf));
                    Ok(())
                }
            })?;
        self.libraries.push(library);
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Declare a udf plugin registrar callback
///
/// # Notes
///
/// This works by automatically generating an `extern "C"` function named `registrar_udf_plugin` with a
/// pre-defined signature and symbol name.
/// Therefore you will only be able to declare one plugin per library.
#[macro_export]
macro_rules! declare_udf_plugin {
    ($curr_plugin_type:ty, $constructor:path) => {
        #[no_mangle]
        pub extern "C" fn registrar_udf_plugin() -> Box<dyn $crate::plugin::udf::UDFPlugin> {
            // make sure the constructor is the correct type.
            let constructor: fn() -> $curr_plugin_type = $constructor;
            let object = constructor();
            Box::new(object)
        }

        $crate::declare_plugin!($crate::plugin::PluginEnum::UDF);
    };
}

/// get a Option of Immutable UDFPluginManager
pub fn get_udf_plugin_manager(path: &str) -> Option<UDFPluginManager> {
    let udf_plugin_manager_opt = {
        let gpm = global_plugin_manager(path).lock().unwrap();
        let plugin_registrar_opt = gpm.plugin_managers.get(&PluginEnum::UDF);
        if let Some(plugin_registrar) = plugin_registrar_opt {
            if let Some(udf_plugin_manager) =
                plugin_registrar.as_any().downcast_ref::<UDFPluginManager>()
            {
                return Some(udf_plugin_manager.clone());
            } else {
                return None;
            }
        }
        None
    };
    udf_plugin_manager_opt
}
