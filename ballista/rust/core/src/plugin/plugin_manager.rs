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
use libloading::Library;
use log::info;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use walkdir::{DirEntry, WalkDir};

use crate::plugin::{
    PluginDeclaration, PluginEnum, PluginRegistrar, CORE_VERSION, RUSTC_VERSION,
};
use once_cell::sync::OnceCell;

/// To prevent the library from being loaded multiple times, we use once_cell defines a Arc<Mutex<GlobalPluginManager>>
/// Because datafusion is a library, not a service, users may not need to load all plug-ins in the process.
/// So fn global_plugin_manager return Arc<Mutex<GlobalPluginManager>>. In this way, users can load the required library through the load method of GlobalPluginManager when needed
static INSTANCE: OnceCell<Arc<Mutex<GlobalPluginManager>>> = OnceCell::new();

/// global_plugin_manager
pub fn global_plugin_manager(
    plugin_path: &str,
) -> &'static Arc<Mutex<GlobalPluginManager>> {
    INSTANCE.get_or_init(move || unsafe {
        let mut gpm = GlobalPluginManager::default();
        gpm.load(plugin_path).unwrap();
        Arc::new(Mutex::new(gpm))
    })
}

#[derive(Default)]
/// manager all plugin_type's plugin_manager
pub struct GlobalPluginManager {
    /// every plugin need a plugin registrar
    pub plugin_managers: HashMap<PluginEnum, Box<dyn PluginRegistrar>>,

    /// loaded plugin files
    pub plugin_files: Vec<String>,
}

impl GlobalPluginManager {
    /// # Safety
    /// find plugin file from `plugin_path` and load it .
    unsafe fn load(&mut self, plugin_path: &str) -> Result<()> {
        if "".eq(plugin_path) {
            return Ok(());
        }
        // find library file from udaf_plugin_path
        info!("load plugin from dir:{}", plugin_path);

        let plugin_files = self.get_all_plugin_files(plugin_path)?;

        for plugin_file in plugin_files {
            let library = Library::new(plugin_file.path()).map_err(|e| {
                BallistaError::IoError(io::Error::new(
                    io::ErrorKind::Other,
                    format!("load library error: {}", e),
                ))
            })?;

            let library = Arc::new(library);

            let dec = library.get::<*mut PluginDeclaration>(b"plugin_declaration\0");
            if dec.is_err() {
                info!(
                    "not found plugin_declaration in the library: {}",
                    plugin_file.path().to_str().unwrap()
                );
                continue;
            }

            let dec = dec.unwrap().read();

            // ersion checks to prevent accidental ABI incompatibilities
            if dec.rustc_version != RUSTC_VERSION || dec.core_version != CORE_VERSION {
                return Err(BallistaError::IoError(io::Error::new(
                    io::ErrorKind::Other,
                    "Version mismatch",
                )));
            }

            let plugin_enum = (dec.plugin_type)();
            let curr_plugin_manager = match self.plugin_managers.get_mut(&plugin_enum) {
                None => {
                    let plugin_manager = plugin_enum.init_plugin_manager();
                    self.plugin_managers.insert(plugin_enum, plugin_manager);
                    self.plugin_managers.get_mut(&plugin_enum).unwrap()
                }
                Some(manager) => manager,
            };
            curr_plugin_manager.load(library)?;
            self.plugin_files
                .push(plugin_file.path().to_str().unwrap().to_string());
        }

        Ok(())
    }

    /// get all plugin file in the dir
    fn get_all_plugin_files(&self, plugin_path: &str) -> io::Result<Vec<DirEntry>> {
        let mut plugin_files = Vec::new();
        for entry in WalkDir::new(plugin_path).into_iter().filter_map(|e| {
            let item = e.unwrap();
            // every file only load once
            if self
                .plugin_files
                .contains(&item.path().to_str().unwrap().to_string())
            {
                return None;
            }

            let file_type = item.file_type();
            if !file_type.is_file() {
                return None;
            }

            if let Some(path) = item.path().extension() {
                if let Some(suffix) = path.to_str() {
                    if suffix == "dylib" || suffix == "so" || suffix == "dll" {
                        info!(
                            "load plugin from library file:{}",
                            item.path().to_str().unwrap()
                        );
                        return Some(item);
                    }
                }
            }

            None
        }) {
            plugin_files.push(entry);
        }
        Ok(plugin_files)
    }
}
