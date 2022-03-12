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

use crate::error::Result;
use crate::plugin::udf::UDFPluginManager;
use libloading::Library;
use std::any::Any;
use std::env;
use std::sync::Arc;

/// plugin manager
pub mod plugin_manager;
/// udf plugin
pub mod udf;

/// CARGO_PKG_VERSION
pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
/// RUSTC_VERSION
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

/// Top plugin trait
pub trait Plugin {
    /// Returns the plugin as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
}

/// The enum of Plugin
#[derive(PartialEq, std::cmp::Eq, std::hash::Hash, Copy, Clone)]
pub enum PluginEnum {
    /// UDF/UDAF plugin
    UDF,
}

impl PluginEnum {
    /// new a struct which impl the PluginRegistrar trait
    pub fn init_plugin_manager(&self) -> Box<dyn PluginRegistrar> {
        match self {
            PluginEnum::UDF => Box::new(UDFPluginManager::default()),
        }
    }
}

/// Every plugin need a PluginDeclaration
#[derive(Copy, Clone)]
pub struct PluginDeclaration {
    /// Rust doesn’t have a stable ABI, meaning different compiler versions can generate incompatible code.
    /// For these reasons, the UDF plug-in must be compiled using the same version of rustc as datafusion.
    pub rustc_version: &'static str,

    /// core version of the plugin. The plugin's core_version need same as plugin manager.
    pub core_version: &'static str,

    /// One of PluginEnum
    pub plugin_type: unsafe extern "C" fn() -> PluginEnum,
}

/// Plugin Registrar , Every plugin need implement this trait
pub trait PluginRegistrar: Send + Sync + 'static {
    /// # Safety
    /// load plugin from library
    unsafe fn load(&mut self, library: Arc<Library>) -> Result<()>;

    /// Returns the plugin as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
}

/// Declare a plugin's PluginDeclaration.
///
/// # Notes
///
/// This works by automatically generating an `extern "C"` function named `get_plugin_type` with a
/// pre-defined signature and symbol name. And then generating a PluginDeclaration.
/// Therefore you will only be able to declare one plugin per library.
#[macro_export]
macro_rules! declare_plugin {
    ($plugin_type:expr) => {
        #[no_mangle]
        pub extern "C" fn get_plugin_type() -> $crate::plugin::PluginEnum {
            $plugin_type
        }

        #[no_mangle]
        pub static plugin_declaration: $crate::plugin::PluginDeclaration =
            $crate::plugin::PluginDeclaration {
                rustc_version: $crate::plugin::RUSTC_VERSION,
                core_version: $crate::plugin::CORE_VERSION,
                plugin_type: get_plugin_type,
            };
    };
}

/// get the plugin dir
pub fn plugin_dir() -> String {
    let current_exe_dir = match env::current_exe() {
        Ok(exe_path) => exe_path.display().to_string(),
        Err(_e) => "".to_string(),
    };

    // If current_exe_dir contain `deps` the root dir is the parent dir
    // eg: /Users/xxx/workspace/rust/rust_plugin_sty/target/debug/deps/plugins_app-067452b3ff2af70e
    // the plugin dir is /Users/xxx/workspace/rust/rust_plugin_sty/target/debug/deps
    // else eg: /Users/xxx/workspace/rust/rust_plugin_sty/target/debug/plugins_app
    // the plugin dir is /Users/xxx/workspace/rust/rust_plugin_sty/target/debug/
    if current_exe_dir.contains("/deps/") {
        let i = current_exe_dir.find("/deps/").unwrap();
        String::from(&current_exe_dir.as_str()[..i + 6])
    } else {
        let i = current_exe_dir.rfind('/').unwrap();
        String::from(&current_exe_dir.as_str()[..i])
    }
}
