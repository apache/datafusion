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
use std::sync::{Arc, RwLock};

use datafusion_common::{exec_err, MacroCatalog, MacroDefinition, Result};

/// Simple in-memory implementation of a macro catalog.
///
/// This catalog stores macro definitions in memory with no persistence.
/// Macro definitions are stored in a map with the macro name as key.
#[derive(Debug, Default)]
pub struct MemoryMacroCatalog {
    macros: RwLock<HashMap<String, Arc<MacroDefinition>>>,
}

impl MemoryMacroCatalog {
    pub fn new() -> Self {
        Self {
            macros: RwLock::new(HashMap::new()),
        }
    }
}

impl MacroCatalog for MemoryMacroCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn macro_exists(&self, name: &str) -> bool {
        self.macros.read().unwrap().contains_key(name)
    }

    fn register_macro(
        &self,
        name: &str,
        macro_def: Arc<MacroDefinition>,
        or_replace: bool,
    ) -> Result<()> {
        let mut macros = self.macros.write().unwrap();

        if !or_replace && macros.contains_key(name) {
            return exec_err!(
                "Macro {name} already exists and OR REPLACE was not specified"
            );
        }

        macros.insert(name.to_string(), macro_def);
        Ok(())
    }

    fn get_macro(&self, name: &str) -> Result<Arc<MacroDefinition>> {
        let macros = self.macros.read().unwrap();

        match macros.get(name) {
            Some(macro_def) => Ok(Arc::clone(macro_def)),
            None => exec_err!("Macro {name} does not exist"),
        }
    }

    fn drop_macro(&self, name: &str) -> Result<Option<Arc<MacroDefinition>>> {
        let mut macros = self.macros.write().unwrap();

        Ok(macros.remove(name))
    }
}
