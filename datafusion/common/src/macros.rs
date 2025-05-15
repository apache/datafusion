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

//! SQL macro catalog interface definitions for DataFusion

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::Result;

/// Trait for SQL Macro catalog functionality, which manages
/// macro definitions used in SQL statements.
///
/// This trait enables support for SQL macros in DataFusion, which allow
/// users to define reusable SQL templates with parameters that can be
/// expanded at query planning time.
pub trait MacroCatalog: Debug + Send + Sync {
    /// Returns the macro catalog as [`Any`] so that it can be downcast to a
    /// specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Check if a macro with the given name exists in the catalog.
    fn macro_exists(&self, name: &str) -> bool;

    /// Get a macro definition by name.
    ///
    /// Returns an error if the macro does not exist.
    fn get_macro(&self, name: &str) -> Result<Arc<MacroDefinition>>;

    /// Register a macro definition with the catalog.
    ///
    /// If a macro with the same name already exists and `or_replace` is true,
    /// the existing macro is replaced. Otherwise, an error is returned.
    fn register_macro(
        &self,
        name: &str,
        macro_def: Arc<MacroDefinition>,
        or_replace: bool,
    ) -> Result<()>;

    /// Remove a macro definition by name.
    ///
    /// Returns the removed macro if it existed, None otherwise.
    fn drop_macro(&self, name: &str) -> Result<Option<Arc<MacroDefinition>>>;
}

/// Definition of a SQL macro
///
/// A SQL macro is a parameterized SQL query that can be expanded at query planning time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MacroDefinition {
    /// Name of the macro
    pub name: String,

    /// List of parameters that can be substituted in the SQL body
    pub parameters: Vec<String>,

    /// SQL body of the macro that will be expanded
    pub body: String,
}
