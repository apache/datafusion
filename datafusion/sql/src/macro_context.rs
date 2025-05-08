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

//! Extension traits for accessing SQL macro catalog

use std::sync::Arc;

use datafusion_common::{not_impl_err, MacroCatalog, Result};
use datafusion_expr::planner::ContextProvider;

/// Extension trait for ContextProvider to provide SQL macro catalog access
pub trait MacroContextProvider: ContextProvider {
    /// Returns the macro catalog, which stores SQL macro definitions
    ///
    /// By default, this returns a "Not Implemented" error. Implementations that
    /// support SQL macros should override this method to provide the macro catalog.
    fn macro_catalog(&self) -> Result<Arc<dyn MacroCatalog>> {
        not_impl_err!("SQL macros are not supported")
    }
}
