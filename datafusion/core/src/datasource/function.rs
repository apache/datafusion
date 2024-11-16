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

//! A table that uses a function to generate data

use super::TableProvider;

use datafusion_common::Result;
use datafusion_expr::Expr;

use std::fmt::Debug;
use std::sync::Arc;

/// A trait for table function implementations
pub trait TableFunctionImpl: Debug + Sync + Send {
    /// Create a table provider
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>>;
}

/// A table that uses a function to generate data
#[derive(Debug)]
pub struct TableFunction {
    /// Name of the table function
    name: String,
    /// Function implementation
    fun: Arc<dyn TableFunctionImpl>,
}

impl TableFunction {
    /// Create a new table function
    pub fn new(name: String, fun: Arc<dyn TableFunctionImpl>) -> Self {
        Self { name, fun }
    }

    /// Get the name of the table function
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the implementation of the table function
    pub fn function(&self) -> &Arc<dyn TableFunctionImpl> {
        &self.fun
    }

    /// Get the function implementation and generate a table
    pub fn create_table_provider(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        self.fun.call(args)
    }
}
