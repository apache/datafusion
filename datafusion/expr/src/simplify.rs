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

//! Structs to provide the information needed for expression simplification.

use std::sync::Arc;

use arrow::datatypes::DataType;
use chrono::{DateTime, Utc};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DFSchema, DFSchemaRef, Result};

use crate::{Expr, ExprSchemable};

/// Provides simplification information based on schema, query execution time,
/// and configuration options.
///
/// # Example
/// See the `simplify_demo` in the [`expr_api` example]
///
/// [`expr_api` example]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/query_planning/expr_api.rs
#[derive(Debug, Clone)]
pub struct SimplifyContext {
    schema: DFSchemaRef,
    query_execution_start_time: Option<DateTime<Utc>>,
    config_options: Arc<ConfigOptions>,
}

impl Default for SimplifyContext {
    fn default() -> Self {
        Self {
            schema: Arc::new(DFSchema::empty()),
            query_execution_start_time: None,
            config_options: Arc::new(ConfigOptions::default()),
        }
    }
}

impl SimplifyContext {
    /// Set the [`ConfigOptions`] for this context
    pub fn with_config_options(mut self, config_options: Arc<ConfigOptions>) -> Self {
        self.config_options = config_options;
        self
    }

    /// Set the schema for this context
    pub fn with_schema(mut self, schema: DFSchemaRef) -> Self {
        self.schema = schema;
        self
    }

    /// Set the query execution start time
    pub fn with_query_execution_start_time(
        mut self,
        query_execution_start_time: Option<DateTime<Utc>>,
    ) -> Self {
        self.query_execution_start_time = query_execution_start_time;
        self
    }

    /// Set the query execution start to the current time
    pub fn with_current_time(mut self) -> Self {
        self.query_execution_start_time = Some(Utc::now());
        self
    }

    /// Returns the schema
    pub fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    /// Returns true if this Expr has boolean type
    pub fn is_boolean_type(&self, expr: &Expr) -> Result<bool> {
        Ok(expr.get_type(&self.schema)? == DataType::Boolean)
    }

    /// Returns true if expr is nullable
    pub fn nullable(&self, expr: &Expr) -> Result<bool> {
        expr.nullable(self.schema.as_ref())
    }

    /// Returns data type of this expr needed for determining optimized int type of a value
    pub fn get_data_type(&self, expr: &Expr) -> Result<DataType> {
        expr.get_type(&self.schema)
    }

    /// Returns the time at which the query execution started.
    /// If `None`, time-dependent functions like `now()` will not be simplified.
    pub fn query_execution_start_time(&self) -> Option<DateTime<Utc>> {
        self.query_execution_start_time
    }

    /// Returns the configuration options for the session.
    pub fn config_options(&self) -> &Arc<ConfigOptions> {
        &self.config_options
    }
}

/// Was the expression simplified?
#[derive(Debug)]
pub enum ExprSimplifyResult {
    /// The function call was simplified to an entirely new Expr
    Simplified(Expr),
    /// The function call could not be simplified, and the arguments
    /// are return unmodified.
    Original(Vec<Expr>),
}
