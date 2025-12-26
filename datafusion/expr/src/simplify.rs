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

//! Structs and traits to provide the information needed for expression simplification.

use std::sync::Arc;

use arrow::datatypes::DataType;
use chrono::{DateTime, Utc};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DFSchemaRef, Result, internal_datafusion_err};

use crate::{Expr, ExprSchemable, execution_props::ExecutionProps};

/// Provides the information necessary to apply algebraic simplification to an
/// [Expr]. See [SimplifyContext] for one concrete implementation.
///
/// This trait exists so that other systems can plug schema
/// information in without having to create `DFSchema` objects. If you
/// have a [`DFSchemaRef`] you can use [`SimplifyContext`]
pub trait SimplifyInfo {
    /// Returns true if this Expr has boolean type
    fn is_boolean_type(&self, expr: &Expr) -> Result<bool>;

    /// Returns true of this expr is nullable (could possibly be NULL)
    fn nullable(&self, expr: &Expr) -> Result<bool>;

    /// Returns data type of this expr needed for determining optimized int type of a value
    fn get_data_type(&self, expr: &Expr) -> Result<DataType>;

    /// Returns the time at which the query execution started.
    /// If `None`, time-dependent functions like `now()` will not be simplified.
    fn query_execution_start_time(&self) -> Option<DateTime<Utc>>;

    /// Returns the configuration options for the session.
    fn config_options(&self) -> Option<&Arc<ConfigOptions>>;
}

/// Provides simplification information based on schema, query execution time,
/// and configuration options.
///
/// # Example
/// See the `simplify_demo` in the [`expr_api` example]
///
/// [`expr_api` example]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/query_planning/expr_api.rs
#[derive(Debug, Clone)]
pub struct SimplifyContext {
    schema: Option<DFSchemaRef>,
    query_execution_start_time: Option<DateTime<Utc>>,
    config_options: Option<Arc<ConfigOptions>>,
}

impl SimplifyContext {
    /// Create a new SimplifyContext from [`ExecutionProps`].
    ///
    /// This constructor extracts `query_execution_start_time` and `config_options`
    /// from the provided `ExecutionProps`.
    pub fn new(props: &ExecutionProps) -> Self {
        Self {
            schema: None,
            query_execution_start_time: props.query_execution_start_time,
            config_options: props.config_options.clone(),
        }
    }

    /// Create a new empty SimplifyContext.
    ///
    /// This is useful when you don't need time-dependent simplification
    /// (like `now()`) or config-based simplification.
    pub fn new_empty() -> Self {
        Self {
            schema: None,
            query_execution_start_time: None,
            config_options: None,
        }
    }

    /// Set the query execution start time
    pub fn with_query_execution_start_time(
        mut self,
        query_execution_start_time: Option<DateTime<Utc>>,
    ) -> Self {
        self.query_execution_start_time = query_execution_start_time;
        self
    }

    /// Set the configuration options
    pub fn with_config_options(mut self, config_options: Arc<ConfigOptions>) -> Self {
        self.config_options = Some(config_options);
        self
    }

    /// Register a [`DFSchemaRef`] with this context
    pub fn with_schema(mut self, schema: DFSchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }
}

impl Default for SimplifyContext {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl SimplifyInfo for SimplifyContext {
    /// Returns true if this Expr has boolean type
    fn is_boolean_type(&self, expr: &Expr) -> Result<bool> {
        if let Some(schema) = &self.schema
            && let Ok(DataType::Boolean) = expr.get_type(schema)
        {
            return Ok(true);
        }

        Ok(false)
    }

    /// Returns true if expr is nullable
    fn nullable(&self, expr: &Expr) -> Result<bool> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            internal_datafusion_err!("attempt to get nullability without schema")
        })?;
        expr.nullable(schema.as_ref())
    }

    /// Returns data type of this expr needed for determining optimized int type of a value
    fn get_data_type(&self, expr: &Expr) -> Result<DataType> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            internal_datafusion_err!("attempt to get data type without schema")
        })?;
        expr.get_type(schema)
    }

    fn query_execution_start_time(&self) -> Option<DateTime<Utc>> {
        self.query_execution_start_time
    }

    fn config_options(&self) -> Option<&Arc<ConfigOptions>> {
        self.config_options.as_ref()
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
