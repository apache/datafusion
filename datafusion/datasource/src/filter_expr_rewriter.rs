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

//! Filter expression rewriter that rewrites expressions based on table and file schemas

use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_physical_plan::PhysicalExpr;

/// Factory for creating FilterExpressionRewriter instances
pub trait FilterExpressionRewriterFactory: Debug + Send + Sync {
    /// Create a new FilterExpressionRewriter instance
    fn create(
        &self,
        table_schema: SchemaRef,
        file_schema: SchemaRef,
    ) -> Box<dyn FilterExpressionRewriter>;
}

/// Options for filter expression rewriting
#[derive(Debug, Clone, Default)]
pub struct FilterExpressionRewriterOptions {
    // Custom options can be added here as needed
}

/// Rewrites filter expressions based on table schema and file schema
pub trait FilterExpressionRewriter: Send + Sync {
    /// Rewrite a physical filter expression to use file-specific columns
    fn rewrite_physical_expr(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn PhysicalExpr>>;
}

/// Utility for transforming expressions based on table and file schemas
#[derive(Debug)]
pub struct ExprRewriteContext {
    /// The table schema
    pub table_schema: SchemaRef,
    /// The file schema
    pub file_schema: SchemaRef,
    /// Rewriter options
    pub options: FilterExpressionRewriterOptions,
}

impl ExprRewriteContext {
    /// Create a new expression rewrite context
    pub fn new(
        table_schema: SchemaRef,
        file_schema: SchemaRef,
        options: FilterExpressionRewriterOptions,
    ) -> Self {
        Self {
            table_schema,
            file_schema,
            options,
        }
    }
}
