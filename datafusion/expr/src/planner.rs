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

//! SQL query planner module

use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::{
    config::ConfigOptions, file_options::file_type::FileType, not_impl_err, DFSchema,
    Result, TableReference,
};

use crate::{AggregateUDF, Expr, GetFieldAccess, ScalarUDF, TableSource, WindowUDF};

/// The ContextProvider trait allows the query planner to obtain meta-data about tables and
/// functions referenced in SQL statements
pub trait ContextProvider {
    /// Getter for a datasource
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>>;

    fn get_file_type(&self, _ext: &str) -> Result<Arc<dyn FileType>> {
        not_impl_err!("Registered file types are not supported")
    }

    /// Getter for a table function
    fn get_table_function_source(
        &self,
        _name: &str,
        _args: Vec<Expr>,
    ) -> Result<Arc<dyn TableSource>> {
        not_impl_err!("Table Functions are not supported")
    }

    /// This provides a worktable (an intermediate table that is used to store the results of a CTE during execution)
    /// We don't directly implement this in the logical plan's ['SqlToRel`]
    /// because the sql code needs access to a table that contains execution-related types that can't be a direct dependency
    /// of the sql crate (namely, the `CteWorktable`).
    /// The [`ContextProvider`] provides a way to "hide" this dependency.
    fn create_cte_work_table(
        &self,
        _name: &str,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn TableSource>> {
        not_impl_err!("Recursive CTE is not implemented")
    }

    /// Getter for a UDF description
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>>;
    /// Getter for a UDAF description
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>>;
    /// Getter for a UDWF
    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>>;
    /// Getter for system/user-defined variable type
    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType>;

    /// Get configuration options
    fn options(&self) -> &ConfigOptions;

    /// Get all user defined scalar function names
    fn udf_names(&self) -> Vec<String>;

    /// Get all user defined aggregate function names
    fn udaf_names(&self) -> Vec<String>;

    /// Get all user defined window function names
    fn udwf_names(&self) -> Vec<String>;
}

/// This trait allows users to customize the behavior of the SQL planner
pub trait UserDefinedPlanner {
    /// Plan the binary operation between two expressions, return None if not possible
    fn plan_binary_op(
        &self,
        expr: BinaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerSimplifyResult> {
        Ok(PlannerSimplifyResult::OriginalBinaryExpr(expr))
    }

    /// Plan the field access expression, return None if not possible
    fn plan_field_access(
        &self,
        expr: FieldAccessExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerSimplifyResult> {
        Ok(PlannerSimplifyResult::OriginalFieldAccessExpr(expr))
    }

    fn plan_array_literal(
        &self,
        exprs: Vec<Expr>,
        _schema: &DFSchema,
    ) -> Result<PlannerSimplifyResult> {
        Ok(PlannerSimplifyResult::OriginalArray(exprs))
    }
}

pub struct BinaryExpr {
    pub op: sqlparser::ast::BinaryOperator,
    pub left: Expr,
    pub right: Expr,
}

pub struct FieldAccessExpr {
    pub field_access: GetFieldAccess,
    pub expr: Expr,
}

pub enum PlannerSimplifyResult {
    /// The function call was simplified to an entirely new Expr
    Simplified(Expr),
    /// the function call could not be simplified, and the arguments
    /// are return unmodified.
    OriginalBinaryExpr(BinaryExpr),
    OriginalFieldAccessExpr(FieldAccessExpr),
    OriginalArray(Vec<Expr>),
}
