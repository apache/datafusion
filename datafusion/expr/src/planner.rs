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

//! [`ContextProvider`] and [`ExprPlanner`] APIs to customize SQL query planning

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion_common::{
    config::ConfigOptions, file_options::file_type::FileType, not_impl_err, DFSchema,
    Result, TableReference,
};

use crate::{AggregateUDF, Expr, GetFieldAccess, ScalarUDF, TableSource, WindowUDF};

/// Provides the `SQL` query planner  meta-data about tables and
/// functions referenced in SQL statements, without a direct dependency on other
/// DataFusion structures
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

    /// Getter for expr planners
    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &[]
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
pub trait ExprPlanner: Send + Sync {
    /// Plan the binary operation between two expressions, returns original
    /// BinaryExpr if not possible
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plan the field access expression
    ///
    /// returns original FieldAccessExpr if not possible
    fn plan_field_access(
        &self,
        expr: RawFieldAccessExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawFieldAccessExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plan the array literal, returns OriginalArray if not possible
    ///
    /// Returns origin expression arguments if not possible
    fn plan_array_literal(
        &self,
        exprs: Vec<Expr>,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(exprs))
    }

    // Plan the POSITION expression, e.g., POSITION(<expr> in <expr>)
    // returns origin expression arguments if not possible
    fn plan_position(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plan the dictionary literal `{ key: value, ...}`
    ///
    /// Returns origin expression arguments if not possible
    fn plan_dictionary_literal(
        &self,
        expr: RawDictionaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawDictionaryExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plan an extract expression, e.g., `EXTRACT(month FROM foo)`
    ///
    /// Returns origin expression arguments if not possible
    fn plan_extract(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plan an substring expression, e.g., `SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])`
    ///
    /// Returns origin expression arguments if not possible
    fn plan_substring(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plans a struct `struct(expression1[, ..., expression_n])`
    /// literal based on the given input expressions.
    /// This function takes a vector of expressions and a boolean flag indicating whether
    /// the struct uses the optional name
    ///
    /// Returns a `PlannerResult` containing either the planned struct expressions or the original
    /// input expressions if planning is not possible.
    fn plan_struct_literal(
        &self,
        args: Vec<Expr>,
        _is_named_struct: bool,
    ) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plans an overlay expression eg `overlay(str PLACING substr FROM pos [FOR count])`
    ///
    /// Returns origin expression arguments if not possible
    fn plan_overlay(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plan a make_map expression, e.g., `make_map(key1, value1, key2, value2, ...)`
    ///
    /// Returns origin expression arguments if not possible
    fn plan_make_map(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plans compound identifier eg `db.schema.table` for non-empty nested names
    ///
    /// Note:
    /// Currently compound identifier for outer query schema is not supported.
    ///
    /// Returns planned expression
    fn plan_compound_identifier(
        &self,
        _field: &Field,
        _qualifier: Option<&TableReference>,
        _nested_names: &[String],
    ) -> Result<PlannerResult<Vec<Expr>>> {
        not_impl_err!(
            "Default planner compound identifier hasn't been implemented for ExprPlanner"
        )
    }

    /// Plans `ANY` expression, e.g., `expr = ANY(array_expr)`
    ///
    /// Returns origin binary expression if not possible
    fn plan_any(&self, expr: RawBinaryExpr) -> Result<PlannerResult<RawBinaryExpr>> {
        Ok(PlannerResult::Original(expr))
    }
}

/// An operator with two arguments to plan
///
/// Note `left` and `right` are DataFusion [`Expr`]s but the `op` is the SQL AST
/// operator.
///
/// This structure is used by [`ExprPlanner`] to plan operators with
/// custom expressions.
#[derive(Debug, Clone)]
pub struct RawBinaryExpr {
    pub op: sqlparser::ast::BinaryOperator,
    pub left: Expr,
    pub right: Expr,
}

/// An expression with GetFieldAccess to plan
///
/// This structure is used by [`ExprPlanner`] to plan operators with
/// custom expressions.
#[derive(Debug, Clone)]
pub struct RawFieldAccessExpr {
    pub field_access: GetFieldAccess,
    pub expr: Expr,
}

/// A Dictionary literal expression `{ key: value, ...}`
///
/// This structure is used by [`ExprPlanner`] to plan operators with
/// custom expressions.
#[derive(Debug, Clone)]
pub struct RawDictionaryExpr {
    pub keys: Vec<Expr>,
    pub values: Vec<Expr>,
}

/// Result of planning a raw expr with [`ExprPlanner`]
#[derive(Debug, Clone)]
pub enum PlannerResult<T> {
    /// The raw expression was successfully planned as a new [`Expr`]
    Planned(Expr),
    /// The raw expression could not be planned, and is returned unmodified
    Original(T),
}
