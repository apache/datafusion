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

use std::fmt::Debug;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion_common::{
    config::ConfigOptions, file_options::file_type::FileType, not_impl_err, DFSchema,
    Result, TableReference,
};
use sqlparser::ast::{self, NullTreatment};

use crate::{
    AggregateUDF, Expr, GetFieldAccess, ScalarUDF, SortExpr, TableSource, WindowFrame,
    WindowFunctionDefinition, WindowUDF,
};

/// Provides the `SQL` query planner meta-data about tables and
/// functions referenced in SQL statements, without a direct dependency on the
/// `datafusion` Catalog structures such as [`TableProvider`]
///
/// [`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
pub trait ContextProvider {
    /// Returns a table by reference, if it exists
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>>;

    /// Return the type of a file based on its extension (e.g. `.parquet`)
    ///
    /// This is used to plan `COPY` statements
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

    /// Provides an intermediate table that is used to store the results of a CTE during execution
    ///
    /// CTE stands for "Common Table Expression"
    ///
    /// # Notes
    /// We don't directly implement this in [`SqlToRel`] as implementing this function
    /// often requires access to a table that contains
    /// execution-related types that can't be a direct dependency
    /// of the sql crate (for example [`CteWorkTable`]).
    ///
    /// The [`ContextProvider`] provides a way to "hide" this dependency.
    ///
    /// [`SqlToRel`]: https://docs.rs/datafusion/latest/datafusion/sql/planner/struct.SqlToRel.html
    /// [`CteWorkTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/cte_worktable/struct.CteWorkTable.html
    fn create_cte_work_table(
        &self,
        _name: &str,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn TableSource>> {
        not_impl_err!("Recursive CTE is not implemented")
    }

    /// Return [`ExprPlanner`] extensions for planning expressions
    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &[]
    }

    /// Return [`TypePlanner`] extensions for planning data types
    fn get_type_planner(&self) -> Option<Arc<dyn TypePlanner>> {
        None
    }

    /// Return the scalar function with a given name, if any
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>>;

    /// Return the aggregate function with a given name, if any
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>>;

    /// Return the window function with a given name, if any
    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>>;

    /// Return the system/user-defined variable type, if any
    ///
    /// A user defined variable is typically accessed via `@var_name`
    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType>;

    /// Return overall configuration options
    fn options(&self) -> &ConfigOptions;

    /// Return all scalar function names
    fn udf_names(&self) -> Vec<String>;

    /// Return all aggregate function names
    fn udaf_names(&self) -> Vec<String>;

    /// Return all window function names
    fn udwf_names(&self) -> Vec<String>;
}

/// Customize planning of SQL AST expressions to [`Expr`]s
pub trait ExprPlanner: Debug + Send + Sync {
    /// Plan the binary operation between two expressions, returns original
    /// BinaryExpr if not possible
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plan the field access expression, such as `foo.bar`
    ///
    /// returns original [`RawFieldAccessExpr`] if not possible
    fn plan_field_access(
        &self,
        expr: RawFieldAccessExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawFieldAccessExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plan an array literal, such as `[1, 2, 3]`
    ///
    /// Returns original expression arguments if not possible
    fn plan_array_literal(
        &self,
        exprs: Vec<Expr>,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(exprs))
    }

    /// Plan a `POSITION` expression, such as `POSITION(<expr> in <expr>)`
    ///
    /// Returns original expression arguments if not possible
    fn plan_position(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plan a dictionary literal, such as `{ key: value, ...}`
    ///
    /// Returns original expression arguments if not possible
    fn plan_dictionary_literal(
        &self,
        expr: RawDictionaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawDictionaryExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plan an extract expression, such as`EXTRACT(month FROM foo)`
    ///
    /// Returns original expression arguments if not possible
    fn plan_extract(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plan an substring expression, such as `SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])`
    ///
    /// Returns original expression arguments if not possible
    fn plan_substring(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plans a struct literal, such as  `{'field1' : expr1, 'field2' : expr2, ...}`
    ///
    /// This function takes a vector of expressions and a boolean flag
    /// indicating whether the struct uses the optional name
    ///
    /// Returns the original input expressions if planning is not possible.
    fn plan_struct_literal(
        &self,
        args: Vec<Expr>,
        _is_named_struct: bool,
    ) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plans an overlay expression, such as `overlay(str PLACING substr FROM pos [FOR count])`
    ///
    /// Returns original expression arguments if not possible
    fn plan_overlay(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plans a `make_map` expression, such as `make_map(key1, value1, key2, value2, ...)`
    ///
    /// Returns original expression arguments if not possible
    fn plan_make_map(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plans compound identifier such as `db.schema.table` for non-empty nested names
    ///
    /// # Note:
    /// Currently compound identifier for outer query schema is not supported.
    ///
    /// Returns original expression if not possible
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

    /// Plans `ANY` expression, such as `expr = ANY(array_expr)`
    ///
    /// Returns origin binary expression if not possible
    fn plan_any(&self, expr: RawBinaryExpr) -> Result<PlannerResult<RawBinaryExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plans aggregate functions, such as `COUNT(<expr>)`
    ///
    /// Returns original expression arguments if not possible
    fn plan_aggregate(
        &self,
        expr: RawAggregateExpr,
    ) -> Result<PlannerResult<RawAggregateExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plans window functions, such as `COUNT(<expr>)`
    ///
    /// Returns original expression arguments if not possible
    fn plan_window(&self, expr: RawWindowExpr) -> Result<PlannerResult<RawWindowExpr>> {
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
    pub op: ast::BinaryOperator,
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

/// This structure is used by `AggregateFunctionPlanner` to plan operators with
/// custom expressions.
#[derive(Debug, Clone)]
pub struct RawAggregateExpr {
    pub func: Arc<AggregateUDF>,
    pub args: Vec<Expr>,
    pub distinct: bool,
    pub filter: Option<Box<Expr>>,
    pub order_by: Vec<SortExpr>,
    pub null_treatment: Option<NullTreatment>,
}

/// This structure is used by `WindowFunctionPlanner` to plan operators with
/// custom expressions.
#[derive(Debug, Clone)]
pub struct RawWindowExpr {
    pub func_def: WindowFunctionDefinition,
    pub args: Vec<Expr>,
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<SortExpr>,
    pub window_frame: WindowFrame,
    pub null_treatment: Option<NullTreatment>,
    pub distinct: bool,
}

/// Result of planning a raw expr with [`ExprPlanner`]
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum PlannerResult<T> {
    /// The raw expression was successfully planned as a new [`Expr`]
    Planned(Expr),
    /// The raw expression could not be planned, and is returned unmodified
    Original(T),
}

/// Customize planning SQL types to DataFusion (Arrow) types.
pub trait TypePlanner: Debug + Send + Sync {
    /// Plan SQL [`ast::DataType`] to DataFusion [`DataType`]
    ///
    /// Returns None if not possible
    fn plan_type(&self, _sql_type: &ast::DataType) -> Result<Option<DataType>> {
        Ok(None)
    }
}
