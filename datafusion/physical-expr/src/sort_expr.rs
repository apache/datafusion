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

//! Sort expressions

use crate::PhysicalExpr;
use arrow::compute::kernels::sort::{SortColumn, SortOptions};
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Represents Sort operation for a column in a RecordBatch
#[derive(Clone, Debug)]
pub struct PhysicalSortExpr {
    /// Physical expression representing the column to sort
    pub expr: Arc<dyn PhysicalExpr>,
    /// Option to specify how the given column should be sorted
    pub options: SortOptions,
}

impl PartialEq for PhysicalSortExpr {
    fn eq(&self, other: &PhysicalSortExpr) -> bool {
        self.options == other.options && self.expr.eq(&other.expr)
    }
}

impl Eq for PhysicalSortExpr {}

impl Hash for PhysicalSortExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.options.hash(state);
    }
}

impl std::fmt::Display for PhysicalSortExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} {}", self.expr, to_str(&self.options))
    }
}

impl PhysicalSortExpr {
    /// evaluate the sort expression into SortColumn that can be passed into arrow sort kernel
    pub fn evaluate_to_sort_column(&self, batch: &RecordBatch) -> Result<SortColumn> {
        let value_to_sort = self.expr.evaluate(batch)?;
        let array_to_sort = match value_to_sort {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => {
                return Err(DataFusionError::Plan(format!(
                    "Sort operation is not applicable to scalar value {scalar}"
                )));
            }
        };
        Ok(SortColumn {
            values: array_to_sort,
            options: Some(self.options),
        })
    }

    /// Check whether sort expression satisfies [`PhysicalSortRequirement`].
    ///
    /// If sort options is Some in `PhysicalSortRequirement`, `expr`
    /// and `options` field are compared for equality.
    ///
    /// If sort options is None in `PhysicalSortRequirement`, only
    /// `expr` is compared for equality.
    pub fn satisfy(&self, requirement: &PhysicalSortRequirement) -> bool {
        self.expr.eq(&requirement.expr)
            && requirement
                .options
                .map_or(true, |opts| self.options == opts)
    }
}

/// Represents sort requirement associated with a plan
///
/// If the requirement includes [`SortOptions`] then both the
/// expression *and* the sort options must match.
///
/// If the requirement does not include [`SortOptions`]) then only the
/// expressions must match.
///
/// # Examples
///
/// With sort options (`A`, `DESC NULLS FIRST`):
/// * `ORDER BY A DESC NULLS FIRST` matches
/// * `ORDER BY A ASC  NULLS FIRST` does not match (`ASC` vs `DESC`)
/// * `ORDER BY B DESC NULLS FIRST` does not match (different expr)
///
/// Without sort options (`A`, None):
/// * `ORDER BY A DESC NULLS FIRST` matches
/// * `ORDER BY A ASC  NULLS FIRST` matches (`ASC` and `NULL` options ignored)
/// * `ORDER BY B DESC NULLS FIRST` does not match  (different expr)
#[derive(Clone, Debug)]
pub struct PhysicalSortRequirement {
    /// Physical expression representing the column to sort
    pub expr: Arc<dyn PhysicalExpr>,
    /// Option to specify how the given column should be sorted.
    /// If unspecified, there are no constraints on sort options.
    pub options: Option<SortOptions>,
}

impl From<PhysicalSortRequirement> for PhysicalSortExpr {
    /// If options is `None`, the default sort options `ASC, NULLS LAST` is used.
    ///
    /// The default is picked to be consistent with
    /// PostgreSQL: <https://www.postgresql.org/docs/current/queries-order.html>    
    fn from(value: PhysicalSortRequirement) -> Self {
        let options = value.options.unwrap_or(SortOptions {
            descending: false,
            nulls_first: false,
        });
        PhysicalSortExpr {
            expr: value.expr,
            options,
        }
    }
}

impl From<PhysicalSortExpr> for PhysicalSortRequirement {
    fn from(value: PhysicalSortExpr) -> Self {
        PhysicalSortRequirement::new(value.expr, Some(value.options))
    }
}

impl PartialEq for PhysicalSortRequirement {
    fn eq(&self, other: &PhysicalSortRequirement) -> bool {
        self.options == other.options && self.expr.eq(&other.expr)
    }
}

impl std::fmt::Display for PhysicalSortRequirement {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let opts_string = self.options.as_ref().map_or("NA", to_str);
        write!(f, "{} {}", self.expr, opts_string)
    }
}

impl PhysicalSortRequirement {
    /// Creates a new requirement.
    ///
    /// If `options` is `Some(..)`, creates an `exact` requirement,
    /// which must match both `options` and `expr`.
    ///
    /// If `options` is `None`, Creates a new `expr_only` requirement,
    /// which must match only `expr`.
    ///
    /// See [`PhysicalSortRequirement`] for examples.
    pub fn new(expr: Arc<dyn PhysicalExpr>, options: Option<SortOptions>) -> Self {
        Self { expr, options }
    }

    /// Replace the required expression for this requirement with the new one
    pub fn with_expr(mut self, expr: Arc<dyn PhysicalExpr>) -> Self {
        self.expr = expr;
        self
    }

    /// Returns whether this requirement is equal or more specific than `other`.
    pub fn compatible(&self, other: &PhysicalSortRequirement) -> bool {
        self.expr.eq(&other.expr)
            && other.options.map_or(true, |other_opts| {
                self.options.map_or(false, |opts| opts == other_opts)
            })
    }

    /// Returns [`PhysicalSortRequirement`] that requires the exact
    /// sort of the [`PhysicalSortExpr`]s in `ordering`
    ///
    /// This method takes `&'a PhysicalSortExpr` to make it easy to
    /// use implementing [`ExecutionPlan::required_input_ordering`].
    ///
    /// [`ExecutionPlan::required_input_ordering`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html#method.required_input_ordering
    pub fn from_sort_exprs<'a>(
        ordering: impl IntoIterator<Item = &'a PhysicalSortExpr>,
    ) -> Vec<PhysicalSortRequirement> {
        ordering
            .into_iter()
            .cloned()
            .map(PhysicalSortRequirement::from)
            .collect()
    }

    /// Converts an iterator of [`PhysicalSortRequirement`] into a Vec
    /// of [`PhysicalSortExpr`]s.
    ///
    /// This function converts `PhysicalSortRequirement` to `PhysicalSortExpr`
    /// for each entry in the input. If required ordering is None for an entry
    /// default ordering `ASC, NULLS LAST` if given (see the `PhysicalSortExpr::from`).
    pub fn to_sort_exprs(
        requirements: impl IntoIterator<Item = PhysicalSortRequirement>,
    ) -> Vec<PhysicalSortExpr> {
        requirements
            .into_iter()
            .map(PhysicalSortExpr::from)
            .collect()
    }
}

/// Returns the SQL string representation of the given [SortOptions] object.
#[inline]
fn to_str(options: &SortOptions) -> &str {
    match (options.descending, options.nulls_first) {
        (true, true) => "DESC",
        (true, false) => "DESC NULLS LAST",
        (false, true) => "ASC",
        (false, false) => "ASC NULLS LAST",
    }
}

///`LexOrdering` is a type alias for lexicographical ordering definition`Vec<PhysicalSortExpr>`
pub type LexOrdering = Vec<PhysicalSortExpr>;

///`LexOrderingReq` is a type alias for lexicographical ordering requirement definition`Vec<PhysicalSortRequirement>`
pub type LexOrderingReq = Vec<PhysicalSortRequirement>;
