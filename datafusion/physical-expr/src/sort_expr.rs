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

impl std::fmt::Display for PhysicalSortExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} {}", self.expr, to_string(&self.options))
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

    pub fn satisfy(&self, requirement: &PhysicalSortRequirement) -> bool {
        self.expr.eq(&requirement.expr)
            && requirement
                .options
                .map_or(true, |opts| self.options == opts)
    }
}

/// Represents sort requirement associated with a plan
#[derive(Clone, Debug)]
pub struct PhysicalSortRequirement {
    /// Physical expression representing the column to sort
    pub expr: Arc<dyn PhysicalExpr>,
    /// Option to specify how the given column should be sorted.
    /// If unspecified, there is no constraint on sort options.
    pub options: Option<SortOptions>,
}

impl From<PhysicalSortExpr> for PhysicalSortRequirement {
    fn from(value: PhysicalSortExpr) -> Self {
        Self {
            expr: value.expr,
            options: Some(value.options),
        }
    }
}

impl PartialEq for PhysicalSortRequirement {
    fn eq(&self, other: &PhysicalSortRequirement) -> bool {
        self.options == other.options && self.expr.eq(&other.expr)
    }
}

impl std::fmt::Display for PhysicalSortRequirement {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let opts_string = self.options.as_ref().map_or("NA", to_string);
        write!(f, "{} {}", self.expr, opts_string)
    }
}

impl PhysicalSortRequirement {
    /// Returns whether this requirement is equal or more specific than `other`.
    pub fn compatible(&self, other: &PhysicalSortRequirement) -> bool {
        self.expr.eq(&other.expr)
            && other.options.map_or(true, |other_opts| {
                self.options.map_or(false, |opts| opts == other_opts)
            })
    }
}

pub fn make_sort_requirements_from_exprs(
    ordering: &[PhysicalSortExpr],
) -> Vec<PhysicalSortRequirement> {
    ordering.iter().map(|e| e.clone().into()).collect()
}

/// Returns the SQL string representation of the given [SortOptions] object.
#[inline]
fn to_string(options: &SortOptions) -> &str {
    match (options.descending, options.nulls_first) {
        (true, true) => "DESC",
        (true, false) => "DESC NULLS LAST",
        (false, true) => "ASC",
        (false, false) => "ASC NULLS LAST",
    }
}
