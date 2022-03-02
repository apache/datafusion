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

impl std::fmt::Display for PhysicalSortExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let opts_string = match (self.options.descending, self.options.nulls_first) {
            (true, true) => "DESC",
            (true, false) => "DESC NULLS LAST",
            (false, true) => "ASC",
            (false, false) => "ASC NULLS LAST",
        };

        write!(f, "{} {}", self.expr, opts_string)
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
                    "Sort operation is not applicable to scalar value {}",
                    scalar
                )));
            }
        };
        Ok(SortColumn {
            values: array_to_sort,
            options: Some(self.options),
        })
    }
}
