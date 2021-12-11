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

//! Defines physical expressions that can evaluated at runtime during query execution

use std::sync::Arc;

use super::ColumnarValue;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::PhysicalExpr;
use arrow::compute::kernels::sort::{SortColumn, SortOptions};
use arrow::record_batch::RecordBatch;

mod approx_distinct;
mod array_agg;
mod average;
#[macro_use]
mod binary;
mod case;
mod cast;
mod coercion;
mod column;
mod count;
mod cume_dist;
mod get_indexed_field;
mod in_list;
mod is_not_null;
mod is_null;
mod lead_lag;
mod literal;
#[macro_use]
mod min_max;
mod negative;
mod not;
mod nth_value;
mod nullif;
mod rank;
mod row_number;
mod sum;
mod try_cast;

/// Module with some convenient methods used in expression building
pub mod helpers {
    pub use super::min_max::{max, min};
}

pub use approx_distinct::ApproxDistinct;
pub use array_agg::ArrayAgg;
pub(crate) use average::is_avg_support_arg_type;
pub use average::{avg_return_type, Avg, AvgAccumulator};
pub use binary::{binary, binary_operator_data_type, BinaryExpr};
pub use case::{case, CaseExpr};
pub use cast::{
    cast, cast_column, cast_with_options, CastExpr, DEFAULT_DATAFUSION_CAST_OPTIONS,
};
pub use column::{col, Column};
pub use count::Count;
pub use cume_dist::cume_dist;
pub use get_indexed_field::GetIndexedFieldExpr;
pub use in_list::{in_list, InListExpr};
pub use is_not_null::{is_not_null, IsNotNullExpr};
pub use is_null::{is_null, IsNullExpr};
pub use lead_lag::{lag, lead};
pub use literal::{lit, Literal};
pub use min_max::{Max, Min};
pub(crate) use min_max::{MaxAccumulator, MinAccumulator};
pub use negative::{negative, NegativeExpr};
pub use not::{not, NotExpr};
pub use nth_value::NthValue;
pub use nullif::{nullif_func, SUPPORTED_NULLIF_TYPES};
pub use rank::{dense_rank, percent_rank, rank};
pub use row_number::RowNumber;
pub(crate) use sum::is_sum_support_arg_type;
pub use sum::{sum_return_type, Sum};
pub use try_cast::{try_cast, TryCastExpr};

/// returns the name of the state
pub fn format_state_name(name: &str, state_name: &str) -> String {
    format!("{}[{}]", name, state_name)
}

/// Represents Sort operation for a column in a RecordBatch
#[derive(Clone, Debug)]
pub struct PhysicalSortExpr {
    /// Physical expression representing the column to sort
    pub expr: Arc<dyn PhysicalExpr>,
    /// Option to specify how the given column should be sorted
    pub options: SortOptions,
}

impl std::fmt::Display for PhysicalSortExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{error::Result, physical_plan::AggregateExpr, scalar::ScalarValue};

    /// macro to perform an aggregation and verify the result.
    #[macro_export]
    macro_rules! generic_test_op {
        ($ARRAY:expr, $DATATYPE:expr, $OP:ident, $EXPECTED:expr, $EXPECTED_DATATYPE:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $DATATYPE, false)]);

            let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![$ARRAY])?;

            let agg = Arc::new(<$OP>::new(
                col("a", &schema)?,
                "bla".to_string(),
                $EXPECTED_DATATYPE,
            ));
            let actual = aggregate(&batch, agg)?;
            let expected = ScalarValue::from($EXPECTED);

            assert_eq!(expected, actual);

            Ok(())
        }};
    }

    pub fn aggregate(
        batch: &RecordBatch,
        agg: Arc<dyn AggregateExpr>,
    ) -> Result<ScalarValue> {
        let mut accum = agg.create_accumulator()?;
        let expr = agg.expressions();
        let values = expr
            .iter()
            .map(|e| e.evaluate(batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect::<Result<Vec<_>>>()?;
        accum.update_batch(&values)?;
        accum.evaluate()
    }
}
