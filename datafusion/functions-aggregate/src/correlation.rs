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

//! [`Correlation`]: correlation sample aggregations.

use std::any::Any;
use std::fmt::Debug;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{
    downcast_array, Array, AsArray, BooleanArray, Float64Array, NullBufferBuilder,
    UInt64Array,
};
use arrow::compute::{and, filter, is_not_null};
use arrow::datatypes::{FieldRef, Float64Type, UInt64Type};
use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field},
};
use datafusion_expr::{EmitTo, GroupsAccumulator};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::accumulate::accumulate_multiple;
use log::debug;

use crate::covariance::CovarianceAccumulator;
use crate::stddev::StddevAccumulator;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    utils::format_state_name,
    Accumulator, AggregateUDFImpl, Documentation, Signature, Volatility,
};
use datafusion_functions_aggregate_common::stats::StatsType;
use datafusion_macros::user_doc;

make_udaf_expr_and_func!(
    Correlation,
    corr,
    y x,
    "Correlation between two numeric values.",
    corr_udaf
);

#[user_doc(
    doc_section(label = "Statistical Functions"),
    description = "Returns the coefficient of correlation between two numeric values.",
    syntax_example = "corr(expression1, expression2)",
    sql_example = r#"```sql
> SELECT corr(column1, column2) FROM table_name;
+--------------------------------+
| corr(column1, column2)         |
+--------------------------------+
| 0.85                           |
+--------------------------------+
```"#,
    standard_argument(name = "expression1", prefix = "First"),
    standard_argument(name = "expression2", prefix = "Second")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Correlation {
    signature: Signature,
}

impl Default for Correlation {
    fn default() -> Self {
        Self::new()
    }
}

impl Correlation {
    /// Create a new CORR aggregate function
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for Correlation {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "corr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CorrelationAccumulator::try_new()?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let name = args.name;
        Ok(vec![
            Field::new(format_state_name(name, "count"), DataType::UInt64, true),
            Field::new(format_state_name(name, "mean1"), DataType::Float64, true),
            Field::new(format_state_name(name, "m2_1"), DataType::Float64, true),
            Field::new(format_state_name(name, "mean2"), DataType::Float64, true),
            Field::new(format_state_name(name, "m2_2"), DataType::Float64, true),
            Field::new(
                format_state_name(name, "algo_const"),
                DataType::Float64,
                true,
            ),
        ]
        .into_iter()
        .map(Arc::new)
        .collect())
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        debug!("GroupsAccumulator is created for aggregate function `corr(c1, c2)`");
        Ok(Box::new(CorrelationGroupsAccumulator::new()))
    }
}

/// An accumulator to compute correlation
#[derive(Debug)]
pub struct CorrelationAccumulator {
    covar: CovarianceAccumulator,
    stddev1: StddevAccumulator,
    stddev2: StddevAccumulator,
}

impl CorrelationAccumulator {
    /// Creates a new `CorrelationAccumulator`
    pub fn try_new() -> Result<Self> {
        Ok(Self {
            covar: CovarianceAccumulator::try_new(StatsType::Population)?,
            stddev1: StddevAccumulator::try_new(StatsType::Population)?,
            stddev2: StddevAccumulator::try_new(StatsType::Population)?,
        })
    }
}

impl Accumulator for CorrelationAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // TODO: null input skipping logic duplicated across Correlation
        // and its children accumulators.
        // This could be simplified by splitting up input filtering and
        // calculation logic in children accumulators, and calling only
        // calculation part from Correlation
        let values = if values[0].null_count() != 0 || values[1].null_count() != 0 {
            let mask = and(&is_not_null(&values[0])?, &is_not_null(&values[1])?)?;
            let values1 = filter(&values[0], &mask)?;
            let values2 = filter(&values[1], &mask)?;

            vec![values1, values2]
        } else {
            values.to_vec()
        };

        self.covar.update_batch(&values)?;
        self.stddev1.update_batch(&values[0..1])?;
        self.stddev2.update_batch(&values[1..2])?;
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let n = self.covar.get_count();
        if n < 2 {
            return Ok(ScalarValue::Float64(None));
        }

        let covar = self.covar.evaluate()?;
        let stddev1 = self.stddev1.evaluate()?;
        let stddev2 = self.stddev2.evaluate()?;

        if let ScalarValue::Float64(Some(c)) = covar {
            if let ScalarValue::Float64(Some(s1)) = stddev1 {
                if let ScalarValue::Float64(Some(s2)) = stddev2 {
                    if s1 == 0_f64 || s2 == 0_f64 {
                        return Ok(ScalarValue::Float64(None));
                    } else {
                        return Ok(ScalarValue::Float64(Some(c / s1 / s2)));
                    }
                }
            }
        }

        Ok(ScalarValue::Float64(None))
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.covar) + self.covar.size()
            - size_of_val(&self.stddev1)
            + self.stddev1.size()
            - size_of_val(&self.stddev2)
            + self.stddev2.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.covar.get_count()),
            ScalarValue::from(self.covar.get_mean1()),
            ScalarValue::from(self.stddev1.get_m2()),
            ScalarValue::from(self.covar.get_mean2()),
            ScalarValue::from(self.stddev2.get_m2()),
            ScalarValue::from(self.covar.get_algo_const()),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let states_c = [
            Arc::clone(&states[0]),
            Arc::clone(&states[1]),
            Arc::clone(&states[3]),
            Arc::clone(&states[5]),
        ];
        let states_s1 = [
            Arc::clone(&states[0]),
            Arc::clone(&states[1]),
            Arc::clone(&states[2]),
        ];
        let states_s2 = [
            Arc::clone(&states[0]),
            Arc::clone(&states[3]),
            Arc::clone(&states[4]),
        ];

        self.covar.merge_batch(&states_c)?;
        self.stddev1.merge_batch(&states_s1)?;
        self.stddev2.merge_batch(&states_s2)?;
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = if values[0].null_count() != 0 || values[1].null_count() != 0 {
            let mask = and(&is_not_null(&values[0])?, &is_not_null(&values[1])?)?;
            let values1 = filter(&values[0], &mask)?;
            let values2 = filter(&values[1], &mask)?;

            vec![values1, values2]
        } else {
            values.to_vec()
        };

        self.covar.retract_batch(&values)?;
        self.stddev1.retract_batch(&values[0..1])?;
        self.stddev2.retract_batch(&values[1..2])?;
        Ok(())
    }
}

#[derive(Default)]
pub struct CorrelationGroupsAccumulator {
    // Number of elements for each group
    // This is also used to track nulls: if a group has 0 valid values accumulated,
    // final aggregation result will be null.
    count: Vec<u64>,
    // Sum of x values for each group
    sum_x: Vec<f64>,
    // Sum of y
    sum_y: Vec<f64>,
    // Sum of x*y
    sum_xy: Vec<f64>,
    // Sum of x^2
    sum_xx: Vec<f64>,
    // Sum of y^2
    sum_yy: Vec<f64>,
}

impl CorrelationGroupsAccumulator {
    pub fn new() -> Self {
        Default::default()
    }
}

/// Specialized version of `accumulate_multiple` for correlation's merge_batch
///
/// Note: Arrays in `state_arrays` should not have null values, because they are all
/// intermediate states created within the accumulator, instead of inputs from
/// outside.
fn accumulate_correlation_states(
    group_indices: &[usize],
    state_arrays: (
        &UInt64Array,  // count
        &Float64Array, // sum_x
        &Float64Array, // sum_y
        &Float64Array, // sum_xy
        &Float64Array, // sum_xx
        &Float64Array, // sum_yy
    ),
    mut value_fn: impl FnMut(usize, u64, &[f64]),
) {
    let (counts, sum_x, sum_y, sum_xy, sum_xx, sum_yy) = state_arrays;

    assert_eq!(counts.null_count(), 0);
    assert_eq!(sum_x.null_count(), 0);
    assert_eq!(sum_y.null_count(), 0);
    assert_eq!(sum_xy.null_count(), 0);
    assert_eq!(sum_xx.null_count(), 0);
    assert_eq!(sum_yy.null_count(), 0);

    let counts_values = counts.values().as_ref();
    let sum_x_values = sum_x.values().as_ref();
    let sum_y_values = sum_y.values().as_ref();
    let sum_xy_values = sum_xy.values().as_ref();
    let sum_xx_values = sum_xx.values().as_ref();
    let sum_yy_values = sum_yy.values().as_ref();

    for (idx, &group_idx) in group_indices.iter().enumerate() {
        let row = [
            sum_x_values[idx],
            sum_y_values[idx],
            sum_xy_values[idx],
            sum_xx_values[idx],
            sum_yy_values[idx],
        ];
        value_fn(group_idx, counts_values[idx], &row);
    }
}

/// GroupsAccumulator implementation for `corr(x, y)` that computes the Pearson correlation coefficient
/// between two numeric columns.
///
/// Online algorithm for correlation:
///
/// r = (n * sum_xy - sum_x * sum_y) / sqrt((n * sum_xx - sum_x^2) * (n * sum_yy - sum_y^2))
/// where:
/// n = number of observations
/// sum_x = sum of x values
/// sum_y = sum of y values  
/// sum_xy = sum of (x * y)
/// sum_xx = sum of x^2 values
/// sum_yy = sum of y^2 values
///
/// Reference: <https://en.wikipedia.org/wiki/Pearson_correlation_coefficient#For_a_sample>
impl GroupsAccumulator for CorrelationGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.count.resize(total_num_groups, 0);
        self.sum_x.resize(total_num_groups, 0.0);
        self.sum_y.resize(total_num_groups, 0.0);
        self.sum_xy.resize(total_num_groups, 0.0);
        self.sum_xx.resize(total_num_groups, 0.0);
        self.sum_yy.resize(total_num_groups, 0.0);

        let array_x = downcast_array::<Float64Array>(&values[0]);
        let array_y = downcast_array::<Float64Array>(&values[1]);

        accumulate_multiple(
            group_indices,
            &[&array_x, &array_y],
            opt_filter,
            |group_index, batch_index, columns| {
                let x = columns[0].value(batch_index);
                let y = columns[1].value(batch_index);
                self.count[group_index] += 1;
                self.sum_x[group_index] += x;
                self.sum_y[group_index] += y;
                self.sum_xy[group_index] += x * y;
                self.sum_xx[group_index] += x * x;
                self.sum_yy[group_index] += y * y;
            },
        );

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        // Resize vectors to accommodate total number of groups
        self.count.resize(total_num_groups, 0);
        self.sum_x.resize(total_num_groups, 0.0);
        self.sum_y.resize(total_num_groups, 0.0);
        self.sum_xy.resize(total_num_groups, 0.0);
        self.sum_xx.resize(total_num_groups, 0.0);
        self.sum_yy.resize(total_num_groups, 0.0);

        // Extract arrays from input values
        let partial_counts = values[0].as_primitive::<UInt64Type>();
        let partial_sum_x = values[1].as_primitive::<Float64Type>();
        let partial_sum_y = values[2].as_primitive::<Float64Type>();
        let partial_sum_xy = values[3].as_primitive::<Float64Type>();
        let partial_sum_xx = values[4].as_primitive::<Float64Type>();
        let partial_sum_yy = values[5].as_primitive::<Float64Type>();

        assert!(opt_filter.is_none(), "aggregate filter should be applied in partial stage, there should be no filter in final stage");

        accumulate_correlation_states(
            group_indices,
            (
                partial_counts,
                partial_sum_x,
                partial_sum_y,
                partial_sum_xy,
                partial_sum_xx,
                partial_sum_yy,
            ),
            |group_index, count, values| {
                self.count[group_index] += count;
                self.sum_x[group_index] += values[0];
                self.sum_y[group_index] += values[1];
                self.sum_xy[group_index] += values[2];
                self.sum_xx[group_index] += values[3];
                self.sum_yy[group_index] += values[4];
            },
        );

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let n = match emit_to {
            EmitTo::All => self.count.len(),
            EmitTo::First(n) => n,
        };

        let mut values = Vec::with_capacity(n);
        let mut nulls = NullBufferBuilder::new(n);

        // Notes for `Null` handling:
        // - If the `count` state of a group is 0, no valid records are accumulated
        //   for this group, so the aggregation result is `Null`.
        // - Correlation can't be calculated when a group only has 1 record, or when
        //   the `denominator` state is 0. In these cases, the final aggregation
        //   result should be `Null` (according to PostgreSQL's behavior).
        //
        for i in 0..n {
            if self.count[i] < 2 {
                values.push(0.0);
                nulls.append_null();
                continue;
            }

            let count = self.count[i];
            let sum_x = self.sum_x[i];
            let sum_y = self.sum_y[i];
            let sum_xy = self.sum_xy[i];
            let sum_xx = self.sum_xx[i];
            let sum_yy = self.sum_yy[i];

            let mean_x = sum_x / count as f64;
            let mean_y = sum_y / count as f64;

            let numerator = sum_xy - sum_x * mean_y;
            let denominator =
                ((sum_xx - sum_x * mean_x) * (sum_yy - sum_y * mean_y)).sqrt();

            if denominator == 0.0 {
                values.push(0.0);
                nulls.append_null();
            } else {
                values.push(numerator / denominator);
                nulls.append_non_null();
            }
        }

        Ok(Arc::new(Float64Array::new(values.into(), nulls.finish())))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let n = match emit_to {
            EmitTo::All => self.count.len(),
            EmitTo::First(n) => n,
        };

        Ok(vec![
            Arc::new(UInt64Array::from(self.count[0..n].to_vec())),
            Arc::new(Float64Array::from(self.sum_x[0..n].to_vec())),
            Arc::new(Float64Array::from(self.sum_y[0..n].to_vec())),
            Arc::new(Float64Array::from(self.sum_xy[0..n].to_vec())),
            Arc::new(Float64Array::from(self.sum_xx[0..n].to_vec())),
            Arc::new(Float64Array::from(self.sum_yy[0..n].to_vec())),
        ])
    }

    fn size(&self) -> usize {
        size_of_val(&self.count)
            + size_of_val(&self.sum_x)
            + size_of_val(&self.sum_y)
            + size_of_val(&self.sum_xy)
            + size_of_val(&self.sum_xx)
            + size_of_val(&self.sum_yy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, UInt64Array};

    #[test]
    fn test_accumulate_correlation_states() {
        // Test data
        let group_indices = vec![0, 1, 0, 1];
        let counts = UInt64Array::from(vec![1, 2, 3, 4]);
        let sum_x = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]);
        let sum_y = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]);
        let sum_xy = Float64Array::from(vec![10.0, 40.0, 90.0, 160.0]);
        let sum_xx = Float64Array::from(vec![100.0, 400.0, 900.0, 1600.0]);
        let sum_yy = Float64Array::from(vec![1.0, 4.0, 9.0, 16.0]);

        let mut accumulated = vec![];
        accumulate_correlation_states(
            &group_indices,
            (&counts, &sum_x, &sum_y, &sum_xy, &sum_xx, &sum_yy),
            |group_idx, count, values| {
                accumulated.push((group_idx, count, values.to_vec()));
            },
        );

        let expected = vec![
            (0, 1, vec![10.0, 1.0, 10.0, 100.0, 1.0]),
            (1, 2, vec![20.0, 2.0, 40.0, 400.0, 4.0]),
            (0, 3, vec![30.0, 3.0, 90.0, 900.0, 9.0]),
            (1, 4, vec![40.0, 4.0, 160.0, 1600.0, 16.0]),
        ];
        assert_eq!(accumulated, expected);

        // Test that function panics with null values
        let counts = UInt64Array::from(vec![Some(1), None, Some(3), Some(4)]);
        let sum_x = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]);
        let sum_y = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]);
        let sum_xy = Float64Array::from(vec![10.0, 40.0, 90.0, 160.0]);
        let sum_xx = Float64Array::from(vec![100.0, 400.0, 900.0, 1600.0]);
        let sum_yy = Float64Array::from(vec![1.0, 4.0, 9.0, 16.0]);

        let result = std::panic::catch_unwind(|| {
            accumulate_correlation_states(
                &group_indices,
                (&counts, &sum_x, &sum_y, &sum_xy, &sum_xx, &sum_yy),
                |_, _, _| {},
            )
        });
        assert!(result.is_err());
    }
}
