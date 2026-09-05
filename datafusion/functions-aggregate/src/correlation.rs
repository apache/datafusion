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

use std::fmt::Debug;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{
    Array, AsArray, BooleanArray, Float64Array, NullBufferBuilder, UInt64Array,
    downcast_array,
};
use arrow::compute::{and, filter, is_not_null};
use arrow::datatypes::{FieldRef, Float64Type, UInt64Type};
use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field},
};
use datafusion_expr::{EmitTo, GroupSelection, GroupsAccumulator};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::accumulate::accumulate_multiple;
use log::debug;

use crate::covariance::CovarianceAccumulator;
use crate::stddev::StddevAccumulator;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Signature, Volatility,
    function::{AccumulatorArgs, StateFieldsArgs},
    utils::format_state_name,
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
            )
            .with_parameter_names(vec!["y".to_string(), "x".to_string()])
            .expect("valid parameter names for corr"),
        }
    }
}

impl AggregateUDFImpl for Correlation {
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
        let covar = self.covar.evaluate()?;
        let stddev1 = self.stddev1.evaluate()?;
        let stddev2 = self.stddev2.evaluate()?;

        // First check if we have NaN values by examining the internal state
        // This handles the case where both inputs are NaN even with count=1
        let mean1 = self.covar.get_mean1();
        let mean2 = self.covar.get_mean2();

        // If both means are NaN, then both input columns contain only NaN values
        if mean1.is_nan() && mean2.is_nan() {
            return Ok(ScalarValue::Float64(Some(f64::NAN)));
        }
        let n = self.covar.get_count();
        if mean1.is_nan() || mean2.is_nan() || n < 2 {
            return Ok(ScalarValue::Float64(None));
        }

        if let ScalarValue::Float64(Some(c)) = covar
            && let ScalarValue::Float64(Some(s1)) = stddev1
            && let ScalarValue::Float64(Some(s2)) = stddev2
        {
            if s1 == 0_f64 || s2 == 0_f64 {
                return Ok(ScalarValue::Float64(None));
            } else {
                return Ok(ScalarValue::Float64(Some(c / s1 / s2)));
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

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[derive(Default)]
pub struct CorrelationGroupsAccumulator {
    // Number of elements for each group
    // This is also used to track nulls: if a group has 0 valid values accumulated,
    // final aggregation result will be null.
    count: Vec<u64>,
    // Means and centered moments, in the same order as the scalar state.
    mean_x: Vec<f64>,
    m2_x: Vec<f64>,
    mean_y: Vec<f64>,
    m2_y: Vec<f64>,
    co_moment: Vec<f64>,
}

fn copy_selected<T: Copy>(selection: GroupSelection<'_>, values: &[T]) -> Vec<T> {
    debug_assert_eq!(selection.total_num_groups(), values.len());
    selection.iter().map(|index| values[index]).collect()
}

impl CorrelationGroupsAccumulator {
    pub fn new() -> Self {
        Default::default()
    }

    fn evaluate_values(
        counts: &[u64],
        mean_xs: &[f64],
        m2_xs: &[f64],
        mean_ys: &[f64],
        m2_ys: &[f64],
        co_moments: &[f64],
    ) -> ArrayRef {
        let n = counts.len();
        let mut values = Vec::with_capacity(n);
        let mut nulls = NullBufferBuilder::new(n);

        for i in 0..n {
            let count = counts[i];
            let mean_x = mean_xs[i];
            let mean_y = mean_ys[i];

            // If both inputs are NaN, return NaN. If only one input is NaN,
            // or there are too few values, return NULL.
            if mean_x.is_nan() && mean_y.is_nan() {
                values.push(f64::NAN);
                nulls.append_non_null();
                continue;
            } else if count < 2 || mean_x.is_nan() || mean_y.is_nan() {
                values.push(0.0);
                nulls.append_null();
                continue;
            }

            let count = count as f64;
            let covariance = co_moments[i] / count;
            let stddev_x = (m2_xs[i] / count).sqrt();
            let stddev_y = (m2_ys[i] / count).sqrt();

            if stddev_x == 0.0 || stddev_y == 0.0 {
                values.push(0.0);
                nulls.append_null();
            } else {
                values.push(covariance / stddev_x / stddev_y);
                nulls.append_non_null();
            }
        }

        Arc::new(Float64Array::new(values.into(), nulls.finish()))
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
        &Float64Array, // mean_x
        &Float64Array, // m2_x
        &Float64Array, // mean_y
        &Float64Array, // m2_y
        &Float64Array, // co_moment
    ),
    mut value_fn: impl FnMut(usize, u64, &[f64]),
) {
    let (counts, mean_x, m2_x, mean_y, m2_y, co_moment) = state_arrays;

    assert_eq!(counts.null_count(), 0);
    assert_eq!(mean_x.null_count(), 0);
    assert_eq!(m2_x.null_count(), 0);
    assert_eq!(mean_y.null_count(), 0);
    assert_eq!(m2_y.null_count(), 0);
    assert_eq!(co_moment.null_count(), 0);

    let counts_values = counts.values().as_ref();
    let mean_x_values = mean_x.values().as_ref();
    let m2_x_values = m2_x.values().as_ref();
    let mean_y_values = mean_y.values().as_ref();
    let m2_y_values = m2_y.values().as_ref();
    let co_moment_values = co_moment.values().as_ref();

    for (idx, &group_idx) in group_indices.iter().enumerate() {
        let row = [
            mean_x_values[idx],
            m2_x_values[idx],
            mean_y_values[idx],
            m2_y_values[idx],
            co_moment_values[idx],
        ];
        value_fn(group_idx, counts_values[idx], &row);
    }
}

/// GroupsAccumulator implementation for `corr(x, y)` that computes the Pearson correlation coefficient
/// between two numeric columns.
///
/// Uses paired Welford updates and merges centered moments to avoid cancellation
/// when input values have large offsets. Its state matches `CorrelationAccumulator`.
impl GroupsAccumulator for CorrelationGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.count.resize(total_num_groups, 0);
        self.mean_x.resize(total_num_groups, 0.0);
        self.m2_x.resize(total_num_groups, 0.0);
        self.mean_y.resize(total_num_groups, 0.0);
        self.m2_y.resize(total_num_groups, 0.0);
        self.co_moment.resize(total_num_groups, 0.0);

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
                let count = self.count[group_index] as f64;
                let delta_x = x - self.mean_x[group_index];
                let delta_y = y - self.mean_y[group_index];
                self.mean_x[group_index] += delta_x / count;
                self.mean_y[group_index] += delta_y / count;
                self.m2_x[group_index] += delta_x * (x - self.mean_x[group_index]);
                self.m2_y[group_index] += delta_y * (y - self.mean_y[group_index]);
                self.co_moment[group_index] += delta_x * (y - self.mean_y[group_index]);
            },
        );

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        Ok(Self::evaluate_values(
            &emit_to.take_needed(&mut self.count),
            &emit_to.take_needed(&mut self.mean_x),
            &emit_to.take_needed(&mut self.m2_x),
            &emit_to.take_needed(&mut self.mean_y),
            &emit_to.take_needed(&mut self.m2_y),
            &emit_to.take_needed(&mut self.co_moment),
        ))
    }

    fn evaluate_preserving(&mut self, selection: GroupSelection<'_>) -> Result<ArrayRef> {
        selection.validate_num_groups(self.count.len())?;
        Ok(Self::evaluate_values(
            &copy_selected(selection, &self.count),
            &copy_selected(selection, &self.mean_x),
            &copy_selected(selection, &self.m2_x),
            &copy_selected(selection, &self.mean_y),
            &copy_selected(selection, &self.m2_y),
            &copy_selected(selection, &self.co_moment),
        ))
    }

    fn supports_evaluate_preserving(&self) -> bool {
        true
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        // Drain the state vectors for the groups being emitted
        let count = emit_to.take_needed(&mut self.count);
        let mean_x = emit_to.take_needed(&mut self.mean_x);
        let m2_x = emit_to.take_needed(&mut self.m2_x);
        let mean_y = emit_to.take_needed(&mut self.mean_y);
        let m2_y = emit_to.take_needed(&mut self.m2_y);
        let co_moment = emit_to.take_needed(&mut self.co_moment);

        Ok(vec![
            Arc::new(UInt64Array::from(count)),
            Arc::new(Float64Array::from(mean_x)),
            Arc::new(Float64Array::from(m2_x)),
            Arc::new(Float64Array::from(mean_y)),
            Arc::new(Float64Array::from(m2_y)),
            Arc::new(Float64Array::from(co_moment)),
        ])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        assert_eq!(values.len(), 2, "two arguments to convert_to_state");
        let array_x = downcast_array::<Float64Array>(&values[0]);
        let array_y = downcast_array::<Float64Array>(&values[1]);

        let len = array_x.len();
        let mut counts = Vec::with_capacity(len);
        let mut mean_x = Vec::with_capacity(len);
        let mut m2_x = Vec::with_capacity(len);
        let mut mean_y = Vec::with_capacity(len);
        let mut m2_y = Vec::with_capacity(len);
        let mut co_moment = Vec::with_capacity(len);

        for row in 0..len {
            let included = array_x.is_valid(row)
                && array_y.is_valid(row)
                && opt_filter
                    .is_none_or(|filter| filter.is_valid(row) && filter.value(row));
            if included {
                let x = array_x.value(row);
                let y = array_y.value(row);
                counts.push(1);
                mean_x.push(x);
                m2_x.push(0.0);
                mean_y.push(y);
                m2_y.push(0.0);
                co_moment.push(0.0);
            } else {
                counts.push(0);
                mean_x.push(0.0);
                m2_x.push(0.0);
                mean_y.push(0.0);
                m2_y.push(0.0);
                co_moment.push(0.0);
            }
        }

        Ok(vec![
            Arc::new(UInt64Array::from(counts)),
            Arc::new(Float64Array::from(mean_x)),
            Arc::new(Float64Array::from(m2_x)),
            Arc::new(Float64Array::from(mean_y)),
            Arc::new(Float64Array::from(m2_y)),
            Arc::new(Float64Array::from(co_moment)),
        ])
    }
    fn state_preserving(
        &mut self,
        selection: GroupSelection<'_>,
    ) -> Result<Vec<ArrayRef>> {
        selection.validate_num_groups(self.count.len())?;
        Ok(vec![
            Arc::new(UInt64Array::from(copy_selected(selection, &self.count))),
            Arc::new(Float64Array::from(copy_selected(selection, &self.mean_x))),
            Arc::new(Float64Array::from(copy_selected(selection, &self.m2_x))),
            Arc::new(Float64Array::from(copy_selected(selection, &self.mean_y))),
            Arc::new(Float64Array::from(copy_selected(selection, &self.m2_y))),
            Arc::new(Float64Array::from(copy_selected(
                selection,
                &self.co_moment,
            ))),
        ])
    }

    fn supports_state_preserving(&self) -> bool {
        true
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        // Resize vectors to accommodate total number of groups
        self.count.resize(total_num_groups, 0);
        self.mean_x.resize(total_num_groups, 0.0);
        self.m2_x.resize(total_num_groups, 0.0);
        self.mean_y.resize(total_num_groups, 0.0);
        self.m2_y.resize(total_num_groups, 0.0);
        self.co_moment.resize(total_num_groups, 0.0);

        // Extract arrays from input values
        let partial_counts = values[0].as_primitive::<UInt64Type>();
        let partial_mean_x = values[1].as_primitive::<Float64Type>();
        let partial_m2_x = values[2].as_primitive::<Float64Type>();
        let partial_mean_y = values[3].as_primitive::<Float64Type>();
        let partial_m2_y = values[4].as_primitive::<Float64Type>();
        let partial_co_moment = values[5].as_primitive::<Float64Type>();

        accumulate_correlation_states(
            group_indices,
            (
                partial_counts,
                partial_mean_x,
                partial_m2_x,
                partial_mean_y,
                partial_m2_y,
                partial_co_moment,
            ),
            |group_index, count, values| {
                if count == 0 {
                    return;
                }
                let old_count = self.count[group_index];
                if old_count == 0 {
                    self.count[group_index] = count;
                    self.mean_x[group_index] = values[0];
                    self.m2_x[group_index] = values[1];
                    self.mean_y[group_index] = values[2];
                    self.m2_y[group_index] = values[3];
                    self.co_moment[group_index] = values[4];
                    return;
                }

                let new_count = old_count + count;
                let delta_x = values[0] - self.mean_x[group_index];
                let delta_y = values[2] - self.mean_y[group_index];
                let weight = count as f64 / new_count as f64;
                let correction = old_count as f64 * weight;
                self.count[group_index] = new_count;
                self.mean_x[group_index] += delta_x * weight;
                self.mean_y[group_index] += delta_y * weight;
                // Apply the weight before multiplying deltas to avoid overflow
                // when the merged centered moment is still representable.
                self.m2_x[group_index] += values[1] + delta_x * (delta_x * correction);
                self.m2_y[group_index] += values[3] + delta_y * (delta_y * correction);
                self.co_moment[group_index] +=
                    values[4] + delta_x * (delta_y * correction);
            },
        );

        Ok(())
    }

    fn size(&self) -> usize {
        self.count.capacity() * size_of::<u64>()
            + self.mean_x.capacity() * size_of::<f64>()
            + self.m2_x.capacity() * size_of::<f64>()
            + self.mean_y.capacity() * size_of::<f64>()
            + self.m2_y.capacity() * size_of::<f64>()
            + self.co_moment.capacity() * size_of::<f64>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correlation_groups_large_offsets() -> Result<()> {
        let values: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![
                1e9,
                1e9,
                1e9 + 7.0,
                1e9 + 7.0,
                1e9 + 15.0,
                1e9 + 15.0,
            ])),
            Arc::new(Float64Array::from(vec![
                2e9,
                -2e9,
                2e9 + 14.0,
                -2e9 - 14.0,
                2e9 + 30.0,
                -2e9 - 30.0,
            ])),
        ];
        let group_indices = [0, 1, 0, 1, 0, 1];
        for batch_size in 1..=6 {
            let mut direct = CorrelationGroupsAccumulator::new();
            let mut merged = CorrelationGroupsAccumulator::new();
            let mut converted = CorrelationGroupsAccumulator::new();
            for start in (0..6).step_by(batch_size) {
                let len = batch_size.min(6 - start);
                let batch: Vec<_> = values.iter().map(|a| a.slice(start, len)).collect();
                let groups = &group_indices[start..start + len];
                direct.update_batch(&batch, groups, None, 2)?;

                let mut partial = CorrelationGroupsAccumulator::new();
                partial.update_batch(&batch, groups, None, 2)?;
                merged.merge_batch(&partial.state(EmitTo::All)?, &[0, 1], 2)?;
                converted.merge_batch(
                    &partial.convert_to_state(&batch, None)?,
                    groups,
                    2,
                )?;
            }
            for mut accumulator in [direct, merged, converted] {
                let result = accumulator.evaluate(EmitTo::All)?;
                let result = result.as_primitive::<Float64Type>();
                assert_eq!(result.null_count(), 0);
                for (actual, expected) in result.values().iter().zip([1.0, -1.0]) {
                    assert!((actual - expected).abs() < 1e-12, "{result:?}");
                }
            }
        }
        Ok(())
    }

    #[test]
    fn correlation_groups_merge_large_range() -> Result<()> {
        let values: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![-8e153, 8e153])),
            Arc::new(Float64Array::from(vec![-0.5, 0.5])),
        ];
        let mut accumulator = CorrelationGroupsAccumulator::new();
        let states = accumulator.convert_to_state(&values, None)?;
        accumulator.merge_batch(&states, &[0, 0], 1)?;
        let result = accumulator.evaluate(EmitTo::All)?;
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.null_count(), 0);
        assert!((result.value(0) - 1.0).abs() < 1e-12, "{result:?}");
        Ok(())
    }

    #[test]
    fn correlation_scalar_and_grouped_states_are_compatible() -> Result<()> {
        let values: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![1e9, 1e9 + 7.0, 1e9 + 15.0])),
            Arc::new(Float64Array::from(vec![3.0, 10.0, 18.0])),
        ];
        let mut scalar = CorrelationAccumulator::try_new()?;
        scalar.update_batch(&values)?;
        let ScalarValue::Float64(Some(expected)) = scalar.evaluate()? else {
            panic!("expected a non-null correlation");
        };
        let state = scalar
            .state()?
            .iter()
            .map(|value| value.to_array_of_size(1))
            .collect::<Result<Vec<_>>>()?;
        let mut grouped = CorrelationGroupsAccumulator::new();
        grouped.merge_batch(&state, &[0], 1)?;
        let result = grouped.evaluate(EmitTo::All)?;
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.null_count(), 0);
        assert!((result.value(0) - expected).abs() < 1e-12);

        grouped.update_batch(&values, &[0, 0, 0], None, 1)?;
        let mut scalar = CorrelationAccumulator::try_new()?;
        scalar.merge_batch(&grouped.state(EmitTo::All)?)?;
        let ScalarValue::Float64(Some(result)) = scalar.evaluate()? else {
            panic!("expected a non-null correlation");
        };
        assert!((result - expected).abs() < 1e-12);
        Ok(())
    }

    #[test]
    fn test_accumulate_correlation_states() {
        // Test data
        let group_indices = vec![0, 1, 0, 1];
        let counts = UInt64Array::from(vec![1, 2, 3, 4]);
        let mean_x = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]);
        let m2_x = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]);
        let mean_y = Float64Array::from(vec![10.0, 40.0, 90.0, 160.0]);
        let m2_y = Float64Array::from(vec![100.0, 400.0, 900.0, 1600.0]);
        let co_moment = Float64Array::from(vec![1.0, 4.0, 9.0, 16.0]);

        let mut accumulated = vec![];
        accumulate_correlation_states(
            &group_indices,
            (&counts, &mean_x, &m2_x, &mean_y, &m2_y, &co_moment),
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
        let mean_x = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]);
        let m2_x = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]);
        let mean_y = Float64Array::from(vec![10.0, 40.0, 90.0, 160.0]);
        let m2_y = Float64Array::from(vec![100.0, 400.0, 900.0, 1600.0]);
        let co_moment = Float64Array::from(vec![1.0, 4.0, 9.0, 16.0]);

        let result = std::panic::catch_unwind(|| {
            accumulate_correlation_states(
                &group_indices,
                (&counts, &mean_x, &m2_x, &mean_y, &m2_y, &co_moment),
                |_, _, _| {},
            )
        });
        assert!(result.is_err());
    }

    #[test]
    fn correlation_groups_preserving_reads() -> Result<()> {
        let mut accumulator = CorrelationGroupsAccumulator::new();
        let x = Arc::new(Float64Array::from(vec![1.0, 2.0, 1.0, 1.0, 2.0]));
        let y = Arc::new(Float64Array::from(vec![2.0, 4.0, 2.0, 3.0, 1.0]));
        accumulator.update_batch(&[x, y], &[0, 0, 1, 2, 2], None, 4)?;

        let selection = GroupSelection::try_from_indices(&[2, 0, 3, 2], 4)?;
        let expected = Float64Array::from(vec![Some(-1.0), Some(1.0), None, Some(-1.0)]);
        for _ in 0..2 {
            assert_eq!(
                accumulator
                    .evaluate_preserving(selection)?
                    .as_primitive::<Float64Type>(),
                &expected
            );
            let state = accumulator.state_preserving(selection)?;
            assert_eq!(state.len(), 6);
            assert_eq!(
                state[0].as_primitive::<UInt64Type>(),
                &UInt64Array::from(vec![2, 2, 0, 2])
            );
            assert_eq!(
                state[1].as_primitive::<Float64Type>(),
                &Float64Array::from(vec![1.5, 1.5, 0.0, 1.5])
            );
            assert_eq!(
                state[2].as_primitive::<Float64Type>(),
                &Float64Array::from(vec![0.5, 0.5, 0.0, 0.5])
            );
            assert_eq!(
                state[3].as_primitive::<Float64Type>(),
                &Float64Array::from(vec![2.0, 3.0, 0.0, 2.0])
            );
            assert_eq!(
                state[4].as_primitive::<Float64Type>(),
                &Float64Array::from(vec![2.0, 2.0, 0.0, 2.0])
            );
            assert_eq!(
                state[5].as_primitive::<Float64Type>(),
                &Float64Array::from(vec![-1.0, 1.0, 0.0, -1.0])
            );
        }

        let empty_selection = GroupSelection::try_from_indices(&[], 4)?;
        assert!(accumulator.evaluate_preserving(empty_selection)?.is_empty());
        assert!(
            accumulator
                .state_preserving(empty_selection)?
                .iter()
                .all(|array| array.is_empty())
        );

        let x = Arc::new(Float64Array::from(vec![2.0, 1.0, 4.0]));
        let y = Arc::new(Float64Array::from(vec![4.0, 2.0, 8.0]));
        accumulator.update_batch(&[x, y], &[1, 3, 3], None, 4)?;
        assert_eq!(
            accumulator
                .evaluate_preserving(GroupSelection::all(4))?
                .as_primitive::<Float64Type>(),
            &Float64Array::from(vec![1.0, 1.0, -1.0, 1.0])
        );
        Ok(())
    }

    #[test]
    fn convert_to_state_roundtrips_through_merge() -> Result<()> {
        let x = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(2.0),
            None,
            Some(4.0),
            Some(8.0),
            Some(16.0),
            Some(32.0),
        ])) as ArrayRef;
        let y = Arc::new(Float64Array::from(vec![
            Some(2.0),
            Some(4.0),
            Some(6.0),
            None,
            Some(16.0),
            Some(32.0),
            Some(64.0),
        ])) as ArrayRef;
        let filter = BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
            Some(true),
            None,
            Some(true),
            Some(true),
        ]);
        let values = vec![x, y];
        let group_indices = vec![0, 1, 0, 1, 0, 0, 0];

        let mut direct = CorrelationGroupsAccumulator::new();
        direct.update_batch(&values, &group_indices, Some(&filter), 2)?;
        let direct = direct.evaluate(EmitTo::All)?;

        let converter = CorrelationGroupsAccumulator::new();
        let state = converter.convert_to_state(&values, Some(&filter))?;
        let mut merged = CorrelationGroupsAccumulator::new();
        merged.merge_batch(&state, &group_indices, 2)?;
        let merged = merged.evaluate(EmitTo::All)?;

        let direct = direct.as_primitive::<Float64Type>();
        let merged = merged.as_primitive::<Float64Type>();
        assert_eq!(direct.nulls(), merged.nulls());
        for (direct, merged) in direct.iter().zip(merged.iter()) {
            if let (Some(direct), Some(merged)) = (direct, merged) {
                assert!((direct - merged).abs() < 1e-12);
            }
        }
        Ok(())
    }

    #[test]
    fn convert_to_state_preserves_empty_and_filtered_rows() -> Result<()> {
        let converter = CorrelationGroupsAccumulator::new();
        let empty_values = vec![
            Arc::new(Float64Array::from(Vec::<Option<f64>>::new())) as ArrayRef,
            Arc::new(Float64Array::from(Vec::<Option<f64>>::new())) as ArrayRef,
        ];
        let state = converter.convert_to_state(&empty_values, None)?;
        for state_array in &state {
            assert_eq!(state_array.len(), 0);
            assert_eq!(state_array.null_count(), 0);
        }

        let values = vec![
            Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0), None])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(2.0), None, Some(4.0)])) as ArrayRef,
        ];
        let filter = BooleanArray::from(vec![Some(false), None, Some(false)]);
        let group_indices = vec![0, 1, 0];
        let state = converter.convert_to_state(&values, Some(&filter))?;
        for state_array in &state {
            assert_eq!(state_array.len(), values[0].len());
            assert_eq!(state_array.null_count(), 0);
        }

        let counts = state[0].as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(counts, &UInt64Array::from(vec![0, 0, 0]));

        let mut merged = CorrelationGroupsAccumulator::new();
        merged.merge_batch(&state, &group_indices, 2)?;
        let result = merged.evaluate(EmitTo::All)?;
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.null_count(), 2);
        Ok(())
    }
}
