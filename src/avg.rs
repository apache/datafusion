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

use arrow::compute::sum;
use arrow_array::{
    builder::PrimitiveBuilder,
    cast::AsArray,
    types::{Float64Type, Int64Type},
    Array, ArrayRef, ArrowNumericType, Int64Array, PrimitiveArray,
};
use arrow_schema::{DataType, Field};
use datafusion::logical_expr::{
    type_coercion::aggregates::avg_return_type, Accumulator, EmitTo, GroupsAccumulator, Signature,
};
use datafusion_common::{not_impl_err, Result, ScalarValue};
use datafusion_physical_expr::expressions::format_state_name;
use std::{any::Any, sync::Arc};

use arrow_array::ArrowNativeTypeOp;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::Volatility::Immutable;
use datafusion_expr::{AggregateUDFImpl, ReversedUDAF};
use DataType::*;

/// AVG aggregate expression
#[derive(Debug, Clone)]
pub struct Avg {
    name: String,
    signature: Signature,
    // expr: Arc<dyn PhysicalExpr>,
    input_data_type: DataType,
    result_data_type: DataType,
}

impl Avg {
    /// Create a new AVG aggregate function
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        let result_data_type = avg_return_type("avg", &data_type).unwrap();

        Self {
            name: name.into(),
            signature: Signature::user_defined(Immutable),
            input_data_type: data_type,
            result_data_type,
        }
    }
}

impl AggregateUDFImpl for Avg {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // instantiate specialized accumulator based for the type
        match (&self.input_data_type, &self.result_data_type) {
            (Float64, Float64) => Ok(Box::<AvgAccumulator>::default()),
            _ => not_impl_err!(
                "AvgAccumulator for ({} --> {})",
                self.input_data_type,
                self.result_data_type
            ),
        }
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                format_state_name(&self.name, "sum"),
                self.input_data_type.clone(),
                true,
            ),
            Field::new(
                format_state_name(&self.name, "count"),
                DataType::Int64,
                true,
            ),
        ])
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        // instantiate specialized accumulator based for the type
        match (&self.input_data_type, &self.result_data_type) {
            (Float64, Float64) => Ok(Box::new(AvgGroupsAccumulator::<Float64Type, _>::new(
                &self.input_data_type,
                |sum: f64, count: i64| Ok(sum / count as f64),
            ))),

            _ => not_impl_err!(
                "AvgGroupsAccumulator for ({} --> {})",
                self.input_data_type,
                self.result_data_type
            ),
        }
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(None))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        avg_return_type(self.name(), &arg_types[0])
    }
}

/// An accumulator to compute the average
#[derive(Debug, Default)]
pub struct AvgAccumulator {
    sum: Option<f64>,
    count: i64,
}

impl Accumulator for AvgAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(self.sum),
            ScalarValue::from(self.count),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<Float64Type>();
        self.count += (values.len() - values.null_count()) as i64;
        let v = self.sum.get_or_insert(0.);
        if let Some(x) = sum(values) {
            *v += x;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // counts are summed
        self.count += sum(states[1].as_primitive::<Int64Type>()).unwrap_or_default();

        // sums are summed
        if let Some(x) = sum(states[0].as_primitive::<Float64Type>()) {
            let v = self.sum.get_or_insert(0.);
            *v += x;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.count == 0 {
            // If all input are nulls, count will be 0 and we will get null after the division.
            // This is consistent with Spark Average implementation.
            Ok(ScalarValue::Float64(None))
        } else {
            Ok(ScalarValue::Float64(
                self.sum.map(|f| f / self.count as f64),
            ))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// An accumulator to compute the average of `[PrimitiveArray<T>]`.
/// Stores values as native types, and does overflow checking
///
/// F: Function that calculates the average value from a sum of
/// T::Native and a total count
#[derive(Debug)]
struct AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, i64) -> Result<T::Native> + Send,
{
    /// The type of the returned average
    return_data_type: DataType,

    /// Count per group (use i64 to make Int64Array)
    counts: Vec<i64>,

    /// Sums per group, stored as the native type
    sums: Vec<T::Native>,

    /// Function that computes the final average (value / count)
    avg_fn: F,
}

impl<T, F> AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, i64) -> Result<T::Native> + Send,
{
    pub fn new(return_data_type: &DataType, avg_fn: F) -> Self {
        Self {
            return_data_type: return_data_type.clone(),
            counts: vec![],
            sums: vec![],
            avg_fn,
        }
    }
}

impl<T, F> GroupsAccumulator for AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, i64) -> Result<T::Native> + Send,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<T>();
        let data = values.values();

        // increment counts, update sums
        self.counts.resize(total_num_groups, 0);
        self.sums.resize(total_num_groups, T::default_value());

        let iter = group_indices.iter().zip(data.iter());
        if values.null_count() == 0 {
            for (&group_index, &value) in iter {
                let sum = &mut self.sums[group_index];
                *sum = (*sum).add_wrapping(value);
                self.counts[group_index] += 1;
            }
        } else {
            for (idx, (&group_index, &value)) in iter.enumerate() {
                if values.is_null(idx) {
                    continue;
                }
                let sum = &mut self.sums[group_index];
                *sum = (*sum).add_wrapping(value);

                self.counts[group_index] += 1;
            }
        }

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "two arguments to merge_batch");
        // first batch is partial sums, second is counts
        let partial_sums = values[0].as_primitive::<T>();
        let partial_counts = values[1].as_primitive::<Int64Type>();
        // update counts with partial counts
        self.counts.resize(total_num_groups, 0);
        let iter1 = group_indices.iter().zip(partial_counts.values().iter());
        for (&group_index, &partial_count) in iter1 {
            self.counts[group_index] += partial_count;
        }

        // update sums
        self.sums.resize(total_num_groups, T::default_value());
        let iter2 = group_indices.iter().zip(partial_sums.values().iter());
        for (&group_index, &new_value) in iter2 {
            let sum = &mut self.sums[group_index];
            *sum = sum.add_wrapping(new_value);
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.counts);
        let sums = emit_to.take_needed(&mut self.sums);
        let mut builder = PrimitiveBuilder::<T>::with_capacity(sums.len());
        let iter = sums.into_iter().zip(counts);

        for (sum, count) in iter {
            if count != 0 {
                builder.append_value((self.avg_fn)(sum, count)?)
            } else {
                builder.append_null();
            }
        }
        let array: PrimitiveArray<T> = builder.finish();

        Ok(Arc::new(array))
    }

    // return arrays for sums and counts
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let counts = emit_to.take_needed(&mut self.counts);
        let counts = Int64Array::new(counts.into(), None);

        let sums = emit_to.take_needed(&mut self.sums);
        let sums = PrimitiveArray::<T>::new(sums.into(), None)
            .with_data_type(self.return_data_type.clone());

        Ok(vec![
            Arc::new(sums) as ArrayRef,
            Arc::new(counts) as ArrayRef,
        ])
    }

    fn size(&self) -> usize {
        self.counts.capacity() * std::mem::size_of::<i64>()
            + self.sums.capacity() * std::mem::size_of::<T>()
    }
}
