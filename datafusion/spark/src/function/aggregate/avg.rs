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

use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowNumericType, BooleanArray, Int64Array,
    PrimitiveArray,
    builder::PrimitiveBuilder,
    cast::AsArray,
    types::{Float64Type, Int64Type},
};
use arrow::compute::sum;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::types::{NativeType, logical_float64};
use datafusion_common::{Result, ScalarValue, not_impl_err};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Coercion, EmitTo, GroupsAccumulator, ReversedUDAF,
    Signature, TypeSignatureClass, Volatility,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::nulls::{
    filtered_null_mask, set_nulls,
};
use std::sync::Arc;

/// AVG aggregate expression
/// Spark average aggregate expression. Differs from standard DataFusion average aggregate
/// in that it uses an `i64` for the count (DataFusion version uses `u64`); also there is ANSI mode
/// support planned in the future for Spark version.

// TODO: see if can deduplicate with DF version
//       https://github.com/apache/datafusion/issues/17964
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkAvg {
    signature: Signature,
}

impl Default for SparkAvg {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkAvg {
    /// Implement AVG aggregate function
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_implicit(
                    TypeSignatureClass::Native(logical_float64()),
                    vec![TypeSignatureClass::Numeric],
                    NativeType::Float64,
                )],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for SparkAvg {
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return not_impl_err!("DistinctAvgAccumulator");
        }

        let data_type = acc_args.exprs[0].data_type(acc_args.schema)?;

        // instantiate specialized accumulator based for the type
        match (&data_type, &acc_args.return_type()) {
            (DataType::Float64, DataType::Float64) => {
                Ok(Box::<AvgAccumulator>::default())
            }
            (dt, return_type) => {
                not_impl_err!("AvgAccumulator for ({dt} --> {return_type})")
            }
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(self.name(), "sum"),
                args.input_fields[0].data_type().clone(),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(self.name(), "count"),
                DataType::Int64,
                true,
            )),
        ])
    }

    fn name(&self) -> &str {
        "avg"
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let data_type = args.exprs[0].data_type(args.schema)?;

        // instantiate specialized accumulator based for the type
        match (&data_type, args.return_type()) {
            (DataType::Float64, DataType::Float64) => {
                Ok(Box::new(AvgGroupsAccumulator::<Float64Type, _>::new(
                    args.return_field.data_type(),
                    |sum: f64, count: i64| Ok(sum / count as f64),
                )))
            }
            (dt, return_type) => {
                not_impl_err!("AvgGroupsAccumulator for ({dt} --> {return_type})")
            }
        }
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(None))
    }

    fn signature(&self) -> &Signature {
        &self.signature
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
        size_of_val(self)
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
    F: Fn(T::Native, i64) -> Result<T::Native> + Send + 'static,
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
    F: Fn(T::Native, i64) -> Result<T::Native> + Send + 'static,
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
    F: Fn(T::Native, i64) -> Result<T::Native> + Send + 'static,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&BooleanArray>,
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
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "two arguments to merge_batch");
        // first batch is partial sums, second is counts
        let partial_sums = values[0].as_primitive::<T>();
        let partial_counts = values[1].as_primitive::<Int64Type>();

        self.counts.resize(total_num_groups, 0);
        self.sums.resize(total_num_groups, T::default_value());

        for (idx, &group_index) in group_indices.iter().enumerate() {
            // Skip null state entries emitted by convert_to_state for
            // filtered / null input rows.
            if partial_counts.is_null(idx) || partial_sums.is_null(idx) {
                continue;
            }
            self.counts[group_index] += partial_counts.value(idx);
            let sum = &mut self.sums[group_index];
            *sum = sum.add_wrapping(partial_sums.value(idx));
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

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let sums = values[0]
            .as_primitive::<T>()
            .clone()
            .with_data_type(self.return_data_type.clone());
        let counts = Int64Array::from_value(1, sums.len());

        let nulls = filtered_null_mask(opt_filter, &sums);
        let counts = set_nulls(counts, nulls.clone());
        let sums = set_nulls(sums, nulls);

        // [sum, count] - must match state() and merge_batch()
        Ok(vec![
            Arc::new(sums) as ArrayRef,
            Arc::new(counts) as ArrayRef,
        ])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.counts.capacity() * size_of::<i64>() + self.sums.capacity() * size_of::<T>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;

    fn make_acc() -> AvgGroupsAccumulator<Float64Type, impl Fn(f64, i64) -> Result<f64>> {
        AvgGroupsAccumulator::<Float64Type, _>::new(&DataType::Float64, |sum, count| {
            Ok(sum / count as f64)
        })
    }

    #[test]
    fn supports_convert_to_state() {
        assert!(make_acc().supports_convert_to_state());
    }

    #[test]
    fn convert_to_state_basic() {
        let acc = make_acc();
        let values: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]))];
        let state = acc.convert_to_state(&values, None).unwrap();

        assert_eq!(state.len(), 2);
        let sums = state[0].as_primitive::<Float64Type>();
        let counts = state[1].as_primitive::<Int64Type>();

        assert_eq!(sums.values().as_ref(), &[1.0, 2.0, 3.0]);
        assert_eq!(counts.values().as_ref(), &[1, 1, 1]);
        assert_eq!(sums.null_count(), 0);
        assert_eq!(counts.null_count(), 0);
    }

    #[test]
    fn convert_to_state_with_nulls() {
        let acc = make_acc();
        let values: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(3.0),
        ]))];
        let state = acc.convert_to_state(&values, None).unwrap();

        let sums = state[0].as_primitive::<Float64Type>();
        let counts = state[1].as_primitive::<Int64Type>();

        assert!(!sums.is_null(0));
        assert!(sums.is_null(1));
        assert!(!sums.is_null(2));

        assert_eq!(counts.value(0), 1);
        assert!(counts.is_null(1));
        assert_eq!(counts.value(2), 1);
    }

    #[test]
    fn convert_to_state_with_filter() {
        let acc = make_acc();
        let values: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]))];
        let filter = BooleanArray::from(vec![true, false, true]);
        let state = acc.convert_to_state(&values, Some(&filter)).unwrap();

        let sums = state[0].as_primitive::<Float64Type>();
        let counts = state[1].as_primitive::<Int64Type>();

        assert!(!sums.is_null(0));
        assert!(sums.is_null(1));
        assert!(!sums.is_null(2));

        assert_eq!(counts.value(0), 1);
        assert!(counts.is_null(1));
        assert_eq!(counts.value(2), 1);
    }

    #[test]
    fn convert_to_state_roundtrips_through_merge() {
        let mut acc = make_acc();
        let input: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0]))];
        let state = acc.convert_to_state(&input, None).unwrap();

        // feed the converted state back through merge_batch
        acc.merge_batch(
            &state,
            &[0, 0, 0],
            None,
            1, // single group
        )
        .unwrap();

        let result = acc.evaluate(EmitTo::All).unwrap();
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.value(0), 20.0); // (10+20+30)/3
    }

    #[test]
    fn convert_to_state_null_merge_matches_direct() {
        // avg([1.0, NULL, 3.0]) must be 2.0 after a convert_to_state → merge_batch
        // round-trip. Before the merge-path null fix this leaked the backing
        // buffer value at the null slot and produced the wrong average.
        let mut acc = make_acc();
        let input: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(3.0),
        ]))];
        let state = acc.convert_to_state(&input, None).unwrap();
        acc.merge_batch(&state, &[0, 0, 0], None, 1).unwrap();

        let result = acc.evaluate(EmitTo::All).unwrap();
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.value(0), 2.0);
    }
}
