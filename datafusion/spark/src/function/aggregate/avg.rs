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
    Array, ArrayRef,
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
    Accumulator, AggregateUDFImpl, Coercion, GroupsAccumulator, ReversedUDAF, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_functions_aggregate::average::spark_avg_groups_accumulator;
use std::mem::size_of_val;
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
                Ok(spark_avg_groups_accumulator(args.return_field.data_type()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::types::Int64Type;
    use arrow::array::{Array, BooleanArray, Float64Array, PrimitiveArray};
    use datafusion_expr::EmitTo;

    fn make_acc() -> Box<dyn GroupsAccumulator> {
        spark_avg_groups_accumulator(&DataType::Float64)
    }

    fn assert_validity(array: &dyn Array, expected: &[bool]) {
        assert_eq!(array.len(), expected.len());
        for (idx, expected_valid) in expected.iter().copied().enumerate() {
            assert_eq!(!array.is_null(idx), expected_valid, "validity at row {idx}");
        }
    }

    fn spark_avg_state(
        state: &[ArrayRef],
    ) -> (&PrimitiveArray<Float64Type>, &PrimitiveArray<Int64Type>) {
        assert_eq!(state.len(), 2);
        (
            state[0].as_primitive::<Float64Type>(),
            state[1].as_primitive::<Int64Type>(),
        )
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

        let (sums, counts) = spark_avg_state(&state);

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

        let (sums, counts) = spark_avg_state(&state);

        assert_validity(sums, &[true, false, true]);

        assert_eq!(counts.value(0), 1);
        assert_validity(counts, &[true, false, true]);
        assert_eq!(counts.value(2), 1);
    }

    #[test]
    fn convert_to_state_with_filter() {
        let acc = make_acc();
        let values: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]))];
        let filter = BooleanArray::from(vec![true, false, true]);
        let state = acc.convert_to_state(&values, Some(&filter)).unwrap();

        let (sums, counts) = spark_avg_state(&state);

        assert_validity(sums, &[true, false, true]);

        assert_eq!(counts.value(0), 1);
        assert_validity(counts, &[true, false, true]);
        assert_eq!(counts.value(2), 1);
    }

    #[test]
    fn convert_to_state_with_null_filter() {
        let acc = make_acc();
        let values: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]))];
        let filter = BooleanArray::from(vec![Some(true), None, Some(true)]);
        let state = acc.convert_to_state(&values, Some(&filter)).unwrap();

        let (sums, counts) = spark_avg_state(&state);

        assert_validity(sums, &[true, false, true]);

        assert_eq!(counts.value(0), 1);
        assert_validity(counts, &[true, false, true]);
        assert_eq!(counts.value(2), 1);
    }

    #[test]
    fn merge_batch_applies_filter() {
        let mut acc = make_acc();
        let input: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0]))];
        let state = acc.convert_to_state(&input, None).unwrap();
        let filter = BooleanArray::from(vec![Some(true), Some(false), None]);

        acc.merge_batch(&state, &[0, 0, 0], Some(&filter), 1)
            .unwrap();

        let result = acc.evaluate(EmitTo::All).unwrap();
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.value(0), 10.0);
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
