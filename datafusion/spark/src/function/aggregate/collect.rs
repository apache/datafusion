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

use arrow::array::{Array, ArrayRef};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion_functions_aggregate::array_agg::{
    ArrayAggAccumulator, DistinctArrayAggAccumulator,
};
use std::sync::Arc;

// Spark implementation of collect_list/collect_set aggregate function.
// Differs from DataFusion ArrayAgg in the following ways:
// - ignores NULL inputs
// - returns an empty list when all inputs are NULL
// - does not support ordering

/// Build an empty list `ScalarValue` for a `List(element_type)` data type.
/// Used as the result for empty window frames and for groups whose inputs
/// were all NULL, matching Spark's `collect_list` / `collect_set` semantics.
fn empty_list_scalar(list_type: &DataType) -> Result<ScalarValue> {
    let DataType::List(field) = list_type else {
        return internal_err!(
            "collect_list/collect_set expected List return type, got {list_type:?}"
        );
    };
    let empty = arrow::array::new_empty_array(field.data_type());
    Ok(SingleRowListArrayBuilder::new(empty)
        .with_field(field)
        .build_list_scalar())
}

fn collect_type(element_type: DataType) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(element_type, false)))
}

/// Rebuild an accumulator result with the aggregate's declared list field.
///
/// The shared array aggregate accumulators use a nullable list field and can
/// derive nested fields from runtime arrays. Spark collect aggregates always
/// drop null inputs, so their element field is non-nullable. Reusing the
/// declared field also keeps nested types consistent with planning.
fn normalize_list_scalar(
    value: ScalarValue,
    list_type: &DataType,
) -> Result<ScalarValue> {
    let DataType::List(field) = list_type else {
        return internal_err!(
            "collect_list/collect_set expected List return type, got {list_type:?}"
        );
    };
    let ScalarValue::List(array) = value else {
        return internal_err!(
            "collect_list/collect_set accumulator returned a non-List value"
        );
    };
    if array.len() != 1 {
        return internal_err!(
            "collect_list/collect_set accumulator returned {} rows, expected one",
            array.len()
        );
    }
    if array.is_null(0) {
        return Ok(ScalarValue::new_null_list(
            field.data_type().clone(),
            field.is_nullable(),
            1,
        ));
    }

    let values = array.value(0);
    let values = if values.data_type() == field.data_type() {
        values
    } else {
        cast(values.as_ref(), field.data_type())?
    };
    Ok(SingleRowListArrayBuilder::new(values)
        .with_field(field)
        .build_list_scalar())
}

// <https://spark.apache.org/docs/latest/api/sql/index.html#collect_list>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCollectList {
    signature: Signature,
}

impl Default for SparkCollectList {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCollectList {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for SparkCollectList {
    fn name(&self) -> &str {
        "collect_list"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(collect_type(arg_types[0].clone()))
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new_list(
                format_state_name(args.name, "collect_list"),
                Field::new_list_field(args.input_fields[0].data_type().clone(), false),
                true,
            )
            .into(),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let element_type = acc_args.expr_fields[0].data_type().clone();
        let ignore_nulls = true;
        Ok(Box::new(NullToEmptyListAccumulator::new(
            ArrayAggAccumulator::try_new(&element_type, ignore_nulls)?,
            acc_args.return_type().clone(),
        )))
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        empty_list_scalar(data_type)
    }
}

// <https://spark.apache.org/docs/latest/api/sql/index.html#collect_set>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCollectSet {
    signature: Signature,
}

impl Default for SparkCollectSet {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCollectSet {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for SparkCollectSet {
    fn name(&self) -> &str {
        "collect_set"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(collect_type(arg_types[0].clone()))
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new_list(
                format_state_name(args.name, "collect_set"),
                Field::new_list_field(args.input_fields[0].data_type().clone(), false),
                true,
            )
            .into(),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let element_type = acc_args.expr_fields[0].data_type().clone();
        let ignore_nulls = true;
        Ok(Box::new(NullToEmptyListAccumulator::new(
            DistinctArrayAggAccumulator::try_new(&element_type, None, ignore_nulls)?,
            acc_args.return_type().clone(),
        )))
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        empty_list_scalar(data_type)
    }
}

/// Wrapper accumulator that returns an empty list instead of NULL when all inputs are NULL.
/// This implements Spark's behavior for collect_list and collect_set.
#[derive(Debug)]
struct NullToEmptyListAccumulator<T: Accumulator> {
    inner: T,
    list_type: DataType,
}

impl<T: Accumulator> NullToEmptyListAccumulator<T> {
    pub fn new(inner: T, list_type: DataType) -> Self {
        Self { inner, list_type }
    }

    fn normalize_input(&self, value: &ArrayRef) -> Result<ArrayRef> {
        let DataType::List(field) = &self.list_type else {
            return internal_err!(
                "collect_list/collect_set expected List return type, got {:?}",
                self.list_type
            );
        };
        if value.data_type() == field.data_type() {
            Ok(Arc::clone(value))
        } else {
            Ok(cast(value.as_ref(), field.data_type())?)
        }
    }
}

impl<T: Accumulator> Accumulator for NullToEmptyListAccumulator<T> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [value] = values else {
            return self.inner.update_batch(values);
        };
        let value = self.normalize_input(value)?;
        self.inner.update_batch(&[value])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner
            .state()?
            .into_iter()
            .map(|value| normalize_list_scalar(value, &self.list_type))
            .collect()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let result = self.inner.evaluate()?;
        if result.is_null() {
            empty_list_scalar(&self.list_type)
        } else {
            normalize_list_scalar(result, &self.list_type)
        }
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.retract_batch(values)
    }

    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }

    fn size(&self) -> usize {
        self.inner.size() + self.list_type.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        DictionaryArray, Int8Array, Int16Array, Int32Array, ListArray, RunArray,
        StringArray, StructArray, UnionArray,
    };
    use arrow::buffer::ScalarBuffer;
    use arrow::datatypes::{Fields, Int8Type, Int16Type, Schema, UnionFields};
    use arrow::record_batch::RecordBatch;
    use arrow::util::display::array_value_to_string;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::AggregateUDF;

    fn list_type(element_type: DataType) -> DataType {
        DataType::List(Arc::new(Field::new_list_field(element_type, false)))
    }

    fn accumulator(
        element_type: &DataType,
        distinct: bool,
    ) -> Result<Box<dyn Accumulator>> {
        let return_type = list_type(element_type.clone());
        if distinct {
            Ok(Box::new(NullToEmptyListAccumulator::new(
                DistinctArrayAggAccumulator::try_new(element_type, None, true)?,
                return_type,
            )))
        } else {
            Ok(Box::new(NullToEmptyListAccumulator::new(
                ArrayAggAccumulator::try_new(element_type, true)?,
                return_type,
            )))
        }
    }

    fn assert_empty_list(value: &ScalarValue) {
        let ScalarValue::List(array) = value else {
            panic!("expected a list scalar")
        };
        assert_eq!(array.value(0).len(), 0);
    }

    fn assert_nested_values(value: &ScalarValue) {
        let ScalarValue::List(array) = value else {
            panic!("expected a list scalar")
        };
        let values = array.value(0);
        let structs = values
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("expected struct values");
        let integers = structs
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("expected Int32 struct field");
        let mut actual: Vec<i32> = integers.iter().map(Option::unwrap).collect();
        actual.sort_unstable();
        assert_eq!(actual, vec![1, 2]);
    }

    fn assert_list_values(
        value: &ScalarValue,
        element_type: &DataType,
        expected: &[&str],
    ) -> Result<()> {
        assert_eq!(value.data_type(), list_type(element_type.clone()));
        let ScalarValue::List(array) = value else {
            panic!("expected a list scalar")
        };
        assert_list_array_values(array, element_type, expected)
    }

    fn assert_list_array_values(
        array: &ListArray,
        element_type: &DataType,
        expected: &[&str],
    ) -> Result<()> {
        assert_eq!(array.data_type(), &list_type(element_type.clone()));
        array.to_data().validate_full()?;

        let values = array.value(0);
        assert_eq!(values.logical_null_count(), 0);
        let mut actual = (0..values.len())
            .map(|index| array_value_to_string(values.as_ref(), index))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        actual.sort_unstable();

        let mut expected = expected
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>();
        expected.sort_unstable();
        assert_eq!(actual, expected);
        Ok(())
    }

    fn assert_accumulator_outputs(
        values: ArrayRef,
        distinct: bool,
        expected: &[&str],
    ) -> Result<()> {
        let element_type = values.data_type().clone();

        let mut partial = accumulator(&element_type, distinct)?;
        partial.update_batch(std::slice::from_ref(&values))?;
        let state = partial.state()?;
        assert_list_values(&state[0], &element_type, expected)?;

        let mut final_accumulator = accumulator(&element_type, distinct)?;
        final_accumulator.merge_batch(&[state[0].to_array()?])?;
        assert_list_values(&final_accumulator.evaluate()?, &element_type, expected)?;

        let mut single = accumulator(&element_type, distinct)?;
        single.update_batch(&[values])?;
        assert_list_values(&single.evaluate()?, &element_type, expected)
    }

    #[test]
    fn collect_types_have_non_nullable_elements() -> Result<()> {
        let element_type = DataType::Int32;
        let expected = list_type(element_type.clone());

        for aggregate in [
            &SparkCollectList::new() as &dyn AggregateUDFImpl,
            &SparkCollectSet::new() as &dyn AggregateUDFImpl,
        ] {
            assert!(!aggregate.is_nullable());
            assert_eq!(
                aggregate.return_type(std::slice::from_ref(&element_type))?,
                expected
            );

            let input_field = Arc::new(Field::new("input", element_type.clone(), true));
            let return_field =
                aggregate.return_field(std::slice::from_ref(&input_field))?;
            assert_eq!(return_field.data_type(), &expected);
            assert!(!return_field.is_nullable());

            let state_fields = aggregate.state_fields(StateFieldsArgs {
                name: aggregate.name(),
                input_fields: &[input_field],
                return_field: Arc::new(Field::new("result", expected.clone(), false)),
                ordering_fields: &[],
                is_distinct: false,
            })?;
            assert_eq!(state_fields[0].data_type(), &expected);
        }

        Ok(())
    }

    #[test]
    fn empty_partial_state_has_non_nullable_elements() -> Result<()> {
        let element_type = DataType::Int32;
        let expected = list_type(element_type.clone());

        for distinct in [false, true] {
            let mut partial = accumulator(&element_type, distinct)?;
            let state = partial.state()?;
            assert_eq!(state.len(), 1);
            assert!(state[0].is_null());
            assert_eq!(state[0].data_type(), expected);

            let ScalarValue::List(array) = &state[0] else {
                panic!("expected a list scalar")
            };
            let DataType::List(field) = array.data_type() else {
                panic!("expected a list data type")
            };
            assert_eq!(field.name(), "item");
            assert!(!field.is_nullable());

            let mut final_accumulator = accumulator(&element_type, distinct)?;
            final_accumulator.merge_batch(&[state[0].to_array()?])?;
            let value = final_accumulator.evaluate()?;
            assert!(!value.is_null());
            assert_eq!(value.data_type(), expected);
            assert_empty_list(&value);
        }

        Ok(())
    }

    #[test]
    fn empty_results_have_non_nullable_elements() -> Result<()> {
        let expected = list_type(DataType::Int32);

        for aggregate in [
            &SparkCollectList::new() as &dyn AggregateUDFImpl,
            &SparkCollectSet::new() as &dyn AggregateUDFImpl,
        ] {
            let value = aggregate.default_value(&expected)?;
            assert!(!value.is_null());
            assert_eq!(value.data_type(), expected);
            assert_empty_list(&value);
        }

        for distinct in [false, true] {
            let value = accumulator(&DataType::Int32, distinct)?.evaluate()?;
            assert!(!value.is_null());
            assert_eq!(value.data_type(), expected);
            assert_empty_list(&value);
        }

        Ok(())
    }

    #[test]
    fn accumulator_state_and_output_preserve_nested_type() -> Result<()> {
        let declared_fields =
            Fields::from(vec![Field::new("required", DataType::Int32, false)]);
        let element_type = DataType::Struct(declared_fields.clone());
        let expected = list_type(element_type.clone());

        // Exercise the downstream case from the issue: runtime arrays can carry
        // different nested nullability than the aggregate's declared type.
        let runtime_fields =
            Fields::from(vec![Field::new("required", DataType::Int32, true)]);
        let values = Arc::new(StructArray::new(
            runtime_fields,
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(2)]))],
            None,
        )) as ArrayRef;

        let scalar =
            SingleRowListArrayBuilder::new(Arc::clone(&values)).build_list_scalar();
        let normalized = normalize_list_scalar(scalar, &expected)?;
        assert_eq!(normalized.data_type(), expected);
        assert_nested_values(&normalized);

        for distinct in [false, true] {
            let mut partial = accumulator(&element_type, distinct)?;
            partial.update_batch(std::slice::from_ref(&values))?;

            let state = partial.state()?;
            assert_eq!(state[0].data_type(), expected);
            assert_nested_values(&state[0]);

            let value = partial.evaluate()?;
            assert_eq!(value.data_type(), expected);
            assert_nested_values(&value);

            let mut final_accumulator = accumulator(&element_type, distinct)?;
            final_accumulator.merge_batch(&[state[0].to_array()?])?;
            let merged = final_accumulator.evaluate()?;
            assert_eq!(merged.data_type(), expected);
            assert_nested_values(&merged);
        }

        Ok(())
    }

    #[test]
    fn nested_runtime_null_in_non_nullable_declared_field_is_rejected() -> Result<()> {
        // Widening the output type would violate the aggregate's declared schema and
        // reintroduce the AggregateExec schema mismatch this normalization prevents.
        let declared_fields =
            Fields::from(vec![Field::new("required", DataType::Int32, false)]);
        let element_type = DataType::Struct(declared_fields);
        let runtime_fields =
            Fields::from(vec![Field::new("required", DataType::Int32, true)]);
        let values = Arc::new(StructArray::new(
            runtime_fields,
            vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
            None,
        )) as ArrayRef;

        let scalar =
            SingleRowListArrayBuilder::new(Arc::clone(&values)).build_list_scalar();
        let error =
            normalize_list_scalar(scalar, &list_type(element_type.clone())).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Found unmasked nulls for non-nullable"),
            "unexpected error: {error}"
        );

        for distinct in [false, true] {
            let mut partial = accumulator(&element_type, distinct)?;
            let error = partial
                .update_batch(std::slice::from_ref(&values))
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("Found unmasked nulls for non-nullable"),
                "unexpected error: {error}"
            );
        }

        Ok(())
    }

    #[test]
    fn normalization_reuses_matching_primitive_values() -> Result<()> {
        let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let values_ptr = values.values().as_ptr();
        let scalar = SingleRowListArrayBuilder::new(values).build_list_scalar();

        let normalized = normalize_list_scalar(scalar, &list_type(DataType::Int32))?;
        let ScalarValue::List(array) = normalized else {
            panic!("expected a list scalar")
        };
        let normalized_values = array.value(0);
        let normalized_values = normalized_values
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("expected Int32 values");

        assert_eq!(normalized_values.values().as_ptr(), values_ptr);
        Ok(())
    }

    #[test]
    fn collect_list_handles_dictionary_with_unused_null() -> Result<()> {
        let keys = Int8Array::from(vec![0, 0]);
        let dictionary_values = Arc::new(StringArray::from(vec![Some("a"), None]));
        let values = Arc::new(DictionaryArray::<Int8Type>::try_new(
            keys,
            dictionary_values,
        )?) as ArrayRef;

        assert_eq!(values.logical_null_count(), 0);
        assert!(values.is_nullable());

        assert_accumulator_outputs(Arc::clone(&values), false, &["a", "a"])?;
        assert_accumulator_outputs(values, true, &["a"])?;

        Ok(())
    }

    #[test]
    fn collect_aggregates_handle_sparse_union_inactive_nulls() -> Result<()> {
        let fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("integer", DataType::Int32, false),
                Field::new("string", DataType::Utf8, false),
            ],
        )?;
        let values = Arc::new(UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![0_i8, 1, 0]),
            None,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None, Some(1)])),
                Arc::new(StringArray::from(vec![None, Some("a"), None])),
            ],
        )?) as ArrayRef;

        assert_eq!(values.logical_null_count(), 0);
        assert!(values.is_nullable());
        assert_accumulator_outputs(
            Arc::clone(&values),
            false,
            &["{integer=1}", "{string=a}", "{integer=1}"],
        )?;
        assert_accumulator_outputs(values, true, &["{integer=1}", "{string=a}"])?;

        Ok(())
    }

    #[test]
    fn collect_aggregates_handle_sliced_run_array_unused_null() -> Result<()> {
        let run_ends = Int16Array::from(vec![2, 4]);
        let run_values = StringArray::from(vec![Some("a"), None]);
        let values =
            Arc::new(RunArray::<Int16Type>::try_new(&run_ends, &run_values)?.slice(0, 2))
                as ArrayRef;

        assert_eq!(values.logical_null_count(), 0);
        assert!(values.is_nullable());
        assert_accumulator_outputs(Arc::clone(&values), false, &["a", "a"])?;
        assert_accumulator_outputs(values, true, &["a"])?;

        Ok(())
    }

    #[tokio::test]
    async fn collect_list_dictionary_sql() -> Result<()> {
        let keys = Int8Array::from(vec![0, 0]);
        let dictionary_values = Arc::new(StringArray::from(vec![Some("a"), None]));
        let values = Arc::new(DictionaryArray::<Int8Type>::try_new(
            keys,
            dictionary_values,
        )?) as ArrayRef;
        let element_type = values.data_type().clone();

        let ctx = SessionContext::new();
        ctx.register_udaf(AggregateUDF::new_from_impl(SparkCollectList::new()));
        ctx.register_batch(
            "dictionary_input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "x",
                    element_type.clone(),
                    false,
                )])),
                vec![values],
            )?,
        )?;

        let batches = ctx
            .sql("SELECT collect_list(x) AS values FROM dictionary_input")
            .await?
            .collect()
            .await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let value = ScalarValue::try_from_array(batches[0].column(0), 0)?;
        assert_list_values(&value, &element_type, &["a", "a"])?;

        Ok(())
    }
}
