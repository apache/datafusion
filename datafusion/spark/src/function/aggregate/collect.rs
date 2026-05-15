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

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::{
    SingleRowListArrayBuilder, nullable_list_item_field_from,
};
use datafusion_common::{Result, ScalarValue};
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
        Ok(DataType::List(Arc::new(Field::new_list_field(
            arg_types[0].clone(),
            true,
        ))))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // Propagate input-field metadata onto the state list's inner field so
        // Arrow extension types survive multi-batch aggregation.
        let inner = nullable_list_item_field_from(&args.input_fields[0]);
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "collect_list"),
            DataType::List(inner),
            true,
        ))])
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let inner = nullable_list_item_field_from(&arg_fields[0]);
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::List(inner),
            true,
        )))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let field = &acc_args.expr_fields[0];
        let ignore_nulls = true;
        Ok(Box::new(NullToEmptyListAccumulator::new(
            ArrayAggAccumulator::try_new(field, ignore_nulls)?,
            Arc::clone(field),
        )))
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
        Ok(DataType::List(Arc::new(Field::new_list_field(
            arg_types[0].clone(),
            true,
        ))))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // Propagate input-field metadata onto the state list's inner field so
        // Arrow extension types survive multi-batch aggregation.
        let inner = nullable_list_item_field_from(&args.input_fields[0]);
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "collect_set"),
            DataType::List(inner),
            true,
        ))])
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let inner = nullable_list_item_field_from(&arg_fields[0]);
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::List(inner),
            true,
        )))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let field = &acc_args.expr_fields[0];
        let ignore_nulls = true;
        Ok(Box::new(NullToEmptyListAccumulator::new(
            DistinctArrayAggAccumulator::try_new(field, None, ignore_nulls)?,
            Arc::clone(field),
        )))
    }
}

/// Wrapper accumulator that returns an empty list instead of NULL when all inputs are NULL.
/// This implements Spark's behavior for collect_list and collect_set.
#[derive(Debug)]
struct NullToEmptyListAccumulator<T: Accumulator> {
    inner: T,
    /// Input element field. Carries the data type for the empty list payload
    /// and the metadata that must flow onto the resulting list's inner field
    /// (Arrow extension types survive the all-NULL case this wrapper handles).
    value_field: FieldRef,
}

impl<T: Accumulator> NullToEmptyListAccumulator<T> {
    pub fn new(inner: T, value_field: FieldRef) -> Self {
        Self { inner, value_field }
    }
}

impl<T: Accumulator> Accumulator for NullToEmptyListAccumulator<T> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let result = self.inner.evaluate()?;
        if result.is_null() {
            let empty_array = arrow::array::new_empty_array(self.value_field.data_type());
            Ok(SingleRowListArrayBuilder::new(empty_array)
                .with_field(&self.value_field)
                .build_list_scalar())
        } else {
            Ok(result)
        }
    }

    fn size(&self) -> usize {
        self.inner.size() + self.value_field.data_type().size()
    }
}
