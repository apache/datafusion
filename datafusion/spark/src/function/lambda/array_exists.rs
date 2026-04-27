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

//! Spark-compatible `exists` higher-order function.
//!
//! Returns true if the predicate holds for one or more elements in the
//! array, false if it holds for none, and NULL in the three-valued-logic
//! cases (null array, or no true result but at least one null predicate /
//! null element).

use std::{fmt::Debug, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, GenericListArray,
        OffsetSizeTrait,
    },
    buffer::NullBuffer,
    datatypes::{DataType, Field, FieldRef},
};
use datafusion_common::{
    Result, ScalarValue, exec_err, not_impl_err, plan_err,
    utils::{list_values, take_function_args},
};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs,
    HigherOrderSignature, HigherOrderUDF, LambdaParametersProgress, ValueOrLambda,
    Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayExists {
    signature: HigherOrderSignature,
}

impl Default for SparkArrayExists {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayExists {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::user_defined(Volatility::Immutable),
        }
    }
}

impl HigherOrderUDF for SparkArrayExists {
    fn name(&self) -> &str {
        "array_exists"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [list] = take_function_args(self.name(), arg_types)?;

        let coerced = match list {
            DataType::List(_) | DataType::LargeList(_) => list.clone(),
            DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
                DataType::List(Arc::clone(field))
            }
            DataType::LargeListView(field) => DataType::LargeList(Arc::clone(field)),
            DataType::Null => DataType::List(Arc::new(Field::new(
                Field::LIST_FIELD_DEFAULT_NAME,
                DataType::Null,
                true,
            ))),
            other => {
                return plan_err!(
                    "{} expected a list as first argument, got {}",
                    self.name(),
                    other
                );
            }
        };

        Ok(vec![coerced])
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let (list, _lambda) = value_lambda_pair(self.name(), fields)?;

        let field = match list.data_type() {
            DataType::List(field) | DataType::LargeList(field) => field,
            other => {
                return plan_err!("{} expected a list, got {other}", self.name());
            }
        };

        Ok(LambdaParametersProgress::Complete(vec![vec![Arc::clone(
            field,
        )]]))
    }

    fn return_field_from_args(
        &self,
        args: HigherOrderReturnFieldArgs,
    ) -> Result<FieldRef> {
        let (_list, lambda) = value_lambda_pair(self.name(), args.arg_fields)?;

        if !matches!(lambda.data_type(), DataType::Boolean | DataType::Null) {
            return plan_err!(
                "{} predicate must return Boolean, got {}",
                self.name(),
                lambda.data_type()
            );
        }

        // Spark `exists` can return NULL either because the input list row is
        // null, or because the predicate returned NULL for some element and
        // no other element evaluated to true. Always nullable.
        Ok(Arc::new(Field::new("", DataType::Boolean, true)))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (list, lambda) = value_lambda_pair(self.name(), &args.args)?;

        let list_array = list.to_array(args.number_rows)?;
        let num_rows = list_array.len();

        // Fast path: fully-null input list — every row is NULL.
        if list_array.null_count() == num_rows {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }

        let list_values = list_values(&list_array)?;

        // Fast path: no elements anywhere in any sublist. Non-null rows are
        // empty arrays → false; null rows stay null.
        if list_values.is_empty() {
            return Ok(ColumnarValue::Array(all_false_with_null_mask(
                num_rows,
                list_array.nulls().cloned(),
            )));
        }

        let values_param = || Ok(Arc::clone(&list_values));

        let predicate = lambda
            .evaluate(&[&values_param], |_| {
                not_impl_err!(
                    "array_exists column capture support is not implemented yet"
                )
            })?
            .into_array(list_values.len())?;

        let predicate = predicate
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(format!(
                    "array_exists predicate expected Boolean, got {}",
                    predicate.data_type()
                ))
            })?;

        let result = match list_array.data_type() {
            DataType::List(_) => compute_exists(list_array.as_list::<i32>(), predicate),
            DataType::LargeList(_) => {
                compute_exists(list_array.as_list::<i64>(), predicate)
            }
            other => {
                return exec_err!("array_exists expected list, got {other}");
            }
        };

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

fn all_false_with_null_mask(num_rows: usize, nulls: Option<NullBuffer>) -> ArrayRef {
    let values = vec![false; num_rows];
    Arc::new(BooleanArray::new(values.into(), nulls))
}

fn compute_exists<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    predicate: &BooleanArray,
) -> BooleanArray {
    // `list_values` returns a sliced values array that already matches the
    // visible portion of `list`. `list.value_offsets()[0]` tells us where
    // the visible portion starts within the unsliced offsets, so we subtract
    // it to index into `predicate` (which was built from the sliced values).
    let offsets = list.value_offsets();
    let base = offsets.first().copied().map(|o| o.as_usize()).unwrap_or(0);
    let nulls = list.nulls();

    let len = list.len();
    let mut values = BooleanBufferBuilder::new(len);
    let mut validity = BooleanBufferBuilder::new(len);

    for row in 0..len {
        if nulls.map(|n| n.is_null(row)).unwrap_or(false) {
            values.append(false);
            validity.append(false);
            continue;
        }

        let start = offsets[row].as_usize() - base;
        let end = offsets[row + 1].as_usize() - base;

        let mut any_true = false;
        let mut any_null = false;

        for i in start..end {
            if predicate.is_null(i) {
                any_null = true;
            } else if predicate.value(i) {
                any_true = true;
                break;
            }
        }

        if any_true {
            values.append(true);
            validity.append(true);
        } else if any_null {
            values.append(false);
            validity.append(false);
        } else {
            values.append(false);
            validity.append(true);
        }
    }

    BooleanArray::new(values.finish(), Some(NullBuffer::new(validity.finish())))
}

fn value_lambda_pair<'a, V: Debug, L: Debug>(
    name: &str,
    args: &'a [ValueOrLambda<V, L>],
) -> Result<(&'a V, &'a L)> {
    let [value, lambda] = take_function_args(name, args)?;

    let (ValueOrLambda::Value(value), ValueOrLambda::Lambda(lambda)) = (value, lambda)
    else {
        return plan_err!(
            "{name} expects a value followed by a lambda, got {value:?} and {lambda:?}"
        );
    };

    Ok((value, lambda))
}
