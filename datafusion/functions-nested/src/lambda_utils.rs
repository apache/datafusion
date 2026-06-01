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

//! Shared utilities for `(array, lambda)` style higher-order functions.

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{
    Result, ScalarValue, plan_err,
    utils::{list_values, take_function_args},
};
use datafusion_expr::{ColumnarValue, LambdaParametersProgress, ValueOrLambda};
use std::sync::Arc;

/// Extracts a `(value, lambda)` pair from a [`ValueOrLambda`] slice.
pub(crate) fn value_lambda_pair<'a, V: std::fmt::Debug, L: std::fmt::Debug>(
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

/// Coerces a single list argument for `(array, lambda)` style higher-order functions.
///
/// Normalises `ListView`/`FixedSizeList` → `List` and `LargeListView` → `LargeList`.
pub(crate) fn coerce_single_list_arg(
    name: &str,
    arg_types: &[DataType],
) -> Result<Vec<DataType>> {
    let list = if arg_types.len() == 1 {
        &arg_types[0]
    } else {
        return plan_err!(
            "{name} function requires 1 value arguments, got {}",
            arg_types.len()
        );
    };

    let coerced = match list {
        DataType::List(_) | DataType::LargeList(_) => list.clone(),
        DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
            DataType::List(Arc::clone(field))
        }
        DataType::LargeListView(field) => DataType::LargeList(Arc::clone(field)),
        _ => return plan_err!("{name} expected a list as first argument, got {list}"),
    };

    Ok(vec![coerced])
}

/// Returns the single lambda parameter set for `(array, v -> body)` style HOFs.
pub(crate) fn single_list_lambda_parameters(
    name: &str,
    fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
) -> Result<LambdaParametersProgress> {
    let (list, _lambda) = value_lambda_pair(name, fields)?;

    let field = match list.data_type() {
        DataType::List(field) | DataType::LargeList(field) => field,
        _ => return plan_err!("expected list, got {list}"),
    };

    Ok(LambdaParametersProgress::Complete(vec![vec![Arc::clone(
        field,
    )]]))
}

/// Result of extracting flat list values, with fast-path short-circuits handled.
pub(crate) enum ListValuesResult {
    /// Caller should return this value immediately.
    EarlyReturn(ColumnarValue),
    /// Flat values extracted from the list; continue with execution.
    Values(ArrayRef),
}

/// Extracts flat list values, handling all fast-path short-circuits.
///
/// - All-null input → `EarlyReturn(null scalar)`
/// - All sublists empty and non-null → `EarlyReturn(default empty-list scalar)`
/// - Otherwise → `Values(flat_values)`
pub(crate) fn extract_list_values(
    list_array: &ArrayRef,
    return_type: &DataType,
) -> Result<ListValuesResult> {
    if list_array.null_count() == list_array.len() {
        return Ok(ListValuesResult::EarlyReturn(ColumnarValue::Scalar(
            ScalarValue::try_new_null(return_type)?,
        )));
    }

    let values = list_values(list_array)?;

    if values.is_empty()
        && list_array.null_count() == 0
        && matches!(return_type, DataType::List(_) | DataType::LargeList(_))
    {
        return Ok(ListValuesResult::EarlyReturn(ColumnarValue::Scalar(
            ScalarValue::new_default(return_type)?,
        )));
    }

    Ok(ListValuesResult::Values(values))
}

#[cfg(test)]
pub(crate) mod test_utils {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch},
        buffer::{NullBuffer, OffsetBuffer},
        datatypes::{DataType, Field},
    };
    use datafusion_common::{DFSchema, Result};
    use datafusion_expr::{
        Expr, HigherOrderUDF, col,
        execution_props::ExecutionProps,
        expr::{HigherOrderFunction, LambdaVariable},
        lambda,
    };
    use datafusion_physical_expr::create_physical_expr;

    pub(crate) fn create_i32_list(
        values: impl Into<Int32Array>,
        offsets: OffsetBuffer<i32>,
        nulls: Option<NullBuffer>,
    ) -> ListArray {
        let list_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        ListArray::new(list_field, offsets, Arc::new(values.into()), nulls)
    }

    pub(crate) fn eval_hof_on_i32_list(
        func: Arc<HigherOrderUDF>,
        list: impl Array + Clone + 'static,
        lambda_body: Expr,
    ) -> Result<ArrayRef> {
        let schema = DFSchema::from_unqualified_fields(
            vec![Field::new(
                "list",
                list.data_type().clone(),
                list.is_nullable(),
            )]
            .into(),
            HashMap::new(),
        )?;

        create_physical_expr(
            &Expr::HigherOrderFunction(HigherOrderFunction::new(
                func,
                vec![col("list"), lambda(["v"], lambda_body)],
            )),
            &schema,
            &ExecutionProps::new(),
        )?
        .evaluate(&RecordBatch::try_new(
            Arc::clone(schema.inner()),
            vec![Arc::new(list.clone())],
        )?)?
        .into_array(list.len())
    }

    pub(crate) fn v() -> Expr {
        Expr::LambdaVariable(LambdaVariable::new(
            "v".to_string(),
            Some(Arc::new(Field::new("v", DataType::Int32, true))),
        ))
    }
}
