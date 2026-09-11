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

use arrow::array::{ArrayRef, AsArray, BooleanArray, OffsetSizeTrait, new_null_array};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::take_arrays;
use arrow::datatypes::{ArrowNativeType, DataType, FieldRef};
use datafusion_common::utils::{adjust_offsets_for_slice, list_values_row_number};
use datafusion_common::{
    Result, ScalarValue, plan_err,
    utils::{list_values, take_function_args},
};
use datafusion_common::{exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, LambdaParametersProgress, ValueOrLambda,
};
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
        DataType::Null => DataType::new_list(DataType::Null, true),
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

    let (DataType::List(field) | DataType::LargeList(field)) = list.data_type() else {
        return plan_err!("expected list, got {list}");
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

pub(crate) enum SingleListLambdaResult {
    EarlyReturn(ColumnarValue),
    Ready(EvaluatedListLambda),
}

pub(crate) struct EvaluatedListLambda {
    pub original_list: ArrayRef,
    pub flattened_values: ArrayRef,
    pub evaluated_result: ColumnarValue,
    row_offsets: Vec<usize>,
}

impl EvaluatedListLambda {
    pub(crate) fn len(&self) -> usize {
        self.original_list.len()
    }

    pub(crate) fn nulls(&self) -> Option<&NullBuffer> {
        self.original_list.nulls()
    }

    pub(crate) fn row_range(&self, i: usize) -> (usize, usize) {
        (self.row_offsets[i], self.row_offsets[i + 1])
    }

    pub(crate) fn adjusted_offsets<O: OffsetSizeTrait>(&self) -> OffsetBuffer<O> {
        OffsetBuffer::from_lengths(self.row_offsets.windows(2).map(|w| w[1] - w[0]))
    }

    pub(crate) fn boolean_predicate(&self, name: &str) -> Result<BooleanArray> {
        let arr = self
            .evaluated_result
            .clone()
            .into_array(self.flattened_values.len())?;

        let predicate = arr.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
            exec_datafusion_err!("{} predicate must return boolean array", name)
        })?;

        Ok(predicate.clone())
    }
}

fn adjusted_row_offsets(list: &ArrayRef) -> Result<Vec<usize>> {
    Ok(match list.data_type() {
        DataType::List(_) => adjust_offsets_for_slice(list.as_list::<i32>())
            .iter()
            .map(|o| o.as_usize())
            .collect(),
        DataType::LargeList(_) => adjust_offsets_for_slice(list.as_list::<i64>())
            .iter()
            .map(|o| o.as_usize())
            .collect(),
        other => return exec_err!("expected list, got {other}"),
    })
}

fn evaluate_single_list_lambda(
    name: &str,
    args: &HigherOrderFunctionArgs,
) -> Result<SingleListLambdaResult> {
    let (original_list, lambda) = value_lambda_pair(name, &args.args)?;
    let original_list = original_list.to_array(args.number_rows)?;

    if original_list.null_count() == original_list.len() {
        return Ok(SingleListLambdaResult::EarlyReturn(ColumnarValue::Array(
            new_null_array(args.return_type(), original_list.len()),
        )));
    }

    let flattened_values = list_values(&original_list)?;
    let values_param = || Ok(Arc::clone(&flattened_values));

    let evaluated_result = lambda.evaluate(&[&values_param], |arrays| {
        let indices = list_values_row_number(&original_list)?;
        Ok(take_arrays(arrays, &indices, None)?)
    })?;

    let row_offsets = adjusted_row_offsets(&original_list)?;

    Ok(SingleListLambdaResult::Ready(EvaluatedListLambda {
        original_list,
        flattened_values,
        evaluated_result,
        row_offsets,
    }))
}

pub(crate) fn evaluate_single_list_predicate(
    name: &str,
    args: &HigherOrderFunctionArgs,
) -> Result<SingleListLambdaResult> {
    let result = evaluate_single_list_lambda(name, args)?;
    let SingleListLambdaResult::Ready(evaluated_list_lambda) = &result else {
        return Ok(result);
    };

    match &evaluated_list_lambda.evaluated_result {
        ColumnarValue::Scalar(ScalarValue::Boolean(_)) => Ok(result),
        ColumnarValue::Scalar(scalar) => exec_err!(
            "{name} lambda must return boolean, got {}",
            scalar.data_type()
        ),
        ColumnarValue::Array(array) if array.as_any().is::<BooleanArray>() => Ok(result),
        ColumnarValue::Array(array) => exec_err!(
            "{name} lambda must return boolean, got {}",
            array.data_type()
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::ArrayRef,
        buffer::{NullBuffer, OffsetBuffer},
        datatypes::{DataType, Field},
    };
    use datafusion_common::Result;

    use super::{adjusted_row_offsets, coerce_single_list_arg};
    use crate::lambda_utils::test_utils::{create_i32_large_list, create_i32_list};

    #[test]
    fn adjusted_row_offsets_matches_list_lengths() -> Result<()> {
        let list = create_i32_list(
            vec![1, 2, 3, 4, 5],
            OffsetBuffer::<i32>::from_lengths(vec![2, 0, 3]),
            None,
        );
        let list = Arc::new(list) as ArrayRef;
        assert_eq!(adjusted_row_offsets(&list)?, vec![0, 2, 2, 5]);
        Ok(())
    }

    #[test]
    fn adjusted_row_offsets_on_sliced_list() -> Result<()> {
        let list = create_i32_list(
            vec![10, 1, 2, 3, 4],
            OffsetBuffer::<i32>::from_lengths(vec![1, 2, 2]),
            None,
        )
        .slice(1, 2);
        let list = Arc::new(list) as ArrayRef;
        assert_eq!(adjusted_row_offsets(&list)?, vec![0, 2, 4]);
        Ok(())
    }

    #[test]
    fn adjusted_row_offsets_null_rows_keep_backing_lengths() -> Result<()> {
        let list = create_i32_list(
            vec![1, 99, 100, 2],
            OffsetBuffer::<i32>::from_lengths(vec![1, 2, 1]),
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let list = Arc::new(list) as ArrayRef;
        assert_eq!(adjusted_row_offsets(&list)?, vec![0, 1, 3, 4]);
        Ok(())
    }

    #[test]
    fn adjusted_row_offsets_large_list_parity() -> Result<()> {
        let list = create_i32_large_list(
            vec![1, 2, 3, 4],
            OffsetBuffer::<i64>::from_lengths(vec![1, 3]),
            None,
        );
        let list = Arc::new(list) as ArrayRef;
        assert_eq!(adjusted_row_offsets(&list)?, vec![0, 1, 4]);
        Ok(())
    }

    #[test]
    fn coerce_single_list_arg_supports_advertised_list_likes() -> Result<()> {
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        assert_eq!(
            coerce_single_list_arg("test", &[DataType::List(Arc::clone(&field))])?,
            vec![DataType::List(Arc::clone(&field))]
        );
        assert_eq!(
            coerce_single_list_arg("test", &[DataType::LargeList(Arc::clone(&field))])?,
            vec![DataType::LargeList(Arc::clone(&field))]
        );
        assert_eq!(
            coerce_single_list_arg(
                "test",
                &[DataType::FixedSizeList(Arc::clone(&field), 3)]
            )?,
            vec![DataType::List(Arc::clone(&field))]
        );
        assert_eq!(
            coerce_single_list_arg("test", &[DataType::ListView(Arc::clone(&field))])?,
            vec![DataType::List(Arc::clone(&field))]
        );
        assert_eq!(
            coerce_single_list_arg(
                "test",
                &[DataType::LargeListView(Arc::clone(&field))]
            )?,
            vec![DataType::LargeList(field)]
        );
        Ok(())
    }

    #[test]
    fn coerce_single_list_arg_rejects_non_list() {
        let err = coerce_single_list_arg("test", &[DataType::Int32]).unwrap_err();
        assert!(err.to_string().contains("expected a list"));
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{Array, ArrayRef, Int32Array, LargeListArray, ListArray, RecordBatch},
        buffer::{NullBuffer, OffsetBuffer},
        datatypes::{DataType, Field},
    };
    use datafusion_common::{DFSchema, Result};
    use datafusion_expr::{
        Expr, HigherOrderUDF, col,
        execution_props::ExecutionProps,
        expr::{HigherOrderFunction, LambdaVariable},
        lambda,
        physical_planning_context::PhysicalPlanningContext,
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

    pub(crate) fn create_i32_large_list(
        values: impl Into<Int32Array>,
        offsets: OffsetBuffer<i64>,
        nulls: Option<NullBuffer>,
    ) -> LargeListArray {
        let list_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        LargeListArray::new(list_field, offsets, Arc::new(values.into()), nulls)
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
            &PhysicalPlanningContext::default(),
        )?
        .evaluate(&RecordBatch::try_new(
            Arc::clone(schema.inner()),
            vec![Arc::new(list.clone())],
        )?)?
        .into_array(list.len())
    }

    /// Evaluates a HOF whose lambda body may capture an outer `number` column.
    pub(crate) fn eval_hof_on_i32_list_with_outer(
        func: Arc<HigherOrderUDF>,
        list: impl Array + Clone + 'static,
        number: Int32Array,
        lambda_body: Expr,
    ) -> Result<ArrayRef> {
        assert_eq!(list.len(), number.len());
        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("list", list.data_type().clone(), list.is_nullable()),
                Field::new("number", DataType::Int32, true),
            ]
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
            &PhysicalPlanningContext::default(),
        )?
        .evaluate(&RecordBatch::try_new(
            Arc::clone(schema.inner()),
            vec![Arc::new(list.clone()), Arc::new(number)],
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
