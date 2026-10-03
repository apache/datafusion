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

//! [`datafusion_expr::HigherOrderUDF`] definitions for array_filter function.

use arrow::{
    array::{Array, ArrayRef, AsArray, BooleanArray, GenericListArray, OffsetSizeTrait},
    compute::{filter as arrow_filter, prep_null_mask_filter},
    datatypes::{DataType, Field, FieldRef},
};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, Documentation, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs,
    HigherOrderSignature, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda,
    Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

use crate::utils::{empty_list_values, prepare_list_filter};

use crate::lambda_utils::{
    EvaluatedListLambda, SingleListLambdaResult, coerce_single_list_arg,
    evaluate_single_list_predicate, single_list_lambda_parameters, value_lambda_pair,
};

make_higher_order_function_expr_and_func!(
    ArrayFilter,
    array_filter,
    array lambda,
    "filters the values of an array using a boolean lambda",
    array_filter_higher_order_function
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "filters the values of an array using a boolean lambda",
    syntax_example = "array_filter(array, x -> x > 2)",
    sql_example = r#"```sql
> select array_filter([1, 2, 3, 4, 5], x -> x > 2);
+-------------------------------------------+
| array_filter([1, 2, 3, 4, 5], x -> x > 2) |
+-------------------------------------------+
| [3, 4, 5]                                 |
+-------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "lambda",
        description = "Lambda that returns a boolean. Elements for which the lambda returns true are kept."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayFilter {
    signature: HigherOrderSignature,
    aliases: Vec<String>,
}

impl Default for ArrayFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayFilter {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("list_filter")],
        }
    }
}

impl HigherOrderUDFImpl for ArrayFilter {
    fn name(&self) -> &str {
        "array_filter"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        single_list_lambda_parameters(self.name(), fields)
    }

    fn return_field_from_args(
        &self,
        args: HigherOrderReturnFieldArgs,
    ) -> Result<Arc<Field>> {
        let (list, _lambda) = value_lambda_pair(self.name(), args.arg_fields)?;
        Ok(Arc::new(Field::new(
            "",
            list.data_type().clone(),
            list.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let evaluated = match evaluate_single_list_predicate(self.name(), &args)? {
            SingleListLambdaResult::EarlyReturn(v) => return Ok(v),
            SingleListLambdaResult::Ready(v) => v,
        };

        let field = match args.return_field.data_type() {
            DataType::List(field) | DataType::LargeList(field) => Arc::clone(field),
            _ => {
                return exec_err!(
                    "{} expected return_field to be a list, got {}",
                    self.name(),
                    args.return_field
                );
            }
        };

        // ListView and LargeListView are coerced to List/LargeList by coerce_value_types.
        let filtered_list = match evaluated.original_list.data_type() {
            DataType::List(_) => filter_list::<i32>(self.name(), evaluated, field)?,
            DataType::LargeList(_) => filter_list::<i64>(self.name(), evaluated, field)?,
            other => exec_err!("expected list, got {other}")?,
        };

        Ok(ColumnarValue::Array(filtered_list))
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_single_list_arg(self.name(), arg_types)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Apply the evaluated predicate and rebuild the result's row offsets.
fn filter_list<O: OffsetSizeTrait>(
    name: &str,
    evaluated: EvaluatedListLambda,
    field: FieldRef,
) -> Result<ArrayRef> {
    let list = evaluated.original_list.as_list::<O>();
    if let ColumnarValue::Scalar(ScalarValue::Boolean(b)) = &evaluated.evaluated_result {
        return match b {
            Some(true) => Ok(evaluated.original_list),
            _ => Ok(empty_list_values(list, field)),
        };
    }

    let predicate = evaluated.boolean_predicate(name)?;
    // Normalize NULL predicate results to false once, so row-offset calculation
    // and filtering use the same keep bitmap. Selected elements may still be NULL.
    let predicate = match predicate.null_count() {
        0 => predicate,
        _ => prep_null_mask_filter(&predicate),
    };
    let (new_offsets, keep_mask) = prepare_list_filter(list, predicate.values());

    let kept_count = new_offsets.last().as_usize();
    if kept_count == evaluated.flattened_values.len() {
        return Ok(evaluated.original_list);
    }
    if kept_count == 0 {
        return Ok(empty_list_values(list, field));
    }

    let filtered_values = arrow_filter(
        evaluated.flattened_values.as_ref(),
        &BooleanArray::new(keep_mask, None),
    )?;
    Ok(Arc::new(GenericListArray::<O>::new(
        field,
        new_offsets,
        filtered_values,
        list.nulls().cloned(),
    )))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{Array, ArrayRef, AsArray, Int32Array, ListArray, OffsetSizeTrait},
        buffer::{NullBuffer, OffsetBuffer},
        compute::cast,
        datatypes::{DataType, Field},
    };

    use crate::array_filter::array_filter_higher_order_function;
    use crate::lambda_utils::test_utils::{
        create_i32_large_list, create_i32_list, eval_hof_on_i32_list,
        eval_hof_on_i32_list_with_outer, v,
    };
    use datafusion_expr::{Expr, col, expr::LambdaVariable, lit};

    /// The result must share the input's element and offset buffers.
    fn assert_reuses_input(actual: &ListArray, input: &ListArray) {
        assert_eq!(actual, input);
        assert!(Arc::ptr_eq(actual.values(), input.values()));
        assert!(actual.offsets().ptr_eq(input.offsets()));
    }

    /// Compare physical layouts, which logical equality ignores.
    fn assert_same_layout<O: OffsetSizeTrait>(actual: &dyn Array, expected: &dyn Array) {
        let (actual, expected) = (actual.as_list::<O>(), expected.as_list::<O>());
        assert_eq!(actual.offsets(), expected.offsets());
        assert_eq!(actual.values(), expected.values());
    }

    fn keep_greater_than_two(
        list: impl Array + Clone + 'static,
    ) -> datafusion_common::Result<ArrayRef> {
        eval_hof_on_i32_list(
            array_filter_higher_order_function(),
            list,
            v().gt(lit(2i32)),
        )
    }

    #[test]
    fn filter_basic() {
        let list = create_i32_list(
            vec![1, 2, 3, 4, 5],
            OffsetBuffer::<i32>::from_lengths(vec![5]),
            None,
        );

        let res = keep_greater_than_two(list).unwrap();
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![3, 4, 5],
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );

        assert_eq!(actual, &expected);
    }

    #[test]
    fn filter_multiple_sublists() {
        let list = create_i32_list(
            vec![1, 5, 2, 4, 3],
            OffsetBuffer::<i32>::from_lengths(vec![2, 3]),
            None,
        );

        let res = keep_greater_than_two(list).unwrap();
        let actual = res.as_list::<i32>();

        // [1,5] -> [5], [2,4,3] -> [4,3]
        let expected = create_i32_list(
            vec![5, 4, 3],
            OffsetBuffer::<i32>::from_lengths(vec![1, 2]),
            None,
        );

        assert_eq!(actual, &expected);
    }

    #[test]
    fn filter_sliced_list() {
        // First sublist [0] is sliced away; sliced array covers sublists [1..3]
        let list = create_i32_list(
            vec![
                0, // Outside the visible list rows.
                1, 5, 2, 4, 3, 7,
            ],
            OffsetBuffer::<i32>::from_lengths(vec![1, 3, 3]),
            None,
        )
        .slice(1, 2);

        let res = keep_greater_than_two(list).unwrap();
        let actual = res.as_list::<i32>();

        // [1,5,2] -> [5], [4,3,7] -> [4,3,7]
        let expected = create_i32_list(
            vec![5, 4, 3, 7],
            OffsetBuffer::<i32>::from_lengths(vec![1, 3]),
            None,
        );

        assert_eq!(actual, &expected);
    }

    #[test]
    fn filter_does_not_copy_values_underlying_null_rows() {
        // The NULL list row contains values that pass the predicate. They must
        // not occupy space in the result's element array.
        let list = create_i32_list(
            vec![1, 5, 99, 100, 3, 7],
            OffsetBuffer::<i32>::from_lengths(vec![2, 2, 2]),
            Some(NullBuffer::from(vec![true, false, true])),
        );

        let res = keep_greater_than_two(list).unwrap();
        let actual = res.as_list::<i32>();

        // sublist 0: [1,5] -> [5]
        // sublist 1: null  -> null (empty range, null bit)
        // sublist 2: [3,7] -> [3,7]
        let expected = create_i32_list(
            vec![5, 3, 7],
            OffsetBuffer::<i32>::from_lengths(vec![1, 0, 2]),
            Some(NullBuffer::from(vec![true, false, true])),
        );

        assert_eq!(actual.data_type(), expected.data_type());
        assert_eq!(actual, &expected);
        assert_same_layout::<i32>(actual, &expected);
    }

    #[test]
    fn filter_all_filtered_out() {
        let list =
            create_i32_list(vec![1, 2], OffsetBuffer::<i32>::from_lengths(vec![2]), None);

        let res = keep_greater_than_two(list).unwrap();
        let actual = res.as_list::<i32>();

        let expected = create_i32_list(
            vec![0i32; 0],
            OffsetBuffer::<i32>::from_lengths(vec![0]),
            None,
        );

        assert_eq!(actual, &expected);
    }

    #[test]
    fn filter_nothing_filtered_reuses_values() {
        let list = create_i32_list(
            vec![3, 4, 5],
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );
        // all elements > 2, so nothing is filtered — values buffer should be reused
        let res = keep_greater_than_two(list.clone()).unwrap();
        assert_reuses_input(res.as_list::<i32>(), &list);
    }

    #[test]
    fn scalar_true_predicate_returns_original_list() {
        let list = create_i32_list(
            vec![1, 2, 3],
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );
        // x -> true: every element kept, should return list unchanged
        let res = eval_hof_on_i32_list(
            array_filter_higher_order_function(),
            list.clone(),
            lit(true),
        )
        .unwrap();
        assert_reuses_input(res.as_list::<i32>(), &list);
    }

    #[test]
    fn filter_sliced_null_elements_and_rows() {
        // Slice both the element array and the list rows. The visible input is
        // [[NULL, 1, 4, NULL], NULL, [], [NULL, 3, 7]]. The NULL row retains 99,100.
        let values = Int32Array::from(vec![
            Some(-1),
            Some(42),
            None,
            Some(1),
            Some(4),
            None,
            Some(99),
            Some(100),
            None,
            Some(3),
            Some(7),
            Some(88),
        ])
        .slice(1, 11);
        let field = Arc::new(
            Field::new("element", DataType::Int32, true)
                .with_metadata(HashMap::from([("key".into(), "value".into())])),
        );
        let value = || {
            Expr::LambdaVariable(LambdaVariable::new(
                "v".into(),
                Some(Arc::clone(&field)),
            ))
        };
        let list = ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::<i32>::from_lengths([1, 4, 2, 0, 3, 1]),
            Arc::new(values),
            Some(NullBuffer::from(vec![true, true, false, true, true, true])),
        )
        .slice(1, 4);

        for large in [false, true] {
            let data_type = if large {
                DataType::LargeList(Arc::clone(&field))
            } else {
                DataType::List(Arc::clone(&field))
            };
            let input = cast(&list, &data_type).unwrap();
            for (predicate, values, lengths) in [
                (
                    value().gt(lit(2i32)),
                    vec![Some(4), Some(3), Some(7)],
                    [1, 0, 0, 2],
                ),
                (value().is_null(), vec![None, None, None], [2, 0, 0, 1]),
                (
                    value().is_null().or(value().gt(lit(2i32))),
                    vec![None, Some(4), None, None, Some(3), Some(7)],
                    [3, 0, 0, 3],
                ),
            ] {
                let result = eval_hof_on_i32_list(
                    array_filter_higher_order_function(),
                    Arc::clone(&input),
                    predicate,
                )
                .unwrap();
                let expected = ListArray::new(
                    Arc::clone(&field),
                    OffsetBuffer::<i32>::from_lengths(lengths),
                    Arc::new(Int32Array::from(values)),
                    list.nulls().cloned(),
                );
                let expected = cast(&expected, &data_type).unwrap();
                assert_eq!(result.as_ref(), expected.as_ref());
                if large {
                    assert_same_layout::<i64>(&result, &expected);
                } else {
                    assert_same_layout::<i32>(&result, &expected);
                }
                result.to_data().validate_full().unwrap();
            }
        }
    }

    #[test]
    fn test_sliced_capacity() -> datafusion_common::Result<()> {
        crate::utils::tests::check_sliced_list_behavior(|input| {
            eval_hof_on_i32_list(
                array_filter_higher_order_function(),
                Arc::clone(input),
                Expr::LambdaVariable(LambdaVariable::new(
                    "v".into(),
                    Some(Arc::new(Field::new("v", DataType::Float64, true))),
                ))
                .gt(lit(3.5)),
            )
        })
    }

    #[test]
    fn scalar_false_predicate_returns_empty_sublists() {
        let list = create_i32_list(
            vec![1, 2, 3, 4],
            OffsetBuffer::<i32>::from_lengths(vec![2, 2]),
            None,
        );
        // x -> false: every sublist emptied
        let res =
            eval_hof_on_i32_list(array_filter_higher_order_function(), list, lit(false))
                .unwrap();
        let actual = res.as_list::<i32>();
        let expected = create_i32_list(
            vec![0i32; 0],
            OffsetBuffer::<i32>::from_lengths(vec![0, 0]),
            None,
        );
        assert_eq!(actual, &expected);
    }

    #[test]
    fn filter_large_list_parity() {
        let list = create_i32_large_list(
            vec![1, 2, 3, 4, 5],
            OffsetBuffer::<i64>::from_lengths(vec![5]),
            None,
        );
        let res = keep_greater_than_two(list).unwrap();
        let actual = res.as_list::<i64>();
        let expected = create_i32_large_list(
            vec![3, 4, 5],
            OffsetBuffer::<i64>::from_lengths(vec![3]),
            None,
        );
        assert_eq!(actual, &expected);
    }

    #[test]
    fn filter_captured_outer_column() {
        let list = create_i32_list(
            vec![1, 50, 4, 50, 7, 50],
            OffsetBuffer::<i32>::from_lengths(vec![2, 2, 2]),
            None,
        );
        let number = Int32Array::from(vec![10, 40, 60]);
        let res = eval_hof_on_i32_list_with_outer(
            array_filter_higher_order_function(),
            list,
            number,
            v().gt(col("number")),
        )
        .unwrap();
        let actual = res.as_list::<i32>();
        let expected = create_i32_list(
            vec![50, 50],
            OffsetBuffer::<i32>::from_lengths(vec![1, 1, 0]),
            None,
        );
        assert_eq!(actual, &expected);
    }
}
