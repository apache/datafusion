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
    array::{
        Array, ArrayRef, AsArray, BooleanArray, LargeListArray, ListArray,
        OffsetSizeTrait, new_empty_array,
    },
    buffer::{OffsetBuffer, ScalarBuffer},
    compute::{filter as arrow_filter, take_arrays},
    datatypes::{DataType, Field, FieldRef},
};
use datafusion_common::{
    Result, ScalarValue, exec_err,
    utils::{adjust_offsets_for_slice, list_values_row_number},
};
use datafusion_expr::{
    ColumnarValue, Documentation, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs,
    HigherOrderSignature, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda,
    Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

use crate::lambda_utils::{
    ListValuesResult, coerce_single_list_arg, extract_list_values,
    single_list_lambda_parameters, value_lambda_pair,
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
+--------------------------------------------+
| array_filter([1, 2, 3, 4, 5], x -> x > 2) |
+--------------------------------------------+
| [3, 4, 5]                                  |
+--------------------------------------------+
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
        let (list, lambda) = value_lambda_pair(self.name(), &args.args)?;
        let list_array = list.to_array(args.number_rows)?;

        let list_values = match extract_list_values(&list_array, args.return_type())? {
            ListValuesResult::EarlyReturn(v) => return Ok(v),
            ListValuesResult::Values(v) => v,
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

        let values_param = || Ok(Arc::clone(&list_values));
        let predicate_output = lambda.evaluate(&[&values_param], |arrays| {
            let indices = list_values_row_number(&list_array)?;
            Ok(take_arrays(arrays, &indices, None)?)
        })?;

        // Scalar predicate short-circuit: x -> true or x -> false/null
        if let ColumnarValue::Scalar(ScalarValue::Boolean(b)) = &predicate_output {
            return match b {
                Some(true) => Ok(ColumnarValue::Array(list_array)),
                _ => Ok(ColumnarValue::Array(empty_filtered_list(
                    &list_array,
                    field,
                )?)),
            };
        }

        let predicate = predicate_output.into_array(list_values.len())?;
        let Some(predicate) = predicate.as_any().downcast_ref::<BooleanArray>() else {
            return exec_err!(
                "{} lambda must return boolean, got {}",
                self.name(),
                predicate.data_type()
            );
        };

        // ListView and LargeListView are coerced to List/LargeList by coerce_value_types.
        let filtered_list = match list_array.data_type() {
            DataType::List(_) => {
                let list = list_array.as_list::<i32>();
                let adjusted_offsets = adjust_offsets_for_slice(list);
                let (filtered_values, new_offsets) =
                    filter_list_values(&list_values, predicate, &adjusted_offsets)?;
                Arc::new(ListArray::new(
                    field,
                    new_offsets,
                    filtered_values,
                    list.nulls().cloned(),
                )) as ArrayRef
            }
            DataType::LargeList(_) => {
                let large_list = list_array.as_list::<i64>();
                let adjusted_offsets = adjust_offsets_for_slice(large_list);
                let (filtered_values, new_offsets) =
                    filter_list_values(&list_values, predicate, &adjusted_offsets)?;
                Arc::new(LargeListArray::new(
                    field,
                    new_offsets,
                    filtered_values,
                    large_list.nulls().cloned(),
                ))
            }
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

/// Returns a list array with every non-null sublist emptied, preserving the null buffer.
/// Used for the `x -> false` / `x -> null` scalar predicate short-circuit.
fn empty_filtered_list(list_array: &ArrayRef, field: FieldRef) -> Result<ArrayRef> {
    let n = list_array.len();
    let empty_values = new_empty_array(field.data_type());
    Ok(match list_array.data_type() {
        DataType::List(_) => {
            let list = list_array.as_list::<i32>();
            Arc::new(ListArray::new(
                field,
                OffsetBuffer::new(ScalarBuffer::from(vec![0i32; n + 1])),
                empty_values,
                list.nulls().cloned(),
            ))
        }
        DataType::LargeList(_) => {
            let list = list_array.as_list::<i64>();
            Arc::new(LargeListArray::new(
                field,
                OffsetBuffer::new(ScalarBuffer::from(vec![0i64; n + 1])),
                empty_values,
                list.nulls().cloned(),
            ))
        }
        other => return exec_err!("expected list, got {other}"),
    })
}

/// Filters flat list values using a boolean predicate, returning filtered values and
/// recomputed per-sublist offsets. Null predicate values are treated as false.
fn filter_list_values<O: OffsetSizeTrait>(
    values: &ArrayRef,
    predicate: &BooleanArray,
    offsets: &OffsetBuffer<O>,
) -> Result<(ArrayRef, OffsetBuffer<O>)> {
    let num_sublists = offsets.len().saturating_sub(1);
    let has_nulls = predicate.null_count() > 0;
    let new_offsets = OffsetBuffer::<O>::from_lengths((0..num_sublists).map(|i| {
        let start = offsets[i].as_usize();
        let end = offsets[i + 1].as_usize();
        if has_nulls {
            (start..end)
                .filter(|&j| predicate.is_valid(j) && predicate.value(j))
                .count()
        } else {
            predicate
                .values()
                .slice(start, end - start)
                .count_set_bits()
        }
    }));

    if new_offsets.last() == offsets.last() {
        return Ok((Arc::clone(values), offsets.clone()));
    }

    // arrow_filter treats null predicate values as false
    let filtered_values = arrow_filter(values.as_ref(), predicate)?;
    Ok((filtered_values, new_offsets))
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, AsArray},
        buffer::{NullBuffer, OffsetBuffer},
    };

    use crate::array_filter::array_filter_higher_order_function;
    use crate::lambda_utils::test_utils::{create_i32_list, eval_hof_on_i32_list, v};
    use datafusion_expr::lit;

    fn keep_greater_than_two(
        list: impl Array + Clone + 'static,
    ) -> datafusion_common::Result<arrow::array::ArrayRef> {
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
    fn filter_on_sliced_list_should_not_evaluate_on_unreachable_values() {
        // First sublist [0] is sliced away; sliced array covers sublists [1..3]
        let list = create_i32_list(
            vec![
                0, // unreachable after slice — if evaluated, it would appear in output
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
    fn filter_should_not_be_evaluated_on_values_underlying_null() {
        // The null sublist (index 1) contains values that would pass the predicate
        // if evaluated. We verify they do NOT appear in the output.
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
        assert_eq!(res.as_list::<i32>(), &list);
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
        assert_eq!(res.as_list::<i32>(), &list);
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
}
