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

//! [`datafusion_expr::HigherOrderUDF`] definitions for array_first function.

use arrow::{
    array::{
        Array, AsArray, BooleanArray, GenericListArray, OffsetSizeTrait, UInt64Array,
        UInt64Builder, new_null_array,
    },
    compute::{take, take_arrays},
    datatypes::{DataType, FieldRef},
};
use datafusion_common::{
    Result, exec_datafusion_err, exec_err, plan_err,
    utils::{adjust_offsets_for_slice, list_values, list_values_row_number},
};
use datafusion_expr::{
    ColumnarValue, Documentation, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs,
    HigherOrderSignature, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda,
    Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

use crate::lambda_utils::{
    coerce_single_list_arg, single_list_lambda_parameters, value_lambda_pair,
};

make_higher_order_function_expr_and_func!(
    ArrayFirst,
    array_first,
    array lambda,
    "returns the first element of an array that satisfies the predicate",
    array_first_higher_order_function
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the first element of an array that satisfies the given predicate. Returns null if the array is empty or no element matches. A predicate that returns null for an element is treated as not matching.",
    syntax_example = "array_first(array, predicate)",
    sql_example = r#"```sql
> select array_first([1, 2, 3, 4], x -> x > 2);
+----------------------------------------+
| array_first([1,2,3,4],x -> x > 2)      |
+----------------------------------------+
| 3                                      |
+----------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "predicate",
        description = "Lambda predicate that returns a boolean. The first element for which it returns true is returned."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayFirst {
    signature: HigherOrderSignature,
    aliases: Vec<String>,
}

impl Default for ArrayFirst {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayFirst {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("list_first")],
        }
    }
}

impl HigherOrderUDFImpl for ArrayFirst {
    fn name(&self) -> &str {
        "array_first"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_single_list_arg(self.name(), arg_types)
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
    ) -> Result<FieldRef> {
        let (list, _lambda) = value_lambda_pair(self.name(), args.arg_fields)?;

        let element_field = match list.data_type() {
            DataType::List(field) | DataType::LargeList(field) => field,
            other => {
                return plan_err!(
                    "{} expected a list as first argument, got {other}",
                    self.name()
                );
            }
        };

        // The result is a single element of the array. It is always nullable
        // because an empty array (or no matching element) yields null.
        Ok(Arc::new(
            element_field
                .as_ref()
                .clone()
                .with_name("")
                .with_nullable(true),
        ))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (list, lambda) = value_lambda_pair(self.name(), &args.args)?;

        let list_array = list.to_array(args.number_rows)?;

        // Fast path: fully null input. Also required for FixedSizeList which
        // can't be handled by clear_null_values when fully null.
        if list_array.null_count() == list_array.len() {
            return Ok(ColumnarValue::Array(new_null_array(
                args.return_type(),
                list_array.len(),
            )));
        }

        let list_values = list_values(&list_array)?;

        // Evaluate the predicate over every flat element. Captured columns are
        // spread to align with the flattened values via list_values_row_number.
        let values_param = || Ok(Arc::clone(&list_values));

        let predicate_results = lambda
            .evaluate(&[&values_param], |arrays| {
                let indices = list_values_row_number(&list_array)?;
                Ok(take_arrays(arrays, &indices, None)?)
            })?
            .into_array(list_values.len())?;

        let predicate_bool = predicate_results
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                exec_datafusion_err!(
                    "{} predicate must return boolean array, got {}",
                    self.name(),
                    predicate_results.data_type()
                )
            })?;

        // For each row, find the flat index of the first element whose predicate
        // is true. Rows with no match, including empty rows and null rows that
        // clear_null_values truncated to empty, map to a null index, producing
        // a null result via `take`.
        let indices = match list_array.data_type() {
            DataType::List(_) => {
                first_match_indices(list_array.as_list::<i32>(), predicate_bool)
            }
            DataType::LargeList(_) => {
                first_match_indices(list_array.as_list::<i64>(), predicate_bool)
            }
            other => return exec_err!("expected list, got {other}"),
        };

        let result = take(list_values.as_ref(), &indices, None)?;
        Ok(ColumnarValue::Array(result))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Builds a `UInt64` index array (one entry per sublist) pointing at the first
/// element whose predicate is true, or null when no element matches. Indices are
/// absolute into the (sliced) flat values array, so `take` gathers the matches.
///
/// A null predicate value is treated as not matching. The matched element itself
/// may be null and is still returned.
fn first_match_indices<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    predicate: &BooleanArray,
) -> UInt64Array {
    // Offsets are adjusted so that sliced lists index correctly into the
    // predicate / values arrays returned by list_values.
    let offsets = adjust_offsets_for_slice(list);
    let mut builder = UInt64Builder::with_capacity(list.len());

    for i in 0..list.len() {
        let start = offsets[i].as_usize();
        let end = offsets[i + 1].as_usize();

        match (start..end).find(|&j| predicate.is_valid(j) && predicate.value(j)) {
            Some(j) => builder.append_value(j as u64),
            None => builder.append_null(),
        }
    }

    builder.finish()
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, AsArray, Int32Array, StringArray},
        buffer::{NullBuffer, OffsetBuffer},
        datatypes::Int32Type,
    };

    use crate::array_first::array_first_higher_order_function;
    use crate::lambda_utils::test_utils::{create_i32_list, eval_hof_on_i32_list, v};
    use datafusion_common::Result;
    use datafusion_expr::lit;

    fn first_greater_than_two(
        list: impl Array + Clone + 'static,
    ) -> Result<arrow::array::ArrayRef> {
        eval_hof_on_i32_list(array_first_higher_order_function(), list, v().gt(lit(2i32)))
    }

    // predicate: (100 / v) > 5; panics on divide by zero if v == 0 is evaluated
    fn first_where_hundred_div_gt_five(
        list: impl Array + Clone + 'static,
    ) -> Result<arrow::array::ArrayRef> {
        eval_hof_on_i32_list(
            array_first_higher_order_function(),
            list,
            (lit(100i32) / v()).gt(lit(5i32)),
        )
    }

    #[test]
    fn test_first_basic() -> Result<()> {
        let list = create_i32_list(
            vec![1, 2, 3, 4, 5],
            OffsetBuffer::<i32>::from_lengths(vec![5]),
            None,
        );
        let res = first_greater_than_two(list)?;
        assert_eq!(
            res.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(3)])
        );
        Ok(())
    }

    #[test]
    fn test_first_no_match_is_null() -> Result<()> {
        let list =
            create_i32_list(vec![1, 2], OffsetBuffer::<i32>::from_lengths(vec![2]), None);
        let res = first_greater_than_two(list)?;
        assert_eq!(
            res.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![None])
        );
        Ok(())
    }

    #[test]
    fn test_first_empty_array_is_null() -> Result<()> {
        let list = create_i32_list(
            Vec::<i32>::new(),
            OffsetBuffer::<i32>::from_lengths(vec![0]),
            None,
        );
        let res = first_greater_than_two(list)?;
        assert_eq!(
            res.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![None])
        );
        Ok(())
    }

    #[test]
    fn test_first_multiple_sublists() -> Result<()> {
        // [1,5] -> 5, [2,4,3] -> 4, [1,2] -> null
        let list = create_i32_list(
            vec![1, 5, 2, 4, 3, 1, 2],
            OffsetBuffer::<i32>::from_lengths(vec![2, 3, 2]),
            None,
        );
        let res = first_greater_than_two(list)?;
        assert_eq!(
            res.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(5), Some(4), None])
        );
        Ok(())
    }

    #[test]
    fn test_first_null_predicate_element_is_skipped() -> Result<()> {
        // [1, NULL, 4] with v > 2: the NULL element's predicate is null and is
        // skipped, so the first match is 4.
        let list = create_i32_list(
            Int32Array::from(vec![Some(1), None, Some(4)]),
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );
        let res = first_greater_than_two(list)?;
        assert_eq!(
            res.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(4)])
        );
        Ok(())
    }

    #[test]
    fn test_first_matched_null_element_is_returned() -> Result<()> {
        // [1, NULL, 3] with `v IS NULL`: the first match is the null element,
        // which is returned as null.
        let list = create_i32_list(
            Int32Array::from(vec![Some(1), None, Some(3)]),
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );
        let res = eval_hof_on_i32_list(
            array_first_higher_order_function(),
            list,
            v().is_null(),
        )?;
        assert_eq!(
            res.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![None])
        );
        Ok(())
    }

    // The 0 in the null row would divide by zero if the predicate were evaluated
    // on it. The result for the null row must be null.
    #[test]
    fn test_first_does_not_evaluate_predicate_on_null_row_values() -> Result<()> {
        let list = create_i32_list(
            vec![1, 2, 0, 4, 5],
            OffsetBuffer::<i32>::from_lengths(vec![3, 2]),
            Some(NullBuffer::from(vec![false, true])),
        );
        let res = first_where_hundred_div_gt_five(list)?;
        assert_eq!(
            res.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![None, Some(4)])
        );
        Ok(())
    }

    // The 0 before the slice offset would divide by zero if evaluated.
    #[test]
    fn test_first_does_not_evaluate_predicate_on_unreachable_values() -> Result<()> {
        // sublists: [0], [4,5], [50,100]; slice away the first
        let list = create_i32_list(
            vec![0, 4, 5, 50, 100],
            OffsetBuffer::<i32>::from_lengths(vec![1, 2, 2]),
            None,
        )
        .slice(1, 2);
        let res = first_where_hundred_div_gt_five(list)?;
        // [4,5]: 100/4=25>5 -> 4. [50,100]: 2>5 false, 1>5 false -> null
        assert_eq!(
            res.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(4), None])
        );
        Ok(())
    }

    #[test]
    fn test_first_string_elements() -> Result<()> {
        use arrow::array::ListArray;
        use arrow::datatypes::{DataType, Field};
        use datafusion_expr::Expr;
        use datafusion_expr::expr::LambdaVariable;
        use std::sync::Arc;

        // ['a', 'bb', 'ccc'] with v > 'a' -> 'bb' (exercises take on a non-primitive type)
        let values = StringArray::from(vec!["a", "bb", "ccc"]);
        let list = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Utf8, true)),
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            Arc::new(values),
            None,
        );

        let x = Expr::LambdaVariable(LambdaVariable::new(
            "v".to_string(),
            Some(Arc::new(Field::new("v", DataType::Utf8, true))),
        ));
        let body = x.gt(lit("a"));

        let res = eval_hof_on_i32_list(array_first_higher_order_function(), list, body)?;
        assert_eq!(res.as_string::<i32>(), &StringArray::from(vec![Some("bb")]));
        Ok(())
    }
}
