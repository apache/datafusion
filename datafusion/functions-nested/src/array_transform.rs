// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`datafusion_expr::HigherOrderUDF`] definitions for array_transform function.

use arrow::{
    array::{Array, ArrayRef, AsArray, Int64Array, LargeListArray, ListArray},
    compute::take_arrays,
    datatypes::{ArrowNativeType, DataType, Field, FieldRef},
};
use datafusion_common::{
    Result, exec_err, plan_err,
    utils::{adjust_offsets_for_slice, list_values_row_number, take_function_args},
};
use datafusion_expr::{
    ColumnarValue, Documentation, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs,
    HigherOrderSignature, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda,
    Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

use crate::lambda_utils::{
    ListValuesResult, coerce_single_list_arg, extract_list_values, value_lambda_pair,
};

make_higher_order_function_expr_and_func!(
    ArrayTransform,
    array_transform,
    array lambda,
    "transforms the values of an array",
    array_transform_higher_order_function
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "transforms the values of an array",
    syntax_example = "array_transform(array, lambda)",
    sql_example = r#"```sql
> select array_transform([1, 2, 3, 4, 5], x -> x*2);
+--------------------------------------------+
| array_transform([1, 2, 3, 4, 5], x -> x*2) |
+--------------------------------------------+
| [2, 4, 6, 8, 10]                           |
+--------------------------------------------+
> select array_transform([10, 20, 30], (x, i) -> x*i);
+----------------------------------------------+
| array_transform([10, 20, 30], (x, i) -> x*i) |
+----------------------------------------------+
| [10, 40, 90]                                 |
+----------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "lambda",
        description = "The lambda function used to transform each value of the array. It receives the value and, optionally, its 1-based index in the array: `(value, index) -> ...`."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayTransform {
    signature: HigherOrderSignature,
    aliases: Vec<String>,
}

impl Default for ArrayTransform {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayTransform {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("list_transform")],
        }
    }
}

impl HigherOrderUDFImpl for ArrayTransform {
    fn name(&self) -> &str {
        "array_transform"
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
        let (list, _lambda) = value_lambda_pair(self.name(), fields)?;

        let (DataType::List(field) | DataType::LargeList(field)) = list.data_type()
        else {
            return plan_err!("expected list, got {list}");
        };

        Ok(LambdaParametersProgress::Complete(vec![vec![
            Arc::clone(field),
            Arc::new(Field::new("index", DataType::Int64, false)),
        ]]))
    }

    fn return_field_from_args(
        &self,
        args: HigherOrderReturnFieldArgs,
    ) -> Result<Arc<Field>> {
        let [ValueOrLambda::Value(list), ValueOrLambda::Lambda(lambda)] =
            take_function_args(self.name(), args.arg_fields)?
        else {
            return plan_err!("{} expects a value followed by a lambda", self.name());
        };

        let field = Arc::new(
            lambda
                .as_ref()
                .clone()
                .with_name(Field::LIST_FIELD_DEFAULT_NAME),
        );

        let return_type = match list.data_type() {
            DataType::List(_) => DataType::List(field),
            DataType::LargeList(_) => DataType::LargeList(field),
            other => plan_err!("expected list, got {other}")?,
        };

        Ok(Arc::new(Field::new("", return_type, list.is_nullable())))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let [list, lambda] = take_function_args(self.name(), &args.args)?;
        let (ValueOrLambda::Value(list), ValueOrLambda::Lambda(lambda)) = (list, lambda)
        else {
            return plan_err!("{} expects a value followed by a lambda", self.name());
        };

        let list_array = list.to_array(args.number_rows)?;

        let list_values = match extract_list_values(&list_array, args.return_type())? {
            ListValuesResult::EarlyReturn(v) => return Ok(v),
            ListValuesResult::Values(v) => v,
        };

        // by passing closures, lambda.evaluate can evaluate only those actually needed
        let values_param = || Ok(Arc::clone(&list_values));
        let index_param = || list_values_index(&list_array);

        // call the transforming lambda
        let transformed_values = lambda
            .evaluate(&[&values_param, &index_param], |arrays| {
                // if any column got captured, we need to adjust it to the values arrays,
                // duplicating values of list with multitple values and removing values of empty lists
                let indices = list_values_row_number(&list_array)?;
                Ok(take_arrays(arrays, &indices, None)?)
            })?
            .into_array(list_values.len())?;

        let field = match args.return_field.data_type() {
            DataType::List(field) | DataType::LargeList(field) => Arc::clone(field),
            _ => {
                return exec_err!(
                    "{} expected ScalarFunctionArgs.return_field to be a list, got {}",
                    self.name(),
                    args.return_field
                );
            }
        };

        let transformed_list = match list_array.data_type() {
            DataType::List(_) => {
                let list = list_array.as_list();

                // since we called list_values above which would return sliced values for
                // a sliced list, we must adjust the offsets here as otherwise they would be invalid
                let adjusted_offsets = adjust_offsets_for_slice(list);

                Arc::new(ListArray::new(
                    field,
                    adjusted_offsets,
                    transformed_values,
                    list.nulls().cloned(),
                )) as ArrayRef
            }
            DataType::LargeList(_) => {
                let large_list = list_array.as_list();

                // since we called list_values above which would return sliced values for
                // a sliced list, we must adjust the offsets here as otherwise they would be invalid
                let adjusted_offsets = adjust_offsets_for_slice(large_list);

                Arc::new(LargeListArray::new(
                    field,
                    adjusted_offsets,
                    transformed_values,
                    large_list.nulls().cloned(),
                ))
            }
            other => exec_err!("expected list, got {other}")?,
        };

        Ok(ColumnarValue::Array(transformed_list))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Returns the 1-based position of each value within its list,
/// e.g. `[[a, b], [], [c]]` -> `[1, 2, 1]`.
fn list_values_index(list_array: &ArrayRef) -> Result<ArrayRef> {
    fn index<O: ArrowNativeType>(offsets: &[O]) -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(
            offsets
                .windows(2)
                .flat_map(|w| 1..=(w[1].as_usize() - w[0].as_usize()) as i64),
        ))
    }

    match list_array.data_type() {
        DataType::List(_) => Ok(index(list_array.as_list::<i32>().offsets())),
        DataType::LargeList(_) => Ok(index(list_array.as_list::<i64>().offsets())),
        other => exec_err!("expected list, got {other}"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, AsArray, Int64Array},
        buffer::{NullBuffer, OffsetBuffer},
        datatypes::{DataType, Field, Int64Type},
    };

    use crate::array_transform::array_transform_higher_order_function;
    use crate::lambda_utils::test_utils::{
        create_i32_list, eval_hof_on_i32_list, eval_hof_on_i32_list_with_params, v,
    };
    use datafusion_expr::expr::LambdaVariable;
    use datafusion_expr::{Expr, lit};

    fn divide_100_by(
        list: impl Array + Clone + 'static,
    ) -> datafusion_common::Result<arrow::array::ArrayRef> {
        eval_hof_on_i32_list(
            array_transform_higher_order_function(),
            list,
            lit(100i32) / v(),
        )
    }

    #[test]
    fn transform_on_sliced_list_should_not_evaluate_on_unreachable_values() {
        let list = create_i32_list(
            vec![
                // Have 0 here so if the expression is called on data that it will fail
                0, 4, 100, 25, 20, 5, 2, 1, 10,
            ],
            OffsetBuffer::<i32>::from_lengths(vec![1, 3, 4, 1]),
            None,
        )
        .slice(1, 3);

        let res = divide_100_by(list).unwrap();

        let actual_list = res.as_list::<i32>();

        let expected_list = create_i32_list(
            vec![25, 1, 4, 5, 20, 50, 100, 10],
            OffsetBuffer::<i32>::from_lengths(vec![3, 4, 1]),
            None,
        );

        assert_eq!(actual_list, &expected_list);
    }

    #[test]
    fn transform_function_should_not_be_evaluated_on_values_underlying_null() {
        let list = create_i32_list(
            // 0 here for one of the values behind null, so if it will be evaluated
            // it will fail due to divide by 0
            vec![100, 20, 10, 0, 1, 2, 0, 1, 50],
            OffsetBuffer::<i32>::from_lengths(vec![3, 4, 2]),
            Some(NullBuffer::from(vec![true, false, true])),
        );

        let res = divide_100_by(list).unwrap();

        let actual_list = res.as_list::<i32>();

        let expected_list = create_i32_list(
            vec![1, 5, 10, 100, 2],
            OffsetBuffer::<i32>::from_lengths(vec![3, 0, 2]),
            Some(NullBuffer::from(vec![true, false, true])),
        );

        assert_eq!(actual_list.data_type(), expected_list.data_type());
        assert_eq!(actual_list, &expected_list);
    }

    #[test]
    fn index_on_sliced_list_restarts_at_one_per_row() {
        let list = create_i32_list(
            vec![0, 1, 2, 3, 4, 5],
            OffsetBuffer::<i32>::from_lengths(vec![1, 3, 2]),
            None,
        )
        .slice(1, 2);

        let i = Expr::LambdaVariable(LambdaVariable::new(
            "i".to_string(),
            Some(Arc::new(Field::new("i", DataType::Int64, false))),
        ));

        let res = eval_hof_on_i32_list_with_params(
            array_transform_higher_order_function(),
            list,
            &["v", "i"],
            i,
        )
        .unwrap();

        let actual_list = res.as_list::<i32>();

        assert_eq!(actual_list.value_offsets(), &[0, 3, 5]);
        assert_eq!(
            actual_list.values().as_primitive::<Int64Type>(),
            &Int64Array::from(vec![1, 2, 3, 1, 2])
        );
    }
}
