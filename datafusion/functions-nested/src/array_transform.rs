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

//! [`HigherOrderUDF`] definitions for array_transform function.

use arrow::{
    array::{Array, ArrayRef, AsArray, LargeListArray, ListArray},
    datatypes::{DataType, Field, FieldRef},
};
use datafusion_common::{
    Result, ScalarValue, exec_err, plan_err,
    utils::{adjust_offsets_for_slice, list_values, take_function_args},
};
use datafusion_expr::{
    ColumnarValue, Documentation, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs,
    HigherOrderSignature, HigherOrderUDF, LambdaParametersProgress, ValueOrLambda,
    Volatility,
};
use datafusion_macros::user_doc;
use std::{fmt::Debug, sync::Arc};

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
    syntax_example = "array_transform(array, x -> x*2)",
    sql_example = r#"```sql
> select array_transform([1, 2, 3, 4, 5], x -> x*2);
+-------------------------------------------+
| array_transform([1, 2, 3, 4, 5], x -> x*2)       |
+-------------------------------------------+
| [2, 4, 6, 8, 10]                          |
+-------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(name = "lambda", description = "Lambda")
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
            signature: HigherOrderSignature::user_defined(Volatility::Immutable),
            aliases: vec![String::from("list_transform")],
        }
    }
}

impl HigherOrderUDF for ArrayTransform {
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
        let list = if arg_types.len() == 1 {
            &arg_types[0]
        } else {
            return plan_err!(
                "{} function requires 1 value arguments, got {}",
                self.name(),
                arg_types.len()
            );
        };

        let coerced = match list {
            DataType::List(_) | DataType::LargeList(_) => list.clone(),
            DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
                DataType::List(Arc::clone(field))
            }
            DataType::LargeListView(field) => DataType::LargeList(Arc::clone(field)),
            _ => {
                return plan_err!(
                    "{} expected a list as first argument, got {}",
                    self.name(),
                    list
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
            DataType::List(field) => field,
            DataType::LargeList(field) => field,
            _ => return plan_err!("expected list, got {list}"),
        };

        // we don't need to check whether the lambda contains more than two parameters,
        // e.g. array_transform([], (v, i, j) -> v+i+j), as datafusion will do that for us
        Ok(LambdaParametersProgress::Complete(vec![vec![Arc::clone(
            field,
        )]]))
    }

    fn return_field_from_args(
        &self,
        args: HigherOrderReturnFieldArgs,
    ) -> Result<Arc<Field>> {
        let (list, lambda) = value_lambda_pair(self.name(), args.arg_fields)?;

        //TODO: should metadata be copied into the transformed array?

        // lambda is the resulting field of executing the lambda body
        // with the parameters returned in lambda_parameters
        let field = Arc::new(Field::new(
            Field::LIST_FIELD_DEFAULT_NAME,
            lambda.data_type().clone(),
            lambda.is_nullable(),
        ));

        let return_type = match list.data_type() {
            DataType::List(_) => DataType::List(field),
            DataType::LargeList(_) => DataType::LargeList(field),
            other => plan_err!("expected list, got {other}")?,
        };

        Ok(Arc::new(Field::new("", return_type, list.is_nullable())))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (list, lambda) = value_lambda_pair(self.name(), &args.args)?;

        let list_array = list.to_array(args.number_rows)?;

        // Fast path for fully null input array
        if list_array.null_count() == list_array.len() {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_new_null(
                args.return_type(),
            )?));
        }

        // as per list_values docs, if list_array is sliced, list_values will be sliced too,
        // so before constructing the transformed array below, we must adjust the list offsets with
        // adjust_offsets_for_slice
        let list_values = list_values(&list_array)?;

        // fast path: when every sublist is empty and non-null we can return a scalar of an non-null empty sublist.
        // If every sublist is null have already been handled above
        if list_values.is_empty()
            && list_array.null_count() == 0
            && matches!(
                args.return_type(),
                DataType::List(_) | DataType::LargeList(_)
            )
        {
            return Ok(ColumnarValue::Scalar(ScalarValue::new_default(
                args.return_type(),
            )?));
        }

        // by passing closures, lambda.evaluate can evaluate only those actually needed
        let values_param = || Ok(Arc::clone(&list_values));

        // call the transforming lambda
        let transformed_values = lambda
            .evaluate(&[&values_param])?
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{Array, ArrayRef, AsArray, Int32Array, ListArray, RecordBatch},
        buffer::{NullBuffer, OffsetBuffer},
        datatypes::{DataType, Field},
    };
    use datafusion_common::{DFSchema, Result};
    use datafusion_expr::{
        Expr, col,
        execution_props::ExecutionProps,
        expr::{HigherOrderFunction, LambdaVariable},
        lambda, lit,
    };
    use datafusion_physical_expr::create_physical_expr;

    use crate::array_transform::array_transform_higher_order_function;

    fn create_i32_list(
        values: impl Into<Int32Array>,
        offsets: OffsetBuffer<i32>,
        nulls: Option<NullBuffer>,
    ) -> ListArray {
        let list_field = Arc::new(Field::new_list_field(DataType::Int32, true));

        ListArray::new(list_field, offsets, Arc::new(values.into()), nulls)
    }

    fn divide_100_by(list: impl Array + Clone + 'static) -> Result<ArrayRef> {
        let array_transform = array_transform_higher_order_function();

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
                array_transform,
                vec![
                    col("list"),
                    lambda(
                        ["v"],
                        lit(100i32)
                            / Expr::LambdaVariable(LambdaVariable::new(
                                "v".to_string(),
                                Some(Arc::new(Field::new("v", DataType::Int32, true))),
                            )),
                    ),
                ],
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
}
