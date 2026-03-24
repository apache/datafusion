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

//! [`LambdaUDF`] definitions for array_transform function.

use arrow::{
    array::{
        Array, ArrayRef, AsArray, FixedSizeListArray, GenericListArray, LargeListArray,
        ListArray, OffsetSizeTrait, UInt64Array, new_null_array,
    },
    buffer::OffsetBuffer,
    compute::take,
    datatypes::{DataType, Field, FieldRef},
};
use datafusion_common::{
    Result, exec_err, internal_datafusion_err, plan_err,
    utils::{adjust_offsets_for_slice, list_values, take_function_args},
};
use datafusion_expr::{
    ColumnarValue, Documentation, LambdaFunctionArgs, LambdaReturnFieldArgs,
    LambdaSignature, LambdaUDF, ValueOrLambda, Volatility,
};
use datafusion_macros::user_doc;
use std::{any::Any, fmt::Debug, sync::Arc};

make_udlf_expr_and_func!(
    ArrayTransform,
    array_transform,
    array lambda,
    "transforms the values of a array",
    array_transform_udlf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "transforms the values of a array",
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
    signature: LambdaSignature,
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
            signature: LambdaSignature::user_defined(Volatility::Immutable),
            aliases: vec![String::from("list_transform")],
        }
    }
}

impl LambdaUDF for ArrayTransform {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_transform"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &LambdaSignature {
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
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _) => list.clone(),
            DataType::ListView(field) => DataType::List(Arc::clone(field)),
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

    fn lambdas_parameters(
        &self,
        args: &[ValueOrLambda<FieldRef, ()>],
    ) -> Result<Vec<Option<Vec<Field>>>> {
        let (list, _lambda) = value_lambda_pair(self.name(), args)?;

        let field = match list.data_type() {
            DataType::List(field) => field,
            DataType::LargeList(field) => field,
            DataType::FixedSizeList(field, _) => field,
            _ => return plan_err!("expected list, got {list}"),
        };

        // we don't need to check whether the lambda contains more than two parameters,
        // e.g. array_transform([], (v, i, j) -> v+i+j), as datafusion will do that for us
        let value = Field::new("", field.data_type().clone(), field.is_nullable())
            .with_metadata(field.metadata().clone());

        Ok(vec![None, Some(vec![value])])
    }

    fn return_field_from_args(&self, args: LambdaReturnFieldArgs) -> Result<Arc<Field>> {
        let (list, lambda) = value_lambda_pair(self.name(), args.arg_fields)?;

        //TODO: should metadata be copied into the transformed array?

        // lambda is the resulting field of executing the lambda body
        // with the parameters returned in lambdas_parameters
        let field = Arc::new(Field::new(
            Field::LIST_FIELD_DEFAULT_NAME,
            lambda.data_type().clone(),
            lambda.is_nullable(),
        ));

        let return_type = match list.data_type() {
            DataType::List(_) => DataType::List(field),
            DataType::LargeList(_) => DataType::LargeList(field),
            DataType::FixedSizeList(_, size) => DataType::FixedSizeList(field, *size),
            other => plan_err!("expected list, got {other}")?,
        };

        Ok(Arc::new(Field::new("", return_type, list.is_nullable())))
    }

    fn invoke_with_args(&self, args: LambdaFunctionArgs) -> Result<ColumnarValue> {
        let (list, lambda) = value_lambda_pair(self.name(), &args.args)?;

        let list_array = list.to_array(args.number_rows)?;

        // Fast path for fully null input array and also the only way to safely work with
        // a fully null fixed size list array as it can't be handled by remove_list_null_values below
        if list_array.null_count() == list_array.len() {
            return Ok(ColumnarValue::Array(new_null_array(args.return_type(), list_array.len())))
        }

        // null sublists may contain values that cause problems, like a 0 used on a division
        // use remove_list_null_values to remove them
        let list_array = remove_list_null_values(&list_array)?;

        // as per list_values docs, if list_array is sliced, list_values will be sliced too,
        // so before constructing the transformed array below, we must adjust the list offsets with
        // adjust_offsets_for_slice
        let list_values = list_values(&list_array)?;

        // by passing closures, lambda.evaluate can evaluate only those actually needed
        let values_param = || Ok(Arc::clone(&list_values));

        // call the transforming lambda
        let transformed_values = lambda
            .evaluate(&[&values_param])?
            .into_array(list_values.len())?;

        let field = match args.return_field.data_type() {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => Arc::clone(field),
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
            DataType::FixedSizeList(_, value_length) => {
                Arc::new(FixedSizeListArray::new(
                    field,
                    *value_length,
                    transformed_values,
                    list_array.as_fixed_size_list().nulls().cloned(),
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

//todo: make this function public and move to a more generic crate like datafusion-common
fn remove_list_null_values(list: &dyn Array) -> Result<ArrayRef> {
    match list.data_type() {
        DataType::List(_) => {
            Ok(Arc::new(truncate_nulls(list.as_list::<i32>())?))
        }
        DataType::LargeList(_) => {
            Ok(Arc::new(truncate_nulls(list.as_list::<i64>())?))
        }
        DataType::FixedSizeList(_, _) => {
            Ok(Arc::new(replace_nulls_with_valid(list.as_fixed_size_list())?))
        }
        dt => exec_err!("expected list, got {dt}"),
    }
}

fn replace_nulls_with_valid(list: &FixedSizeListArray) -> Result<FixedSizeListArray> {
    if let Some(nulls) = list.nulls() {
        let null_count = list.null_count();

        if null_count > 0 {
            if null_count == list.len() {
                return exec_err!("no valid value to use");
            }

            let first_valid = nulls
                .inner()
                .set_indices()
                .next()
                .ok_or_else(|| internal_datafusion_err!("fixed size list should have been checked to contain at least one valid value"))? as u64;

            let mut indices = Vec::with_capacity(list.values().len());

            let size = list.value_length() as u64;

            for (i, is_valid) in nulls.iter().enumerate() {
                let range = if is_valid {
                    let i = i as u64;

                    i * size..(i + 1) * size
                } else {
                    first_valid * size..(first_valid + 1) * size
                };

                indices.extend(range)
            }

            let indices = UInt64Array::from(indices);
            let values = take(list.values(), &indices, None)?;

            let DataType::FixedSizeList(field, size) = list.data_type() else {
                unreachable!()
            };

            return Ok(FixedSizeListArray::try_new(
                Arc::clone(field),
                *size,
                values,
                list.nulls().cloned(),
            )?);
        }
    }

    Ok(list.clone())
}

fn truncate_nulls<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
) -> Result<GenericListArray<O>> {
    if let Some(nulls) = list.nulls() {
        if list.null_count() > 0 {
            let contains_null_and_non_empty =
                std::iter::zip(list.offsets().lengths(), nulls)
                    .any(|(len, is_valid)| len > 0 && !is_valid);

            if contains_null_and_non_empty {
                let mut indices = Vec::with_capacity(list.values().len());

                let lengths = list.offsets().windows(2).enumerate().map(|(i, window)| {
                    let start = window[0].as_usize();
                    let end = window[1].as_usize();

                    if list.is_valid(i) {
                        indices.extend((start..end).map(|i| i as u64));

                        end - start
                    } else {
                        0
                    }
                });

                let offsets = OffsetBuffer::from_lengths(lengths);
                let indices = UInt64Array::from(indices);
                let values = take(list.values(), &indices, None)?;

                let field = match list.data_type() {
                    DataType::List(field) => field,
                    DataType::LargeList(field) => field,
                    _ => unreachable!(),
                };

                return Ok(GenericListArray::try_new(
                    Arc::clone(field),
                    offsets,
                    values,
                    list.nulls().cloned(),
                )?);
            }
        }
    }

    Ok(list.clone())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, ArrayRef, AsArray, FixedSizeListArray, Int32Array, ListArray},
        buffer::{NullBuffer, OffsetBuffer},
        datatypes::{DataType, Field, FieldRef},
    };
    use datafusion_common::{DFSchema, Result, config::ConfigOptions};
    use datafusion_expr::{
        LambdaArgument, LambdaFunctionArgs, ValueOrLambda,
        execution_props::ExecutionProps, lambda_var, lit,
    };
    use datafusion_physical_expr::create_physical_expr;

    use crate::array_transform::array_transform_udlf;

    fn create_i32_list(
        values: impl Into<Int32Array>,
        offsets: OffsetBuffer<i32>,
        nulls: Option<NullBuffer>,
    ) -> ListArray {
        let list_field = Arc::new(Field::new_list_field(DataType::Int32, true));

        ListArray::new(list_field, offsets, Arc::new(values.into()), nulls)
    }

    fn create_i32_fsl(
        size: i32,
        values: Vec<i32>,
        nulls: Option<NullBuffer>,
    ) -> FixedSizeListArray {
        FixedSizeListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            size,
            Arc::new(Int32Array::from(values)),
            nulls,
        )
    }

    fn int32_field() -> FieldRef {
        Arc::new(Field::new("", DataType::Int32, true))
    }

    fn divide_100_by(list: impl Array + Clone + 'static) -> Result<ArrayRef> {
        let array_transform = array_transform_udlf();

        let lambda = create_physical_expr(
            &(lit(100i32) / lambda_var("v", int32_field())),
            &DFSchema::empty(),
            &ExecutionProps::new(),
        )?;

        array_transform
            .invoke_with_args(LambdaFunctionArgs {
                args: vec![
                    ValueOrLambda::Value(datafusion_expr::ColumnarValue::Array(
                        Arc::new(list.clone()),
                    )),
                    ValueOrLambda::Lambda(LambdaArgument::new(
                        vec![Arc::new(Field::new("v", DataType::Int32, true))],
                        lambda,
                    )),
                ],
                arg_fields: vec![
                    ValueOrLambda::Value(Arc::new(Field::new(
                        "",
                        list.data_type().clone(),
                        list.is_nullable(),
                    ))),
                    ValueOrLambda::Lambda(int32_field()),
                ],
                number_rows: list.len(),
                return_field: Arc::new(Field::new_list(
                    "",
                    Field::new_list_field(DataType::Int32, true),
                    list.is_nullable(),
                )),
                config_options: Arc::new(ConfigOptions::new()),
            })?
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
    fn transform_on_sliced_fsl_should_not_evaluate_on_unreachable_values() {
        let list = create_i32_fsl(
            3,
            vec![
                // Have 0 here so if the expression is called on data that it will fail
                0, 4, 100, 25, 20, 5, 2, 1, 10,
            ],
            None,
        )
        .slice(1, 2);

        let res = divide_100_by(list).unwrap();

        let actual_list = res.as_fixed_size_list();

        let expected_list = create_i32_fsl(3, vec![4, 5, 20, 50, 100, 10], None);

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
    fn transform_function_should_not_be_evaluated_on_values_underlying_null_fsl() {
        let list = create_i32_fsl(
            3,
            // 0 here for one of the values behind null, so if it will be evaluated
            // it will fail due to divide by 0
            vec![100, 20, 10, 0, 1, 2, 0, 1, 50],
            Some(NullBuffer::from(vec![true, false, false])),
        );

        let res = divide_100_by(list).unwrap();

        let actual_list = res.as_fixed_size_list();

        let expected_list = create_i32_fsl(
            3,
            vec![1, 5, 10, 1, 5, 10, 1, 5, 10],
            Some(NullBuffer::from(vec![true, false, false])),
        );

        assert_eq!(actual_list, &expected_list);
    }
}
