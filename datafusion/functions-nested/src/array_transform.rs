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
    array::{Array, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray},
    compute::take_arrays,
    datatypes::{DataType, Field, FieldRef},
};
use datafusion_common::{
    exec_err, plan_err,
    utils::{
        adjust_offsets_for_slice, list_values, list_values_index, list_values_row_number,
        take_function_args,
    },
    Result,
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

    fn coerce_value_types(
        &self,
        arg_types: &[ValueOrLambda<DataType, ()>],
    ) -> Result<Vec<Option<DataType>>> {
        let (list, _lambda) = value_lambda_pair(self.name(), arg_types)?;

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
                )
            }
        };

        Ok(vec![Some(coerced), None])
    }

    fn lambdas_parameters(
        &self,
        args: &[ValueOrLambda<FieldRef, ()>],
    ) -> Result<Vec<Option<Vec<Field>>>> {
        let (list, _lambda) = value_lambda_pair(self.name(), args)?;

        let (field, index_type) = match list.data_type() {
            DataType::List(field) => (field, DataType::Int32),
            DataType::LargeList(field) => (field, DataType::Int64),
            DataType::FixedSizeList(field, _) => (field, DataType::Int32),
            _ => return plan_err!("expected list, got {list}"),
        };

        // we don't need to omit the index in the case the lambda don't specify, e.g. array_transform([], v -> v*2),
        // nor check whether the lambda contains more than two parameters, e.g. array_transform([], (v, i, j) -> v+i+j),
        // as datafusion will do that for us
        let value = Field::new("", field.data_type().clone(), field.is_nullable())
            .with_metadata(field.metadata().clone());
        let index = Field::new("", index_type, false);

        Ok(vec![None, Some(vec![value, index])])
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
        
        // as per list_values docs, if list_array is sliced, list_values will be sliced too,
        // so before constructing the transformed array below, we must adjust the list offsets with
        // adjust_offsets_for_slice
        let list_values = list_values(&list_array)?;

        // if any column got captured, we need to adjust it to the values arrays,
        // duplicating values of list with mulitple values and removing values of empty lists
        // list_values_row_number is not cheap so is important to avoid it when no column is captured
        let mut adjust_indices = None;

        // by passing closures, lambda.evaluate can evaluate only those actually needed
        let values_param = || Ok(Arc::clone(&list_values));
        let indices_param = || list_values_index(&list_array);

        // call the transforming lambda
        let transformed_values = lambda
            .evaluate(&[&values_param, &indices_param], |arrays| {
                let indices = match &adjust_indices {
                    Some(v) => v,
                    None => adjust_indices.insert(list_values_row_number(&list_array)?),
                };
                Ok(take_arrays(arrays, indices, None)?)
            })?
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
                )
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
