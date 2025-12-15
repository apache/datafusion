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

//! [`ScalarUDFImpl`] definitions for array_transform function.

use arrow::{
    array::{
        Array, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray,
        RecordBatch, RecordBatchOptions,
    },
    compute::take_record_batch,
    datatypes::{DataType, Field, FieldRef, Schema},
};
use datafusion_common::{
    HashMap, Result, exec_err, internal_err, tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter}, utils::{elements_indices, list_indices, list_values, take_function_args}
};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    ValueOrLambda, ValueOrLambdaField, ValueOrLambdaParameter, Volatility,
};
use datafusion_macros::user_doc;
use datafusion_physical_expr::expressions::{LambdaColumn, LambdaExpr};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::{any::Any, sync::Arc};

make_udf_expr_and_func!(
    ArrayTransform,
    array_transform,
    array lambda,
    "transforms the values of a array",
    array_transform_udf
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
    signature: Signature,
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
            signature: Signature::any(2, Volatility::Immutable),
            aliases: vec![String::from("list_transform")],
        }
    }
}

impl ScalarUDFImpl for ArrayTransform {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_transform"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_type called instead of return_field_from_args")
    }

    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> Result<Arc<Field>> {
        let args = args.to_lambda_args();

        let [ValueOrLambdaField::Value(list), ValueOrLambdaField::Lambda(lambda)] =
            take_function_args(self.name(), &args)?
        else {
            return exec_err!(
                "{} expects a value follewed by a lambda, got {:?}",
                self.name(),
                args
            );
        };

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
            _ => unreachable!(),
        };

        Ok(Arc::new(Field::new("", return_type, list.is_nullable())))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // args.lambda_args allows the convenient match below, instead of inspecting both args.args and args.lambdas
        let lambda_args = args.to_lambda_args();
        let [list_value, lambda] = take_function_args(self.name(), &lambda_args)?;

        let (ValueOrLambda::Value(list_value), ValueOrLambda::Lambda(lambda)) =
            (list_value, lambda)
        else {
            return exec_err!(
                "{} expects a value followed by a lambda, got {:?}",
                self.name(),
                &lambda_args
            );
        };

        let list_array = list_value.to_array(args.number_rows)?;
        let list_values = list_values(&list_array)?;

        // if any column got captured, we need to adjust it to the values arrays,
        // duplicating values of list with mulitple values and removing values of empty lists
        // list_indices is not cheap so is important to avoid it when no column is captured
        let adjusted_captures = lambda
            .captures
            .as_ref()
            .map(|captures| take_record_batch(captures, &list_indices(&list_array)?))
            .transpose()?;

        // use closures and merge_captures_with_lazy_args so that it calls only the needed ones based on the number of arguments
        // avoiding unnecessary computations
        let values_param = || Ok(Arc::clone(list_values));
        let indices_param = || elements_indices(&list_array);

        let binded_body = bind_lambda_columns(
            Arc::clone(&lambda.body),
            &lambda.params,
            &[&values_param, &indices_param],
        )?;

        // call the transforming expression with the record batch composed of the list values merged with captured columns
        let transformed_values = binded_body
            .evaluate(&adjusted_captures.unwrap_or_else(|| {
                RecordBatch::try_new_with_options(
                    Arc::new(Schema::empty()),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(list_values.len())),
                )
                .unwrap()
            }))?
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

                Arc::new(ListArray::new(
                    field,
                    list.offsets().clone(),
                    transformed_values,
                    list.nulls().cloned(),
                )) as ArrayRef
            }
            DataType::LargeList(_) => {
                let large_list = list_array.as_list();

                Arc::new(LargeListArray::new(
                    field,
                    large_list.offsets().clone(),
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

    fn lambdas_parameters(
        &self,
        args: &[ValueOrLambdaParameter],
    ) -> Result<Vec<Option<Vec<Field>>>> {
        let [ValueOrLambdaParameter::Value(list), ValueOrLambdaParameter::Lambda] =
            args
        else {
            return exec_err!(
                "{} expects a value follewed by a lambda, got {:?}",
                self.name(),
                args
            );
        };

        let (field, index_type) = match list.data_type() {
            DataType::List(field) => (field, DataType::Int32),
            DataType::LargeList(field) => (field, DataType::Int64),
            DataType::FixedSizeList(field, _) => (field, DataType::Int32),
            _ => return exec_err!("expected list, got {list}"),
        };

        // we don't need to omit the index in the case the lambda don't specify, e.g. array_transform([], v -> v*2),
        // nor check whether the lambda contains more than two parameters, e.g. array_transform([], (v, i, j) -> v+i+j),
        // as datafusion will do that for us
        let value = Field::new("", field.data_type().clone(), field.is_nullable())
            .with_metadata(field.metadata().clone());
        let index = Field::new("", index_type, false);

        Ok(vec![None, Some(vec![value, index])])
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn bind_lambda_columns(
    expr: Arc<dyn PhysicalExpr>,
    params: &[FieldRef],
    args: &[&dyn Fn() -> Result<ArrayRef>],
) -> Result<Arc<dyn PhysicalExpr>> {
    let columns = std::iter::zip(params, args)
        .map(|(param, arg)| Ok((param.name().as_str(), (arg()?, 0))))
        .collect::<Result<HashMap<_, _>>>()?;

    expr.rewrite(&mut BindLambdaColumn::new(columns)).data()
}

struct BindLambdaColumn<'a> {
    columns: HashMap<&'a str, (ArrayRef, usize)>,
}

impl<'a> BindLambdaColumn<'a> {
    fn new(columns: HashMap<&'a str, (ArrayRef, usize)>) -> Self {
        Self { columns }
    }
}

impl TreeNodeRewriter for BindLambdaColumn<'_> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        if let Some(lambda_column) = node.as_any().downcast_ref::<LambdaColumn>() {
            if let Some((value, shadows)) = self.columns.get(lambda_column.name()) {
                if *shadows == 0 {
                    return Ok(Transformed::yes(Arc::new(
                        lambda_column.clone().with_value(value.clone()),
                    )));
                }
            }
        } else if let Some(inner_lambda) = node.as_any().downcast_ref::<LambdaExpr>() {
            for param in inner_lambda.params() {
                if let Some((_value, shadows)) = self.columns.get_mut(param.as_str()) {
                    *shadows += 1;
                }
            }

            if self.columns.values().all(|(_value, shadows)| *shadows > 0) {
                return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump))
            }
        }

        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        if let Some(inner_lambda) = node.as_any().downcast_ref::<LambdaExpr>() {
            for param in inner_lambda.params() {
                if let Some((_value, shadows)) = self.columns.get_mut(param.as_str()) {
                    *shadows -= 1;
                }
            }
        }

        Ok(Transformed::no(node))
    }
}
