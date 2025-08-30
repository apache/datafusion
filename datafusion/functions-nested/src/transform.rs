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

//! [`ScalarUDFImpl`] definition for array_transform function.

use arrow::array::{Array, ArrayRef, GenericListArray, NullArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::as_large_list_array;
use datafusion_common::cast::as_list_array;
use datafusion_common::{exec_datafusion_err, exec_err, plan_err, Result};
use datafusion_expr::type_coercion::functions::data_types_with_scalar_udf;
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarUDFImpl, Signature,
    TypeSignature,
};
use datafusion_expr::{Expr, ScalarUDF};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

#[doc = "ScalarFunction that returns a [`ScalarUDF`](datafusion_expr::ScalarUDF) for "]
#[doc = "ArrayTransform"]
pub fn array_transform_udf(
    inner: Arc<ScalarUDF>,
    argument_index: usize,
) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(<ArrayTransform>::new(
        inner,
        argument_index,
    )))
}

#[user_doc(
    doc_section(label = "Array Transform"),
    description = "Transform every element of an array according to a scalar function.",
    syntax_example = "array_transform(inner_function(arg1, arg2, arg3), arg_index)",
    sql_example = r#"```sql
> select array_transform(abs([-3,1,-4,2]), 0);
+-----------------------------------------+
| array_transform(abs([-3,1,-4,2]), 0)    |
+-----------------------------------------+
| [3,1,4,2]                               |
+-----------------------------------------+
```"#,
    argument(
        name = "inner_function",
        description = "Scalar function with arguments."
    ),
    argument(
        name = "arg_index",
        description = "0 based index that specifies which argument to the scalar function represents the array to transform."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayTransform {
    signature: Signature,
    aliases: Vec<String>,
    function: Arc<ScalarUDF>,
    argument_index: usize,
}

impl ArrayTransform {
    pub fn new(function: Arc<ScalarUDF>, argument_index: usize) -> Self {
        let signature = Signature {
            type_signature: TypeSignature::UserDefined,
            volatility: function.signature().volatility,
        };

        Self {
            signature,
            aliases: vec![String::from("list_transform")],
            function,
            argument_index,
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

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        let mut arg_names = args.iter().map(ToString::to_string).collect::<Vec<_>>();
        arg_names.insert(0, "[]".to_string());

        Ok(format!(
            "{}({})]",
            self.function.name(),
            arg_names.join(", ")
        ))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let mut arg_names = args.iter().map(ToString::to_string).collect::<Vec<_>>();
        arg_names.insert(0, "[]".to_string());

        Ok(format!(
            "{}({})]",
            self.function.name(),
            arg_names.join(", ")
        ))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        unimplemented!()
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let replacement_field =
            args.arg_fields
                .get(self.argument_index)
                .ok_or(exec_datafusion_err!(
                    "Invalid argument index {} for number of arguments provided {}",
                    self.argument_index,
                    args.arg_fields.len()
                ))?;

        let replacement_field = match replacement_field.data_type() {
            DataType::Null => {
                return Ok(Arc::new(Field::new("null", DataType::Null, true)))
            }
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field) => Ok(Arc::clone(field)),
            arg_type => plan_err!("{} does not support type {arg_type}", self.name()),
        }?;

        let mut inner_arg_fields = args.arg_fields.to_vec();
        inner_arg_fields[self.argument_index] = replacement_field;

        let inner_args = ReturnFieldArgs {
            arg_fields: &inner_arg_fields,
            scalar_arguments: args.scalar_arguments,
        };

        let inner_return = self.function.return_field_from_args(inner_args)?;
        let name = inner_return.name().to_owned();

        match args.arg_fields[self.argument_index].data_type() {
            DataType::List(_) => Ok(Arc::new(Field::new(
                name,
                DataType::List(inner_return),
                true,
            ))),
            DataType::ListView(_) => Ok(Arc::new(Field::new(
                name,
                DataType::ListView(inner_return),
                true,
            ))),
            DataType::LargeList(_) => Ok(Arc::new(Field::new(
                name,
                DataType::LargeList(inner_return),
                true,
            ))),
            DataType::LargeListView(_) => Ok(Arc::new(Field::new(
                name,
                DataType::LargeListView(inner_return),
                true,
            ))),
            _ => unreachable!(),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if self.argument_index >= arg_types.len() {
            return exec_err!(
                "Invalid argument index {} for array_transform with {} arguments.",
                self.argument_index,
                arg_types.len()
            );
        }

        let mut replacement_types = arg_types.to_vec();
        let replacement = match &arg_types[self.argument_index] {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field) => field.data_type().clone(),
            _ => {
                return exec_err!(
                    "Expected list type for the argument index {} in array_transform",
                    self.argument_index
                )
            }
        };
        replacement_types[self.argument_index] = replacement;

        let mut return_types =
            data_types_with_scalar_udf(&replacement_types, self.function.as_ref())?;

        let replacement_type = return_types[self.argument_index].clone();
        return_types[self.argument_index] = match &arg_types[self.argument_index] {
            DataType::List(field) => {
                DataType::List(Arc::new(Field::new(field.name(), replacement_type, true)))
            }
            DataType::LargeList(field) => DataType::LargeList(Arc::new(Field::new(
                field.name(),
                replacement_type,
                true,
            ))),
            DataType::ListView(field) => DataType::ListView(Arc::new(Field::new(
                field.name(),
                replacement_type,
                true,
            ))),
            DataType::LargeListView(field) => DataType::LargeListView(Arc::new(
                Field::new(field.name(), replacement_type, true),
            )),
            _ => unreachable!(),
        };

        Ok(return_types)
    }

    fn invoke_with_args(
        &self,
        mut args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let return_field = match args.return_field.data_type() {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field) => Arc::clone(field),
            _ => {
                return exec_err!(
                "Unexpected return field for array_transform. Expected list data type."
            )
            }
        };

        let replacement_array =
            args.args
                .get(self.argument_index)
                .ok_or(exec_datafusion_err!(
                    "Invalid number of arguments. Expected at least {} but received {}",
                    self.argument_index + 1,
                    args.args.len()
                ))?;

        let ColumnarValue::Array(replacement_array) = replacement_array else {
            return exec_err!("Unexpected scalar value in array_transform");
        };

        let result = match &replacement_array.data_type() {
            DataType::Null => {
                Ok(Arc::new(NullArray::new(replacement_array.len())) as ArrayRef)
            }
            DataType::List(_) => {
                let array = as_list_array(&replacement_array)?;
                let offsets = array.offsets().clone();
                let nulls = array.nulls().cloned();

                let values = array.values();

                args.args[self.argument_index] = ColumnarValue::Array(Arc::clone(values));

                let results = self.function.invoke_with_args(args)?;

                let ColumnarValue::Array(result_array) = results else {
                    return Ok(results);
                };

                Ok(Arc::new(GenericListArray::try_new(
                    return_field,
                    offsets,
                    result_array,
                    nulls,
                )?) as ArrayRef)
            }
            DataType::LargeList(_) => {
                let array = as_large_list_array(&replacement_array)?;
                let offsets = array.offsets().clone();
                let nulls = array.nulls().cloned();

                let values = array.values();

                args.args[self.argument_index] = ColumnarValue::Array(Arc::clone(values));

                let results = self.function.invoke_with_args(args)?;

                let ColumnarValue::Array(result_array) = results else {
                    return Ok(results);
                };

                Ok(Arc::new(GenericListArray::try_new(
                    return_field,
                    offsets,
                    result_array,
                    nulls,
                )?) as ArrayRef)
            }
            arg_type => {
                exec_err!("array_transform does not support type {arg_type}")
            }
        }?;

        Ok(ColumnarValue::Array(result))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use super::array_transform_udf;
    use arrow::array::{create_array, ArrayRef, GenericListArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::utils::SingleRowListArrayBuilder;
    use datafusion_common::{exec_err, DataFusionError, ScalarValue};
    use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs};
    use datafusion_functions::math::{abs, round};
    use std::sync::Arc;

    #[test]
    fn test_array_transform_apply_single_valued_function() -> Result<(), DataFusionError>
    {
        let udf = array_transform_udf(abs(), 0);

        let data = SingleRowListArrayBuilder::new(create_array!(
            Int32,
            [Some(1), Some(-2), None]
        ))
        .build_list_array();
        let data = Arc::new(data) as ArrayRef;
        let input_field = Arc::new(Field::new(
            "a",
            DataType::List(Field::new("b", DataType::Int32, true).into()),
            true,
        ));
        let return_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[Arc::clone(&input_field)],
            scalar_arguments: &[None],
        })?;

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(data)],
            arg_fields: vec![input_field],
            number_rows: 3,
            return_field,
            config_options: Arc::new(Default::default()),
        };

        let ColumnarValue::Array(result) = udf.invoke_with_args(args)? else {
            return exec_err!("Invalid return type");
        };
        let list_array = result
            .as_any()
            .downcast_ref::<GenericListArray<i32>>()
            .unwrap();

        let expected = create_array!(Int32, [Some(1), Some(2), None]) as ArrayRef;

        assert_eq!(&list_array.value(0), &expected);

        Ok(())
    }

    #[test]
    fn test_array_transform_test_argument_index() -> Result<(), DataFusionError> {
        let udf = array_transform_udf(round(), 1);

        let data = SingleRowListArrayBuilder::new(create_array!(
            Int32,
            [Some(1), Some(2), Some(3), None]
        ))
        .build_list_array();
        let data = Arc::new(data) as ArrayRef;
        let input_fields = vec![
            Arc::new(Field::new("b", DataType::Float64, true)),
            Arc::new(Field::new(
                "a",
                DataType::List(Field::new("b", DataType::Int64, true).into()),
                true,
            )),
        ];

        let original_value = ScalarValue::Float64(Some(0.123456));
        let return_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &input_fields,
            scalar_arguments: &[Some(&original_value), None],
        })?;

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(original_value),
                ColumnarValue::Array(data),
            ],
            arg_fields: input_fields,
            number_rows: 4,
            return_field,
            config_options: Arc::new(Default::default()),
        };

        let ColumnarValue::Array(result) = udf.invoke_with_args(args)? else {
            return exec_err!("Invalid return type");
        };
        let list_array = result
            .as_any()
            .downcast_ref::<GenericListArray<i32>>()
            .unwrap();

        let expected = create_array!(Float64, [Some(0.1), Some(0.12), Some(0.123), None])
            as ArrayRef;

        assert_eq!(&list_array.value(0), &expected);

        Ok(())
    }
}
