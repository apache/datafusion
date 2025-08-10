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

#[cfg(test)]
pub mod test {
    /// $FUNC ScalarUDFImpl to test
    /// $ARGS arguments (vec) to pass to function
    /// $EXPECTED a Result<ColumnarValue>
    /// $EXPECTED_TYPE is the expected value type
    /// $EXPECTED_DATA_TYPE is the expected result type
    /// $ARRAY_TYPE is the column type after function applied
    /// $CONFIG_OPTIONS config options to pass to function
    macro_rules! test_scalar_function {
        ($FUNC:expr, $ARGS:expr, $EXPECTED:expr, $EXPECTED_TYPE:ty, $EXPECTED_DATA_TYPE:expr, $ARRAY_TYPE:ident, $CONFIG_OPTIONS:expr) => {
            let expected: datafusion_common::Result<Option<$EXPECTED_TYPE>> = $EXPECTED;
            let func = $FUNC;

            let arg_fields: Vec<arrow::datatypes::FieldRef> = $ARGS
                .iter()
                .enumerate()
                .map(|(idx, arg)| {

                let nullable = match arg {
                    datafusion_expr::ColumnarValue::Scalar(scalar) => scalar.is_null(),
                    datafusion_expr::ColumnarValue::Array(a) => a.null_count() > 0,
                };

                std::sync::Arc::new(arrow::datatypes::Field::new(format!("arg_{idx}"), arg.data_type(), nullable))
            })
                .collect::<Vec<_>>();

            let cardinality = $ARGS
                .iter()
                .fold(Option::<usize>::None, |acc, arg| match arg {
                    datafusion_expr::ColumnarValue::Scalar(_) => acc,
                    datafusion_expr::ColumnarValue::Array(a) => Some(a.len()),
                })
                .unwrap_or(1);

            let scalar_arguments = $ARGS.iter().map(|arg| match arg {
                datafusion_expr::ColumnarValue::Scalar(scalar) => Some(scalar.clone()),
                datafusion_expr::ColumnarValue::Array(_) => None,
            }).collect::<Vec<_>>();
            let scalar_arguments_refs = scalar_arguments.iter().map(|arg| arg.as_ref()).collect::<Vec<_>>();


            let return_field = func.return_field_from_args(datafusion_expr::ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_arguments_refs
            });

            match expected {
                Ok(expected) => {
                    let return_field = return_field.unwrap();
                    assert_eq!(return_field.data_type(), &$EXPECTED_DATA_TYPE);

                    let result = func.invoke_with_args(datafusion_expr::ScalarFunctionArgs{
                        args: $ARGS,
                        number_rows: cardinality,
                        return_field,
                        arg_fields: arg_fields.clone(),
                        config_options: $CONFIG_OPTIONS,
                    });
                    assert_eq!(result.is_ok(), true, "function returned an error: {}", result.unwrap_err());

                    let result = result.unwrap().to_array(cardinality).expect("Failed to convert to array");
                    let result = result.as_any().downcast_ref::<$ARRAY_TYPE>().expect("Failed to convert to type");
                    assert_eq!(result.data_type(), &$EXPECTED_DATA_TYPE);

                    // value is correct
                    match expected {
                        Some(v) => assert_eq!(result.value(0), v),
                        None => assert!(result.is_null(0)),
                    };
                }
                Err(expected_error) => {
                    if return_field.is_err() {
                        match return_field {
                            Ok(_) => assert!(false, "expected error"),
                            Err(error) => { datafusion_common::assert_contains!(expected_error.strip_backtrace(), error.strip_backtrace()); }
                        }
                    }
                    else {
                        let return_field = return_field.unwrap();

                        // invoke is expected error - cannot use .expect_err() due to Debug not being implemented
                        match func.invoke_with_args(datafusion_expr::ScalarFunctionArgs{
                            args: $ARGS,
                            number_rows: cardinality,
                            return_field,
                            arg_fields,
                            config_options: $CONFIG_OPTIONS,
                        }) {
                            Ok(_) => assert!(false, "expected error"),
                            Err(error) => {
                                assert!(expected_error.strip_backtrace().starts_with(&error.strip_backtrace()));
                            }
                        }
                    }
                }
            };
        };

        ($FUNC:expr, $ARGS:expr, $EXPECTED:expr, $EXPECTED_TYPE:ty, $EXPECTED_DATA_TYPE:expr, $ARRAY_TYPE:ident) => {
            test_scalar_function!(
                $FUNC,
                $ARGS,
                $EXPECTED,
                $EXPECTED_TYPE,
                $EXPECTED_DATA_TYPE,
                $ARRAY_TYPE,
                std::sync::Arc::new(datafusion_common::config::ConfigOptions::default())
            )
        };
    }

    pub(crate) use test_scalar_function;
}
