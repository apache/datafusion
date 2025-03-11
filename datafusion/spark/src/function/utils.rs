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

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::Hint;
use datafusion_expr::ColumnarValue;

#[cfg(test)]
pub mod test {
    /// $FUNC ScalarUDFImpl to test
    /// $ARGS arguments (vec) to pass to function
    /// $EXPECTED a Result<ColumnarValue>
    /// $EXPECTED_TYPE is the expected value type
    /// $EXPECTED_DATA_TYPE is the expected result type
    /// $ARRAY_TYPE is the column type after function applied
    macro_rules! test_scalar_function {
        ($FUNC:expr, $ARGS:expr, $EXPECTED:expr, $EXPECTED_TYPE:ty, $EXPECTED_DATA_TYPE:expr, $ARRAY_TYPE:ident) => {
            let expected: Result<Option<$EXPECTED_TYPE>> = $EXPECTED;
            let func = $FUNC;

            let type_array = $ARGS.iter().map(|arg| arg.data_type()).collect::<Vec<_>>();
            let cardinality = $ARGS
                .iter()
                .fold(Option::<usize>::None, |acc, arg| match arg {
                    ColumnarValue::Scalar(_) => acc,
                    ColumnarValue::Array(a) => Some(a.len()),
                })
                .unwrap_or(1);

            let scalar_arguments = $ARGS.iter().map(|arg| match arg {
                ColumnarValue::Scalar(scalar) => Some(scalar.clone()),
                ColumnarValue::Array(_) => None,
            }).collect::<Vec<_>>();
            let scalar_arguments_refs = scalar_arguments.iter().map(|arg| arg.as_ref()).collect::<Vec<_>>();

            let nullables = $ARGS.iter().map(|arg| match arg {
                ColumnarValue::Scalar(scalar) => scalar.is_null(),
                ColumnarValue::Array(a) => a.null_count() > 0,
            }).collect::<Vec<_>>();

            let return_info = func.return_type_from_args(datafusion_expr::ReturnTypeArgs {
                arg_types: &type_array,
                scalar_arguments: &scalar_arguments_refs,
                nullables: &nullables
            });

            match expected {
                Ok(expected) => {
                    assert_eq!(return_info.is_ok(), true);
                    let (return_type, _nullable) = return_info.unwrap().into_parts();
                    assert_eq!(return_type, $EXPECTED_DATA_TYPE);

                    let result = func.invoke_with_args(datafusion_expr::ScalarFunctionArgs{args: $ARGS, number_rows: cardinality, return_type: &return_type});
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
                    if return_info.is_err() {
                        match return_info {
                            Ok(_) => assert!(false, "expected error"),
                            Err(error) => { datafusion_common::assert_contains!(expected_error.strip_backtrace(), error.strip_backtrace()); }
                        }
                    }
                    else {
                        let (return_type, _nullable) = return_info.unwrap().into_parts();

                        // invoke is expected error - cannot use .expect_err() due to Debug not being implemented
                        match func.invoke_with_args(datafusion_expr::ScalarFunctionArgs{args: $ARGS, number_rows: cardinality, return_type: &return_type}) {
                            Ok(_) => assert!(false, "expected error"),
                            Err(error) => {
                                assert!(expected_error.strip_backtrace().starts_with(&error.strip_backtrace()));
                            }
                        }
                    }
                }
            };
        };
    }

    use super::*;
    pub(crate) use test_scalar_function;
}
