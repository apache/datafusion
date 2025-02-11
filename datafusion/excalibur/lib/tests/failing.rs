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

use arrow::array::BooleanArray;
use arrow::datatypes::DataType;
use datafusion_common::types::NativeType;
use datafusion_common::Result;
use datafusion_common::{exec_err, ScalarValue};
use datafusion_excalibur_macros::excalibur_function;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, Signature, TypeSignatureClass, Volatility,
};
use datafusion_expr_common::signature::Coercion;
use std::sync::Arc;

#[excalibur_function]
fn maybe_fail(fail: bool) -> Result<bool> {
    if fail {
        exec_err!("This test function just failed")
    } else {
        Ok(true)
    }
}

#[test]
fn test_function_signature() {
    let udf = maybe_fail_udf();
    assert_eq!(udf.name(), "maybe_fail");

    assert_eq!(
        udf.signature(),
        &Signature::coercible(
            vec![Coercion::new_exact(TypeSignatureClass::Native(Arc::new(
                NativeType::Boolean
            )))],
            Volatility::Immutable
        )
    );
    let return_type = udf.return_type(&[DataType::Boolean]).unwrap();
    assert_eq!(return_type, DataType::Boolean);
}

#[test]
fn test_invoke_array() {
    let udf = maybe_fail_udf();

    let invoke_args = vec![ColumnarValue::Array(Arc::new(BooleanArray::from(vec![
        false, false, false,
    ])))];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 3,
            return_type: &DataType::Boolean,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(&*result_array, &BooleanArray::from(vec![true, true, true]));
}

#[test]
fn test_invoke_array_fail() {
    let udf = maybe_fail_udf();

    let invoke_args = vec![ColumnarValue::Array(Arc::new(BooleanArray::from(vec![
        false, true, false,
    ])))];
    let error = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 3,
            return_type: &DataType::Boolean,
        })
        .err()
        .unwrap();

    assert_eq!(
        error.strip_backtrace(),
        "Execution error: This test function just failed"
    );
}

#[test]
fn test_invoke_array_with_nulls() {
    let udf = maybe_fail_udf();

    let invoke_args = vec![ColumnarValue::Array(Arc::new(BooleanArray::from(vec![
        Some(false),
        None,
        Some(false),
    ])))];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 3,
            return_type: &DataType::Boolean,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(
        &*result_array,
        &BooleanArray::from(vec![Some(true), None, Some(true)])
    );
}

#[test]
fn test_invoke_scalar() {
    let udf = maybe_fail_udf();

    let invoke_args = vec![ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)))];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 1,
            return_type: &DataType::Boolean,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(&*result_array, &BooleanArray::from(vec![true]));
}

#[test]
fn test_invoke_scalar_fail() {
    let udf = maybe_fail_udf();

    let invoke_args = vec![ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))];
    let error = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 1,
            return_type: &DataType::Boolean,
        })
        .err()
        .unwrap();

    assert_eq!(
        error.strip_backtrace(),
        "Execution error: This test function just failed"
    );
}

#[test]
fn test_invoke_scalar_null() {
    let udf = maybe_fail_udf();

    let invoke_args = vec![ColumnarValue::Scalar(ScalarValue::Boolean(None))];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 1,
            return_type: &DataType::Boolean,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(&*result_array, &BooleanArray::from(vec![None]));
}
