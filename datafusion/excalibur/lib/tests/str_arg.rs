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

use arrow::array::{StringArray, StringViewArray, UInt64Array};
use arrow::datatypes::DataType;
use datafusion_common::types::NativeType;
use datafusion_common::ScalarValue;
use datafusion_excalibur_macros::excalibur_function;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, Signature, TypeSignatureClass, Volatility,
};
use datafusion_expr_common::signature::Coercion;
use std::sync::Arc;

#[excalibur_function]
fn character_length(s: &str) -> u64 {
    s.chars().count() as u64
}

#[test]
fn test_function_signature() {
    let udf = character_length_udf();
    assert_eq!(udf.name(), "character_length");

    assert_eq!(
        udf.signature(),
        &Signature::coercible(
            vec![Coercion::new_exact(TypeSignatureClass::Native(Arc::new(
                NativeType::String
            )))],
            Volatility::Immutable
        )
    );
    let return_type = udf.return_type(&[DataType::Utf8]).unwrap();
    assert_eq!(return_type, DataType::UInt64);
}

#[test]
fn test_invoke_string_array() {
    let udf = character_length_udf();

    let invoke_args = vec![ColumnarValue::Array(Arc::new(StringArray::from(vec![
        "",
        "abc",
        "Idę piękną łąką pod Warszawą",
    ])))];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 3,
            return_type: &DataType::UInt64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(&*result_array, &UInt64Array::from(vec![0, 3, 28]));
}

#[test]
fn test_invoke_string_view_array() {
    let udf = character_length_udf();

    let invoke_args = vec![ColumnarValue::Array(Arc::new(StringViewArray::from(vec![
        "",
        "abc",
        "Idę piękną łąką pod Warszawą",
    ])))];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 3,
            return_type: &DataType::UInt64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(&*result_array, &UInt64Array::from(vec![0, 3, 28]));
}

#[test]
fn test_invoke_array_with_nulls() {
    let udf = character_length_udf();

    let invoke_args = vec![ColumnarValue::Array(Arc::new(StringArray::from(vec![
        Some(""),
        None,
        Some("Idę piękną łąką pod Warszawą"),
    ])))];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 3,
            return_type: &DataType::UInt64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(
        &*result_array,
        &UInt64Array::from(vec![Some(0), None, Some(28)])
    );
}

#[test]
fn test_invoke_scalar_utf8() {
    let udf = character_length_udf();

    let invoke_args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
        "Idę piękną łąką pod Warszawą".to_string(),
    )))];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 1,
            return_type: &DataType::UInt64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(&*result_array, &UInt64Array::from(vec![Some(28)]));
}

#[test]
fn test_invoke_scalar_utf8view() {
    let udf = character_length_udf();

    let invoke_args = vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
        "Idę piękną łąką pod Warszawą".to_string(),
    )))];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 1,
            return_type: &DataType::UInt64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(&*result_array, &UInt64Array::from(vec![Some(28)]));
}

#[test]
fn test_invoke_scalar_null() {
    let udf = character_length_udf();

    let invoke_args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(None))];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 1,
            return_type: &DataType::UInt64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(&*result_array, &UInt64Array::from(vec![None]));
}
