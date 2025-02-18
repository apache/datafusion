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

use arrow::array::Int32Array;
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
fn try_div(a: i32, b: i32) -> Option<i32> {
    if b == 0 {
        None
    } else {
        Some(a / b)
    }
}

#[test]
fn test_function_signature() {
    let udf = try_div_udf();
    assert_eq!(udf.name(), "try_div");

    assert_eq!(
        udf.signature(),
        &Signature::coercible(
            vec![
                Coercion::new_exact(TypeSignatureClass::Native(Arc::new(
                    NativeType::Int32
                ))),
                Coercion::new_exact(TypeSignatureClass::Native(Arc::new(
                    NativeType::Int32
                ))),
            ],
            Volatility::Immutable
        )
    );
    let return_type = udf
        .return_type(&[DataType::Int32, DataType::Int32])
        .unwrap();
    assert_eq!(return_type, DataType::Int32);
}

#[test]
fn test_invoke_array() {
    let udf = try_div_udf();

    let invoke_args = vec![
        ColumnarValue::Array(Arc::new(Int32Array::from(vec![0, 3, 15, 0, 3, 60]))),
        ColumnarValue::Array(Arc::new(Int32Array::from(vec![1, 3, 3, 0, 0, 15]))),
    ];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 6,
            return_type: &DataType::Int32,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(
        &*result_array,
        &Int32Array::from(vec![Some(0), Some(1), Some(5), None, None, Some(4)])
    );
}

#[test]
fn test_invoke_array_with_nulls() {
    let udf = try_div_udf();

    let invoke_args = vec![
        ColumnarValue::Array(Arc::new(Int32Array::from(vec![
            Some(0),
            None,
            Some(15),
            Some(0),
            Some(3),
            Some(60),
        ]))),
        ColumnarValue::Array(Arc::new(Int32Array::from(vec![
            Some(1),
            Some(3),
            None,
            Some(0),
            Some(0),
            Some(15),
        ]))),
    ];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 6,
            return_type: &DataType::Int32,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(
        &*result_array,
        &Int32Array::from(vec![Some(0), None, None, None, None, Some(4)])
    );
}

#[test]
fn test_invoke_scalar() {
    let udf = try_div_udf();

    let invoke_args = vec![
        ColumnarValue::Scalar(ScalarValue::Int32(Some(33))),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(0))),
    ];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 1,
            return_type: &DataType::Int32,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(&*result_array, &Int32Array::from(vec![None]));
}

#[test]
fn test_invoke_scalar_null() {
    let udf = try_div_udf();

    let invoke_args = vec![
        ColumnarValue::Scalar(ScalarValue::Int32(Some(-3))),
        ColumnarValue::Scalar(ScalarValue::Int32(None)),
    ];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 1,
            return_type: &DataType::Int32,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(&*result_array, &Int32Array::from(vec![None]));
}
