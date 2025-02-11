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

use arrow::array::{Int32Array, Int64Array, UInt32Array};
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
fn add(a: i32, b: u32) -> i64 {
    a as i64 + b as i64
}

#[test]
fn test_function_signature() {
    let udf = add_udf();
    assert_eq!(udf.name(), "add");

    assert_eq!(
        udf.signature(),
        &Signature::coercible(
            vec![
                Coercion::new_exact(TypeSignatureClass::Native(Arc::new(
                    NativeType::Int32
                ))),
                Coercion::new_exact(TypeSignatureClass::Native(Arc::new(
                    NativeType::UInt32
                ))),
            ],
            Volatility::Immutable
        )
    );
    let return_type = udf
        .return_type(&[DataType::Int32, DataType::UInt32])
        .unwrap();
    assert_eq!(return_type, DataType::Int64);
}

#[test]
fn test_invoke_array() {
    let udf = add_udf();

    let invoke_args = vec![
        ColumnarValue::Array(Arc::new(Int32Array::from(vec![1000, 2, 3000, -4, 5000]))),
        ColumnarValue::Array(Arc::new(UInt32Array::from(vec![5, 111, 3000, 0, 13]))),
    ];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 5,
            return_type: &DataType::Int64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(
        &*result_array,
        &Int64Array::from(vec![1005, 113, 6000, -4, 5013])
    );
}

#[test]
fn test_invoke_array_with_nulls() {
    let udf = add_udf();

    let invoke_args = vec![
        ColumnarValue::Array(Arc::new(Int32Array::from(vec![
            None,
            Some(2),
            Some(3000),
            Some(-4),
            Some(5000),
        ]))),
        ColumnarValue::Array(Arc::new(UInt32Array::from(vec![
            Some(5),
            Some(111),
            None,
            Some(0),
            Some(13),
        ]))),
    ];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 5,
            return_type: &DataType::Int64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(
        &*result_array,
        &Int64Array::from(vec![None, Some(113), None, Some(-4), Some(5013)])
    );
}

#[test]
fn test_invoke_scalar() {
    let udf = add_udf();

    let invoke_args = vec![
        ColumnarValue::Scalar(ScalarValue::Int32(Some(-3))),
        ColumnarValue::Scalar(ScalarValue::UInt32(Some(55))),
    ];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 1,
            return_type: &DataType::Int64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(&*result_array, &Int64Array::from(vec![52]));
}

#[test]
fn test_invoke_scalar_null() {
    let udf = add_udf();

    let invoke_args = vec![
        ColumnarValue::Scalar(ScalarValue::Int32(Some(-3))),
        ColumnarValue::Scalar(ScalarValue::UInt32(None)),
    ];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 1,
            return_type: &DataType::Int64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(&*result_array, &Int64Array::from(vec![None]));
}
