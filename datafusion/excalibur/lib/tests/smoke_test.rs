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

use arrow::array::UInt64Array;
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
fn add_one(a: u64) -> u64 {
    a + 1
}

#[test]
fn test_function_signature() {
    let udf = add_one_udf();
    assert_eq!(udf.name(), "add_one");

    assert_eq!(
        udf.signature(),
        &Signature::coercible(
            vec![Coercion::new_exact(TypeSignatureClass::Native(Arc::new(
                NativeType::UInt64
            )))],
            Volatility::Immutable
        )
    );
    let return_type = udf.return_type(&[DataType::UInt64]).unwrap();
    assert_eq!(return_type, DataType::UInt64);
}

#[test]
fn test_invoke_array() {
    let udf = add_one_udf();

    let invoke_args = vec![ColumnarValue::Array(Arc::new(UInt64Array::from(vec![
        1000, 2000, 3000, 4000, 5000,
    ])))];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 5,
            return_type: &DataType::UInt64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(
        &*result_array,
        &UInt64Array::from(vec![1001, 2001, 3001, 4001, 5001])
    );
}

#[test]
fn test_invoke_array_with_nulls() {
    let udf = add_one_udf();

    let invoke_args = vec![ColumnarValue::Array(Arc::new(UInt64Array::from(vec![
        Some(1000),
        None,
        Some(3000),
        None,
        Some(5000),
    ])))];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 5,
            return_type: &DataType::UInt64,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(
        &*result_array,
        &UInt64Array::from(vec![Some(1001), None, Some(3001), None, Some(5001)])
    );
}

#[test]
fn test_invoke_scalar() {
    let udf = add_one_udf();

    let invoke_args = vec![ColumnarValue::Scalar(ScalarValue::UInt64(Some(1000)))];
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

    assert_eq!(&*result_array, &UInt64Array::from(vec![1001]));
}

#[test]
fn test_invoke_scalar_null() {
    let udf = add_one_udf();

    let invoke_args = vec![ColumnarValue::Scalar(ScalarValue::UInt64(None))];
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
