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

use arrow::array::StringArray;
use arrow::datatypes::DataType;
use datafusion_common::types::NativeType;
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_excalibur::ValuePresence;
use datafusion_excalibur_macros::excalibur_function;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, Signature, TypeSignatureClass, Volatility,
};
use datafusion_expr_common::signature::Coercion;
use std::sync::Arc;

#[excalibur_function]
fn concat(a: &str, b: &str, out: &mut impl std::fmt::Write) -> Result<ValuePresence> {
    if a.is_empty() && b.is_empty() {
        // Be like Oracle
        return Ok(ValuePresence::Null);
    }
    out.write_str(a)?;
    out.write_str(b)?;
    Ok(ValuePresence::Value)
}

#[test]
fn test_function_signature() {
    let udf = concat_udf();
    assert_eq!(udf.name(), "concat");

    assert_eq!(
        udf.signature(),
        &Signature::coercible(
            vec![
                Coercion::new_exact(TypeSignatureClass::Native(Arc::new(
                    NativeType::String
                ))),
                Coercion::new_exact(TypeSignatureClass::Native(Arc::new(
                    NativeType::String
                ))),
            ],
            Volatility::Immutable
        )
    );
    let return_type = udf.return_type(&[DataType::Utf8, DataType::Utf8]).unwrap();
    assert_eq!(return_type, DataType::Utf8);
}

#[test]
fn test_invoke_array() {
    let udf = concat_udf();

    let invoke_args = vec![
        ColumnarValue::Array(Arc::new(StringArray::from(vec![
            "hello, ",
            "",
            "Idę piękną łąką",
        ]))),
        ColumnarValue::Array(Arc::new(StringArray::from(vec![
            "world!",
            "",
            " pod Warszawą",
        ]))),
    ];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 3,
            return_type: &DataType::Utf8,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(
        &*result_array,
        &StringArray::from(vec![
            Some("hello, world!"),
            None,
            Some("Idę piękną łąką pod Warszawą"),
        ])
    );
}

#[test]
fn test_invoke_array_with_nulls() {
    let udf = concat_udf();

    let invoke_args = vec![
        ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some(""),
            Some(""),
            None,
            None,
            Some("Idę piękną łąką"),
            Some("Idę piękną łąką"),
            None,
        ]))),
        ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some(""),
            None,
            Some(""),
            None,
            Some("Idę piękną łąką"),
            None,
            Some("Idę piękną łąką"),
        ]))),
    ];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 7,
            return_type: &DataType::Utf8,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(
        &*result_array,
        &StringArray::from(vec![
            None,
            None,
            None,
            None,
            Some("Idę piękną łąkąIdę piękną łąką"),
            None,
            None,
        ])
    );
}

#[test]
fn test_invoke_scalar() {
    let udf = concat_udf();

    let invoke_args = vec![
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("Idę piękną łąką".to_string()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(" pod Warszawą".to_string()))),
    ];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 1,
            return_type: &DataType::Utf8,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(
        &*result_array,
        &StringArray::from(vec![("Idę piękną łąką pod Warszawą"),])
    );
}

#[test]
fn test_invoke_scalar_null() {
    let udf = concat_udf();

    let invoke_args = vec![
        ColumnarValue::Scalar(ScalarValue::Utf8(None)),
        ColumnarValue::Scalar(ScalarValue::Utf8(None)),
    ];
    let ColumnarValue::Array(result_array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: invoke_args,
            number_rows: 1,
            return_type: &DataType::Utf8,
        })
        .unwrap()
    else {
        panic!("Expected array result");
    };

    assert_eq!(&*result_array, &StringArray::from(vec![None::<&str>]));
}
