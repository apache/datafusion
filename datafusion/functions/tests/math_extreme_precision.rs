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

use std::sync::Arc;
use arrow::datatypes::{DataType, Field};
use datafusion_common::config::ConfigOptions;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions::math::round::RoundFunc;
use datafusion_functions::math::trunc::TruncFunc;

#[test]
fn test_round_float32_extreme_precision() {
    let round_func = RoundFunc::new();

    let args_overflow = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Scalar(ScalarValue::Float32(Some(1.5))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(40))),
        ],
        arg_fields: vec![
            Field::new("value", DataType::Float32, true).into(),
            Field::new("decimal_places", DataType::Int32, true).into(),
        ],
        number_rows: 1,
        return_field: Field::new("round", DataType::Float32, true).into(),
        config_options: Arc::new(ConfigOptions::default()),
    };

    let result = round_func.invoke_with_args(args_overflow).unwrap();
    if let ColumnarValue::Scalar(ScalarValue::Float32(Some(val))) = result {
        assert_eq!(val, 1.5_f32, "Expected 1.5, got: {val}");
    } else {
        panic!("Unexpected result shape: {:?}", result);
    }

    let args_underflow = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Scalar(ScalarValue::Float32(Some(1.5))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(-50))),
        ],
        arg_fields: vec![
            Field::new("value", DataType::Float32, true).into(),
            Field::new("decimal_places", DataType::Int32, true).into(),
        ],
        number_rows: 1,
        return_field: Field::new("round", DataType::Float32, true).into(),
        config_options: Arc::new(ConfigOptions::default()),
    };

    let result_underflow = round_func.invoke_with_args(args_underflow).unwrap();
    if let ColumnarValue::Scalar(ScalarValue::Float32(Some(val))) = result_underflow {
        assert_eq!(val, 0.0_f32, "Expected 0.0, got: {val}");
    } else {
        panic!("Unexpected result shape: {:?}", result_underflow);
    }
}

#[test]
fn test_trunc_float32_extreme_precision() {
    let trunc_func = TruncFunc::new();

    let args_overflow = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Scalar(ScalarValue::Float32(Some(42.5))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(40))),
        ],
        arg_fields: vec![
            Field::new("value", DataType::Float32, true).into(),
            Field::new("decimal_places", DataType::Int64, true).into(),
        ],
        number_rows: 1,
        return_field: Field::new("trunc", DataType::Float32, true).into(),
        config_options: Arc::new(ConfigOptions::default()),
    };

    let result = trunc_func.invoke_with_args(args_overflow).unwrap();
    if let ColumnarValue::Scalar(ScalarValue::Float32(Some(val))) = result {
        assert_eq!(val, 42.5_f32, "Expected 42.5, got: {val}");
    } else {
        panic!("Unexpected result shape: {:?}", result);
    }

    let args_underflow = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Scalar(ScalarValue::Float32(Some(42.5))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(-50))),
        ],
        arg_fields: vec![
            Field::new("value", DataType::Float32, true).into(),
            Field::new("decimal_places", DataType::Int64, true).into(),
        ],
        number_rows: 1,
        return_field: Field::new("trunc", DataType::Float32, true).into(),
        config_options: Arc::new(ConfigOptions::default()),
    };

    let result_underflow = trunc_func.invoke_with_args(args_underflow).unwrap();
    if let ColumnarValue::Scalar(ScalarValue::Float32(Some(val))) = result_underflow {
        assert_eq!(val, 0.0_f32, "Expected 0.0, got: {val}");
    } else {
        panic!("Unexpected result shape: {:?}", result_underflow);
    }
}

#[test]
fn test_round_float64_extreme_precision() {
    let round_func = RoundFunc::new();

    let args_overflow = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Scalar(ScalarValue::Float64(Some(123.456))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(309))),
        ],
        arg_fields: vec![
            Field::new("value", DataType::Float64, true).into(),
            Field::new("decimal_places", DataType::Int32, true).into(),
        ],
        number_rows: 1,
        return_field: Field::new("round", DataType::Float64, true).into(),
        config_options: Arc::new(ConfigOptions::default()),
    };

    let result = round_func.invoke_with_args(args_overflow).unwrap();
    if let ColumnarValue::Scalar(ScalarValue::Float64(Some(val))) = result {
        assert_eq!(val, 123.456_f64, "Expected 123.456, got: {val}");
    } else {
        panic!("Unexpected result shape: {:?}", result);
    }

    let args_underflow = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Scalar(ScalarValue::Float64(Some(123.456))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(-325))),
        ],
        arg_fields: vec![
            Field::new("value", DataType::Float64, true).into(),
            Field::new("decimal_places", DataType::Int32, true).into(),
        ],
        number_rows: 1,
        return_field: Field::new("round", DataType::Float64, true).into(),
        config_options: Arc::new(ConfigOptions::default()),
    };

    let result_underflow = round_func.invoke_with_args(args_underflow).unwrap();
    if let ColumnarValue::Scalar(ScalarValue::Float64(Some(val))) = result_underflow {
        assert_eq!(val, 0.0_f64, "Expected 0.0, got: {val}");
    } else {
        panic!("Unexpected result shape: {:?}", result_underflow);
    }
}
