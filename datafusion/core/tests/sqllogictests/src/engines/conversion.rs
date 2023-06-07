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

use arrow::datatypes::{Decimal128Type, DecimalType};
use bigdecimal::BigDecimal;
use half::f16;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;

pub const NULL_STR: &str = "NULL";

pub fn bool_to_str(value: bool) -> String {
    if value {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

pub fn varchar_to_str(value: &str) -> String {
    if value.is_empty() {
        "(empty)".to_string()
    } else {
        value.trim_end_matches('\n').to_string()
    }
}

pub fn f16_to_str(value: f16) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f16::INFINITY {
        "Infinity".to_string()
    } else if value == f16::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
    }
}

pub fn f32_to_str(value: f32) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f32::INFINITY {
        "Infinity".to_string()
    } else if value == f32::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
    }
}

pub fn f64_to_str(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "Infinity".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
    }
}

pub fn i128_to_str(value: i128, precision: &u8, scale: &i8) -> String {
    big_decimal_to_str(
        BigDecimal::from_str(&Decimal128Type::format_decimal(value, *precision, *scale))
            .unwrap(),
    )
}

pub fn decimal_to_str(value: Decimal) -> String {
    big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
}

pub fn big_decimal_to_str(value: BigDecimal) -> String {
    value.round(12).normalized().to_string()
}
