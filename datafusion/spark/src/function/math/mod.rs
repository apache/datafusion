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

use std::cmp::{max, min};

pub mod ceil_floor;
pub mod expm1;

use arrow::datatypes::{DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION};
use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(expm1::SparkExpm1, expm1);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((expm1, "Returns exp(expr) - 1 as a Float64.", arg1));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![expm1()]
}

// https://github.com/apache/spark/blob/50a328ba98577ea12bbae50f2cbf406438b01a2f/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L1491-L1508
#[inline]
pub fn round_decimal_base(
    precision: i32,
    scale: i32,
    target_scale: i32,
    decimal_128: bool,
) -> (u8, i8) {
    let integral_least_num_digits = precision - scale + 1;
    if target_scale < 0 {
        let new_precision = max(integral_least_num_digits, -target_scale + 1) as u8;
        if decimal_128 {
            (min(new_precision, DECIMAL128_MAX_PRECISION), 0)
        } else {
            (min(new_precision, DECIMAL256_MAX_PRECISION), 0)
        }
    } else {
        let new_scale = min(scale, target_scale);
        let max_precision = if decimal_128 {
            DECIMAL128_MAX_PRECISION
        } else {
            DECIMAL256_MAX_PRECISION
        } as i32;
        (
            min(integral_least_num_digits + new_scale, max_precision) as u8,
            new_scale as i8,
        )
    }
}
