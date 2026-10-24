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

//! Spark-compatible `pow` / `power` function.
//!
//! Unlike the default DataFusion (PostgreSQL) implementation, Spark returns
//! `Infinity` for `pow(0, <negative>)` rather than raising an error, and
//! follows `java.lang.StrictMath.pow` for the special cases.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::datatypes::DataType;

use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions::math::power::PowerFunc;

/// Spark-compatible implementation of `pow` / `power`.
///
/// Behavioural differences from the DataFusion default:
/// - `pow(0, <negative>)` → `Infinity`  (IEEE 754 / Spark semantics)
///   The default raises `"zero raised to a negative power is undefined"` to
///   match PostgreSQL.
/// - A `NaN` exponent, or a base of `1` or `-1` with an infinite exponent,
///   gives `NaN` (`StrictMath.pow` semantics) instead of `1`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkPow {
    inner: PowerFunc,
    aliases: Vec<String>,
}

impl Default for SparkPow {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkPow {
    pub fn new() -> Self {
        Self {
            inner: PowerFunc::new(),
            // SparkPow is named "pow"; expose "power" as an alias so that
            // both names resolve to Spark semantics when this crate is active.
            aliases: vec!["power".to_string()],
        }
    }
}

impl ScalarUDFImpl for SparkPow {
    fn name(&self) -> &str {
        "pow"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Only Float64 × Float64 needs the Spark override.
        // Decimal / integer / mixed-type paths are delegated to the standard
        // PowerFunc which already handles them correctly (decimal can't
        // represent Infinity anyway).
        match args.args.as_slice() {
            [base, exponent]
                if matches!(base.data_type(), DataType::Float64)
                    && matches!(exponent.data_type(), DataType::Float64) => {}
            _ => return self.inner.invoke_with_args(args),
        }

        let num_rows = args.number_rows;

        // ── Scalar × Scalar fast path ────────────────────────────────────────
        // Pattern-match on the slice to avoid any ownership issues.
        if let [
            ColumnarValue::Scalar(ScalarValue::Float64(base)),
            ColumnarValue::Scalar(ScalarValue::Float64(exp)),
        ] = args.args.as_slice()
        {
            // base and exp are &Option<f64>; Option<f64> is Copy.
            let result = (*base).zip(*exp).map(|(base, exp)| spark_pow(base, exp));
            return Ok(ColumnarValue::Scalar(ScalarValue::Float64(result)));
        }

        // ── Array path ───────────────────────────────────────────────────────
        let [base, exponent] = take_function_args(self.name(), &args.args)?;

        let base_arr: ArrayRef = base.to_array(num_rows)?;
        let exp_arr: ArrayRef = exponent.to_array(num_rows)?;

        let base_f64 = base_arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("base must be Float64Array");
        let exp_f64 = exp_arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("exponent must be Float64Array");

        let result: Float64Array = base_f64
            .iter()
            .zip(exp_f64.iter())
            .map(|(base, exp)| match (base, exp) {
                (Some(base), Some(exp)) => Some(spark_pow(base, exp)),
                _ => None,
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

/// `pow` with the special cases of Java's `StrictMath.pow`, which Spark uses.
///
/// `f64::powf` follows C99, which agrees with Java except that C99 returns `1`
/// for `pow(1, NaN)` and `pow(±1, ±Infinity)`, where Java returns `NaN`.
/// Both give `+Infinity` for `pow(0.0, <negative>)` and `-Infinity` for
/// `pow(-0.0, <negative odd integer>)`.
fn spark_pow(base: f64, exp: f64) -> f64 {
    if exp.is_nan() || (base.abs() == 1.0 && exp.is_infinite()) {
        f64::NAN
    } else {
        base.powf(exp)
    }
}
