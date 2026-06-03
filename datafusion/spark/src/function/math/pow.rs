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
//! `Infinity` for `pow(0, <negative>)` rather than raising an error.

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
/// Behavioural difference from the DataFusion default:
/// - `pow(0, <negative>)` → `Infinity`  (IEEE 754 / Spark semantics)
///   The default raises `"zero raised to a negative power is undefined"` to
///   match PostgreSQL.
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
            let result = (*base).zip(*exp).map(|(base, exp)| {
                if base == 0.0 && exp < 0.0 {
                    f64::INFINITY
                } else {
                    base.powf(exp)
                }
            });
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

        // Spark: 0^negative = +Infinity (covers both 0.0 and -0.0)
        // IEEE 754: 0.0^-1.0 = +Infinity, -0.0^-1.0 = -Infinity
        // Thus we need an explicit guard for base == 0.0 to ensure +Infinity.
        let result: Float64Array = base_f64
            .iter()
            .zip(exp_f64.iter())
            .map(|(base, exp)| match (base, exp) {
                (Some(base), Some(exp)) => {
                    if base == 0.0 && exp < 0.0 {
                        Some(f64::INFINITY)
                    } else {
                        Some(base.powf(exp))
                    }
                }
                _ => None,
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}
