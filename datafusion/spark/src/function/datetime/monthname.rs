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

use arrow::array::{AsArray, StringArray};
use arrow::compute::{DatePart, date_part};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::types::{NativeType, logical_date};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignatureClass, Volatility,
};

const MONTH_NAMES: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

fn month_number_to_name(month: i32) -> Option<&'static str> {
    MONTH_NAMES.get((month - 1) as usize).copied()
}

/// Spark-compatible `monthname` expression.
/// Returns the three-letter abbreviated month name from a date or timestamp.
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#monthname>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMonthName {
    signature: Signature,
}

impl Default for SparkMonthName {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMonthName {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_implicit(
                    TypeSignatureClass::Native(logical_date()),
                    vec![TypeSignatureClass::Timestamp],
                    NativeType::Date,
                )],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkMonthName {
    fn name(&self) -> &str {
        "monthname"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), args.args)?;
        match arg {
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }
                let arr = scalar.to_array_of_size(1)?;
                let month_arr = date_part(&arr, DatePart::Month)?;
                let month_val = month_arr
                    .as_primitive::<arrow::datatypes::Int32Type>()
                    .value(0);
                let name = month_number_to_name(month_val).map(|s| s.to_string());
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(name)))
            }
            ColumnarValue::Array(arr) => {
                let month_arr = date_part(&arr, DatePart::Month)?;
                let int_arr = month_arr.as_primitive::<arrow::datatypes::Int32Type>();

                let result: StringArray = int_arr
                    .iter()
                    .map(|maybe_month| maybe_month.and_then(month_number_to_name))
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }
}
