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

use arrow::array::{Array, ArrayRef, AsArray, StringArray};
use arrow::compute::{CastOptions, DatePart, cast_with_options, date_part};
use arrow::datatypes::{DataType, Field, FieldRef, Int32Type};
use datafusion::logical_expr::{
    Coercion, ColumnarValue, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_common::types::{logical_date, logical_string};
use datafusion_common::{Result, internal_err, plan_err, ScalarValue};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use std::sync::Arc;
use arrow::util::display::FormatOptions;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDayName {
    signature: Signature,
}

impl Default for SparkDayName {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDayName {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Timestamp,
                    )]),
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Native(logical_date()),
                    )]),
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Native(logical_string()),
                    )]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkDayName {
    fn name(&self) -> &str {
        "dayname"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Utf8,
            args.arg_fields[0].is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err!("dayname expects exactly 1 argument");
        }
        let cast_options = CastOptions {
            safe: args.config_options.execution.enable_ansi_mode,
            format_options: FormatOptions::default(),
        };
        match &args.args[0] {
            ColumnarValue::Array(array) => {
                let array = spark_day_name_array(array, cast_options)?;
                Ok(ColumnarValue::Array(array))
            }
            ColumnarValue::Scalar(scalar) => {
                let scalar = spark_day_name_scalar(scalar, cast_options)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
        }
    }
}

fn spark_day_name_scalar(scalar: &ScalarValue, cast_options: &CastOptions) -> Result<ScalarValue> {
    match scalar.data_type() {
        DataType::Date32 | DataType::Timestamp(_, _) => unimplemented!(),
        other => {
            internal_err!("Unsupported arg {other:?} for Spark function `dayname`")
        }
    }
}

fn spark_day_name_array(array: &ArrayRef, cast_options: &CastOptions) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Date32 | DataType::Timestamp(_, _) => spark_day_name_array_inner(array),
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
            let date_array = cast_with_options(array, &DataType::Date32, cast_options)?;
            spark_day_name_array_inner(&date_array)
        }
        other => {
            internal_err!("Unsupported arg {other:?} for Spark function `dayname`")
        }
    }
}

fn spark_day_name_array_inner(array: &ArrayRef) -> Result<ArrayRef> {
    let result: StringArray = date_part(array, DatePart::DayOfWeekMonday0)?
        .as_primitive::<Int32Type>()
        .iter()
        .map(|x| x.and_then(get_display_name))
        .collect();
    Ok(Arc::new(result))
}

/// This function supports only the English locale, matching the behavior of Spark's
/// `dayname` function which return English day names regardless of the system or session locale.
fn get_display_name(day: i32) -> Option<String> {
    match day {
        0 => Some(String::from("Mon")),
        1 => Some(String::from("Tue")),
        2 => Some(String::from("Wed")),
        3 => Some(String::from("Thu")),
        4 => Some(String::from("Fri")),
        5 => Some(String::from("Sat")),
        6 => Some(String::from("Sun")),
        _ => None,
    }
}
