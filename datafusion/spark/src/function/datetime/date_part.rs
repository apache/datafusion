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

use arrow::array::{Array, ArrayRef, Decimal128Array};
use arrow::compute::{DatePart, date_part};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::as_int32_array;
use datafusion_common::types::logical_date;
use datafusion_common::{
    Result, ScalarValue, exec_err, internal_err, types::logical_string,
    utils::take_function_args,
};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Coercion, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use std::sync::Arc;

fn is_second(part: &str) -> bool {
    matches!(part, "second" | "s" | "sec" | "seconds" | "secs")
}

fn spark_date_part_second(array: &dyn Array) -> Result<ArrayRef> {
    if matches!(array.data_type(), DataType::Date32 | DataType::Date64) {
        let values = std::iter::repeat_n(0_i128, array.len());
        let dec_array =
            Decimal128Array::from_iter_values_with_nulls(values, array.nulls().cloned())
                .with_precision_and_scale(8, 6)?;
        return Ok(Arc::new(dec_array));
    }

    let secs = date_part(array, DatePart::Second)?;
    let secs = as_int32_array(secs.as_ref())?;
    let subsecs = date_part(array, DatePart::Nanosecond)?;
    let subsecs = as_int32_array(subsecs.as_ref())?;

    let values = (0..array.len()).map(|i| {
        if array.is_null(i) {
            0_i128
        } else {
            let s = secs.value(i) as i128;
            let ns = (subsecs.value(i) as i128) % 1_000_000_000;
            let micros = ns / 1_000;
            s * 1_000_000 + micros
        }
    });

    let dec_array =
        Decimal128Array::from_iter_values_with_nulls(values, array.nulls().cloned())
            .with_precision_and_scale(8, 6)?;

    Ok(Arc::new(dec_array))
}

/// Wrapper around datafusion date_part function to handle
/// Spark behavior returning day of the week 1-indexed instead of 0-indexed and different part aliases.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#date_part>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDatePart {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkDatePart {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDatePart {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Timestamp),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_date())),
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("datepart")],
        }
    }
}

impl ScalarUDFImpl for SparkDatePart {
    fn name(&self) -> &str {
        "date_part"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("Use return_field_from_args in this case instead.")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field, _] = take_function_args(self.name(), args.scalar_arguments)?;
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());

        let data_type = match field.and_then(|sv| sv.try_as_str().flatten()) {
            Some(part) if is_second(&part.to_lowercase()) => DataType::Decimal128(8, 6),
            _ => DataType::Int32,
        };

        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [part_arg, date_arg] = take_function_args(self.name(), args.args)?;

        let part = match &part_arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(v)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(v)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(v))) => v.to_lowercase(),
            _ => {
                return exec_err!(
                    "First argument of `{}` must be non-null scalar Utf8",
                    self.name()
                );
            }
        };

        if is_second(&part) {
            let is_scalar = matches!(date_arg, ColumnarValue::Scalar(_));
            let array = match date_arg {
                ColumnarValue::Array(array) => Arc::clone(&array),
                ColumnarValue::Scalar(scalar) => scalar.to_array()?,
            };

            let result_array = spark_date_part_second(array.as_ref())?;

            if is_scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    result_array.as_ref(),
                    0,
                )?))
            } else {
                Ok(ColumnarValue::Array(result_array))
            }
        } else {
            // Map Spark-specific date part aliases to DataFusion ones.
            let mapped_part = match part.as_str() {
                "yearofweek" | "year_iso" => "isoyear",
                "dayofweek" | "dow" => "dow1",
                "dayofweek_iso" | "dow_iso" => "isodow",
                other => other,
            };

            let mapped_part_arg =
                ColumnarValue::Scalar(ScalarValue::new_utf8(mapped_part));
            let new_args = ScalarFunctionArgs {
                args: vec![mapped_part_arg, date_arg],
                ..args
            };
            datafusion_functions::datetime::date_part().invoke_with_args(new_args)
        }
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [part_expr, date_expr] = take_function_args(self.name(), args)?;

        let part = match part_expr.as_literal() {
            Some(ScalarValue::Utf8(Some(v)))
            | Some(ScalarValue::Utf8View(Some(v)))
            | Some(ScalarValue::LargeUtf8(Some(v))) => v.to_lowercase(),
            _ => {
                return internal_err!(
                    "First argument of `DATE_PART` must be non-null scalar Utf8"
                );
            }
        };

        if is_second(&part) {
            return Ok(ExprSimplifyResult::Original(vec![part_expr, date_expr]));
        }

        // Map Spark-specific date part aliases to DataFusion ones.
        let part = match part.as_str() {
            "yearofweek" | "year_iso" => "isoyear",
            "dayofweek" | "dow" => "dow1",
            "dayofweek_iso" | "dow_iso" => "isodow",
            other => other,
        };

        let part_expr = Expr::Literal(ScalarValue::new_utf8(part), None);

        let date_part_expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            datafusion_functions::datetime::date_part(),
            vec![part_expr, date_expr],
        ));

        Ok(ExprSimplifyResult::Simplified(date_part_expr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Date32Array, TimestampMicrosecondArray};
    use arrow::datatypes::Field;

    #[test]
    fn test_spark_date_part_second() {
        let func = SparkDatePart::new();

        // Timestamp with fractional seconds: 12:30:45.123456
        let ts_micros = 1_705_329_045_123_456_i64;
        let ts_array = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(ts_micros),
            Some(1_705_329_045_000_000_i64),
            None,
        ])) as ArrayRef;

        let field_ref = Arc::new(Field::new("ts", ts_array.data_type().clone(), true));
        let part_arg = ColumnarValue::Scalar(ScalarValue::from("second"));
        let date_arg = ColumnarValue::Array(Arc::clone(&ts_array));

        let res = func
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![part_arg, date_arg],
                arg_fields: vec![
                    Arc::new(Field::new("part", DataType::Utf8, false)),
                    Arc::clone(&field_ref),
                ],
                number_rows: 3,
                return_field: Arc::new(Field::new(
                    "date_part",
                    DataType::Decimal128(8, 6),
                    true,
                )),
                config_options: Arc::new(
                    datafusion_common::config::ConfigOptions::default(),
                ),
            })
            .unwrap();

        let ColumnarValue::Array(res_arr) = res else {
            panic!("Expected array");
        };
        let dec_arr = res_arr.as_any().downcast_ref::<Decimal128Array>().unwrap();

        assert_eq!(dec_arr.precision(), 8);
        assert_eq!(dec_arr.scale(), 6);
        assert_eq!(dec_arr.value(0), 45_123_456);
        assert_eq!(dec_arr.value(1), 45_000_000);
        assert!(dec_arr.is_null(2));
    }

    #[test]
    fn test_spark_date_part_second_date32() {
        let func = SparkDatePart::new();
        let date_array = Arc::new(Date32Array::from(vec![Some(100), None])) as ArrayRef;
        let field_ref = Arc::new(Field::new("d", date_array.data_type().clone(), true));

        let part_arg = ColumnarValue::Scalar(ScalarValue::from("s"));
        let date_arg = ColumnarValue::Array(Arc::clone(&date_array));

        let res = func
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![part_arg, date_arg],
                arg_fields: vec![
                    Arc::new(Field::new("part", DataType::Utf8, false)),
                    Arc::clone(&field_ref),
                ],
                number_rows: 2,
                return_field: Arc::new(Field::new(
                    "date_part",
                    DataType::Decimal128(8, 6),
                    true,
                )),
                config_options: Arc::new(
                    datafusion_common::config::ConfigOptions::default(),
                ),
            })
            .unwrap();

        let ColumnarValue::Array(res_arr) = res else {
            panic!("Expected array");
        };
        let dec_arr = res_arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(dec_arr.precision(), 8);
        assert_eq!(dec_arr.scale(), 6);
        assert_eq!(dec_arr.value(0), 0);
        assert!(dec_arr.is_null(1));
    }

    #[test]
    fn test_spark_date_part_second_return_type() {
        let func = SparkDatePart::new();

        for part in ["second", "s", "sec", "seconds", "secs", "SECOND", "SECS"] {
            let part_sv = ScalarValue::from(part);
            let field = func
                .return_field_from_args(ReturnFieldArgs {
                    arg_fields: &[
                        Arc::new(Field::new("part", DataType::Utf8, false)),
                        Arc::new(Field::new(
                            "ts",
                            DataType::Timestamp(
                                arrow::datatypes::TimeUnit::Microsecond,
                                None,
                            ),
                            true,
                        )),
                    ],
                    scalar_arguments: &[Some(&part_sv), None],
                })
                .unwrap();
            assert_eq!(field.data_type(), &DataType::Decimal128(8, 6));
        }

        let non_second_sv = ScalarValue::from("year");
        let field = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[
                    Arc::new(Field::new("part", DataType::Utf8, false)),
                    Arc::new(Field::new(
                        "ts",
                        DataType::Timestamp(
                            arrow::datatypes::TimeUnit::Microsecond,
                            None,
                        ),
                        true,
                    )),
                ],
                scalar_arguments: &[Some(&non_second_sv), None],
            })
            .unwrap();
        assert_eq!(field.data_type(), &DataType::Int32);
    }
}
