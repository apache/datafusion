// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::{Array, ArrayRef, AsArray, Decimal128Array};
use arrow::compute::{DatePart, cast, date_part};
use arrow::datatypes::{DataType, Field, FieldRef, Int32Type, TimeUnit};
use datafusion_common::types::logical_date;
use datafusion_common::{
    Result, ScalarValue, internal_err, types::logical_string, utils::take_function_args,
};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Coercion, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use std::sync::{Arc, OnceLock};

/// Precision and scale of the `DECIMAL(8, 6)` Spark returns for the seconds
/// part, which carries the fraction down to microseconds.
const SECONDS_PRECISION: u8 = 8;
const SECONDS_SCALE: i8 = 6;

/// Returns true when `part` is a Spark alias for the seconds field.
///
/// Spark returns `DECIMAL(8, 6)` with the fractional seconds for these instead
/// of an integer.
pub fn is_seconds_part(part: &str) -> bool {
    matches!(
        part.to_lowercase().as_str(),
        "second" | "s" | "sec" | "seconds" | "secs"
    )
}

/// UDF that implements Spark `date_part('SECOND', …)` / `EXTRACT(SECOND FROM …)`.
///
/// Always returns `DECIMAL(8, 6)`. Rewritten onto call sites by
/// [`crate::planner::SparkFunctionPlanner`] so the return type is known before
/// analysis (user-written casts on the part literal hide it from
/// `return_field_from_args`).
pub fn date_part_seconds() -> Arc<ScalarUDF> {
    static INSTANCE: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    INSTANCE
        .get_or_init(|| Arc::new(ScalarUDF::from(SparkDatePartSeconds::new())))
        .clone()
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

    // Nullable, like the integer parts of `date_part`, which this simplifies to,
    // because they can be NULL for a non-null input. Seconds calls are rewritten
    // to [`date_part_seconds`] (DECIMAL(8, 6)) by the Spark expression planner.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let is_seconds = matches!(
            args.scalar_arguments.first(),
            Some(Some(
                ScalarValue::Utf8(Some(part))
                    | ScalarValue::Utf8View(Some(part))
                    | ScalarValue::LargeUtf8(Some(part))
            )) if is_seconds_part(part)
        );
        let data_type = if is_seconds {
            DataType::Decimal128(SECONDS_PRECISION, SECONDS_SCALE)
        } else {
            DataType::Int32
        };

        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("spark date_part should have been simplified to standard date_part")
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

        // Prefer the planner rewrite; keep this path for DataFrame callers that
        // pass a bare string literal (return type already Decimal via
        // `return_field_from_args`).
        if is_seconds_part(&part) {
            let seconds_expr = Expr::ScalarFunction(ScalarFunction::new_udf(
                date_part_seconds(),
                vec![date_expr],
            ));
            return Ok(ExprSimplifyResult::Simplified(seconds_expr));
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

/// `date_part('SECOND', ts)` for Spark: seconds including the microsecond
/// fraction as `DECIMAL(8, 6)`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkDatePartSeconds {
    signature: Signature,
}

impl SparkDatePartSeconds {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Timestamp,
                    )]),
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Native(logical_date()),
                    )]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkDatePartSeconds {
    fn name(&self) -> &str {
        // Keep the display name as date_part so EXPLAIN/DESCRIBE stay familiar.
        "date_part"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Decimal128(SECONDS_PRECISION, SECONDS_SCALE))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Decimal128(SECONDS_PRECISION, SECONDS_SCALE),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_date_part_seconds, vec![])(&args.args)
    }
}

/// Seconds of a timestamp or date including the fraction, as `DECIMAL(8, 6)`.
fn spark_date_part_seconds(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [date_arg] = take_function_args("date_part", args)?;

    // A date has no time of day; Spark casts it to a timestamp at midnight.
    let date_arg = match date_arg.data_type() {
        DataType::Date32 | DataType::Date64 => {
            cast(date_arg, &DataType::Timestamp(TimeUnit::Microsecond, None))?
        }
        _ => Arc::clone(date_arg),
    };

    let secs = date_part(date_arg.as_ref(), DatePart::Second)?;
    let micros = date_part(date_arg.as_ref(), DatePart::Microsecond)?;
    let secs = secs.as_primitive::<Int32Type>();
    let micros = micros.as_primitive::<Int32Type>();

    // `DatePart::Microsecond` is the fraction of the current second, so the
    // unscaled decimal value is the whole seconds times 10^6 plus the fraction.
    let result: Decimal128Array = secs
        .iter()
        .zip(micros.iter())
        .map(|(s, us)| match (s, us) {
            (Some(s), Some(us)) => Some(s as i128 * 1_000_000 + us as i128),
            _ => None,
        })
        .collect();
    let result = result.with_precision_and_scale(SECONDS_PRECISION, SECONDS_SCALE)?;

    Ok(Arc::new(result))
}
