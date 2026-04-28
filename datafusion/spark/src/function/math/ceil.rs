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

use arrow::array::{ArrowNativeTypeOp, AsArray, Decimal128Array};
use arrow::datatypes::{
    DataType, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion_common::types::{NativeType, logical_int32};
use datafusion_common::{Result, ScalarValue, exec_err, not_impl_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};

use super::scale::get_scale;

/// Spark-compatible `ceil` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#ceil>
///
/// Differences with DataFusion ceil:
///  - Spark's 1-arg `ceil` returns Int64 for float inputs; DataFusion preserves
///    the input type (Float32→Float32, Float64→Float64)
///  - Spark's `ceil` on Decimal128(p, s) returns Decimal128(p−s+1, 0), reducing scale
///    to 0; DataFusion preserves the original precision and scale
///  - Spark only supports Decimal128; DataFusion also supports Decimal32/64/256
///  - Spark does not check for decimal overflow; DataFusion errors on overflow
///
/// Two-argument form `ceil(expr, scale)` returns the same type as `expr` for
/// integer and floating-point inputs (regardless of `scale`).  Decimal inputs
/// are not yet supported in the 2-arg form (see TODO in execution path).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCeil {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkCeil {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCeil {
    pub fn new() -> Self {
        let numeric = Coercion::new_exact(TypeSignatureClass::Numeric);
        let decimal_places = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int32()),
            vec![TypeSignatureClass::Integer],
            NativeType::Int32,
        );
        Self {
            signature: Signature::one_of(
                vec![
                    // ceil(numeric)
                    TypeSignature::Numeric(1),
                    // ceil(numeric, scale)
                    TypeSignature::Coercible(vec![numeric, decimal_places]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["ceiling".to_string()],
        }
    }
}

impl ScalarUDFImpl for SparkCeil {
    fn name(&self) -> &str {
        "ceil"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let has_scale = arg_types.len() == 2;

        match &arg_types[0] {
            // 2-arg decimal is not yet supported; report input type so the planner
            // does not reject the call before we surface a proper error at execution.
            DataType::Decimal128(p, s) if has_scale => Ok(DataType::Decimal128(*p, *s)),
            DataType::Decimal128(p, s) => {
                if *s > 0 {
                    Ok(DataType::Decimal128(decimal128_ceil_precision(*p, *s), 0))
                } else {
                    // scale <= 0 means the value is already a whole number
                    // (or represents multiples of 10^(-scale)), so ceil is a no-op
                    Ok(DataType::Decimal128(*p, *s))
                }
            }
            // 2-arg form preserves the input float type for any scale (including 0),
            // matching Spark's `ceil(double, scale)` semantics.
            DataType::Float32 if has_scale => Ok(DataType::Float32),
            DataType::Float64 if has_scale => Ok(DataType::Float64),
            dt if matches!(dt, DataType::Float32 | DataType::Float64)
                || dt.is_integer() =>
            {
                Ok(DataType::Int64)
            }
            other => exec_err!("Unsupported data type {other:?} for function ceil"),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_ceil(&args.args)
    }
}

fn spark_ceil(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.is_empty() || args.len() > 2 {
        return exec_err!(
            "ceil function requires 1 or 2 arguments, got {}",
            args.len()
        );
    }

    let has_scale = args.len() == 2;
    let scale = match get_scale("ceil", args)? {
        Some(scale) => scale,
        None => {
            // NULL scale → return NULL with the same type the planner advertised
            return Ok(ColumnarValue::Scalar(ScalarValue::try_from(
                args[0].data_type(),
            )?));
        }
    };
    let input = &args[0];

    match input {
        ColumnarValue::Scalar(value) => spark_ceil_scalar(value, scale, has_scale),
        ColumnarValue::Array(input) => spark_ceil_array(input, scale, has_scale),
    }
}

/// Compute ceil for a single decimal128 value with the given scale.
#[inline]
fn decimal128_ceil(value: i128, scale: u32) -> i128 {
    let div = 10_i128.pow_wrapping(scale);
    let d = value / div;
    let r = value % div;
    if r > 0 { d + 1 } else { d }
}

/// Compute the return precision for a decimal128 ceil result.
#[inline]
fn decimal128_ceil_precision(precision: u8, scale: i8) -> u8 {
    ((precision as i64) - (scale as i64) + 1).clamp(1, 38) as u8
}

/// Compute the ceiling of an integer at the given decimal `scale`.
///
/// - `scale >= 0`: integers have no fractional part; returns `value` unchanged.
/// - `scale  < 0`: rounds *up* to the nearest multiple of `10^(-scale)`.
///   For positive values this rounds away from zero; for negative values it
///   rounds toward zero (ceiling = toward +∞).
///
/// If `10^(-scale)` overflows `i64`, returns `0` (matching Spark / `round`).
fn ceil_integer(value: i64, scale: i32) -> i64 {
    if scale >= 0 {
        return value;
    }
    let abs_scale = (-scale) as u32;
    let Some(factor) = 10_i64.checked_pow(abs_scale) else {
        return 0;
    };
    let remainder = value % factor;
    if remainder > 0 {
        // Positive remainder: bump up to the next multiple.
        value.wrapping_sub(remainder).wrapping_add(factor)
    } else if remainder < 0 {
        // Negative remainder (negative value not on boundary): truncating toward
        // zero already gives the ceiling.
        value.wrapping_sub(remainder)
    } else {
        value
    }
}

fn spark_ceil_scalar(
    value: &ScalarValue,
    scale: i32,
    has_scale: bool,
) -> Result<ColumnarValue> {
    let result = match value {
        // Floats: 2-arg form preserves the input float type for *every* scale,
        // so the runtime type matches what `return_type` advertised.
        ScalarValue::Float32(v) if has_scale => {
            ScalarValue::Float32(v.map(|x| ceil_float(x, scale)))
        }
        ScalarValue::Float64(v) if has_scale => {
            ScalarValue::Float64(v.map(|x| ceil_float(x, scale)))
        }
        // 1-arg float: Spark returns Int64.
        ScalarValue::Float32(v) => ScalarValue::Int64(v.map(|x| x.ceil() as i64)),
        ScalarValue::Float64(v) => ScalarValue::Int64(v.map(|x| x.ceil() as i64)),
        // Integers: the 1-arg path was a plain `cast_to(Int64)`. The 2-arg path
        // additionally applies a (possibly negative) scale before casting.
        v if v.data_type().is_integer() && has_scale => {
            match v.cast_to(&DataType::Int64)? {
                ScalarValue::Int64(opt) => {
                    ScalarValue::Int64(opt.map(|x| ceil_integer(x, scale)))
                }
                other => {
                    return exec_err!(
                        "Internal error: integer cast_to(Int64) yielded {:?}",
                        other.data_type()
                    );
                }
            }
        }
        v if v.data_type().is_integer() => v.cast_to(&DataType::Int64)?,
        // Decimal128 with positive scale (1-arg only).
        ScalarValue::Decimal128(_, _, _) if has_scale => {
            return not_impl_err!(
                "2-argument ceil is not yet supported for decimal inputs"
            );
        }
        ScalarValue::Decimal128(v, p, s) if *s > 0 => {
            let new_p = decimal128_ceil_precision(*p, *s);
            ScalarValue::Decimal128(v.map(|x| decimal128_ceil(x, *s as u32)), new_p, 0)
        }
        ScalarValue::Decimal128(_, _, _) => value.clone(),
        other => {
            return exec_err!(
                "Unsupported data type {:?} for function ceil",
                other.data_type()
            );
        }
    };
    Ok(ColumnarValue::Scalar(result))
}

macro_rules! impl_integer_array_ceil {
    ($array:expr, $arrow_type:ty, $scale:expr) => {{
        let array = $array.as_primitive::<$arrow_type>();
        let result = array.unary::<_, Int64Type>(|x| ceil_integer(x as i64, $scale));
        Arc::new(result) as Arc<dyn arrow::array::Array>
    }};
}

fn spark_ceil_array(
    input: &Arc<dyn arrow::array::Array>,
    scale: i32,
    has_scale: bool,
) -> Result<ColumnarValue> {
    let result = match input.data_type() {
        // 2-arg float: preserve input float type for any scale.
        DataType::Float32 if has_scale => Arc::new(
            input
                .as_primitive::<Float32Type>()
                .unary::<_, Float32Type>(|x| ceil_float(x, scale)),
        ) as _,
        DataType::Float64 if has_scale => Arc::new(
            input
                .as_primitive::<Float64Type>()
                .unary::<_, Float64Type>(|x| ceil_float(x, scale)),
        ) as _,
        // 1-arg float: Spark returns Int64.
        DataType::Float32 => Arc::new(
            input
                .as_primitive::<Float32Type>()
                .unary::<_, Int64Type>(|x| x.ceil() as i64),
        ) as _,
        DataType::Float64 => Arc::new(
            input
                .as_primitive::<Float64Type>()
                .unary::<_, Int64Type>(|x| x.ceil() as i64),
        ) as _,
        // 2-arg integer: widen to Int64 and apply scale-aware ceiling.
        DataType::Int8 if has_scale => impl_integer_array_ceil!(input, Int8Type, scale),
        DataType::Int16 if has_scale => impl_integer_array_ceil!(input, Int16Type, scale),
        DataType::Int32 if has_scale => impl_integer_array_ceil!(input, Int32Type, scale),
        DataType::Int64 if has_scale => impl_integer_array_ceil!(input, Int64Type, scale),
        DataType::UInt8 if has_scale => impl_integer_array_ceil!(input, UInt8Type, scale),
        DataType::UInt16 if has_scale => {
            impl_integer_array_ceil!(input, UInt16Type, scale)
        }
        DataType::UInt32 if has_scale => {
            impl_integer_array_ceil!(input, UInt32Type, scale)
        }
        DataType::UInt64 if has_scale => {
            let array = input.as_primitive::<UInt64Type>();
            let result = array
                .try_unary::<_, Int64Type, datafusion_common::DataFusionError>(|x| {
                    let v = i64::try_from(x).map_err(|_| {
                        (exec_err!(
                        "ceil: UInt64 value {x} exceeds i64::MAX and cannot be processed"
                    )
                        as Result<(), _>)
                        .unwrap_err()
                    })?;
                    Ok(ceil_integer(v, scale))
                })?;
            Arc::new(result) as _
        }
        // 1-arg integer: cast to Int64.
        dt if dt.is_integer() => arrow::compute::cast(input, &DataType::Int64)?,
        // Decimal128 with the new 2-arg form is deferred to a follow-up.
        DataType::Decimal128(_, _) if has_scale => {
            return not_impl_err!(
                "2-argument ceil is not yet supported for decimal inputs"
            );
        }
        DataType::Decimal128(p, s) if *s > 0 => {
            let new_p = decimal128_ceil_precision(*p, *s);
            let result: Decimal128Array = input
                .as_primitive::<Decimal128Type>()
                .unary(|x| decimal128_ceil(x, *s as u32));
            Arc::new(result.with_data_type(DataType::Decimal128(new_p, 0)))
        }
        DataType::Decimal128(_, _) => Arc::clone(input),
        other => return exec_err!("Unsupported data type {other:?} for function ceil"),
    };

    Ok(ColumnarValue::Array(result))
}

fn ceil_float<T: num_traits::Float>(value: T, scale: i32) -> T {
    if scale >= 0 {
        let factor = T::from(10.0f64.powi(scale)).unwrap_or_else(T::infinity);
        if factor.is_infinite() {
            return value;
        }
        (value * factor).ceil() / factor
    } else {
        let factor = T::from(10.0f64.powi(-scale)).unwrap_or_else(T::infinity);
        if factor.is_infinite() {
            return T::zero();
        }
        (value / factor).ceil() * factor
    }
}
