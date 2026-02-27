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

use crate::function::conversion::cast_boolean::{
    cast_boolean_to_decimal, is_df_cast_from_bool_spark_compatible,
};
use crate::function::conversion::cast_complex::{
    cast_array_to_string, cast_binary_to_string, cast_int_to_binary, cast_struct_to_struct,
    casts_struct_to_string,
};
use crate::function::conversion::cast_datetime::{cast_date_to_timestamp, cast_int_to_timestamp};
use crate::function::conversion::cast_numeric::{
    cast_float32_to_decimal128, cast_float64_to_decimal128, cast_int_to_decimal128,
    spark_cast_decimal_to_boolean, spark_cast_float32_to_utf8, spark_cast_float64_to_utf8,
    spark_cast_int_to_int, spark_cast_nonintegral_numeric_to_integral,
};
use crate::function::conversion::cast_string::{
    cast_string_to_date, cast_string_to_decimal, cast_string_to_float, cast_string_to_int,
    cast_string_to_timestamp, is_df_cast_from_string_spark_compatible,
    spark_cast_utf8_to_boolean,
};
use crate::function::conversion::cast_utils::{
    array_with_timezone, parse_spark_datatype, spark_cast_postprocess, EvalMode,
    SparkCastOptions, CAST_OPTIONS, TIMESTAMP_FORMAT,
};
use arrow::array::{
    Array, ArrayRef, DictionaryArray, PrimitiveArray,
};
use arrow::compute::{can_cast_types, cast_with_options, take, CastOptions};
use arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNativeType, DataType, Int32Type,
};
use arrow::util::display::FormatOptions;
use datafusion_common::{internal_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

// --- SparkCast UDF ---

/// Spark-compatible CAST function.
/// Usage: `spark_cast(expr, 'TYPE_NAME')`
///
/// Behavior depends on the `enable_ansi_mode` config option:
/// - `enable_ansi_mode = false` (default): Legacy mode, returns NULL on errors
/// - `enable_ansi_mode = true`: ANSI mode, raises errors on invalid input
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCast {
    signature: Signature,
}

impl Default for SparkCast {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCast {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkCast {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_type_from_args should be called instead")
    }

    fn return_type_from_args(
        &self,
        args: datafusion_expr::ReturnTypeArgs,
    ) -> Result<DataType> {
        // The second argument should be a string literal specifying the target type
        if args.scalar_arguments.len() != 2 {
            return internal_err!("spark_cast requires exactly 2 arguments");
        }
        match &args.scalar_arguments[1] {
            Some(ScalarValue::Utf8(Some(type_str)))
            | Some(ScalarValue::LargeUtf8(Some(type_str))) => {
                parse_spark_datatype(type_str)
            }
            _ => internal_err!(
                "spark_cast second argument must be a string literal type name"
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let target_type = self.resolve_target_type(&args)?;
        let timezone = args
            .config_options
            .execution
            .time_zone
            .clone();

        let eval_mode = if args.config_options.execution.enable_ansi_mode {
            EvalMode::Ansi
        } else {
            EvalMode::Legacy
        };

        let cast_options = SparkCastOptions::new(eval_mode, &timezone);
        spark_cast_inner(args.args.into_iter().next().unwrap(), &target_type, &cast_options)
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        Ok(input[0].sort_properties)
    }
}

impl SparkCast {
    fn resolve_target_type(&self, args: &ScalarFunctionArgs) -> Result<DataType> {
        // The second argument must be a string literal
        match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(type_str)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(type_str))) => {
                parse_spark_datatype(type_str)
            }
            _ => internal_err!(
                "spark_cast second argument must be a string literal type name"
            ),
        }
    }
}

// --- SparkTryCast UDF ---

/// Spark-compatible TRY_CAST function.
/// Usage: `spark_try_cast(expr, 'TYPE_NAME')`
///
/// Always uses Try mode: returns NULL instead of raising errors on invalid input.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryCast {
    signature: Signature,
}

impl Default for SparkTryCast {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryCast {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkTryCast {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_try_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_type_from_args should be called instead")
    }

    fn return_type_from_args(
        &self,
        args: datafusion_expr::ReturnTypeArgs,
    ) -> Result<DataType> {
        if args.scalar_arguments.len() != 2 {
            return internal_err!("spark_try_cast requires exactly 2 arguments");
        }
        match &args.scalar_arguments[1] {
            Some(ScalarValue::Utf8(Some(type_str)))
            | Some(ScalarValue::LargeUtf8(Some(type_str))) => {
                parse_spark_datatype(type_str)
            }
            _ => internal_err!(
                "spark_try_cast second argument must be a string literal type name"
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let target_type = self.resolve_target_type(&args)?;
        let timezone = args
            .config_options
            .execution
            .time_zone
            .clone();

        let cast_options = SparkCastOptions::new(EvalMode::Try, &timezone);
        spark_cast_inner(args.args.into_iter().next().unwrap(), &target_type, &cast_options)
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        Ok(input[0].sort_properties)
    }
}

impl SparkTryCast {
    fn resolve_target_type(&self, args: &ScalarFunctionArgs) -> Result<DataType> {
        match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(type_str)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(type_str))) => {
                parse_spark_datatype(type_str)
            }
            _ => internal_err!(
                "spark_try_cast second argument must be a string literal type name"
            ),
        }
    }
}

// --- Core cast logic ---

/// Entry point: cast a ColumnarValue using Spark semantics.
pub fn spark_cast_inner(
    arg: ColumnarValue,
    data_type: &DataType,
    cast_options: &SparkCastOptions,
) -> Result<ColumnarValue> {
    match arg {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(cast_array(
            array, data_type, cast_options,
        )?)),
        ColumnarValue::Scalar(scalar) => {
            let array = scalar.to_array()?;
            let scalar = ScalarValue::try_from_array(
                &cast_array(array, data_type, cast_options)?,
                0,
            )?;
            Ok(ColumnarValue::Scalar(scalar))
        }
    }
}

/// Helper to create DictionaryArray from values
fn dict_from_values<K: ArrowDictionaryKeyType>(
    values_array: ArrayRef,
) -> Result<ArrayRef> {
    let key_array: PrimitiveArray<K> = (0..values_array.len())
        .map(|index| {
            if values_array.is_valid(index) {
                let native_index = K::Native::from_usize(index).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Can not create index of type {} from value {}",
                        K::DATA_TYPE,
                        index
                    ))
                })?;
                Ok(Some(native_index))
            } else {
                Ok(None)
            }
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .collect();

    let dict_array = DictionaryArray::<K>::try_new(key_array, values_array)?;
    Ok(Arc::new(dict_array))
}

/// Main router: cast an Arrow array using Spark semantics.
pub(crate) fn cast_array(
    array: ArrayRef,
    to_type: &DataType,
    cast_options: &SparkCastOptions,
) -> Result<ArrayRef> {
    use DataType::*;
    let array =
        array_with_timezone(array, cast_options.timezone.clone(), Some(to_type))?;
    let from_type = array.data_type().clone();

    let native_cast_options: CastOptions = CastOptions {
        safe: !matches!(cast_options.eval_mode, EvalMode::Ansi),
        format_options: FormatOptions::new()
            .with_timestamp_tz_format(TIMESTAMP_FORMAT)
            .with_timestamp_format(TIMESTAMP_FORMAT),
    };

    // Handle dictionary input
    let array = match &from_type {
        Dictionary(key_type, value_type)
            if key_type.as_ref() == &Int32
                && (value_type.as_ref() == &Utf8
                    || value_type.as_ref() == &LargeUtf8
                    || value_type.as_ref() == &Binary
                    || value_type.as_ref() == &LargeBinary) =>
        {
            let dict_array = array
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected a dictionary array");

            let casted_result = match to_type {
                Dictionary(_, to_value_type) => {
                    let casted_dictionary = DictionaryArray::<Int32Type>::new(
                        dict_array.keys().clone(),
                        cast_array(
                            Arc::clone(dict_array.values()),
                            to_value_type,
                            cast_options,
                        )?,
                    );
                    Arc::new(casted_dictionary.clone())
                }
                _ => {
                    let casted_dictionary = DictionaryArray::<Int32Type>::new(
                        dict_array.keys().clone(),
                        cast_array(
                            Arc::clone(dict_array.values()),
                            to_type,
                            cast_options,
                        )?,
                    );
                    take(
                        casted_dictionary.values().as_ref(),
                        dict_array.keys(),
                        None,
                    )?
                }
            };
            return Ok(spark_cast_postprocess(
                casted_result,
                &from_type,
                to_type,
            ));
        }
        _ => {
            if let Dictionary(_, _) = to_type {
                let dict_array = dict_from_values::<Int32Type>(array)?;
                let casted_result = cast_array(dict_array, to_type, cast_options)?;
                return Ok(spark_cast_postprocess(
                    casted_result,
                    &from_type,
                    to_type,
                ));
            } else {
                array
            }
        }
    };
    let from_type = array.data_type();
    let eval_mode = cast_options.eval_mode;

    let cast_result = match (from_type, to_type) {
        // String → Boolean
        (Utf8, Boolean) => spark_cast_utf8_to_boolean::<i32>(&array, eval_mode),
        (LargeUtf8, Boolean) => spark_cast_utf8_to_boolean::<i64>(&array, eval_mode),

        // String → Timestamp
        (Utf8, Timestamp(_, _)) => {
            let tz_str = if cast_options.timezone.is_empty() {
                "UTC"
            } else {
                cast_options.timezone.as_str()
            };
            let tz: arrow::array::timezone::Tz = tz_str.parse().map_err(|e| {
                DataFusionError::Internal(format!("Failed to parse timezone: {e}"))
            })?;
            cast_string_to_timestamp(&array, to_type, eval_mode, &tz)
        }

        // String → Date
        (Utf8, Date32) => cast_string_to_date(&array, to_type, eval_mode),

        // Date → Int (reinterpret days as i32)
        (Date32, Int32) => Ok(cast_with_options(&array, to_type, &CAST_OPTIONS)?),

        // String → Float
        (Utf8, Float32 | Float64) => cast_string_to_float(&array, to_type, eval_mode),

        // String → Decimal
        (Utf8 | LargeUtf8, Decimal128(precision, scale)) => {
            cast_string_to_decimal(&array, to_type, precision, scale, eval_mode)
        }
        (Utf8 | LargeUtf8, Decimal256(precision, scale)) => {
            cast_string_to_decimal(&array, to_type, precision, scale, eval_mode)
        }

        // Int → Int narrowing (not Try mode)
        (Int64, Int32)
        | (Int64, Int16)
        | (Int64, Int8)
        | (Int32, Int16)
        | (Int32, Int8)
        | (Int16, Int8)
            if eval_mode != EvalMode::Try =>
        {
            spark_cast_int_to_int(&array, eval_mode, from_type, to_type)
        }

        // Int → Decimal
        (Int8 | Int16 | Int32 | Int64, Decimal128(precision, scale)) => {
            cast_int_to_decimal128(&array, eval_mode, from_type, to_type, *precision, *scale)
        }

        // String → Int
        (Utf8, Int8 | Int16 | Int32 | Int64) => {
            cast_string_to_int::<i32>(to_type, &array, eval_mode)
        }
        (LargeUtf8, Int8 | Int16 | Int32 | Int64) => {
            cast_string_to_int::<i64>(to_type, &array, eval_mode)
        }

        // Float → String
        (Float64, Utf8) => spark_cast_float64_to_utf8::<i32>(&array, eval_mode),
        (Float64, LargeUtf8) => spark_cast_float64_to_utf8::<i64>(&array, eval_mode),
        (Float32, Utf8) => spark_cast_float32_to_utf8::<i32>(&array, eval_mode),
        (Float32, LargeUtf8) => spark_cast_float32_to_utf8::<i64>(&array, eval_mode),

        // Float → Decimal
        (Float32, Decimal128(precision, scale)) => {
            cast_float32_to_decimal128(&array, *precision, *scale, eval_mode)
        }
        (Float64, Decimal128(precision, scale)) => {
            cast_float64_to_decimal128(&array, *precision, *scale, eval_mode)
        }

        // Float/Decimal → Int (not Try mode)
        (Float32, Int8)
        | (Float32, Int16)
        | (Float32, Int32)
        | (Float32, Int64)
        | (Float64, Int8)
        | (Float64, Int16)
        | (Float64, Int32)
        | (Float64, Int64)
        | (Decimal128(_, _), Int8)
        | (Decimal128(_, _), Int16)
        | (Decimal128(_, _), Int32)
        | (Decimal128(_, _), Int64)
            if eval_mode != EvalMode::Try =>
        {
            spark_cast_nonintegral_numeric_to_integral(&array, eval_mode, from_type, to_type)
        }

        // Decimal → Boolean
        (Decimal128(_p, _s), Boolean) => spark_cast_decimal_to_boolean(&array),

        // Utf8View → Utf8
        (Utf8View, Utf8) => Ok(cast_with_options(&array, to_type, &CAST_OPTIONS)?),

        // Struct → String
        (Struct(_), Utf8) => {
            Ok(casts_struct_to_string(array.as_struct(), cast_options)?)
        }

        // Struct → Struct
        (Struct(_), Struct(_)) => Ok(cast_struct_to_struct(
            array.as_struct(),
            from_type,
            to_type,
            cast_options,
        )?),

        // List → String
        (List(_), Utf8) => {
            Ok(cast_array_to_string(array.as_list(), cast_options)?)
        }

        // List → List (delegate to Arrow if supported)
        (List(_), List(_)) if can_cast_types(from_type, to_type) => {
            Ok(cast_with_options(&array, to_type, &CAST_OPTIONS)?)
        }

        // Binary → String
        (Binary, Utf8) => Ok(cast_binary_to_string::<i32>(&array, cast_options)?),

        // Date → Timestamp
        (Date32, Timestamp(_, tz)) => {
            Ok(cast_date_to_timestamp(&array, cast_options, tz)?)
        }

        // Int → Binary (Legacy mode only)
        (Int8 | Int16 | Int32 | Int64, Binary) if eval_mode == EvalMode::Legacy => {
            cast_int_to_binary(&array, from_type)
        }

        // Boolean → Decimal
        (Boolean, Decimal128(precision, scale)) => {
            cast_boolean_to_decimal(&array, *precision, *scale)
        }

        // Int → Timestamp
        (Int8 | Int16 | Int32 | Int64, Timestamp(_, tz)) => {
            cast_int_to_timestamp(&array, tz)
        }

        // Fallback: use DataFusion cast when known to be compatible
        _ if is_datafusion_spark_compatible(from_type, to_type) => {
            Ok(cast_with_options(&array, to_type, &native_cast_options)?)
        }

        // Unsupported cast
        _ => {
            internal_err!(
                "Spark cast invoked for unsupported cast from {from_type:?} to {to_type:?}"
            )
        }
    };
    Ok(spark_cast_postprocess(cast_result?, from_type, to_type))
}

/// Determines if DataFusion supports the given cast in a way that is
/// compatible with Spark.
fn is_datafusion_spark_compatible(from_type: &DataType, to_type: &DataType) -> bool {
    if from_type == to_type {
        return true;
    }
    match from_type {
        DataType::Null => {
            matches!(to_type, DataType::List(_))
        }
        DataType::Boolean => is_df_cast_from_bool_spark_compatible(to_type),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            matches!(
                to_type,
                DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Utf8
            )
        }
        DataType::Float32 | DataType::Float64 => matches!(
            to_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
        ),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => matches!(
            to_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Utf8
        ),
        DataType::Utf8 => is_df_cast_from_string_spark_compatible(to_type),
        DataType::Date32 => matches!(to_type, DataType::Int32 | DataType::Utf8),
        DataType::Timestamp(_, _) => {
            matches!(
                to_type,
                DataType::Int64
                    | DataType::Date32
                    | DataType::Utf8
                    | DataType::Timestamp(_, _)
            )
        }
        DataType::Binary => {
            matches!(to_type, DataType::Utf8)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray, TimestampMicrosecondType};
    use arrow::datatypes::{Field, Fields, TimeUnit};

    #[test]
    fn test_spark_cast_scalar_string_to_int() {
        let opts = SparkCastOptions::new(EvalMode::Legacy, "UTC");
        let result = spark_cast_inner(
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("42".to_string()))),
            &DataType::Int32,
            &opts,
        )
        .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(42))) => {}
            other => panic!("Expected ScalarValue::Int32(42), got {:?}", other),
        }
    }

    #[test]
    fn test_spark_cast_array_string_to_int() {
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("1"),
            Some("2"),
            Some("invalid"),
            None,
        ]));
        let opts = SparkCastOptions::new(EvalMode::Legacy, "UTC");
        let result = cast_array(array, &DataType::Int32, &opts).unwrap();
        let int_arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_arr.value(0), 1);
        assert_eq!(int_arr.value(1), 2);
        assert!(int_arr.is_null(2)); // "invalid" -> null in Legacy
        assert!(int_arr.is_null(3));
    }

    #[test]
    fn test_spark_cast_struct_to_struct() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2)]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let c: ArrayRef = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("a", DataType::Int32, true)), a),
            (Arc::new(Field::new("b", DataType::Utf8, true)), b),
        ]));

        let fields = Fields::from(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]);

        let opts = SparkCastOptions::new(EvalMode::Legacy, "UTC");
        let result = spark_cast_inner(
            ColumnarValue::Array(c),
            &DataType::Struct(fields),
            &opts,
        )
        .unwrap();
        if let ColumnarValue::Array(cast_array) = result {
            assert_eq!(2, cast_array.len());
            let a = cast_array.as_struct().column(0).as_string::<i32>();
            assert_eq!("1", a.value(0));
        } else {
            unreachable!()
        }
    }

    #[test]
    fn test_is_datafusion_spark_compatible() {
        assert!(is_datafusion_spark_compatible(
            &DataType::Int32,
            &DataType::Int32
        ));
        assert!(is_datafusion_spark_compatible(
            &DataType::Int32,
            &DataType::Float64
        ));
        assert!(is_datafusion_spark_compatible(
            &DataType::Boolean,
            &DataType::Int32
        ));
        assert!(!is_datafusion_spark_compatible(
            &DataType::Boolean,
            &DataType::Decimal128(10, 2)
        ));
    }
}
