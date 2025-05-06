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

use std::{any::Any, sync::Arc};

use arrow::{
    array::{ArrayRef, ArrowNativeTypeOp, AsArray},
    datatypes::{
        DataType, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type,
        Int64Type, Int8Type, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE,
    },
};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use num::integer::{div_ceil, div_floor};

use crate::function::error_utils::{
    generic_exec_err, generic_internal_err, invalid_arg_count_exec_err,
    unsupported_data_type_exec_err, unsupported_data_types_exec_err,
};

use super::round_decimal_base;

fn ceil_floor_coerce_types(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() == 1 {
        if arg_types[0].is_numeric() {
            Ok(vec![ceil_floor_coerce_first_arg(name, &arg_types[0])?])
        } else {
            Err(unsupported_data_types_exec_err(
                name,
                "Numeric Type",
                arg_types,
            ))
        }
    } else if arg_types.len() == 2 {
        if arg_types[0].is_numeric() && arg_types[1].is_integer() {
            Ok(vec![
                ceil_floor_coerce_first_arg(name, &arg_types[0])?,
                DataType::Int32,
            ])
        } else {
            Err(unsupported_data_types_exec_err(
                name,
                "Numeric Type for expr and Integer Type for target scale",
                arg_types,
            ))
        }
    } else {
        Err(invalid_arg_count_exec_err(name, (1, 2), arg_types.len()))
    }
}

#[allow(dead_code)]
fn ceil_floor_return_type_from_args(
    name: &str,
    args: ReturnFieldArgs,
) -> Result<DataType> {
    let arg_fields = args.arg_fields;
    let scalar_arguments = args.scalar_arguments;
    let return_type = if arg_fields.len() == 1 {
        match &arg_fields[0].data_type() {
            DataType::Decimal128(precision, scale) => {
                let (precision, scale) =
                    round_decimal_base(*precision as i32, *scale as i32, 0, true);
                Ok(DataType::Decimal128(precision, scale))
            }
            DataType::Decimal256(precision, scale) => {
                if *precision <= DECIMAL128_MAX_PRECISION
                    && *scale <= DECIMAL128_MAX_SCALE
                {
                    let (precision, scale) =
                        round_decimal_base(*precision as i32, *scale as i32, 0, false);
                    Ok(DataType::Decimal128(precision, scale))
                } else {
                    Err(unsupported_data_type_exec_err(
                        name,
                        format!("Decimal Type must have precision <= {DECIMAL128_MAX_PRECISION} and scale <= {DECIMAL128_MAX_SCALE}").as_str(),
                        &arg_fields[0].data_type(),
                    ))
                }
            }
            _ => Ok(DataType::Int64),
        }
    } else if arg_fields.len() == 2 {
        if let Some(target_scale) = scalar_arguments[1] {
            let expr = &arg_fields[0].data_type();
            let target_scale: i32 = match target_scale {
                ScalarValue::Int8(Some(v)) => Ok(*v as i32),
                ScalarValue::Int16(Some(v)) => Ok(*v as i32),
                ScalarValue::Int32(Some(v)) => Ok(*v),
                ScalarValue::Int64(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt8(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt16(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt32(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt64(Some(v)) => Ok(*v as i32),
                _ => Err(unsupported_data_type_exec_err(
                    name,
                    "Target scale must be Integer literal",
                    &target_scale.data_type(),
                )),
            }?;
            if target_scale < -38 {
                return Err(generic_exec_err(
                    name,
                    "Target scale must be greater than -38",
                ));
            }
            let (precision, scale) = match expr {
                DataType::Int8 => Ok((3, 0)),
                DataType::UInt8 | DataType::Int16 => Ok((5, 0)),
                DataType::UInt16 | DataType::Int32 => Ok((10, 0)),
                DataType::UInt32 | DataType::UInt64 | DataType::Int64 => Ok((20, 0)),
                DataType::Float32 => Ok((14, 7)),
                DataType::Float64 => Ok((30, 15)),
                DataType::Decimal128(precision, scale)
                | DataType::Decimal256(precision, scale) => {
                    if *precision <= DECIMAL128_MAX_PRECISION
                        && *scale <= DECIMAL128_MAX_SCALE
                    {
                        Ok((*precision as i32, *scale as i32))
                    } else {
                        Err(unsupported_data_type_exec_err(
                            name,
                            format!("Decimal Type must have precision <= {DECIMAL128_MAX_PRECISION} and scale <= {DECIMAL128_MAX_SCALE}").as_str(),
                            &arg_fields[0].data_type(),
                        ))
                    }
                }
                _ => Err(unsupported_data_type_exec_err(
                    name,
                    "Numeric Type for expr",
                    expr,
                )),
            }?;
            let (precision, scale) =
                round_decimal_base(precision, scale, target_scale, true);
            Ok(DataType::Decimal128(precision, scale))
        } else {
            Err(generic_exec_err(
                name,
                "Target scale must be Integer literal, received: None",
            ))
        }
    } else {
        Err(invalid_arg_count_exec_err(name, (1, 2), arg_fields.len()))
    }?;
    Ok(return_type)
}

fn ceil_floor_coerce_first_arg(name: &str, arg_type: &DataType) -> Result<DataType> {
    if arg_type.is_numeric() {
        match arg_type {
            DataType::UInt8 => Ok(DataType::Int16),
            DataType::UInt16 => Ok(DataType::Int32),
            DataType::UInt32 | DataType::UInt64 => Ok(DataType::Int64),
            DataType::Decimal256(precision, scale) => {
                if *precision <= DECIMAL128_MAX_PRECISION
                    && *scale <= DECIMAL128_MAX_SCALE
                {
                    Ok(DataType::Decimal128(*precision, *scale))
                } else {
                    Err(unsupported_data_type_exec_err(
                        name,
                        format!("Decimal Type must have precision <= {DECIMAL128_MAX_PRECISION} and scale <= {DECIMAL128_MAX_SCALE}").as_str(),
                        arg_type,
                    ))
                }
            }
            other => Ok(other.clone()),
        }
    } else {
        Err(unsupported_data_type_exec_err(
            name,
            "First arg must be Numeric Type",
            arg_type,
        ))
    }
}

#[inline]
fn get_return_type_precision_scale(return_type: &DataType) -> Result<(u8, i8)> {
    match return_type {
        DataType::Decimal128(precision, scale) => Ok((*precision, *scale)),
        other => Err(generic_internal_err(
            "ceil",
            format!("Expected return type to be Decimal128, got: {other}").as_str(),
        )),
    }
}

#[derive(Debug)]
pub struct SparkCeil {
    signature: Signature,
}

impl Default for SparkCeil {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCeil {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkCeil {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ceil"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(generic_internal_err(
            "ceil",
            "`return_type` should not be called, call `return_type_from_args` instead",
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg_len = args.args.len();
        let target_scale = if arg_len == 1 {
            Ok(&None)
        } else if arg_len == 2 {
            let target_scale = &args.args[1];
            match target_scale {
                ColumnarValue::Scalar(ScalarValue::Int32(value)) => Ok(value),
                _ => Err(unsupported_data_type_exec_err(
                    "ceil",
                    "Target scale must be Integer literal",
                    &target_scale.data_type(),
                )),
            }
        } else {
            Err(invalid_arg_count_exec_err("ceil", (1, 2), arg_len))
        }?;
        let arg = &args.args[0];
        let return_type = args.return_field.data_type();
        spark_ceil_floor("ceil", arg, target_scale, return_type)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        ceil_floor_coerce_types("ceil", arg_types)
    }
}

#[derive(Debug)]
pub struct SparkFloor {
    signature: Signature,
}

impl Default for SparkFloor {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkFloor {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkFloor {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "spark_floor"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(generic_internal_err(
            "floor",
            "`return_type` should not be called, call `return_type_from_args` instead",
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg_len = args.args.len();
        let target_scale = if arg_len == 1 {
            Ok(&None)
        } else if arg_len == 2 {
            let target_scale = &args.args[1];
            match target_scale {
                ColumnarValue::Scalar(ScalarValue::Int32(value)) => Ok(value),
                _ => Err(unsupported_data_type_exec_err(
                    "floor",
                    "Target scale must be Integer literal",
                    &target_scale.data_type(),
                )),
            }
        } else {
            Err(invalid_arg_count_exec_err("floor", (1, 2), arg_len))
        }?;
        let arg = &args.args[0];
        let return_type = args.return_field.data_type();
        spark_ceil_floor("floor", arg, target_scale, return_type)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        ceil_floor_coerce_types("floor", arg_types)
    }
}

fn spark_ceil_floor(
    name: &str,
    arg: &ColumnarValue,
    target_scale: &Option<i32>,
    return_type: &DataType,
) -> Result<ColumnarValue> {
    if matches!(
        arg.data_type(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    ) {
        if let Some(target_scale) = *target_scale {
            if target_scale >= 0 {
                Ok(arg.cast_to(return_type, None)?)
            } else {
                let (return_type_precision, return_type_scale) =
                    get_return_type_precision_scale(return_type)?;
                match arg.data_type() {
                    DataType::Int8 => match arg {
                        ColumnarValue::Scalar(ScalarValue::Int8(value)) => {
                            let result = value.map(|decimal| {
                                ceil_floor_with_target_scale(
                                    name,
                                    decimal as i128,
                                    0,
                                    target_scale,
                                )
                            });
                            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                                result,
                                return_type_precision,
                                return_type_scale,
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(
                            Arc::new(
                                array
                                    .as_primitive::<Int8Type>()
                                    .unary::<_, Decimal128Type>(|decimal| {
                                        ceil_floor_with_target_scale(
                                            name,
                                            decimal as i128,
                                            0,
                                            target_scale,
                                        )
                                    })
                                    .with_data_type(DataType::Decimal128(
                                        return_type_precision,
                                        return_type_scale,
                                    )),
                            ) as ArrayRef,
                        )),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Int8).as_str(),
                            &arg.data_type(),
                        )),
                    },
                    DataType::Int16 => match arg {
                        ColumnarValue::Scalar(ScalarValue::Int16(value)) => {
                            let result = value.map(|decimal| {
                                ceil_floor_with_target_scale(
                                    name,
                                    decimal as i128,
                                    0,
                                    target_scale,
                                )
                            });
                            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                                result,
                                return_type_precision,
                                return_type_scale,
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(
                            Arc::new(
                                array
                                    .as_primitive::<Int16Type>()
                                    .unary::<_, Decimal128Type>(|decimal| {
                                        ceil_floor_with_target_scale(
                                            name,
                                            decimal as i128,
                                            0,
                                            target_scale,
                                        )
                                    })
                                    .with_data_type(DataType::Decimal128(
                                        return_type_precision,
                                        return_type_scale,
                                    )),
                            ) as ArrayRef,
                        )),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Int16).as_str(),
                            &arg.data_type(),
                        )),
                    },
                    DataType::Int32 => match arg {
                        ColumnarValue::Scalar(ScalarValue::Int32(value)) => {
                            let result = value.map(|decimal| {
                                ceil_floor_with_target_scale(
                                    name,
                                    decimal as i128,
                                    0,
                                    target_scale,
                                )
                            });
                            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                                result,
                                return_type_precision,
                                return_type_scale,
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(
                            Arc::new(
                                array
                                    .as_primitive::<Int32Type>()
                                    .unary::<_, Decimal128Type>(|decimal| {
                                        ceil_floor_with_target_scale(
                                            name,
                                            decimal as i128,
                                            0,
                                            target_scale,
                                        )
                                    })
                                    .with_data_type(DataType::Decimal128(
                                        return_type_precision,
                                        return_type_scale,
                                    )),
                            ) as ArrayRef,
                        )),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Int32).as_str(),
                            &arg.data_type(),
                        )),
                    },
                    DataType::Int64 => match arg {
                        ColumnarValue::Scalar(ScalarValue::Int64(value)) => {
                            let result = value.map(|decimal| {
                                ceil_floor_with_target_scale(
                                    name,
                                    decimal as i128,
                                    0,
                                    target_scale,
                                )
                            });
                            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                                result,
                                return_type_precision,
                                return_type_scale,
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(
                            Arc::new(
                                array
                                    .as_primitive::<Int64Type>()
                                    .unary::<_, Decimal128Type>(|decimal| {
                                        ceil_floor_with_target_scale(
                                            name,
                                            decimal as i128,
                                            0,
                                            target_scale,
                                        )
                                    })
                                    .with_data_type(DataType::Decimal128(
                                        return_type_precision,
                                        return_type_scale,
                                    )),
                            ) as ArrayRef,
                        )),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Int64).as_str(),
                            &arg.data_type(),
                        )),
                    },
                    other => Err(unsupported_data_type_exec_err(
                        name,
                        "Numeric Type for expr",
                        &other,
                    )),
                }
            }
        } else {
            Ok(arg.cast_to(&DataType::Int64, None)?)
        }
    } else {
        match arg.data_type() {
            DataType::Float32 => {
                if let Some(target_scale) = *target_scale {
                    let (return_type_precision, return_type_scale) =
                        get_return_type_precision_scale(return_type)?;
                    let arg = arg.cast_to(
                        &DataType::Decimal128(return_type_precision, return_type_scale),
                        None,
                    )?;
                    decimal128_ceil_floor(name, &arg, &Some(target_scale), return_type)
                } else {
                    let func = if matches!(name, "ceil") {
                        f32::ceil
                    } else {
                        f32::floor
                    };
                    match arg {
                        ColumnarValue::Scalar(ScalarValue::Float32(value)) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                                value.map(|x| func(x) as i64),
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(
                            Arc::new(
                                array
                                    .as_primitive::<Float32Type>()
                                    .unary::<_, Int64Type>(|x| func(x) as i64),
                            ) as ArrayRef,
                        )),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Float32).as_str(),
                            &arg.data_type(),
                        )),
                    }
                }
            }
            DataType::Float64 => {
                if let Some(target_scale) = *target_scale {
                    let (return_type_precision, return_type_scale) =
                        get_return_type_precision_scale(return_type)?;
                    let arg = arg.cast_to(
                        &DataType::Decimal128(return_type_precision, return_type_scale),
                        None,
                    )?;
                    decimal128_ceil_floor(name, &arg, &Some(target_scale), return_type)
                } else {
                    let func = if matches!(name, "ceil") {
                        f64::ceil
                    } else {
                        f64::floor
                    };
                    match arg {
                        ColumnarValue::Scalar(ScalarValue::Float64(value)) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                                value.map(|x| func(x) as i64),
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(
                            Arc::new(
                                array
                                    .as_primitive::<Float64Type>()
                                    .unary::<_, Int64Type>(|x| func(x) as i64),
                            ) as ArrayRef,
                        )),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Float32).as_str(),
                            &arg.data_type(),
                        )),
                    }
                }
            }
            DataType::Decimal128(_precision, _scale) => {
                decimal128_ceil_floor(name, arg, target_scale, return_type)
            }
            other => Err(unsupported_data_type_exec_err(
                name,
                "Numeric Type for expr",
                &other,
            )),
        }
    }
}

fn decimal128_ceil_floor(
    name: &str,
    arg: &ColumnarValue,
    target_scale: &Option<i32>,
    return_type: &DataType,
) -> Result<ColumnarValue> {
    match arg.data_type() {
        DataType::Decimal128(_precision, scale) => {
            let (return_type_precision, return_type_scale) =
                get_return_type_precision_scale(return_type)?;
            let target_scale = (*target_scale).unwrap_or(0);
            match arg {
                ColumnarValue::Scalar(ScalarValue::Decimal128(
                    value,
                    _precision,
                    _scale,
                )) => {
                    if let Some(value) = value {
                        Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                            Some(ceil_floor_with_target_scale(
                                name,
                                *value,
                                scale,
                                target_scale,
                            )),
                            return_type_precision,
                            return_type_scale,
                        )))
                    } else {
                        Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                            None,
                            return_type_precision,
                            return_type_scale,
                        )))
                    }
                }
                ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                    array
                        .as_primitive::<Decimal128Type>()
                        .unary::<_, Decimal128Type>(|value| {
                            ceil_floor_with_target_scale(name, value, scale, target_scale)
                        })
                        .with_data_type(DataType::Decimal128(
                            return_type_precision,
                            return_type_scale,
                        )),
                )
                    as ArrayRef)),
                _ => Err(unsupported_data_type_exec_err(
                    name,
                    "Decimal128 Type",
                    &arg.data_type(),
                )),
            }
        }
        other => Err(unsupported_data_type_exec_err(
            name,
            "Decimal128 Type for Decimal128 ceil",
            &other,
        )),
    }
}

#[inline]
fn ceil_floor_with_target_scale(
    name: &str,
    decimal: i128,
    scale: i8,
    target_scale: i32,
) -> i128 {
    // Round to powers of 10 to the left of decimal point when target_scale < 0
    if target_scale < 0 {
        // Convert to integer with scale 0
        let integer_value = match scale.cmp(&0) {
            std::cmp::Ordering::Greater => {
                let factor = 10_i128.pow_wrapping(scale as u32);
                if matches!(name, "ceil") {
                    div_ceil(decimal, factor)
                } else {
                    div_floor(decimal, factor)
                }
            }
            std::cmp::Ordering::Less => decimal * 10_i128.pow_wrapping((-scale) as u32),
            std::cmp::Ordering::Equal => decimal,
        };
        let pow_factor = 10_i128.pow_wrapping((-target_scale) as u32);
        if matches!(name, "ceil") {
            div_ceil(integer_value, pow_factor) * pow_factor
        } else {
            div_floor(integer_value, pow_factor) * pow_factor
        }
    } else {
        let scale_diff = target_scale - (scale as i32);
        if scale_diff >= 0 {
            decimal * 10_i128.pow_wrapping(scale_diff as u32)
        } else {
            let abs_diff = (-scale_diff) as u32;
            if matches!(name, "ceil") {
                div_ceil(decimal, 10_i128.pow_wrapping(abs_diff))
            } else {
                div_floor(decimal, 10_i128.pow_wrapping(abs_diff))
            }
        }
    }
}
