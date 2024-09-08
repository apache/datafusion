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

use arrow::datatypes::{
    Date32Type, Date64Type, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
use arrow_schema::{DataType, Field, TimeUnit};
use datafusion_common::{not_impl_err, Result};
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion_functions_aggregate_common::accumulator::{
    AccumulatorArgs, StateFieldsArgs,
};
use datafusion_functions_aggregate_common::aggregate::mode::{
    BytesModeAccumulator, BytesViewModeAccumulator, FloatModeAccumulator,
    PrimitiveModeAccumulator,
};
use datafusion_physical_expr::binary_map::OutputType;
use std::any::Any;
use std::fmt::Debug;

make_udaf_expr_and_func!(
    ModeFunction,
    mode,
    x,
    "Calculates the most frequent value.",
    mode_udaf
);

/// The `ModeFunction` calculates the mode (most frequent value) from a set of values.
///
/// - Null values are ignored during the calculation.
/// - If multiple values have the same frequency, the first encountered value with the highest frequency is returned.
/// - In the case of `Utf8` or `Utf8View`, the first value encountered in the original order with the highest frequency is returned.
pub struct ModeFunction {
    signature: Signature,
}

impl Debug for ModeFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModeFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for ModeFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl ModeFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ModeFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "mode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        let value_type = args.input_types[0].clone();

        Ok(vec![
            Field::new("values", value_type, true),
            Field::new("frequencies", DataType::UInt64, true),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = &acc_args.exprs[0].data_type(acc_args.schema)?;

        let accumulator: Box<dyn Accumulator> = match data_type {
            DataType::Int8 => {
                Box::new(PrimitiveModeAccumulator::<Int8Type>::new(data_type))
            }
            DataType::Int16 => {
                Box::new(PrimitiveModeAccumulator::<Int16Type>::new(data_type))
            }
            DataType::Int32 => {
                Box::new(PrimitiveModeAccumulator::<Int32Type>::new(data_type))
            }
            DataType::Int64 => {
                Box::new(PrimitiveModeAccumulator::<Int64Type>::new(data_type))
            }
            DataType::UInt8 => {
                Box::new(PrimitiveModeAccumulator::<UInt8Type>::new(data_type))
            }
            DataType::UInt16 => {
                Box::new(PrimitiveModeAccumulator::<UInt16Type>::new(data_type))
            }
            DataType::UInt32 => {
                Box::new(PrimitiveModeAccumulator::<UInt32Type>::new(data_type))
            }
            DataType::UInt64 => {
                Box::new(PrimitiveModeAccumulator::<UInt64Type>::new(data_type))
            }

            DataType::Date32 => {
                Box::new(PrimitiveModeAccumulator::<Date32Type>::new(data_type))
            }
            DataType::Date64 => {
                Box::new(PrimitiveModeAccumulator::<Date64Type>::new(data_type))
            }
            DataType::Time32(TimeUnit::Millisecond) => Box::new(
                PrimitiveModeAccumulator::<Time32MillisecondType>::new(data_type),
            ),
            DataType::Time32(TimeUnit::Second) => {
                Box::new(PrimitiveModeAccumulator::<Time32SecondType>::new(data_type))
            }
            DataType::Time64(TimeUnit::Microsecond) => Box::new(
                PrimitiveModeAccumulator::<Time64MicrosecondType>::new(data_type),
            ),
            DataType::Time64(TimeUnit::Nanosecond) => Box::new(
                PrimitiveModeAccumulator::<Time64NanosecondType>::new(data_type),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Box::new(
                PrimitiveModeAccumulator::<TimestampMicrosecondType>::new(data_type),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Box::new(
                PrimitiveModeAccumulator::<TimestampMillisecondType>::new(data_type),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Box::new(
                PrimitiveModeAccumulator::<TimestampNanosecondType>::new(data_type),
            ),
            DataType::Timestamp(TimeUnit::Second, _) => Box::new(
                PrimitiveModeAccumulator::<TimestampSecondType>::new(data_type),
            ),

            DataType::Float16 => {
                Box::new(FloatModeAccumulator::<Float16Type>::new(data_type))
            }
            DataType::Float32 => {
                Box::new(FloatModeAccumulator::<Float32Type>::new(data_type))
            }
            DataType::Float64 => {
                Box::new(FloatModeAccumulator::<Float64Type>::new(data_type))
            }

            DataType::Utf8 => {
                Box::new(BytesModeAccumulator::<i32>::new(OutputType::Utf8))
            }
            DataType::LargeUtf8 => {
                Box::new(BytesModeAccumulator::<i64>::new(OutputType::Utf8))
            }
            DataType::Utf8View => {
                Box::new(BytesViewModeAccumulator::new(OutputType::Utf8View))
            }
            _ => {
                return not_impl_err!(
                    "Unsupported data type: {:?} for mode function",
                    data_type
                );
            }
        };

        Ok(accumulator)
    }
}
