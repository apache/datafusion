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

use std::any::Any;

use arrow::array::ArrayRef;
use arrow::compute::{DatePart, date_part};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_common::utils::take_function_args;
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

/// Creates a signature for datetime extraction functions that accept timestamp types.
fn extract_signature() -> Signature {
    Signature::coercible(
        vec![Coercion::new_exact(TypeSignatureClass::Timestamp)],
        Volatility::Immutable,
    )
}

// -----------------------------------------------------------------------------
// SparkHour
// -----------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkHour {
    signature: Signature,
}

impl Default for SparkHour {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkHour {
    pub fn new() -> Self {
        Self {
            signature: extract_signature(),
        }
    }
}

impl ScalarUDFImpl for SparkHour {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "hour"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_hour, vec![])(&args.args)
    }
}

fn spark_hour(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [ts_arg] = take_function_args("hour", args)?;
    let result = date_part(ts_arg.as_ref(), DatePart::Hour)?;
    Ok(result)
}

// -----------------------------------------------------------------------------
// SparkMinute
// -----------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMinute {
    signature: Signature,
}

impl Default for SparkMinute {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMinute {
    pub fn new() -> Self {
        Self {
            signature: extract_signature(),
        }
    }
}

impl ScalarUDFImpl for SparkMinute {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "minute"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_minute, vec![])(&args.args)
    }
}

fn spark_minute(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [ts_arg] = take_function_args("minute", args)?;
    let result = date_part(ts_arg.as_ref(), DatePart::Minute)?;
    Ok(result)
}

// -----------------------------------------------------------------------------
// SparkSecond
// -----------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSecond {
    signature: Signature,
}

impl Default for SparkSecond {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSecond {
    pub fn new() -> Self {
        Self {
            signature: extract_signature(),
        }
    }
}

impl ScalarUDFImpl for SparkSecond {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "second"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_second, vec![])(&args.args)
    }
}

fn spark_second(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [ts_arg] = take_function_args("second", args)?;
    let result = date_part(ts_arg.as_ref(), DatePart::Second)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, TimestampMicrosecondArray};
    use arrow::datatypes::TimeUnit;
    use std::sync::Arc;

    #[test]
    fn test_spark_hour() {
        // Create a timestamp array: 2024-01-15 14:30:45 UTC (in microseconds)
        // 14:30:45 -> hour = 14
        let ts_micros = 1_705_329_045_000_000_i64; // 2024-01-15 14:30:45 UTC
        let ts_array = TimestampMicrosecondArray::from(vec![Some(ts_micros), None]);
        let ts_array = Arc::new(ts_array) as ArrayRef;

        let result = spark_hour(&[ts_array]).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(result.value(0), 14);
        assert!(result.is_null(1));
    }

    #[test]
    fn test_spark_minute() {
        // 14:30:45 -> minute = 30
        let ts_micros = 1_705_329_045_000_000_i64;
        let ts_array = TimestampMicrosecondArray::from(vec![Some(ts_micros), None]);
        let ts_array = Arc::new(ts_array) as ArrayRef;

        let result = spark_minute(&[ts_array]).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(result.value(0), 30);
        assert!(result.is_null(1));
    }

    #[test]
    fn test_spark_second() {
        // 14:30:45 -> second = 45
        let ts_micros = 1_705_329_045_000_000_i64;
        let ts_array = TimestampMicrosecondArray::from(vec![Some(ts_micros), None]);
        let ts_array = Arc::new(ts_array) as ArrayRef;

        let result = spark_second(&[ts_array]).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(result.value(0), 45);
        assert!(result.is_null(1));
    }

    #[test]
    fn test_hour_return_type() {
        let func = SparkHour::new();
        let result = func
            .return_type(&[DataType::Timestamp(TimeUnit::Microsecond, None)])
            .unwrap();
        assert_eq!(result, DataType::Int32);
    }

    #[test]
    fn test_minute_return_type() {
        let func = SparkMinute::new();
        let result = func
            .return_type(&[DataType::Timestamp(TimeUnit::Microsecond, None)])
            .unwrap();
        assert_eq!(result, DataType::Int32);
    }

    #[test]
    fn test_second_return_type() {
        let func = SparkSecond::new();
        let result = func
            .return_type(&[DataType::Timestamp(TimeUnit::Microsecond, None)])
            .unwrap();
        assert_eq!(result, DataType::Int32);
    }
}
