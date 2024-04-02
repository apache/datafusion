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

use arrow::datatypes::{DataType, TimeUnit};

use crate::datetime::common::*;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use super::to_timestamp::ToTimestampSecondsFunc;

#[derive(Debug)]
pub struct ToUnixtimeFunc {
    signature: Signature,
}

impl Default for ToUnixtimeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToUnixtimeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToUnixtimeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_unixtime"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.is_empty() {
            return exec_err!("to_unixtime function requires 1 or more arguments, got 0");
        }

        // validate that any args after the first one are Utf8
        if args.len() > 1 {
            validate_data_types(args, "to_unixtime")?;
        }

        match args[0].data_type() {
            DataType::Int32 | DataType::Int64 | DataType::Null | DataType::Float64 => {
                args[0].cast_to(&DataType::Int64, None)
            }
            DataType::Date64 | DataType::Date32 | DataType::Timestamp(_, None) => args[0]
                .cast_to(&DataType::Timestamp(TimeUnit::Second, None), None)?
                .cast_to(&DataType::Int64, None),
            DataType::Utf8 => ToTimestampSecondsFunc::new()
                .invoke(args)?
                .cast_to(&DataType::Int64, None),
            other => {
                exec_err!("Unsupported data type {:?} for function to_unixtime", other)
            }
        }
    }
}
