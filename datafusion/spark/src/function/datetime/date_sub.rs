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
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute;
use arrow::datatypes::{DataType, Date32Type};
use datafusion_common::cast::{
    as_date32_array, as_int16_array, as_int32_array, as_int8_array,
};
use datafusion_common::{internal_err, Result};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateSub {
    signature: Signature,
}

impl Default for SparkDateSub {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDateSub {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Date32, DataType::Int8]),
                    TypeSignature::Exact(vec![DataType::Date32, DataType::Int16]),
                    TypeSignature::Exact(vec![DataType::Date32, DataType::Int32]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkDateSub {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "date_sub"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_date_sub, vec![])(&args.args)
    }
}

fn spark_date_sub(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [date_arg, days_arg] = args else {
        return internal_err!(
            "Spark `date_sub` function requires 2 arguments, got {}",
            args.len()
        );
    };
    let date_array = as_date32_array(date_arg)?;
    let result = match days_arg.data_type() {
        DataType::Int8 => {
            let days_array = as_int8_array(days_arg)?;
            compute::binary::<_, _, _, Date32Type>(
                date_array,
                days_array,
                |date, days| date - days as i32,
            )?
        }
        DataType::Int16 => {
            let days_array = as_int16_array(days_arg)?;
            compute::binary::<_, _, _, Date32Type>(
                date_array,
                days_array,
                |date, days| date - days as i32,
            )?
        }
        DataType::Int32 => {
            let days_array = as_int32_array(days_arg)?;
            compute::binary::<_, _, _, Date32Type>(
                date_array,
                days_array,
                |date, days| date - days,
            )?
        }
        _ => {
            return internal_err!(
                "Spark `date_sub` function: argument must be int8, int16, int32, got {:?}",
                days_arg.data_type()
            );
        }
    };
    Ok(Arc::new(result))
}
