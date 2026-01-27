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
use arrow::datatypes::{DataType, Date32Type, Field, FieldRef};
use datafusion_common::cast::{
    as_date32_array, as_int8_array, as_int16_array, as_int32_array,
};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateAdd {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkDateAdd {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDateAdd {
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
            aliases: vec!["dateadd".to_string()],
        }
    }
}

impl ScalarUDFImpl for SparkDateAdd {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "date_add"
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
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Date32,
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_date_add, vec![])(&args.args)
    }
}

fn spark_date_add(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [date_arg, days_arg] = take_function_args("date_add", args)?;
    let date_array = as_date32_array(date_arg)?;
    let result = match days_arg.data_type() {
        DataType::Int8 => {
            let days_array = as_int8_array(days_arg)?;
            compute::binary::<_, _, _, Date32Type>(
                date_array,
                days_array,
                |date, days| date.wrapping_add(days as i32),
            )?
        }
        DataType::Int16 => {
            let days_array = as_int16_array(days_arg)?;
            compute::binary::<_, _, _, Date32Type>(
                date_array,
                days_array,
                |date, days| date.wrapping_add(days as i32),
            )?
        }
        DataType::Int32 => {
            let days_array = as_int32_array(days_arg)?;
            compute::binary::<_, _, _, Date32Type>(
                date_array,
                days_array,
                |date, days| date.wrapping_add(days),
            )?
        }
        _ => {
            return internal_err!(
                "Spark `date_add` function: argument must be int8, int16, int32, got {:?}",
                days_arg.data_type()
            );
        }
    };
    Ok(Arc::new(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    #[test]
    fn test_date_add_non_nullable_inputs() {
        let func = SparkDateAdd::new();
        let args = &[
            Arc::new(Field::new("date", DataType::Date32, false)),
            Arc::new(Field::new("num", DataType::Int8, false)),
        ];

        let ret_field = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: args,
                scalar_arguments: &[None, None],
            })
            .unwrap();

        assert_eq!(ret_field.data_type(), &DataType::Date32);
        assert!(!ret_field.is_nullable());
    }

    #[test]
    fn test_date_add_nullable_inputs() {
        let func = SparkDateAdd::new();
        let args = &[
            Arc::new(Field::new("date", DataType::Date32, false)),
            Arc::new(Field::new("num", DataType::Int16, true)),
        ];

        let ret_field = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: args,
                scalar_arguments: &[None, None],
            })
            .unwrap();

        assert_eq!(ret_field.data_type(), &DataType::Date32);
        assert!(ret_field.is_nullable());
    }
}
