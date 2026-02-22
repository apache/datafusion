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

use crate::function::functions_nested_utils::make_scalar_function;
use arrow::datatypes::{DataType, Field, FieldRef, IntervalMonthDayNano};
use datafusion_common::internal_err;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions_nested::range::Range;
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible `sequence` expression.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#sequence>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSequence {
    signature: Signature,
}

impl Default for SparkSequence {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSequence {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkSequence {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sequence"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        let return_type = if args.arg_fields[0].data_type().is_null()
            || args.arg_fields[1].data_type().is_null()
        {
            DataType::Null
        } else {
            DataType::List(Arc::new(Field::new_list_field(
                args.arg_fields[0].data_type().clone(),
                true,
            )))
        };

        Ok(Arc::new(Field::new(
            "this_field_name_is_irrelevant",
            return_type,
            true,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types.len() {
            2 => {
                let first_data_type =
                    check_type(arg_types[0].clone(), "first".to_string().as_str())?;
                let second_data_type =
                    check_type(arg_types[1].clone(), "second".to_string().as_str())?;

                if !first_data_type.is_null()
                    && !second_data_type.is_null()
                    && (first_data_type != second_data_type)
                {
                    return exec_err!(
                        "first({first_data_type}) and second({second_data_type}) input types should be same"
                    );
                }

                Ok(vec![first_data_type, second_data_type])
            }
            3 => {
                let first_data_type =
                    check_type(arg_types[0].clone(), "first".to_string().as_str())?;
                let second_data_type =
                    check_type(arg_types[1].clone(), "second".to_string().as_str())?;
                let third_data_type = check_interval_type(
                    arg_types[2].clone(),
                    "third".to_string().as_str(),
                )?;

                if !first_data_type.is_null() && !second_data_type.is_null() {
                    if first_data_type != second_data_type {
                        return exec_err!(
                            "first({first_data_type}) and second({second_data_type}) input types should be same"
                        );
                    }

                    if !check_interval_type_by_first_type(
                        &first_data_type,
                        &third_data_type,
                    ) {
                        return exec_err!(
                            "interval type should be integer for integer input or time based"
                        );
                    }
                }

                Ok(vec![first_data_type, second_data_type, third_data_type])
            }
            _ => {
                exec_err!("num of input parameters should be 2 or 3")
            }
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = &args.args;

        if args.iter().any(|arg| arg.data_type().is_null()) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }
        match args[0].data_type() {
            DataType::Int64 => make_scalar_function(|args| {
                Range::generate_series().gen_range_inner(args)
            })(args),
            DataType::Date32 | DataType::Date64 => {
                let optional_new_args = add_interval_if_not_exists(args);
                let new_args = match optional_new_args {
                    Some(new_args) => &new_args.to_owned(),
                    None => args,
                };
                make_scalar_function(|args| Range::generate_series().gen_range_date(args))(
                    new_args,
                )
            }
            DataType::Timestamp(_, _) => {
                let optional_new_args = add_interval_if_not_exists(args);
                let new_args = match optional_new_args {
                    Some(new_args) => &new_args.to_owned(),
                    None => args,
                };
                make_scalar_function(|args| {
                    Range::generate_series().gen_range_timestamp(args)
                })(new_args)
            }
            dt => {
                internal_err!(
                    "Signature failed to guard unknown input type for {}: {dt}",
                    self.name()
                )
            }
        }
    }
}

fn check_type(
    data_type: DataType,
    param_name: &str,
) -> Result<DataType, DataFusionError> {
    let result_type = match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            DataType::Int64
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            DataType::UInt64
        }
        DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Null => data_type,
        _ => {
            return exec_err!(
                "{} parameter type must be one of integer, date or timestamp type but found: {}",
                param_name,
                data_type
            );
        }
    };
    Ok(result_type)
}

fn check_interval_type(
    data_type: DataType,
    param_name: &str,
) -> Result<DataType, DataFusionError> {
    let result_type = match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            DataType::Int64
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            DataType::UInt64
        }
        DataType::Interval(_) => data_type,
        _ => {
            return exec_err!(
                "{} parameter type must be one of integer or interval type but found: {}",
                param_name,
                data_type
            );
        }
    };
    Ok(result_type)
}

fn check_interval_type_by_first_type(
    first_data_type: &DataType,
    third_data_type: &DataType,
) -> bool {
    match first_data_type {
        DataType::Int64 | DataType::UInt64 => first_data_type == third_data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
            matches!(third_data_type, DataType::Interval(_))
        }
        _ => false,
    }
}

fn add_interval_if_not_exists(args: &[ColumnarValue]) -> Option<Vec<ColumnarValue>> {
    if args.len() == 2 {
        let mut new_args = args.to_owned();
        new_args.push(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
            Some(IntervalMonthDayNano {
                months: 0,
                days: 1,
                nanoseconds: 0,
            }),
        )));
        Some(new_args)
    } else {
        None
    }
}
