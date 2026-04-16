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

use arrow::array::{ArrayRef, LargeStringArray, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::cast::{
    as_binary_array, as_binary_view_array, as_large_binary_array,
};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions::utils::make_scalar_function;
use std::sync::Arc;

/// Spark-compatible `make_valid_utf8` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#make_valid_utf8>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeValidUtf8 {
    signature: Signature,
}

impl Default for SparkMakeValidUtf8 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMakeValidUtf8 {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![
                    DataType::Utf8,
                    DataType::LargeUtf8,
                    DataType::Utf8View,
                    DataType::Binary,
                    DataType::BinaryView,
                    DataType::LargeBinary,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkMakeValidUtf8 {
    fn name(&self) -> &str {
        "make_valid_utf8"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [make_valid_utf8] = take_function_args(self.name(), args.arg_fields)?;
        let return_type = match make_valid_utf8.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Ok(make_valid_utf8.data_type().clone())
            }
            DataType::Binary | DataType::BinaryView => Ok(DataType::Utf8),
            DataType::LargeBinary => Ok(DataType::LargeUtf8),
            data_type => internal_err!("make_valid_utf8 does not support: {data_type}"),
        }?;
        Ok(Arc::new(Field::new(
            self.name(),
            return_type,
            make_valid_utf8.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_make_valid_utf8_inner, vec![])(&args.args)
    }
}

fn spark_make_valid_utf8_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let array = &args[0];
    match &array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(array.to_owned()),
        DataType::Binary => Ok(Arc::new(
            as_binary_array(&array)?
                .iter()
                .map(|x| x.map(String::from_utf8_lossy))
                .collect::<StringArray>(),
        )),
        DataType::BinaryView => Ok(Arc::new(
            as_binary_view_array(&array)?
                .iter()
                .map(|x| x.map(String::from_utf8_lossy))
                .collect::<StringArray>(),
        )),
        DataType::LargeBinary => Ok(Arc::new(
            as_large_binary_array(&array)?
                .iter()
                .map(|x| x.map(String::from_utf8_lossy))
                .collect::<LargeStringArray>(),
        )),
        data_type => {
            internal_err!("make_valid_utf8 does not support: {data_type}")
        }
    }
}
