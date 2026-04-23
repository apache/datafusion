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

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::buffer::BooleanBuffer;
use datafusion_common::cast::{
    as_binary_array, as_binary_view_array, as_large_binary_array,
};
use datafusion_common::utils::take_function_args;
use datafusion_functions::utils::make_scalar_function;

use std::sync::Arc;

/// Spark-compatible `is_valid_utf8` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#is_valid_utf8>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIsValidUtf8 {
    signature: Signature,
}

impl Default for SparkIsValidUtf8 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkIsValidUtf8 {
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

impl ScalarUDFImpl for SparkIsValidUtf8 {
    fn name(&self) -> &str {
        "is_valid_utf8"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Boolean, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_is_valid_utf8_inner, vec![])(&args.args)
    }
}

fn spark_is_valid_utf8_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("is_valid_utf8", args)?;
    match array.data_type() {
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
            Ok(Arc::new(BooleanArray::new(
                BooleanBuffer::new_set(array.len()),
                array.nulls().cloned(),
            )))
        }
        DataType::Binary => Ok(Arc::new(
            as_binary_array(array)?
                .iter()
                .map(|x| x.map(|y| str::from_utf8(y).is_ok()))
                .collect::<BooleanArray>(),
        )),
        DataType::LargeBinary => Ok(Arc::new(
            as_large_binary_array(array)?
                .iter()
                .map(|x| x.map(|y| str::from_utf8(y).is_ok()))
                .collect::<BooleanArray>(),
        )),
        DataType::BinaryView => Ok(Arc::new(
            as_binary_view_array(array)?
                .iter()
                .map(|x| x.map(|y| str::from_utf8(y).is_ok()))
                .collect::<BooleanArray>(),
        )),
        data_type => {
            internal_err!("is_valid_utf8 does not support: {data_type}")
        }
    }
}
