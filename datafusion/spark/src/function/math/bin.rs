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

use arrow::array::{ArrayRef, AsArray, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
use datafusion_common::types::{NativeType, logical_int64};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{Coercion, ScalarFunctionArgs, ScalarUDFImpl, TypeSignatureClass};
use datafusion_functions::utils::make_scalar_function;
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible `bin` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#bin>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBin {
    signature: Signature,
}

impl Default for SparkBin {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBin {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Coercible(vec![Coercion::new_implicit(
                    TypeSignatureClass::Native(logical_int64()),
                    vec![TypeSignatureClass::Numeric],
                    NativeType::Int64,
                )])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkBin {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bin"
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
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Utf8,
            args.arg_fields[0].is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_bin_inner, vec![])(&args.args)
    }
}

fn spark_bin_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("bin", arg)?;
    match &array.data_type() {
        DataType::Int64 => {
            let result: StringArray = array
                .as_primitive::<Int64Type>()
                .iter()
                .map(|opt| opt.map(spark_bin))
                .collect();
            Ok(Arc::new(result))
        }
        data_type => {
            internal_err!("bin does not support: {data_type}")
        }
    }
}

fn spark_bin(value: i64) -> String {
    format!("{value:b}")
}
