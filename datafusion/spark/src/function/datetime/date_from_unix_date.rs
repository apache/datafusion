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

use arrow::datatypes::DataType;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ExprSchemable, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

/// Spark-compatible `date_from_unix_date` function.
/// Creates a date from the number of days since epoch (1970-01-01).
/// <https://spark.apache.org/docs/latest/api/sql/index.html#date_from_unix_date>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateFromUnixDate {
    signature: Signature,
}

impl Default for SparkDateFromUnixDate {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDateFromUnixDate {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkDateFromUnixDate {
    fn name(&self) -> &str {
        "date_from_unix_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("invoke_with_args should not be called on SparkDateFromUnixDate")
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [days] = take_function_args(self.name(), args)?;
        Ok(ExprSimplifyResult::Simplified(
            days.cast_to(&DataType::Date32, info.schema())?,
        ))
    }
}
