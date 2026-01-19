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
use std::ops::Add;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, IntervalUnit};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};

/// <https://spark.apache.org/docs/latest/api/sql/index.html#add_months>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkAddMonths {
    signature: Signature,
}

impl Default for SparkAddMonths {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkAddMonths {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Date32, DataType::Int32],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkAddMonths {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "add_months"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());

        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Date32,
            nullable,
        )))
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [date_arg, months_arg] = take_function_args("add_months", args)?;
        let interval = months_arg
            .cast_to(&DataType::Interval(IntervalUnit::YearMonth), info.schema())?;
        Ok(ExprSimplifyResult::Simplified(date_arg.add(interval)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("invoke should not be called on a simplified add_months() function")
    }
}
