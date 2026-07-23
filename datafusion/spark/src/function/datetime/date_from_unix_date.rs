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

use std::sync::Arc;

use arrow::array::{Array, Date32Array, Int32Array};
use arrow::datatypes::DataType;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err, internal_datafusion_err};
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [days] = take_function_args(self.name(), args.args)?;
        match days {
            ColumnarValue::Array(array) => {
                let days =
                    array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                        internal_datafusion_err!(
                            "date_from_unix_date expected an Int32 array"
                        )
                    })?;
                // Date32 and Int32 both count days since the Unix epoch
                // (1970-01-01), so the i32 values transfer directly.
                let dates =
                    Date32Array::new(days.values().clone(), days.nulls().cloned());
                Ok(ColumnarValue::Array(Arc::new(dates)))
            }
            ColumnarValue::Scalar(ScalarValue::Int32(days)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(days)))
            }
            ColumnarValue::Scalar(other) => {
                exec_err!("date_from_unix_date expected an Int32 argument, got {other:?}")
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    /// Exercises the physical `invoke_with_args` path directly, as consumers
    /// that build physical `ScalarFunctionExpr` nodes (e.g. DataFusion Comet)
    /// call it without ever running the logical `SimplifyExpressions` pass.
    #[test]
    fn test_date_from_unix_date_array() -> Result<()> {
        let func = SparkDateFromUnixDate::new();

        let input = Int32Array::from(vec![Some(0), Some(1), Some(-1), None, Some(18628)]);
        let result = func.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input))],
            arg_fields: vec![Arc::new(Field::new("arg", DataType::Int32, true))],
            number_rows: 5,
            return_field: Arc::new(Field::new(
                "date_from_unix_date",
                DataType::Date32,
                true,
            )),
            config_options: Arc::new(Default::default()),
        })?;
        match result {
            ColumnarValue::Array(arr) => {
                let expected = Date32Array::from(vec![
                    Some(0),
                    Some(1),
                    Some(-1),
                    None,
                    Some(18628),
                ]);
                assert_eq!(arr.as_ref(), &expected as &dyn Array);
            }
            other => panic!("unexpected result: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_date_from_unix_date_scalar() -> Result<()> {
        let func = SparkDateFromUnixDate::new();

        let result = func.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(0)))],
            arg_fields: vec![Arc::new(Field::new("arg", DataType::Int32, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new(
                "date_from_unix_date",
                DataType::Date32,
                true,
            )),
            config_options: Arc::new(Default::default()),
        })?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Date32(Some(v))) => assert_eq!(v, 0),
            other => panic!("unexpected result: {other:?}"),
        }

        let result = func.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Int32(None))],
            arg_fields: vec![Arc::new(Field::new("arg", DataType::Int32, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new(
                "date_from_unix_date",
                DataType::Date32,
                true,
            )),
            config_options: Arc::new(Default::default()),
        })?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Date32(None)) => {}
            other => panic!("unexpected result: {other:?}"),
        }

        Ok(())
    }
}
