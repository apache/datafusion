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

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::prelude::*;
use datafusion_common::test_util::format_batches;
use datafusion_common::{Result, assert_batches_eq};
use datafusion_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

fn register_table_and_udf() -> Result<SessionContext> {
    let num_rows = 3;
    let batch_size = 2;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("prompt", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from((0..num_rows).collect::<Vec<i32>>())),
            Arc::new(StringArray::from(
                (0..num_rows)
                    .map(|i| format!("prompt{i}"))
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("test_table", batch)?;

    ctx.register_udf(
        AsyncScalarUDF::new(Arc::new(TestAsyncUDFImpl::new(batch_size)))
            .into_scalar_udf(),
    );

    Ok(ctx)
}

// This test checks the case where batch_size doesn't evenly divide
// the number of rows.
#[tokio::test]
async fn test_async_udf_with_non_modular_batch_size() -> Result<()> {
    let ctx = register_table_and_udf()?;

    let df = ctx
        .sql("SELECT id, test_async_udf(prompt) as result FROM test_table")
        .await?;

    let result = df.collect().await?;

    assert_batches_eq!(
        &[
            "+----+---------+",
            "| id | result  |",
            "+----+---------+",
            "| 0  | prompt0 |",
            "| 1  | prompt1 |",
            "| 2  | prompt2 |",
            "+----+---------+"
        ],
        &result
    );

    Ok(())
}

// This test checks if metrics are printed for `AsyncFuncExec`
#[tokio::test]
async fn test_async_udf_metrics() -> Result<()> {
    let ctx = register_table_and_udf()?;

    let df = ctx
        .sql(
            "EXPLAIN ANALYZE SELECT id, test_async_udf(prompt) as result FROM test_table",
        )
        .await?;

    let result = df.collect().await?;

    let explain_analyze_str = format_batches(&result)?.to_string();
    let async_func_exec_without_metrics =
        explain_analyze_str.split("\n").any(|metric_line| {
            metric_line.contains("AsyncFuncExec")
                && !metric_line.contains("output_rows=3")
        });

    assert!(!async_func_exec_without_metrics);

    Ok(())
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct TestAsyncUDFImpl {
    batch_size: usize,
    signature: Signature,
}

impl TestAsyncUDFImpl {
    fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for TestAsyncUDFImpl {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "test_async_udf"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        panic!("Call invoke_async_with_args instead")
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for TestAsyncUDFImpl {
    fn ideal_batch_size(&self) -> Option<usize> {
        Some(self.batch_size)
    }
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let arg1 = &args.args[0];
        let results = call_external_service(arg1.clone()).await?;
        Ok(results)
    }
}

/// Simulates calling an async external service
async fn call_external_service(arg1: ColumnarValue) -> Result<ColumnarValue> {
    Ok(arg1)
}
