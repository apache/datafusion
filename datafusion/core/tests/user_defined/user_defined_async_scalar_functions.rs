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
use std::time::Duration;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::prelude::*;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[tokio::test]
async fn test_async_udf_with_non_modular_batch_size() -> Result<()> {
    let num_rows = 3;
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
                    .map(|i| format!("prompt{}", i))
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;

    println!("Created test data with {} rows\n", batch.num_rows());

    // Create context and register UDF
    let ctx = SessionContext::new();
    ctx.register_batch("test_table", batch)?;

    ctx.register_udf(
        AsyncScalarUDF::new(Arc::new(TestAsyncUDFImpl::new(2))).into_scalar_udf(),
    );

    // Execute query
    println!("Executing query...\n");
    let df = ctx
        .sql("SELECT id, test_async_udf(prompt) as result FROM test_table")
        .await?;

    let results = df.collect().await?;

    println!("=== Final Results ===");
    for batch in results {
        println!("Result batch has {} rows", batch.num_rows());
        println!("{:?}", batch);
    }

    Ok(())
}

/// Helper function to convert ColumnarValue to Vec<String>
fn columnar_to_vec_string(cv: &ColumnarValue) -> Result<Vec<String>> {
    match cv {
        ColumnarValue::Array(arr) => {
            let string_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(string_arr
                .iter()
                .map(|s| s.unwrap_or("").to_string())
                .collect())
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(vec![s.clone()]),
        _ => panic!("Unexpected type"),
    }
}

/// Simulates calling an async external service
async fn call_external_service(arg1: &ColumnarValue) -> Result<Vec<String>> {
    let vec1 = columnar_to_vec_string(arg1)?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    Ok(vec1)
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
        let results = call_external_service(arg1).await?;
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(results))))
    }
}
