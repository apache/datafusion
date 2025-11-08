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

//! Simplified test to isolate LATERAL planning issue

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::BatchedTableFunctionImpl;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_catalog::batched_function::helpers::materialized_batch_stream;
use datafusion_common::DataFusionError;
use datafusion_expr::{Expr, Signature, Volatility};

#[derive(Debug)]
struct DoubleFn {
    signature: Signature,
}

impl DoubleFn {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        }
    }
}

#[async_trait::async_trait]
impl BatchedTableFunctionImpl for DoubleFn {
    fn name(&self) -> &str {
        "double"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<Schema> {
        Ok(Schema::new(vec![Field::new(
            "doubled",
            DataType::Int64,
            true,
        )]))
    }

    async fn invoke_batch(
        &self,
        args: &[ArrayRef],
        _projection: Option<&[usize]>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<datafusion_catalog::BatchResultStream> {
        if args.is_empty() {
            return Err(DataFusionError::Internal(
                "Expected at least one argument".to_string(),
            ));
        }

        let input_array = args[0].as_primitive::<Int64Type>();

        let output_array: Int64Array =
            input_array.iter().map(|v| v.map(|val| val * 2)).collect();

        let schema = self.return_type(&[DataType::Int64])?;
        let output_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(output_array)])?;

        // 1:1 mapping
        let input_row_indices: Vec<u32> = (0..input_array.len() as u32).collect();

        Ok(materialized_batch_stream(output_batch, input_row_indices))
    }
}

#[tokio::test]
async fn test_lateral_no_select() -> Result<()> {
    let ctx = SessionContext::new();

    {
        let state_ref = ctx.state_ref();
        let mut state = state_ref.write();
        state.register_batched_table_function("double", Arc::new(DoubleFn::new()));
    }

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )?;

    ctx.register_batch("numbers", batch)?;

    // Verify LATERAL join creates the correct plan
    let result = ctx
        .sql(
            "SELECT value, doubled FROM numbers CROSS JOIN LATERAL double(numbers.value)",
        )
        .await;

    match result {
        Ok(_df) => {
            println!("Plan created successfully!");
            Ok(())
        }
        Err(e) => {
            println!("Error: {e}");
            Err(e)
        }
    }
}

#[tokio::test]
async fn test_lateral_simple_explain() -> Result<()> {
    let ctx = SessionContext::new();

    {
        let state_ref = ctx.state_ref();
        let mut state = state_ref.write();
        state.register_batched_table_function("double", Arc::new(DoubleFn::new()));
    }

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )?;

    ctx.register_batch("numbers", batch)?;

    // Verify logical planning works
    let df = ctx
        .sql("EXPLAIN SELECT * FROM numbers CROSS JOIN LATERAL double(numbers.value)")
        .await?;

    let results = df.collect().await?;

    println!("EXPLAIN results:");
    for batch in &results {
        println!("{batch:?}");
    }

    Ok(())
}
