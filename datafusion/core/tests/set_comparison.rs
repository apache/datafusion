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

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_common::{Result, assert_batches_eq};

fn build_table(values: &[i32]) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
    let array =
        Arc::new(Int32Array::from(values.to_vec())) as Arc<dyn arrow::array::Array>;
    RecordBatch::try_new(schema, vec![array]).map_err(Into::into)
}

#[tokio::test]
async fn set_comparison_any() -> Result<()> {
    let ctx = SessionContext::new();

    ctx.register_batch("t", build_table(&[1, 6, 10])?)?;
    // Include a NULL in the subquery input to ensure we propagate UNKNOWN correctly.
    ctx.register_batch("s", {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let array = Arc::new(Int32Array::from(vec![Some(5), None]))
            as Arc<dyn arrow::array::Array>;
        RecordBatch::try_new(schema, vec![array])?
    })?;

    let df = ctx
        .sql("select v from t where v > any(select v from s)")
        .await?;
    let results = df.collect().await?;

    assert_batches_eq!(
        &["+----+", "| v  |", "+----+", "| 6  |", "| 10 |", "+----+",],
        &results
    );
    Ok(())
}

#[tokio::test]
async fn set_comparison_all_empty() -> Result<()> {
    let ctx = SessionContext::new();

    ctx.register_batch("t", build_table(&[1, 6, 10])?)?;
    ctx.register_batch(
        "e",
        RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Int32,
            true,
        )]))),
    )?;

    let df = ctx
        .sql("select v from t where v < all(select v from e)")
        .await?;
    let results = df.collect().await?;

    assert_batches_eq!(
        &[
            "+----+", "| v  |", "+----+", "| 1  |", "| 6  |", "| 10 |", "+----+",
        ],
        &results
    );
    Ok(())
}
