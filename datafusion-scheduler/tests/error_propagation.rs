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

//! Proves a task execution error surfaces as a `DataFusionError` out of
//! `run_distributed` -- not swallowed, not hung, not a panic escaping the
//! spawned task -- for the simplest case: a single (no-shuffle) final stage
//! whose plan errors while executing.
//!
//! The failing node here is a `ScalarUDF` that always returns `Err`, wired
//! into a plan of otherwise-ordinary, `datafusion-proto`-native nodes (table
//! scan + projection). That's deliberate: the executor always round-trips
//! every stage's plan through `encode_plan`/`decode_plan` with
//! [`ExchangeCodec`](datafusion_scheduler::ExchangeCodec), which only knows how
//! to serialize `ExchangeSinkExec`/`ExchangeSourceExec`. A hand-rolled leaf
//! `ExecutionPlan` (as opposed to a UDF referenced by name and resolved from
//! the rebuilt session's function registry on decode) would fail at the
//! encode step instead, which would only prove plan serialization errors
//! propagate -- not task execution errors.

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion_scheduler::{SchedulerConfig, run_distributed};

/// A scalar UDF that unconditionally fails when invoked, with a message
/// ("boom") distinctive enough to assert on without false-matching some
/// unrelated error path.
#[derive(Debug, PartialEq, Eq, Hash)]
struct BoomUdf {
    signature: Signature,
}

impl BoomUdf {
    fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![DataType::Int32], Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for BoomUdf {
    fn name(&self) -> &str {
        "boom"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> datafusion::error::Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        Err(DataFusionError::Execution("boom".to_string()))
    }
}

#[tokio::test]
async fn task_execution_error_surfaces_from_run_distributed() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();
    ctx.register_udf(ScalarUDF::from(BoomUdf::new()));

    // A single-partition, no-shuffle plan: `create_stages` produces exactly
    // one (final) stage, so this exercises `execute_stage_graph`'s
    // single-stage path end to end, including the encode/decode round-trip.
    let df = ctx.sql("SELECT boom(a) FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();

    let config = SchedulerConfig::in_memory(&ctx);
    let result = run_distributed(&ctx, plan, config).await;

    let err = result
        .expect_err("task execution error must surface as an Err, not be swallowed");
    let msg = err.to_string();
    assert!(
        msg.contains("boom"),
        "expected error message to contain \"boom\", got: {msg}"
    );
}
