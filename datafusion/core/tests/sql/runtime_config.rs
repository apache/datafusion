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

//! Tests for runtime configuration SQL interface

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::execution::context::TaskContext;
use datafusion_physical_plan::common::collect;

#[tokio::test]
async fn test_memory_limit_with_spill() {
    let ctx = SessionContext::new();

    ctx.sql("SET datafusion.runtime.memory_limit = '1M'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    ctx.sql("SET datafusion.execution.sort_spill_reservation_bytes = 0")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let query = "select * from generate_series(1,10000000) as t1(v1) order by v1;";
    let df = ctx.sql(query).await.unwrap();

    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = Arc::new(TaskContext::from(&ctx.state()));
    let stream = plan.execute(0, task_ctx).unwrap();

    let _results = collect(stream).await;
    let metrics = plan.metrics().unwrap();
    let spill_count = metrics.spill_count().unwrap();
    assert!(spill_count > 0, "Expected spills but none occurred");
}

#[tokio::test]
async fn test_no_spill_with_adequate_memory() {
    let ctx = SessionContext::new();

    ctx.sql("SET datafusion.runtime.memory_limit = '10M'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("SET datafusion.execution.sort_spill_reservation_bytes = 0")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let query = "select * from generate_series(1,100000) as t1(v1) order by v1;";
    let df = ctx.sql(query).await.unwrap();

    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = Arc::new(TaskContext::from(&ctx.state()));
    let stream = plan.execute(0, task_ctx).unwrap();

    let _results = collect(stream).await;
    let metrics = plan.metrics().unwrap();
    let spill_count = metrics.spill_count().unwrap();
    assert_eq!(spill_count, 0, "Expected no spills but some occurred");
}

#[tokio::test]
async fn test_multiple_configs() {
    let ctx = SessionContext::new();

    ctx.sql("SET datafusion.runtime.memory_limit = '100M'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("SET datafusion.execution.batch_size = '2048'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let query = "select * from generate_series(1,100000) as t1(v1) order by v1;";
    let result = ctx.sql(query).await.unwrap().collect().await;

    assert!(result.is_ok(), "Should not fail due to memory limit");

    let state = ctx.state();
    let batch_size = state.config().options().execution.batch_size;
    assert_eq!(batch_size, 2048);
}

#[tokio::test]
async fn test_memory_limit_enforcement() {
    let ctx = SessionContext::new();

    ctx.sql("SET datafusion.runtime.memory_limit = '1M'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let query = "select * from generate_series(1,100000) as t1(v1) order by v1;";
    let result = ctx.sql(query).await.unwrap().collect().await;

    assert!(result.is_err(), "Should fail due to memory limit");

    ctx.sql("SET datafusion.runtime.memory_limit = '100M'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let result = ctx.sql(query).await.unwrap().collect().await;

    assert!(result.is_ok(), "Should not fail due to memory limit");
}

#[tokio::test]
async fn test_invalid_memory_limit() {
    let ctx = SessionContext::new();

    let result = ctx
        .sql("SET datafusion.runtime.memory_limit = '100X'")
        .await;

    assert!(result.is_err());
    let error_message = result.unwrap_err().to_string();
    assert!(error_message.contains("Unsupported unit 'X'"));
}

#[tokio::test]
async fn test_max_temp_directory_size_enforcement() {
    let ctx = SessionContext::new();

    ctx.sql("SET datafusion.runtime.memory_limit = '1M'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    ctx.sql("SET datafusion.execution.sort_spill_reservation_bytes = 0")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    ctx.sql("SET datafusion.runtime.max_temp_directory_size = '0K'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let query = "select * from generate_series(1,100000) as t1(v1) order by v1;";
    let result = ctx.sql(query).await.unwrap().collect().await;

    assert!(
        result.is_err(),
        "Should fail due to max temp directory size limit"
    );

    ctx.sql("SET datafusion.runtime.max_temp_directory_size = '1M'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let result = ctx.sql(query).await.unwrap().collect().await;

    assert!(
        result.is_ok(),
        "Should not fail due to max temp directory size limit"
    );
}

#[tokio::test]
async fn test_unknown_runtime_config() {
    let ctx = SessionContext::new();

    let result = ctx
        .sql("SET datafusion.runtime.unknown_config = 'value'")
        .await;

    assert!(result.is_err());
    let error_message = result.unwrap_err().to_string();
    assert!(error_message.contains("Unknown runtime configuration"));
}
