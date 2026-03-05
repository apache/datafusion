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
use std::time::Duration;

use datafusion::execution::context::SessionContext;
use datafusion::execution::context::TaskContext;
use datafusion::prelude::SessionConfig;
use datafusion_execution::cache::DefaultListFilesCache;
use datafusion_execution::cache::cache_manager::CacheManagerConfig;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
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
async fn test_invalid_memory_limit_when_unit_is_invalid() {
    let ctx = SessionContext::new();

    let result = ctx
        .sql("SET datafusion.runtime.memory_limit = '100X'")
        .await;

    assert!(result.is_err());
    let error_message = result.unwrap_err().to_string();
    assert!(
        error_message
            .contains("Unsupported unit 'X' in 'datafusion.runtime.memory_limit'")
            && error_message.contains("Unit must be one of: 'K', 'M', 'G'")
    );
}

#[tokio::test]
async fn test_invalid_memory_limit_when_limit_is_not_numeric() {
    let ctx = SessionContext::new();

    let result = ctx
        .sql("SET datafusion.runtime.memory_limit = 'invalid_memory_limit'")
        .await;

    assert!(result.is_err());
    let error_message = result.unwrap_err().to_string();
    assert!(error_message.contains(
        "Failed to parse number from 'datafusion.runtime.memory_limit', limit 'invalid_memory_limit'"
    ));
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
async fn test_test_metadata_cache_limit() {
    let ctx = SessionContext::new();

    let update_limit = async |ctx: &SessionContext, limit: &str| {
        ctx.sql(
            format!("SET datafusion.runtime.metadata_cache_limit = '{limit}'").as_str(),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    };

    let get_limit = |ctx: &SessionContext| -> usize {
        ctx.task_ctx()
            .runtime_env()
            .cache_manager
            .get_file_metadata_cache()
            .cache_limit()
    };

    update_limit(&ctx, "100M").await;
    assert_eq!(get_limit(&ctx), 100 * 1024 * 1024);

    update_limit(&ctx, "2G").await;
    assert_eq!(get_limit(&ctx), 2 * 1024 * 1024 * 1024);

    update_limit(&ctx, "123K").await;
    assert_eq!(get_limit(&ctx), 123 * 1024);
}

#[tokio::test]
async fn test_list_files_cache_limit() {
    let list_files_cache = Arc::new(DefaultListFilesCache::default());

    let rt = RuntimeEnvBuilder::new()
        .with_cache_manager(
            CacheManagerConfig::default().with_list_files_cache(Some(list_files_cache)),
        )
        .build_arc()
        .unwrap();

    let ctx = SessionContext::new_with_config_rt(SessionConfig::default(), rt);

    let update_limit = async |ctx: &SessionContext, limit: &str| {
        ctx.sql(
            format!("SET datafusion.runtime.list_files_cache_limit = '{limit}'").as_str(),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    };

    let get_limit = |ctx: &SessionContext| -> usize {
        ctx.task_ctx()
            .runtime_env()
            .cache_manager
            .get_list_files_cache()
            .unwrap()
            .cache_limit()
    };

    update_limit(&ctx, "100M").await;
    assert_eq!(get_limit(&ctx), 100 * 1024 * 1024);

    update_limit(&ctx, "2G").await;
    assert_eq!(get_limit(&ctx), 2 * 1024 * 1024 * 1024);

    update_limit(&ctx, "123K").await;
    assert_eq!(get_limit(&ctx), 123 * 1024);
}

#[tokio::test]
async fn test_list_files_cache_ttl() {
    let list_files_cache = Arc::new(DefaultListFilesCache::default());

    let rt = RuntimeEnvBuilder::new()
        .with_cache_manager(
            CacheManagerConfig::default().with_list_files_cache(Some(list_files_cache)),
        )
        .build_arc()
        .unwrap();

    let ctx = SessionContext::new_with_config_rt(SessionConfig::default(), rt);

    let update_limit = async |ctx: &SessionContext, limit: &str| {
        ctx.sql(
            format!("SET datafusion.runtime.list_files_cache_ttl = '{limit}'").as_str(),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    };

    let get_limit = |ctx: &SessionContext| -> Duration {
        ctx.task_ctx()
            .runtime_env()
            .cache_manager
            .get_list_files_cache()
            .unwrap()
            .cache_ttl()
            .unwrap()
    };

    update_limit(&ctx, "1m").await;
    assert_eq!(get_limit(&ctx), Duration::from_secs(60));

    update_limit(&ctx, "30s").await;
    assert_eq!(get_limit(&ctx), Duration::from_secs(30));

    update_limit(&ctx, "1m30s").await;
    assert_eq!(get_limit(&ctx), Duration::from_secs(90));
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
