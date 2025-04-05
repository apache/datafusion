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

use datafusion::prelude::*;
use datafusion_common::Result;

#[tokio::test]
async fn test_set_memory_limit() -> Result<()> {
    let ctx = SessionContext::new();

    // Set memory limit to 100MB using SQL - note the quotes around the value
    ctx.sql("SET datafusion.runtime.memory_limit = '100M'")
        .await?
        .collect()
        .await?;

    ctx.sql("CREATE TABLE test (a INT) AS VALUES (1), (2), (3)")
        .await?
        .collect()
        .await?;
    let df = ctx.sql("SELECT * FROM test").await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 3);

    Ok(())
}

#[tokio::test]
async fn test_invalid_memory_limit() -> Result<()> {
    let ctx = SessionContext::new();

    // Try to set an invalid memory limit - note the quotes around the value
    let result = ctx
        .sql("SET datafusion.runtime.memory_limit = '100X'")
        .await;

    assert!(result.is_err());
    let error_message = result.unwrap_err().to_string();
    assert!(error_message.contains("Unsupported unit 'X'"));
    Ok(())
}

#[tokio::test]
async fn test_unknown_runtime_config() -> Result<()> {
    let ctx = SessionContext::new();

    let result = ctx
        .sql("SET datafusion.runtime.unknown_config = 'value'")
        .await;

    assert!(result.is_err());
    let error_message = result.unwrap_err().to_string();
    assert!(error_message.contains("Unknown runtime configuration"));
    Ok(())
}
