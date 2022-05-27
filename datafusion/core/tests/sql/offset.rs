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

use super::*;

#[tokio::test]
async fn csv_offset_without_limit() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1 FROM aggregate_test_100 OFFSET 99";
    let actual = execute_to_batches(&ctx, sql).await;

    #[rustfmt::skip]
        let expected = vec![
        "+----+",
        "| c1 |",
        "+----+",
        "| e  |",
        "+----+"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_offset() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1 FROM aggregate_test_100 OFFSET 2 LIMIT 2";
    let actual = execute_to_batches(&ctx, sql).await;

    #[rustfmt::skip]
    let expected = vec![
        "+----+", 
        "| c1 |", 
        "+----+", 
        "| b  |", 
        "| a  |", 
        "+----+"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_offset_the_same_as_nbr_of_rows() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1 FROM aggregate_test_100 LIMIT 1 OFFSET 100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_offset_bigger_than_nbr_of_rows() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1 FROM aggregate_test_100 LIMIT 1 OFFSET 101";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
