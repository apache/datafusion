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
use test_utils::{batches_to_vec, partitions_to_sorted_vec};

#[tokio::test]
async fn sort_with_lots_of_repetition_values() -> Result<()> {
    let ctx = SessionContext::new();
    let filename = "tests/parquet/data/repeat_much.snappy.parquet";

    ctx.register_parquet("rep", filename, ParquetReadOptions::default())
        .await?;
    let sql = "select a from rep order by a";
    let actual = execute_to_batches(&ctx, sql).await;
    let actual = batches_to_vec(&actual);

    let sql1 = "select a from rep";
    let expected = execute_to_batches(&ctx, sql1).await;
    let expected = partitions_to_sorted_vec(&[expected]);

    assert_eq!(actual.len(), expected.len());
    for i in 0..actual.len() {
        assert_eq!(actual[i], expected[i]);
    }
    Ok(())
}
