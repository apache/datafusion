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

//! End-to-end SQL tests for the multi-`COUNT(DISTINCT)` logical optimizer rewrite.

use super::*;
use arrow::array::{Int32Array, StringArray};
use datafusion::common::test_util::batches_to_sort_string;
use datafusion_catalog::MemTable;

#[tokio::test]
async fn multi_count_distinct_matches_expected_with_nulls() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int32, false),
        Field::new("b", DataType::Utf8, true),
        Field::new("c", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 1, 1])),
            Arc::new(StringArray::from(vec![Some("x"), None, Some("x")])),
            Arc::new(StringArray::from(vec![None, Some("y"), Some("y")])),
        ],
    )?;
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let sql =
        "SELECT g, COUNT(DISTINCT b) AS cb, COUNT(DISTINCT c) AS cc FROM t GROUP BY g";
    let batches = ctx.sql(sql).await?.collect().await?;
    let out = batches_to_sort_string(&batches);

    assert_eq!(
        out,
        "+---+----+----+\n\
         | g | cb | cc |\n\
         +---+----+----+\n\
         | 1 | 1  | 1  |\n\
         +---+----+----+"
    );
    Ok(())
}
