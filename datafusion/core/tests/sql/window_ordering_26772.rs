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

use datafusion::arrow::array::{StringViewArray, UInt64Array};
use datafusion::common::Result;
use datafusion::prelude::{SessionConfig, SessionContext};

#[tokio::test]
async fn test_window_ordering_constant_group_key_26772() -> Result<()> {
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);

    ctx.sql("CREATE TABLE t (g VARCHAR, v BIGINT)")
        .await?
        .collect()
        .await?;

    ctx.sql("INSERT INTO t VALUES ('a', 1), ('b', 2)")
        .await?
        .collect()
        .await?;

    let sql = r#"
        SELECT k, DENSE_RANK() OVER (ORDER BY total DESC, k) AS r
        FROM (
            SELECT k, SUM(s) OVER (PARTITION BY k) AS total
            FROM (
                SELECT lower(g) AS k, SUM(v) AS s
                FROM t
                WHERE g = 'a'
                GROUP BY lower(g)
            )
        )
    "#;

    let batches = ctx.sql(sql).await?.collect().await?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    let batch = &batches[0];
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("expected Utf8View key");
    let ranks = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("expected UInt64 rank");

    assert_eq!(keys.value(0), "a");
    assert_eq!(ranks.value(0), 1);

    Ok(())
}
