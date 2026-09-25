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

use datafusion::{assert_batches_sorted_eq, physical_plan::collect};

use super::*;

#[tokio::test]
async fn intersect_all_preserves_duplicate_counts_and_nulls() -> Result<()> {
    let ctx = SessionContext::new();
    let batches = collect(
        ctx.sql(
            "SELECT * FROM VALUES (NULL), (NULL), (1), (1), (2)
             INTERSECT ALL
             SELECT * FROM VALUES (NULL), (1), (1), (1), (3)",
        )
        .await?
        .create_physical_plan()
        .await?,
        ctx.task_ctx(),
    )
    .await?;

    assert_batches_sorted_eq!(
        [
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 1       |",
            "| 1       |",
            "|         |",
            "+---------+",
        ],
        &batches
    );
    Ok(())
}

#[tokio::test]
async fn except_all_preserves_duplicate_counts_and_nulls() -> Result<()> {
    let ctx = SessionContext::new();
    let batches = collect(
        ctx.sql(
            "SELECT * FROM VALUES (NULL), (NULL), (1), (1), (2)
             EXCEPT ALL
             SELECT * FROM VALUES (NULL), (1), (1), (1), (3)",
        )
        .await?
        .create_physical_plan()
        .await?,
        ctx.task_ctx(),
    )
    .await?;

    assert_batches_sorted_eq!(
        [
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 2       |",
            "|         |",
            "+---------+",
        ],
        &batches
    );
    Ok(())
}
