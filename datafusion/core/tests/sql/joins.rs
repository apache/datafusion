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
#[ignore]
/// TODO: need to repair. Wrong Test: ambiguous column name: a
async fn nestedjoin_with_alias() -> Result<()> {
    // repro case for https://github.com/apache/arrow-datafusion/issues/2867
    let sql = "select * from ((select 1 as a, 2 as b) c INNER JOIN (select 1 as a, 3 as d) e on c.a = e.a) f;";
    let expected = vec![
        "+---+---+---+---+",
        "| a | b | a | d |",
        "+---+---+---+---+",
        "| 1 | 2 | 1 | 3 |",
        "+---+---+---+---+",
    ];
    let ctx = SessionContext::new();
    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn join_partitioned() -> Result<()> {
    // self join on partition id (workaround for duplicate column name)
    let results = execute_with_partition(
        "SELECT 1 FROM test JOIN (SELECT c1 AS id1 FROM test) AS a ON c1=id1",
        4,
    )
    .await?;

    assert_eq!(
        results.iter().map(|b| b.num_rows()).sum::<usize>(),
        4 * 10 * 10
    );

    Ok(())
}

#[tokio::test]
#[ignore = "Test ignored, will be enabled after fixing the NAAJ bug"]
// https://github.com/apache/arrow-datafusion/issues/4211
async fn null_aware_left_anti_join() -> Result<()> {
    let test_repartition_joins = vec![true, false];
    for repartition_joins in test_repartition_joins {
        let ctx = create_left_semi_anti_join_context_with_null_ids(
            "t1_id",
            "t2_id",
            repartition_joins,
        )
        .unwrap();

        let sql = "SELECT t1_id, t1_name FROM t1 WHERE t1_id NOT IN (SELECT t2_id FROM t2) ORDER BY t1_id";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec!["++", "++"];
        assert_batches_eq!(expected, &actual);
    }

    Ok(())
}
