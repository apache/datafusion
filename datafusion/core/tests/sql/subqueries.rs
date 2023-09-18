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
use crate::sql::execute_to_batches;

#[tokio::test]
#[ignore]
async fn correlated_scalar_subquery_sum_agg_bug() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "select t1.t1_int from t1 where (select sum(t2_int) is null from t2 where t1.t1_id = t2.t2_id)";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Projection: t1.t1_int [t1_int:UInt32;N]",
        "  Inner Join: t1.t1_id = __scalar_sq_1.t2_id [t1_id:UInt32;N, t1_int:UInt32;N, t2_id:UInt32;N]",
        "    TableScan: t1 projection=[t1_id, t1_int] [t1_id:UInt32;N, t1_int:UInt32;N]",
        "    SubqueryAlias: __scalar_sq_1 [t2_id:UInt32;N]",
        "      Projection: t2.t2_id [t2_id:UInt32;N]",
        "        Filter: SUM(t2.t2_int) IS NULL [t2_id:UInt32;N, SUM(t2.t2_int):UInt64;N]",
        "          Aggregate: groupBy=[[t2.t2_id]], aggr=[[SUM(t2.t2_int)]] [t2_id:UInt32;N, SUM(t2.t2_int):UInt64;N]",
        "            TableScan: t2 projection=[t2_id, t2_int] [t2_id:UInt32;N, t2_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = [
        "+--------+",
        "| t1_int |",
        "+--------+",
        "| 2      |",
        "| 4      |",
        "| 3      |",
        "+--------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}
